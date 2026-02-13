#!/usr/bin/env python3
"""Autonomous TMDB -> MDBList -> PMDB sync pipeline.

Architecture:
- Harvester loop: Fetch configured TMDB sources, enrich with MDBList (batched),
  normalize ratings, and persist pending work in SQLite.
- Submitter loop: Drain pending ratings/mappings from SQLite and submit to PMDB
  under configured contribution limits.

No interactive prompts are used. Queue state is fully persisted in SQLite so the
process can resume after restarts.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import datetime as dt
import email.utils
import json
import logging
import math
import random
import signal
import socket
import sqlite3
import threading
import time
import warnings
from collections import defaultdict, deque
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

# Suppress urllib3/OpenSSL startup warning on macOS LibreSSL Python builds.
warnings.filterwarnings(
    "ignore",
    message=r"urllib3 v2 only supports OpenSSL 1\.1\.1\+.*",
)

import requests
from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text


LOGGER = logging.getLogger("mdblist-pmdb")


DEFAULT_CONFIG: Dict[str, Any] = {
    "api_keys": {
        "tmdb": "",
        "mdblist": "",
        "pmdb": "",
    },
    "runtime": {
        "database_path": "mdblist_pmdb.sqlite3",
        "log_file_path": "logs/mdblist_pmdb.log",
        "log_file_max_bytes": 10485760,
        "log_file_backup_count": 5,
        "ratings_refresh_days": 7,
        "harvest_mode": "all",
        "run_mode": "continuous",
        "harvester_cycle_sleep_seconds": 30,
        "submitter_poll_seconds": 2,
        "submitter_workers": 8,
        "submitter_in_flight_lease_seconds": 300,
        "submitter_max_retry_attempts": 12,
        "console_mode": "dashboard",
        "debug_raw_console_logs": False,
        "dashboard_event_lines": 8,
        "dashboard_event_dedupe_window_seconds": 30,
        "dashboard_event_max_message_length": 160,
        "log_level": "INFO",
    },
    "tmdb": {
        "base_url": "https://api.themoviedb.org/3",
        "language": "en-US",
        "timeout_seconds": 20,
        "max_retries": 5,
        "details_concurrency": 10,
        "rate_limit": {
            "requests": 45,
            "per_seconds": 1,
        },
        "sources": [
            {"name": "movie/popular", "enabled": True, "max_pages": 500},
            {"name": "movie/top_rated", "enabled": True, "max_pages": 500},
            {"name": "tv/popular", "enabled": True, "max_pages": 500},
            {"name": "tv/top_rated", "enabled": True, "max_pages": 500},
            {"name": "trending/movie/day", "enabled": True, "max_pages": 500},
            {"name": "trending/movie/week", "enabled": True, "max_pages": 500},
            {"name": "trending/tv/day", "enabled": True, "max_pages": 500},
            {"name": "trending/tv/week", "enabled": True, "max_pages": 500},
            {"name": "trending/all/day", "enabled": True, "max_pages": 500},
            {"name": "trending/all/week", "enabled": True, "max_pages": 500},
        ],
    },
    "mdblist": {
        "base_url": "https://api.mdblist.com",
        "timeout_seconds": 25,
        "max_retries": 5,
        "batch_size": 100,
        "max_batch_size": 200,
        "max_pause_block_seconds": 20,
        "rate_limit": {
            "requests": 1,
            "per_seconds": 1,
        },
        "append_to_response": [],
    },
    "pmdb": {
        "base_url": "https://publicmetadb.com",
        "timeout_seconds": 20,
        "max_retries": 5,
        "api_rate_limit": {
            "requests": 300,
            "per_seconds": 10,
        },
        "ratings_limit": {
            "requests": 200,
            "per_seconds": 3600,
        },
        "mappings_limit": {
            "requests": 100,
            "per_seconds": 3600,
        },
    },
}


SUPPORTED_PMDB_MAPPING_TYPES: Tuple[str, ...] = (
    "imdb",
    "tvdb",
    "mal",
    "anilist",
    "anidb",
    "trakt",
)

SUPPORTED_TMDB_SOURCE_NAMES: Set[str] = {
    "movie/popular",
    "movie/top_rated",
    "movie/now_playing",
    "movie/upcoming",
    "tv/popular",
    "tv/top_rated",
    "tv/on_the_air",
    "tv/airing_today",
    "trending/movie/day",
    "trending/movie/week",
    "trending/tv/day",
    "trending/tv/week",
    "trending/all/day",
    "trending/all/week",
}

SUPPORTED_HARVEST_MODES: Set[str] = {
    "new_only",
    "failed_only",
    "ttl_only",
    "all",
}

SUPPORTED_RUN_MODES: Set[str] = {
    "continuous",
    "once",
    "until_idle",
}

SUPPORTED_CONSOLE_MODES: Set[str] = {
    "dashboard",
    "raw",
}

HARVEST_CHECKPOINT_KEY = "harvester_checkpoint_v1"
RETRY_STORM_CLEANUP_KEY = "submitter_retry_storm_cleanup_v1"


def now_epoch() -> int:
    return int(time.time())


def local_day_key(ts: Optional[int] = None) -> str:
    epoch = now_epoch() if ts is None else int(ts)
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).astimezone().strftime("%Y-%m-%d")


def to_iso(ts: int) -> str:
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).astimezone().strftime(
        "%d-%m-%y %H:%M:%S"
    )


def parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def clamp_0_100(value: float) -> Optional[float]:
    if value is None:
        return None
    if not math.isfinite(value):
        return None
    if value <= 0:
        return None
    return round(min(100.0, max(0.0, value)), 1)


def parse_retry_after(value: Optional[str], default_seconds: int = 5) -> int:
    if not value:
        return default_seconds

    stripped = value.strip()
    as_int = parse_int(stripped)
    if as_int is not None:
        return max(1, as_int)

    try:
        dt_value = email.utils.parsedate_to_datetime(stripped)
        if dt_value.tzinfo is None:
            dt_value = dt_value.replace(tzinfo=dt.timezone.utc)
        delta = int((dt_value - dt.datetime.now(dt.timezone.utc)).total_seconds())
        return max(1, delta)
    except Exception:
        return default_seconds


def sanitize_url_for_logs(url: str) -> str:
    sensitive_keys = {
        "api_key",
        "apikey",
        "token",
        "access_token",
        "auth",
        "authorization",
        "key",
    }
    try:
        parts = urlsplit(url)
        if not parts.query:
            return url
        sanitized_query = []
        for key, value in parse_qsl(parts.query, keep_blank_values=True):
            if key.lower() in sensitive_keys:
                sanitized_query.append((key, "***"))
            else:
                sanitized_query.append((key, value))
        return urlunsplit(
            (
                parts.scheme,
                parts.netloc,
                parts.path,
                urlencode(sanitized_query, doseq=True),
                parts.fragment,
            )
        )
    except Exception:
        return url


def is_network_unavailable_error(exc: requests.RequestException) -> bool:
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True
    marker_text = str(exc).lower()
    markers = (
        "nameresolutionerror",
        "failed to resolve",
        "temporary failure in name resolution",
        "nodename nor servname provided",
        "network is unreachable",
        "no route to host",
        "connection refused",
    )
    if any(marker in marker_text for marker in markers):
        return True
    cause = getattr(exc, "__cause__", None)
    if isinstance(cause, (socket.gaierror, TimeoutError, OSError)):
        return True
    return False


def merge_dict(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge_dict(result[key], value)
        else:
            result[key] = value
    return result


def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(
            f"Config file not found at {path}. Create one (for example from config.example.json)."
        )

    with path.open("r", encoding="utf-8") as handle:
        loaded = json.load(handle)

    config = merge_dict(DEFAULT_CONFIG, loaded)

    missing_keys = []
    for key_name in ("tmdb", "mdblist", "pmdb"):
        raw_value = str(config.get("api_keys", {}).get(key_name, "")).strip()
        if not raw_value:
            missing_keys.append(f"api_keys.{key_name}")
            continue

        placeholder_markers = (
            "YOUR_",
            "YOUR-",
            "CHANGEME",
            "REPLACE_ME",
        )
        if any(marker in raw_value.upper() for marker in placeholder_markers):
            missing_keys.append(f"api_keys.{key_name}")
    if missing_keys:
        raise ValueError(
            "Missing required API keys in config: " + ", ".join(missing_keys)
        )

    # Guardrails for values that must be positive.
    config["runtime"]["submitter_poll_seconds"] = max(
        0.01, float(config["runtime"]["submitter_poll_seconds"])
    )
    config["runtime"]["submitter_workers"] = max(
        1, int(config["runtime"].get("submitter_workers", 8))
    )
    config["runtime"]["submitter_in_flight_lease_seconds"] = max(
        30, int(config["runtime"].get("submitter_in_flight_lease_seconds", 300))
    )
    config["runtime"]["submitter_max_retry_attempts"] = max(
        1, int(config["runtime"].get("submitter_max_retry_attempts", 12))
    )
    log_file_path = str(
        config["runtime"].get("log_file_path", "logs/mdblist_pmdb.log")
    ).strip()
    config["runtime"]["log_file_path"] = log_file_path or "logs/mdblist_pmdb.log"
    config["runtime"]["log_file_max_bytes"] = max(
        1024, int(config["runtime"].get("log_file_max_bytes", 10485760))
    )
    config["runtime"]["log_file_backup_count"] = max(
        0, int(config["runtime"].get("log_file_backup_count", 5))
    )
    config["runtime"]["harvester_cycle_sleep_seconds"] = max(
        1, int(config["runtime"]["harvester_cycle_sleep_seconds"])
    )
    config["runtime"]["ratings_refresh_days"] = max(
        1, int(config["runtime"]["ratings_refresh_days"])
    )
    config["runtime"]["dashboard_event_lines"] = max(
        3, min(20, int(config["runtime"].get("dashboard_event_lines", 8)))
    )
    config["runtime"]["dashboard_event_dedupe_window_seconds"] = max(
        1, int(config["runtime"].get("dashboard_event_dedupe_window_seconds", 30))
    )
    config["runtime"]["dashboard_event_max_message_length"] = max(
        60, int(config["runtime"].get("dashboard_event_max_message_length", 160))
    )
    config["runtime"]["debug_raw_console_logs"] = bool(
        config["runtime"].get("debug_raw_console_logs", False)
    )

    harvest_mode = str(config["runtime"].get("harvest_mode", "all")).strip().lower()
    if harvest_mode not in SUPPORTED_HARVEST_MODES:
        raise ValueError(
            "Invalid runtime.harvest_mode. Expected one of: "
            + ", ".join(sorted(SUPPORTED_HARVEST_MODES))
        )
    config["runtime"]["harvest_mode"] = harvest_mode

    run_mode = str(config["runtime"].get("run_mode", "continuous")).strip().lower()
    if run_mode not in SUPPORTED_RUN_MODES:
        raise ValueError(
            "Invalid runtime.run_mode. Expected one of: "
            + ", ".join(sorted(SUPPORTED_RUN_MODES))
        )
    config["runtime"]["run_mode"] = run_mode

    console_mode = str(config["runtime"].get("console_mode", "dashboard")).strip().lower()
    if console_mode not in SUPPORTED_CONSOLE_MODES:
        raise ValueError(
            "Invalid runtime.console_mode. Expected one of: "
            + ", ".join(sorted(SUPPORTED_CONSOLE_MODES))
        )
    config["runtime"]["console_mode"] = console_mode

    for section, rate_key in (
        ("tmdb", "rate_limit"),
        ("mdblist", "rate_limit"),
        ("pmdb", "api_rate_limit"),
        ("pmdb", "ratings_limit"),
        ("pmdb", "mappings_limit"),
    ):
        rate_cfg = config[section][rate_key]
        rate_cfg["requests"] = max(1, int(rate_cfg["requests"]))
        rate_cfg["per_seconds"] = max(0.001, float(rate_cfg["per_seconds"]))

    config["mdblist"]["batch_size"] = max(1, int(config["mdblist"]["batch_size"]))
    config["mdblist"]["max_batch_size"] = max(1, int(config["mdblist"]["max_batch_size"]))
    config["mdblist"]["max_pause_block_seconds"] = max(
        1, int(config["mdblist"]["max_pause_block_seconds"])
    )

    for section in ("tmdb", "mdblist", "pmdb"):
        config[section]["max_retries"] = max(1, int(config[section]["max_retries"]))
        config[section]["timeout_seconds"] = max(
            1, int(config[section]["timeout_seconds"])
        )

    raw_sources = config["tmdb"].get("sources")
    if not isinstance(raw_sources, list) or not raw_sources:
        raise ValueError("tmdb.sources must be a non-empty list")

    normalized_sources: List[Dict[str, Any]] = []
    for idx, source in enumerate(raw_sources):
        if not isinstance(source, dict):
            raise ValueError(f"tmdb.sources[{idx}] must be an object")

        name = str(source.get("name", "")).strip().lower().lstrip("/")
        if name not in SUPPORTED_TMDB_SOURCE_NAMES:
            raise ValueError(
                f"tmdb.sources[{idx}].name='{name}' is unsupported. "
                f"Supported values: {', '.join(sorted(SUPPORTED_TMDB_SOURCE_NAMES))}"
            )

        enabled = bool(source.get("enabled", True))
        max_pages = max(1, min(500, int(source.get("max_pages", 500))))
        normalized_sources.append(
            {
                "name": name,
                "enabled": enabled,
                "max_pages": max_pages,
            }
        )

    if not any(src["enabled"] for src in normalized_sources):
        raise ValueError("At least one tmdb.sources entry must be enabled")

    config["tmdb"]["sources"] = normalized_sources

    return config


@dataclass
class APIResponse:
    status: int
    headers: Dict[str, str]
    data: Any
    text: str

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300


@dataclass
class Candidate:
    tmdb_id: int
    media_type: str  # 'movie' or 'tv'
    title: str
    popularity: float


@dataclass
class TMDBSource:
    name: str
    endpoint: str
    media_type_hint: Optional[str]
    max_pages: int


@dataclass
class HarvestCycleResult:
    selected_candidates: int
    tmdb_list_request_errors: int
    mdblist_request_failures: int
    interrupted: bool = False
    resumed_from_checkpoint: bool = False


@dataclass
class DashboardEvent:
    timestamp: int
    level: str
    message: str
    count: int = 1
    signature: str = ""


class DashboardEventBuffer:
    def __init__(
        self,
        *,
        max_lines: int,
        dedupe_window_seconds: int,
        max_message_length: int,
    ):
        self.max_lines = max(1, int(max_lines))
        self.dedupe_window_seconds = max(1, int(dedupe_window_seconds))
        self.max_message_length = max(40, int(max_message_length))
        self._events: deque[DashboardEvent] = deque(maxlen=self.max_lines)
        self._lock = threading.Lock()

    def _normalize_message(self, message: str) -> str:
        collapsed = " ".join(str(message or "").replace("\r", " ").replace("\n", " ").split())
        if not collapsed:
            return "-"
        if len(collapsed) <= self.max_message_length:
            return collapsed
        cutoff = max(1, self.max_message_length - 3)
        return f"{collapsed[:cutoff]}..."

    def add(self, *, level: str, message: str, now_ts: Optional[int] = None) -> None:
        ts = now_epoch() if now_ts is None else int(now_ts)
        level_name = str(level or "INFO").upper()
        normalized = self._normalize_message(message)
        signature = f"{level_name}:{normalized}"

        with self._lock:
            if self._events:
                last = self._events[-1]
                if (
                    last.signature == signature
                    and ts - last.timestamp <= self.dedupe_window_seconds
                ):
                    last.count += 1
                    last.timestamp = ts
                    return
            self._events.append(
                DashboardEvent(
                    timestamp=ts,
                    level=level_name,
                    message=normalized,
                    count=1,
                    signature=signature,
                )
            )

    def snapshot(self) -> List[DashboardEvent]:
        with self._lock:
            return list(self._events)


class LiveLogState:
    def __init__(self):
        self._live_active = False
        self._lock = threading.Lock()

    def set_live_active(self, active: bool) -> None:
        with self._lock:
            self._live_active = bool(active)

    def is_live_active(self) -> bool:
        with self._lock:
            return self._live_active


class LiveAwareConsoleHandler(logging.StreamHandler):
    def __init__(self, *, live_state: LiveLogState, allow_while_live: bool):
        super().__init__()
        self.live_state = live_state
        self.allow_while_live = bool(allow_while_live)

    def emit(self, record: logging.LogRecord) -> None:
        if self.live_state.is_live_active() and not self.allow_while_live:
            return
        clean_record = logging.makeLogRecord(record.__dict__.copy())
        # Keep terminal output single-line and stable; tracebacks go to file logs.
        clean_record.exc_info = None
        clean_record.exc_text = None
        clean_record.stack_info = None
        super().emit(clean_record)


class DashboardEventHandler(logging.Handler):
    def __init__(self, *, buffer: DashboardEventBuffer, min_level: int = logging.WARNING):
        super().__init__(level=min_level)
        self.buffer = buffer

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = record.getMessage()
            if record.exc_info:
                exc_type = record.exc_info[0].__name__ if record.exc_info[0] else "Exception"
                exc_value = str(record.exc_info[1] or "").strip()
                if exc_value:
                    message = f"{message} ({exc_type}: {exc_value})"
                else:
                    message = f"{message} ({exc_type})"
            self.buffer.add(level=record.levelname, message=message)
        except Exception:
            self.handleError(record)


@dataclass
class LoggingRuntime:
    live_state: LiveLogState
    event_buffer: DashboardEventBuffer
    log_file_path: Path


class AsyncWindowLimiter:
    """Simple async sliding-window limiter."""

    def __init__(self, max_requests: int, period_seconds: float, name: str):
        self.max_requests = max_requests
        self.period_seconds = period_seconds
        self.name = name
        self._events: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                while self._events and now - self._events[0] >= self.period_seconds:
                    self._events.popleft()

                if len(self._events) < self.max_requests:
                    self._events.append(now)
                    return

                wait_for = self.period_seconds - (now - self._events[0])
                wait_for = max(0.001, wait_for)

            await asyncio.sleep(wait_for)


class LocalDatabase:
    def __init__(self, path: Path):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        with self.conn:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")

            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS titles (
                    tmdb_id INTEGER NOT NULL,
                    media_type TEXT NOT NULL,
                    title TEXT,
                    imdb_id TEXT,
                    popularity REAL,
                    tmdb_vote_average REAL,
                    last_seen_at INTEGER NOT NULL,
                    last_harvested_at INTEGER,
                    last_mdblist_fetch_at INTEGER,
                    last_error TEXT,
                    PRIMARY KEY (tmdb_id, media_type)
                )
                """
            )

            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ratings (
                    tmdb_id INTEGER NOT NULL,
                    media_type TEXT NOT NULL,
                    label TEXT NOT NULL,
                    score REAL NOT NULL,
                    fetched_at INTEGER NOT NULL,
                    pmdb_item_id TEXT,
                    pmdb_status TEXT NOT NULL DEFAULT 'pending',
                    pmdb_claimed_at INTEGER,
                    pmdb_submitted_at INTEGER,
                    pmdb_attempts INTEGER NOT NULL DEFAULT 0,
                    pmdb_last_error TEXT,
                    pmdb_retry_after INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (tmdb_id, media_type, label)
                )
                """
            )

            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS mappings (
                    tmdb_id INTEGER NOT NULL,
                    media_type TEXT NOT NULL,
                    id_type TEXT NOT NULL,
                    id_value TEXT NOT NULL,
                    fetched_at INTEGER NOT NULL,
                    pmdb_item_id TEXT,
                    pmdb_status TEXT NOT NULL DEFAULT 'pending',
                    pmdb_claimed_at INTEGER,
                    pmdb_submitted_at INTEGER,
                    pmdb_attempts INTEGER NOT NULL DEFAULT 0,
                    pmdb_last_error TEXT,
                    pmdb_retry_after INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (tmdb_id, media_type, id_type)
                )
                """
            )

            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS service_state (
                    service TEXT PRIMARY KEY,
                    paused_until INTEGER DEFAULT 0,
                    pause_reason TEXT,
                    rate_limit INTEGER,
                    rate_remaining INTEGER,
                    rate_reset INTEGER,
                    last_status INTEGER,
                    updated_at INTEGER
                )
                """
            )

            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS submitted_titles (
                    tmdb_id INTEGER NOT NULL,
                    media_type TEXT NOT NULL,
                    first_submitted_at INTEGER NOT NULL,
                    last_submitted_at INTEGER NOT NULL,
                    PRIMARY KEY (tmdb_id, media_type)
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS submitted_title_days (
                    day_key TEXT NOT NULL,
                    tmdb_id INTEGER NOT NULL,
                    media_type TEXT NOT NULL,
                    PRIMARY KEY (day_key, tmdb_id, media_type)
                )
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_submitted_title_days_day
                ON submitted_title_days (day_key)
                """
            )

            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ratings_pending
                ON ratings (pmdb_status, pmdb_retry_after)
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_mappings_pending
                ON mappings (pmdb_status, pmdb_retry_after)
                """
            )

            # Backward-compatible migrations for existing local DB files.
            self._ensure_column("ratings", "pmdb_item_id", "TEXT")
            self._ensure_column("mappings", "pmdb_item_id", "TEXT")
            self._ensure_column("ratings", "pmdb_claimed_at", "INTEGER")
            self._ensure_column("mappings", "pmdb_claimed_at", "INTEGER")

    def _ensure_column(self, table_name: str, column_name: str, column_type: str) -> None:
        columns = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        existing = {str(row["name"]) for row in columns}
        if column_name in existing:
            return
        self.conn.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
        )

    def get_state_int(self, key: str, default: int) -> int:
        row = self.conn.execute(
            "SELECT value FROM state WHERE key = ?", (key,)
        ).fetchone()
        if not row:
            return default
        parsed = parse_int(row["value"])
        return default if parsed is None else parsed

    def get_state(self, key: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT value FROM state WHERE key = ?",
            (key,),
        ).fetchone()
        if row is None:
            return None
        return str(row["value"])

    def set_state(self, key: str, value: Any) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO state(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                (key, str(value)),
            )

    def delete_state(self, key: str) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM state WHERE key = ?", (key,))

    def get_service_state(self, service: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM service_state WHERE service = ?", (service,)
        ).fetchone()

    def update_service_state(
        self,
        service: str,
        paused_until: Optional[int] = None,
        pause_reason: Optional[str] = None,
        rate_limit: Optional[int] = None,
        rate_remaining: Optional[int] = None,
        rate_reset: Optional[int] = None,
        last_status: Optional[int] = None,
    ) -> None:
        now_ts = now_epoch()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO service_state(
                    service,
                    paused_until,
                    pause_reason,
                    rate_limit,
                    rate_remaining,
                    rate_reset,
                    last_status,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(service) DO UPDATE SET
                    paused_until=COALESCE(excluded.paused_until, service_state.paused_until),
                    pause_reason=COALESCE(excluded.pause_reason, service_state.pause_reason),
                    rate_limit=COALESCE(excluded.rate_limit, service_state.rate_limit),
                    rate_remaining=COALESCE(excluded.rate_remaining, service_state.rate_remaining),
                    rate_reset=COALESCE(excluded.rate_reset, service_state.rate_reset),
                    last_status=COALESCE(excluded.last_status, service_state.last_status),
                    updated_at=excluded.updated_at
                """,
                (
                    service,
                    paused_until,
                    pause_reason,
                    rate_limit,
                    rate_remaining,
                    rate_reset,
                    last_status,
                    now_ts,
                ),
            )

    def should_harvest(
        self,
        tmdb_id: int,
        media_type: str,
        now_ts: int,
        ratings_ttl_seconds: int,
        harvest_mode: str,
    ) -> bool:
        row = self.conn.execute(
            "SELECT * FROM titles WHERE tmdb_id = ? AND media_type = ?",
            (tmdb_id, media_type),
        ).fetchone()

        is_new = row is None or not row["last_harvested_at"]
        has_failed_enrichment = bool(
            row is not None
            and str(row["last_error"] or "").strip()
        )
        is_ttl_stale = bool(
            row is not None
            and row["last_harvested_at"]
            and now_ts - int(row["last_harvested_at"]) >= ratings_ttl_seconds
        )

        if harvest_mode == "new_only":
            return is_new
        if harvest_mode == "failed_only":
            return has_failed_enrichment
        if harvest_mode == "ttl_only":
            return is_ttl_stale

        # default: all => new + failed + ttl
        return is_new or has_failed_enrichment or is_ttl_stale

    def _upsert_title(
        self,
        tmdb_id: int,
        media_type: str,
        title: str,
        imdb_id: Optional[str],
        popularity: float,
        tmdb_vote_average: Optional[float],
        now_ts: int,
        error_message: Optional[str],
    ) -> None:
        existing = self.conn.execute(
            "SELECT * FROM titles WHERE tmdb_id = ? AND media_type = ?",
            (tmdb_id, media_type),
        ).fetchone()

        if existing:
            final_imdb = imdb_id or existing["imdb_id"]
            final_last_mdblist = now_ts

            self.conn.execute(
                """
                UPDATE titles
                SET title = ?,
                    imdb_id = ?,
                    popularity = ?,
                    tmdb_vote_average = ?,
                    last_seen_at = ?,
                    last_harvested_at = ?,
                    last_mdblist_fetch_at = ?,
                    last_error = ?
                WHERE tmdb_id = ? AND media_type = ?
                """,
                (
                    title,
                    final_imdb,
                    popularity,
                    tmdb_vote_average,
                    now_ts,
                    now_ts,
                    final_last_mdblist,
                    error_message,
                    tmdb_id,
                    media_type,
                ),
            )
            return

        self.conn.execute(
            """
            INSERT INTO titles(
                tmdb_id,
                media_type,
                title,
                imdb_id,
                popularity,
                tmdb_vote_average,
                last_seen_at,
                last_harvested_at,
                last_mdblist_fetch_at,
                last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tmdb_id,
                media_type,
                title,
                imdb_id,
                popularity,
                tmdb_vote_average,
                now_ts,
                now_ts,
                now_ts,
                error_message,
            ),
        )

    def _upsert_rating(
        self,
        tmdb_id: int,
        media_type: str,
        label: str,
        score: float,
        fetched_at: int,
    ) -> bool:
        existing = self.conn.execute(
            """
            SELECT score, pmdb_status, pmdb_item_id
            FROM ratings
            WHERE tmdb_id = ? AND media_type = ? AND label = ?
            """,
            (tmdb_id, media_type, label),
        ).fetchone()

        if not existing:
            self.conn.execute(
                """
                INSERT INTO ratings(
                    tmdb_id,
                    media_type,
                    label,
                    score,
                    fetched_at,
                    pmdb_item_id,
                    pmdb_status,
                    pmdb_claimed_at,
                    pmdb_submitted_at,
                    pmdb_attempts,
                    pmdb_last_error,
                    pmdb_retry_after
                ) VALUES (?, ?, ?, ?, ?, NULL, 'pending', NULL, NULL, 0, NULL, 0)
                """,
                (tmdb_id, media_type, label, score, fetched_at),
            )
            return True

        score_changed = abs(float(existing["score"]) - score) > 0.01
        previous_status = str(existing["pmdb_status"])

        if score_changed or previous_status in {"failed", "retry"}:
            self.conn.execute(
                """
                UPDATE ratings
                SET score = ?,
                    fetched_at = ?,
                    pmdb_status = 'pending',
                    pmdb_claimed_at = NULL,
                    pmdb_submitted_at = NULL,
                    pmdb_attempts = 0,
                    pmdb_last_error = NULL,
                    pmdb_retry_after = 0
                WHERE tmdb_id = ? AND media_type = ? AND label = ?
                """,
                (score, fetched_at, tmdb_id, media_type, label),
            )
            return True

        self.conn.execute(
            """
            UPDATE ratings
            SET fetched_at = ?
            WHERE tmdb_id = ? AND media_type = ? AND label = ?
            """,
            (fetched_at, tmdb_id, media_type, label),
        )
        return False

    def _upsert_mapping(
        self,
        tmdb_id: int,
        media_type: str,
        id_type: str,
        id_value: str,
        fetched_at: int,
    ) -> bool:
        existing = self.conn.execute(
            """
            SELECT id_value, pmdb_status, pmdb_item_id
            FROM mappings
            WHERE tmdb_id = ? AND media_type = ? AND id_type = ?
            """,
            (tmdb_id, media_type, id_type),
        ).fetchone()

        if not existing:
            self.conn.execute(
                """
                INSERT INTO mappings(
                    tmdb_id,
                    media_type,
                    id_type,
                    id_value,
                    fetched_at,
                    pmdb_item_id,
                    pmdb_status,
                    pmdb_claimed_at,
                    pmdb_submitted_at,
                    pmdb_attempts,
                    pmdb_last_error,
                    pmdb_retry_after
                ) VALUES (?, ?, ?, ?, ?, NULL, 'pending', NULL, NULL, 0, NULL, 0)
                """,
                (tmdb_id, media_type, id_type, id_value, fetched_at),
            )
            return True

        value_changed = str(existing["id_value"]) != id_value
        previous_status = str(existing["pmdb_status"])

        if value_changed or previous_status in {"failed", "retry"}:
            self.conn.execute(
                """
                UPDATE mappings
                SET id_value = ?,
                    fetched_at = ?,
                    pmdb_status = 'pending',
                    pmdb_claimed_at = NULL,
                    pmdb_submitted_at = NULL,
                    pmdb_attempts = 0,
                    pmdb_last_error = NULL,
                    pmdb_retry_after = 0
                WHERE tmdb_id = ? AND media_type = ? AND id_type = ?
                """,
                (id_value, fetched_at, tmdb_id, media_type, id_type),
            )
            return True

        self.conn.execute(
            """
            UPDATE mappings
            SET fetched_at = ?
            WHERE tmdb_id = ? AND media_type = ? AND id_type = ?
            """,
            (fetched_at, tmdb_id, media_type, id_type),
        )
        return False

    def save_enriched_item(
        self,
        *,
        tmdb_id: int,
        media_type: str,
        title: str,
        imdb_id: Optional[str],
        popularity: float,
        tmdb_vote_average: Optional[float],
        enrichment_error: Optional[str],
        ratings: Dict[str, float],
        mappings: Dict[str, str],
        now_ts: int,
    ) -> Tuple[int, int]:
        queued_ratings = 0
        queued_mappings = 0
        with self.conn:
            self._upsert_title(
                tmdb_id=tmdb_id,
                media_type=media_type,
                title=title,
                imdb_id=imdb_id,
                popularity=popularity,
                tmdb_vote_average=tmdb_vote_average,
                now_ts=now_ts,
                error_message=enrichment_error,
            )

            for label, score in ratings.items():
                safe_score = clamp_0_100(score)
                if safe_score is None:
                    continue
                queued = self._upsert_rating(
                    tmdb_id=tmdb_id,
                    media_type=media_type,
                    label=label.upper(),
                    score=safe_score,
                    fetched_at=now_ts,
                )
                if queued:
                    queued_ratings += 1

            for id_type, id_value in mappings.items():
                if not id_value:
                    continue
                normalized_type = id_type.lower()
                if normalized_type not in SUPPORTED_PMDB_MAPPING_TYPES:
                    continue
                queued = self._upsert_mapping(
                    tmdb_id=tmdb_id,
                    media_type=media_type,
                    id_type=normalized_type,
                    id_value=str(id_value),
                    fetched_at=now_ts,
                )
                if queued:
                    queued_mappings += 1
        return queued_ratings, queued_mappings

    def recover_in_flight_rows(self) -> None:
        """Re-queue rows that were claimed before an unclean shutdown."""
        with self.conn:
            self.conn.execute(
                """
                UPDATE ratings
                SET pmdb_status = 'retry',
                    pmdb_retry_after = 0,
                    pmdb_claimed_at = NULL,
                    pmdb_last_error = COALESCE(pmdb_last_error, 'Recovered from in_flight')
                WHERE pmdb_status = 'in_flight'
                """
            )
            self.conn.execute(
                """
                UPDATE mappings
                SET pmdb_status = 'retry',
                    pmdb_retry_after = 0,
                    pmdb_claimed_at = NULL,
                    pmdb_last_error = COALESCE(pmdb_last_error, 'Recovered from in_flight')
                WHERE pmdb_status = 'in_flight'
                """
            )

    def recover_stale_in_flight_rows(self, lease_seconds: int) -> Dict[str, int]:
        lease = max(30, int(lease_seconds))
        cutoff = now_epoch() - lease
        ratings_count_row = self.conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM ratings
            WHERE pmdb_status = 'in_flight'
              AND COALESCE(pmdb_claimed_at, 0) <= ?
            """,
            (cutoff,),
        ).fetchone()
        mappings_count_row = self.conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM mappings
            WHERE pmdb_status = 'in_flight'
              AND COALESCE(pmdb_claimed_at, 0) <= ?
            """,
            (cutoff,),
        ).fetchone()
        with self.conn:
            self.conn.execute(
                """
                UPDATE ratings
                SET pmdb_status = 'retry',
                    pmdb_retry_after = 0,
                    pmdb_claimed_at = NULL,
                    pmdb_last_error = COALESCE(pmdb_last_error, 'Recovered stale in_flight lease')
                WHERE pmdb_status = 'in_flight'
                  AND COALESCE(pmdb_claimed_at, 0) <= ?
                """,
                (cutoff,),
            )
            self.conn.execute(
                """
                UPDATE mappings
                SET pmdb_status = 'retry',
                    pmdb_retry_after = 0,
                    pmdb_claimed_at = NULL,
                    pmdb_last_error = COALESCE(pmdb_last_error, 'Recovered stale in_flight lease')
                WHERE pmdb_status = 'in_flight'
                  AND COALESCE(pmdb_claimed_at, 0) <= ?
                """,
                (cutoff,),
            )
        return {
            "ratings": int(ratings_count_row["c"] if ratings_count_row else 0),
            "mappings": int(mappings_count_row["c"] if mappings_count_row else 0),
        }

    def cleanup_retry_storm_rows(self, max_attempts: int) -> Dict[str, int]:
        threshold = max(1, int(max_attempts))
        ratings_error = (
            '{"service":"pmdb","endpoint":"/api/external/ratings","status":500,'
            '"code":"auto_failed_max_attempts","retryable":false,'
            '"message":"Auto-failed retry storm row after max retry attempts '
            '(likely permanent create conflict)."}'
        )
        mappings_error = (
            '{"service":"pmdb","endpoint":"/api/external/mappings","status":500,'
            '"code":"auto_failed_max_attempts","retryable":false,'
            '"message":"Auto-failed retry storm row after max retry attempts '
            '(likely permanent create conflict)."}'
        )
        ratings_count_row = self.conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM ratings
            WHERE pmdb_status = 'retry'
              AND pmdb_attempts >= ?
              AND COALESCE(pmdb_last_error, '') LIKE '%"status":500%'
              AND COALESCE(pmdb_last_error, '') LIKE '%Failed to create rating%'
            """,
            (threshold,),
        ).fetchone()
        mappings_count_row = self.conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM mappings
            WHERE pmdb_status = 'retry'
              AND pmdb_attempts >= ?
              AND COALESCE(pmdb_last_error, '') LIKE '%"status":500%'
              AND COALESCE(pmdb_last_error, '') LIKE '%Failed to create ID mapping%'
            """,
            (threshold,),
        ).fetchone()
        with self.conn:
            self.conn.execute(
                """
                UPDATE ratings
                SET pmdb_status = 'failed',
                    pmdb_claimed_at = NULL,
                    pmdb_last_error = ?
                WHERE pmdb_status = 'retry'
                  AND pmdb_attempts >= ?
                  AND COALESCE(pmdb_last_error, '') LIKE '%"status":500%'
                  AND COALESCE(pmdb_last_error, '') LIKE '%Failed to create rating%'
                """,
                (ratings_error, threshold),
            )
            self.conn.execute(
                """
                UPDATE mappings
                SET pmdb_status = 'failed',
                    pmdb_claimed_at = NULL,
                    pmdb_last_error = ?
                WHERE pmdb_status = 'retry'
                  AND pmdb_attempts >= ?
                  AND COALESCE(pmdb_last_error, '') LIKE '%"status":500%'
                  AND COALESCE(pmdb_last_error, '') LIKE '%Failed to create ID mapping%'
                """,
                (mappings_error, threshold),
            )
        return {
            "ratings": int(ratings_count_row["c"] if ratings_count_row else 0),
            "mappings": int(mappings_count_row["c"] if mappings_count_row else 0),
        }

    def claim_next_pending_rating(self, now_ts: int) -> Optional[sqlite3.Row]:
        with self.conn:
            row = self.conn.execute(
                """
                SELECT tmdb_id, media_type, label, score, pmdb_item_id, pmdb_attempts
                FROM ratings
                WHERE pmdb_status IN ('pending', 'retry')
                  AND pmdb_retry_after <= ?
                ORDER BY fetched_at ASC
                LIMIT 1
                """,
                (now_ts,),
            ).fetchone()

            if row is None:
                return None

            self.conn.execute(
                """
                UPDATE ratings
                SET pmdb_status = 'in_flight',
                    pmdb_claimed_at = ?
                WHERE tmdb_id = ? AND media_type = ? AND label = ?
                """,
                (now_ts, row["tmdb_id"], row["media_type"], row["label"]),
            )
            return row

    def claim_next_pending_mapping(self, now_ts: int) -> Optional[sqlite3.Row]:
        with self.conn:
            row = self.conn.execute(
                """
                SELECT tmdb_id, media_type, id_type, id_value, pmdb_item_id, pmdb_attempts
                FROM mappings
                WHERE pmdb_status IN ('pending', 'retry')
                  AND pmdb_retry_after <= ?
                ORDER BY fetched_at ASC
                LIMIT 1
                """,
                (now_ts,),
            ).fetchone()

            if row is None:
                return None

            self.conn.execute(
                """
                UPDATE mappings
                SET pmdb_status = 'in_flight',
                    pmdb_claimed_at = ?
                WHERE tmdb_id = ? AND media_type = ? AND id_type = ?
                """,
                (now_ts, row["tmdb_id"], row["media_type"], row["id_type"]),
            )
            return row

    def mark_rating_submitted(
        self,
        tmdb_id: int,
        media_type: str,
        label: str,
        submitted_at: int,
        pmdb_item_id: Optional[str] = None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE ratings
                SET pmdb_status = 'submitted',
                    pmdb_item_id = COALESCE(?, pmdb_item_id),
                    pmdb_claimed_at = NULL,
                    pmdb_submitted_at = ?,
                    pmdb_last_error = NULL,
                    pmdb_retry_after = 0
                WHERE tmdb_id = ? AND media_type = ? AND label = ?
                """,
                (pmdb_item_id, submitted_at, tmdb_id, media_type, label),
            )
            self._record_submission_success(
                kind="ratings",
                tmdb_id=tmdb_id,
                media_type=media_type,
                submitted_at=submitted_at,
            )

    def mark_mapping_submitted(
        self,
        tmdb_id: int,
        media_type: str,
        id_type: str,
        submitted_at: int,
        pmdb_item_id: Optional[str] = None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE mappings
                SET pmdb_status = 'submitted',
                    pmdb_item_id = COALESCE(?, pmdb_item_id),
                    pmdb_claimed_at = NULL,
                    pmdb_submitted_at = ?,
                    pmdb_last_error = NULL,
                    pmdb_retry_after = 0
                WHERE tmdb_id = ? AND media_type = ? AND id_type = ?
                """,
                (pmdb_item_id, submitted_at, tmdb_id, media_type, id_type),
            )
            self._record_submission_success(
                kind="mappings",
                tmdb_id=tmdb_id,
                media_type=media_type,
                submitted_at=submitted_at,
            )

    def mark_rating_retry(
        self,
        tmdb_id: int,
        media_type: str,
        label: str,
        retry_after: int,
        error_text: str,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE ratings
                SET pmdb_status = 'retry',
                    pmdb_attempts = pmdb_attempts + 1,
                    pmdb_last_error = ?,
                    pmdb_claimed_at = NULL,
                    pmdb_retry_after = ?
                WHERE tmdb_id = ? AND media_type = ? AND label = ?
                """,
                (error_text, retry_after, tmdb_id, media_type, label),
            )

    def mark_mapping_retry(
        self,
        tmdb_id: int,
        media_type: str,
        id_type: str,
        retry_after: int,
        error_text: str,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE mappings
                SET pmdb_status = 'retry',
                    pmdb_attempts = pmdb_attempts + 1,
                    pmdb_last_error = ?,
                    pmdb_claimed_at = NULL,
                    pmdb_retry_after = ?
                WHERE tmdb_id = ? AND media_type = ? AND id_type = ?
                """,
                (error_text, retry_after, tmdb_id, media_type, id_type),
            )

    def mark_rating_failed(
        self,
        tmdb_id: int,
        media_type: str,
        label: str,
        error_text: str,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE ratings
                SET pmdb_status = 'failed',
                    pmdb_attempts = pmdb_attempts + 1,
                    pmdb_last_error = ?,
                    pmdb_claimed_at = NULL
                WHERE tmdb_id = ? AND media_type = ? AND label = ?
                """,
                (error_text, tmdb_id, media_type, label),
            )

    def mark_mapping_failed(
        self,
        tmdb_id: int,
        media_type: str,
        id_type: str,
        error_text: str,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE mappings
                SET pmdb_status = 'failed',
                    pmdb_attempts = pmdb_attempts + 1,
                    pmdb_last_error = ?,
                    pmdb_claimed_at = NULL
                WHERE tmdb_id = ? AND media_type = ? AND id_type = ?
                """,
                (error_text, tmdb_id, media_type, id_type),
            )

    def queue_counts(self) -> Dict[str, int]:
        row = self.conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM ratings WHERE pmdb_status IN ('pending', 'retry')) AS ratings_pending,
                (SELECT COUNT(*) FROM mappings WHERE pmdb_status IN ('pending', 'retry')) AS mappings_pending,
                (SELECT COUNT(*) FROM ratings WHERE pmdb_status = 'in_flight') AS ratings_in_flight,
                (SELECT COUNT(*) FROM mappings WHERE pmdb_status = 'in_flight') AS mappings_in_flight,
                (SELECT COUNT(*) FROM ratings WHERE pmdb_status = 'failed') AS ratings_failed,
                (SELECT COUNT(*) FROM mappings WHERE pmdb_status = 'failed') AS mappings_failed
            """
        ).fetchone()
        return {
            "ratings_pending": int(row["ratings_pending"]),
            "mappings_pending": int(row["mappings_pending"]),
            "ratings_in_flight": int(row["ratings_in_flight"]),
            "mappings_in_flight": int(row["mappings_in_flight"]),
            "ratings_failed": int(row["ratings_failed"]),
            "mappings_failed": int(row["mappings_failed"]),
        }

    def count_due_queue(self, *, kind: str, now_ts: int) -> int:
        table = "ratings" if str(kind).strip().lower() == "ratings" else "mappings"
        row = self.conn.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM {table}
            WHERE pmdb_status IN ('pending', 'retry')
              AND pmdb_retry_after <= ?
            """,
            (int(now_ts),),
        ).fetchone()
        return int((row["c"] if row else 0) or 0)

    def _record_submission_success(
        self,
        *,
        kind: str,
        tmdb_id: int,
        media_type: str,
        submitted_at: int,
    ) -> None:
        kind_name = "ratings" if str(kind).strip().lower() == "ratings" else "mappings"
        day = local_day_key(submitted_at)
        total_key = f"metrics:pmdb_submitted:{kind_name}:total"
        day_key = f"metrics:pmdb_submitted:{kind_name}:day:{day}"

        self.conn.execute(
            """
            INSERT INTO state(key, value) VALUES(?, '1')
            ON CONFLICT(key) DO UPDATE SET value = CAST(state.value AS INTEGER) + 1
            """,
            (total_key,),
        )
        self.conn.execute(
            """
            INSERT INTO state(key, value) VALUES(?, '1')
            ON CONFLICT(key) DO UPDATE SET value = CAST(state.value AS INTEGER) + 1
            """,
            (day_key,),
        )

        self.conn.execute(
            """
            INSERT INTO submitted_titles(
                tmdb_id, media_type, first_submitted_at, last_submitted_at
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(tmdb_id, media_type) DO UPDATE SET
                last_submitted_at=excluded.last_submitted_at
            """,
            (tmdb_id, media_type, submitted_at, submitted_at),
        )
        self.conn.execute(
            """
            INSERT OR IGNORE INTO submitted_title_days(day_key, tmdb_id, media_type)
            VALUES (?, ?, ?)
            """,
            (day, tmdb_id, media_type),
        )

    def dashboard_snapshot(self, day_key: str) -> Dict[str, int]:
        row = self.conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM titles) AS titles_total,
                (SELECT COUNT(*) FROM ratings) AS ratings_total,
                (SELECT COUNT(*) FROM mappings) AS mappings_total,
                (SELECT COUNT(*) FROM submitted_titles) AS titles_submitted_total,
                (SELECT COUNT(*) FROM submitted_title_days WHERE day_key = ?) AS titles_submitted_today,
                (SELECT CAST(value AS INTEGER) FROM state WHERE key = ?) AS ratings_submitted_total,
                (SELECT CAST(value AS INTEGER) FROM state WHERE key = ?) AS mappings_submitted_total,
                (SELECT CAST(value AS INTEGER) FROM state WHERE key = ?) AS ratings_submitted_today,
                (SELECT CAST(value AS INTEGER) FROM state WHERE key = ?) AS mappings_submitted_today,
                (SELECT MAX(pmdb_submitted_at) FROM ratings) AS ratings_last_submit_at,
                (SELECT MAX(pmdb_submitted_at) FROM mappings) AS mappings_last_submit_at,
                (SELECT MAX(last_harvested_at) FROM titles) AS titles_last_harvested_at,
                (SELECT MAX(last_mdblist_fetch_at) FROM titles) AS titles_last_mdblist_fetch_at
            """,
            (
                day_key,
                "metrics:pmdb_submitted:ratings:total",
                "metrics:pmdb_submitted:mappings:total",
                f"metrics:pmdb_submitted:ratings:day:{day_key}",
                f"metrics:pmdb_submitted:mappings:day:{day_key}",
            ),
        ).fetchone()
        return {
            "titles_total": int(row["titles_total"] or 0),
            "ratings_total": int(row["ratings_total"] or 0),
            "mappings_total": int(row["mappings_total"] or 0),
            "titles_submitted_total": int(row["titles_submitted_total"] or 0),
            "titles_submitted_today": int(row["titles_submitted_today"] or 0),
            "ratings_submitted_total": int(row["ratings_submitted_total"] or 0),
            "mappings_submitted_total": int(row["mappings_submitted_total"] or 0),
            "ratings_submitted_today": int(row["ratings_submitted_today"] or 0),
            "mappings_submitted_today": int(row["mappings_submitted_today"] or 0),
            "ratings_last_submit_at": int(row["ratings_last_submit_at"] or 0),
            "mappings_last_submit_at": int(row["mappings_last_submit_at"] or 0),
            "titles_last_harvested_at": int(row["titles_last_harvested_at"] or 0),
            "titles_last_mdblist_fetch_at": int(row["titles_last_mdblist_fetch_at"] or 0),
        }


class ServiceGate:
    """Combines local window throttling and persisted pause state."""

    def __init__(
        self,
        name: str,
        db: LocalDatabase,
        limiter: AsyncWindowLimiter,
    ):
        self.name = name
        self.db = db
        self.limiter = limiter

        self.paused_until = 0
        self.pause_reason = ""
        self.rate_limit = None
        self.rate_remaining = None
        self.rate_reset = None
        self.last_status = None
        self._last_pause_log = 0

        existing = self.db.get_service_state(self.name)
        if existing:
            self.paused_until = int(existing["paused_until"] or 0)
            self.pause_reason = str(existing["pause_reason"] or "")
            self.rate_limit = existing["rate_limit"]
            self.rate_remaining = existing["rate_remaining"]
            self.rate_reset = existing["rate_reset"]
            self.last_status = existing["last_status"]

    def pause_remaining(self) -> int:
        return max(0, int(self.paused_until - now_epoch()))

    def pause_for(self, seconds: int, reason: str) -> None:
        until = now_epoch() + max(1, int(seconds))
        self.paused_until = max(self.paused_until, until)
        self.pause_reason = reason
        self.db.update_service_state(
            service=self.name,
            paused_until=self.paused_until,
            pause_reason=self.pause_reason,
        )

    def pause_until_timestamp(self, epoch_seconds: int, reason: str) -> None:
        if epoch_seconds <= now_epoch():
            return
        self.paused_until = max(self.paused_until, epoch_seconds)
        self.pause_reason = reason
        self.db.update_service_state(
            service=self.name,
            paused_until=self.paused_until,
            pause_reason=self.pause_reason,
        )

    def observe_headers(self, headers: Dict[str, str], status: int) -> None:
        limit = parse_int(headers.get("x-ratelimit-limit"))
        remaining = parse_int(headers.get("x-ratelimit-remaining"))
        reset = parse_int(headers.get("x-ratelimit-reset"))

        if (
            limit is not None
            or remaining is not None
            or reset is not None
            or status != self.last_status
        ):
            self.rate_limit = limit if limit is not None else self.rate_limit
            self.rate_remaining = (
                remaining if remaining is not None else self.rate_remaining
            )
            self.rate_reset = reset if reset is not None else self.rate_reset
            self.last_status = status

            self.db.update_service_state(
                service=self.name,
                rate_limit=self.rate_limit,
                rate_remaining=self.rate_remaining,
                rate_reset=self.rate_reset,
                last_status=self.last_status,
            )

        if remaining == 0:
            reset_ts = reset or (now_epoch() + 300)
            self.pause_until_timestamp(
                reset_ts,
                "Daily Limit Reached",
            )
            LOGGER.warning(
                "[%s] Daily Limit Reached. Paused until %s",
                self.name,
                to_iso(self.paused_until),
            )

    async def acquire(self, max_pause_wait_seconds: Optional[int] = None) -> bool:
        while True:
            remaining = self.pause_remaining()
            if remaining <= 0:
                break

            if max_pause_wait_seconds is not None and remaining > max_pause_wait_seconds:
                return False

            now_ts = now_epoch()
            if now_ts - self._last_pause_log >= 30:
                reason = self.pause_reason or "Rate limit pause"
                LOGGER.warning(
                    "[%s] Paused for %ss (%s)",
                    self.name,
                    remaining,
                    reason,
                )
                self._last_pause_log = now_ts
            await asyncio.sleep(min(5, remaining))

        await self.limiter.acquire()
        return True


class HTTPClient:
    def __init__(self, timeout_seconds: int, max_retries: int):
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self._network_error_throttle_seconds = 20
        self._last_network_error_log_at: Dict[str, int] = {}

    def _should_log_network_error(self, key: str) -> bool:
        now_ts = now_epoch()
        last = self._last_network_error_log_at.get(key, 0)
        if now_ts - last >= self._network_error_throttle_seconds:
            self._last_network_error_log_at[key] = now_ts
            return True
        return False

    async def request_json(
        self,
        *,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        gate: Optional[ServiceGate] = None,
        max_pause_wait_seconds: Optional[int] = None,
    ) -> APIResponse:
        last_response: Optional[APIResponse] = None
        safe_url = sanitize_url_for_logs(url)

        for attempt in range(self.max_retries):
            if gate is not None:
                acquired = await gate.acquire(
                    max_pause_wait_seconds=max_pause_wait_seconds
                )
                if not acquired:
                    return APIResponse(
                        status=429,
                        headers={},
                        data={"error": "service paused"},
                        text="service paused",
                    )

            try:
                raw_resp = await asyncio.to_thread(
                    requests.request,
                    method,
                    url,
                    params=params,
                    headers=headers,
                    json=json_body,
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as exc:
                sleep_for = min(60, (2**attempt) + random.random())
                if is_network_unavailable_error(exc):
                    # During DNS/network outages, pause the gate briefly so we don't
                    # keep hammering and spamming logs across many concurrent workers.
                    pause_for = int(max(5, min(60, sleep_for)))
                    if gate is not None:
                        gate.pause_for(pause_for, "Network unavailable")
                    gate_name = gate.name if gate is not None else "http"
                    throttle_key = (
                        f"{gate_name}:{method}:{safe_url}:{exc.__class__.__name__}"
                    )
                    if self._should_log_network_error(throttle_key):
                        LOGGER.warning(
                            "Network unavailable for %s %s (attempt %s/%s). "
                            "Backing off %.1fs: %s",
                            method,
                            safe_url,
                            attempt + 1,
                            self.max_retries,
                            sleep_for,
                            exc,
                        )
                else:
                    LOGGER.warning(
                        "HTTP error calling %s %s (attempt %s/%s): %s",
                        method,
                        safe_url,
                        attempt + 1,
                        self.max_retries,
                        exc,
                    )
                if attempt == self.max_retries - 1:
                    return APIResponse(
                        status=0,
                        headers={},
                        data={"error": str(exc)},
                        text=str(exc),
                    )
                await asyncio.sleep(sleep_for)
                continue

            normalized_headers = {
                str(k).lower(): str(v) for k, v in raw_resp.headers.items()
            }
            data: Any = None
            text = raw_resp.text or ""
            if text:
                try:
                    data = raw_resp.json()
                except ValueError:
                    data = None

            response = APIResponse(
                status=raw_resp.status_code,
                headers=normalized_headers,
                data=data,
                text=text,
            )
            last_response = response

            if gate is not None:
                gate.observe_headers(response.headers, response.status)

            if response.status == 429:
                retry_after = parse_retry_after(response.headers.get("retry-after"), 5)
                if gate is not None:
                    gate.pause_for(retry_after, "429 Too Many Requests")
                LOGGER.warning(
                    "429 from %s %s. Retry-After=%ss",
                    method,
                    safe_url,
                    retry_after,
                )
                if attempt == self.max_retries - 1:
                    return response
                await asyncio.sleep(retry_after)
                continue

            if response.status in (500, 502, 503, 504):
                sleep_for = min(60, (2**attempt) + random.random())
                LOGGER.warning(
                    "%s from %s %s. Retrying in %.1fs (attempt %s/%s)",
                    response.status,
                    method,
                    safe_url,
                    sleep_for,
                    attempt + 1,
                    self.max_retries,
                )
                if attempt == self.max_retries - 1:
                    return response
                await asyncio.sleep(sleep_for)
                continue

            if response.status in (401, 403):
                throttle_key = f"auth:{method}:{safe_url}:{response.status}"
                if self._should_log_network_error(throttle_key):
                    compact = " ".join(text.split())[:180]
                    suffix = f" body={compact}" if compact else ""
                    LOGGER.warning(
                        "%s from %s %s%s",
                        response.status,
                        method,
                        safe_url,
                        suffix,
                    )

            return response

        if last_response is not None:
            return last_response

        return APIResponse(status=0, headers={}, data=None, text="unknown error")


def parse_value_and_scale(value: Any) -> Tuple[Optional[float], Optional[float]]:
    if value is None:
        return None, None

    if isinstance(value, (int, float)):
        numeric = float(value)
        if math.isfinite(numeric):
            return numeric, None
        return None, None

    text = str(value).strip()
    if not text:
        return None, None

    lowered = text.lower()
    if lowered in {"n/a", "none", "null", "nan"}:
        return None, None

    text = text.replace("%", "").strip()

    if "/" in text:
        left, right = text.split("/", 1)
        try:
            num = float(left.strip())
            den = float(right.strip())
            if not (math.isfinite(num) and math.isfinite(den)):
                return None, None
            return num, den
        except ValueError:
            return None, None

    try:
        num = float(text)
        if not math.isfinite(num):
            return None, None
        return num, None
    except ValueError:
        return None, None


def scale_to_100(
    numeric: Optional[float],
    denominator: Optional[float] = None,
    default_scale_hint: Optional[int] = None,
) -> Optional[float]:
    if numeric is None:
        return None

    if denominator is not None and denominator > 0:
        return clamp_0_100((numeric / denominator) * 100.0)

    if default_scale_hint == 5:
        if numeric <= 5:
            return clamp_0_100(numeric * 20.0)
        if numeric <= 10:
            return clamp_0_100(numeric * 10.0)
        return clamp_0_100(numeric)
    if default_scale_hint == 10:
        if numeric <= 10:
            return clamp_0_100(numeric * 10.0)
        return clamp_0_100(numeric)

    if numeric <= 5:
        return clamp_0_100(numeric * 20.0)
    if numeric <= 10:
        return clamp_0_100(numeric * 10.0)

    return clamp_0_100(numeric)


def parse_mdblist_ratings(mdblist_data: Optional[Dict[str, Any]]) -> Dict[str, float]:
    """Parse ratings into PMDB labels on a 0-100 scale.

    Priority order:
    1. Top-level Metascore and imdbRating.
    2. ratings[] providers with source-specific rules:
       - RT critics vs audience split
       - Metacritic critic-score guard (score > 10 in raw scale)
       - Letterboxd and Trakt scale normalization
       - IMDb as fallback only
    """

    if not mdblist_data:
        return {}

    ratings: Dict[str, float] = {}

    metascore_raw = mdblist_data.get("Metascore")
    if metascore_raw is None:
        metascore_raw = mdblist_data.get("metascore")

    metascore_num, metascore_den = parse_value_and_scale(metascore_raw)
    if metascore_num is not None:
        if metascore_den is None:
            mc_score = clamp_0_100(metascore_num)
        else:
            mc_score = scale_to_100(metascore_num, metascore_den)
        if mc_score is not None:
            ratings["MC"] = mc_score

    imdb_top_raw = mdblist_data.get("imdbRating")
    if imdb_top_raw is None:
        imdb_top_raw = mdblist_data.get("imdbrating")

    imdb_top_num, imdb_top_den = parse_value_and_scale(imdb_top_raw)
    imdb_top_score = scale_to_100(imdb_top_num, imdb_top_den, default_scale_hint=10)
    if imdb_top_score is not None:
        ratings["IM"] = imdb_top_score

    raw_ratings = mdblist_data.get("ratings")
    if not isinstance(raw_ratings, list):
        raw_ratings = []

    for entry in raw_ratings:
        if not isinstance(entry, dict):
            continue

        source = str(entry.get("source", "")).strip().lower()
        if not source:
            continue

        # MDBList often returns both:
        # - value: provider-native scale (can be inconsistent by source)
        # - score: normalized 0-100
        # Prefer normalized score when present to avoid scale ambiguity.
        score_num, score_den = parse_value_and_scale(entry.get("score"))
        value_num, value_den = parse_value_and_scale(entry.get("value"))
        if score_num is not None:
            numeric, denominator = score_num, score_den
        else:
            numeric, denominator = value_num, value_den

        if numeric is None:
            continue

        # IMDb fallback only.
        if source in {"imdb", "internet movie database"}:
            if "IM" not in ratings:
                im_score = scale_to_100(numeric, denominator, default_scale_hint=10)
                if im_score is not None:
                    ratings["IM"] = im_score
            continue

        # Rotten Tomatoes critics.
        if source in {"rotten tomatoes", "tomatoes", "tomatometer"}:
            if "audience" not in source:
                rt_score = scale_to_100(numeric, denominator)
                if rt_score is not None:
                    ratings["RT"] = rt_score
            continue

        # Rotten Tomatoes audience/popcornmeter.
        if "audience" in source or "popcorn" in source:
            pc_score = scale_to_100(numeric, denominator)
            if pc_score is not None:
                ratings["PC"] = pc_score
            continue

        # Metacritic critic score only.
        if source.startswith("metacritic"):
            if "user" in source:
                continue
            # Guard: avoid user-style 0-10 values.
            if denominator == 10 or (denominator is None and numeric <= 10):
                continue
            if "MC" not in ratings:
                mc_score = scale_to_100(numeric, denominator)
                if mc_score is not None:
                    ratings["MC"] = mc_score
            continue

        if "letterboxd" in source:
            lb_hint = 5 if denominator == 5 or (denominator is None and numeric <= 5) else 10
            lb_score = scale_to_100(numeric, denominator, default_scale_hint=lb_hint)
            if lb_score is not None:
                ratings["LB"] = lb_score
            continue

        if "trakt" in source:
            tr_score = scale_to_100(numeric, denominator, default_scale_hint=10)
            if tr_score is not None:
                ratings["TR"] = tr_score
            continue

    if "TR" not in ratings:
        trakt_fallback = mdblist_data.get("score")
        tr_num, tr_den = parse_value_and_scale(trakt_fallback)
        tr_score = scale_to_100(tr_num, tr_den)
        if tr_score is not None:
            ratings["TR"] = tr_score

    return ratings


def parse_tmdb_vote_average(tmdb_details: Optional[Dict[str, Any]]) -> Optional[float]:
    if not tmdb_details:
        return None
    vote = tmdb_details.get("vote_average")
    if vote is None:
        return None
    try:
        return clamp_0_100(float(vote) * 10.0)
    except (TypeError, ValueError):
        return None


def first_non_empty(*values: Any) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        value_str = str(value).strip()
        if value_str and value_str.lower() not in {"none", "null", "n/a"}:
            return value_str
    return None


def extract_mappings(
    media_type: str,
    tmdb_details: Optional[Dict[str, Any]],
    mdblist_item: Optional[Dict[str, Any]],
) -> Dict[str, str]:
    mappings: Dict[str, str] = {}

    tmdb_external = {}
    if tmdb_details and isinstance(tmdb_details, dict):
        external_ids = tmdb_details.get("external_ids")
        if isinstance(external_ids, dict):
            tmdb_external = external_ids

    md_ids = {}
    if mdblist_item and isinstance(mdblist_item, dict):
        ids_block = mdblist_item.get("ids")
        if isinstance(ids_block, dict):
            md_ids = ids_block

    imdb_id = first_non_empty(
        tmdb_external.get("imdb_id"),
        (tmdb_details or {}).get("imdb_id"),
        md_ids.get("imdb"),
        (mdblist_item or {}).get("imdb_id"),
        (mdblist_item or {}).get("imdbid"),
    )
    if imdb_id:
        mappings["imdb"] = imdb_id

    tvdb_id = first_non_empty(
        tmdb_external.get("tvdb_id"),
        md_ids.get("tvdb"),
        (mdblist_item or {}).get("tvdb_id"),
        (mdblist_item or {}).get("tvdbid"),
    )
    if tvdb_id:
        mappings["tvdb"] = tvdb_id

    trakt_id = first_non_empty(
        md_ids.get("trakt"),
        (mdblist_item or {}).get("trakt_id"),
        (mdblist_item or {}).get("traktid"),
    )
    if trakt_id:
        mappings["trakt"] = trakt_id

    mal_id = first_non_empty(
        md_ids.get("mal"),
        (mdblist_item or {}).get("mal_id"),
        (mdblist_item or {}).get("malid"),
        (mdblist_item or {}).get("myanimelist_id"),
    )
    if mal_id:
        mappings["mal"] = mal_id

    anilist_id = first_non_empty(
        md_ids.get("anilist"),
        (mdblist_item or {}).get("anilist_id"),
        (mdblist_item or {}).get("anilistid"),
    )
    if anilist_id:
        mappings["anilist"] = anilist_id

    anidb_id = first_non_empty(
        md_ids.get("anidb"),
        (mdblist_item or {}).get("anidb_id"),
        (mdblist_item or {}).get("anidbid"),
    )
    if anidb_id:
        mappings["anidb"] = anidb_id

    return mappings


class TMDBClient:
    def __init__(
        self,
        *,
        api_key: str,
        config: Dict[str, Any],
        gate: ServiceGate,
    ):
        self.api_key = api_key
        self.base_url = config["base_url"].rstrip("/")
        self.language = config["language"]
        self.http = HTTPClient(
            timeout_seconds=config["timeout_seconds"],
            max_retries=config["max_retries"],
        )
        self.gate = gate

    @staticmethod
    def build_source(entry: Dict[str, Any]) -> TMDBSource:
        name = str(entry.get("name", "")).strip().lower().lstrip("/")
        endpoint = f"/{name}"
        media_type_hint: Optional[str] = None
        if name.startswith("movie/") or name.startswith("trending/movie/"):
            media_type_hint = "movie"
        elif name.startswith("tv/") or name.startswith("trending/tv/"):
            media_type_hint = "tv"
        elif name.startswith("trending/all/"):
            media_type_hint = None
        else:
            raise ValueError(f"Unsupported TMDB source name: {name}")

        max_pages = max(1, min(500, int(entry.get("max_pages", 500))))
        return TMDBSource(
            name=name,
            endpoint=endpoint,
            media_type_hint=media_type_hint,
            max_pages=max_pages,
        )

    async def fetch_source_page(self, source: TMDBSource, page: int) -> APIResponse:
        url = f"{self.base_url}{source.endpoint}"
        return await self.http.request_json(
            method="GET",
            url=url,
            params={
                "api_key": self.api_key,
                "language": self.language,
                "page": page,
            },
            gate=self.gate,
        )

    async def fetch_details(self, media_type: str, tmdb_id: int) -> APIResponse:
        endpoint_media = "movie" if media_type == "movie" else "tv"
        url = f"{self.base_url}/{endpoint_media}/{tmdb_id}"
        return await self.http.request_json(
            method="GET",
            url=url,
            params={
                "api_key": self.api_key,
                "language": self.language,
                "append_to_response": "external_ids",
            },
            gate=self.gate,
        )


class MDBListClient:
    def __init__(
        self,
        *,
        api_key: str,
        config: Dict[str, Any],
        gate: ServiceGate,
    ):
        self.api_key = api_key
        self.base_url = config["base_url"].rstrip("/")
        self.batch_size = min(config["batch_size"], config["max_batch_size"], 200)
        self.max_pause_block_seconds = config["max_pause_block_seconds"]
        self.append_to_response = config.get("append_to_response") or []
        self.gate = gate
        self.http = HTTPClient(
            timeout_seconds=config["timeout_seconds"],
            max_retries=config["max_retries"],
        )

    def _md_media_type(self, media_type: str) -> str:
        return "movie" if media_type == "movie" else "show"

    async def _fetch_batch(
        self, media_type: str, tmdb_ids: Sequence[int]
    ) -> Tuple[Dict[int, Dict[str, Any]], Set[int], bool, bool, str]:
        """Returns: (results_by_tmdb_id, attempted_tmdb_ids, success, halt, halt_reason)

        success=False means request did not run to completion (e.g., pause or hard error)
        and caller should treat enrichment as incomplete for these ids.
        """
        if not tmdb_ids:
            return {}, set(), True, False, ""

        md_media_type = self._md_media_type(media_type)
        url = f"{self.base_url}/tmdb/{md_media_type}"

        body: Dict[str, Any] = {"ids": list(tmdb_ids)}
        if self.append_to_response:
            body["append_to_response"] = list(self.append_to_response)

        response = await self.http.request_json(
            method="POST",
            url=url,
            params={"apikey": self.api_key},
            json_body=body,
            gate=self.gate,
            max_pause_wait_seconds=self.max_pause_block_seconds,
        )

        if response.status == 429 and response.text == "service paused":
            LOGGER.warning(
                "[MDBList] Paused too long, skipping batch of %s %s items for now.",
                len(tmdb_ids),
                media_type,
            )
            return {}, set(), False, True, "service paused"

        if response.status == 401:
            LOGGER.error("[MDBList] Unauthorized (401). Check API key.")
            self.gate.pause_for(300, "Unauthorized API key")
            return {}, set(), False, True, "unauthorized"

        if response.status == 403:
            LOGGER.error("[MDBList] Forbidden (403).")
            self.gate.pause_for(300, "Forbidden")
            return {}, set(), False, True, "forbidden"

        if response.status == 429:
            # request_json already paused according to Retry-After.
            return {}, set(), False, True, "rate limited"

        if not response.ok:
            LOGGER.warning(
                "[MDBList] Batch request failed (%s): %s",
                response.status,
                response.text[:300],
            )
            return {}, set(), False, False, f"http {response.status}"

        payload = response.data
        items: List[Dict[str, Any]] = []
        if isinstance(payload, list):
            items = [x for x in payload if isinstance(x, dict)]
        elif isinstance(payload, dict):
            candidate = payload.get("items")
            if isinstance(candidate, list):
                items = [x for x in candidate if isinstance(x, dict)]

        by_id: Dict[int, Dict[str, Any]] = {}
        for item in items:
            ids_block = item.get("ids") if isinstance(item.get("ids"), dict) else {}
            tmdb_value = (
                ids_block.get("tmdb")
                or item.get("tmdb_id")
                or item.get("tmdbid")
                or item.get("id")
            )
            tmdb_int = parse_int(tmdb_value)
            if tmdb_int is None:
                continue
            by_id[tmdb_int] = item

        return by_id, set(int(x) for x in tmdb_ids), True, False, ""

    async def fetch_for_candidates(
        self,
        candidates: Sequence[Candidate],
        on_chunk: Optional[
            Callable[
                [str, Sequence[int], Dict[int, Dict[str, Any]], Set[int], int, int], None
            ]
        ] = None,
        stop_event: Optional[asyncio.Event] = None,
    ) -> Tuple[
        Dict[Tuple[str, int], Dict[str, Any]],
        Set[Tuple[str, int]],
        bool,
        bool,
        str,
        Dict[str, int],
    ]:
        grouped: Dict[str, List[int]] = defaultdict(list)
        for c in candidates:
            grouped[c.media_type].append(c.tmdb_id)

        results: Dict[Tuple[str, int], Dict[str, Any]] = {}
        attempted: Set[Tuple[str, int]] = set()
        all_success = True
        halted_early = False
        halt_reason = ""
        failed_batches: Dict[str, int] = {"movie": 0, "tv": 0}

        for media_type, tmdb_ids in grouped.items():
            if stop_event is not None and stop_event.is_set():
                halted_early = True
                halt_reason = "stop requested"
                break
            unique_ids = sorted(set(tmdb_ids))
            if not unique_ids:
                continue
            total_chunks = max(1, math.ceil(len(unique_ids) / self.batch_size))
            LOGGER.debug(
                "[MDBList] Fetching %s %s IDs in %s batch(es) (batch_size=%s).",
                len(unique_ids),
                media_type,
                total_chunks,
                self.batch_size,
            )

            chunk_index = 0
            for i in range(0, len(unique_ids), self.batch_size):
                if stop_event is not None and stop_event.is_set():
                    halted_early = True
                    halt_reason = "stop requested"
                    break
                chunk_index += 1
                chunk = unique_ids[i : i + self.batch_size]
                chunk_results, chunk_attempted, chunk_success, chunk_halt, chunk_reason = (
                    await self._fetch_batch(media_type, chunk)
                )
                if not chunk_success:
                    failed_batches[media_type] = failed_batches.get(media_type, 0) + 1
                    all_success = False
                    if chunk_halt:
                        halted_early = True
                        halt_reason = chunk_reason or "paused"

                if (
                    chunk_index == 1
                    or chunk_index == total_chunks
                    or chunk_index % 25 == 0
                ):
                    LOGGER.debug(
                        "[MDBList] %s batch %s/%s: requested=%s attempted=%s found=%s success=%s",
                        media_type,
                        chunk_index,
                        total_chunks,
                        len(chunk),
                        len(chunk_attempted),
                        len(chunk_results),
                        chunk_success,
                    )

                if not chunk_success and chunk_halt:
                    LOGGER.info(
                        "[MDBList] Halting remaining %s batches for this cycle (%s).",
                        media_type,
                        halt_reason,
                    )
                    break

                for tmdb_id in chunk_attempted:
                    attempted.add((media_type, tmdb_id))

                for tmdb_id, item in chunk_results.items():
                    results[(media_type, tmdb_id)] = item

                if on_chunk is not None:
                    on_chunk(
                        media_type,
                        chunk,
                        chunk_results,
                        chunk_attempted,
                        chunk_index,
                        total_chunks,
                    )

            if halted_early:
                break

        return (
            results,
            attempted,
            all_success,
            halted_early,
            halt_reason,
            failed_batches,
        )


@dataclass
class PMDBSubmitResult:
    success: bool
    retryable: bool
    retry_after_seconds: int
    duplicate_or_exists: bool
    error_text: str
    item_id: Optional[str]
    status_code: int = 0
    error_code: str = ""
    endpoint: str = ""


@dataclass
class PMDBDeleteResult:
    success: bool
    retryable: bool
    retry_after_seconds: int
    error_text: str
    status_code: int = 0
    error_code: str = ""
    endpoint: str = ""


class PMDBClient:
    def __init__(
        self,
        *,
        api_key: str,
        config: Dict[str, Any],
        api_gate: ServiceGate,
        rating_gate: ServiceGate,
        mapping_gate: ServiceGate,
    ):
        self.api_key = api_key
        self.base_url = config["base_url"].rstrip("/")
        self.http = HTTPClient(
            timeout_seconds=config["timeout_seconds"],
            max_retries=config["max_retries"],
        )
        self.api_gate = api_gate
        self.rating_gate = rating_gate
        self.mapping_gate = mapping_gate

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _auth_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
        }

    async def _post_with_gates(
        self,
        *,
        url: str,
        payload: Dict[str, Any],
        contribution_gate: ServiceGate,
    ) -> APIResponse:
        # Respect both global API rate and contribution/action-specific rate.
        acquired_global = await self.api_gate.acquire()
        if not acquired_global:
            return APIResponse(status=429, headers={}, data=None, text="global gate paused")

        acquired_action = await contribution_gate.acquire()
        if not acquired_action:
            return APIResponse(status=429, headers={}, data=None, text="action gate paused")

        response = await self.http.request_json(
            method="POST",
            url=url,
            headers=self._headers(),
            json_body=payload,
            gate=None,
        )

        self._observe_submission_response(
            response,
            contribution_gate,
            method="POST",
            endpoint=url,
        )

        return response

    async def _delete_with_gates(
        self,
        *,
        url: str,
        contribution_gate: ServiceGate,
    ) -> APIResponse:
        acquired_global = await self.api_gate.acquire()
        if not acquired_global:
            return APIResponse(status=429, headers={}, data=None, text="global gate paused")

        acquired_action = await contribution_gate.acquire()
        if not acquired_action:
            return APIResponse(status=429, headers={}, data=None, text="action gate paused")

        response = await self.http.request_json(
            method="DELETE",
            url=url,
            headers=self._auth_headers(),
            gate=None,
        )
        self._observe_submission_response(
            response,
            contribution_gate,
            method="DELETE",
            endpoint=url,
        )
        return response

    def _observe_submission_response(
        self,
        response: APIResponse,
        contribution_gate: ServiceGate,
        *,
        method: str,
        endpoint: str,
    ) -> None:
        # Mirror returned status into service-state rows for visibility.
        self.api_gate.observe_headers(response.headers, response.status)
        contribution_gate.observe_headers(response.headers, response.status)
        safe_endpoint = sanitize_url_for_logs(endpoint)
        error_code = self._extract_error_code(response.data, response.text or "")
        error_suffix = f" code={error_code}" if error_code else ""

        if response.status == 429:
            retry_after = parse_retry_after(response.headers.get("retry-after"), 5)
            self.api_gate.pause_for(retry_after, "PMDB 429")
            contribution_gate.pause_for(retry_after, "PMDB contribution 429")

        elif response.status == 401:
            self.api_gate.pause_for(300, "PMDB unauthorized")
            contribution_gate.pause_for(300, "PMDB unauthorized")
            LOGGER.error(
                "[PMDB] 401 Unauthorized on %s %s%s. Pausing PMDB gates for 300s.",
                method,
                safe_endpoint,
                error_suffix,
            )
        elif response.status == 403:
            LOGGER.warning(
                "[PMDB] 403 Forbidden on %s %s%s. Treating as non-retryable item-level conflict (no global pause).",
                method,
                safe_endpoint,
                error_suffix,
            )

    @staticmethod
    def _extract_item_id(payload: Any) -> Optional[str]:
        if isinstance(payload, dict):
            item = payload.get("item")
            if isinstance(item, dict):
                item_id = first_non_empty(item.get("id"))
                if item_id:
                    return item_id
            return first_non_empty(payload.get("id"))
        return None

    @staticmethod
    def _extract_error_code(payload: Any, text: str) -> str:
        if isinstance(payload, dict):
            for key in ("error_code", "code", "error", "type", "status"):
                value = payload.get(key)
                if value is None:
                    continue
                value_str = str(value).strip()
                if value_str:
                    return value_str[:80]
        text_norm = " ".join((text or "").split()).lower()
        if "duplicate" in text_norm:
            return "duplicate"
        if "exists" in text_norm:
            return "exists"
        if "forbidden" in text_norm:
            return "forbidden"
        if "unauthorized" in text_norm:
            return "unauthorized"
        if "rate" in text_norm and "limit" in text_norm:
            return "rate_limited"
        return ""

    @staticmethod
    def _is_create_failed_rating(result: PMDBSubmitResult) -> bool:
        code = str(result.error_code or "").strip().lower()
        text = str(result.error_text or "").strip().lower()
        return (
            result.status_code == 500
            and ("failed to create rating" in code or "failed to create rating" in text)
        )

    @staticmethod
    def _is_create_failed_mapping(result: PMDBSubmitResult) -> bool:
        code = str(result.error_code or "").strip().lower()
        text = str(result.error_text or "").strip().lower()
        return (
            result.status_code == 500
            and (
                "failed to create id mapping" in code
                or "failed to create id mapping" in text
            )
        )

    @staticmethod
    def _is_duplicate_or_exists_result(result: PMDBSubmitResult) -> bool:
        code = str(result.error_code or "").strip().lower()
        text = str(result.error_text or "").strip().lower()
        status = int(result.status_code or 0)
        if code in {"duplicate", "exists", "already_exists", "already-exists"}:
            return True
        if status in (400, 409, 422, 500) and (
            "duplicate" in text
            or "already exists" in text
            or "already-exists" in text
        ):
            return True
        return False

    @staticmethod
    def _extract_entry_id(entry: Dict[str, Any]) -> Optional[str]:
        return first_non_empty(entry.get("id"), entry.get("item_id"))

    @staticmethod
    def _to_submit_result(response: APIResponse, endpoint: str = "") -> PMDBSubmitResult:
        text = response.text or ""
        item_id = PMDBClient._extract_item_id(response.data)
        error_code = PMDBClient._extract_error_code(response.data, text)

        if response.ok:
            return PMDBSubmitResult(
                True,
                False,
                0,
                False,
                "",
                item_id,
                status_code=response.status,
                error_code="",
                endpoint=endpoint,
            )

        if response.status == 429:
            return PMDBSubmitResult(
                success=False,
                retryable=True,
                retry_after_seconds=parse_retry_after(
                    response.headers.get("retry-after"), 10
                ),
                duplicate_or_exists=False,
                error_text=text,
                item_id=item_id,
                status_code=response.status,
                error_code=error_code,
                endpoint=endpoint,
            )

        if response.status in (500, 502, 503, 504, 0):
            return PMDBSubmitResult(
                success=False,
                retryable=True,
                retry_after_seconds=30,
                duplicate_or_exists=False,
                error_text=text,
                item_id=item_id,
                status_code=response.status,
                error_code=error_code,
                endpoint=endpoint,
            )

        if response.status == 401:
            return PMDBSubmitResult(
                success=False,
                retryable=True,
                retry_after_seconds=300,
                duplicate_or_exists=False,
                error_text=text,
                item_id=item_id,
                status_code=response.status,
                error_code=error_code,
                endpoint=endpoint,
            )
        if response.status == 403:
            return PMDBSubmitResult(
                success=False,
                retryable=False,
                retry_after_seconds=0,
                duplicate_or_exists=False,
                error_text=text,
                item_id=item_id,
                status_code=response.status,
                error_code=error_code,
                endpoint=endpoint,
            )

        return PMDBSubmitResult(
            success=False,
            retryable=False,
            retry_after_seconds=0,
            duplicate_or_exists=False,
            error_text=text,
            item_id=item_id,
            status_code=response.status,
            error_code=error_code,
            endpoint=endpoint,
        )

    @staticmethod
    def _to_delete_result(response: APIResponse, endpoint: str = "") -> PMDBDeleteResult:
        text = response.text or ""
        error_code = PMDBClient._extract_error_code(response.data, text)
        if response.ok or response.status == 404:
            return PMDBDeleteResult(
                True,
                False,
                0,
                text,
                status_code=response.status,
                error_code="",
                endpoint=endpoint,
            )

        if response.status == 429:
            return PMDBDeleteResult(
                success=False,
                retryable=True,
                retry_after_seconds=parse_retry_after(
                    response.headers.get("retry-after"), 10
                ),
                error_text=text,
                status_code=response.status,
                error_code=error_code,
                endpoint=endpoint,
            )

        if response.status in (500, 502, 503, 504, 0):
            return PMDBDeleteResult(
                success=False,
                retryable=True,
                retry_after_seconds=30,
                error_text=text,
                status_code=response.status,
                error_code=error_code,
                endpoint=endpoint,
            )

        return PMDBDeleteResult(
            success=False,
            retryable=False,
            retry_after_seconds=0,
            error_text=text,
            status_code=response.status,
            error_code=error_code,
            endpoint=endpoint,
        )

    async def _fetch_existing_ratings(
        self, tmdb_id: int, media_type: str
    ) -> APIResponse:
        return await self.http.request_json(
            method="GET",
            url=f"{self.base_url}/api/external/ratings",
            headers=self._auth_headers(),
            params={
                "tmdb_id": tmdb_id,
                "media_type": media_type,
            },
            gate=self.api_gate,
        )

    async def _fetch_existing_mappings(
        self, tmdb_id: int, media_type: str
    ) -> APIResponse:
        return await self.http.request_json(
            method="GET",
            url=f"{self.base_url}/api/external/mappings",
            headers=self._auth_headers(),
            params={
                "tmdb_id": tmdb_id,
                "media_type": media_type,
            },
            gate=self.api_gate,
        )

    @staticmethod
    def _extract_ratings_for_label(payload: Any, label: str) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []
        items = payload.get("items")
        if not isinstance(items, list):
            return []

        wanted = label.strip().lower()
        output: List[Dict[str, Any]] = []
        for entry in items:
            if not isinstance(entry, dict):
                continue
            entry_label = str(entry.get("label", "")).strip().lower()
            if entry_label == wanted:
                output.append(entry)
        return output

    @staticmethod
    def _extract_mappings_for_type(payload: Any, id_type: str) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []
        mappings = payload.get("mappings")
        if not isinstance(mappings, dict):
            return []

        entries: Optional[Any] = None
        lookup_keys = (id_type, id_type.lower(), id_type.upper())
        for key in lookup_keys:
            if key in mappings:
                entries = mappings[key]
                break

        if not isinstance(entries, list):
            return []
        return [entry for entry in entries if isinstance(entry, dict)]

    @staticmethod
    def _rating_entry_matches_score(entry: Dict[str, Any], score: float) -> bool:
        existing_score = entry.get("score")
        try:
            existing_numeric = float(existing_score)
        except (TypeError, ValueError):
            return False
        return abs(existing_numeric - score) <= 0.1

    @staticmethod
    def _mapping_entry_matches_value(entry: Dict[str, Any], id_value: str) -> bool:
        existing_value = first_non_empty(
            entry.get("value"),
            entry.get("id_value"),
        )
        if not existing_value:
            return False
        return existing_value.strip().lower() == str(id_value).strip().lower()

    async def confirm_rating_exists(
        self,
        *,
        tmdb_id: int,
        media_type: str,
        label: str,
        score: float,
    ) -> Tuple[bool, Optional[str]]:
        lookup = await self._fetch_existing_ratings(tmdb_id, media_type)
        if lookup.status != 200 or not isinstance(lookup.data, dict):
            return False, None
        entries = self._extract_ratings_for_label(lookup.data, label)
        for entry in entries:
            if self._rating_entry_matches_score(entry, score):
                return True, self._extract_entry_id(entry)
        return False, None

    async def confirm_mapping_exists(
        self,
        *,
        tmdb_id: int,
        media_type: str,
        id_type: str,
        id_value: str,
    ) -> Tuple[bool, Optional[str]]:
        lookup = await self._fetch_existing_mappings(tmdb_id, media_type)
        if lookup.status != 200 or not isinstance(lookup.data, dict):
            return False, None
        entries = self._extract_mappings_for_type(lookup.data, id_type)
        for entry in entries:
            if self._mapping_entry_matches_value(entry, id_value):
                return True, self._extract_entry_id(entry)
        return False, None

    async def _delete_rating_by_id(self, rating_id: str) -> PMDBDeleteResult:
        response = await self._delete_with_gates(
            url=f"{self.base_url}/api/external/ratings/{rating_id}",
            contribution_gate=self.rating_gate,
        )
        return self._to_delete_result(
            response,
            endpoint=f"/api/external/ratings/{rating_id}",
        )

    async def _delete_mapping_by_id(self, mapping_id: str) -> PMDBDeleteResult:
        response = await self._delete_with_gates(
            url=f"{self.base_url}/api/external/mappings/{mapping_id}",
            contribution_gate=self.mapping_gate,
        )
        return self._to_delete_result(
            response,
            endpoint=f"/api/external/mappings/{mapping_id}",
        )

    async def _replace_rating_after_duplicate(
        self,
        *,
        tmdb_id: int,
        media_type: str,
        label: str,
        score: float,
        known_item_id: Optional[str],
    ) -> PMDBSubmitResult:
        lookup = await self._fetch_existing_ratings(tmdb_id, media_type)
        if lookup.status in (429, 500, 502, 503, 504, 0):
            return PMDBSubmitResult(
                success=False,
                retryable=True,
                retry_after_seconds=parse_retry_after(lookup.headers.get("retry-after"), 30),
                duplicate_or_exists=False,
                error_text=lookup.text or "PMDB rating lookup failed",
                item_id=None,
                status_code=lookup.status,
                error_code=self._extract_error_code(lookup.data, lookup.text),
                endpoint="/api/external/ratings",
            )
        if lookup.status == 401:
            return PMDBSubmitResult(
                success=False,
                retryable=True,
                retry_after_seconds=300,
                duplicate_or_exists=False,
                error_text=lookup.text or "PMDB unauthorized while listing ratings",
                item_id=None,
                status_code=lookup.status,
                error_code=self._extract_error_code(lookup.data, lookup.text),
                endpoint="/api/external/ratings",
            )
        if lookup.status == 403:
            return PMDBSubmitResult(
                success=False,
                retryable=False,
                retry_after_seconds=0,
                duplicate_or_exists=False,
                error_text=lookup.text or "PMDB forbidden while listing ratings",
                item_id=None,
                status_code=lookup.status,
                error_code=self._extract_error_code(lookup.data, lookup.text),
                endpoint="/api/external/ratings",
            )

        entries = self._extract_ratings_for_label(lookup.data, label)
        candidate_ids: List[str] = []
        if known_item_id:
            candidate_ids.append(known_item_id)
        for entry in entries:
            entry_id = self._extract_entry_id(entry)
            if entry_id and entry_id not in candidate_ids:
                candidate_ids.append(entry_id)

        deleted_any = False
        for entry_id in candidate_ids:
            delete_result = await self._delete_rating_by_id(entry_id)
            if delete_result.retryable:
                return PMDBSubmitResult(
                    success=False,
                    retryable=True,
                    retry_after_seconds=delete_result.retry_after_seconds,
                    duplicate_or_exists=False,
                    error_text=delete_result.error_text,
                    item_id=None,
                    status_code=delete_result.status_code,
                    error_code=delete_result.error_code,
                    endpoint=delete_result.endpoint or "/api/external/ratings",
                )
            if delete_result.success:
                deleted_any = True

        if deleted_any:
            retry_response = await self._post_with_gates(
                url=f"{self.base_url}/api/external/ratings",
                payload={
                    "tmdb_id": tmdb_id,
                    "media_type": media_type,
                    "score": score,
                    "label": label,
                },
                contribution_gate=self.rating_gate,
            )
            retry_result = self._to_submit_result(
                retry_response,
                endpoint="/api/external/ratings",
            )
            if retry_result.success:
                return retry_result

        # If the target score already exists remotely, avoid retry loops.
        for entry in entries:
            if self._rating_entry_matches_score(entry, score):
                return PMDBSubmitResult(
                    success=True,
                    retryable=False,
                    retry_after_seconds=0,
                    duplicate_or_exists=True,
                    error_text="",
                    item_id=self._extract_entry_id(entry),
                    status_code=200,
                    error_code="exists",
                    endpoint="/api/external/ratings",
                )

        if candidate_ids and not deleted_any:
            return PMDBSubmitResult(
                success=False,
                retryable=False,
                retry_after_seconds=0,
                duplicate_or_exists=False,
                error_text=(
                    "PMDB has existing rating entry for this label but it could not be "
                    "deleted with the current API key."
                ),
                item_id=None,
                status_code=409,
                error_code="conflict_not_owned",
                endpoint="/api/external/ratings",
            )

        return PMDBSubmitResult(
            success=False,
            retryable=False,
            retry_after_seconds=0,
            duplicate_or_exists=False,
            error_text="PMDB rating create failed and no replace target was found.",
            item_id=None,
            status_code=500,
            error_code="create_failed_unresolved",
            endpoint="/api/external/ratings",
        )

    async def _replace_mapping_after_duplicate(
        self,
        *,
        tmdb_id: int,
        media_type: str,
        id_type: str,
        id_value: str,
        known_item_id: Optional[str],
    ) -> PMDBSubmitResult:
        lookup = await self._fetch_existing_mappings(tmdb_id, media_type)
        if lookup.status in (429, 500, 502, 503, 504, 0):
            return PMDBSubmitResult(
                success=False,
                retryable=True,
                retry_after_seconds=parse_retry_after(lookup.headers.get("retry-after"), 30),
                duplicate_or_exists=False,
                error_text=lookup.text or "PMDB mapping lookup failed",
                item_id=None,
                status_code=lookup.status,
                error_code=self._extract_error_code(lookup.data, lookup.text),
                endpoint="/api/external/mappings",
            )
        if lookup.status == 401:
            return PMDBSubmitResult(
                success=False,
                retryable=True,
                retry_after_seconds=300,
                duplicate_or_exists=False,
                error_text=lookup.text or "PMDB unauthorized while listing mappings",
                item_id=None,
                status_code=lookup.status,
                error_code=self._extract_error_code(lookup.data, lookup.text),
                endpoint="/api/external/mappings",
            )
        if lookup.status == 403:
            return PMDBSubmitResult(
                success=False,
                retryable=False,
                retry_after_seconds=0,
                duplicate_or_exists=False,
                error_text=lookup.text or "PMDB forbidden while listing mappings",
                item_id=None,
                status_code=lookup.status,
                error_code=self._extract_error_code(lookup.data, lookup.text),
                endpoint="/api/external/mappings",
            )

        entries = self._extract_mappings_for_type(lookup.data, id_type)
        candidate_ids: List[str] = []
        if known_item_id:
            candidate_ids.append(known_item_id)
        for entry in entries:
            entry_id = self._extract_entry_id(entry)
            if entry_id and entry_id not in candidate_ids:
                candidate_ids.append(entry_id)

        deleted_any = False
        for entry_id in candidate_ids:
            delete_result = await self._delete_mapping_by_id(entry_id)
            if delete_result.retryable:
                return PMDBSubmitResult(
                    success=False,
                    retryable=True,
                    retry_after_seconds=delete_result.retry_after_seconds,
                    duplicate_or_exists=False,
                    error_text=delete_result.error_text,
                    item_id=None,
                    status_code=delete_result.status_code,
                    error_code=delete_result.error_code,
                    endpoint=delete_result.endpoint or "/api/external/mappings",
                )
            if delete_result.success:
                deleted_any = True

        if deleted_any:
            retry_response = await self._post_with_gates(
                url=f"{self.base_url}/api/external/mappings",
                payload={
                    "tmdb_id": tmdb_id,
                    "media_type": media_type,
                    "id_type": id_type,
                    "id_value": id_value,
                },
                contribution_gate=self.mapping_gate,
            )
            retry_result = self._to_submit_result(
                retry_response,
                endpoint="/api/external/mappings",
            )
            if retry_result.success:
                return retry_result

        # If the exact mapping exists remotely, avoid retry loops.
        for entry in entries:
            if self._mapping_entry_matches_value(entry, id_value):
                return PMDBSubmitResult(
                    success=True,
                    retryable=False,
                    retry_after_seconds=0,
                    duplicate_or_exists=True,
                    error_text="",
                    item_id=self._extract_entry_id(entry),
                    status_code=200,
                    error_code="exists",
                    endpoint="/api/external/mappings",
                )

        if candidate_ids and not deleted_any:
            return PMDBSubmitResult(
                success=False,
                retryable=False,
                retry_after_seconds=0,
                duplicate_or_exists=False,
                error_text=(
                    "PMDB has existing mapping entry for this id_type but it could not be "
                    "deleted with the current API key."
                ),
                item_id=None,
                status_code=409,
                error_code="conflict_not_owned",
                endpoint="/api/external/mappings",
            )

        return PMDBSubmitResult(
            success=False,
            retryable=False,
            retry_after_seconds=0,
            duplicate_or_exists=False,
            error_text="PMDB mapping create failed and no replace target was found.",
            item_id=None,
            status_code=500,
            error_code="create_failed_unresolved",
            endpoint="/api/external/mappings",
        )

    async def submit_rating(
        self,
        *,
        tmdb_id: int,
        media_type: str,
        label: str,
        score: float,
        existing_pmdb_item_id: Optional[str] = None,
    ) -> PMDBSubmitResult:
        if existing_pmdb_item_id:
            delete_result = await self._delete_rating_by_id(existing_pmdb_item_id)
            if not delete_result.success:
                return PMDBSubmitResult(
                    success=False,
                    retryable=delete_result.retryable,
                    retry_after_seconds=delete_result.retry_after_seconds
                    if delete_result.retryable
                    else 0,
                    duplicate_or_exists=False,
                    error_text=delete_result.error_text,
                    item_id=None,
                    status_code=delete_result.status_code,
                    error_code=delete_result.error_code,
                    endpoint=delete_result.endpoint or "/api/external/ratings",
                )

        response = await self._post_with_gates(
            url=f"{self.base_url}/api/external/ratings",
            payload={
                "tmdb_id": tmdb_id,
                "media_type": media_type,
                "score": score,
                "label": label,
            },
            contribution_gate=self.rating_gate,
        )
        result = self._to_submit_result(
            response,
            endpoint="/api/external/ratings",
        )
        if not result.success and (
            self._is_create_failed_rating(result)
            or self._is_duplicate_or_exists_result(result)
        ):
            resolved = await self._replace_rating_after_duplicate(
                tmdb_id=tmdb_id,
                media_type=media_type,
                label=label,
                score=score,
                known_item_id=existing_pmdb_item_id,
            )
            return resolved
        return result

    async def submit_mapping(
        self,
        *,
        tmdb_id: int,
        media_type: str,
        id_type: str,
        id_value: str,
        existing_pmdb_item_id: Optional[str] = None,
    ) -> PMDBSubmitResult:
        if existing_pmdb_item_id:
            delete_result = await self._delete_mapping_by_id(existing_pmdb_item_id)
            if not delete_result.success:
                return PMDBSubmitResult(
                    success=False,
                    retryable=delete_result.retryable,
                    retry_after_seconds=delete_result.retry_after_seconds
                    if delete_result.retryable
                    else 0,
                    duplicate_or_exists=False,
                    error_text=delete_result.error_text,
                    item_id=None,
                    status_code=delete_result.status_code,
                    error_code=delete_result.error_code,
                    endpoint=delete_result.endpoint or "/api/external/mappings",
                )

        response = await self._post_with_gates(
            url=f"{self.base_url}/api/external/mappings",
            payload={
                "tmdb_id": tmdb_id,
                "media_type": media_type,
                "id_type": id_type,
                "id_value": id_value,
            },
            contribution_gate=self.mapping_gate,
        )
        result = self._to_submit_result(
            response,
            endpoint="/api/external/mappings",
        )
        if not result.success and (
            self._is_create_failed_mapping(result)
            or self._is_duplicate_or_exists_result(result)
        ):
            resolved = await self._replace_mapping_after_duplicate(
                tmdb_id=tmdb_id,
                media_type=media_type,
                id_type=id_type,
                id_value=id_value,
                known_item_id=existing_pmdb_item_id,
            )
            return resolved
        return result


class RuntimeDashboard:
    """In-process terminal dashboard rendered with rich."""

    STAGE_COL_WIDTH = 19
    PROGRESS_COL_WIDTH = 32
    BAR_WIDTH = 30
    EVENTS_SYSTEM_LABEL_WIDTH = 24

    def __init__(
        self,
        *,
        db: LocalDatabase,
        submitter_workers: int,
        event_buffer: DashboardEventBuffer,
        event_lines: int = 8,
        refresh_seconds: float = 0.5,
    ):
        self.db = db
        self.submitter_workers = max(1, int(submitter_workers))
        self.event_buffer = event_buffer
        self.event_lines = max(3, int(event_lines))
        self.refresh_seconds = max(0.1, float(refresh_seconds))
        self.started_at = now_epoch()

        # Harvester lifecycle
        self.cycle_index = 0
        self.phase = "Idle"
        self.cycle_started_at = 0

        self.scan_pages_done = 0
        self.scan_pages_total = 0
        self.scan_selected = 0
        self.scan_raw_seen = 0
        self.scan_current_source = ""
        self.scan_current_source_page = 0
        self.scan_current_source_total = 0
        self.scan_source_order: List[str] = []
        self.scan_source_stats: Dict[str, Dict[str, int]] = {}

        self.details_total = 0
        self.details_done = 0
        self.details_ok = 0
        self.details_failed = 0
        self.details_started_at = 0

        self.md_movie_batches_total = 0
        self.md_movie_batches_done = 0
        self.md_movie_attempted = 0
        self.md_movie_found = 0
        self.md_movie_ids_total = 0
        self.md_movie_failed_batches = 0

        self.md_tv_batches_total = 0
        self.md_tv_batches_done = 0
        self.md_tv_attempted = 0
        self.md_tv_found = 0
        self.md_tv_ids_total = 0
        self.md_tv_failed_batches = 0

        self.enriched_total = 0
        self.enriched_done = 0
        self.enriched_mdblist_ok = 0
        self.enriched_mdblist_miss = 0
        self.enriched_tmdb_only = 0
        self.enriched_queue_ratings = 0
        self.enriched_queue_mappings = 0

        # Submitter runtime
        self.submit_events_ratings: deque[int] = deque()
        self.submit_events_mappings: deque[int] = deque()
        self.last_submit_error = ""
        self.last_submit_error_at = 0

        self._ratings_queue_peak = 0
        self._mappings_queue_peak = 0

        # DB-backed snapshots refreshed periodically.
        self._last_db_refresh = 0.0
        self._db_refresh_interval = 2.0
        self.queue_snapshot: Dict[str, int] = {
            "ratings_pending": 0,
            "mappings_pending": 0,
            "ratings_in_flight": 0,
            "mappings_in_flight": 0,
            "ratings_failed": 0,
            "mappings_failed": 0,
        }
        self.db_totals_snapshot: Dict[str, int] = {
            "titles_total": 0,
            "ratings_total": 0,
            "mappings_total": 0,
            "titles_submitted_total": 0,
            "titles_submitted_today": 0,
            "ratings_submitted_total": 0,
            "mappings_submitted_total": 0,
            "ratings_submitted_today": 0,
            "mappings_submitted_today": 0,
            "ratings_last_submit_at": 0,
            "mappings_last_submit_at": 0,
            "titles_last_harvested_at": 0,
            "titles_last_mdblist_fetch_at": 0,
        }
        self.service_snapshot: Dict[str, Optional[sqlite3.Row]] = {
            "tmdb": None,
            "mdblist": None,
            "pmdb_api": None,
        }

    def set_submitter_workers(self, workers: int) -> None:
        self.submitter_workers = max(1, int(workers))

    def begin_cycle(self, *, sources: Sequence[TMDBSource]) -> None:
        self.cycle_index += 1
        self.phase = "TMDB candidate scan"
        self.cycle_started_at = now_epoch()
        self._ratings_queue_peak = 0
        self._mappings_queue_peak = 0

        self.scan_pages_done = 0
        self.scan_pages_total = 0
        self.scan_selected = 0
        self.scan_raw_seen = 0
        self.scan_current_source = ""
        self.scan_current_source_page = 0
        self.scan_current_source_total = 0
        self.scan_source_order = []
        self.scan_source_stats = {}
        for source in sources:
            source_name = source.name
            max_pages = max(1, int(source.max_pages))
            self.scan_source_order.append(source_name)
            self.scan_source_stats[source_name] = {
                "pages_configured": max_pages,
                "pages_effective": max_pages,
                "pages_discovered": 0,
                "pages_fetched": 0,
                "raw_seen": 0,
                "added": 0,
                "duplicates": 0,
                "skipped": 0,
                "unsupported": 0,
                "errors": 0,
            }
            self.scan_pages_total += max_pages

        self.details_total = 0
        self.details_done = 0
        self.details_ok = 0
        self.details_failed = 0
        self.details_started_at = 0

        self.md_movie_batches_total = 0
        self.md_movie_batches_done = 0
        self.md_movie_attempted = 0
        self.md_movie_found = 0
        self.md_movie_ids_total = 0
        self.md_movie_failed_batches = 0

        self.md_tv_batches_total = 0
        self.md_tv_batches_done = 0
        self.md_tv_attempted = 0
        self.md_tv_found = 0
        self.md_tv_ids_total = 0
        self.md_tv_failed_batches = 0

        self.enriched_total = 0
        self.enriched_done = 0
        self.enriched_mdblist_ok = 0
        self.enriched_mdblist_miss = 0
        self.enriched_tmdb_only = 0
        self.enriched_queue_ratings = 0
        self.enriched_queue_mappings = 0

    def update_scan_progress(
        self,
        *,
        pages_done: int,
        pages_total: int,
        selected: int,
        raw_seen: int,
        current_source: str,
        current_source_page: int,
        current_source_total: int,
        source_stats: Optional[Dict[str, Dict[str, int]]] = None,
    ) -> None:
        self.scan_pages_done = max(0, int(pages_done))
        self.scan_pages_total = max(1, int(pages_total))
        self.scan_selected = max(0, int(selected))
        self.scan_raw_seen = max(0, int(raw_seen))
        self.scan_current_source = str(current_source or "")
        self.scan_current_source_page = max(0, int(current_source_page))
        self.scan_current_source_total = max(0, int(current_source_total))

        if not source_stats:
            return

        for source_name, source_data in source_stats.items():
            existing = self.scan_source_stats.setdefault(
                source_name,
                {
                    "pages_configured": 0,
                    "pages_effective": 0,
                    "pages_discovered": 0,
                    "pages_fetched": 0,
                    "raw_seen": 0,
                    "added": 0,
                    "duplicates": 0,
                    "skipped": 0,
                    "unsupported": 0,
                    "errors": 0,
                },
            )
            if source_name not in self.scan_source_order:
                self.scan_source_order.append(source_name)
            for key in (
                "pages_configured",
                "pages_effective",
                "pages_discovered",
                "pages_fetched",
                "raw_seen",
                "added",
                "duplicates",
                "skipped",
                "unsupported",
                "errors",
            ):
                existing[key] = max(0, int(source_data.get(key) or 0))

    def complete_scan(
        self,
        *,
        selected: int,
        raw_seen: int,
        scanned_pages: Optional[int] = None,
        total_pages: Optional[int] = None,
        source_stats: Optional[Dict[str, Dict[str, int]]] = None,
    ) -> None:
        if total_pages is not None:
            self.scan_pages_total = max(1, int(total_pages))
        if scanned_pages is not None:
            self.scan_pages_done = max(0, int(scanned_pages))
        else:
            self.scan_pages_done = self.scan_pages_total
        self.scan_selected = max(0, int(selected))
        self.scan_raw_seen = max(0, int(raw_seen))
        if source_stats:
            self.update_scan_progress(
                pages_done=self.scan_pages_done,
                pages_total=self.scan_pages_total,
                selected=self.scan_selected,
                raw_seen=self.scan_raw_seen,
                current_source=self.scan_current_source,
                current_source_page=self.scan_current_source_page,
                current_source_total=self.scan_current_source_total,
                source_stats=source_stats,
            )

    def begin_tmdb_details(self, *, total: int) -> None:
        self.phase = "TMDB details"
        self.details_total = max(0, int(total))
        self.details_done = 0
        self.details_ok = 0
        self.details_failed = 0
        self.details_started_at = now_epoch()

    def update_tmdb_details(
        self,
        *,
        completed: int,
        succeeded: int,
        failed: int,
    ) -> None:
        self.details_done = max(0, int(completed))
        self.details_ok = max(0, int(succeeded))
        self.details_failed = max(0, int(failed))

    def begin_mdblist(
        self,
        *,
        movie_ids_total: int,
        movie_batches_total: int,
        tv_ids_total: int,
        tv_batches_total: int,
        enrich_total: int,
    ) -> None:
        self.phase = "MDBList enrichment"
        self.md_movie_ids_total = max(0, int(movie_ids_total))
        self.md_movie_batches_total = max(0, int(movie_batches_total))
        self.md_movie_batches_done = 0
        self.md_movie_attempted = 0
        self.md_movie_found = 0
        self.md_movie_failed_batches = 0

        self.md_tv_ids_total = max(0, int(tv_ids_total))
        self.md_tv_batches_total = max(0, int(tv_batches_total))
        self.md_tv_batches_done = 0
        self.md_tv_attempted = 0
        self.md_tv_found = 0
        self.md_tv_failed_batches = 0

        self.enriched_total = max(0, int(enrich_total))
        self.enriched_done = 0
        self.enriched_mdblist_ok = 0
        self.enriched_mdblist_miss = 0
        self.enriched_tmdb_only = 0
        self.enriched_queue_ratings = 0
        self.enriched_queue_mappings = 0

    def update_mdblist_chunk(
        self,
        *,
        media_type: str,
        chunk_index: int,
        attempted_count: int,
        found_count: int,
    ) -> None:
        if media_type == "movie":
            self.md_movie_batches_done = max(self.md_movie_batches_done, int(chunk_index))
            self.md_movie_attempted += max(0, int(attempted_count))
            self.md_movie_found += max(0, int(found_count))
            return

        self.md_tv_batches_done = max(self.md_tv_batches_done, int(chunk_index))
        self.md_tv_attempted += max(0, int(attempted_count))
        self.md_tv_found += max(0, int(found_count))

    def set_mdblist_failed_batches(self, *, movie_failed: int, tv_failed: int) -> None:
        self.md_movie_failed_batches = max(0, int(movie_failed))
        self.md_tv_failed_batches = max(0, int(tv_failed))

    def update_enriched(
        self,
        *,
        enriched: int,
        total: int,
        mdblist_ok: int,
        mdblist_miss: int,
        tmdb_only: int,
        queue_ratings: int,
        queue_mappings: int,
    ) -> None:
        self.enriched_done = max(0, int(enriched))
        self.enriched_total = max(self.enriched_total, int(total))
        self.enriched_mdblist_ok = max(0, int(mdblist_ok))
        self.enriched_mdblist_miss = max(0, int(mdblist_miss))
        self.enriched_tmdb_only = max(0, int(tmdb_only))
        self.enriched_queue_ratings = max(0, int(queue_ratings))
        self.enriched_queue_mappings = max(0, int(queue_mappings))

    def complete_enrichment(
        self,
        *,
        enriched: int,
        mdblist_ok: int,
        mdblist_miss: int,
        tmdb_only: int,
        queue_ratings: int,
        queue_mappings: int,
    ) -> None:
        self.phase = "Cycle complete"
        self.enriched_done = max(0, int(enriched))
        if self.enriched_total <= 0:
            self.enriched_total = self.enriched_done
        self.enriched_mdblist_ok = max(0, int(mdblist_ok))
        self.enriched_mdblist_miss = max(0, int(mdblist_miss))
        self.enriched_tmdb_only = max(0, int(tmdb_only))
        self.enriched_queue_ratings = max(0, int(queue_ratings))
        self.enriched_queue_mappings = max(0, int(queue_mappings))

    def record_submit_result(self, *, kind: str, outcome: str, error_text: str = "") -> None:
        now_ts = now_epoch()
        if outcome == "success":
            if kind == "rating":
                self.submit_events_ratings.append(now_ts)
            else:
                self.submit_events_mappings.append(now_ts)
            return

        if outcome in {"retry", "failed"}:
            if error_text:
                short_error = " ".join(str(error_text).split())
                self.last_submit_error = short_error[:220]
                self.last_submit_error_at = now_ts

    @staticmethod
    def _render_bar(total: int, completed: int, width: int = BAR_WIDTH) -> Any:
        if total <= 0:
            return Text("-")
        safe_total = max(1, int(total))
        safe_done = max(0, min(int(completed), safe_total))
        return ProgressBar(total=safe_total, completed=safe_done, width=width)

    @staticmethod
    def _fmt_int(value: int, width: int) -> str:
        return f"{max(0, int(value)):>{max(1, int(width))}d}"

    @staticmethod
    def _fmt_pct(value: float, width: int = 6) -> str:
        return f"{float(value):>{max(1, int(width))}.2f}%"

    @staticmethod
    def _format_iso(ts: int) -> str:
        if not ts:
            return "-"
        return to_iso(ts)

    @staticmethod
    def _format_duration(seconds: int) -> str:
        s = max(0, int(seconds))
        hours, rem = divmod(s, 3600)
        minutes, secs = divmod(rem, 60)
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        return f"{minutes:02d}:{secs:02d}"

    @staticmethod
    def _local_day_key() -> str:
        return local_day_key()

    def _trim_submit_events(self, now_ts: int) -> None:
        cutoff = now_ts - 3600
        while self.submit_events_ratings and self.submit_events_ratings[0] < cutoff:
            self.submit_events_ratings.popleft()
        while self.submit_events_mappings and self.submit_events_mappings[0] < cutoff:
            self.submit_events_mappings.popleft()

    def _count_recent(self, events: deque[int], window_seconds: int, now_ts: int) -> int:
        cutoff = now_ts - max(1, int(window_seconds))
        return sum(1 for ts in events if ts >= cutoff)

    def _refresh_from_db(self, force: bool = False) -> None:
        now_mono = time.monotonic()
        if not force and now_mono - self._last_db_refresh < self._db_refresh_interval:
            return
        self._last_db_refresh = now_mono

        self.queue_snapshot = self.db.queue_counts()
        self.db_totals_snapshot = self.db.dashboard_snapshot(self._local_day_key())
        self.service_snapshot = {
            "tmdb": self.db.get_service_state("tmdb"),
            "mdblist": self.db.get_service_state("mdblist"),
            "pmdb_api": self.db.get_service_state("pmdb_api"),
        }

        ratings_remaining = (
            self.queue_snapshot["ratings_pending"]
            + self.queue_snapshot["ratings_in_flight"]
        )
        mappings_remaining = (
            self.queue_snapshot["mappings_pending"]
            + self.queue_snapshot["mappings_in_flight"]
        )
        if ratings_remaining <= 0:
            self._ratings_queue_peak = 0
        elif ratings_remaining > self._ratings_queue_peak:
            self._ratings_queue_peak = ratings_remaining
        if mappings_remaining <= 0:
            self._mappings_queue_peak = 0
        elif mappings_remaining > self._mappings_queue_peak:
            self._mappings_queue_peak = mappings_remaining

    def _render_harvester_panel(self, now_ts: int) -> Panel:
        table = Table.grid(expand=True, padding=(0, 0))
        table.add_column(
            "Stage",
            no_wrap=True,
            style="bold cyan",
            width=self.STAGE_COL_WIDTH,
            min_width=self.STAGE_COL_WIDTH,
            max_width=self.STAGE_COL_WIDTH,
        )
        table.add_column(
            "Progress",
            no_wrap=True,
            width=self.PROGRESS_COL_WIDTH,
            min_width=self.PROGRESS_COL_WIDTH,
            max_width=self.PROGRESS_COL_WIDTH,
        )
        table.add_column(
            "Details",
            style="white",
            no_wrap=True,
            overflow="crop",
            justify="left",
            ratio=1,
        )

        scan_added = self.scan_selected
        scan_seen = self.scan_raw_seen
        scan_duplicates = 0
        scan_skipped = 0
        scan_errors = 0
        for stat in self.scan_source_stats.values():
            scan_duplicates += max(0, int(stat.get("duplicates") or 0))
            scan_skipped += max(0, int(stat.get("skipped") or 0))
            scan_errors += max(0, int(stat.get("errors") or 0))

        eligibility_pct = (
            (self.scan_selected / self.scan_raw_seen) * 100.0
            if self.scan_raw_seen > 0
            else 0.0
        )
        table.add_row(
            "TMDB scan",
            self._render_bar(self.scan_pages_total, self.scan_pages_done),
            (
                f"p={int(self.scan_pages_done)}/{int(self.scan_pages_total)} "
                f"add={int(scan_added)} "
                f"dup={int(scan_duplicates)} "
                f"skip={int(scan_skipped)} "
                f"seen={int(scan_seen)} "
                f"err={int(scan_errors)} "
                f"elig={eligibility_pct:.2f}%"
            ),
        )

        for source_name in self.scan_source_order:
            stat = self.scan_source_stats.get(source_name)
            if not stat:
                continue
            pages_effective = max(1, int(stat.get("pages_effective") or 0))
            pages_fetched = max(0, int(stat.get("pages_fetched") or 0))
            source_added = max(0, int(stat.get("added") or 0))
            source_duplicates = max(0, int(stat.get("duplicates") or 0))
            source_skipped = max(0, int(stat.get("skipped") or 0))
            source_seen = max(0, int(stat.get("raw_seen") or 0))
            source_errors = max(0, int(stat.get("errors") or 0))
            source_eligibility = (
                (source_added / source_seen) * 100.0 if source_seen > 0 else 0.0
            )
            table.add_row(
                source_name,
                self._render_bar(pages_effective, pages_fetched),
                (
                    f"p={int(pages_fetched)}/{int(pages_effective)} "
                    f"add={int(source_added)} "
                    f"dup={int(source_duplicates)} "
                    f"skip={int(source_skipped)} "
                    f"seen={int(source_seen)} "
                    f"err={int(source_errors)} "
                    f"elig={source_eligibility:.2f}%"
                ),
            )

        details_success_pct = (
            (self.details_ok / self.details_done) * 100.0
            if self.details_done > 0
            else 0.0
        )
        table.add_row(
            "TMDB details",
            self._render_bar(self.details_total, self.details_done),
            (
                f"{int(self.details_done)}/{int(self.details_total)} "
                f"ok={int(self.details_ok)} "
                f"fail={int(self.details_failed)} "
                f"suc={details_success_pct:.2f}%"
            ),
        )

        md_movie_miss = max(0, self.md_movie_attempted - self.md_movie_found)
        md_movie_success = (
            (self.md_movie_found / self.md_movie_attempted) * 100.0
            if self.md_movie_attempted > 0
            else 0.0
        )
        table.add_row(
            "MDBList movie",
            self._render_bar(self.md_movie_batches_total, self.md_movie_batches_done),
            (
                f"{int(self.md_movie_attempted)}/{int(self.md_movie_ids_total)} "
                f"ok={int(self.md_movie_found)} "
                f"miss={int(md_movie_miss)} "
                f"suc={md_movie_success:.2f}% "
                f"b={int(self.md_movie_batches_done)}/{int(self.md_movie_batches_total)} "
                f"f={int(self.md_movie_failed_batches)}"
            ),
        )
        md_tv_miss = max(0, self.md_tv_attempted - self.md_tv_found)
        md_tv_success = (
            (self.md_tv_found / self.md_tv_attempted) * 100.0
            if self.md_tv_attempted > 0
            else 0.0
        )
        table.add_row(
            "MDBList tv",
            self._render_bar(self.md_tv_batches_total, self.md_tv_batches_done),
            (
                f"{int(self.md_tv_attempted)}/{int(self.md_tv_ids_total)} "
                f"ok={int(self.md_tv_found)} "
                f"miss={int(md_tv_miss)} "
                f"suc={md_tv_success:.2f}% "
                f"b={int(self.md_tv_batches_done)}/{int(self.md_tv_batches_total)} "
                f"f={int(self.md_tv_failed_batches)}"
            ),
        )
        table.add_row(
            "Enriched",
            self._render_bar(self.enriched_total, self.enriched_done),
            (
                f"{int(self.enriched_done)}/{int(self.enriched_total)} "
                f"md_ok={int(self.enriched_mdblist_ok)} "
                f"md_miss={int(self.enriched_mdblist_miss)} "
                f"tm_only={int(self.enriched_tmdb_only)} "
                f"q(r={int(self.enriched_queue_ratings)},m={int(self.enriched_queue_mappings)})"
            ),
        )

        subtitle = (
            f"phase={self.phase} cycle={self.cycle_index} "
            f"cycle_started={self._format_iso(self.cycle_started_at)}"
        )
        return Panel(
            table,
            title="Harvester",
            subtitle=subtitle,
            border_style="cyan",
            title_align="left",
            subtitle_align="left",
        )

    def _render_submitter_panel(self, now_ts: int) -> Panel:
        ratings_pending = self.queue_snapshot["ratings_pending"]
        ratings_in_flight = self.queue_snapshot["ratings_in_flight"]
        ratings_failed = self.queue_snapshot["ratings_failed"]

        mappings_pending = self.queue_snapshot["mappings_pending"]
        mappings_in_flight = self.queue_snapshot["mappings_in_flight"]
        mappings_failed = self.queue_snapshot["mappings_failed"]

        ratings_remaining = ratings_pending + ratings_in_flight
        mappings_remaining = mappings_pending + mappings_in_flight

        ratings_peak = max(1, self._ratings_queue_peak)
        mappings_peak = max(1, self._mappings_queue_peak)

        # Queue bars are intentionally remaining-based (decreasing as queue drains).
        ratings_bar: Any = self._render_bar(ratings_peak, max(0, int(ratings_remaining)))
        mappings_bar: Any = self._render_bar(mappings_peak, max(0, int(mappings_remaining)))

        ratings_due = self.db.count_due_queue(kind="ratings", now_ts=now_ts)
        mappings_due = self.db.count_due_queue(kind="mappings", now_ts=now_ts)
        ratings_wait = max(0, ratings_pending - ratings_due)
        mappings_wait = max(0, mappings_pending - mappings_due)

        workers_busy = min(self.submitter_workers, ratings_in_flight + mappings_in_flight)
        worker_util = (
            (workers_busy / self.submitter_workers) * 100.0
            if self.submitter_workers > 0
            else 0.0
        )
        self._trim_submit_events(now_ts)
        ratings_ok_60 = self._count_recent(self.submit_events_ratings, 60, now_ts)
        mappings_ok_60 = self._count_recent(self.submit_events_mappings, 60, now_ts)
        total_ok_60 = ratings_ok_60 + mappings_ok_60

        table = Table.grid(expand=True, padding=(0, 0))
        table.add_column(
            "Stage",
            no_wrap=True,
            style="bold green",
            width=self.STAGE_COL_WIDTH,
            min_width=self.STAGE_COL_WIDTH,
            max_width=self.STAGE_COL_WIDTH,
        )
        table.add_column(
            "Progress",
            no_wrap=True,
            width=self.PROGRESS_COL_WIDTH,
            min_width=self.PROGRESS_COL_WIDTH,
            max_width=self.PROGRESS_COL_WIDTH,
        )
        table.add_column(
            "Details",
            style="white",
            no_wrap=True,
            overflow="crop",
            justify="left",
            ratio=1,
        )

        table.add_row(
            "Ratings queue",
            ratings_bar,
            (
                f"q={int(ratings_pending)} "
                f"d={int(ratings_due)} "
                f"w={int(ratings_wait)} "
                f"a={int(ratings_in_flight)} "
                f"f={int(ratings_failed)} "
                f"s={int(self.db_totals_snapshot['ratings_submitted_total'])}"
            ),
        )
        table.add_row(
            "Mappings queue",
            mappings_bar,
            (
                f"q={int(mappings_pending)} "
                f"d={int(mappings_due)} "
                f"w={int(mappings_wait)} "
                f"a={int(mappings_in_flight)} "
                f"f={int(mappings_failed)} "
                f"s={int(self.db_totals_snapshot['mappings_submitted_total'])}"
            ),
        )
        workers_bar: Any = self._render_bar(self.submitter_workers, workers_busy)
        table.add_row(
            "Workers",
            workers_bar,
            (
                f"{int(workers_busy)}/{int(self.submitter_workers)} "
                f"(r={int(ratings_in_flight)},m={int(mappings_in_flight)}) "
                f"util={worker_util:.1f}%"
            ),
        )
        return Panel(
            table,
            title="Submitter",
            subtitle=(
                f"ok60: r={int(ratings_ok_60)} "
                f"m={int(mappings_ok_60)} total={int(total_ok_60)}"
            ),
            border_style="green",
            title_align="left",
            subtitle_align="left",
        )

    def _render_events_panel(self) -> Panel:
        table = Table.grid(expand=True, padding=(0, 0))
        table.add_column(
            "When",
            no_wrap=True,
            style="bold yellow",
            width=self.EVENTS_SYSTEM_LABEL_WIDTH,
            min_width=self.EVENTS_SYSTEM_LABEL_WIDTH,
            max_width=self.EVENTS_SYSTEM_LABEL_WIDTH,
        )
        table.add_column(
            "Event",
            style="white",
            no_wrap=True,
            overflow="crop",
            justify="left",
            ratio=1,
        )

        events = self.event_buffer.snapshot()
        rendered = []
        for entry in reversed(events[-self.event_lines :]):
            level = "WARN" if str(entry.level).upper() == "WARNING" else str(entry.level).upper()
            when = self._format_iso(int(entry.timestamp))
            suffix = f" x{entry.count}" if entry.count > 1 else ""
            rendered.append((f"{when} {level}", f"{entry.message}{suffix}"))

        while len(rendered) < self.event_lines:
            rendered.append(("-", "-"))

        for when, message in rendered:
            table.add_row(when, message)

        return Panel(table, title="Events", border_style="yellow", title_align="left")

    def _render_system_panel(self) -> Panel:
        def service_line(service_key: str) -> str:
            row = self.service_snapshot.get(service_key)
            if row is None:
                return "-"
            paused_until = int(row["paused_until"] or 0)
            paused = paused_until > now_epoch()
            status = int(row["last_status"] or 0)
            updated = self._format_iso(int(row["updated_at"] or 0))
            if paused:
                return (
                    f"paused status={status} until={self._format_iso(paused_until)} "
                    f"updated={updated}"
                )
            return (
                f"ready status={status} updated={updated}"
            )

        mdblist_row = self.service_snapshot.get("mdblist")
        mdblist_line = "-"
        if mdblist_row is not None:
            mdblist_line = (
                "{remaining}/{limit} reset={reset}".format(
                    remaining=mdblist_row["rate_remaining"],
                    limit=mdblist_row["rate_limit"],
                    reset=self._format_iso(int(mdblist_row["rate_reset"] or 0)),
                )
            )

        table = Table.grid(expand=True, padding=(0, 0))
        table.add_column(
            style="bold magenta",
            no_wrap=True,
            width=self.EVENTS_SYSTEM_LABEL_WIDTH,
            min_width=self.EVENTS_SYSTEM_LABEL_WIDTH,
            max_width=self.EVENTS_SYSTEM_LABEL_WIDTH,
            justify="left",
        )
        table.add_column(
            style="white",
            no_wrap=True,
            overflow="crop",
            justify="left",
            ratio=1,
        )
        table.add_row(
            "Total local DB",
            (
                f"t={self.db_totals_snapshot['titles_total']} "
                f"r={self.db_totals_snapshot['ratings_total']} "
                f"m={self.db_totals_snapshot['mappings_total']}"
            ),
        )
        table.add_row(
            "Total submitted",
            (
                f"t={self.db_totals_snapshot['titles_submitted_total']} "
                f"r={self.db_totals_snapshot['ratings_submitted_total']} "
                f"m={self.db_totals_snapshot['mappings_submitted_total']}"
            ),
        )
        table.add_row(
            "Submitted today",
            (
                f"t={self.db_totals_snapshot['titles_submitted_today']} "
                f"r={self.db_totals_snapshot['ratings_submitted_today']} "
                f"m={self.db_totals_snapshot['mappings_submitted_today']}"
            ),
        )
        table.add_row(
            "Harvester activity",
            (
                f"harvested={self._format_iso(self.db_totals_snapshot['titles_last_harvested_at'])} "
                f"mdblist={self._format_iso(self.db_totals_snapshot['titles_last_mdblist_fetch_at'])}"
            ),
        )
        table.add_row(
            "Submitter activity",
            (
                f"ratings={self._format_iso(self.db_totals_snapshot['ratings_last_submit_at'])} "
                f"mappings={self._format_iso(self.db_totals_snapshot['mappings_last_submit_at'])}"
            ),
        )
        table.add_row("MDBList limit", mdblist_line)
        table.add_row("Service TMDB", service_line("tmdb"))
        table.add_row("Service MDBList", service_line("mdblist"))
        table.add_row("Service PMDB", service_line("pmdb_api"))

        return Panel(table, title="System", border_style="magenta", title_align="left")

    def render(self) -> Group:
        now_ts = now_epoch()
        uptime = self._format_duration(now_ts - self.started_at)
        header = Text(
            f"mdblist-pmdb live status | uptime={uptime} | cycle={self.cycle_index} | phase={self.phase}",
            style="bold",
        )
        return Group(
            header,
            self._render_harvester_panel(now_ts),
            self._render_submitter_panel(now_ts),
            self._render_events_panel(),
            self._render_system_panel(),
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        self._refresh_from_db(force=True)
        with Live(
            self.render(),
            auto_refresh=False,
            refresh_per_second=max(1, int(1 / self.refresh_seconds)),
            transient=False,
            screen=False,
        ) as live:
            while not stop_event.is_set():
                self._refresh_from_db(force=False)
                live.update(self.render(), refresh=True)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self.refresh_seconds)
                except asyncio.TimeoutError:
                    pass


class Harvester:
    def __init__(
        self,
        *,
        config: Dict[str, Any],
        db: LocalDatabase,
        tmdb_client: TMDBClient,
        mdblist_client: MDBListClient,
        status: Optional[RuntimeDashboard] = None,
    ):
        self.config = config
        self.db = db
        self.tmdb_client = tmdb_client
        self.mdblist_client = mdblist_client
        self.status = status

        self.ratings_ttl_seconds = int(config["runtime"]["ratings_refresh_days"]) * 86400
        self.harvest_mode = str(config["runtime"]["harvest_mode"]).strip().lower()
        self.run_mode = str(config["runtime"].get("run_mode", "continuous")).strip().lower()

        self.cycle_sleep_seconds = int(config["runtime"]["harvester_cycle_sleep_seconds"])

        self.details_concurrency = int(config["tmdb"]["details_concurrency"])
        self.sources: List[TMDBSource] = [
            self.tmdb_client.build_source(entry)
            for entry in config["tmdb"]["sources"]
            if bool(entry.get("enabled", True))
        ]
        self._checkpoint_key = HARVEST_CHECKPOINT_KEY

    @staticmethod
    def _serialize_candidate(candidate: Candidate) -> Dict[str, Any]:
        return {
            "tmdb_id": int(candidate.tmdb_id),
            "media_type": str(candidate.media_type),
            "title": str(candidate.title),
            "popularity": float(candidate.popularity),
        }

    @staticmethod
    def _deserialize_candidate(payload: Dict[str, Any]) -> Optional[Candidate]:
        if not isinstance(payload, dict):
            return None
        tmdb_id = parse_int(payload.get("tmdb_id"))
        media_type = str(payload.get("media_type") or "").strip().lower()
        title = str(payload.get("title") or "").strip()
        popularity = payload.get("popularity", 0.0)
        try:
            popularity_value = float(popularity or 0.0)
        except (TypeError, ValueError):
            popularity_value = 0.0
        if tmdb_id is None or media_type not in {"movie", "tv"}:
            return None
        return Candidate(
            tmdb_id=tmdb_id,
            media_type=media_type,
            title=title or f"TMDB-{tmdb_id}",
            popularity=popularity_value,
        )

    def _save_checkpoint(
        self,
        *,
        phase: str,
        candidates: Sequence[Candidate],
        source_stats: Optional[Dict[str, Dict[str, int]]] = None,
        tmdb_list_request_errors: int = 0,
        note: str = "",
    ) -> None:
        payload = {
            "version": 1,
            "phase": phase,
            "updated_at": now_epoch(),
            "harvest_mode": self.harvest_mode,
            "tmdb_list_request_errors": int(tmdb_list_request_errors),
            "source_stats": source_stats or {},
            "candidates": [self._serialize_candidate(c) for c in candidates],
            "note": note,
        }
        self.db.set_state(self._checkpoint_key, json.dumps(payload))

    def _clear_checkpoint(self) -> None:
        self.db.delete_state(self._checkpoint_key)

    def _load_checkpoint(self) -> Optional[Dict[str, Any]]:
        raw = self.db.get_state(self._checkpoint_key)
        if not raw:
            return None
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            LOGGER.warning("[Harvester] Invalid checkpoint JSON found. Clearing it.")
            self._clear_checkpoint()
            return None
        if not isinstance(parsed, dict):
            self._clear_checkpoint()
            return None
        return parsed

    def _restore_checkpoint_candidates(
        self, payload: Dict[str, Any]
    ) -> Tuple[List[Candidate], Dict[str, Dict[str, int]], int, str]:
        candidates_raw = payload.get("candidates")
        source_stats_raw = payload.get("source_stats")
        tmdb_list_request_errors = parse_int(payload.get("tmdb_list_request_errors")) or 0
        phase = str(payload.get("phase") or "").strip() or "unknown"

        candidates: List[Candidate] = []
        if isinstance(candidates_raw, list):
            for item in candidates_raw:
                candidate = self._deserialize_candidate(item)
                if candidate is not None:
                    candidates.append(candidate)

        filtered_candidates: List[Candidate] = []
        now_ts = now_epoch()
        for candidate in candidates:
            if self.db.should_harvest(
                tmdb_id=candidate.tmdb_id,
                media_type=candidate.media_type,
                now_ts=now_ts,
                ratings_ttl_seconds=self.ratings_ttl_seconds,
                harvest_mode=self.harvest_mode,
            ):
                filtered_candidates.append(candidate)

        source_stats: Dict[str, Dict[str, int]] = {}
        if isinstance(source_stats_raw, dict):
            for key, value in source_stats_raw.items():
                if not isinstance(key, str) or not isinstance(value, dict):
                    continue
                source_stats[key] = {
                    "pages_configured": int(value.get("pages_configured") or 0),
                    "pages_effective": int(value.get("pages_effective") or 0),
                    "pages_discovered": int(value.get("pages_discovered") or 0),
                    "pages_fetched": int(value.get("pages_fetched") or 0),
                    "raw_seen": int(value.get("raw_seen") or 0),
                    "added": int(value.get("added") or 0),
                    "duplicates": int(value.get("duplicates") or 0),
                    "skipped": int(value.get("skipped") or 0),
                    "unsupported": int(value.get("unsupported") or 0),
                    "errors": int(value.get("errors") or 0),
                    "first_page": int(value.get("first_page") or 0),
                    "last_page": int(value.get("last_page") or 0),
                }

        return filtered_candidates, source_stats, tmdb_list_request_errors, phase

    def _save_candidate_enrichment(
        self,
        *,
        candidate: Candidate,
        details: Optional[Dict[str, Any]],
        md_item: Optional[Dict[str, Any]],
        now_ts: int,
    ) -> Tuple[int, int, int, int, int]:
        ratings = parse_mdblist_ratings(md_item)
        tm_score = parse_tmdb_vote_average(details)
        if tm_score is not None:
            ratings["TM"] = tm_score

        mappings = extract_mappings(candidate.media_type, details, md_item)
        imdb_id = mappings.get("imdb")

        error_reasons: List[str] = []
        if details is None:
            error_reasons.append("TMDB details failed or unavailable")
        if md_item is None:
            error_reasons.append("MDBList item missing or unavailable")
        enrichment_error = "; ".join(error_reasons) if error_reasons else None

        queued_ratings, queued_mappings = self.db.save_enriched_item(
            tmdb_id=candidate.tmdb_id,
            media_type=candidate.media_type,
            title=candidate.title,
            imdb_id=imdb_id,
            popularity=candidate.popularity,
            tmdb_vote_average=(details or {}).get("vote_average")
            if isinstance(details, dict)
            else None,
            enrichment_error=enrichment_error,
            ratings=ratings,
            mappings=mappings,
            now_ts=now_ts,
        )
        mdblist_ok = 1 if md_item is not None else 0
        mdblist_miss = 1 if md_item is None else 0
        tmdb_only = 1 if (md_item is None and details is not None) else 0
        return mdblist_ok, mdblist_miss, tmdb_only, queued_ratings, queued_mappings

    async def _collect_candidates(
        self, stop_event: asyncio.Event
    ) -> Tuple[List[Candidate], Dict[str, Dict[str, int]], bool]:
        candidates: List[Candidate] = []
        seen: Set[Tuple[str, int]] = set()
        interrupted = False

        now_ts = now_epoch()
        cycle_started_ts = now_ts
        configured_target_pages = sum(source.max_pages for source in self.sources)
        effective_target_pages = configured_target_pages
        pages_scanned = 0
        raw_seen_total = 0

        source_stats: Dict[str, Dict[str, int]] = {
            source.name: {
                "pages_configured": source.max_pages,
                "pages_effective": source.max_pages,
                "pages_discovered": 0,
                "pages_fetched": 0,
                "raw_seen": 0,
                "added": 0,
                "duplicates": 0,
                "skipped": 0,
                "unsupported": 0,
                "errors": 0,
                "first_page": 0,
                "last_page": 0,
            }
            for source in self.sources
        }

        if self.status is not None:
            self.status.begin_cycle(sources=self.sources)

        def publish_scan_progress(
            current_source: TMDBSource,
            *,
            current_page: int,
        ) -> None:
            if self.status is None:
                return
            stat = source_stats[current_source.name]
            self.status.update_scan_progress(
                pages_done=pages_scanned,
                pages_total=max(pages_scanned, effective_target_pages),
                selected=len(candidates),
                raw_seen=raw_seen_total,
                current_source=current_source.name,
                current_source_page=current_page,
                current_source_total=max(1, int(stat["pages_effective"])),
                source_stats=source_stats,
            )

        for source in self.sources:
            if stop_event.is_set():
                interrupted = True
                break
            stat = source_stats[source.name]
            effective_max_pages = source.max_pages

            for page in range(1, source.max_pages + 1):
                if stop_event.is_set():
                    interrupted = True
                    break
                if page > effective_max_pages:
                    break

                response = await self.tmdb_client.fetch_source_page(source, page)
                pages_scanned += 1
                if not response.ok or not isinstance(response.data, dict):
                    stat["errors"] += 1
                    LOGGER.warning(
                        "[TMDB] %s page %s failed: %s %s",
                        source.name,
                        page,
                        response.status,
                        response.text[:240],
                    )
                    publish_scan_progress(source, current_page=page)
                    continue

                total_page_value = parse_int(response.data.get("total_pages")) or 1
                effective_max_pages = max(1, min(total_page_value, 500, source.max_pages))
                discovered_pages = max(1, min(total_page_value, 500))
                previous_effective = int(stat["pages_effective"])
                stat["pages_discovered"] = discovered_pages
                stat["pages_effective"] = effective_max_pages
                if stat["pages_effective"] != previous_effective:
                    effective_target_pages += int(stat["pages_effective"]) - previous_effective

                results = response.data.get("results")
                if not isinstance(results, list):
                    publish_scan_progress(source, current_page=page)
                    continue

                stat["pages_fetched"] += 1
                stat["raw_seen"] += len(results)
                raw_seen_total += len(results)
                if stat["first_page"] == 0:
                    stat["first_page"] = page
                stat["last_page"] = page

                for item in results:
                    if not isinstance(item, dict):
                        continue

                    tmdb_id = parse_int(item.get("id"))
                    if tmdb_id is None:
                        continue

                    media_type = source.media_type_hint
                    if media_type is None:
                        item_media = str(item.get("media_type") or "").strip().lower()
                        if item_media not in {"movie", "tv"}:
                            stat["unsupported"] += 1
                            continue
                        media_type = item_media

                    key = (media_type, tmdb_id)
                    if key in seen:
                        stat["duplicates"] += 1
                        continue
                    seen.add(key)

                    should_harvest = self.db.should_harvest(
                        tmdb_id=tmdb_id,
                        media_type=media_type,
                        now_ts=now_ts,
                        ratings_ttl_seconds=self.ratings_ttl_seconds,
                        harvest_mode=self.harvest_mode,
                    )
                    if not should_harvest:
                        stat["skipped"] += 1
                        continue

                    title = (
                        str(item.get("title") or "").strip()
                        if media_type == "movie"
                        else str(item.get("name") or "").strip()
                    )
                    popularity = 0.0
                    try:
                        popularity = float(item.get("popularity") or 0.0)
                    except (TypeError, ValueError):
                        popularity = 0.0

                    candidates.append(
                        Candidate(
                            tmdb_id=tmdb_id,
                            media_type=media_type,
                            title=title or f"TMDB-{tmdb_id}",
                            popularity=popularity,
                        )
                    )
                    stat["added"] += 1

                if pages_scanned == 1 or pages_scanned % 25 == 0:
                    eligible_rate = (
                        (len(candidates) / raw_seen_total) * 100.0
                        if raw_seen_total > 0
                        else 0.0
                    )
                    LOGGER.debug(
                        "[Harvester] TMDB scan progress: pages=%s/%s selected=%s raw_seen=%s eligible_rate=%.2f%% current_source=%s page=%s/%s",
                        pages_scanned,
                        max(pages_scanned, effective_target_pages),
                        len(candidates),
                        raw_seen_total,
                        eligible_rate,
                        source.name,
                        page,
                        stat["pages_effective"],
                    )
                publish_scan_progress(source, current_page=page)
            if interrupted:
                break

        cycle_finished_ts = now_epoch()
        cycle_metrics = {
            "started_at": cycle_started_ts,
            "finished_at": cycle_finished_ts,
            "pages_scanned": pages_scanned,
            "max_pages_target_configured": configured_target_pages,
            "max_pages_target_effective": max(pages_scanned, effective_target_pages),
            "selected_candidates": len(candidates),
            "raw_seen": raw_seen_total,
            "harvest_mode": self.harvest_mode,
            "interrupted": interrupted,
            "source_order": [source.name for source in self.sources],
            "sources": source_stats,
        }
        self.db.set_state("tmdb_cycle_metrics", json.dumps(cycle_metrics))

        if self.status is not None:
            self.status.complete_scan(
                selected=len(candidates),
                raw_seen=raw_seen_total,
                scanned_pages=pages_scanned,
                total_pages=max(pages_scanned, effective_target_pages),
                source_stats=source_stats,
            )

        eligible_rate = (
            (len(candidates) / raw_seen_total) * 100.0 if raw_seen_total > 0 else 0.0
        )
        LOGGER.debug(
            "[Harvester] Selected %s candidates from configured TMDB sources (pages=%s/%s raw_seen=%s eligible_rate=%.2f%% mode=%s).",
            len(candidates),
            pages_scanned,
            max(pages_scanned, effective_target_pages),
            raw_seen_total,
            eligible_rate,
            self.harvest_mode,
        )
        return candidates, source_stats, interrupted

    async def _fetch_tmdb_details(
        self, candidates: Sequence[Candidate], stop_event: asyncio.Event
    ) -> Tuple[Dict[Tuple[str, int], Optional[Dict[str, Any]]], bool]:
        details: Dict[Tuple[str, int], Optional[Dict[str, Any]]] = {}
        progress_lock = asyncio.Lock()
        queue: asyncio.Queue[Candidate] = asyncio.Queue()
        for candidate in candidates:
            queue.put_nowait(candidate)
        completed = 0
        succeeded = 0
        failed = 0
        total = len(candidates)
        started_ts = now_epoch()
        progress_every = max(250, min(2000, total // 20 if total > 0 else 250))

        if self.status is not None:
            self.status.begin_tmdb_details(total=total)

        async def worker() -> None:
            nonlocal completed, succeeded, failed
            while True:
                if stop_event.is_set():
                    return
                try:
                    candidate = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return

                success = False
                try:
                    response = await self.tmdb_client.fetch_details(
                        candidate.media_type, candidate.tmdb_id
                    )
                    key = (candidate.media_type, candidate.tmdb_id)
                    if response.ok and isinstance(response.data, dict):
                        details[key] = response.data
                        success = True
                    else:
                        details[key] = None
                        LOGGER.warning(
                            "[TMDB] details failed for %s %s (%s): %s",
                            candidate.media_type,
                            candidate.tmdb_id,
                            response.status,
                            response.text[:240],
                        )
                except Exception as exc:
                    details[(candidate.media_type, candidate.tmdb_id)] = None
                    LOGGER.warning(
                        "[TMDB] details failed for %s %s: %s",
                        candidate.media_type,
                        candidate.tmdb_id,
                        exc,
                    )
                finally:
                    queue.task_done()

                async with progress_lock:
                    completed += 1
                    if success:
                        succeeded += 1
                    else:
                        failed += 1

                    if (
                        completed == 1
                        or completed == total
                        or completed % progress_every == 0
                    ):
                        elapsed = max(1, now_epoch() - started_ts)
                        rps = completed / elapsed
                        LOGGER.debug(
                            "[Harvester] TMDB details progress: %s/%s (ok=%s, failed=%s, ~%.2f items/s)",
                            completed,
                            total,
                            succeeded,
                            failed,
                            rps,
                        )
                    if self.status is not None:
                        self.status.update_tmdb_details(
                            completed=completed,
                            succeeded=succeeded,
                            failed=failed,
                        )

        worker_count = max(1, min(self.details_concurrency, total if total > 0 else 1))
        workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
        await asyncio.gather(*workers)
        interrupted = stop_event.is_set() and completed < total
        return details, interrupted

    def _submitter_queue_is_empty(self) -> bool:
        queue_counts = self.db.queue_counts()
        return (
            queue_counts["ratings_pending"] == 0
            and queue_counts["ratings_in_flight"] == 0
            and queue_counts["mappings_pending"] == 0
            and queue_counts["mappings_in_flight"] == 0
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        cycle_attempted = 0
        while not stop_event.is_set():
            cycle_start = now_epoch()
            cycle_attempted += 1
            cycle_result: Optional[HarvestCycleResult] = None
            cycle_failed = False
            try:
                cycle_result = await self._run_cycle(stop_event)
            except Exception:
                cycle_failed = True
                LOGGER.exception("[Harvester] Unexpected cycle failure")

            if stop_event.is_set():
                LOGGER.info(
                    "[Harvester] Stop requested; exiting after cycle %s.",
                    cycle_attempted,
                )
                break

            if self.run_mode == "once":
                LOGGER.info("[Harvester] run_mode=once: stopping after cycle %s.", cycle_attempted)
                stop_event.set()
                break

            if self.run_mode == "until_idle":
                if cycle_failed or cycle_result is None:
                    LOGGER.info(
                        "[Harvester] run_mode=until_idle: cycle %s failed, continuing.",
                        cycle_attempted,
                    )
                else:
                    has_upstream_failures = (
                        cycle_result.tmdb_list_request_errors > 0
                        or cycle_result.mdblist_request_failures > 0
                    )
                    if cycle_result.interrupted:
                        LOGGER.info(
                            "[Harvester] run_mode=until_idle: cycle %s interrupted, continuing.",
                            cycle_attempted,
                        )
                        has_upstream_failures = True
                    queue_empty = self._submitter_queue_is_empty()
                    is_idle = (
                        cycle_result.selected_candidates == 0
                        and queue_empty
                        and not has_upstream_failures
                    )
                    if is_idle:
                        LOGGER.info(
                            "[Harvester] run_mode=until_idle: idle reached on cycle %s (selected=0, submitter queues empty, no upstream request failures). Stopping.",
                            cycle_attempted,
                        )
                        stop_event.set()
                        break
                    LOGGER.debug(
                        "[Harvester] run_mode=until_idle check after cycle %s: selected=%s queue_empty=%s tmdb_list_errors=%s mdblist_failures=%s -> continuing.",
                        cycle_attempted,
                        cycle_result.selected_candidates,
                        queue_empty,
                        cycle_result.tmdb_list_request_errors,
                        cycle_result.mdblist_request_failures,
                    )

            elapsed = now_epoch() - cycle_start
            sleep_for = max(1, self.cycle_sleep_seconds - elapsed)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=sleep_for)
            except asyncio.TimeoutError:
                pass

    async def _run_cycle(self, stop_event: asyncio.Event) -> HarvestCycleResult:
        resumed_from_checkpoint = False
        tmdb_list_request_errors = 0
        source_stats: Dict[str, Dict[str, int]] = {}
        candidates: List[Candidate] = []

        checkpoint_payload = self._load_checkpoint()
        if checkpoint_payload is not None:
            (
                restored_candidates,
                restored_source_stats,
                restored_tmdb_errors,
                restored_phase,
            ) = self._restore_checkpoint_candidates(checkpoint_payload)
            if restored_candidates:
                resumed_from_checkpoint = True
                candidates = restored_candidates
                source_stats = restored_source_stats
                tmdb_list_request_errors = restored_tmdb_errors
                LOGGER.info(
                    "[Harvester] Resuming checkpoint: phase=%s pending_candidates=%s.",
                    restored_phase,
                    len(candidates),
                )
                if self.status is not None:
                    self.status.begin_cycle(sources=self.sources)
                    resumed_raw_seen = sum(
                        int((s or {}).get("raw_seen") or 0)
                        for s in source_stats.values()
                    )
                    resumed_total_pages = sum(
                        int((s or {}).get("pages_effective") or 0)
                        for s in source_stats.values()
                    )
                    self.status.complete_scan(
                        selected=len(candidates),
                        raw_seen=max(resumed_raw_seen, len(candidates)),
                        scanned_pages=0,
                        total_pages=max(1, resumed_total_pages),
                        source_stats=source_stats or None,
                    )
            else:
                LOGGER.info("[Harvester] Dropping empty/stale checkpoint.")
                self._clear_checkpoint()

        if not resumed_from_checkpoint:
            LOGGER.debug("[Harvester] Cycle start: collecting TMDB candidates.")
            candidates, source_stats, scan_interrupted = await self._collect_candidates(stop_event)
            tmdb_list_request_errors = sum(
                int(stat.get("errors") or 0) for stat in source_stats.values()
            )
            if scan_interrupted:
                if candidates:
                    self._save_checkpoint(
                        phase="tmdb_details_pending",
                        candidates=candidates,
                        source_stats=source_stats,
                        tmdb_list_request_errors=tmdb_list_request_errors,
                        note="stopped during tmdb scan",
                    )
                LOGGER.info(
                    "[Harvester] Stop requested during TMDB scan. Checkpointed %s candidate(s).",
                    len(candidates),
                )
                return HarvestCycleResult(
                    selected_candidates=len(candidates),
                    tmdb_list_request_errors=tmdb_list_request_errors,
                    mdblist_request_failures=0,
                    interrupted=True,
                    resumed_from_checkpoint=False,
                )

        LOGGER.info(
            "[Harvester] Cycle completed: collected %s TMDB candidates.",
            len(candidates),
        )
        if not candidates:
            LOGGER.info("[Harvester] No candidates eligible this cycle.")
            self._clear_checkpoint()
            return HarvestCycleResult(
                selected_candidates=0,
                tmdb_list_request_errors=tmdb_list_request_errors,
                mdblist_request_failures=0,
                interrupted=False,
                resumed_from_checkpoint=resumed_from_checkpoint,
            )

        self._save_checkpoint(
            phase="tmdb_details_pending",
            candidates=candidates,
            source_stats=source_stats,
            tmdb_list_request_errors=tmdb_list_request_errors,
            note="tmdb scan complete",
        )
        LOGGER.info(
            "[Harvester] Fetching start: collecting TMDB details."
        )
        LOGGER.debug(
            "[Harvester] Fetching TMDB details for %s candidates (concurrency=%s).",
            len(candidates),
            self.details_concurrency,
        )
        tmdb_details, details_interrupted = await self._fetch_tmdb_details(
            candidates,
            stop_event,
        )
        if details_interrupted or stop_event.is_set():
            self._save_checkpoint(
                phase="tmdb_details_pending",
                candidates=candidates,
                source_stats=source_stats,
                tmdb_list_request_errors=tmdb_list_request_errors,
                note="stopped during tmdb details",
            )
            LOGGER.info(
                "[Harvester] Stop requested during TMDB details. Checkpoint remains for %s candidate(s).",
                len(candidates),
            )
            return HarvestCycleResult(
                selected_candidates=len(candidates),
                tmdb_list_request_errors=tmdb_list_request_errors,
                mdblist_request_failures=0,
                interrupted=True,
                resumed_from_checkpoint=resumed_from_checkpoint,
            )

        self._save_checkpoint(
            phase="mdblist_pending",
            candidates=candidates,
            source_stats=source_stats,
            tmdb_list_request_errors=tmdb_list_request_errors,
            note="tmdb details complete",
        )
        LOGGER.info(
            "[Harvester] Fetching start: collecting MDBList details."
        )
        LOGGER.debug(
            "[Harvester] TMDB details phase complete. Starting MDBList enrichment for %s candidates.",
            len(candidates),
        )

        movie_ids_total = len({c.tmdb_id for c in candidates if c.media_type == "movie"})
        tv_ids_total = len({c.tmdb_id for c in candidates if c.media_type == "tv"})
        movie_batches_total = (
            math.ceil(movie_ids_total / self.mdblist_client.batch_size)
            if movie_ids_total > 0
            else 0
        )
        tv_batches_total = (
            math.ceil(tv_ids_total / self.mdblist_client.batch_size)
            if tv_ids_total > 0
            else 0
        )
        if self.status is not None:
            self.status.begin_mdblist(
                movie_ids_total=movie_ids_total,
                movie_batches_total=movie_batches_total,
                tv_ids_total=tv_ids_total,
                tv_batches_total=tv_batches_total,
                enrich_total=len(candidates),
            )

        candidate_lookup: Dict[Tuple[str, int], Candidate] = {
            (candidate.media_type, candidate.tmdb_id): candidate for candidate in candidates
        }
        enriched = 0
        enriched_mdblist_ok = 0
        enriched_mdblist_miss = 0
        enriched_tmdb_only = 0
        queue_added_ratings = 0
        queue_added_mappings = 0
        persisted_keys: Set[Tuple[str, int]] = set()
        queue_log_every = max(100, min(2000, len(candidates) // 10 or 100))

        def on_mdblist_chunk(
            media_type: str,
            chunk_ids: Sequence[int],
            chunk_results: Dict[int, Dict[str, Any]],
            chunk_attempted: Set[int],
            chunk_index: int,
            total_chunks: int,
        ) -> None:
            nonlocal enriched
            nonlocal enriched_mdblist_ok
            nonlocal enriched_mdblist_miss
            nonlocal enriched_tmdb_only
            nonlocal queue_added_ratings
            nonlocal queue_added_mappings
            now_ts = now_epoch()
            for tmdb_id in chunk_ids:
                key = (media_type, tmdb_id)
                if key in persisted_keys:
                    continue
                candidate = candidate_lookup.get(key)
                if candidate is None:
                    continue
                (
                    mdblist_ok_inc,
                    mdblist_miss_inc,
                    tmdb_only_inc,
                    queue_r_inc,
                    queue_m_inc,
                ) = self._save_candidate_enrichment(
                    candidate=candidate,
                    details=tmdb_details.get(key),
                    md_item=chunk_results.get(tmdb_id),
                    now_ts=now_ts,
                )
                persisted_keys.add(key)
                enriched += 1
                enriched_mdblist_ok += mdblist_ok_inc
                enriched_mdblist_miss += mdblist_miss_inc
                enriched_tmdb_only += tmdb_only_inc
                queue_added_ratings += queue_r_inc
                queue_added_mappings += queue_m_inc

            if (
                chunk_index == 1
                or chunk_index == total_chunks
                or enriched % queue_log_every == 0
            ):
                queue_counts = self.db.queue_counts()
                LOGGER.debug(
                    "[Harvester] Persist progress: enriched=%s/%s queue(ratings=%s mappings=%s).",
                    enriched,
                    len(candidates),
                    queue_counts["ratings_pending"],
                    queue_counts["mappings_pending"],
                )
            if self.status is not None:
                self.status.update_mdblist_chunk(
                    media_type=media_type,
                    chunk_index=chunk_index,
                    attempted_count=len(chunk_attempted),
                    found_count=len(chunk_results),
                )
                self.status.update_enriched(
                    enriched=enriched,
                    total=len(candidates),
                    mdblist_ok=enriched_mdblist_ok,
                    mdblist_miss=enriched_mdblist_miss,
                    tmdb_only=enriched_tmdb_only,
                    queue_ratings=queue_added_ratings,
                    queue_mappings=queue_added_mappings,
                )

        (
            mdblist_data,
            _,
            mdblist_all_success,
            mdblist_halted,
            mdblist_halt_reason,
            mdblist_failed_batches,
        ) = (
            await self.mdblist_client.fetch_for_candidates(
                candidates,
                on_chunk=on_mdblist_chunk,
                stop_event=stop_event,
            )
        )
        if self.status is not None:
            self.status.set_mdblist_failed_batches(
                movie_failed=int(mdblist_failed_batches.get("movie") or 0),
                tv_failed=int(mdblist_failed_batches.get("tv") or 0),
            )

        if mdblist_halted or stop_event.is_set():
            remaining_candidates = [
                candidate
                for candidate in candidates
                if (candidate.media_type, candidate.tmdb_id) not in persisted_keys
            ]
            if remaining_candidates:
                self._save_checkpoint(
                    phase="mdblist_pending",
                    candidates=remaining_candidates,
                    source_stats=source_stats,
                    tmdb_list_request_errors=tmdb_list_request_errors,
                    note=mdblist_halt_reason or "stopped during mdblist enrichment",
                )
            else:
                self._clear_checkpoint()
            LOGGER.info(
                "[Harvester] MDBList enrichment paused (%s). Deferred %s candidate(s) for next run.",
                mdblist_halt_reason or "interrupted",
                len(remaining_candidates),
            )
            if self.status is not None:
                self.status.complete_enrichment(
                    enriched=enriched,
                    mdblist_ok=enriched_mdblist_ok,
                    mdblist_miss=enriched_mdblist_miss,
                    tmdb_only=enriched_tmdb_only,
                    queue_ratings=queue_added_ratings,
                    queue_mappings=queue_added_mappings,
                )
            return HarvestCycleResult(
                selected_candidates=len(candidates),
                tmdb_list_request_errors=tmdb_list_request_errors,
                mdblist_request_failures=1,
                interrupted=bool(stop_event.is_set()),
                resumed_from_checkpoint=resumed_from_checkpoint,
            )

        # Safety net: ensure every candidate is persisted, even if a chunk callback
        # did not fire (for example due to unexpected mid-cycle failures).
        if len(persisted_keys) < len(candidates):
            LOGGER.warning(
                "[Harvester] Missing persisted items after chunking (%s/%s). Backfilling.",
                len(candidates) - len(persisted_keys),
                len(candidates),
            )
            now_ts = now_epoch()
            for candidate in candidates:
                key = (candidate.media_type, candidate.tmdb_id)
                if key in persisted_keys:
                    continue
                (
                    mdblist_ok_inc,
                    mdblist_miss_inc,
                    tmdb_only_inc,
                    queue_r_inc,
                    queue_m_inc,
                ) = self._save_candidate_enrichment(
                    candidate=candidate,
                    details=tmdb_details.get(key),
                    md_item=mdblist_data.get(key),
                    now_ts=now_ts,
                )
                enriched += 1
                enriched_mdblist_ok += mdblist_ok_inc
                enriched_mdblist_miss += mdblist_miss_inc
                enriched_tmdb_only += tmdb_only_inc
                queue_added_ratings += queue_r_inc
                queue_added_mappings += queue_m_inc
                persisted_keys.add(key)

        queue_counts = self.db.queue_counts()
        LOGGER.debug(
            "[Harvester] Enriched %s titles. Queue: ratings=%s mappings=%s (in-flight r=%s, m=%s, failed r=%s, m=%s)",
            enriched,
            queue_counts["ratings_pending"],
            queue_counts["mappings_pending"],
            queue_counts["ratings_in_flight"],
            queue_counts["mappings_in_flight"],
            queue_counts["ratings_failed"],
            queue_counts["mappings_failed"],
        )
        if self.status is not None:
            self.status.complete_enrichment(
                enriched=enriched,
                mdblist_ok=enriched_mdblist_ok,
                mdblist_miss=enriched_mdblist_miss,
                tmdb_only=enriched_tmdb_only,
                queue_ratings=queue_added_ratings,
                queue_mappings=queue_added_mappings,
            )
        self._clear_checkpoint()
        LOGGER.info("[Harvester] Cycle completed: enriched %s titles.", enriched)
        return HarvestCycleResult(
            selected_candidates=len(candidates),
            tmdb_list_request_errors=tmdb_list_request_errors,
            mdblist_request_failures=0 if mdblist_all_success else 1,
            interrupted=False,
            resumed_from_checkpoint=resumed_from_checkpoint,
        )


class Submitter:
    def __init__(
        self,
        *,
        config: Dict[str, Any],
        db: LocalDatabase,
        pmdb_client: PMDBClient,
        status: Optional[RuntimeDashboard] = None,
    ):
        self.config = config
        self.db = db
        self.pmdb_client = pmdb_client
        self.status = status
        self.poll_seconds = max(
            0.01, float(config["runtime"]["submitter_poll_seconds"])
        )
        self.worker_count = max(1, int(config["runtime"].get("submitter_workers", 8)))
        self.in_flight_lease_seconds = max(
            30, int(config["runtime"].get("submitter_in_flight_lease_seconds", 300))
        )
        self.max_retry_attempts = max(
            1, int(config["runtime"].get("submitter_max_retry_attempts", 12))
        )
        self.lease_recovery_interval = max(
            10.0, min(120.0, float(self.in_flight_lease_seconds) / 2.0)
        )
        self._verify_after_transient_statuses = {0, 500, 502, 503, 504}
        if self.status is not None:
            self.status.set_submitter_workers(self.worker_count)

    @staticmethod
    def _format_pmdb_error(
        *,
        endpoint_hint: str,
        result: PMDBSubmitResult,
    ) -> str:
        payload = {
            "service": "pmdb",
            "endpoint": result.endpoint or endpoint_hint,
            "status": int(result.status_code or 0),
            "code": (result.error_code or "unknown")[:80],
            "retryable": bool(result.retryable),
            "message": " ".join(str(result.error_text or "").split())[:320],
        }
        return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))

    @staticmethod
    def _format_manual_error(
        *,
        endpoint: str,
        status: int,
        code: str,
        retryable: bool,
        message: str,
    ) -> str:
        payload = {
            "service": "pmdb",
            "endpoint": endpoint,
            "status": int(status),
            "code": str(code)[:80],
            "retryable": bool(retryable),
            "message": " ".join(str(message).split())[:320],
        }
        return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))

    def _retry_delay_seconds(self, result: PMDBSubmitResult, current_attempts: int) -> int:
        attempt_index = max(1, int(current_attempts) + 1)
        base = max(5, int(result.retry_after_seconds or 30))
        multiplier = min(2 ** max(0, attempt_index - 1), 64)
        delay = min(base * multiplier, 6 * 3600)
        return max(5, int(delay))

    def _run_one_time_retry_storm_cleanup(self) -> None:
        cleanup_key = f"{RETRY_STORM_CLEANUP_KEY}:{self.max_retry_attempts}"
        existing = self.db.get_state(cleanup_key)
        if existing:
            return
        affected = self.db.cleanup_retry_storm_rows(self.max_retry_attempts)
        payload = {
            "ran_at": now_epoch(),
            "max_retry_attempts": self.max_retry_attempts,
            "ratings_failed": int(affected["ratings"]),
            "mappings_failed": int(affected["mappings"]),
        }
        self.db.set_state(cleanup_key, json.dumps(payload, separators=(",", ":")))
        if affected["ratings"] > 0 or affected["mappings"] > 0:
            LOGGER.warning(
                "[Submitter] One-time retry-storm cleanup marked rows failed (ratings=%s mappings=%s, threshold=%s).",
                affected["ratings"],
                affected["mappings"],
                self.max_retry_attempts,
            )
        else:
            LOGGER.info("[Submitter] One-time retry-storm cleanup: no rows matched.")

    async def run(self, stop_event: asyncio.Event) -> None:
        self._run_one_time_retry_storm_cleanup()
        self.db.recover_in_flight_rows()
        LOGGER.info("[Submitter] Startup recovery: moved stale in_flight rows to retry.")
        LOGGER.debug("[Submitter] Starting %s worker(s).", self.worker_count)

        workers = [
            asyncio.create_task(self._worker_loop(stop_event, i + 1))
            for i in range(self.worker_count)
        ]
        lease_recovery_task = asyncio.create_task(
            self._lease_recovery_loop(stop_event),
            name="submitter_lease_recovery",
        )
        stop_waiter = asyncio.create_task(stop_event.wait())

        done, pending = await asyncio.wait(
            [*workers, lease_recovery_task, stop_waiter],
            return_when=asyncio.FIRST_COMPLETED,
        )

        worker_errors: List[Exception] = []
        for task in done:
            if task is stop_waiter:
                continue
            exc = task.exception()
            if exc is not None:
                worker_errors.append(exc)

        if worker_errors:
            stop_event.set()

        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

        # Ensure claimed rows are not stranded after cancellation/shutdown.
        self.db.recover_in_flight_rows()
        LOGGER.info("[Submitter] Shutdown recovery: moved in_flight rows to retry.")

        if worker_errors:
            raise worker_errors[0]

        LOGGER.info("[Submitter] Stopped.")

    async def _lease_recovery_loop(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self.lease_recovery_interval,
                )
                break
            except asyncio.TimeoutError:
                pass

            recovered = self.db.recover_stale_in_flight_rows(self.in_flight_lease_seconds)
            if recovered["ratings"] > 0 or recovered["mappings"] > 0:
                LOGGER.warning(
                    "[Submitter] Recovered stale in_flight rows (ratings=%s mappings=%s, lease=%ss).",
                    recovered["ratings"],
                    recovered["mappings"],
                    self.in_flight_lease_seconds,
                )

    async def _worker_loop(self, stop_event: asyncio.Event, _worker_id: int) -> None:
        while not stop_event.is_set():
            did_work = False

            now_ts = now_epoch()
            mapping_row = self.db.claim_next_pending_mapping(now_ts)
            if mapping_row is not None:
                did_work = True
                await self._submit_mapping(mapping_row)

            now_ts = now_epoch()
            rating_row = self.db.claim_next_pending_rating(now_ts)
            if rating_row is not None:
                did_work = True
                await self._submit_rating(rating_row)

            if not did_work:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self.poll_seconds)
                except asyncio.TimeoutError:
                    pass

    async def _submit_mapping(self, row: sqlite3.Row) -> None:
        tmdb_id = int(row["tmdb_id"])
        media_type = str(row["media_type"])
        id_type = str(row["id_type"])
        id_value = str(row["id_value"])
        pmdb_item_id = first_non_empty(row["pmdb_item_id"])
        attempts = int(row["pmdb_attempts"] or 0)

        result = await self.pmdb_client.submit_mapping(
            tmdb_id=tmdb_id,
            media_type=media_type,
            id_type=id_type,
            id_value=id_value,
            existing_pmdb_item_id=pmdb_item_id,
        )

        if result.success:
            self.db.mark_mapping_submitted(
                tmdb_id,
                media_type,
                id_type,
                now_epoch(),
                pmdb_item_id=result.item_id,
            )
            if self.status is not None:
                self.status.record_submit_result(kind="mapping", outcome="success")
            if result.duplicate_or_exists:
                LOGGER.debug(
                    "[Submitter] Mapping already exists: %s %s %s=%s",
                    media_type,
                    tmdb_id,
                    id_type,
                    id_value,
                )
            else:
                LOGGER.debug(
                    "[Submitter] Mapping submitted: %s %s %s=%s",
                    media_type,
                    tmdb_id,
                    id_type,
                    id_value,
                )
            return

        if result.retryable:
            if int(result.status_code or 0) in self._verify_after_transient_statuses:
                found_existing, found_item_id = await self.pmdb_client.confirm_mapping_exists(
                    tmdb_id=tmdb_id,
                    media_type=media_type,
                    id_type=id_type,
                    id_value=id_value,
                )
                if found_existing:
                    self.db.mark_mapping_submitted(
                        tmdb_id,
                        media_type,
                        id_type,
                        now_epoch(),
                        pmdb_item_id=found_item_id or pmdb_item_id,
                    )
                    if self.status is not None:
                        self.status.record_submit_result(kind="mapping", outcome="success")
                    LOGGER.info(
                        "[Submitter] Mapping confirmed remote after transient failure: %s %s %s=%s",
                        media_type,
                        tmdb_id,
                        id_type,
                        id_value,
                    )
                    return

            if attempts + 1 >= self.max_retry_attempts:
                self.db.mark_mapping_failed(
                    tmdb_id,
                    media_type,
                    id_type,
                    self._format_manual_error(
                        endpoint=result.endpoint or "/api/external/mappings",
                        status=int(result.status_code or 0),
                        code="max_retry_attempts_exceeded",
                        retryable=False,
                        message=(
                            f"Exceeded max retry attempts ({self.max_retry_attempts}) "
                            f"for mapping submission."
                        ),
                    ),
                )
                if self.status is not None:
                    self.status.record_submit_result(
                        kind="mapping",
                        outcome="failed",
                        error_text=result.error_text,
                    )
                LOGGER.error(
                    "[Submitter] Mapping failed permanently after max attempts (%s): %s %s %s=%s",
                    self.max_retry_attempts,
                    media_type,
                    tmdb_id,
                    id_type,
                    id_value,
                )
                return

            retry_delay = self._retry_delay_seconds(result, attempts)
            retry_at = now_epoch() + retry_delay
            error_payload = self._format_pmdb_error(
                endpoint_hint="/api/external/mappings",
                result=result,
            )
            self.db.mark_mapping_retry(
                tmdb_id,
                media_type,
                id_type,
                retry_at,
                error_payload,
            )
            if self.status is not None:
                self.status.record_submit_result(
                    kind="mapping",
                    outcome="retry",
                    error_text=result.error_text,
                )
            LOGGER.warning(
                "[Submitter] Mapping retry scheduled in %ss: %s %s %s=%s",
                retry_delay,
                media_type,
                tmdb_id,
                id_type,
                id_value,
            )
            return

        self.db.mark_mapping_failed(
            tmdb_id,
            media_type,
            id_type,
            self._format_pmdb_error(
                endpoint_hint="/api/external/mappings",
                result=result,
            ),
        )
        if self.status is not None:
            self.status.record_submit_result(
                kind="mapping",
                outcome="failed",
                error_text=result.error_text,
            )
        LOGGER.error(
            "[Submitter] Mapping failed permanently: %s %s %s=%s (%s)",
            media_type,
            tmdb_id,
            id_type,
            id_value,
            result.error_text[:200],
        )

    async def _submit_rating(self, row: sqlite3.Row) -> None:
        tmdb_id = int(row["tmdb_id"])
        media_type = str(row["media_type"])
        label = str(row["label"])
        score = float(row["score"])
        pmdb_item_id = first_non_empty(row["pmdb_item_id"])
        attempts = int(row["pmdb_attempts"] or 0)

        result = await self.pmdb_client.submit_rating(
            tmdb_id=tmdb_id,
            media_type=media_type,
            label=label,
            score=score,
            existing_pmdb_item_id=pmdb_item_id,
        )

        if result.success:
            self.db.mark_rating_submitted(
                tmdb_id,
                media_type,
                label,
                now_epoch(),
                pmdb_item_id=result.item_id,
            )
            if self.status is not None:
                self.status.record_submit_result(kind="rating", outcome="success")
            if result.duplicate_or_exists:
                LOGGER.debug(
                    "[Submitter] Rating already exists: %s %s %s=%.1f",
                    media_type,
                    tmdb_id,
                    label,
                    score,
                )
            else:
                LOGGER.debug(
                    "[Submitter] Rating submitted: %s %s %s=%.1f",
                    media_type,
                    tmdb_id,
                    label,
                    score,
                )
            return

        if result.retryable:
            if int(result.status_code or 0) in self._verify_after_transient_statuses:
                found_existing, found_item_id = await self.pmdb_client.confirm_rating_exists(
                    tmdb_id=tmdb_id,
                    media_type=media_type,
                    label=label,
                    score=score,
                )
                if found_existing:
                    self.db.mark_rating_submitted(
                        tmdb_id,
                        media_type,
                        label,
                        now_epoch(),
                        pmdb_item_id=found_item_id or pmdb_item_id,
                    )
                    if self.status is not None:
                        self.status.record_submit_result(kind="rating", outcome="success")
                    LOGGER.info(
                        "[Submitter] Rating confirmed remote after transient failure: %s %s %s=%.1f",
                        media_type,
                        tmdb_id,
                        label,
                        score,
                    )
                    return

            if attempts + 1 >= self.max_retry_attempts:
                self.db.mark_rating_failed(
                    tmdb_id,
                    media_type,
                    label,
                    self._format_manual_error(
                        endpoint=result.endpoint or "/api/external/ratings",
                        status=int(result.status_code or 0),
                        code="max_retry_attempts_exceeded",
                        retryable=False,
                        message=(
                            f"Exceeded max retry attempts ({self.max_retry_attempts}) "
                            f"for rating submission."
                        ),
                    ),
                )
                if self.status is not None:
                    self.status.record_submit_result(
                        kind="rating",
                        outcome="failed",
                        error_text=result.error_text,
                    )
                LOGGER.error(
                    "[Submitter] Rating failed permanently after max attempts (%s): %s %s %s=%.1f",
                    self.max_retry_attempts,
                    media_type,
                    tmdb_id,
                    label,
                    score,
                )
                return

            retry_delay = self._retry_delay_seconds(result, attempts)
            retry_at = now_epoch() + retry_delay
            error_payload = self._format_pmdb_error(
                endpoint_hint="/api/external/ratings",
                result=result,
            )
            self.db.mark_rating_retry(
                tmdb_id,
                media_type,
                label,
                retry_at,
                error_payload,
            )
            if self.status is not None:
                self.status.record_submit_result(
                    kind="rating",
                    outcome="retry",
                    error_text=result.error_text,
                )
            LOGGER.warning(
                "[Submitter] Rating retry scheduled in %ss: %s %s %s=%.1f",
                retry_delay,
                media_type,
                tmdb_id,
                label,
                score,
            )
            return

        self.db.mark_rating_failed(
            tmdb_id,
            media_type,
            label,
            self._format_pmdb_error(
                endpoint_hint="/api/external/ratings",
                result=result,
            ),
        )
        if self.status is not None:
            self.status.record_submit_result(
                kind="rating",
                outcome="failed",
                error_text=result.error_text,
            )
        LOGGER.error(
            "[Submitter] Rating failed permanently: %s %s %s=%.1f (%s)",
            media_type,
            tmdb_id,
            label,
            score,
            result.error_text[:200],
        )


def configure_logging(config: Dict[str, Any]) -> LoggingRuntime:
    runtime_cfg = config.get("runtime", {})
    level_name = runtime_cfg.get("log_level", "INFO")
    level = getattr(logging, str(level_name).upper(), logging.INFO)

    raw_log_path = Path(str(runtime_cfg.get("log_file_path", "logs/mdblist_pmdb.log"))).expanduser()
    if not raw_log_path.is_absolute():
        raw_log_path = (Path.cwd() / raw_log_path).resolve()
    raw_log_path.parent.mkdir(parents=True, exist_ok=True)

    log_file_max_bytes = max(1024, int(runtime_cfg.get("log_file_max_bytes", 10485760)))
    log_file_backup_count = max(0, int(runtime_cfg.get("log_file_backup_count", 5)))
    console_mode = str(runtime_cfg.get("console_mode", "dashboard")).strip().lower()
    debug_raw_console_logs = bool(runtime_cfg.get("debug_raw_console_logs", False))
    allow_raw_while_live = console_mode == "raw" or (
        level <= logging.DEBUG and debug_raw_console_logs
    )

    event_buffer = DashboardEventBuffer(
        max_lines=int(runtime_cfg.get("dashboard_event_lines", 8)),
        dedupe_window_seconds=int(
            runtime_cfg.get("dashboard_event_dedupe_window_seconds", 30)
        ),
        max_message_length=int(
            runtime_cfg.get("dashboard_event_max_message_length", 160)
        ),
    )
    live_state = LiveLogState()

    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass
    root_logger.setLevel(level)

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = RotatingFileHandler(
        raw_log_path,
        maxBytes=log_file_max_bytes,
        backupCount=log_file_backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    console_handler = LiveAwareConsoleHandler(
        live_state=live_state,
        allow_while_live=allow_raw_while_live,
    )
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    event_handler = DashboardEventHandler(
        buffer=event_buffer,
        min_level=logging.WARNING,
    )

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(event_handler)

    logging.captureWarnings(True)

    # Keep third-party debug noise out of terminal output.
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("py.warnings").setLevel(logging.WARNING)

    return LoggingRuntime(
        live_state=live_state,
        event_buffer=event_buffer,
        log_file_path=raw_log_path,
    )


def chunks(values: Sequence[int], size: int) -> Iterable[Sequence[int]]:
    for i in range(0, len(values), size):
        yield values[i : i + size]


async def run_app(config: Dict[str, Any], logging_runtime: LoggingRuntime) -> None:
    db_path = Path(config["runtime"]["database_path"]).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    db = LocalDatabase(db_path)
    live_active = False

    try:
        tmdb_gate = ServiceGate(
            "tmdb",
            db,
            AsyncWindowLimiter(
                max_requests=int(config["tmdb"]["rate_limit"]["requests"]),
                period_seconds=float(config["tmdb"]["rate_limit"]["per_seconds"]),
                name="tmdb",
            ),
        )

        mdblist_gate = ServiceGate(
            "mdblist",
            db,
            AsyncWindowLimiter(
                max_requests=int(config["mdblist"]["rate_limit"]["requests"]),
                period_seconds=float(config["mdblist"]["rate_limit"]["per_seconds"]),
                name="mdblist",
            ),
        )

        pmdb_api_gate = ServiceGate(
            "pmdb_api",
            db,
            AsyncWindowLimiter(
                max_requests=int(config["pmdb"]["api_rate_limit"]["requests"]),
                period_seconds=float(config["pmdb"]["api_rate_limit"]["per_seconds"]),
                name="pmdb_api",
            ),
        )
        pmdb_rating_gate = ServiceGate(
            "pmdb_ratings",
            db,
            AsyncWindowLimiter(
                max_requests=int(config["pmdb"]["ratings_limit"]["requests"]),
                period_seconds=float(config["pmdb"]["ratings_limit"]["per_seconds"]),
                name="pmdb_ratings",
            ),
        )
        pmdb_mapping_gate = ServiceGate(
            "pmdb_mappings",
            db,
            AsyncWindowLimiter(
                max_requests=int(config["pmdb"]["mappings_limit"]["requests"]),
                period_seconds=float(config["pmdb"]["mappings_limit"]["per_seconds"]),
                name="pmdb_mappings",
            ),
        )

        tmdb_client = TMDBClient(
            api_key=config["api_keys"]["tmdb"],
            config=config["tmdb"],
            gate=tmdb_gate,
        )
        mdblist_client = MDBListClient(
            api_key=config["api_keys"]["mdblist"],
            config=config["mdblist"],
            gate=mdblist_gate,
        )
        pmdb_client = PMDBClient(
            api_key=config["api_keys"]["pmdb"],
            config=config["pmdb"],
            api_gate=pmdb_api_gate,
            rating_gate=pmdb_rating_gate,
            mapping_gate=pmdb_mapping_gate,
        )
        dashboard = RuntimeDashboard(
            db=db,
            submitter_workers=int(config["runtime"].get("submitter_workers", 8)),
            event_buffer=logging_runtime.event_buffer,
            event_lines=int(config["runtime"].get("dashboard_event_lines", 8)),
        )

        harvester = Harvester(
            config=config,
            db=db,
            tmdb_client=tmdb_client,
            mdblist_client=mdblist_client,
            status=dashboard,
        )
        submitter = Submitter(
            config=config,
            db=db,
            pmdb_client=pmdb_client,
            status=dashboard,
        )

        stop_event = asyncio.Event()

        loop = asyncio.get_running_loop()

        def _signal_stop() -> None:
            if not stop_event.is_set():
                LOGGER.info("Stop signal received. Beginning graceful shutdown...")
                stop_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _signal_stop)
            except NotImplementedError:
                # Windows event loops may not support this.
                pass

        LOGGER.info("Database: %s", db_path)
        LOGGER.info("Log file: %s", logging_runtime.log_file_path)
        LOGGER.info(
            "Starting loops: run_mode=%s, harvest_mode=%s, ratings_refresh_days=%s, tmdb_sources=%s, submitter_lease=%ss, submitter_max_retry_attempts=%s",
            config["runtime"]["run_mode"],
            config["runtime"]["harvest_mode"],
            config["runtime"]["ratings_refresh_days"],
            len([s for s in config["tmdb"]["sources"] if s.get("enabled", True)]),
            config["runtime"].get("submitter_in_flight_lease_seconds", 300),
            config["runtime"].get("submitter_max_retry_attempts", 12),
        )
        LOGGER.info(
            "[Submitter] Ready: workers=%s",
            config["runtime"].get("submitter_workers", 8),
        )

        logging_runtime.live_state.set_live_active(True)
        live_active = True
        tasks = [
            asyncio.create_task(dashboard.run(stop_event), name="dashboard"),
            asyncio.create_task(harvester.run(stop_event), name="harvester"),
            asyncio.create_task(submitter.run(stop_event), name="submitter"),
        ]

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

        # If one task failed, propagate the exception after stopping the others.
        for finished in done:
            exc = finished.exception()
            if exc is not None:
                LOGGER.error(
                    "Task %s failed: %s. Initiating shutdown.",
                    finished.get_name(),
                    exc,
                )
                stop_event.set()
                LOGGER.info("Shutdown: draining tasks after failure.")
                for p in pending:
                    p.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                LOGGER.info("Shutdown complete.")
                raise exc

        # Normal shutdown path (run_mode stop or signal).
        LOGGER.info("Shutdown: stop requested, waiting for remaining tasks.")
        for p in pending:
            p.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        LOGGER.info("Shutdown complete.")

    finally:
        if live_active:
            logging_runtime.live_state.set_live_active(False)
        db.close()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Autonomous MDBList -> PMDB sync bot",
    )
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to config JSON file (default: config.json)",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    config_path = Path(args.config).expanduser().resolve()

    try:
        config = load_config(config_path)
    except Exception as exc:
        print(f"[FATAL] Could not load config: {exc}")
        return 1

    logging_runtime = configure_logging(config)

    try:
        asyncio.run(run_app(config, logging_runtime))
        LOGGER.info("Log file: %s", logging_runtime.log_file_path)
    except KeyboardInterrupt:
        LOGGER.info("Interrupted by user")
        LOGGER.info("Log file: %s", logging_runtime.log_file_path)
        return 0
    except Exception:
        LOGGER.exception("Fatal runtime error")
        LOGGER.info("Log file: %s", logging_runtime.log_file_path)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
