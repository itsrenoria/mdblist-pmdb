# mdblist-pmdb

Background worker for syncing and normalizing media metadata between external provider APIs and a local queue-backed datastore.

## Overview

This project runs as an autonomous process that:

- collects candidate items from upstream catalog sources,
- enriches and normalizes metadata,
- queues outbound updates,
- and submits updates using retry-safe delivery.

The implementation intentionally keeps provider-specific integration details internal.

## Usage

1. Prepare local runtime configuration and credentials.
2. Start the worker:

```bash
python3 mdblist_pmdb.py --config config.json
```

3. Stop with `Ctrl+C` (or use configured runtime stop behavior).

## Compliance

Use this software only with authorized API access and in accordance with all applicable provider terms, rate limits, and data usage policies.

You are responsible for validating legal, contractual, and operational compliance for your environment.
