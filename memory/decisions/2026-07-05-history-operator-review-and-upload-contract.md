# History Operator Review And Upload Contract

Date: 2026-07-05

## Decision

- Raw historical fetch exceptions must be operator-reviewed and recorded in the raw fetch manifest instead of being handled by ad hoc scripts.
- The supported review mechanism is `history-manifest-mark`, which updates one manifest row with an explicit status and reason.
- Historical upload should use a combined reviewed raw manifest such as `raw_fetch_ALL.parquet` when uploading combined NSE+BSE history.
- The normal operator path is: fetch per exchange, review/mark exceptions, combine manifests, build combined history, verify, doctor, upload.
- `history-build --clean --exchange X` is exchange-scoped. It must not wipe unrelated exchange partitions in a combined history tree.
- `partition_manifest.parquet` is derived state and may be regenerated from existing partition files with `history-partition-manifest-refresh`.
- GitHub Actions daily paper runs should install pinned Python dependencies from `requirements.lock` and upload paper results by default on scheduled runs.

## Why

- This makes archive-gap acceptance auditable.
- It aligns historical upload with the combined history tree expected by the operator workflow.
- It reduces accidental destructive rebuilds and manifest drift.
- It improves reproducibility for scheduled paper-trading runs.
