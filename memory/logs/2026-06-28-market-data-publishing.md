# Market Data Publishing

Timestamp: 2026-06-28

Implemented the missing one-time historical market-data publishing path.

Added:
- R2 key deletion support for partition rewrites
- local parquet scan and partition planning for canonical daily stock data
- `market-data-upload` CLI command for local-to-R2 monthly partition publishing
- tests for schema validation, partition listing, and stale partition replacement
- README and operator runbook updates for the new market-data workflow

Verified:
- full test suite passes
- live sample market-data upload rewrites the existing `2026-01` and `2026-02` R2 partitions
- live history reads still return both uploaded dates after the rewrite
