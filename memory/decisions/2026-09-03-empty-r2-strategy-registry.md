# Empty R2 Strategy Registry

Date: 2026-09-03

## Decision

- A missing `registry/strategies.parquet` object on R2 represents zero configured strategies.
- R2-backed paper and performance processing must succeed as no-ops in that state.
- The daily market-data refresh remains independent and continues to publish market data.
- A registry object that exists but is malformed remains an error.
- Missing artifacts for a strategy explicitly marked active remain an error.

## Reason

Market-data operation must not depend on strategies being deployed. An empty strategy deployment is a valid operating state, while configuration corruption and incomplete active deployments still require operator attention.
