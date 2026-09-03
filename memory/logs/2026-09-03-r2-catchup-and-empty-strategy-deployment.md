# R2 Catch-Up And Empty Strategy Deployment

Date: 2026-09-03

## Empty Strategy Deployment

- Daily Actions had failed since 2026-07-13 because `registry/strategies.parquet` was absent from R2.
- Market-data refresh succeeded before paper processing failed, leaving NSE ahead of BSE.
- Missing R2 registry now means zero active strategies and paper/performance processing becomes a no-op.
- Malformed registries, permission failures, and missing artifacts for explicitly active strategies still fail.

## Market-Data Catch-Up

- Fetched 2026-07-01 through 2026-09-02 for NSE and BSE, reusing 20 existing July files and downloading 26 files per exchange.
- Merged the fetch manifests and rebuilt only the affected July, August, and September partitions with `history-build --incremental --from-manifest`.
- Partition-wise verification passed across 620 partitions and 27,742,872 rows with no duplicate keys or invalid OHLC rows.
- Local and R2 coverage now ends on 2026-09-02:
  - NSE: 11,858,406 rows, 383 partitions.
  - BSE: 15,884,466 rows, 237 partitions.
- Guarded history upload promoted four stale or missing partitions: BSE July/August/September and NSE August.
- Post-upload `r2-sync-check` and `history-doctor --compare-r2` returned `ok` for both exchanges.
