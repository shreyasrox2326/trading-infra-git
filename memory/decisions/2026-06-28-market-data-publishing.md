# Market Data Publishing

Date: 2026-06-28

## Decisions

- Use `python -m trading_infra market-data-upload` as the canonical local-to-R2 market-data publishing path.
- Treat local canonical Parquet files as the heavy-compute output and R2 as the persisted source of truth.
- Rewrite market data on R2 by canonical `exchange/year/month` partitions.
- Remove existing parquet objects under each targeted market-data partition prefix before uploading replacement `part.parquet` files.
- Keep strategy and backtest flows R2-first after the one-time historical market-data load.
