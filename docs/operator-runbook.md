# Operator Runbook

This runbook covers the steps required to bring the infrastructure to a usable README-ready state.

## 1. Cloudflare R2 Setup

Create a private R2 bucket for the project.

Create access credentials with read/write access to that bucket.

Record these values:

- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_S3_API`
- `R2_BUCKET_NAME`

Upload or sync the initial market-data layout:

```text
data/daily_stock_data/exchange=NSE/year=YYYY/month=MM/*.parquet
```

For one-time historical setup from local canonical parquet files, use:

```bash
python -m trading_infra market-data-upload \
  --path /workspaces/code/trading-infra-git/data/import/full_history.parquet
```

The command validates the canonical market-data schema, rewrites monthly `exchange/year/month` partitions locally, removes stale parquet objects under each targeted R2 partition prefix, and uploads a single canonical `part.parquet` per partition.

## 2. Local Strategy Preparation

Create a versioned local strategy folder under `strategies/<strategy_id>/` only when preparing a new strategy version for first upload to R2.

Minimum files:

- `config.yaml`
- `metadata.json`

For the current supported strategy type, use the example in `examples/strategies/top_n_adj_close_v1/`.

After the strategy version is uploaded, treat R2 as the canonical source. Local `strategies/`, `registry/`, and `decisions/` directories are workspace/cache state only.

## 3. Local Backtest

Run a backtest locally:

```bash
python -m trading_infra backtest-run \
  --base-path /workspaces/code/trading-infra-git \
  --strategy-id top_n_adj_close_v1 \
  --market-data-path /path/to/local/market.parquet \
  --start-date 2026-01-01 \
  --end-date 2026-01-31
```

Or run the historical backtest directly against R2-backed market data:

```bash
python -m trading_infra backtest-run \
  --base-path /workspaces/code/trading-infra-git \
  --strategy-id top_n_adj_close_v1 \
  --use-r2 \
  --exchange NSE \
  --start-date 2026-01-01 \
  --end-date 2026-01-31
```

In the R2-backed mode, the CLI downloads strategy artifacts from R2 and loads all available market history up to `end-date` before emitting decisions only for the requested `start-date` to `end-date` window. This preserves warm-up history for blackbox strategies.

If the historical market-data input parquet is very large, do the heavy normalization locally first and then point `market-data-upload` at the resulting canonical parquet file or directory of parquet files.

Inspect the resulting file:

```text
decisions/backtest/<strategy_id>/decisions.parquet
```

## 4. Publish To R2

Upload strategy artifacts:

```bash
python -m trading_infra strategy-upload \
  --base-path /workspaces/code/trading-infra-git \
  --strategy-id top_n_adj_close_v1
```

Upload backtest decisions:

```bash
python -m trading_infra backtest-upload \
  --strategy-id top_n_adj_close_v1 \
  --path /workspaces/code/trading-infra-git/decisions/backtest/top_n_adj_close_v1/decisions.parquet
```

The upload commands validate and rewrite the local Parquet before publishing, so only canonical registry and decision schemas are stored in R2.

Upload the registry:

```bash
python -m trading_infra registry-upload \
  --path /workspaces/code/trading-infra-git/registry/strategies.parquet
```

## 5. GitHub Actions Setup

In GitHub repository settings, add these Actions secrets:

- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_S3_API`
- `R2_BUCKET_NAME`

Then trigger the `Daily Paper Trading` workflow manually using `workflow_dispatch`.

Recommended first run inputs:

- `run_date`: a known date with available market data
- `exchange`: `NSE`

## 6. Validation Checklist

Run a local R2-backed paper dry-run:

```bash
python -m trading_infra paper-dry-run \
  --date 2026-01-31 \
  --use-r2 \
  --exchange NSE
```

Then verify:

- strategy artifacts exist under `strategies/<strategy_id>/...`
- registry exists under `registry/strategies.parquet`
- backtest decisions exist under `decisions/backtest/<strategy_id>/decisions.parquet`
- paper decisions are created or updated under `decisions/paper/<strategy_id>/decisions.parquet`
- R2-backed paper runs append to existing `decisions/paper/<strategy_id>/decisions.parquet` history instead of replacing it
- rerunning the same date does not create duplicate paper rows

The R2-backed paper flow also loads all available market history up to the requested date so lookback-dependent strategies receive full context.

## 7. Current Limitation

The first production-supported strategy type is `top_n_adj_close`. Add more strategy builders only after the storage and paper workflow remain stable.
