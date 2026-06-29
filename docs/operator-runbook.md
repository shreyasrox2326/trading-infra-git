# Operator Runbook

This runbook covers local historical data assembly, verified R2 publishing, strategy publishing, and GitHub Actions daily operation.

## 1. Cloudflare R2 Setup

Create a private R2 bucket for the project.

Create access credentials with read/write access to that bucket.

Record these values:

- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_S3_API`
- `R2_BUCKET_NAME`

Canonical market data on R2 lives under:

```text
data/daily_stock_data/exchange=NSE/year=YYYY/month=MM/part.parquet
data/daily_stock_data/exchange=BSE/year=YYYY/month=MM/part.parquet
```

Do not upload raw bhavcopy files as canonical R2 market data.

## 2. Local Full-History Bootstrap

Fetch raw bhavcopy files into ignored local operator state. Run long downloads inside `tmux` and keep the per-date status log under `data/import/` or `data/raw/`.

```bash
tmux new -s history-fetch

python -m trading_infra history-fetch \
  --exchange NSE \
  --start-date 1994-01-01 \
  --end-date YYYY-MM-DD \
  --output-path /workspaces/code/trading-infra-git/data/raw/bhavcopy/NSE \
  --workers 1 \
  --retries 5 \
  --retry-sleep-seconds 60 \
  --request-sleep-seconds 1 \
  --log-path /workspaces/code/trading-infra-git/data/import/history-fetch-nse.log

python -m trading_infra history-fetch \
  --exchange BSE \
  --start-date 2007-01-01 \
  --end-date YYYY-MM-DD \
  --output-path /workspaces/code/trading-infra-git/data/raw/bhavcopy/BSE \
  --workers 8 \
  --retries 3 \
  --log-path /workspaces/code/trading-infra-git/data/import/history-fetch-bse.log
```

`history-fetch` is resumable by default: existing files are skipped unless `--overwrite` is passed. It uses a progress bar by default; use `--no-progress` only for non-interactive logging. The fetch log is appended and flushed as each date completes, so it can be monitored while the command is running:

```bash
tail -n 40 -f /workspaces/code/trading-infra-git/data/import/history-fetch-nse.log
```

`history-fetch` also writes a raw fetch manifest parquet. By default it uses:

```text
data/import/manifests/raw_fetch_<EXCHANGE>.parquet
```

Override it with `--manifest-path`. The manifest records one row per expected weekday with the expected format id, expected filename, primary URL, local path, status, bytes, SHA256, last error, and parser hint.

For NSE, `rate_limited` means the official archive returned HTTP 403. Stop the run, wait before retrying, and resume with low concurrency; do not continue a full-range run that is logging only `rate_limited` rows. NSE-facing bulk fetches should stay conservative: one worker plus roughly one second between requests.

Build one canonical parquet locally:

```bash
python -m trading_infra history-build \
  --input-path /workspaces/code/trading-infra-git/data/raw/bhavcopy \
  --output-path /workspaces/code/trading-infra-git/data/import/daily_stock_data_full \
  --workers 4 \
  --log-path /workspaces/code/trading-infra-git/data/import/history-build.log
```

`history-build` writes monthly local partitions shaped like R2:

```text
data/import/daily_stock_data_full/
  exchange=NSE/year=YYYY/month=MM/part.parquet
  exchange=BSE/year=YYYY/month=MM/part.parquet
```

It shows progress by default and writes timestamped phase logs. Monitor a long run with:

```bash
tail -n 40 -f /workspaces/code/trading-infra-git/data/import/history-build.log
```

Current ingestion uses identity adjustment:

```text
adj_open=open
adj_high=high
adj_low=low
adj_close=close
adj_factor=1.0
```

Delivery fields may be null when the source bhavcopy does not include them.

Older NSE legacy bhavcopies do not include `ISIN`. For those rows, canonical `isin` is populated with the source `SYMBOL` so the full history keeps a non-null security identifier.

Inspect the expected source format for a date before debugging fetch or parse failures:

```bash
python -m trading_infra format-inspect \
  --exchange NSE \
  --date 2024-07-08
```

The format registry lives in `src/trading_infra/data/formats.yaml`. It documents the expected filename, primary/fallback URLs, required columns, optional columns, parser name, and known quirks for NSE/BSE legacy and UDiFF/common bhavcopy periods.

## 3. Local Verification Before Upload

Verify before any R2 historical replacement:

```bash
python -m trading_infra history-verify \
  --path /workspaces/code/trading-infra-git/data/import/daily_stock_data_full \
  --report-path /workspaces/code/trading-infra-git/data/import/history_audit.json \
  --partition-wise
```

`history-verify` verifies monthly parquet files partition by partition and writes compact aggregate metadata instead of loading the full history into one frame. Inspect `history_audit.json` and `history_audit.md`. Confirm:

- audit passed
- accepted date ranges by exchange
- row counts by exchange/year/month look reasonable
- duplicate key count is zero
- invalid OHLC count is zero
- negative volume/turnover counts are zero
- identity adjustment is acceptable for this load
- every `partition_summaries` entry has the expected row count, file size, and SHA256

User approval is required before replacing or extending canonical R2 historical market data.

## 4. One-Time Verified R2 Historical Upload

Upload only after local verification passes:

```bash
python -m trading_infra history-upload \
  --path /workspaces/code/trading-infra-git/data/import/daily_stock_data_full \
  --audit-path /workspaces/code/trading-infra-git/data/import/history_audit.json \
  --exchange NSE \
  --exchange BSE
```

The upload path writes monthly canonical partitions locally, uploads them to `_staging/history-load/<run_id>/...`, verifies staged object sizes, then promotes canonical `part.parquet` files under `data/daily_stock_data/`. A manifest is written to `data/daily_stock_data/_manifest.json`.

Use `market-data-upload` only for explicit operator-controlled canonical parquet uploads. The full historical bootstrap should use `history-upload`.

## 5. Daily Market-Data Refresh

Refresh one exchange/date into its affected monthly R2 partition:

```bash
python -m trading_infra market-data-refresh \
  --date YYYY-MM-DD \
  --exchange NSE

python -m trading_infra market-data-refresh \
  --date YYYY-MM-DD \
  --exchange BSE
```

The refresh command fetches only the requested date, normalizes it to the canonical schema, downloads only the affected R2 month, merges and deduplicates by `date + exchange + isin + series`, stages the monthly parquet, then promotes `part.parquet`.

If the exchange file is unavailable, the command reports `status=no_data` and exits successfully so holidays do not fail the workflow.

## 6. Local Strategy Preparation

Create a versioned local strategy folder under `strategies/<strategy_id>/` only when preparing a new strategy version for first upload to R2.

Minimum files:

- `config.yaml`
- `metadata.json`

For the current supported strategy type, use the example in `examples/strategies/top_n_adj_close_v1/`.

After the strategy version is uploaded, treat R2 as the canonical source. Local `strategies/`, `registry/`, and `decisions/` directories are workspace/cache state only.

## 7. Local Backtest

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

R2-backed mode downloads strategy artifacts from R2 and loads all available market history up to `end-date` before emitting decisions only for the requested window.

## 8. Publish Strategies And Decisions To R2

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

Upload the registry:

```bash
python -m trading_infra registry-upload \
  --path /workspaces/code/trading-infra-git/registry/strategies.parquet
```

Upload commands validate and rewrite local Parquet before publishing.

## 9. GitHub Actions Setup

In GitHub repository settings, add these Actions secrets:

- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_S3_API`
- `R2_BUCKET_NAME`

Then trigger the `Daily Paper Trading` workflow manually using `workflow_dispatch`.

Recommended first run inputs:

- `run_date`: a known completed trading date
- `exchange`: `NSE BSE`
- `skip_market_refresh`: `false`
- `upload_results`: `true`

The scheduled workflow refreshes market data before paper evaluation. If refresh returns `no_data` for an exchange/date, paper evaluation is skipped for that exchange/date.

## 10. Validation Checklist

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
- R2-backed paper runs append to existing paper history instead of replacing it
- rerunning the same date does not create duplicate paper rows

## 11. Current Limitation

The first production-supported strategy type is `top_n_adj_close`.

Optional ML artifacts can be stored and uploaded, but ML strategy execution, feature generation, model loading, and inference contracts are not implemented yet.
