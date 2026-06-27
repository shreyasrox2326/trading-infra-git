# Historical NSE Data Plan Checklist

Plan: Resolve README Misalignment Starting With Historical NSE Data

Status syntax:

- `- [ ]` not done
- `- [x]` done

## Phase 1: Stabilize And Commit Current Work

- [x] Check current git status before committing.
- [x] Re-run `pytest -q` on current uncommitted work.
- [x] Inspect current diff/stat.
- [x] Stage current R2, market-data upload, paper-history, and progress-checklist changes.
- [x] Commit current staged work with scoped commit message.
- [x] Confirm working tree state after commit.

## Phase 2: Add Historical Data Source Adapter

- [x] Add repo-local NSE bhavcopy source adapter module.
- [ ] Prefer `bhavcopy` library if it works reliably.
- [x] Use `nsefin` or direct NSE download only as fallback.
- [x] Store raw fetched files under `data/raw/bhavcopy/`.
- [x] Store canonical generated parquet under `data/import/daily_stock_data.parquet`.

## Phase 3: Implement `bhavcopy-fetch`

- [x] Add `python -m trading_infra bhavcopy-fetch` CLI command.
- [x] Support `--exchange`, `--start-date`, `--end-date`, `--output-path`.
- [x] Skip already-downloaded raw files unless `--overwrite` is passed.
- [x] Fetch only available trading-day files.
- [x] Log concise progress/failures to ignored local state.
- [ ] Stop if source requires CAPTCHA/login/paid credentials/manual cookies.

## Phase 4: Implement `bhavcopy-ingest`

- [x] Add `python -m trading_infra bhavcopy-ingest` CLI command.
- [x] Support `--input-path`, `--output-path`, `--exchange`.
- [x] Normalize raw bhavcopy columns to README `daily_stock_data` schema.
- [x] Use identity adjustment for v1: `adj_* = raw OHLC`, `adj_factor = 1.0`.
- [x] Validate required columns and types.
- [x] Validate non-null keys.
- [x] Validate uniqueness on `date + exchange + isin + series`.
- [x] Emit summary: rows, date range, symbols, exchanges, missing delivery fields, duplicate count.

## Phase 5: Upload Canonical History To R2

- [x] Generate canonical parquet from fetched bhavcopies.
- [x] Confirm canonical parquet coverage: 5,786,247 rows from 2016-01-01 to 2026-06-25.
- [x] Run one-partition `market-data-upload` smoke test for generated canonical parquet.
- [x] Run full `market-data-upload` for generated canonical parquet.
- [x] Verify available R2 month partitions: 126 NSE month partitions from 2016-01 to 2026-06.
- [x] Verify R2 row count: 5,786,247 rows.
- [x] Verify R2 min/max date: 2016-01-01 to 2026-06-25.
- [x] Verify sample rows.
- [x] Do not upload backtest decisions until market-data coverage is confirmed.

## Phase 6: Run Full Historical Backtest From R2

- [x] Run R2-backed `backtest-run` for confirmed historical range.
- [x] Validate backtest rows are non-zero: 12,930 rows.
- [x] Validate backtest date range: 2016-01-01 to 2026-06-25.
- [x] Validate no duplicate decision keys.
- [x] Validate decision schema.

## Phase 7: Upload Backtest Decisions And Validate Paper Dry Run

- [x] Upload backtest decisions with `backtest-upload`.
- [x] Run R2-backed `paper-dry-run` for latest confirmed market date: 2026-06-25.
- [x] Inspect paper decision output: 5 rows for `top_n_adj_close_v1`.
- [x] Do not use `--upload-results` until paper output is inspected.

## Phase 8: Commit Ingestion And Historical Setup Work

- [x] Commit fetch command work.
- [x] Commit canonical ingestion work.
- [x] Commit bhavcopy pipeline tests.
- [x] Commit historical setup docs.
- [x] Run final `pytest -q`.
- [ ] Confirm final git status.

## Stop Conditions

- [ ] Stop if NSE/source requires CAPTCHA, browser login, paid credentials, or manual cookies.
- [ ] Stop if no source can fetch useful historical data.
- [ ] Stop if raw data lacks fields needed for README schema and no deterministic fallback exists.
- [ ] Stop if full fetch needs a user-approved long-running `tmux` job.
- [ ] Stop if corporate-action-adjusted prices are required before first historical load.
