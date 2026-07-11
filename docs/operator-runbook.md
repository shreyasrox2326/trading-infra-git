# Operator Runbook

This runbook is the operator path for:

- local full-history bootstrap
- local verification
- one-time historical upload to R2
- daily GitHub Actions paper runs

Use it as a flowchart, not a command dump.

## 1. What Lives Where

Durable system roles:

- R2: source of truth for canonical market data, strategies, registries, and decision logs
- GitHub Actions: daily market refresh plus paper-decision generation/upload
- Local machine: raw fetches, full rebuilds, verification, backtests, research, and approved uploads

Normal historical shape:

```text
fetch raw per exchange
-> review manifest exceptions
-> combine manifests
-> build one combined NSE+BSE history tree
-> verify
-> doctor
-> upload
```

## 2. Before You Start

R2 env vars:

- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_S3_API`
- `R2_BUCKET_NAME`

If you are in container `/bin/sh`, load env with:

```bash
. ./.env
```

Do not assume `source .env` works unless you are explicitly in Bash.

Check current R2 usage first:

```bash
python -m trading_infra r2-usage
python -m trading_infra r2-budget-check
```

## 3. Full Historical Bootstrap

This section is for rebuilding or extending the local full-history tree.

### 3.1 Fetch Raw Files

Run long fetches in `tmux`.

Verified parseable starts:

- NSE: `1994-11-03`
- BSE: `2007-01-02`

Fetch NSE:

```bash
tmux new -s history-fetch

python -m trading_infra history-fetch \
  --exchange NSE \
  --start-date 1994-11-03 \
  --end-date YYYY-MM-DD \
  --output-path /workspaces/code/trading-infra-git/data/raw/bhavcopy/NSE \
  --workers 1 \
  --retries 1 \
  --retry-sleep-seconds 1 \
  --request-sleep-seconds 0.5 \
  --log-path /workspaces/code/trading-infra-git/data/import/history-fetch-nse.log
```

Fetch BSE:

```bash
python -m trading_infra history-fetch \
  --exchange BSE \
  --start-date 2007-01-02 \
  --end-date YYYY-MM-DD \
  --output-path /workspaces/code/trading-infra-git/data/raw/bhavcopy/BSE \
  --workers 8 \
  --retries 1 \
  --retry-sleep-seconds 1 \
  --request-sleep-seconds 0.5 \
  --log-path /workspaces/code/trading-infra-git/data/import/history-fetch-bse.log
```

Monitor a long fetch:

```bash
tail -n 40 -f /workspaces/code/trading-infra-git/data/import/history-fetch-nse.log
```

Default manifest outputs:

```text
data/import/manifests/raw_fetch_NSE.parquet
data/import/manifests/raw_fetch_BSE.parquet
```

If you are resuming only problem rows:

```bash
python -m trading_infra history-fetch \
  --exchange NSE \
  --start-date 1994-11-03 \
  --end-date YYYY-MM-DD \
  --output-path /workspaces/code/trading-infra-git/data/raw/bhavcopy/NSE \
  --manifest-path /workspaces/code/trading-infra-git/data/import/manifests/raw_fetch_NSE.parquet \
  --only missing,rate_limited,failed \
  --fail-fast-rate-limit-ratio 0.2
```

When to stop:

- If NSE is returning mostly `rate_limited`, stop and retry later with conservative settings.
- Do not treat a large wall of `rate_limited` rows as success.

### 3.2 Review Manifest Exceptions

Do not hand-edit data with one-off scripts if the issue is really a fetch-status decision.

Use manifest review for accepted exceptions such as known archive gaps:

```bash
python -m trading_infra history-manifest-mark \
  --manifest-path /workspaces/code/trading-infra-git/data/import/manifests/raw_fetch_NSE.parquet \
  --exchange NSE \
  --date 1995-09-06 \
  --status not_available \
  --reason "known archive gap after operator review"
```

This is the preferred path when you want the decision to be auditable and reusable later.

### 3.3 Combine Reviewed Manifests

Historical upload expects a reviewed combined manifest for the combined history tree.

```bash
python -m trading_infra history-manifest-combine \
  --output /workspaces/code/trading-infra-git/data/import/manifests/raw_fetch_ALL.parquet \
  /workspaces/code/trading-infra-git/data/import/manifests/raw_fetch_NSE.parquet \
  /workspaces/code/trading-infra-git/data/import/manifests/raw_fetch_BSE.parquet
```

Next step:

- build one combined NSE+BSE history tree

### 3.4 Build Combined Local History

Normal build:

```bash
python -m trading_infra history-build \
  --input-path /workspaces/code/trading-infra-git/data/raw/bhavcopy \
  --output-path /workspaces/code/trading-infra-git/data/import/daily_stock_data_full \
  --workers 4 \
  --clean \
  --log-path /workspaces/code/trading-infra-git/data/import/history-build.log
```

Output shape:

```text
data/import/daily_stock_data_full/
  exchange=NSE/year=YYYY/month=MM/part.parquet
  exchange=BSE/year=YYYY/month=MM/part.parquet
```

Monitor a long build:

```bash
tail -n 40 -f /workspaces/code/trading-infra-git/data/import/history-build.log
```

Important build behavior:

- The normal upload shape is a combined NSE+BSE tree.
- `history-build --clean --exchange X` is exchange-scoped. It should clean only that exchange subtree, not the whole combined tree.
- `--incremental`, `--only-missing`, and `--repair-partition` are for updating an existing tree without full deletion.
- `--from-manifest` builds only from rows whose status is `downloaded`, `skipped_existing`, or `validated`.

If the data files are fine but `partition_manifest.parquet` needs regeneration:

```bash
python -m trading_infra history-partition-manifest-refresh \
  --history-path /workspaces/code/trading-infra-git/data/import/daily_stock_data_full
```

### 3.5 Optional Single-Command Bootstrap

`history-bootstrap` is useful when you want one command for fetch -> build -> verify -> doctor, but keep upload off until you are satisfied with the local results.

Strict mode:

```bash
python -m trading_infra history-bootstrap \
  --exchange NSE \
  --start-date 1994-11-03 \
  --end-date YYYY-MM-DD \
  --raw-output-path /workspaces/code/trading-infra-git/data/raw/bhavcopy/NSE \
  --history-path /workspaces/code/trading-infra-git/data/import/daily_stock_data_full \
  --audit-path /workspaces/code/trading-infra-git/data/import/history_audit.json \
  --resume \
  --upload false
```

Override mode for a one-off reviewed continuation:

```bash
python -m trading_infra history-bootstrap \
  --exchange NSE \
  --start-date 1994-11-03 \
  --end-date YYYY-MM-DD \
  --raw-output-path /workspaces/code/trading-infra-git/data/raw/bhavcopy/NSE \
  --history-path /workspaces/code/trading-infra-git/data/import/daily_stock_data_full \
  --audit-path /workspaces/code/trading-infra-git/data/import/history_audit.json \
  --resume \
  --allow-fetch-status rate_limited \
  --upload false
```

Guideline:

- Prefer `history-manifest-mark` for durable reviewed exceptions.
- Use `--allow-fetch-status ...` only when you intentionally want a one-run override.

## 4. Verify Before Upload

You usually want both `history-verify` and `history-doctor`.

They answer different questions:

- `history-verify`: are the local parquet files valid and internally sane?
- `history-doctor`: does the fetch/build pipeline look complete for the reviewed manifest?

So yes, `history-verify` can pass while `history-doctor` still warns or fails.

### 4.1 Verify Parquet Integrity

```bash
python -m trading_infra history-verify \
  --path /workspaces/code/trading-infra-git/data/import/daily_stock_data_full \
  --report-path /workspaces/code/trading-infra-git/data/import/history_audit.json \
  --partition-wise \
  --max-memory-gb 4
```

You want:

- `passed=true`
- zero duplicate keys
- zero invalid OHLC rows
- sensible date ranges and partition counts

### 4.2 Doctor The Pipeline State

Run per exchange:

```bash
python -m trading_infra history-doctor \
  --exchange NSE \
  --raw-manifest-path /workspaces/code/trading-infra-git/data/import/manifests/raw_fetch_NSE.parquet \
  --history-path /workspaces/code/trading-infra-git/data/import/daily_stock_data_full
```

Optionally compare against R2 too:

```bash
python -m trading_infra history-doctor \
  --exchange NSE \
  --raw-manifest-path /workspaces/code/trading-infra-git/data/import/manifests/raw_fetch_NSE.parquet \
  --history-path /workspaces/code/trading-infra-git/data/import/daily_stock_data_full \
  --compare-r2
```

Useful doctor counters:

- `raw_downloaded`
- `raw_skipped_existing`
- `raw_validated`
- `raw_usable`

If `raw_usable` is high and `raw_downloaded=0`, that can still be healthy on a resume-heavy run.

### 4.3 Compare Local Partition Manifest To R2

```bash
python -m trading_infra r2-sync-check \
  --exchange NSE \
  --partition-manifest-path /workspaces/code/trading-infra-git/data/import/manifests/partition_manifest.parquet
```

What it does:

- reads the local partition manifest and canonical R2 keys for the selected exchange
- calls R2 `head_object` on matching canonical `part.parquet` objects
- compares remote `ETag` and size against local expected values
- computes multipart-style expected ETags locally when the remote object uses multipart upload form

What it does not do:

- it does not download remote parquet files during the normal sync check
- it does not recompute remote row counts during the normal sync check

Next step:

- upload only after local verification is acceptable and the operator explicitly wants upload

## 5. Historical Upload To R2

Use the combined history tree and combined reviewed raw manifest:

```bash
python -m trading_infra history-upload \
  --path /workspaces/code/trading-infra-git/data/import/daily_stock_data_full \
  --audit-path /workspaces/code/trading-infra-git/data/import/history_audit.json \
  --raw-manifest-path /workspaces/code/trading-infra-git/data/import/manifests/raw_fetch_ALL.parquet \
  --partition-manifest-path /workspaces/code/trading-infra-git/data/import/manifests/partition_manifest.parquet \
  --exchange NSE \
  --exchange BSE \
  --workers 8
```

What it does:

- compares canonical R2 objects first using `head_object` metadata
- uploads only missing or stale monthly partitions to `_staging/history-load/<run_id>/...`
- verifies staged object sizes with R2 metadata
- promotes only the uploaded partitions with server-side copy
- writes canonical `part.parquet` objects
- writes `data/daily_stock_data/_manifest.json`

What it does not do:

- it does not make raw bhavcopy files canonical R2 artifacts

## 6. Clean Old Staging Objects

Staging objects are safe but not canonical.

Historical staging dry run:

```bash
python -m trading_infra r2-cleanup-staging \
  --prefix _staging/history-load/ \
  --older-than-days 7 \
  --dry-run
```

Daily refresh staging dry run:

```bash
python -m trading_infra r2-cleanup-staging \
  --prefix _staging/daily-refresh/ \
  --older-than-days 7 \
  --dry-run
```

Delete only after you are comfortable with the matched keys.

## 7. Daily GitHub Actions Behavior

Workflow file:

- `.github/workflows/daily-paper.yml`

Current intended behavior:

- installs pinned Python dependencies from `requirements.lock`
- performs `r2-budget-check`
- refreshes market data for each requested exchange/date
- runs `paper-dry-run --use-r2`
- runs `performance-refresh --decision-kind paper`
- uploads paper results by default on scheduled runs

Important daily-run behavior:

- the workflow fetches raw bhavcopy into temporary runner state only
- the durable output is canonical cleaned parquet plus paper decisions
- full-history raw bhavcopies remain local/operator state and are not kept as canonical R2 artifacts
- rerunning the same date should not create duplicate market-data rows
- rerunning the same date should not create duplicate paper-decision rows
- rerunning the same date refreshes paper performance from the stored decision log

If the bhavcopy is unavailable for the requested date, refresh returns `status=no_data` and paper evaluation is skipped for that exchange/date.

## 8. Local Strategy And Decision Operations

Private strategy folder shape:

```text
strategies/<strategy_id>/
  config.yaml
  metadata.json
  model.pkl
  feature_config.yaml
```

For private pickle-backed strategies:

- `strategy_type: private_pickle_v1`
- `runtime_contract: private_pickle_v1`
- `lookback_days: <bounded integer>`
- full cash day means zero decision rows for that date

Upload strategy artifacts:

```bash
python -m trading_infra strategy-upload \
  --base-path /workspaces/code/trading-infra-git \
  --strategy-id top_n_adj_close_v1
```

Run local backtest:

```bash
python -m trading_infra backtest-run \
  --base-path /workspaces/code/trading-infra-git \
  --strategy-id top_n_adj_close_v1 \
  --market-data-path /workspaces/code/trading-infra-git/data/import/daily_stock_data_full/exchange=NSE \
  --exchange NSE \
  --start-date 2026-01-01 \
  --end-date 2026-01-31
```

Local backtest behavior:

- local `backtest-run` now loads market data in chunks instead of one giant in-memory frame
- bounded-lookback strategies use overlapped warmup windows between chunks
- private pickle-backed strategies reuse one runtime per chunk and precompute feature tables once per chunk
- progress is shown by default through `tqdm`
- for full-history local runs, prefer an exchange-scoped path such as `.../daily_stock_data_full/exchange=NSE`

Useful tuning flags:

```bash
--chunk-size-days 252
--warmup-days 180
--no-progress
```

Example full-history NSE private-strategy run:

```bash
python -m trading_infra backtest-run \
  --base-path /workspaces/code/trading-infra-git \
  --strategy-id v1_regime_sideways_etf_rotation_long \
  --market-data-path /workspaces/code/trading-infra-git/data/import/daily_stock_data_full/exchange=NSE \
  --exchange NSE \
  --start-date 1994-11-03 \
  --end-date 2026-06-25
```

Upload backtest decisions:

```bash
python -m trading_infra backtest-upload \
  --strategy-id top_n_adj_close_v1 \
  --path /workspaces/code/trading-infra-git/decisions/backtest/top_n_adj_close_v1/decisions.parquet
```

Upload registry:

```bash
python -m trading_infra registry-upload \
  --path /workspaces/code/trading-infra-git/registry/strategies.parquet
```

Compute realized performance locally from existing decisions:

```bash
python -m trading_infra performance-compute \
  --strategy-id top_n_adj_close_v1 \
  --decision-kind backtest \
  --decisions-path /workspaces/code/trading-infra-git/decisions/backtest/top_n_adj_close_v1/decisions.parquet \
  --market-data-path /workspaces/code/trading-infra-git/data/import/daily_stock_data_full \
  --output-dir /workspaces/code/trading-infra-git/performance/backtest/top_n_adj_close_v1
```

Refresh realized performance from R2-backed paper decisions:

```bash
python -m trading_infra performance-refresh \
  --decision-kind paper \
  --exchange NSE \
  --upload-results
```

## 9. Quick “What Do I Run Now?” Guide

If you want to rebuild history from raw:

1. `history-fetch` per exchange
2. `history-manifest-mark` for reviewed exceptions
3. `history-manifest-combine`
4. `history-build --clean`
5. `history-verify`
6. `history-doctor` per exchange
7. `r2-sync-check`
8. `history-upload`

If you only need to repair a few fetch failures:

1. `history-fetch --only missing,rate_limited,failed`
2. review statuses
3. rebuild incrementally or repair selected partitions
4. re-run verify and doctor

If partition files are correct but the manifest looks wrong:

1. `history-partition-manifest-refresh`
2. re-run `history-doctor` or `r2-sync-check`

If you want to inspect the expected file format for one date:

```bash
python -m trading_infra format-inspect \
  --exchange NSE \
  --date 2024-07-08
```
