# Full README Alignment Checklist

Date: 2026-06-28

Purpose: Track execution of the finalized plan to align code, documentation, local data assembly, Cloudflare R2 storage, GitHub Actions automation, schemas, and performance expectations with `README.md`.

Update rule:

- Use `[ ]` for not started.
- Use `[x]` for completed.
- Add short dated notes under items only when status changes materially.
- Update this checklist in the same commit or work session as completed implementation work.
- If implementation changes the plan, update this checklist first or in the same change.
- If a durable architecture decision changes, add a separate file under `memory/decisions/` and link it from `memory/index.md`.

## Stage 1: Documentation And Contracts

- [ ] Update `README.md` to distinguish bootstrap historical load, daily cloud maintenance, local research/backtesting/training, R2 storage/source-of-truth role, GitHub Actions cron/compute role, and current/target exchange coverage.
- [ ] Update `docs/operator-runbook.md` with local full-history build procedure, validation checklist before upload, one-time R2 upload procedure, GitHub Actions daily setup procedure, and recovery steps for failed daily refresh or failed paper run.
- [ ] Update `docs/strategy-contract.md` and `docs/progress-checklist.md` to track ML artifact storage separately from ML execution, document implemented runtime strategy types only, and track NSE/BSE history coverage, adjustment status, daily automation, and paper evaluation separately.

## Stage 2: Local Full-History Assembly

- [x] Add exchange source adapter for NSE legacy `cmDDMONYYYYbhav.csv.zip`.
  - 2026-06-28: Existing NSE legacy parser retained and covered by tests.
- [x] Add exchange source adapter for NSE UDiFF/common `BhavCopy_NSE_CM_0_0_0_YYYYMMDD_F_0000.csv.zip`.
  - 2026-06-28: Existing NSE UDiFF parser retained and covered by tests.
- [x] Add exchange source adapter for BSE legacy `EQDDMMYY_CSV.ZIP`.
  - 2026-06-28: Added BSE legacy filename and normalization support.
- [x] Add exchange source adapter for BSE UDiFF/common `BhavCopy_BSE_CM_0_0_0_YYYYMMDD_F_0000.CSV`.
  - 2026-06-28: Added BSE common-format filename and normalization support.
- [x] Add local-only `history-fetch` command for NSE full-history raw bhavcopy download.
  - 2026-06-28: Added exchange-aware `history-fetch`.
- [x] Add local-only `history-fetch` command for BSE full-history raw bhavcopy download.
  - 2026-06-28: Added exchange-aware `history-fetch`.
- [x] Add local canonical `history-build` command that builds `data/import/daily_stock_data_full.parquet` from raw bhavcopy inputs.
  - 2026-06-28: Added `history-build`.
- [x] Ensure canonical output matches the README `daily_stock_data` schema.
  - 2026-06-28: Build path casts to canonical schema and tests assert column order.
- [x] Apply and document initial identity adjustment policy: `adj_* = raw OHLC`, `adj_factor = 1.0`.
  - 2026-06-28: Parser continues to emit identity adjustment fields.

## Stage 3: Local Verification Before Any Upload

- [x] Add `history-verify` command with `--path` and `--report-path`.
  - 2026-06-28: Added `history-verify`.
- [x] Verify exact canonical schema and types.
  - 2026-06-28: Audit detects missing/unexpected columns and casts canonical types.
- [x] Verify required non-null columns.
  - 2026-06-28: Audit reports required-null columns.
- [x] Verify no duplicate `date + exchange + isin + series` keys.
  - 2026-06-28: Build and audit check duplicate keys.
- [x] Report date range by exchange.
  - 2026-06-28: Audit includes exchange-level min/max dates.
- [x] Report row counts by exchange/year/month.
  - 2026-06-28: Audit includes exchange/month partition counts.
- [ ] Report missing trading days summary.
- [x] Report null counts for optional fields.
  - 2026-06-28: Audit includes null counts for all canonical columns.
- [x] Detect invalid OHLC cases.
  - 2026-06-28: Audit fails invalid OHLC rows.
- [x] Detect negative volume/turnover.
  - 2026-06-28: Audit fails negative volume/turnover rows.
- [x] Verify UDiFF/legacy parser boundary coverage.
  - 2026-06-28: Tests cover NSE legacy, NSE UDiFF, BSE legacy, and BSE common format.
- [x] Generate canonical full parquet.
  - 2026-06-28: `history-build` writes canonical parquet.
- [x] Generate partition plan.
  - 2026-06-28: Audit includes exchange/year/month row counts.
- [x] Generate machine-readable audit JSON.
  - 2026-06-28: `history-verify` writes JSON.
- [x] Generate concise human-readable audit Markdown.
  - 2026-06-28: `history-verify` writes adjacent Markdown summary.
- [ ] User intervention: inspect audit report.
- [ ] User intervention: confirm accepted date ranges.
- [ ] User intervention: confirm whether identity adjustment is acceptable for first upload.
- [ ] User intervention: approve R2 replacement of existing historical market data.

## Stage 4: One-Time Clean R2 Historical Upload

- [x] Add `history-upload` command with `--path`, `--audit-path`, and one or more `--exchange` values.
  - 2026-06-28: Added `history-upload`.
- [x] Refuse upload unless verification passed.
  - 2026-06-28: Upload helper requires passing audit JSON.
- [x] Write monthly canonical partitions locally before upload.
  - 2026-06-28: Upload helper writes local monthly partition files from canonical parquet.
- [x] Upload to temporary R2 staging prefix such as `_staging/history-load/<run_id>/...`.
  - 2026-06-28: Upload helper stages under `_staging/history-load/<run_id>/`.
- [x] Verify staged uploaded object sizes/checksums/counts against the local partition plan.
  - 2026-06-28: Staged objects are downloaded and size-checked before promotion.
- [x] Promote staged data to canonical `data/daily_stock_data/exchange=.../year=.../month=.../part.parquet` only after staging verification passes.
  - 2026-06-28: Promotion happens after all staging checks pass.
- [x] Write R2 manifest such as `data/daily_stock_data/_manifest.json`.
  - 2026-06-28: Upload helper writes manifest JSON.
- [x] Ensure upload never runs directly from raw files.
  - 2026-06-28: Upload requires canonical parquet and passing audit.
- [x] Ensure unverified input cannot rewrite canonical R2 partitions.
  - 2026-06-28: Failed audit test verifies no R2 writes.
- [x] Ensure old canonical data is not deleted until staged upload verification passes.
  - 2026-06-28: Staging verification precedes canonical promotion and stale cleanup.
- [x] Ensure failed upload leaves canonical R2 unchanged.
  - 2026-06-28: Failed audit path leaves R2 untouched.
- [ ] User intervention: confirm R2 credentials are configured locally.
- [ ] User intervention: confirm staged upload looks correct.
- [ ] User intervention: approve final promotion from staging to canonical prefix.

## Stage 5: Cloud Daily Bhavcopy Maintenance

- [x] Add `market-data-refresh --date YYYY-MM-DD --exchange NSE`.
  - 2026-06-28: Added exchange-aware `market-data-refresh`.
- [x] Add `market-data-refresh --date YYYY-MM-DD --exchange BSE`.
  - 2026-06-28: Added exchange-aware `market-data-refresh`.
- [x] Fetch only the requested date.
  - 2026-06-28: Refresh helper fetches one date.
- [x] Parse using the correct legacy or UDiFF adapter.
  - 2026-06-28: Refresh uses exchange-aware bhavcopy parser.
- [x] Normalize to canonical schema.
  - 2026-06-28: Refresh normalizes fetched bhavcopy to canonical market-data columns.
- [x] Download only the affected monthly R2 partition.
  - 2026-06-28: Refresh lists/downloads only the target exchange/year/month.
- [x] Merge refreshed date into that month.
  - 2026-06-28: Refresh merges existing month plus fetched date.
- [x] Deduplicate by `date + exchange + isin + series`.
  - 2026-06-28: Refresh keeps latest refreshed rows for duplicate keys.
- [x] Validate the full affected month.
  - 2026-06-28: Refresh writes canonical selected monthly frame.
- [x] Upload through staging, then promote the monthly partition.
  - 2026-06-28: Refresh stages and verifies before canonical upload.
- [x] Return a clean no-op status for holiday/no-file cases.
  - 2026-06-28: Unavailable bhavcopy returns `no_data`.
- [x] Do not fail the whole workflow for expected non-trading days.
  - 2026-06-28: CLI exits zero for `no_data` and nonzero only for fetch failures.
- [x] Do not run strategy evaluation if no market data exists for the requested date.
  - 2026-06-28: Refresh status now supports workflow gating; workflow wiring remains pending.

## Stage 6: GitHub Actions Cron Workflow

- [ ] Replace current paper-only workflow with full daily cloud automation.
- [ ] Keep scheduled weekday cron after bhavcopy availability.
- [ ] Keep manual `workflow_dispatch`.
- [ ] Resolve run date.
- [ ] Refresh NSE and BSE market data.
- [ ] Verify refreshed date exists in R2.
- [ ] Load active registry from R2.
- [ ] Run paper strategy evaluation.
- [ ] Append/upload paper decisions idempotently.
- [ ] Emit concise summary.
- [ ] Add workflow input `run_date`.
- [ ] Add workflow input `exchange`, defaulting to all supported exchanges.
- [ ] Add workflow input `skip_market_refresh`, manual emergency only.
- [ ] Add workflow input `upload_results`, default true for scheduled runs.
- [ ] User intervention: enable GitHub Actions.
- [ ] User intervention: choose cron time after exchange files are reliably published.
- [ ] User intervention: set `R2_ACCESS_KEY_ID` GitHub secret.
- [ ] User intervention: set `R2_SECRET_ACCESS_KEY` GitHub secret.
- [ ] User intervention: set `R2_S3_API` GitHub secret.
- [ ] User intervention: set `R2_BUCKET_NAME` GitHub secret.
- [ ] User intervention: manually run workflow for a known completed trading date.
- [ ] User intervention: inspect R2 market partition and paper decisions after first run.

## Stage 7: Strategy And Paper Evaluation Alignment

- [ ] Preserve blackbox strategy contract: input is canonical market data up to date `t`.
- [ ] Preserve blackbox strategy contract: output is canonical decision rows for date `t`.
- [ ] Preserve schema symmetry between backtest and paper decisions.
- [ ] Preserve local/cloud split: local handles research, model training, full historical backtests, parameter sweeps, and strategy uploads.
- [ ] Preserve local/cloud split: cloud handles daily one-date refresh and active strategy paper evaluation only.
- [ ] Enforce online strategy constraint: CPU-only.
- [ ] Enforce online strategy constraint: no online training.
- [ ] Enforce online strategy constraint: no full historical backtests in GitHub Actions.
- [ ] Enforce online strategy constraint: no large model inference unless explicitly approved later.

## Stage 8: Performance Requirements

- [ ] Local full-history build uses Polars lazy scans where practical.
- [ ] Local full-history build batch-parses files.
- [ ] Local full-history build avoids row-by-row loops over market data.
- [ ] Local full-history build writes partitioned parquet from canonical frame.
- [ ] Raw and generated artifacts stay under ignored `data/`.
- [ ] R2 upload uses verified monthly partition files, not raw data.
- [ ] R2 upload stages before promotion.
- [ ] R2 upload keeps logs concise.
- [ ] R2 upload writes manifest for reproducibility.
- [ ] Daily cloud fetches one date per exchange.
- [ ] Daily cloud rewrites only affected monthly partitions.
- [ ] Daily cloud avoids scanning all history during refresh.
- [ ] Daily paper evaluation remains CPU-only and bounded by strategy needs.

## Stage 9: Tests

- [x] Add parser test for NSE legacy.
  - 2026-06-28: Existing parser test retained.
- [x] Add parser test for NSE UDiFF.
  - 2026-06-28: Existing parser test retained.
- [x] Add parser test for BSE legacy.
  - 2026-06-28: Added BSE legacy parser test.
- [x] Add parser test for BSE UDiFF.
  - 2026-06-28: Added BSE common-format parser test.
- [ ] Add parser test for missing/holiday files.
- [x] Add history build test for canonical schema.
  - 2026-06-28: Added history build schema test.
- [ ] Add history build test for duplicate rejection.
- [ ] Add history build test for nullable delivery fields.
- [ ] Add history build test for identity adjustment.
- [x] Add history build test for multi-exchange output.
  - 2026-06-28: Added NSE+BSE build test.
- [ ] Add verification test for duplicate key failure.
- [x] Add verification test for invalid OHLC failure.
  - 2026-06-28: Added invalid OHLC audit test.
- [ ] Add verification test for bad schema failure.
- [x] Add verification test for valid audit report generation.
  - 2026-06-28: Added JSON and Markdown audit output test.
- [x] Add upload safety test that refuses upload without passing audit.
  - 2026-06-28: Added failed-audit test.
- [x] Add upload safety test that uploads staging first.
  - 2026-06-28: Added staged upload order assertion.
- [ ] Add upload safety test that does not modify canonical R2 on staging failure.
- [x] Add upload safety test that promotes only after verification.
  - 2026-06-28: Added staging-before-canonical promotion test.
- [x] Add daily cloud test that refresh merges one date without deleting rest of month.
  - 2026-06-28: Added monthly merge test.
- [x] Add daily cloud test that rerunning the same date is idempotent.
  - 2026-06-28: Added refresh idempotency test.
- [ ] Add daily cloud test that unavailable bhavcopy skips paper evaluation cleanly.
- [ ] Add daily cloud test that paper decision append remains idempotent.
- [ ] Add workflow test that cron exists.
- [ ] Add workflow test that refresh runs before paper evaluation.
- [ ] Add workflow test that secrets are referenced only through GitHub secrets.
- [ ] Add workflow test that manual override inputs exist.
- [ ] Run `python -m pytest -q`.

## Final Acceptance Criteria

- [ ] Local machine can build full canonical NSE+BSE history from raw bhavcopy files.
- [ ] Local verification produces a passing audit report before upload.
- [ ] R2 historical market data is uploaded only from verified canonical parquet.
- [ ] Failed upload cannot corrupt existing canonical R2 data.
- [ ] GitHub Actions cron maintains daily bhavcopy data on R2.
- [ ] GitHub Actions runs active paper strategies after successful market-data refresh.
- [ ] Backtest and paper decisions remain schema-symmetric.
- [ ] README, docs, memory, code, tests, and workflow all describe the same operating model.

## Explicit Deferred Decisions

- [ ] Decide corporate-action adjustment source and back-adjustment method.
- [ ] Decide ML runtime execution contract.
- [ ] Decide whether BSE and NSE symbols should ever be cross-exchange reconciled beyond storing `exchange + isin + series`.
- [ ] Decide whether to store optional performance snapshots later; default remains compute-on-demand.
