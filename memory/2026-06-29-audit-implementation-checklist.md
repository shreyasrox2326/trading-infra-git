# Audit Implementation Checklist

Source audit: `memory/Audit.md`

Created: 2026-06-29

Status labels:

- `DONE`: completed and committed
- `IN PROGRESS`: currently being changed
- `PARTIAL`: implementation exists but does not satisfy the audit target
- `TODO`: not implemented
- `BLOCKED`: needs external credentials, data, or operator approval

## Context Pass

- `DONE` Read `README.md` and confirmed the audit does not require an architecture rewrite.
- `DONE` Read `memory/index.md` and current durable memory map.
- `DONE` Read all of `memory/Audit.md` and mapped every problem, required artifact, command, priority item, and acceptance criterion to this checklist.
- `DONE` Reviewed relevant current implementation:
  - `src/trading_infra/cli.py`
  - `src/trading_infra/data/bhavcopy.py`
  - `src/trading_infra/data/history.py`
  - `src/trading_infra/storage/history.py`
  - `src/trading_infra/storage/r2.py`
  - `docs/operator-runbook.md`
  - `docs/progress-checklist.md`
  - history and upload tests under `tests/`

## Current Codebase Relation

- `PARTIAL` Historical fetch exists in `fetch_bhavcopy_archives` and `history-fetch`, with retries, per-date statuses, progress, and logs.
- `PARTIAL` Historical build is already partition-first in `build_history_partitions`, but rebuild modes and partition manifests are missing.
- `PARTIAL` Historical verification exists in `write_history_audit`, but it still loads all partitions into one Polars frame.
- `PARTIAL` Historical upload stages and promotes monthly partitions, but `upload_verified_history` still reads the selected history into one frame before upload.
- `PARTIAL` NSE/BSE legacy and UDiFF filenames/parsers exist in `bhavcopy.py`, but there is no explicit format registry or per-format schema contract.
- `TODO` No `history-doctor`, `history-bootstrap`, `r2-sync-check`, `r2-usage`, or `r2-budget-check` CLI commands exist.
- `TODO` No `format-inspect` command exists.
- `TODO` GitHub Actions does not run R2 budget or sync guard checks.
- `TODO` GitHub Actions scope is not yet documented as daily-only, not historical repair/full rebuild compute.
- `PARTIAL` Long-running commands have progress/log files in places, but no uniform logging module usage, machine-readable summary output, or `status=ok|warn|fail` final line standard.
- `TODO` Current trial `top_n_adj_close_v1` strategy is still available as a runtime strategy path and must be kept example-only/inactive by default.
- `TODO` Daily paper/backtest runtime still downloads broad/full R2 market history instead of using strategy-declared lookback windows and targeted partition loading.
- `TODO` Docs do not yet describe the future doctor/bootstrap/budget workflow.

## Implementation Phases

### Phase 1: Manifested Fetch Safety

- `DONE` Add a manifest model for one row per expected trading weekday.
- `DONE` Persist fetch manifests as `data/import/manifests/raw_fetch_NSE.parquet` and `data/import/manifests/raw_fetch_BSE.parquet`.
- `DONE` Include fetch manifest columns: `exchange`, `date`, `expected_format_id`, `expected_filename`, `expected_url_primary`, `local_path`, `status`, `http_status`, `bytes`, `sha256`, `attempts`, `last_attempt_at`, `last_error`, and `parser_hint`.
- `DONE` Support fetch status values from the audit: `expected`, `downloaded`, `skipped_existing`, `not_available`, `holiday_or_no_session`, `rate_limited`, `failed`, `corrupt_html`, `parse_failed`, and `validated`.
- `DONE` Add `history-fetch --only missing,rate_limited,failed` repair mode.
- `DONE` Add `history-fetch --fail-fast-rate-limit-ratio <ratio>`.
- `DONE` Ensure request sleep happens only after real network requests, not skipped existing files.
- `TODO` Require manifest completeness before build/upload flows that claim full history.
- `DONE` Add tests for skipped-existing sleep behavior, repair filtering, and fail-fast rate-limit policy.

Target files:

- `src/trading_infra/data/bhavcopy.py`
- `src/trading_infra/data/history.py`
- `src/trading_infra/cli.py`
- `tests/test_bhavcopy.py`
- `tests/test_history.py`
- `docs/operator-runbook.md`

### Phase 2: Explicit Bhavcopy Format Registry

- `DONE` Add an exchange format registry for NSE/BSE legacy and UDiFF/common periods.
- `DONE` Store the registry at `src/trading_infra/data/formats.yaml` or `src/trading_infra/data/formats/*.yaml`.
- `DONE` Connect filename, URL, expected date range, required columns, optional columns, and parser id through the registry.
- `DONE` Include registry fields for exchange, `format_id`, date range, filename pattern, URL patterns, required columns, optional columns, parser name, known quirks, and fixture references.
- `DONE` Make parser errors include `format_id` and required-column failures.
- `DONE` Add `format-inspect --exchange EXCHANGE --date YYYY-MM-DD`.
- `DONE` Add fixtures/tests for each registered format.
- `DONE` Document known quirks, including old NSE missing `ISIN` fallback to `SYMBOL`.

Target files:

- `src/trading_infra/data/bhavcopy.py`
- `src/trading_infra/data/formats.yaml` or `src/trading_infra/data/formats/`
- `src/trading_infra/cli.py`
- `tests/test_bhavcopy.py`
- `docs/operator-runbook.md`
- `memory/decisions/`

### Phase 3: Streaming History Verification

- `DONE` Refactor `write_history_audit` to verify monthly partitions independently.
- `DONE` Keep only compact partition summaries in memory.
- `DONE` Include per-partition schema, dtypes, row count, min/max date, duplicate keys, nulls, invalid OHLC, negative values, symbol count, file size, and SHA256.
- `DONE` Add cross-partition summaries without loading full history.
- `DONE` Add `history-verify --partition-wise`.
- `DONE` Add `history-verify --streaming` and `--max-memory-gb`; make partition-wise/streaming the documented default.
- `DONE` Hard fail if estimated verification memory exceeds the configured cap.
- `DONE` Add tests proving partition-directory verification does not require a combined frame.

Target files:

- `src/trading_infra/data/history.py`
- `src/trading_infra/cli.py`
- `tests/test_history.py`
- `docs/operator-runbook.md`

### Phase 4: Non-Destructive Build And Partition Manifests

- `DONE` Add `history-build --clean` to preserve current destructive rebuild behavior explicitly.
- `DONE` Add `history-build --incremental`.
- `DONE` Add `history-build --only-missing`.
- `DONE` Add `history-build --repair-partition EXCHANGE YEAR MONTH`.
- `DONE` Add `history-build --from-manifest`.
- `DONE` Write `data/import/manifests/partition_manifest.parquet`.
- `DONE` Include partition manifest columns: `exchange`, `year`, `month`, `partition_path`, `row_count`, `min_date`, `max_date`, `symbols`, `file_size_bytes`, `sha256`, `source_raw_count`, `parser_versions`, `created_at`, `verified_at`, and `status`.
- `DONE` Preserve source raw files, source hashes, parser version, format id, row count, min/max date, created timestamp, local file size, and SHA256 in build metadata.
- `DONE` Add targeted rebuild tests.

Target files:

- `src/trading_infra/data/history.py`
- `src/trading_infra/cli.py`
- `tests/test_history.py`
- `docs/operator-runbook.md`

### Phase 5: History Doctor

- `DONE` Add `history-doctor --exchange EXCHANGE`.
- `DONE` Add `history-doctor --compare-r2`.
- `DONE` Report expected weekdays, raw downloaded/missing/rate-limited/unparseable counts, local parquet partitions, R2 partitions, mismatches, suspicious row counts, and status.
- `DONE` Report corrupt HTML, parse failures, missing months, partition size/memory warnings, and local/R2 sync status.
- `DONE` Write `data/import/audit/history_doctor_<EXCHANGE>.json`.
- `DONE` Write `data/import/audit/history_doctor_<EXCHANGE>.md`.
- `DONE` Add unit tests with fake local/R2 state.

Target files:

- New history audit/doctor module under `src/trading_infra/data/` or `src/trading_infra/storage/`
- `src/trading_infra/cli.py`
- `tests/test_history.py`
- `tests/test_r2.py` or new focused tests
- `docs/operator-runbook.md`

### Phase 6: Streaming Upload And R2 Sync Visibility

- `DONE` Refactor `upload_verified_history` to upload partition files directly instead of reading full history into one frame.
- `DONE` Preserve staging, staged-size verification, canonical promotion, stale parquet cleanup, and manifest write.
- `DONE` Add `r2-sync-check --exchange EXCHANGE`.
- `DONE` Compare local partition manifest to R2 object existence, size, row count, and hash/etag where available.
- `DONE` Include sync report fields: local partition path, R2 key, local row count, R2 row count, local file size, R2 file size, local SHA256 or etag-equivalent metadata, manifest entry, and status.
- `DONE` Emit sync statuses such as `OK`, `MISSING`, `STALE`, and `EXTRA`.
- `TODO` Block historical upload if sync check cannot be produced.
- `TODO` Block historical upload unless source fetch manifest and partition audit pass.
- `PARTIAL` Expand R2 manifest at `data/daily_stock_data/_manifest.json` with run id, created timestamp, exchange coverage, partition list, row counts, file sizes, SHA256s if available, source local audit id, and upload status; current manifest includes run id, timestamp, exchange coverage, partition rows, source/audit path, and upload status, but not SHA256s yet.
- `DONE` Add tests for partition-directory upload without combined-frame loading and sync mismatch reporting.

Target files:

- `src/trading_infra/storage/history.py`
- `src/trading_infra/storage/r2.py`
- `src/trading_infra/cli.py`
- `tests/test_history_upload.py`
- `tests/test_r2.py`
- `docs/operator-runbook.md`

### Phase 7: R2 Usage And Budget Guardrails

- `TODO` Add object inventory usage summary from S3-compatible listing: bucket, storage bytes, and object count.
- `TODO` Add optional Cloudflare analytics API support for Class A/Class B operation counts.
- `TODO` Add `r2-usage`.
- `TODO` Add `r2-budget-check`.
- `TODO` Implement warn/fail thresholds from the audit, with config overrides if needed.
- `TODO` Check budget before bulk historical upload.
- `TODO` Optionally add GitHub Actions budget check.
- `TODO` Add monthly R2 usage snapshots.
- `TODO` Report required usage fields: bucket, storage bytes, object count, Class A operations month-to-date, Class B operations month-to-date, estimated free-tier remaining, estimated monthly cost, and status.
- `TODO` Add tests with fake R2 inventory and fake analytics responses.

Target files:

- `src/trading_infra/storage/r2.py`
- New R2 usage/budget module under `src/trading_infra/storage/`
- `src/trading_infra/cli.py`
- `.github/workflows/daily-paper.yml`
- `tests/test_r2.py`
- `docs/operator-runbook.md`

### Phase 8: Bootstrap Orchestrator

- `TODO` Add `history-bootstrap --exchange EXCHANGE --start-date DATE --end-date DATE --resume --upload false`.
- `TODO` Orchestrate expected calendar generation, format selection, fetch/repair, raw validation, partition build, streaming verification, local/R2 comparison, optional staged upload, promotion, and manifest write.
- `TODO` Make upload disabled by default and require explicit `--upload true`.
- `TODO` Add tests that mock each phase and assert safe stop behavior.

Target files:

- New orchestration module under `src/trading_infra/`
- `src/trading_infra/cli.py`
- `tests/`
- `docs/operator-runbook.md`

### Phase 9: Logging And Command Output Standards

- `TODO` Adopt the standard Python logging module for long-running commands.
- `TODO` Add log levels and a consistent event format.
- `TODO` Ensure each long-running command has a live append-only log file flushed regularly.
- `TODO` Emit a machine-readable JSON summary at command end.
- `TODO` Emit a final concise status line with `status=ok|warn|fail`.
- `TODO` Preserve human progress via `tqdm` or concise progress lines.
- `TODO` Add tests for JSON summaries/status lines where practical.

Target files:

- `src/trading_infra/cli.py`
- Existing data/storage modules touched by long-running commands
- `tests/test_cli.py`
- `docs/operator-runbook.md`

### Phase 10: Strategy And Runtime Safety

- `TODO` Keep `top_n_adj_close_v1` under `examples/` only.
- `TODO` Ensure the default/example registry does not mark trial strategy active by default.
- `TODO` Keep trial strategy usage limited to tests and workflow smoke checks.
- `TODO` Avoid treating row iteration in the top-N trial strategy as a pattern for heavy code.
- `TODO` Add strategy-declared lookback windows.
- `TODO` Avoid full-history R2 download for daily paper runs when lookback/partition bounds are available.
- `TODO` Parallelize R2 partition downloads only if profiling or scale requires it.
- `TODO` Parallelize independent strategy runs only if strategy count grows enough to justify it.
- `TODO` Keep corporate-action adjustment source selection as an open durable decision until resolved.
- `TODO` Add tests/docs for inactive trial strategy behavior and lookback-bounded loading when implemented.

Target files:

- `examples/`
- `src/trading_infra/registry.py`
- `src/trading_infra/strategy.py`
- `src/trading_infra/strategy_builder.py`
- `src/trading_infra/pipelines/paper.py`
- `src/trading_infra/storage/remote.py`
- `tests/`
- `docs/operator-runbook.md`
- `docs/progress-checklist.md`
- `memory/decisions/`

### Phase 11: GitHub Actions Scope And Maintenance

- `TODO` Document that GitHub Actions is for daily refresh/paper only.
- `TODO` Explicitly keep full historical download, full rebuild, full backup verification, large backtests, parameter sweeps, and model training out of GitHub Actions.
- `TODO` Add R2 budget/sync checks to Actions only where they are cheap and daily-safe.
- `TODO` Add a dependency lockfile if the project packaging approach supports it cleanly.

Target files:

- `.github/workflows/daily-paper.yml`
- `docs/operator-runbook.md`
- `docs/progress-checklist.md`
- dependency metadata/lockfile

### Phase 12: Docs, Progress, And Memory

- `TODO` Update `docs/operator-runbook.md` after each CLI behavior change.
- `TODO` Update `docs/progress-checklist.md` when audit-driven phases move from TODO/PARTIAL to DONE.
- `TODO` Add durable decisions for format registry shape, manifest format, and R2 budget thresholds when implemented.
- `TODO` Add timestamped session logs for implementation chunks.
- `TODO` Keep this checklist current after each scoped commit.

## Audit Traceability Matrix

- `COVERED` Audit 4.1, No R2 budget and usage tracking: Phase 7.
- `COVERED` Audit 4.2, Historical ingestion is too manual: Phases 1, 5, 8, and 12.
- `COVERED` Audit 4.3, No single local health check: Phase 5.
- `COVERED` Audit 4.4, NSE rate limit handling is incomplete: Phase 1.
- `COVERED` Audit 4.5, BSE has same correctness visibility problem: Phases 1, 2, 5, and 6 apply to both NSE and BSE.
- `COVERED` Audit 4.6, NSE/BSE format handling is not explicit enough: Phase 2.
- `COVERED` Audit 4.7, Parquet verification is too memory-heavy: Phase 3.
- `COVERED` Audit 4.8, Parquet rebuild process is too destructive: Phase 4.
- `COVERED` Audit 4.9, Local/R2 sync is not visible enough: Phase 6.
- `COVERED` Audit 4.10, Current trial strategy should be removed from runtime path: Phase 10.
- `COVERED` Audit 4.11, Logging/output standards are partial: Phase 9.
- `COVERED` Audit 4.12, GitHub Actions is okay for daily processing, not historical repair: Phase 11.
- `COVERED` Audit 5.1, Fetch manifest artifact: Phase 1.
- `COVERED` Audit 5.2, Partition manifest artifact: Phase 4.
- `COVERED` Audit 5.3, R2 manifest artifact: Phase 6.
- `COVERED` Audit 5.4, Format registry artifact: Phase 2.
- `COVERED` Audit 6 P0 commands: `history-doctor` in Phase 5, `history-verify --partition-wise` in Phase 3, and `r2-sync-check` in Phase 6.
- `COVERED` Audit 6 P1 commands: `history-bootstrap` in Phase 8, `history-build --repair-partition` and build modes in Phase 4, and `history-fetch --only` in Phase 1.
- `COVERED` Audit 6 P2 commands: `r2-usage` and `r2-budget-check` in Phase 7, and `format-inspect` in Phase 2.
- `COVERED` Audit 7 P0 correctness and safety: Phases 1, 2, 3, 5, and 6.
- `COVERED` Audit 7 P1 operator usability: Phases 1, 4, 8, and 9.
- `COVERED` Audit 7 P2 budget and maintenance: Phases 7, 10, and 11.
- `COVERED` Audit 7 P3 strategy/runtime improvements: Phase 10.
- `COVERED` Audit 8 acceptance criteria: Acceptance Criteria Coverage section below.
- `PRESERVED` Audit 1-3 context and strengths: README remains the architecture source of truth; existing code partitioning, Polars/Parquet, monthly partitioning, daily refresh, progress support, and tests are treated as constraints to preserve while implementing phases.
- `PRESERVED` Audit 9 revised verdict: The checklist prioritizes manifest-driven, format-explicit, partition-wise, memory-bounded, rate-limit-aware, R2-sync-aware, operator-safe historical ingestion before final strategy evaluation.

## Acceptance Criteria Coverage

- `TODO` `history-doctor` returns `status=ok` for NSE and BSE.
- `TODO` Raw fetch manifests have no unresolved `failed` or `rate_limited` rows.
- `TODO` Partition manifests cover expected months.
- `TODO` Partition-wise verify completes under the configured memory limit.
- `TODO` R2 sync check returns zero missing/stale canonical partitions.
- `TODO` `history-upload` refuses incomplete source data.
- `TODO` R2 usage check reports storage and operations within budget thresholds.
- `TODO` GitHub Actions daily run succeeds on a known trading date.
- `TODO` Holiday/no-data date exits cleanly without false failure.
- `TODO` Trial strategy is not active by default.

## Commit Plan

- `DONE` Commit audit implementation plan and memory index update.
- `TODO` Commit manifest fetch safety as a focused `data` or `fix` commit.
- `TODO` Commit format registry as a focused `data` commit.
- `TODO` Commit streaming verification as a focused `data` commit.
- `TODO` Commit build modes and partition manifests as a focused `data` commit.
- `TODO` Commit history doctor as a focused `data` commit.
- `TODO` Commit streaming upload and R2 sync check as focused `infra` commits.
- `TODO` Commit R2 usage/budget checks as focused `infra` commits.
- `TODO` Commit bootstrap orchestrator as a focused `data` or `infra` commit.
- `TODO` Commit logging/output standards as focused `infra` or `chore` commits.
- `TODO` Commit trial strategy/runtime safety as focused `strategy` or `paper` commits.
- `TODO` Commit GitHub Actions scope/dependency lockfile maintenance as focused `infra` or `chore` commits.
- `TODO` Commit docs/progress/memory updates with the relevant implementation commits where practical.
