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

- `DONE` Historical fetch exists in `fetch_bhavcopy_archives` and `history-fetch`, with retries, per-date statuses, progress, logs, manifests, repair mode, and rate-limit guardrails.
- `DONE` Historical build is partition-first and supports clean, incremental, only-missing, repair-partition, build-from-manifest, and partition manifests.
- `DONE` Historical verification is partition-wise, memory-capped, and emits per-partition metadata.
- `DONE` Historical upload stages/promotes partitions without loading full history and requires clean source manifests.
- `DONE` NSE/BSE legacy and UDiFF filenames/parsers have an explicit format registry and per-format schema contract.
- `DONE` `history-doctor`, `history-bootstrap`, `r2-sync-check`, `r2-usage`, and `r2-budget-check` CLI commands exist.
- `DONE` `format-inspect` command exists.
- `DONE` GitHub Actions runs a cheap R2 budget guard check.
- `DONE` GitHub Actions scope is documented as daily-only, not historical repair/full rebuild compute.
- `DONE` Long-running commands have progress/log support plus logging, machine-readable summaries, and `status=ok|warn|fail` lines.
- `DONE` Current trial `top_n_adj_close_v1` is tracked only under `examples/` and remains draft/example-only by default.
- `DONE` Daily paper avoids full-history R2 downloads when active strategies declare bounded lookbacks.
- `DONE` Docs describe the doctor/bootstrap/budget workflow.

## Implementation Phases

### Phase 1: Manifested Fetch Safety

- `DONE` Add a manifest model for one row per expected trading weekday.
- `DONE` Persist fetch manifests as `data/import/manifests/raw_fetch_NSE.parquet` and `data/import/manifests/raw_fetch_BSE.parquet`.
- `DONE` Include fetch manifest columns: `exchange`, `date`, `expected_format_id`, `expected_filename`, `expected_url_primary`, `local_path`, `status`, `http_status`, `bytes`, `sha256`, `attempts`, `last_attempt_at`, `last_error`, and `parser_hint`.
- `DONE` Support fetch status values from the audit: `expected`, `downloaded`, `skipped_existing`, `not_available`, `holiday_or_no_session`, `rate_limited`, `failed`, `corrupt_html`, `parse_failed`, and `validated`.
- `DONE` Add `history-fetch --only missing,rate_limited,failed` repair mode.
- `DONE` Add `history-fetch --fail-fast-rate-limit-ratio <ratio>`.
- `DONE` Ensure request sleep happens only after real network requests, not skipped existing files.
- `DONE` Require manifest completeness before build/upload flows that claim full history.
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
- `DONE` Block historical upload if sync check cannot be produced.
- `DONE` Block historical upload unless source fetch manifest and partition audit pass.
- `DONE` Expand R2 manifest at `data/daily_stock_data/_manifest.json` with run id, created timestamp, exchange coverage, partition list, row counts, file sizes, SHA256s if available, source local audit id, and upload status.
- `DONE` Add tests for partition-directory upload without combined-frame loading and sync mismatch reporting.

Target files:

- `src/trading_infra/storage/history.py`
- `src/trading_infra/storage/r2.py`
- `src/trading_infra/cli.py`
- `tests/test_history_upload.py`
- `tests/test_r2.py`
- `docs/operator-runbook.md`

### Phase 7: R2 Usage And Budget Guardrails

- `DONE` Add object inventory usage summary from S3-compatible listing: bucket, storage bytes, and object count.
- `DONE` Add optional Cloudflare analytics API support for Class A/Class B operation counts through nullable operation fields in the usage schema; direct analytics API wiring remains optional configuration work.
- `DONE` Add `r2-usage`.
- `DONE` Add `r2-budget-check`.
- `DONE` Implement warn/fail thresholds from the audit, with config overrides if needed.
- `DONE` Check budget before bulk historical upload.
- `DONE` Optionally add GitHub Actions budget check.
- `DONE` Add monthly R2 usage snapshots.
- `DONE` Report required usage fields: bucket, storage bytes, object count, Class A operations month-to-date, Class B operations month-to-date, estimated free-tier remaining, estimated monthly cost, and status.
- `DONE` Add tests with fake R2 inventory and fake analytics responses.

Target files:

- `src/trading_infra/storage/r2.py`
- New R2 usage/budget module under `src/trading_infra/storage/`
- `src/trading_infra/cli.py`
- `.github/workflows/daily-paper.yml`
- `tests/test_r2.py`
- `docs/operator-runbook.md`

### Phase 8: Bootstrap Orchestrator

- `DONE` Add `history-bootstrap --exchange EXCHANGE --start-date DATE --end-date DATE --resume --upload false`.
- `DONE` Orchestrate expected calendar generation, format selection, fetch/repair, raw validation, partition build, streaming verification, local/R2 comparison, optional staged upload, promotion, and manifest write.
- `DONE` Make upload disabled by default and require explicit `--upload true`.
- `DONE` Add tests that mock each phase and assert safe stop behavior.

Target files:

- New orchestration module under `src/trading_infra/`
- `src/trading_infra/cli.py`
- `tests/`
- `docs/operator-runbook.md`

### Phase 9: Logging And Command Output Standards

- `DONE` Adopt the standard Python logging module for long-running commands.
- `DONE` Add log levels and a consistent event format.
- `DONE` Ensure each long-running command has a live append-only log file flushed regularly.
- `DONE` Emit a machine-readable JSON summary at command end.
- `DONE` Emit a final concise status line with `status=ok|warn|fail`.
- `DONE` Preserve human progress via `tqdm` or concise progress lines.
- `DONE` Add tests for JSON summaries/status lines where practical.

Target files:

- `src/trading_infra/cli.py`
- Existing data/storage modules touched by long-running commands
- `tests/test_cli.py`
- `docs/operator-runbook.md`

### Phase 10: Strategy And Runtime Safety

- `DONE` Keep `top_n_adj_close_v1` under `examples/` only.
- `DONE` Ensure the default/example registry does not mark trial strategy active by default.
- `DONE` Keep trial strategy usage limited to tests and workflow smoke checks.
- `DONE` Avoid treating row iteration in the top-N trial strategy as a pattern for heavy code.
- `DONE` Add strategy-declared lookback windows.
- `DONE` Avoid full-history R2 download for daily paper runs when lookback/partition bounds are available.
- `DONE` Parallelize R2 partition downloads only if profiling or scale requires it.
- `DONE` Parallelize independent strategy runs only if strategy count grows enough to justify it.
- `DONE` Keep corporate-action adjustment source selection as an open durable decision until resolved.
- `DONE` Add tests/docs for inactive trial strategy behavior and lookback-bounded loading when implemented.

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

- `DONE` Document that GitHub Actions is for daily refresh/paper only.
- `DONE` Explicitly keep full historical download, full rebuild, full backup verification, large backtests, parameter sweeps, and model training out of GitHub Actions.
- `DONE` Add R2 budget/sync checks to Actions only where they are cheap and daily-safe.
- `DONE` Add a dependency lockfile if the project packaging approach supports it cleanly.

Target files:

- `.github/workflows/daily-paper.yml`
- `docs/operator-runbook.md`
- `docs/progress-checklist.md`
- dependency metadata/lockfile

### Phase 12: Docs, Progress, And Memory

- `DONE` Update `docs/operator-runbook.md` after each CLI behavior change.
- `DONE` Update `docs/progress-checklist.md` when audit-driven phases move from TODO/PARTIAL to DONE.
- `DONE` Add durable decisions for format registry shape, manifest format, and R2 budget thresholds when implemented.
- `DONE` Add timestamped session logs for implementation chunks.
- `DONE` Keep this checklist current after each scoped commit.

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

- `DONE` `history-doctor` returns `status=ok` for NSE and BSE when local manifests and partitions are complete.
- `DONE` Raw fetch manifests have no unresolved `failed` or `rate_limited` rows before upload.
- `DONE` Partition manifests cover expected built months.
- `DONE` Partition-wise verify completes under the configured memory limit.
- `DONE` R2 sync check returns zero missing/stale canonical partitions when R2 matches the local manifest.
- `DONE` `history-upload` refuses incomplete source data.
- `DONE` R2 usage check reports storage and operations within budget thresholds.
- `DONE` GitHub Actions daily workflow includes install, R2 budget check, daily refresh, and paper run steps.
- `DONE` Holiday/no-data date exits cleanly without false failure through existing market refresh behavior.
- `DONE` Trial strategy is not active by default in tracked example artifacts.

## Commit Plan

- `DONE` Commit audit implementation plan and memory index update.
- `DONE` Commit manifest fetch safety as a focused `data` or `fix` commit.
- `DONE` Commit format registry as a focused `data` commit.
- `DONE` Commit streaming verification as a focused `data` commit.
- `DONE` Commit build modes and partition manifests as a focused `data` commit.
- `DONE` Commit history doctor as a focused `data` commit.
- `DONE` Commit streaming upload and R2 sync check as focused `infra` commits.
- `DONE` Commit R2 usage/budget checks as focused `infra` commits.
- `DONE` Commit bootstrap orchestrator as a focused `data` or `infra` commit.
- `DONE` Commit logging/output standards as focused `infra` or `chore` commits.
- `DONE` Commit trial strategy/runtime safety as focused `strategy` or `paper` commits.
- `DONE` Commit GitHub Actions scope/dependency lockfile maintenance as focused `infra` or `chore` commits.
- `DONE` Commit docs/progress/memory updates with the relevant implementation commits where practical.
