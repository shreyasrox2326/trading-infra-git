# Operator Friction And Follow-Ups

Date: 2026-07-05

Context: After implementing the audit checklist, the operator ran full NSE/BSE historical fetch/build/verify/doctor/upload preparation and inspected the GitHub Actions daily run. Local history reached a valid state, but several operator-flow and tooling issues became clear. These are not fully resolved in code and should be considered future work.

## Current Good State From Session

- Local combined history was rebuilt from raw NSE+BSE files with `history-build --clean`.
- Latest verified local history had 27,379,551 rows across 616 monthly partitions.
- `history-verify` passed with zero duplicate keys and zero invalid OHLC rows.
- `history-doctor` passed for NSE and BSE after the known NSE archive gap was reclassified.
- R2 budget check worked after loading `.env` with POSIX `. ./.env`.
- Historical upload was not run by the agent after the user said not to upload.
- `history-upload` was later enhanced with bounded parallelism and tqdm progress bars, but it still uses staging/download-size-verify/reupload promotion.

## Operator/Tooling Issues Observed

- The operator runbook is still too much like a command catalog. It needs a clearer "what to do now" flow for full history bootstrap, manual build/verify/doctor, upload readiness, and post-upload checks.
- `history-bootstrap` is too strict for real archive gaps. It stops on any `failed` or `rate_limited` manifest row, even when the operator wants to consciously proceed after accepting an archive gap.
- There is no auditable command to reclassify a raw manifest row. The operator had to manually mark `1995-09-06` as `not_available` using a Polars script.
- Uploading both exchanges together requires one raw manifest, but normal fetch produces `raw_fetch_NSE.parquet` and `raw_fetch_BSE.parquet`. The operator had to manually create `raw_fetch_ALL.parquet`.
- `history-build --incremental --exchange NSE` preserved existing BSE parquet files but rewrote `partition_manifest.parquet` with only NSE rows. This made the data directory and partition manifest disagree until a clean combined rebuild was run.
- `history-build --clean --exchange <one exchange>` cleans the whole output path, not just that exchange. This is easy to misuse when trying to maintain a combined NSE+BSE history tree.
- `history-verify` and `history-doctor` caused confusion. Verify can pass while doctor fails because verify checks parquet validity, while doctor checks manifest/pipeline completeness.
- `history-doctor` output can be misleading after resume-heavy fetches. `raw_downloaded=0` looks bad even when all usable rows are `skipped_existing`; it should surface a `raw_usable` count.
- Historical and daily refresh staging objects under `_staging/...` are not deleted automatically. This is safe for debugging but creates cleanup debt.
- Historical upload is still mechanically inefficient: it uploads to staging, downloads staged objects for size verification, then uploads the same local files again to canonical keys. The safer/faster future design is `head_object` verification plus server-side `copy_object` promotion.
- GitHub Actions daily workflow ran successfully for market refresh, but scheduled run logs showed `UPLOAD_RESULTS=false` and `paper-dry-run ... uploaded=false`. Paper decisions were not uploaded even though scheduled runs should likely upload by default.
- GitHub Actions installs latest compatible dependencies via `pip install -e .`, not the local lockfile. This creates dependency drift risk.
- Shell quoting was a repeated operator/agent friction point when nesting PowerShell -> Docker -> `sh -lc` -> inline Python. The reliable workaround was piping scripts over stdin to `docker exec -i ... python -`.
- `source .env` failed under container `sh`; POSIX `. ./.env` worked. Docs should either use POSIX syntax or explicitly require Bash.
- Large patch application failed repeatedly for one `storage/history.py` edit, requiring smaller surgical patches. This is agent/tooling friction, not repo behavior, but it matters for future agents editing large blocks.

## Potential Fixes

- Add an operator flow page or a top-level runbook section: "Full History From Raw To Upload" with exact phases, stop conditions, and next command after each phase.
- Add `history-bootstrap --ignore-fetch-status failed,rate_limited` or `--allow-incomplete-fetch`, with the ignored statuses recorded in the final summary.
- Add `history-manifest-mark --manifest-path ... --date YYYY-MM-DD --status not_available --reason ...` so operator overrides are explicit and auditable.
- Add `history-manifest-combine --output raw_fetch_ALL.parquet raw_fetch_NSE.parquet raw_fetch_BSE.parquet`.
- Add `history-partition-manifest-refresh --history-path ...` to regenerate `partition_manifest.parquet` from existing partition files without rebuilding data.
- Make `history-build --clean --exchange X` require a stronger confirmation flag or change clean behavior to clean only selected exchanges.
- Update doctor output with `raw_usable`, `raw_skipped_existing`, and clearer fail reasons.
- Add `r2-cleanup-staging --prefix _staging/history-load/ --older-than Nd --dry-run` and an equivalent daily-refresh staging cleanup mode.
- Add `R2Client.head_object` and `R2Client.copy_object`; change historical upload promotion to server-side copy after metadata verification.
- Fix GitHub Actions scheduled `UPLOAD_RESULTS` default so scheduled runs upload paper decisions unless manually disabled.
- Consider using `requirements.lock` or pinned constraints in GitHub Actions install.
- Prefer POSIX `. ./.env` in docs and examples that run under `/bin/sh`.
