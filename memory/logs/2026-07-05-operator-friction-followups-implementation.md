# Operator Friction Follow-Ups Implementation

Date: 2026-07-05

## Summary

- Added `history-manifest-mark`, `history-manifest-combine`, `history-partition-manifest-refresh`, and `r2-cleanup-staging` CLI commands.
- Added reviewed-status override support to `history-bootstrap` via `--allow-fetch-status`, with allowed statuses recorded in bootstrap summary steps.
- Changed `history-build` safety behavior so exchange-scoped `--clean` preserves unrelated exchange trees and partition manifest refresh now reflects the full on-disk partition set.
- Improved `history-doctor` reporting with `raw_skipped_existing`, `raw_validated`, and `raw_usable`.
- Changed historical upload verification/promotion to use R2 metadata plus server-side copy instead of staging-download-then-reupload promotion.
- Updated GitHub Actions daily workflow to install pinned dependencies from `requirements.lock` and upload results by default on scheduled runs.
- Updated `docs/operator-runbook.md` and `AGENTS.md` with clearer operator flow and container-shell guidance.

## Verification

- Ran `pytest -q tests/test_history.py tests/test_history_upload.py`
- Result: `38 passed`
