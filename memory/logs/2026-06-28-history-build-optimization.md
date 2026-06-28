# History Build Optimization

Date: 2026-06-28

Optimized the local canonical history build path before running full NSE/BSE assembly:

- Refactored bhavcopy normalization to support per-file normalization.
- Added bounded parallel parsing to `history-build`.
- Added `tqdm` progress bars for build runs.
- Added `--workers`, `--progress`, and `--no-progress` CLI options.
- Updated the operator runbook to use `history-build --workers 4`.

Verification:

- `python -m pytest tests/test_bhavcopy.py tests/test_history.py -q` passed: 22 tests.
