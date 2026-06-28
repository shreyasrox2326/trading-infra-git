# History Fetch Optimization

Date: 2026-06-28

Optimized long-running raw bhavcopy fetches before the local full-history rebuild:

- Added bounded parallel downloads for `bhavcopy-fetch` and `history-fetch`.
- Added retry/backoff controls.
- Added `tqdm` progress bars.
- Kept resumability through existing-file skips unless `--overwrite` is passed.
- Added `history-fetch` per-date log output.
- Updated the operator runbook to recommend `tmux`, `--workers 8`, retries, and log paths for full NSE/BSE history downloads.

Verification:

- `python -m pytest -q` passed: 103 tests.
