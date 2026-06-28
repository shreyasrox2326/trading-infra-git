# Full README Alignment Implementation

Date: 2026-06-28

Implemented the local-first historical data bootstrap and daily cloud maintenance foundation:

- Added exchange-aware NSE/BSE bhavcopy filename, fetch, and normalization support for legacy and UDiFF/common formats.
- Added `history-fetch`, `history-build`, `history-verify`, and `history-upload` CLI commands.
- Added local audit JSON/Markdown generation for canonical historical market data.
- Added staged and verified R2 historical upload with manifest generation.
- Added `market-data-refresh` for one-date, one-exchange R2 monthly partition refresh.
- Updated GitHub Actions to refresh market data before paper evaluation and skip paper on `no_data`.
- Updated README, operator runbook, strategy contract, progress checklist, and full alignment checklist.
- Added parser, history build/audit, upload safety, refresh, and workflow tests.

Verification:

- `python -m pytest -q` passed: 101 tests.

Remaining user-dependent steps:

- Run full local NSE 1994+ and BSE 2007+ history fetch/build.
- Inspect and approve `history_audit`.
- Run one-time verified `history-upload`.
- Configure/confirm GitHub Actions secrets and first manual workflow run.
- Decide corporate-action adjustment source and ML runtime contract.
