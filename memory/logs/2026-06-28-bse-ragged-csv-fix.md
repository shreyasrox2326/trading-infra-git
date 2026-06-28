# BSE Ragged CSV Fix

Date: 2026-06-28

During local `history-build`, a BSE CSV failed with Polars `found more fields than defined in 'Schema'`.

Fix:

- Enabled `truncate_ragged_lines=True` for bhavcopy CSV reads.
- Added a regression test for a legacy BSE CSV row with an extra trailing field.

Verification:

- `python -m pytest -q` passed: 104 tests.
