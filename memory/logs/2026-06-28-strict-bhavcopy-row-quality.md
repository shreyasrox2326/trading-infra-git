# Strict Bhavcopy Row Quality

Date: 2026-06-28

Tightened local history build behavior after BSE malformed CSV rows appeared during full build.

Policy:

- CSV reader may tolerate ragged file shape to recover parseable rows.
- Canonical output must not silently drop incomplete market rows.
- If normalized row count differs from raw row count for a source file, the build fails with the exact file path.

Verification:

- `python -m pytest -q` passed: 105 tests.
