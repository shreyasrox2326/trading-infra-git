# Partition-First History Build

Date: 2026-06-28

Reworked `history-build` for full local NSE/BSE assembly:

- Builds local monthly partitions shaped like R2 instead of materializing one giant full-history frame.
- Writes timestamped phase logs with normalization and merge progress.
- Uses fragment parquet files during normalization and merges one exchange/year/month partition at a time.
- `history-verify` and `history-upload` can read the partition directory directly.
- BSE HTML/error pages saved as `.CSV` or `.ZIP` are skipped as non-bhavcopy files.
- BSE legacy dates can be inferred from `EQDDMMYY_CSV.ZIP` filenames.
- BSE `#` symbol duplicate variants are dropped only when the plain symbol exists for the same README key.
- Incomplete non-empty market rows still fail the build.

Verification:

- BSE-only real build smoke completed: 15,647,010 rows, 234 monthly partitions, 314 non-bhavcopy files skipped.
- `python -m pytest -q` passed: 112 tests.
