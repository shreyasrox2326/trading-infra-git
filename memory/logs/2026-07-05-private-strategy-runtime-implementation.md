# Private Strategy Runtime Implementation

Date: 2026-07-05

Implemented public support for private pickle-backed strategies and local/cloud performance computation.

## Public repo changes

- Added `private_pickle_v1` runtime loading through `strategy_builder.py`.
- Added `PrivateStrategyRuntimeV1` and `PrivatePickleStrategy`.
- Added realized-performance computation plus CLI commands:
  - `performance-compute`
  - `performance-refresh`
- Updated docs:
  - `README.md`
  - `docs/strategy-contract.md`
  - `docs/progress-checklist.md`
  - `docs/operator-runbook.md`
- Added targeted tests for mock private artifact loading and performance computation.
- Fixed lock-style dependency installation by correcting the `s3transfer` pin.

## Local private operator state created

- Ignored private source builder:
  - `strategies/private_sources/build_v1_v2.py`
- Ignored strategy artifacts:
  - `strategies/v1_regime_sideways_etf_rotation_long/`
  - `strategies/v2_rotation_or_shock_overlay_long/`
- Ignored local registry:
  - `registry/strategies.parquet`

## Local verification

- `pytest -q tests/test_private_pickle_and_performance.py tests/test_history.py tests/test_history_upload.py`
- Local backtest smoke:
  - both private strategies over June 1, 2026 to June 25, 2026
- Local paper smoke:
  - both strategies emitted one row for June 25, 2026
- Local realized-performance smoke:
  - both strategies produced `daily.parquet` and `summary.json`
- Mocked cloud-style test coverage:
  - R2-backed private paper execution and upload path
  - `performance-refresh` upload path

## Notes

- The current private descendants are intentionally trimmed to long-only ETF overlays because the public infra does not support shorts.
- R2-backed daily wiring exists in public code and GitHub Actions, but the exact private strategy upload and cloud run were not exercised in this session.
