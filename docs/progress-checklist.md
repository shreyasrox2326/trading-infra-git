# Progress Checklist

This file is the operational progress tracker for `trading-infra-git`.

Use it as the first status check after `README.md` when an agent or operator needs to answer:

- what is already working
- what is only partially working
- what is still missing before the README target state is real

Update this file whenever a phase meaningfully changes status.

Status labels:

- `DONE`: implemented and verified enough to rely on
- `PARTIAL`: some path exists but the README target state is not fully satisfied
- `BLOCKED`: cannot complete without missing data, missing code, or external setup
- `TODO`: not implemented yet

## Goal State

Target operating state from the README:

1. Historical daily stock data is stored on R2 in canonical monthly parquet partitions.
2. Strategy artifacts live on R2 and can include `config.yaml`, `metadata.json`, `model.pkl`, and `feature_config.yaml`.
3. A strategy can be backtested locally against historical R2-backed data.
4. Backtest decisions can be uploaded to R2 as canonical `decisions.parquet`.
5. A strategy can be activated in the registry.
6. GitHub Actions can fetch or update daily market data, run active strategies, append daily paper decisions, and upload the updated result.
7. Optional ML artifacts can be stored; ML execution requires a separate runtime contract before it is considered complete.

## Phase Checklist

### Phase 1: Storage And Contracts

- `DONE` README architecture, R2 layout, decision schema, and registry contract are documented.
- `DONE` Strategy, registry, decision, and market-data upload paths exist.
- `DONE` R2-backed backtest and paper flows use R2 as the canonical source.
- `DONE` README and operator docs distinguish local full-history bootstrap, verified R2 upload, daily cloud refresh, and local/cloud compute split.

### Phase 2: Historical Market Data On R2

- `DONE` Canonical market-data upload command exists: `python -m trading_infra market-data-upload`.
- `DONE` Verified full-history upload command exists: `python -m trading_infra history-upload`.
- `DONE` Historical NSE and BSE bhavcopy fetch/build commands exist.
- `DONE` Raw NSE and BSE bhavcopy archives can be fetched into ignored local operator state.
- `DONE` Raw NSE and BSE bhavcopies can be normalized into canonical parquet with identity adjustments.
- `DONE` Historical fetch/build/verify/upload now use explicit format registry, raw fetch manifests, partition manifests, partition-wise verification, doctor reports, R2 sync checks, and upload guardrails.
- `PARTIAL` R2 currently contains verified NSE history: 5,786,247 rows, 126 monthly partitions, 2016-01-01 to 2026-06-25. Full NSE 1994+ and BSE 2007+ bootstrap still requires local assembly, user audit approval, and one-time verified upload.
- `PARTIAL` Delivery fields are nullable when the raw bhavcopy source does not include them.
- `TODO` Corporate-action adjustment data is not integrated.
- `DONE` The source adapter handles NSE/BSE legacy and UDiFF/common bhavcopy files.

### Phase 3: Local Strategy Development And Backtest

- `DONE` Local backtests work with local parquet inputs.
- `DONE` R2-backed backtests now download strategy artifacts from R2 and use full prior history up to `end-date`.
- `DONE` Backtest decisions can be validated and uploaded to R2.
- `DONE` Runtime supports public `top_n_adj_close` strategies and private pickle-backed `private_pickle_v1` strategies.
- `DONE` Local backtests now load market data in chunks and reuse one precomputed private runtime per chunk for `private_pickle_v1`.

### Phase 4: Daily Paper Automation

- `DONE` GitHub Actions can run a scheduled or manual R2-backed paper job.
- `DONE` Paper history append behavior is idempotent and preserves prior history.
- `DONE` R2-backed paper runs use full prior market history up to the decision date.
- `DONE` The workflow refreshes market data on R2 before paper evaluation.
- `DONE` Automated daily market-data refresh is wired into GitHub Actions.
- `DONE` The workflow now refreshes realized paper performance daily through `performance-refresh`.

### Phase 5: ML Strategy Readiness

- `DONE` Storage contract allows `model.pkl` and `feature_config.yaml`.
- `DONE` Strategy artifact upload supports optional model and feature-config files.
- `DONE` Strategy builder/runtime loads and executes `private_pickle_v1` artifacts through a stable in-process runtime contract.
- `DONE` Public runtime now exposes reusable market-data and precomputed feature-table access for private strategies.
- `DONE` Targeted tests cover mock private strategy loading, local paper execution, mocked R2-backed paper execution/upload, and cloud-side performance refresh wiring.

## Current Misalignments Against README Target

These are the active gaps between the documented target state and the current implementation.

1. Full historical bootstrap is not complete on R2.
   The local build/verify/upload commands exist, but the user still needs to assemble full NSE 1994+ and BSE 2007+ data locally, inspect the audit, and approve the one-time R2 replacement.

2. Private strategy execution is narrow.
   The runtime now supports `private_pickle_v1`, but the public contract is intentionally small and exposes only market-data slices plus trading dates.

3. Corporate-action adjustment is identity-only.
   No corporate-action source or back-adjustment method has been selected.

## Immediate Next Milestones

1. Run `history-bootstrap --upload false` for local full-history NSE+BSE assembly and inspect `history_audit` plus `history_doctor`.
2. Approve and run one-time guarded `history-upload` to replace/extend R2 market data.
3. Decide corporate-action adjustment source and method.
4. Broaden the private runtime only when a real strategy needs another stable method.
