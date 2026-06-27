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
7. The full flow works for ML strategies, not only the example `top_n_adj_close` rule strategy.

## Phase Checklist

### Phase 1: Storage And Contracts

- `DONE` README architecture, R2 layout, decision schema, and registry contract are documented.
- `DONE` Strategy, registry, decision, and market-data upload paths exist.
- `DONE` R2-backed backtest and paper flows use R2 as the canonical source.

### Phase 2: Historical Market Data On R2

- `DONE` Canonical market-data upload command exists: `python -m trading_infra market-data-upload`.
- `DONE` Upload rewrites canonical `exchange/year/month/part.parquet` partitions and clears stale parquet objects.
- `DONE` Historical NSE bhavcopy fetch and canonical ingestion commands exist.
- `DONE` Raw NSE equity bhavcopy archives can be fetched into ignored local operator state.
- `DONE` Raw NSE equity bhavcopies can be normalized into canonical parquet with identity adjustments.
- `DONE` R2 contains verified NSE history: 5,786,247 rows, 126 monthly partitions, 2016-01-01 to 2026-06-25.
- `PARTIAL` Delivery fields are nullable when the raw bhavcopy source does not include them.
- `TODO` Corporate-action adjustment data is not integrated.
- `PARTIAL` The source adapter handles NSE historical and UDiFF bhavcopy files, but daily automated refresh is not wired into GitHub Actions yet.

### Phase 3: Local Strategy Development And Backtest

- `DONE` Local backtests work with local parquet inputs.
- `DONE` R2-backed backtests now download strategy artifacts from R2 and use full prior history up to `end-date`.
- `DONE` Backtest decisions can be validated and uploaded to R2.
- `PARTIAL` Current runtime only supports one concrete strategy type: `top_n_adj_close`.

### Phase 4: Daily Paper Automation

- `DONE` GitHub Actions can run a scheduled or manual R2-backed paper job.
- `DONE` Paper history append behavior is idempotent and preserves prior history.
- `DONE` R2-backed paper runs use full prior market history up to the decision date.
- `PARTIAL` The workflow assumes market data is already updated on R2 before the paper job runs.
- `TODO` Automated daily market-data fetch/update step is not implemented in the workflow.

### Phase 5: ML Strategy Readiness

- `DONE` Storage contract allows `model.pkl` and `feature_config.yaml`.
- `DONE` Strategy artifact upload supports optional model and feature-config files.
- `TODO` Strategy builder/runtime does not load or execute any ML strategy type.
- `TODO` No feature generation, model loading, or inference contract exists in code.
- `TODO` No backtest or paper tests cover ML strategies.

## Current Misalignments Against README Target

These are the active gaps between the documented target state and the current implementation.

1. Daily market-data auto-update is missing.
   README says GitHub Actions updates latest `daily_stock_data` on R2 before paper decisions. The workflow currently only runs `paper-dry-run --use-r2 --upload-results`.

2. ML strategy execution is missing.
   The README and strategy contract allow model artifacts, but runtime code only supports `top_n_adj_close`.

3. Daily market-data refresh uses local/operator commands only.
   Historical R2 data is loaded and verified, but GitHub Actions does not yet fetch the next bhavcopy, ingest it, and upload the updated market-data partition before paper decisions.

## Immediate Next Milestones

1. Add GitHub Actions support for daily market-data refresh before paper execution.
2. Add an ML strategy execution type to `strategy_builder.py` and the runtime contract.
3. Add an end-to-end ML deployment test:
   local backtest -> upload strategy/model -> upload backtest decisions -> activate registry -> daily paper run.
