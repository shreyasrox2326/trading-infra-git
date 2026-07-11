# Backtesting / Paper-Trading Infrastructure Notes

```trading-infra-git ```

The system is for **pure technical, individual-stock strategies**.

Each strategy uses only historical price-volume data for each stock. No fundamentals, news, options, tick data, or order book data are included initially.

## Getting Started

Operator and setup docs live in:

- `docs/operator-runbook.md`
- `docs/strategy-contract.md`
- `docs/progress-checklist.md`

Primary local CLI entrypoints:

- `python -m trading_infra backtest-run`
- `python -m trading_infra paper-dry-run`
- `python -m trading_infra bhavcopy-fetch`
- `python -m trading_infra bhavcopy-ingest`
- `python -m trading_infra history-fetch`
- `python -m trading_infra history-build`
- `python -m trading_infra history-verify`
- `python -m trading_infra history-upload`
- `python -m trading_infra market-data-upload`
- `python -m trading_infra market-data-refresh`
- `python -m trading_infra strategy-upload`
- `python -m trading_infra registry-upload`
- `python -m trading_infra backtest-upload`
- `python -m trading_infra performance-compute`
- `python -m trading_infra performance-refresh`

## Git Boundary

This repo intentionally tracks:

- source code
- tests
- GitHub Actions workflows
- documentation
- example strategy assets under `examples/`

This repo intentionally does **not** track local/operator working state such as:

- `.env`
- `data/`
- `decisions/`
- `registry/`
- `strategies/`

---

## Core Split

```text
Cloudflare R2 = online storage / source of truth
GitHub Actions = daily scheduled processing
Local machine = research and full backtests
```

R2 stores datasets, strategy files, model files, registries, and decision logs.

GitHub Actions runs daily jobs and updates online data/results.

The local machine is used for strategy research, training, full backtests, and approved strategy uploads.

The initial historical market-data bootstrap is local-first:

```text
download raw exchange bhavcopies locally
    ↓
build one canonical full-history parquet locally
    ↓
verify schema, keys, ranges, counts, and data sanity locally
    ↓
upload verified monthly parquet partitions to R2 through staging
```

After bootstrap, GitHub Actions is the daily cron. It refreshes only the latest exchange bhavcopy date into the affected R2 monthly partition, then runs active paper strategies for that date.

---

## Main Market Data

The main dataset is stored as Parquet on R2.

```text
daily_stock_data
----------------
date
exchange
isin
symbol
series

open
high
low
close
prev_close
vwap

volume
turnover
trades

deliverable_qty
delivery_pct

adj_open
adj_high
adj_low
adj_close
adj_factor
```

Each row means:

```text
1 stock on 1 trading day
```

Main key:

```text
date + exchange + isin + series
```

Adjusted prices are used for indicators, ML features, and returns.

Current historical ingestion uses identity adjustment until a corporate-action source is selected:

```text
adj_open   = open
adj_high   = high
adj_low    = low
adj_close  = close
adj_factor = 1.0
```

Target free exchange coverage:

- NSE legacy bhavcopy from 1994 onward where public files are available.
- NSE UDiFF/common bhavcopy from July 2024 onward.
- BSE legacy bhavcopy from 2007 onward where public files are available.
- BSE UDiFF/common bhavcopy from July 2024 onward.

---

## R2 Folder Shape

```text
bucket/
  data/
    daily_stock_data/
      exchange=NSE/
        year=2024/
          month=01/
            part.parquet

  strategies/
    strategy_id/
      config.yaml
      metadata.json
      model.pkl
      feature_config.yaml

  decisions/
    backtest/
      strategy_id/
        decisions.parquet

    paper/
      strategy_id/
        decisions.parquet

  registry/
    strategies.parquet
```

`model.pkl` and `feature_config.yaml` are optional strategy artifacts. The runtime now supports `private_pickle_v1`, where a private pickled artifact executes against a small in-process public runtime. `feature_config.yaml` is intended as a coarse compatibility manifest, not a disclosure of private logic.

---

## Strategy Abstraction

Each strategy is treated as a **blackbox**.

```text
market data up to date t
        ↓
strategy blackbox
        ↓
final decision rows for date t
```

The strategy may internally use rules, ML models, scores, filters, rankings, or allocation logic.

The infrastructure only stores the final decision output.

Current runnable strategy types:

- `top_n_adj_close`
- `private_pickle_v1`

For `private_pickle_v1`, the public runtime currently exposes market-data slices and trading dates. The private artifact computes its own internal indicators from those slices and returns canonical decision rows.

---

## Local Workflow

New strategies are developed locally.

```text
fetch latest Parquet data from R2
    ↓
build / modify strategy
    ↓
run full historical backtest locally
    ↓
upload strategy files to R2
    ↓
upload backtest decisions to R2
```

The uploaded backtest output is:

```text
decisions/backtest/strategy_id/decisions.parquet
```

Performance metrics do not need to be permanently stored initially. They can be computed on demand from decision logs, market data, and strategy behavior.

The repo now includes:

- `performance-compute` for local or R2-backed realized performance generation
- `performance-refresh` for daily cloud-side performance refresh from stored decisions

Full historical backtests, parameter sweeps, model training, and large research jobs stay local. GitHub Actions should not run full backtests, training, or heavy model inference.

Local `backtest-run` now executes in chunked market-data windows with warmup overlap instead of loading the entire local history tree into memory at once. For private pickle-backed strategies, one runtime is reused across all dates in a chunk and precomputes reusable feature tables once per chunk. For full-history local runs, prefer an exchange-scoped path such as `data/import/daily_stock_data_full/exchange=NSE`.

---

## Daily Online Workflow

GitHub Actions runs daily after market data is available.

```text
scheduled GitHub Actions job starts
    ↓
fetch latest exchange bhavcopy for each configured exchange
    ↓
merge refreshed date into the affected monthly R2 partition
    ↓
load active strategies from R2
    ↓
run daily paper-trading logic
    ↓
append today’s paper decision
    ↓
upload updated decisions to R2
```

Daily online computation does **not** rerun full historical backtests.

It only computes the next paper-trading decision for active strategies.

If the exchange bhavcopy is unavailable for a requested date, such as a holiday, the workflow treats the refresh as a no-op and skips paper evaluation for that exchange/date.

---

## Backtest vs Paper Decisions

Backtest and paper trading are symmetric.

Both produce the same artifact:

```text
decisions.parquet
```

Difference:

```text
backtest decisions = historical decisions generated locally
paper decisions    = live daily decisions generated by GitHub Actions
```

Both use the same schema and are stored under `decisions/`.

---

## Decision Log

The main strategy output is a thin decision log.

```text
decisions
---------
date
strategy_id
exchange
isin
symbol
target_weight
rank
score
```

Each row means:

```text
1 selected stock for 1 strategy on 1 date
```

`target_weight` means the fraction of portfolio capital assigned to that stock during the strategy’s holding period.

For equal-weight top-K strategies:

```text
target_weight = 1 / K
```

`score` is optional/debug metadata. The required output is the final selected stock and its target weight.

No `action` field is needed initially.

Buy/sell/hold can be inferred later from:

```text
current target weights
previous target weights
strategy behavior
```

---

## Strategy Storage

Each strategy gets its own folder.

```text
strategies/
  momentum_v1/
    config.yaml
    metadata.json

  ml_model_v1/
    config.yaml
    feature_config.yaml
    model.pkl
    metadata.json
```

Strategies should be versioned.

Old versions should not be overwritten because decisions must map to the exact version that produced them.

---

## Strategy Behavior

Strategy-level behavior is stored once with the strategy, not repeated in every decision row.

Examples:

```text
intraday or multi-day
entry at open or close
exit at close or hold until next rebalance
rebalance frequency
```

Example:

If a strategy enters at market open and exits at close, the decision row only stores the selected stocks and weights. The close exit is understood from the strategy behavior.

```text
Decision log = what the strategy selected
Strategy behavior = how those selections are executed
```

---

## Registry

A simple strategy registry is stored on R2.

```text
registry/strategies.parquet
```

Suggested columns:

```text
strategy_id
strategy_name
version
strategy_type
status
created_at
activated_at
notes
```

Only active strategies are used in the daily paper-trading job.

---

## Public GitHub / Private R2

GitHub repo can be public.

It contains:

```text
pipeline code
strategy runner code
GitHub Actions workflows
example configs
documentation
```

Cloudflare R2 remains private.

It contains:

```text
Parquet market data
strategy files
model files
backtest decision logs
paper decision logs
strategy registry
```

R2 credentials are stored as GitHub Actions secrets:

```text
R2_ACCESS_KEY_ID
R2_SECRET_ACCESS_KEY
R2_S3_API
R2_BUCKET_NAME
```

---

## Compute Philosophy

Storage is limited, so the system should not depend on extensive caching.

```text
Do not speed up by caching everything.
Speed up by making recomputation cheap.
```

Avoid storing large derived feature tables or precomputed technical indicators by default.

General rules:

```text
read only required columns
filter by date/symbol early
use fixed-size lookback windows
batch computation where possible
avoid Python row-by-row loops
avoid repeated full-history scans where possible
```

---

## Tech Stack

The project remains **Python-first**.

Rust will not be added in the initial version.

Current stack:

```text
Python
Polars / DuckDB for Parquet scans and transformations
NumPy for array computation
PyTorch / sklearn / XGBoost / LightGBM for modelling
GitHub Actions for scheduled daily processing
Cloudflare R2 for object storage
local machine for research/backtesting/training
```

Rust remains a possible future optimization only if profiling proves a specific bottleneck.

---

## Local Compute

Local machine:

```text
6-core CPU
RTX 3050 GPU
```

Use local hardware for:

```text
strategy research
model training
model experiments
full historical backtests
parameter sweeps
performance analysis
```

Use optimized libraries:

```text
Polars / DuckDB → multithreaded data processing
NumPy           → native array computation
PyTorch         → GPU training/inference experiments
joblib / multiprocessing → independent sweeps
```

---

## Online Compute

Online daily jobs should be treated as **CPU-only**.

The deployed strategy/model must be small enough to run cheaply in GitHub Actions.

Avoid:

```text
GPU-dependent online inference
large models
heavy online retraining
per-stock model calls
```

Online flow:

```text
latest market data
        ↓
active strategy blackbox
        ↓
final decision rows
        ↓
append to paper decisions
```

Train and experiment locally. Deploy only compact finalized strategies/models online.

---

## Performance Computation

Performance is computed from:

```text
decision log + market data + strategy behavior
```

This can produce:

```text
daily returns
portfolio multiple
drawdown
CAGR
Sharpe
turnover
trade reconstruction if needed
```

Performance tables are not required as permanent first-class storage initially.

They can be computed on demand. Optional derived outputs can be added later only if repeated computation becomes slow.

---

## Final Setup

```text
R2 stores persistent data, strategies, models, registries, and decisions.
GitHub Actions updates daily market data and paper decisions.
Local machine builds strategies, trains models, and uploads backtest decisions.
Performance is computed on demand from decisions + market data.
```

This keeps storage thin, treats strategies as blackboxes, keeps backtest and paper outputs symmetric, and avoids premature optimization.
