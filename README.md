# NSE/BSE Backtesting and Paper-Trading Infrastructure

This repository implements a Python pipeline for collecting end-of-day NSE and BSE price-volume data, validating it into canonical monthly Parquet partitions, and running versioned technical strategies through local backtests or scheduled paper evaluation. Cloudflare R2 holds durable data and artifacts, while backtest and paper runs share the same compact decision-log contract.

The scope is deliberately narrow: individual securities, daily price-volume inputs, long-only target weights, and CPU-compatible scheduled execution. It does not ingest fundamentals, news, options, tick data, or order-book data, and this repository makes no trading-performance claim.

## What Is Implemented

- Format-aware NSE/BSE bhavcopy fetching and normalization across legacy and current exchange formats.
- Partition-first history builds with resumable fetch manifests, monthly partition manifests, repair modes, audits, and local/R2 synchronization checks.
- Staged, guarded publication to R2 and idempotent daily monthly-partition refreshes.
- One strategy interface and decision schema shared by backtests and paper runs.
- Versioned public example strategies and private pickle-backed strategy artifacts through a bounded runtime contract.
- On-demand realized-performance calculation from decisions and market data.

## Verified State

As of 2026-09-02, the local canonical history and R2 are synchronized across **27,742,872 rows in 620 monthly partitions**:

- NSE: 11,858,406 rows, 383 partitions, 1994-11-03 through 2026-09-02.
- BSE: 15,884,466 rows, 237 partitions, 2007-01-02 through 2026-09-02.
- The partition-wise audit reports zero duplicate keys and zero invalid OHLC rows.
- The test suite contains 170 passing tests.

The scheduled workflow supports an absent R2 registry as a valid zero-active-strategy deployment. No strategy is currently deployed, so paper and performance stages are expected no-ops. The code path is tested locally, but a post-fix GitHub Actions run has not yet completed; the scheduled service should not be described as verified healthy until that run succeeds.

## Architecture

| Component | Responsibility |
|---|---|
| Local environment | Historical fetches, incremental/full builds, verification, research, backtests, and approved uploads |
| Cloudflare R2 | Canonical monthly market data, strategy artifacts, registry, decision logs, and optional performance outputs |
| GitHub Actions | One-date market refresh followed by paper and performance processing for active strategies |

Historical publication is intentionally local-first:

```text
raw exchange files
    -> reviewed fetch manifests
    -> canonical monthly Parquet partitions
    -> partition-wise audit and doctor reports
    -> staged R2 upload and promotion
```

After bootstrap, the scheduled path updates only the affected monthly partition for the requested trading date. It does not rebuild history, train models, or rerun full backtests.

## Quick Start

Python 3.11 or newer is required. From a fresh checkout:

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.lock
python -m pip install -e . --no-deps
python -m pytest -q
python -m trading_infra --help
```

The maintained operator workspace runs project commands inside the container documented in `AGENTS.md`. R2-backed commands additionally require the variables documented in `.env.example`. Credentials and private data must remain outside Git.

Common entry points:

```bash
python -m trading_infra history-fetch --help
python -m trading_infra history-build --help
python -m trading_infra history-verify --help
python -m trading_infra history-upload --help
python -m trading_infra market-data-refresh --help
python -m trading_infra paper-dry-run --help
python -m trading_infra performance-compute --help
```

## Repository Map

```text
src/trading_infra/data/         exchange formats, fetch, normalize, build, verify, doctor
src/trading_infra/storage/      R2 client, object paths, publish, refresh, sync, usage checks
src/trading_infra/pipelines/    shared backtest and paper orchestration
src/trading_infra/strategies/   public and private-artifact strategy adapters
tests/                          unit and integration-style coverage with mocked R2 boundaries
.github/workflows/              scheduled daily market-data and paper workflow
docs/                           operator runbook, strategy contract, progress tracker
memory/                         durable decisions and dated implementation notes
```

Operator-facing documentation:

- `docs/operator-runbook.md`
- `docs/strategy-contract.md`
- `docs/progress-checklist.md`

## Public/Private Boundary

This repository tracks source code, tests, workflows, documentation, and example strategy assets. It intentionally excludes credentials and operator state under `.env`, `data/`, `decisions/`, `performance/`, `registry/`, and `strategies/`.

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

For `private_pickle_v1`, the public runtime exposes market-data slices, trading dates, and reusable precomputed feature tables. The private artifact applies its own selection logic and returns canonical decision rows.

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

If the registry object is absent, the workflow treats it as an empty registry: market-data refresh still completes, and paper/performance processing exits successfully without loading strategy data.

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

The registry itself is optional. Its absence means that no strategies are deployed; a present but malformed registry remains an error.

---

## R2 Credentials

Cloudflare R2 remains private. R2 credentials are stored as GitHub Actions secrets:

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

Current runtime stack:

```text
Python
Polars for Parquet scans, validation, and transformations
Pandas / NumPy for the private strategy feature runtime
Boto3 for the S3-compatible R2 API
GitHub Actions for scheduled daily processing
Cloudflare R2 for object storage
```

Model-training libraries are not part of the deployed runtime dependency set. Rust remains a possible future optimization only if profiling proves a specific bottleneck.

---

## Compute Boundaries

Use local compute for:

```text
strategy research
model training
model experiments
full historical backtests
parameter sweeps
performance analysis
```

Research code may use additional local dependencies, but they are intentionally separate from this package's scheduled runtime. Online daily jobs are treated as **CPU-only**.

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

Train and experiment locally. Deploy only compact finalized strategies or models online.

---

## Performance Computation

Performance is computed from:

```text
decision log + market data + strategy behavior
```

The current implementation produces:

```text
daily returns
cumulative portfolio multiple
invested and cash weight
drawdown
summary final multiple and maximum drawdown
```

Returns are currently realized from the next available adjusted close. CAGR, Sharpe, turnover, transaction costs, and trade reconstruction are not implemented metrics.

Performance tables are not required as permanent first-class storage initially.

They can be computed on demand. Optional derived outputs can be added later only if repeated computation becomes slow.
