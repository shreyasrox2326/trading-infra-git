# Strategy And Registry Contract

This document defines the operator-facing contract for adding strategies to `trading-infra-git`.

## Strategy Folder Layout

Each deployable strategy must live under:

```text
strategies/<strategy_id>/
  config.yaml
  metadata.json
```

Optional artifacts:

```text
strategies/<strategy_id>/
  model.pkl
  feature_config.yaml
```

`strategy_id` should be versioned and immutable after deployment, for example:

```text
momentum_v1
momentum_v2
top_n_adj_close_v1
```

Do not overwrite an old deployed strategy version in place. Create a new `strategy_id` when behavior changes.

## Required Config Fields

The first production-supported strategy execution type is:

```yaml
strategy_type: top_n_adj_close
strategy_id: top_n_adj_close_v1
top_n: 5
```

Required fields for `top_n_adj_close`:

- `strategy_type`
- `strategy_id`
- `top_n`

Optional execution fields:

- `lookback_days`

When every active strategy declares a bounded `lookback_days`, R2-backed daily paper runs download only the required market-data date range. If any active strategy does not declare a bounded lookback, the runtime falls back to full available history up to the decision date.

`metadata.json` is for descriptive information such as version, notes, or strategy name. Execution behavior should live in `config.yaml`.

Storage and upload support optional `model.pkl` and `feature_config.yaml`. A strategy is runnable only when its `strategy_type` is implemented by `strategy_builder.py`.

Private pickle-backed strategies are now also supported:

```yaml
strategy_type: private_pickle_v1
strategy_id: v1_regime_sideways_etf_rotation_long
runtime_contract: private_pickle_v1
lookback_days: 120
```

Required fields for `private_pickle_v1`:

- `strategy_type`
- `strategy_id`
- `runtime_contract`
- `lookback_days`

Required artifacts for `private_pickle_v1`:

- `config.yaml`
- `metadata.json`
- `model.pkl`

Recommended artifact:

- `feature_config.yaml`

`feature_config.yaml` is a compatibility manifest, not a place to reveal private logic. Keep it coarse. A typical shape is:

```yaml
required_features:
  - market_data_slice
  - breadth_family
  - momentum_family
required_aggregates:
  - exchange_median
  - breadth_above_ema
max_lookback_days: 120
```

## Private Runtime Contract

Runtime contract `private_pickle_v1` is:

- Artifact object must expose `decision(as_of_date, runtime)` or `run(as_of_date, runtime)`.
- `as_of_date` is the decision date.
- `runtime` is a small in-process helper, not an HTTP service.

Current runtime methods:

- `runtime.market_data(...) -> pandas.DataFrame`
- `runtime.trading_dates(...) -> list[date]`
- `runtime.feature_data(...) -> pandas.DataFrame`
- `runtime.latest_features(...) -> pandas.DataFrame`

`runtime.market_data(...)` currently supports filtering by:

- `exchange`
- `as_of_date`
- `start_date`
- `lookback_days`
- `symbols`
- `columns`
- `series`

Performance architecture for `private_pickle_v1`:

- local backtests load market data in warmup-overlapped chunks
- one runtime instance is reused across all dates inside a chunk
- the runtime precomputes reusable feature tables once per chunk and serves filtered views to the private artifact

This means private artifacts should prefer `feature_data(...)` or `latest_features(...)` for repeated indicator-based logic instead of rebuilding rolling features from raw slices on every day.

## Decision Output Contract

Private artifacts must return final decision rows only, in the existing canonical schema:

- `date`
- `strategy_id`
- `exchange`
- `isin`
- `symbol`
- `target_weight`
- `rank`
- `score`

Practical rules:

- Long-only only. Negative `target_weight` is invalid.
- Partial cash is allowed by emitting invested rows summing to less than `1.0`.
- Full cash day is represented by zero rows for that strategy-date.
- No extra sleeve, short, or auxiliary columns are allowed in stored `decisions.parquet`.

## Registry Contract

The online activation source is:

```text
registry/strategies.parquet
```

Minimum required columns:

- `strategy_id`
- `version`
- `status`

Activation rule:

- `status == "active"` means the strategy runs in daily paper processing.
- Any other status means it does not run.

Recommended additional columns:

- `strategy_name`
- `strategy_type`
- `created_at`
- `activated_at`
- `notes`

## Publishing Flow

1. Create or update a versioned local strategy folder.
2. Run local backtests.
3. Upload strategy artifacts to R2.
4. Upload backtest decisions to R2.
5. Update the registry and set the desired strategy row to `active`.
6. Run a local or GitHub Actions paper dry-run to validate the deployment.
