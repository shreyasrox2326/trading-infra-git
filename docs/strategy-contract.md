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

`metadata.json` is for descriptive information such as version, notes, or strategy name. Execution behavior should live in `config.yaml`.

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
