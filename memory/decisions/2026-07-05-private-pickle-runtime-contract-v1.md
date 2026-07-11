# Private Pickle Runtime Contract V1

Date: 2026-07-05

## Decision

The first private strategy runtime contract is `private_pickle_v1`.

Private strategy artifacts are stored as one `model.pkl` per strategy and executed through a small in-process public runtime.

## Contract

- Strategy config declares:
  - `strategy_type: private_pickle_v1`
  - `runtime_contract: private_pickle_v1`
  - bounded `lookback_days`
- Artifact exposes `decision(as_of_date, runtime)` or `run(as_of_date, runtime)`.
- Public runtime currently exposes:
  - `runtime.market_data(...) -> pandas.DataFrame`
  - `runtime.trading_dates(...) -> list[date]`
- Artifact returns only canonical decision rows.
- Full cash day is represented as zero rows.
- Partial cash is represented by emitted `target_weight` summing to less than `1.0`.
- Negative `target_weight` remains invalid.

## Packaging

Private strategy folder layout is:

```text
strategies/<strategy_id>/
  config.yaml
  metadata.json
  model.pkl
  feature_config.yaml
```

`feature_config.yaml` is a coarse compatibility manifest, not a disclosure of private logic.

## Applied Strategy IDs

- `v1_regime_sideways_etf_rotation_long`
- `v2_rotation_or_shock_overlay_long`
