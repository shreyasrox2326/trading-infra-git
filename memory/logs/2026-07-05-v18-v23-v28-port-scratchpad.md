# V18 V23 V28 Port Scratchpad

Date: 2026-07-05

Goal: port `V23 h5 dd6 hard 1Cr` and `V28 V18 rotation or shock hard 1Cr` from sibling repo `trading-git` into `trading-infra-git` as flat backtestable/daily-paper strategies.

## Located Source Modules In `trading-git`

- `active/india_sota/src/trading_system/strategies/v18_h1.py`
- `active/india_sota/src/trading_system/strategies/v20_regime_fallback.py`
- `active/india_sota/src/trading_system/strategies/v22_risk_on.py`
- `active/india_sota/src/trading_system/strategies/v23_h5_dd6.py`
- `active/india_sota/src/trading_system/strategies/v24_strict_smallmid.py`
- `active/india_sota/src/trading_system/strategies/v25_selector.py`
- `active/india_sota/src/trading_system/strategies/v28_overlay.py`

## Dependency Chain

- `V18 h1 hard 1Cr`
  - ETF rotation sleeve plus short basket fallback
  - depends on ETF price-cache features
  - depends on `v8_bhavcopy_short_only_daily.csv`

- `V20 required hard 1Cr`
  - depends on `v19_daily_liquid_regime.csv`
  - depends on `v19_short_sleeve_signals.csv`
  - depends on `v12_actionable_etf_rotation_daily.csv`

- `V22 required hard 1Cr`
  - depends on `V18`
  - depends on `V20`
  - depends on regime columns
  - adds risk-on ETF sleeves

- `V23 h5 dd6 hard 1Cr`
  - depends on `V22`
  - depends on regime columns
  - chooses between safe/aggressive ETF sleeves

- `V24 strict smallmid hard 1Cr`
  - depends on `V18`
  - depends on `V23`
  - depends on regime columns
  - depends on ETF price-cache features

- `V25 selector hard 1Cr`
  - depends on `V18`
  - depends on `V23`
  - depends on `V24`
  - depends on regime columns

- `V28 V18 rotation or shock hard 1Cr`
  - depends on `V18`
  - depends on `V23`
  - depends on `v27_feature_table.csv`
  - chooses top-level sleeve between V18 and V23

## Important Semantic Observation

The source repo daily recommendations for `V23` and `V28` are often `action=SLEEVE`, not direct ticker execution. The old system resolves those sleeves through nested policies and external outputs.

The requested new repo deliverable requires a flat strategy with no external references, so the full nested chain must be inlined if we want exact logical equivalence.

## Current `trading-infra-git` Strategy Contract

- Strategy runtime is blackbox `run(context) -> decisions frame`
- Current decision schema only supports:
  - `date`
  - `strategy_id`
  - `exchange`
  - `isin`
  - `symbol`
  - `target_weight`
  - `rank`
  - `score`
- Validator currently rejects negative `target_weight`
- Current built-in strategy support is only `top_n_adj_close`

## Likely Infrastructure Gaps For Exact Mirror

- No current concept of short positions in decision schema or validator.
- No explicit action field for long / short / cash / sleeve resolution.
- No current portfolio-level composite strategy runtime that naturally emits resolved execution from nested sleeves.
- No existing ETF feature-building pipeline in `trading-infra-git` matching old repo `price_cache.py`.
- No current regime feature tables analogous to:
  - `v19_daily_liquid_regime.csv`
  - `v27_feature_table.csv`
- No current equivalent for old repo short-basket source artifacts:
  - `v8_bhavcopy_short_only_daily.csv`
  - `v19_short_sleeve_signals.csv`
  - `v12_actionable_etf_rotation_daily.csv`

## Initial Conclusion

An exact mirror of `V23` and `V28 V18 rotation or shock` is not a simple “add two strategy classes” task in the current repo.

To be exact, we likely need one of:

1. Extend `trading-infra-git` to support ETF/short/composite execution plus derived regime features and then port the full dependency chain.
2. Accept a narrower deliverable that ports only the sleeve-selection logic over precomputed inputs, which would not satisfy “flat, no external reference, logically identical” on its own.
