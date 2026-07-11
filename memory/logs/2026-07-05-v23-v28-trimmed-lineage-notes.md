# V23 V28 Trimmed Lineage Notes

Date: 2026-07-05

This note records how the private local descendants map to the original `trading-git` lineage.

## Final local private strategy IDs

- `v1_regime_sideways_etf_rotation_long`
- `v2_rotation_or_shock_overlay_long`

## Source lineage

### V1

Main source intent:

- `v23_h5_dd6.py`
- nested dependency ideas from `v22_risk_on.py`
- nested dependency ideas from `v18_h1.py`

Kept:

- regime-aware switch between a safer broad ETF sleeve and a more aggressive sleeve
- regime labels based on breadth, median return, and median RSI
- aggressive activation mainly in sideways or mixed conditions, with mild-risk-on override
- drawdown gate on the aggressive sleeve
- balanced fallback when aggressive conditions are on but recent aggressive momentum is weak

Dropped or reinterpreted:

- all short sleeves
- all dependence on precomputed CSV sleeve returns
- all public nested sleeve references
- old exact V18/V22 implementation details that depended on the short stack

Replacement used:

- direct long-only ETF selection from current parquet-derived features
- broad safe sleeve from the old broad ETF family
- aggressive sleeve from long-only risk ETF candidates

### V2

Main source intent:

- `v28_overlay.py`
- base sleeve ideas from `v18_h1.py`
- overlay dependency on the V23 descendant

Kept:

- top-level choice between a broad safer sleeve and the V23-style sleeve
- rotation-or-shock trigger idea
- broad-extended suppression of the overlay
- leadership, volatility, and trend checks

Dropped or reinterpreted:

- dependence on old precomputed `V18`, `V23`, and `V25` daily-return CSVs
- dependence on the short-capable original V18 lineage
- direct V25 branch in the deployed descendant

Replacement used:

- direct use of the long-only V1 descendant as the overlay sleeve
- direct use of the long-only safe broad ETF sleeve as the fallback sleeve

## Public runtime feature families actually used

- market-data slice
- per-symbol rolling returns
- per-symbol rolling drawdown
- RSI 14
- ADX 14
- turnover
- turnover rolling median
- realized volatility
- EMA trend
- exchange-level breadth above EMA 20
- exchange-level median return and median RSI
- small/mid leadership relative to `NIFTYBEES`

## Caveat

These are meaningful descendants, not exact byte-for-byte mirrors of the original production lineage. The main reason is structural: the current infra is long-only and intentionally does not reproduce the old short stack.
