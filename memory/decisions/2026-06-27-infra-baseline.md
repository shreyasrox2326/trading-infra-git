# Infrastructure Baseline

Date: 2026-06-27

## Decisions

- The system is for pure technical, individual-stock strategies.
- `daily_stock_data` is stored as Parquet on Cloudflare R2.
- R2 remains the private source of truth.
- GitHub can be public; R2 remains private.
- R2 stores data, strategies, decisions, and registry files.
- Strategies are treated as blackboxes.
- Backtest and paper decisions are symmetric.
- Both backtest and paper workflows produce `decisions.parquet`.
- The decision log stores final selected stocks and target weights.
- There is no `action` field initially.
- `score` is optional/debug metadata.
- Performance is computed on demand from decision logs, market data, and strategy behavior.

## R2 Folder Shape

```text
bucket/
  data/
  strategies/
  decisions/
  registry/
```

## Strategy Blackbox

```text
market data up to date t -> strategy -> final decision rows for date t
```

The required strategy output is selected stock plus target weight. `score` may be null.

## Decision Log Schema

```text
date
strategy_id
exchange
isin
symbol
target_weight
rank
score
```
