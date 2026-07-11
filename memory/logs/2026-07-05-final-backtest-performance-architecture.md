# Final Backtest Performance Architecture

Date: 2026-07-05

Improved the private-strategy backtest architecture from a memory-safe baseline to a chunk-reuse model.

## Previous bottleneck

- local backtests loaded too much market data at once
- private strategies recomputed rolling features on every decision date
- each backtest day created a fresh runtime over a truncated historical slice

## New architecture

- local `backtest-run` loads trading dates first
- local market data is loaded chunk by chunk with warmup overlap
- one `PrivateStrategyRuntimeV1` instance is reused across all dates in the chunk
- the runtime precomputes a reusable per-symbol feature table once per chunk
- private strategies consume `latest_features(...)` and `feature_data(...)`

## Runtime feature families now public

- rolling returns: 5, 20, 60
- drawdown: 20, 60
- RSI 14
- ADX 14
- turnover median: 20, 60
- EMA 20 and EMA trend 20
- realized volatility 20
- daily return
- above-EMA indicator

## Verification

- targeted tests confirm chunked local loading
- targeted tests confirm one feature build per chunk for a feature-driven private strategy
- both private strategies were rebuilt and passed local smoke backtests on real local NSE parquet
