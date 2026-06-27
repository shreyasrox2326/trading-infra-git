"""Backtest pipeline helpers."""

from __future__ import annotations

from datetime import date

import polars as pl

from trading_infra.decisions import validate_decisions_frame
from trading_infra.strategy import Strategy, StrategyContext


def run_backtest_for_date(
    strategy: Strategy,
    market_data: pl.DataFrame,
    as_of_date: date,
) -> pl.DataFrame:
    """Execute a strategy for a single historical date."""
    context = StrategyContext(
        strategy_id=strategy.strategy_id,
        as_of_date=as_of_date,
        market_data=market_data,
    )
    return validate_decisions_frame(strategy.run(context))
