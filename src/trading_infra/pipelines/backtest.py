"""Backtest pipeline helpers."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date

import polars as pl

from trading_infra.decisions import empty_decisions_frame
from trading_infra.decisions import validate_decisions_frame
from trading_infra.strategy import Strategy, StrategyContext


def run_backtest_for_date(
    strategy: Strategy,
    market_data: pl.DataFrame,
    as_of_date: date,
) -> pl.DataFrame:
    """Execute a strategy for a single historical date."""
    historical_market_data = market_data.filter(pl.col("date") <= as_of_date)
    context = StrategyContext(
        strategy_id=strategy.strategy_id,
        as_of_date=as_of_date,
        market_data=historical_market_data,
    )
    return validate_decisions_frame(strategy.run(context))


def backtest_dates(
    market_data: pl.DataFrame,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[date]:
    """Return sorted backtest dates from the available market data."""
    dates = market_data.select(pl.col("date").cast(pl.Date).unique().sort())
    if start_date is not None:
        dates = dates.filter(pl.col("date") >= start_date)
    if end_date is not None:
        dates = dates.filter(pl.col("date") <= end_date)
    return dates.get_column("date").to_list()


def run_backtest(
    strategy: Strategy,
    market_data: pl.DataFrame,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    dates: Iterable[date] | None = None,
) -> pl.DataFrame:
    """Execute a strategy across a sequence of historical dates."""
    scheduled_dates = list(dates) if dates is not None else backtest_dates(
        market_data,
        start_date=start_date,
        end_date=end_date,
    )
    if not scheduled_dates:
        return empty_decisions_frame()

    daily_frames = [
        run_backtest_for_date(strategy, market_data=market_data, as_of_date=decision_date)
        for decision_date in scheduled_dates
    ]
    combined = pl.concat(daily_frames) if daily_frames else empty_decisions_frame()
    return validate_decisions_frame(combined)
