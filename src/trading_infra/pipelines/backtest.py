"""Backtest pipeline helpers."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import date, timedelta

import polars as pl
from tqdm import tqdm

from trading_infra.decisions import empty_decisions_frame
from trading_infra.decisions import validate_decisions_frame
from trading_infra.strategy import Strategy, StrategyContext


def run_backtest_for_date(
    strategy: Strategy,
    market_data: pl.DataFrame,
    as_of_date: date,
) -> pl.DataFrame:
    """Execute a strategy for a single historical date."""
    if getattr(strategy, "requires_historical_slice", True):
        historical_market_data = market_data.filter(pl.col("date") <= as_of_date)
    else:
        historical_market_data = market_data
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
    show_progress: bool = False,
    progress_label: str | None = None,
) -> pl.DataFrame:
    """Execute a strategy across a sequence of historical dates."""
    scheduled_dates = list(dates) if dates is not None else backtest_dates(
        market_data,
        start_date=start_date,
        end_date=end_date,
    )
    if not scheduled_dates:
        return empty_decisions_frame()

    iterator = scheduled_dates
    if show_progress:
        iterator = tqdm(
            scheduled_dates,
            desc=progress_label or f"backtest:{strategy.strategy_id}",
            unit="day",
            leave=False,
        )

    daily_frames = [
        run_backtest_for_date(strategy, market_data=market_data, as_of_date=decision_date)
        for decision_date in iterator
    ]
    combined = pl.concat(daily_frames) if daily_frames else empty_decisions_frame()
    return validate_decisions_frame(combined)


def run_backtest_chunked(
    strategy: Strategy,
    *,
    dates: list[date],
    load_market_data: Callable[[date | None, date], pl.DataFrame],
    warmup_days: int,
    chunk_size: int,
    show_progress: bool = True,
) -> pl.DataFrame:
    """Execute a backtest by loading only one warmup-overlapped chunk at a time."""
    if not dates:
        return empty_decisions_frame()
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive.")

    chunk_frames: list[pl.DataFrame] = []
    chunk_offsets = range(0, len(dates), chunk_size)
    total_chunks = (len(dates) + chunk_size - 1) // chunk_size

    for chunk_number, offset in enumerate(chunk_offsets, start=1):
        chunk_dates = dates[offset : offset + chunk_size]
        chunk_start = chunk_dates[0]
        chunk_end = chunk_dates[-1]
        market_start = chunk_start - timedelta(days=warmup_days) if warmup_days > 0 else None
        market_data = load_market_data(market_start, chunk_end)
        if market_data.is_empty():
            raise ValueError(
                f"No market data loaded for strategy={strategy.strategy_id} chunk={chunk_start.isoformat()}..{chunk_end.isoformat()}."
            )
        chunk_frames.append(
            run_backtest(
                strategy,
                market_data,
                dates=chunk_dates,
                show_progress=show_progress,
                progress_label=(
                    f"{strategy.strategy_id} chunk {chunk_number}/{total_chunks} "
                    f"{chunk_start.isoformat()}..{chunk_end.isoformat()}"
                ),
            )
        )

    combined = pl.concat(chunk_frames) if chunk_frames else empty_decisions_frame()
    return validate_decisions_frame(combined)
