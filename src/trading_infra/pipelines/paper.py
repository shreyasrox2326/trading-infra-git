"""Paper-trading pipeline helpers."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from trading_infra.decisions import empty_decisions_frame, validate_decisions_frame
from trading_infra.registry import active_strategy_ids, load_strategy_registry
from trading_infra.storage.decisions import read_decisions_parquet, write_decisions_parquet
from trading_infra.storage.paths import paper_decisions_key
from trading_infra.strategy import Strategy, StrategyContext
from trading_infra.strategy_builder import build_strategy
from trading_infra.strategy_store import load_stored_strategy


def run_paper_for_date(
    strategy: Strategy,
    market_data: pl.DataFrame,
    as_of_date: date,
) -> pl.DataFrame:
    """Execute a strategy for the latest available date."""
    historical_market_data = market_data.filter(pl.col("date") <= as_of_date)
    context = StrategyContext(
        strategy_id=strategy.strategy_id,
        as_of_date=as_of_date,
        market_data=historical_market_data,
    )
    return validate_decisions_frame(strategy.run(context))


def append_paper_decisions(
    existing: pl.DataFrame,
    today: pl.DataFrame,
) -> pl.DataFrame:
    """Append daily paper decisions and keep the result idempotent."""
    if existing.is_empty() and today.is_empty():
        return empty_decisions_frame()
    if existing.is_empty():
        return validate_decisions_frame(today)
    if today.is_empty():
        return validate_decisions_frame(existing)

    combined = pl.concat([existing, today])
    deduped = combined.unique(
        subset=["date", "strategy_id", "exchange", "isin", "symbol"],
        keep="last",
        maintain_order=True,
    )
    return validate_decisions_frame(deduped)


def run_daily_paper_job(
    *,
    base_path: str | Path,
    market_data: pl.DataFrame,
    as_of_date: date,
    registry_path: str | Path | None = None,
    output_base_path: str | Path | None = None,
) -> dict[str, pl.DataFrame]:
    """Run the local daily paper workflow for all active strategies."""
    root = Path(base_path)
    registry_source = registry_path if registry_path is not None else root / "registry" / "strategies.parquet"
    load_strategy_registry(registry_source)
    active_ids = active_strategy_ids(registry_source)

    results: dict[str, pl.DataFrame] = {}
    output_root = Path(output_base_path) if output_base_path is not None else root

    for strategy_id in active_ids:
        stored = load_stored_strategy(root, strategy_id)
        strategy = build_strategy(stored)
        today = run_paper_for_date(strategy, market_data, as_of_date)

        decisions_path = output_root / Path(paper_decisions_key(strategy_id))
        existing = read_decisions_parquet(decisions_path)
        updated = append_paper_decisions(existing, today)
        write_decisions_parquet(decisions_path, updated)
        results[strategy_id] = updated

    return results
