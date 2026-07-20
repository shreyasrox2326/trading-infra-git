"""Paper-trading pipeline helpers."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl

from trading_infra.decisions import empty_decisions_frame, validate_decisions_frame
from trading_infra.registry import active_strategy_ids, load_strategy_registry
from trading_infra.storage.decisions import read_decisions_parquet, write_decisions_parquet
from trading_infra.storage.market_data_remote import (
    load_daily_stock_data_history_from_r2,
    load_daily_stock_data_range_from_r2,
)
from trading_infra.storage.paths import paper_decisions_key
from trading_infra.storage.r2 import R2Client
from trading_infra.storage.remote import (
    download_paper_decisions,
    download_strategy_artifacts,
    load_strategy_registry_from_r2,
    upload_paper_decisions,
)
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


def run_daily_paper_job_from_r2(
    *,
    client: R2Client,
    exchange: str,
    as_of_date: date,
    upload_results: bool = False,
) -> dict[str, pl.DataFrame]:
    """Run the daily paper workflow using R2-backed inputs."""
    registry = load_strategy_registry_from_r2(client)
    active_ids = active_strategy_ids(registry)

    with TemporaryDirectory() as tmpdir:
        workspace = Path(tmpdir)
        registry_root = workspace / "registry"
        registry_root.mkdir(parents=True, exist_ok=True)
        registry.write_parquet(registry_root / "strategies.parquet")

        strategies = {}
        for strategy_id in active_ids:
            download_strategy_artifacts(client, strategy_id, workspace)
            strategies[strategy_id] = build_strategy(load_stored_strategy(workspace, strategy_id))
            existing = download_paper_decisions(client, strategy_id)
            if not existing.is_empty():
                decisions_path = workspace / Path(paper_decisions_key(strategy_id))
                write_decisions_parquet(decisions_path, existing)

        lookbacks = [getattr(strategy, "lookback_days", None) for strategy in strategies.values()]
        if lookbacks and all(value is not None for value in lookbacks):
            start_date = as_of_date - timedelta(days=max(int(value) for value in lookbacks))
            market_data = load_daily_stock_data_range_from_r2(
                client,
                exchange=exchange,
                start_date=start_date,
                end_date=as_of_date,
            )
        else:
            market_data = load_daily_stock_data_history_from_r2(
                client,
                exchange=exchange,
                end_date=as_of_date,
            )

        results = run_daily_paper_job(
            base_path=workspace,
            market_data=market_data,
            as_of_date=as_of_date,
        )

        if upload_results:
            for strategy_id in results:
                decisions_path = workspace / Path(paper_decisions_key(strategy_id))
                upload_paper_decisions(client, strategy_id, decisions_path)

        return results
