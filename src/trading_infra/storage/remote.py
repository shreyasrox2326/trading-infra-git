"""Higher-level R2 helpers for market data and strategy artifacts."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl

from trading_infra.data.market_data import load_daily_stock_data
from trading_infra.decisions import empty_decisions_frame
from trading_infra.registry import load_strategy_registry
from trading_infra.storage.decisions import read_decisions_parquet
from trading_infra.storage.paths import (
    backtest_decisions_key,
    daily_stock_data_prefix,
    paper_decisions_key,
    registry_strategies_key,
    strategy_artifact_keys,
    strategy_prefix,
)
from trading_infra.storage.r2 import R2Client


def _month_starts(start_date: date, end_date: date) -> list[tuple[int, int]]:
    """Return inclusive `(year, month)` tuples spanning a date range."""
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date.")

    months: list[tuple[int, int]] = []
    year = start_date.year
    month = start_date.month
    while (year, month) <= (end_date.year, end_date.month):
        months.append((year, month))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return months


def list_daily_stock_data_keys(client: R2Client, exchange: str, year: int, month: int) -> list[str]:
    """List market-data parquet objects for a monthly partition."""
    prefix = daily_stock_data_prefix(exchange, year, month)
    return [key for key in client.list_keys(prefix) if key.endswith(".parquet")]


def load_daily_stock_data_from_r2(
    client: R2Client,
    *,
    exchange: str,
    year: int,
    month: int,
    as_of_date: date | None = None,
    symbols: list[str] | None = None,
    columns: list[str] | None = None,
) -> pl.DataFrame:
    """Download a monthly market-data partition and load it through the local parquet loader."""
    keys = list_daily_stock_data_keys(client, exchange, year, month)
    if not keys:
        raise FileNotFoundError(
            f"No market-data parquet objects found for exchange={exchange}, year={year}, month={month:02d}."
        )

    with TemporaryDirectory() as tmpdir:
        local_paths: list[str] = []
        for index, key in enumerate(keys):
            local_path = Path(tmpdir) / f"part-{index}.parquet"
            client.download_file(key, local_path)
            local_paths.append(str(local_path))

        return load_daily_stock_data(
            local_paths,
            as_of_date=as_of_date,
            exchanges=[exchange],
            symbols=symbols,
            columns=columns,
        )


def load_daily_stock_data_range_from_r2(
    client: R2Client,
    *,
    exchange: str,
    start_date: date,
    end_date: date,
    symbols: list[str] | None = None,
    columns: list[str] | None = None,
) -> pl.DataFrame:
    """Download all monthly partitions needed for a date range and load them locally."""
    months = _month_starts(start_date, end_date)

    with TemporaryDirectory() as tmpdir:
        local_paths: list[str] = []
        for year, month in months:
            keys = list_daily_stock_data_keys(client, exchange, year, month)
            if not keys:
                raise FileNotFoundError(
                    f"Missing market-data parquet objects for exchange={exchange}, year={year}, month={month:02d}."
                )
            for index, key in enumerate(keys):
                local_path = Path(tmpdir) / f"{year}-{month:02d}-{index}.parquet"
                client.download_file(key, local_path)
                local_paths.append(str(local_path))

        loaded = load_daily_stock_data(
            local_paths,
            as_of_date=end_date,
            exchanges=[exchange],
            symbols=symbols,
            columns=columns,
        )
        return loaded.filter(pl.col("date") >= start_date)


def download_strategy_artifacts(client: R2Client, strategy_id: str, target_dir: str | Path) -> Path:
    """Download a strategy folder into a local target directory."""
    target_root = Path(target_dir) / "strategies" / strategy_id
    target_root.mkdir(parents=True, exist_ok=True)
    prefix = strategy_prefix(strategy_id)
    keys = client.list_keys(prefix)
    if not keys:
        raise FileNotFoundError(f"No strategy artifacts found for strategy_id={strategy_id}.")

    for key in keys:
        relative_parts = Path(key).parts[2:]
        client.download_file(key, target_root.joinpath(*relative_parts))

    return target_root


def load_strategy_registry_from_r2(client: R2Client) -> pl.DataFrame:
    """Download and validate the strategy registry from R2."""
    with TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "strategies.parquet"
        client.download_file(registry_strategies_key(), local_path)
        return load_strategy_registry(local_path)


def upload_strategy_artifacts(client: R2Client, strategy_id: str, base_path: str | Path) -> None:
    """Upload a local strategy folder to its canonical R2 location."""
    strategy_root = Path(base_path) / "strategies" / strategy_id
    keys = strategy_artifact_keys(strategy_id)
    required = {
        "config": strategy_root / "config.yaml",
        "metadata": strategy_root / "metadata.json",
    }
    optional = {
        "model": strategy_root / "model.pkl",
        "feature_config": strategy_root / "feature_config.yaml",
    }

    missing = [name for name, path in required.items() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required strategy artifacts for {strategy_id}: {missing}")

    for logical_name, path in required.items():
        client.upload_file(path, keys[logical_name])
    for logical_name, path in optional.items():
        if path.exists():
            client.upload_file(path, keys[logical_name])


def upload_strategy_registry(client: R2Client, local_path: str | Path) -> None:
    """Upload a local strategy registry parquet file to R2."""
    client.upload_file(local_path, registry_strategies_key())


def upload_backtest_decisions(client: R2Client, strategy_id: str, local_path: str | Path) -> None:
    """Upload a local backtest decisions parquet file to R2."""
    client.upload_file(local_path, backtest_decisions_key(strategy_id))


def upload_paper_decisions(client: R2Client, strategy_id: str, local_path: str | Path) -> None:
    """Upload a local paper decisions parquet file to R2."""
    client.upload_file(local_path, paper_decisions_key(strategy_id))


def download_paper_decisions(client: R2Client, strategy_id: str) -> pl.DataFrame:
    """Download paper decisions for a strategy if they exist."""
    key = paper_decisions_key(strategy_id)
    if not client.exists(key):
        return empty_decisions_frame()
    with TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "decisions.parquet"
        client.download_file(key, local_path)
        return read_decisions_parquet(local_path)
