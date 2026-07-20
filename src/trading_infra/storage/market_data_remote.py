"""Higher-level R2 helpers for canonical market-data parquet."""

from __future__ import annotations

from calendar import monthrange
from datetime import date
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory

import polars as pl

from trading_infra.data.market_data import load_daily_stock_data
from trading_infra.storage.paths import daily_stock_data_prefix
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


def list_daily_stock_data_months(client: R2Client, exchange: str) -> list[tuple[int, int]]:
    """List available `(year, month)` partitions for an exchange."""
    prefix = str(PurePosixPath("data") / "daily_stock_data" / f"exchange={exchange}")
    months: set[tuple[int, int]] = set()
    for key in client.list_keys(prefix):
        if not key.endswith(".parquet"):
            continue
        parts = PurePosixPath(key).parts
        year = next((part for part in parts if part.startswith("year=")), None)
        month = next((part for part in parts if part.startswith("month=")), None)
        if year is None or month is None:
            continue
        months.add((int(year.split("=", 1)[1]), int(month.split("=", 1)[1])))
    return sorted(months)


def _load_daily_stock_data_for_keys(
    client: R2Client,
    *,
    keys: list[str],
    exchange: str,
    end_date: date,
    symbols: list[str] | None = None,
    columns: list[str] | None = None,
) -> pl.DataFrame:
    """Download parquet keys and load them through the local parquet loader."""
    with TemporaryDirectory() as tmpdir:
        local_paths: list[str] = []
        for index, key in enumerate(keys):
            local_path = Path(tmpdir) / f"part-{index}.parquet"
            client.download_file(key, local_path)
            local_paths.append(str(local_path))

        return load_daily_stock_data(
            local_paths,
            as_of_date=end_date,
            exchanges=[exchange],
            symbols=symbols,
            columns=columns,
        )


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
    end_date = as_of_date if as_of_date is not None else date(year, month, monthrange(year, month)[1])
    return _load_daily_stock_data_for_keys(
        client,
        keys=keys,
        exchange=exchange,
        end_date=end_date,
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
    keys: list[str] = []
    for year, month in months:
        month_keys = list_daily_stock_data_keys(client, exchange, year, month)
        if not month_keys:
            raise FileNotFoundError(
                f"Missing market-data parquet objects for exchange={exchange}, year={year}, month={month:02d}."
            )
        keys.extend(month_keys)

    loaded = _load_daily_stock_data_for_keys(
        client,
        keys=keys,
        exchange=exchange,
        end_date=end_date,
        symbols=symbols,
        columns=columns,
    )
    return loaded.filter(pl.col("date") >= start_date)


def load_daily_stock_data_history_from_r2(
    client: R2Client,
    *,
    exchange: str,
    end_date: date,
    symbols: list[str] | None = None,
    columns: list[str] | None = None,
) -> pl.DataFrame:
    """Load all available history for an exchange up to `end_date`."""
    months = [
        (year, month)
        for year, month in list_daily_stock_data_months(client, exchange)
        if (year, month) <= (end_date.year, end_date.month)
    ]
    if not months:
        raise FileNotFoundError(
            f"No market-data parquet objects found for exchange={exchange} up to {end_date.isoformat()}."
        )

    keys: list[str] = []
    for year, month in months:
        keys.extend(list_daily_stock_data_keys(client, exchange, year, month))

    return _load_daily_stock_data_for_keys(
        client,
        keys=keys,
        exchange=exchange,
        end_date=end_date,
        symbols=symbols,
        columns=columns,
    )
