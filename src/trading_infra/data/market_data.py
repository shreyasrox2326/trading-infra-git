"""Load canonical daily stock data from parquet."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date

import polars as pl

DAILY_STOCK_DATA_COLUMNS: tuple[str, ...] = (
    "date",
    "exchange",
    "isin",
    "symbol",
    "series",
    "open",
    "high",
    "low",
    "close",
    "prev_close",
    "vwap",
    "volume",
    "turnover",
    "trades",
    "deliverable_qty",
    "delivery_pct",
    "adj_open",
    "adj_high",
    "adj_low",
    "adj_close",
    "adj_factor",
)

DAILY_STOCK_DATA_REQUIRED_COLUMNS: tuple[str, ...] = (
    "date",
    "exchange",
    "isin",
    "symbol",
    "series",
    "close",
    "adj_close",
    "volume",
)

DAILY_STOCK_DATA_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,
    "isin": pl.Utf8,
    "symbol": pl.Utf8,
    "series": pl.Utf8,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "prev_close": pl.Float64,
    "vwap": pl.Float64,
    "volume": pl.Int64,
    "turnover": pl.Float64,
    "trades": pl.Int64,
    "deliverable_qty": pl.Int64,
    "delivery_pct": pl.Float64,
    "adj_open": pl.Float64,
    "adj_high": pl.Float64,
    "adj_low": pl.Float64,
    "adj_close": pl.Float64,
    "adj_factor": pl.Float64,
}


def load_daily_stock_data(
    path: str | list[str],
    *,
    start_date: date | None = None,
    as_of_date: date | None = None,
    exchanges: Iterable[str] | None = None,
    symbols: Iterable[str] | None = None,
    columns: Iterable[str] | None = None,
) -> pl.DataFrame:
    """Read canonical daily market data with early filters and column pruning."""
    requested_columns = tuple(columns) if columns is not None else DAILY_STOCK_DATA_COLUMNS
    required_requested = [column for column in requested_columns if column not in DAILY_STOCK_DATA_COLUMNS]
    if required_requested:
        raise ValueError(f"Requested unsupported market-data columns: {required_requested}")

    scan = pl.scan_parquet(path)

    for required in DAILY_STOCK_DATA_REQUIRED_COLUMNS:
        if required not in scan.collect_schema().names():
            raise ValueError(f"Market data is missing required column: {required}")

    filters: list[pl.Expr] = []
    if start_date is not None:
        filters.append(pl.col("date") >= pl.lit(start_date))
    if as_of_date is not None:
        filters.append(pl.col("date") <= pl.lit(as_of_date))
    if exchanges:
        filters.append(pl.col("exchange").is_in(list(exchanges)))
    if symbols:
        filters.append(pl.col("symbol").is_in(list(symbols)))

    filtered = scan
    if filters:
        predicate = filters[0]
        for condition in filters[1:]:
            predicate = predicate & condition
        filtered = filtered.filter(predicate)

    selected = filtered.select([pl.col(column).cast(DAILY_STOCK_DATA_SCHEMA[column]) for column in requested_columns])
    return selected.collect().sort(["date", "exchange", "symbol"])


def load_trading_dates(
    path: str | list[str],
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    exchanges: Iterable[str] | None = None,
) -> list[date]:
    """Read only the sorted distinct trading dates for the requested slice."""
    scan = pl.scan_parquet(path)

    if "date" not in scan.collect_schema().names():
        raise ValueError("Market data is missing required column: date")

    filters: list[pl.Expr] = []
    if start_date is not None:
        filters.append(pl.col("date") >= pl.lit(start_date))
    if end_date is not None:
        filters.append(pl.col("date") <= pl.lit(end_date))
    if exchanges:
        filters.append(pl.col("exchange").is_in(list(exchanges)))

    filtered = scan
    if filters:
        predicate = filters[0]
        for condition in filters[1:]:
            predicate = predicate & condition
        filtered = filtered.filter(predicate)

    dates = filtered.select(pl.col("date").cast(pl.Date).unique().sort()).collect()
    return dates.get_column("date").to_list()
