"""Local-to-R2 market-data publishing helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl

from trading_infra.data.market_data import (
    DAILY_STOCK_DATA_COLUMNS,
    DAILY_STOCK_DATA_REQUIRED_COLUMNS,
    DAILY_STOCK_DATA_SCHEMA,
)
from trading_infra.storage.paths import daily_stock_data_prefix
from trading_infra.storage.r2 import R2Client


@dataclass(frozen=True)
class MarketDataPartition:
    """A canonical R2 partition for daily stock data."""

    exchange: str
    year: int
    month: int
    rows: int

    @property
    def prefix(self) -> str:
        return daily_stock_data_prefix(self.exchange, self.year, self.month)

    @property
    def key(self) -> str:
        return f"{self.prefix}/part.parquet"


def _resolve_input_paths(paths: list[str | Path]) -> list[str]:
    """Resolve parquet input files from explicit paths or directories."""
    resolved: list[str] = []
    for raw_path in paths:
        path = Path(raw_path)
        if not path.exists():
            raise FileNotFoundError(f"Market-data input path not found: {path}")
        if path.is_dir():
            resolved.extend(str(candidate) for candidate in sorted(path.rglob("*.parquet")))
        elif path.suffix == ".parquet":
            resolved.append(str(path))

    if not resolved:
        raise FileNotFoundError("No parquet input files found for market-data upload.")
    return resolved


def scan_market_data_inputs(paths: list[str | Path]) -> pl.LazyFrame:
    """Scan local parquet inputs with the canonical daily-stock schema."""
    resolved = _resolve_input_paths(paths)
    scan = pl.scan_parquet(resolved)
    schema_names = scan.collect_schema().names()

    missing = [column for column in DAILY_STOCK_DATA_REQUIRED_COLUMNS if column not in schema_names]
    if missing:
        raise ValueError(f"Market data is missing required column(s): {missing}")

    return scan.select([pl.col(column).cast(DAILY_STOCK_DATA_SCHEMA[column]) for column in DAILY_STOCK_DATA_COLUMNS])


def list_market_data_partitions(
    paths: list[str | Path],
    *,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[MarketDataPartition]:
    """List canonical `(exchange, year, month)` partitions present in the input."""
    scan = scan_market_data_inputs(paths)
    if date_from is not None:
        scan = scan.filter(pl.col("date") >= pl.lit(date_from))
    if date_to is not None:
        scan = scan.filter(pl.col("date") <= pl.lit(date_to))

    partitions = (
        scan.with_columns(
            pl.col("date").dt.year().alias("year"),
            pl.col("date").dt.month().alias("month"),
        )
        .group_by(["exchange", "year", "month"])
        .agg(pl.len().alias("rows"))
        .sort(["exchange", "year", "month"])
        .collect()
    )

    return [
        MarketDataPartition(
            exchange=row["exchange"],
            year=int(row["year"]),
            month=int(row["month"]),
            rows=int(row["rows"]),
        )
        for row in partitions.iter_rows(named=True)
    ]


def upload_market_data_partitions(
    client: R2Client,
    *,
    paths: list[str | Path],
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[MarketDataPartition]:
    """Rewrite canonical monthly market-data partitions on R2 from local parquet inputs."""
    scan = scan_market_data_inputs(paths)
    if date_from is not None:
        scan = scan.filter(pl.col("date") >= pl.lit(date_from))
    if date_to is not None:
        scan = scan.filter(pl.col("date") <= pl.lit(date_to))

    partitions = list_market_data_partitions(paths, date_from=date_from, date_to=date_to)
    if not partitions:
        return []

    with TemporaryDirectory() as tmpdir:
        tmp_root = Path(tmpdir)
        enriched = scan.with_columns(
            pl.col("date").dt.year().alias("year"),
            pl.col("date").dt.month().alias("month"),
        )

        for partition in partitions:
            existing_keys = [key for key in client.list_keys(partition.prefix) if key.endswith(".parquet")]
            if existing_keys:
                client.delete_keys(existing_keys)

            frame = (
                enriched.filter(
                    (pl.col("exchange") == partition.exchange)
                    & (pl.col("year") == partition.year)
                    & (pl.col("month") == partition.month)
                )
                .select([pl.col(column) for column in DAILY_STOCK_DATA_COLUMNS])
                .sort(["date", "exchange", "symbol", "isin", "series"])
                .collect()
            )
            local_path = tmp_root / f"{partition.exchange}-{partition.year}-{partition.month:02d}.parquet"
            frame.write_parquet(local_path)
            client.upload_file(local_path, partition.key)

    return partitions
