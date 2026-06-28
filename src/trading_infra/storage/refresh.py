"""Daily market-data refresh helpers for R2-backed paper runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import polars as pl

from trading_infra.data.bhavcopy import fetch_bhavcopy_archive, normalize_bhavcopy_inputs
from trading_infra.data.market_data import DAILY_STOCK_DATA_COLUMNS
from trading_infra.storage.history import _verify_uploaded_size
from trading_infra.storage.paths import daily_stock_data_prefix
from trading_infra.storage.r2 import R2Client
from trading_infra.storage.remote import list_daily_stock_data_keys


@dataclass(frozen=True)
class MarketDataRefreshResult:
    """Result of one exchange/date market-data refresh."""

    status: str
    exchange: str
    refresh_date: date
    rows: int = 0
    staging_key: str | None = None
    canonical_key: str | None = None
    message: str = ""


def _canonical_key(exchange: str, refresh_date: date) -> str:
    prefix = daily_stock_data_prefix(exchange, refresh_date.year, refresh_date.month)
    return f"{prefix}/part.parquet"


def _staging_key(run_id: str, exchange: str, refresh_date: date) -> str:
    return f"_staging/daily-refresh/{run_id}/{_canonical_key(exchange, refresh_date)}"


def _load_existing_month(client: R2Client, *, exchange: str, refresh_date: date, tmp_root: Path) -> pl.DataFrame | None:
    keys = list_daily_stock_data_keys(client, exchange, refresh_date.year, refresh_date.month)
    if not keys:
        return None
    local_paths: list[str] = []
    for index, key in enumerate(keys):
        local_path = tmp_root / f"existing-{index}.parquet"
        client.download_file(key, local_path)
        local_paths.append(str(local_path))
    return pl.read_parquet(local_paths)


def refresh_market_data_for_date(
    client: R2Client,
    *,
    exchange: str,
    refresh_date: date,
    run_id: str | None = None,
) -> MarketDataRefreshResult:
    """Refresh one exchange/date into the affected monthly R2 partition."""
    exchange = exchange.upper()
    with TemporaryDirectory() as tmpdir:
        tmp_root = Path(tmpdir)
        raw_root = tmp_root / "raw"
        fetch_result = fetch_bhavcopy_archive(refresh_date, exchange=exchange, output_path=raw_root)
        if fetch_result.status in {"not_available", "failed"}:
            return MarketDataRefreshResult(
                status="no_data" if fetch_result.status == "not_available" else "failed",
                exchange=exchange,
                refresh_date=refresh_date,
                message=fetch_result.message,
            )
        if fetch_result.path is None:
            return MarketDataRefreshResult(
                status="no_data",
                exchange=exchange,
                refresh_date=refresh_date,
                message="No bhavcopy path returned.",
            )

        refreshed = normalize_bhavcopy_inputs(fetch_result.path, exchange=exchange)
        refreshed = refreshed.filter(pl.col("date") == refresh_date)
        if refreshed.is_empty():
            return MarketDataRefreshResult(
                status="no_data",
                exchange=exchange,
                refresh_date=refresh_date,
                message="Bhavcopy contained no rows for requested date.",
            )

        existing = _load_existing_month(client, exchange=exchange, refresh_date=refresh_date, tmp_root=tmp_root)
        combined = refreshed if existing is None else pl.concat([existing, refreshed], how="vertical_relaxed")
        month = (
            combined.select([pl.col(column) for column in DAILY_STOCK_DATA_COLUMNS])
            .unique(subset=["date", "exchange", "isin", "series"], keep="last", maintain_order=True)
            .sort(["date", "exchange", "symbol", "isin", "series"])
        )

        local_month = tmp_root / "month.parquet"
        month.write_parquet(local_month)
        load_id = run_id or uuid4().hex
        staging_key = _staging_key(load_id, exchange, refresh_date)
        canonical_key = _canonical_key(exchange, refresh_date)

        client.upload_file(local_month, staging_key)
        _verify_uploaded_size(client, staging_key, local_month)
        client.upload_file(local_month, canonical_key)

        prefix = daily_stock_data_prefix(exchange, refresh_date.year, refresh_date.month)
        stale_keys = [
            key
            for key in client.list_keys(prefix)
            if key.endswith(".parquet") and key != canonical_key
        ]
        client.delete_keys(stale_keys)
        return MarketDataRefreshResult(
            status="refreshed",
            exchange=exchange,
            refresh_date=refresh_date,
            rows=month.height,
            staging_key=staging_key,
            canonical_key=canonical_key,
        )
