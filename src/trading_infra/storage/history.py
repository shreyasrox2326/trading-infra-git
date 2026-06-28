"""Verified historical market-data upload helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import polars as pl

from trading_infra.data.history import resolve_history_parquet_files
from trading_infra.data.market_data import DAILY_STOCK_DATA_COLUMNS
from trading_infra.storage.market_data import MarketDataPartition, list_market_data_partitions
from trading_infra.storage.paths import daily_stock_data_prefix
from trading_infra.storage.r2 import R2Client


@dataclass(frozen=True)
class HistoryUploadResult:
    """Summary for one promoted historical market-data partition."""

    exchange: str
    year: int
    month: int
    rows: int
    staging_key: str
    canonical_key: str


def _load_passing_audit(audit_path: str | Path) -> dict:
    audit = json.loads(Path(audit_path).read_text(encoding="utf-8"))
    if not audit.get("passed"):
        raise ValueError(f"History audit did not pass: {audit_path}")
    return audit


def _partition_key(partition: MarketDataPartition) -> str:
    return f"{daily_stock_data_prefix(partition.exchange, partition.year, partition.month)}/part.parquet"


def _staging_key(run_id: str, partition: MarketDataPartition) -> str:
    return f"_staging/history-load/{run_id}/{_partition_key(partition)}"


def _write_partition(frame: pl.DataFrame, partition: MarketDataPartition, path: Path) -> None:
    (
        frame.filter(
            (pl.col("exchange") == partition.exchange)
            & (pl.col("date").dt.year() == partition.year)
            & (pl.col("date").dt.month() == partition.month)
        )
        .select([pl.col(column) for column in DAILY_STOCK_DATA_COLUMNS])
        .sort(["date", "exchange", "symbol", "isin", "series"])
        .write_parquet(path)
    )


def _verify_uploaded_size(client: R2Client, key: str, local_path: Path) -> None:
    with TemporaryDirectory() as tmpdir:
        downloaded = Path(tmpdir) / local_path.name
        client.download_file(key, downloaded)
        if downloaded.stat().st_size != local_path.stat().st_size:
            raise ValueError(f"Uploaded object size mismatch for key: {key}")


def upload_verified_history(
    client: R2Client,
    *,
    path: str | Path,
    audit_path: str | Path,
    exchanges: list[str] | None = None,
    run_id: str | None = None,
) -> list[HistoryUploadResult]:
    """Upload verified canonical history through staging before canonical promotion."""
    _load_passing_audit(audit_path)
    selected_exchanges = [exchange.upper() for exchange in exchanges] if exchanges else None
    frame = pl.read_parquet(resolve_history_parquet_files(path))
    if selected_exchanges:
        frame = frame.filter(pl.col("exchange").is_in(selected_exchanges))
    if frame.is_empty():
        raise ValueError("No rows available for selected historical upload exchanges.")

    with TemporaryDirectory() as tmpdir:
        tmp_root = Path(tmpdir)
        filtered_path = tmp_root / "filtered-history.parquet"
        frame.write_parquet(filtered_path)
        partitions = list_market_data_partitions([filtered_path])
        load_id = run_id or uuid4().hex
        local_partitions: list[tuple[MarketDataPartition, Path, str, str]] = []

        for partition in partitions:
            local_path = tmp_root / f"{partition.exchange}-{partition.year}-{partition.month:02d}.parquet"
            staging_key = _staging_key(load_id, partition)
            canonical_key = _partition_key(partition)
            _write_partition(frame, partition, local_path)
            client.upload_file(local_path, staging_key)
            local_partitions.append((partition, local_path, staging_key, canonical_key))

        for _partition, local_path, staging_key, _canonical_key in local_partitions:
            _verify_uploaded_size(client, staging_key, local_path)

        results: list[HistoryUploadResult] = []
        for partition, local_path, staging_key, canonical_key in local_partitions:
            client.upload_file(local_path, canonical_key)
            stale_keys = [
                key
                for key in client.list_keys(partition.prefix)
                if key.endswith(".parquet") and key != canonical_key
            ]
            client.delete_keys(stale_keys)
            results.append(
                HistoryUploadResult(
                    exchange=partition.exchange,
                    year=partition.year,
                    month=partition.month,
                    rows=partition.rows,
                    staging_key=staging_key,
                    canonical_key=canonical_key,
                )
            )

        manifest = {
            "run_id": load_id,
            "source_path": str(path),
            "audit_path": str(audit_path),
            "partitions": [result.__dict__ for result in results],
        }
        client.upload_text("data/daily_stock_data/_manifest.json", json.dumps(manifest, indent=2) + "\n")
        return results
