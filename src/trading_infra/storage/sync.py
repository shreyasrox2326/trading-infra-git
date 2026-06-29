"""Local partition manifest to R2 sync checks."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import polars as pl

from trading_infra.storage.paths import daily_stock_data_prefix
from trading_infra.storage.r2 import R2Client


@dataclass(frozen=True)
class R2SyncResult:
    """R2 sync check rows and status."""

    status: str
    rows: list[dict[str, Any]]


def _sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_key(exchange: str, year: int, month: int) -> str:
    return f"{daily_stock_data_prefix(exchange, year, month)}/part.parquet"


def _row_count(path: Path) -> int:
    return int(pl.scan_parquet(path).select(pl.len()).collect().item())


def check_r2_sync(
    client: R2Client,
    *,
    exchange: str,
    partition_manifest_path: str | Path,
) -> R2SyncResult:
    """Compare local partition manifest entries with canonical R2 objects."""
    normalized_exchange = exchange.upper()
    manifest = pl.read_parquet(partition_manifest_path)
    required = {"exchange", "year", "month", "partition_path", "row_count", "file_size_bytes", "sha256"}
    missing = required - set(manifest.columns)
    if missing:
        raise ValueError(f"Partition manifest is missing columns: {sorted(missing)}")
    local_rows = [
        row
        for row in manifest.filter(pl.col("exchange") == normalized_exchange).iter_rows(named=True)
    ]
    local_keys = {
        _canonical_key(row["exchange"], int(row["year"]), int(row["month"]))
        for row in local_rows
    }
    r2_keys = {
        key
        for key in client.list_keys(f"data/daily_stock_data/exchange={normalized_exchange}/")
        if key.endswith("/part.parquet")
    }

    rows: list[dict[str, Any]] = []
    with TemporaryDirectory() as tmpdir:
        tmp_root = Path(tmpdir)
        for row in local_rows:
            key = _canonical_key(row["exchange"], int(row["year"]), int(row["month"]))
            local_path = Path(row["partition_path"])
            sync_row = {
                "exchange": row["exchange"],
                "year": int(row["year"]),
                "month": int(row["month"]),
                "local_partition_path": row["partition_path"],
                "r2_key": key,
                "local_row_count": int(row["row_count"]),
                "r2_row_count": None,
                "local_file_size": int(row["file_size_bytes"]),
                "r2_file_size": None,
                "local_sha256": row["sha256"],
                "r2_sha256": None,
                "manifest_entry": True,
                "status": "MISSING",
            }
            if key in r2_keys:
                downloaded = tmp_root / f"{row['exchange']}-{row['year']}-{row['month']:02d}.parquet"
                client.download_file(key, downloaded)
                sync_row["r2_file_size"] = downloaded.stat().st_size
                sync_row["r2_row_count"] = _row_count(downloaded)
                sync_row["r2_sha256"] = _sha256(downloaded)
                sync_row["status"] = (
                    "OK"
                    if sync_row["r2_file_size"] == sync_row["local_file_size"]
                    and sync_row["r2_row_count"] == sync_row["local_row_count"]
                    and sync_row["r2_sha256"] == sync_row["local_sha256"]
                    else "STALE"
                )
            rows.append(sync_row)

    for extra_key in sorted(r2_keys - local_keys):
        rows.append(
            {
                "exchange": normalized_exchange,
                "year": None,
                "month": None,
                "local_partition_path": None,
                "r2_key": extra_key,
                "local_row_count": None,
                "r2_row_count": None,
                "local_file_size": None,
                "r2_file_size": None,
                "local_sha256": None,
                "r2_sha256": None,
                "manifest_entry": False,
                "status": "EXTRA",
            }
        )

    status = "ok" if all(row["status"] == "OK" for row in rows) else "fail"
    return R2SyncResult(status=status, rows=rows)
