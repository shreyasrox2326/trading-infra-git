"""Local partition manifest to R2 sync checks."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
from tqdm import tqdm

from trading_infra.storage.paths import daily_stock_data_prefix
from trading_infra.storage.r2 import R2Client, expected_upload_etag, is_multipart_etag


@dataclass(frozen=True)
class R2SyncResult:
    """R2 sync check rows and status."""

    status: str
    rows: list[dict[str, Any]]


def _canonical_key(exchange: str, year: int, month: int) -> str:
    return f"{daily_stock_data_prefix(exchange, year, month)}/part.parquet"


def check_r2_sync(
    client: R2Client,
    *,
    exchange: str,
    partition_manifest_path: str | Path,
    show_progress: bool = False,
    workers: int = 8,
) -> R2SyncResult:
    """Compare local partition manifest entries with canonical R2 objects."""
    normalized_exchange = exchange.upper()
    manifest = pl.read_parquet(partition_manifest_path)
    required = {"exchange", "year", "month", "partition_path", "file_size_bytes", "md5"}
    missing = required - set(manifest.columns)
    if missing:
        if missing == {"md5"}:
            raise ValueError(
                "Partition manifest is missing columns: ['md5']; regenerate it with "
                "`python -m trading_infra history-partition-manifest-refresh --history-path ...`."
            )
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

    def build_sync_row(row: dict[str, Any]) -> dict[str, Any]:
        key = _canonical_key(row["exchange"], int(row["year"]), int(row["month"]))
        local_path = Path(row["partition_path"])
        sync_row = {
            "exchange": row["exchange"],
            "year": int(row["year"]),
            "month": int(row["month"]),
            "local_partition_path": row["partition_path"],
            "r2_key": key,
            "local_file_size": int(row["file_size_bytes"]),
            "r2_file_size": None,
            "local_etag": f'"{row["md5"]}"',
            "r2_etag": None,
            "manifest_entry": True,
            "status": "MISSING",
        }
        if key in r2_keys:
            remote = client.head_object(key)
            sync_row["r2_file_size"] = int(remote["size"])
            sync_row["r2_etag"] = remote.get("etag")
            if is_multipart_etag(sync_row["r2_etag"]):
                sync_row["local_etag"] = expected_upload_etag(local_path, md5_hex=row["md5"])
            sync_row["status"] = (
                "OK"
                if sync_row["r2_file_size"] == sync_row["local_file_size"]
                and sync_row["r2_etag"] == sync_row["local_etag"]
                else "STALE"
            )
        return sync_row

    worker_count = max(1, workers)
    if worker_count <= 1:
        iterable = tqdm(local_rows, desc="r2-sync-check", unit="partition") if show_progress else local_rows
        rows = [build_sync_row(row) for row in iterable]
    else:
        rows = []
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [executor.submit(build_sync_row, row) for row in local_rows]
            completed = as_completed(futures)
            if show_progress:
                completed = tqdm(completed, total=len(futures), desc="r2-sync-check", unit="partition")
            for future in completed:
                rows.append(future.result())

    for extra_key in sorted(r2_keys - local_keys):
        rows.append(
            {
                "exchange": normalized_exchange,
                "year": None,
                "month": None,
                "local_partition_path": None,
                "r2_key": extra_key,
                "local_file_size": None,
                "r2_file_size": None,
                "local_etag": None,
                "r2_etag": None,
                "manifest_entry": False,
                "status": "EXTRA",
            }
        )

    rows.sort(
        key=lambda row: (
            row["exchange"],
            row["year"] is None,
            -1 if row["year"] is None else int(row["year"]),
            -1 if row["month"] is None else int(row["month"]),
            row["r2_key"],
        )
    )
    status = "ok" if all(row["status"] == "OK" for row in rows) else "fail"
    return R2SyncResult(status=status, rows=rows)
