"""Historical market-data partition manifest helpers."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
from tqdm import tqdm

from trading_infra.data.market_data import DAILY_STOCK_DATA_COLUMNS
from trading_infra.storage.r2 import file_md5


def _partition_manifest_path(output_root: Path) -> Path:
    return output_root.parent / "manifests" / "partition_manifest.parquet"


def _partition_identity(path: Path) -> tuple[str, int, int]:
    parts = {part.split("=", 1)[0]: part.split("=", 1)[1] for part in path.parts[-3:]}
    return parts["exchange"], int(parts["year"]), int(parts["month"])


def _write_partition_manifest(manifest_rows: list[dict[str, Any]], output_root: Path) -> Path:
    manifest_path = _partition_manifest_path(output_root)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(manifest_rows).write_parquet(manifest_path)
    return manifest_path


def _build_partition_manifest_row_from_file(partition_file: Path) -> dict[str, Any]:
    frame = pl.read_parquet(partition_file).select([pl.col(column) for column in DAILY_STOCK_DATA_COLUMNS])
    exchange, year, month = _partition_identity(partition_file.parent)
    return {
        "exchange": exchange,
        "year": year,
        "month": month,
        "partition_path": partition_file.as_posix(),
        "row_count": frame.height,
        "min_date": frame.get_column("date").min(),
        "max_date": frame.get_column("date").max(),
        "symbols": frame.get_column("symbol").n_unique(),
        "file_size_bytes": partition_file.stat().st_size,
        "md5": file_md5(partition_file),
        "source_raw_count": None,
        "source_raw_files": None,
        "source_md5s": None,
        "format_ids": None,
        "parser_versions": "bhavcopy",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "verified_at": None,
        "status": "refreshed",
    }


def refresh_partition_manifest(
    *,
    history_path: str | Path,
    show_progress: bool = False,
    workers: int = 4,
) -> Path:
    """Regenerate partition_manifest.parquet from the current partition files on disk."""
    output_root = _resolve_history_output_path(history_path)
    partition_files = sorted(output_root.glob("exchange=*/year=*/month=*/part.parquet"))
    if not partition_files:
        raise FileNotFoundError(f"No history partition files found under: {output_root}")
    worker_count = max(1, workers)
    if worker_count <= 1:
        iterable = (
            tqdm(partition_files, desc="history-manifest-refresh", unit="partition")
            if show_progress
            else partition_files
        )
        manifest_rows = [_build_partition_manifest_row_from_file(path) for path in iterable]
    else:
        manifest_rows = []
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [executor.submit(_build_partition_manifest_row_from_file, path) for path in partition_files]
            completed = as_completed(futures)
            if show_progress:
                completed = tqdm(completed, total=len(futures), desc="history-manifest-refresh", unit="partition")
            for future in completed:
                manifest_rows.append(future.result())
    manifest_rows.sort(key=lambda row: (row["exchange"], int(row["year"]), int(row["month"])))
    return _write_partition_manifest(manifest_rows, output_root)


def _resolve_history_output_path(output_path: str | Path) -> Path:
    path = Path(output_path)
    if path.suffix == ".parquet":
        return path.with_suffix("")
    return path


