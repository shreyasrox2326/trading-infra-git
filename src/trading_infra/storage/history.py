"""Verified historical market-data upload helpers."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import polars as pl
from tqdm import tqdm

from trading_infra.data.history import resolve_history_parquet_files
from trading_infra.data.market_data import DAILY_STOCK_DATA_COLUMNS
from trading_infra.data.fetch_manifest import read_raw_fetch_manifest
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
    file_size_bytes: int
    sha256: str


@dataclass(frozen=True)
class _UploadCandidate:
    partition: MarketDataPartition
    source_file: Path
    local_path: Path
    staging_key: str
    canonical_key: str


def _load_passing_audit(audit_path: str | Path) -> dict:
    audit = json.loads(Path(audit_path).read_text(encoding="utf-8"))
    if not audit.get("passed"):
        raise ValueError(f"History audit did not pass: {audit_path}")
    return audit


def _validate_raw_fetch_manifest(path: str | Path) -> None:
    manifest = read_raw_fetch_manifest(path)
    unresolved = manifest.filter(pl.col("status").is_in(["expected", "missing", "failed", "rate_limited"]))
    if unresolved.height:
        raise ValueError(f"Raw fetch manifest has unresolved expected/missing/failed/rate_limited rows: {path}")


def _validate_partition_manifest(path: str | Path) -> None:
    manifest_path = Path(path)
    if not manifest_path.exists():
        raise FileNotFoundError(f"Partition manifest not found: {manifest_path}")
    manifest = pl.read_parquet(manifest_path)
    required = {"exchange", "year", "month", "partition_path", "row_count", "file_size_bytes", "sha256"}
    missing = required - set(manifest.columns)
    if missing:
        raise ValueError(f"Partition manifest is missing columns: {sorted(missing)}")


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


def _materialize_upload_partition(source_file: Path, partition: MarketDataPartition, target_path: Path) -> None:
    frame = pl.read_parquet(source_file)
    partition_frame = (
        frame.filter(
            (pl.col("exchange") == partition.exchange)
            & (pl.col("date").dt.year() == partition.year)
            & (pl.col("date").dt.month() == partition.month)
        )
        .select([pl.col(column) for column in DAILY_STOCK_DATA_COLUMNS])
        .sort(["date", "exchange", "symbol", "isin", "series"])
    )
    partition_frame.write_parquet(target_path)


def _verify_uploaded_size(client: R2Client, key: str, local_path: Path) -> None:
    with TemporaryDirectory() as tmpdir:
        downloaded = Path(tmpdir) / local_path.name
        client.download_file(key, downloaded)
        if downloaded.stat().st_size != local_path.stat().st_size:
            raise ValueError(f"Uploaded object size mismatch for key: {key}")


def _sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _run_upload_tasks(
    items: list,
    worker,
    *,
    workers: int,
    description: str,
    show_progress: bool,
) -> list:
    if workers <= 1:
        iterable = tqdm(items, desc=description, unit="partition") if show_progress else items
        return [worker(item) for item in iterable]

    results = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(worker, item) for item in items]
        completed = as_completed(futures)
        if show_progress:
            completed = tqdm(completed, total=len(futures), desc=description, unit="partition")
        for future in completed:
            results.append(future.result())
    return results


def upload_verified_history(
    client: R2Client,
    *,
    path: str | Path,
    audit_path: str | Path,
    exchanges: list[str] | None = None,
    run_id: str | None = None,
    raw_manifest_path: str | Path | None = None,
    partition_manifest_path: str | Path | None = None,
    workers: int = 4,
    show_progress: bool = True,
) -> list[HistoryUploadResult]:
    """Upload verified canonical history through staging before canonical promotion."""
    _load_passing_audit(audit_path)
    if raw_manifest_path is None:
        raise ValueError("raw_manifest_path is required for verified historical upload.")
    if partition_manifest_path is None:
        raise ValueError("partition_manifest_path is required for verified historical upload.")
    _validate_raw_fetch_manifest(raw_manifest_path)
    _validate_partition_manifest(partition_manifest_path)
    selected_exchanges = [exchange.upper() for exchange in exchanges] if exchanges else None
    source_files = resolve_history_parquet_files(path)

    with TemporaryDirectory() as tmpdir:
        tmp_root = Path(tmpdir)
        load_id = run_id or uuid4().hex
        candidates: list[_UploadCandidate] = []

        for source_file in source_files:
            partitions = list_market_data_partitions([source_file])
            for partition in partitions:
                if selected_exchanges and partition.exchange not in selected_exchanges:
                    continue
                local_path = tmp_root / f"{partition.exchange}-{partition.year}-{partition.month:02d}.parquet"
                staging_key = _staging_key(load_id, partition)
                canonical_key = _partition_key(partition)
                candidates.append(_UploadCandidate(partition, source_file, local_path, staging_key, canonical_key))

        if not candidates:
            raise ValueError("No rows available for selected historical upload exchanges.")

        worker_count = max(1, workers)

        def stage(candidate: _UploadCandidate) -> _UploadCandidate:
            _materialize_upload_partition(candidate.source_file, candidate.partition, candidate.local_path)
            client.upload_file(candidate.local_path, candidate.staging_key)
            return candidate

        staged = _run_upload_tasks(
            candidates,
            stage,
            workers=worker_count,
            description="history-upload stage",
            show_progress=show_progress,
        )

        _run_upload_tasks(
            staged,
            lambda candidate: _verify_uploaded_size(client, candidate.staging_key, candidate.local_path),
            workers=worker_count,
            description="history-upload verify",
            show_progress=show_progress,
        )

        def promote(candidate: _UploadCandidate) -> HistoryUploadResult:
            partition = candidate.partition
            client.upload_file(candidate.local_path, candidate.canonical_key)
            stale_keys = [
                key
                for key in client.list_keys(partition.prefix)
                if key.endswith(".parquet") and key != candidate.canonical_key
            ]
            client.delete_keys(stale_keys)
            return HistoryUploadResult(
                exchange=partition.exchange,
                year=partition.year,
                month=partition.month,
                rows=partition.rows,
                staging_key=candidate.staging_key,
                canonical_key=candidate.canonical_key,
                file_size_bytes=candidate.local_path.stat().st_size,
                sha256=_sha256(candidate.local_path),
            )

        results = _run_upload_tasks(
            staged,
            promote,
            workers=worker_count,
            description="history-upload promote",
            show_progress=show_progress,
        )
        results = sorted(results, key=lambda result: (result.exchange, result.year, result.month))

        manifest = {
            "run_id": load_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "source_path": str(path),
            "audit_path": str(audit_path),
            "source_local_audit_id": str(audit_path),
            "raw_manifest_path": str(raw_manifest_path),
            "partition_manifest_path": str(partition_manifest_path),
            "exchange_coverage": sorted({result.exchange for result in results}),
            "partitions": [result.__dict__ for result in results],
            "upload_status": "promoted",
        }
        client.upload_text("data/daily_stock_data/_manifest.json", json.dumps(manifest, indent=2) + "\n")
        return results
