"""Local historical market-data assembly and verification."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import shutil
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import polars as pl
from tqdm import tqdm

from trading_infra.data.bhavcopy import (
    fetch_bhavcopy_archives,
)
from trading_infra.data.bhavcopy_normalize import (
    NonBhavcopyFileError,
    _infer_bhavcopy_format_from_filename,
    _normalize_exchange,
    _resolve_bhavcopy_inputs,
    normalize_bhavcopy_file,
)
from trading_infra.data.market_data import DAILY_STOCK_DATA_COLUMNS, DAILY_STOCK_DATA_SCHEMA
from trading_infra.data.history_partitions import (
    _partition_identity,
    _partition_manifest_path,
    _resolve_history_output_path,
    _write_partition_manifest,
    refresh_partition_manifest,
)
from trading_infra.data.history_verify import (
    resolve_history_parquet_files,
    summarize_history_frame,
    verify_history_frame,
    verify_history_partitions,
    write_history_audit,
)
from trading_infra.storage.r2 import file_md5

SUPPORTED_HISTORY_EXCHANGES: tuple[str, ...] = ("NSE", "BSE")


@dataclass(frozen=True)
class HistoryBuildResult:
    """Summary for a partition-first history build."""

    output_path: Path
    log_path: Path
    rows: int
    partitions: int
    skipped_non_bhavcopy: int
    exchanges: list[str]
    manifest_path: Path


class HistoryBuildLogger:
    """Small timestamped logfile writer for long local builds."""

    def __init__(self, path: str | Path | None) -> None:
        if path is None:
            stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            path = Path("data") / "import" / f"history-build-{stamp}.log"
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("", encoding="utf-8")

    def write(self, message: str) -> None:
        timestamp = datetime.now().isoformat(timespec="seconds")
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(f"{timestamp} {message}\n")


def _normalize_exchanges(exchanges: list[str] | None) -> list[str]:
    if not exchanges:
        return list(SUPPORTED_HISTORY_EXCHANGES)
    normalized = [exchange.upper() for exchange in exchanges]
    unsupported = [exchange for exchange in normalized if exchange not in SUPPORTED_HISTORY_EXCHANGES]
    if unsupported:
        raise ValueError(f"Unsupported history exchange(s): {unsupported}")
    return normalized


def fetch_history_bhavcopies(
    *,
    exchange: str,
    start_date: date,
    end_date: date,
    output_path: str | Path,
    overwrite: bool = False,
    workers: int = 1,
    retries: int = 3,
    show_progress: bool = False,
) -> dict[str, int]:
    """Fetch raw bhavcopy files for one exchange and return status counts."""
    results = fetch_bhavcopy_archives(
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
        output_path=output_path,
        overwrite=overwrite,
        workers=workers,
        retries=retries,
        show_progress=show_progress,
    )
    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    return counts


def _exchange_input_path(root: Path, exchange: str) -> Path:
    exchange_root = root / exchange
    return exchange_root if exchange_root.exists() else root


def _partition_columns(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.month().alias("month"),
    )


def _write_fragment_partitions(
    frame: pl.DataFrame,
    *,
    fragments_root: Path,
    counters: dict[tuple[str, int, int], int],
    source_path: Path | None = None,
    source_map: dict[tuple[str, int, int], list[dict[str, str]]] | None = None,
    exchange: str | None = None,
) -> None:
    enriched = _partition_columns(frame)
    partitions = enriched.select(["exchange", "year", "month"]).unique().iter_rows(named=True)
    for row in partitions:
        key = (row["exchange"], int(row["year"]), int(row["month"]))
        counters[key] = counters.get(key, 0) + 1
        fragment_dir = fragments_root / f"exchange={key[0]}" / f"year={key[1]}" / f"month={key[2]:02d}"
        fragment_dir.mkdir(parents=True, exist_ok=True)
        (
            enriched.filter(
                (pl.col("exchange") == key[0])
                & (pl.col("year") == key[1])
                & (pl.col("month") == key[2])
            )
            .select(DAILY_STOCK_DATA_COLUMNS)
            .write_parquet(fragment_dir / f"fragment-{counters[key]:06d}.parquet")
        )
        if source_path is not None and source_map is not None:
            format_info = _infer_bhavcopy_format_from_filename(
                source_path,
                exchange=_normalize_exchange(exchange or key[0]),
            )
            source_map.setdefault(key, []).append(
                {
                    "path": source_path.as_posix(),
                    "md5": file_md5(source_path),
                    "format_id": format_info.format_id if format_info is not None else "",
                }
            )


def _normalize_exchange_files_to_fragments(
    files: list[Path],
    *,
    exchange: str,
    workers: int,
    show_progress: bool,
    fragments_root: Path,
    logger: HistoryBuildLogger,
) -> tuple[int, int, dict[tuple[str, int, int], list[dict[str, str]]]]:
    if not files:
        raise FileNotFoundError(f"No bhavcopy CSV or ZIP files found for exchange={exchange}.")

    def normalize_one(path: Path) -> pl.DataFrame | None:
        try:
            return normalize_bhavcopy_file(path, exchange=exchange)
        except NonBhavcopyFileError:
            return None

    processed = 0
    skipped = 0
    counters: dict[tuple[str, int, int], int] = {}
    source_map: dict[tuple[str, int, int], list[dict[str, str]]] = {}
    logger.write(f"exchange={exchange} phase=normalize_start files={len(files)} workers={workers}")

    if workers <= 1:
        iterable = tqdm(files, desc=f"{exchange} build", unit="file") if show_progress else files
        for path in iterable:
            frame = normalize_one(path)
            processed += 1
            if frame is None:
                skipped += 1
                logger.write(f"exchange={exchange} status=skipped_non_bhavcopy file={path}")
                continue
            _write_fragment_partitions(
                frame,
                fragments_root=fragments_root,
                counters=counters,
                source_path=path,
                source_map=source_map,
                exchange=exchange,
            )
            if processed % 100 == 0:
                logger.write(f"exchange={exchange} phase=normalize_progress processed={processed} skipped={skipped}")
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(normalize_one, path): path for path in files}
            completed = as_completed(futures)
            if show_progress:
                completed = tqdm(completed, total=len(futures), desc=f"{exchange} build", unit="file")
            for future in completed:
                path = futures[future]
                frame = future.result()
                processed += 1
                if frame is None:
                    skipped += 1
                    logger.write(f"exchange={exchange} status=skipped_non_bhavcopy file={path}")
                    continue
                _write_fragment_partitions(
                    frame,
                    fragments_root=fragments_root,
                    counters=counters,
                    source_path=path,
                    source_map=source_map,
                    exchange=exchange,
                )
                if processed % 100 == 0:
                    logger.write(f"exchange={exchange} phase=normalize_progress processed={processed} skipped={skipped}")

    if not counters:
        raise FileNotFoundError(f"No parseable bhavcopy data files found for exchange={exchange}.")

    logger.write(
        f"exchange={exchange} phase=normalize_done processed={processed} skipped={skipped} "
        f"fragment_partitions={len(counters)}"
    )
    return processed, skipped, source_map


def _merge_fragment_partitions(
    *,
    fragments_root: Path,
    output_root: Path,
    logger: HistoryBuildLogger,
    source_map: dict[tuple[str, int, int], list[dict[str, str]]],
) -> tuple[int, int, list[dict[str, Any]]]:
    partition_dirs = sorted(path for path in fragments_root.glob("exchange=*/year=*/month=*") if path.is_dir())
    rows = 0
    manifest_rows: list[dict[str, Any]] = []
    logger.write(f"phase=merge_start partitions={len(partition_dirs)}")
    for index, partition_dir in enumerate(partition_dirs, start=1):
        files = sorted(partition_dir.glob("fragment-*.parquet"))
        exchange, year, month = _partition_identity(partition_dir)
        logger.write(
            f"phase=merge_partition_start index={index}/{len(partition_dirs)} exchange={exchange} "
            f"year={year} month={month:02d} fragments={len(files)}"
        )
        frame = (
            pl.read_parquet(files)
            .select([pl.col(column).cast(DAILY_STOCK_DATA_SCHEMA[column]) for column in DAILY_STOCK_DATA_COLUMNS])
        )
        duplicate_count = frame.select(
            pl.struct(["date", "exchange", "isin", "series"]).is_duplicated().sum()
        ).item()
        if duplicate_count:
            raise ValueError(
                f"Canonical partition contains duplicate date/exchange/isin/series rows: "
                f"exchange={exchange}, year={year}, month={month:02d}"
            )
        frame = frame.sort(["date", "exchange", "symbol", "isin", "series"])
        output_dir = output_root / f"exchange={exchange}" / f"year={year}" / f"month={month:02d}"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "part.parquet"
        frame.write_parquet(output_file)
        rows += frame.height
        source_entries = source_map.get((exchange, year, month), [])
        manifest_rows.append(
            {
                "exchange": exchange,
                "year": year,
                "month": month,
                "partition_path": output_file.as_posix(),
                "row_count": frame.height,
                "min_date": frame.get_column("date").min(),
                "max_date": frame.get_column("date").max(),
                "symbols": frame.get_column("symbol").n_unique(),
                "file_size_bytes": output_file.stat().st_size,
                "md5": file_md5(output_file),
                "source_raw_count": len(source_entries) or len(files),
                "source_raw_files": json.dumps([entry["path"] for entry in source_entries]),
                "source_md5s": json.dumps([entry["md5"] for entry in source_entries]),
                "format_ids": json.dumps(sorted({entry["format_id"] for entry in source_entries if entry["format_id"]})),
                "parser_versions": "bhavcopy",
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "verified_at": None,
                "status": "built",
            }
        )
        logger.write(
            f"phase=merge_partition_done index={index}/{len(partition_dirs)} exchange={exchange} "
            f"year={year} month={month:02d} rows={frame.height}"
        )
    logger.write(f"phase=merge_done partitions={len(partition_dirs)} rows={rows}")
    return rows, len(partition_dirs), manifest_rows


def build_history_partitions(
    *,
    input_path: str | Path,
    output_path: str | Path,
    exchanges: list[str] | None = None,
    workers: int = 1,
    show_progress: bool = False,
    log_path: str | Path | None = None,
    clean: bool = True,
    incremental: bool = False,
    only_missing: bool = False,
    repair_partition: tuple[str, int, int] | None = None,
    source_manifest_path: str | Path | None = None,
) -> HistoryBuildResult:
    """Build canonical monthly history parquet partitions from raw bhavcopy files."""
    input_root = Path(input_path)
    output_root = _resolve_history_output_path(output_path)
    logger = HistoryBuildLogger(log_path)
    selected_partition = (
        (repair_partition[0].upper(), int(repair_partition[1]), int(repair_partition[2]))
        if repair_partition
        else None
    )
    update_mode = incremental or only_missing or selected_partition is not None
    logger.write(
        f"phase=build_start input_path={input_root} output_path={output_root} "
        f"workers={workers} exchanges={_normalize_exchanges(exchanges)} clean={clean} "
        f"incremental={incremental} only_missing={only_missing} repair_partition={selected_partition}"
    )

    selected_exchanges = _normalize_exchanges(exchanges)
    scoped_clean = clean and output_root.exists() and exchanges and len(selected_exchanges) < len(SUPPORTED_HISTORY_EXCHANGES)
    if output_root.exists() and clean and not scoped_clean:
        shutil.rmtree(output_root)
    if scoped_clean:
        for exchange in selected_exchanges:
            exchange_dir = output_root / f"exchange={exchange}"
            if exchange_dir.exists():
                shutil.rmtree(exchange_dir)
    if output_root.exists() and not clean and not update_mode:
        raise FileExistsError(f"History output exists; pass --clean, --incremental, --only-missing, or --repair-partition: {output_root}")

    with TemporaryDirectory() as tmpdir:
        build_root = Path(tmpdir) / "history-build" if update_mode else output_root
        build_root.mkdir(parents=True, exist_ok=True)
        result = _build_history_partitions_into(
            input_root=input_root,
            output_root=build_root,
            exchanges=selected_exchanges,
            workers=workers,
            show_progress=show_progress,
            logger=logger,
            source_manifest_path=source_manifest_path,
        )
        if update_mode:
            output_root.mkdir(parents=True, exist_ok=True)
            _copy_built_partitions(
                source_root=build_root,
                output_root=output_root,
                only_missing=only_missing,
                selected_partition=selected_partition,
            )
            manifest_path = refresh_partition_manifest(history_path=output_root)
            manifest_rows = pl.read_parquet(manifest_path)
            rows = int(manifest_rows.get_column("row_count").sum())
            partitions = int(manifest_rows.height)
        else:
            rows = result.rows
            partitions = result.partitions
            manifest_path = refresh_partition_manifest(history_path=output_root) if scoped_clean else result.manifest_path

    logger.write(
        f"phase=build_done output_path={output_root} rows={rows} partitions={partitions} "
        f"skipped_non_bhavcopy={result.skipped_non_bhavcopy}"
    )
    return HistoryBuildResult(
        output_path=output_root,
        log_path=logger.path,
        rows=rows,
        partitions=partitions,
        skipped_non_bhavcopy=result.skipped_non_bhavcopy,
        exchanges=selected_exchanges if scoped_clean else result.exchanges,
        manifest_path=manifest_path,
    )


def _build_history_partitions_into(
    *,
    input_root: Path,
    output_root: Path,
    exchanges: list[str] | None,
    workers: int,
    show_progress: bool,
    logger: HistoryBuildLogger,
    source_manifest_path: str | Path | None,
) -> HistoryBuildResult:
    """Build canonical monthly history parquet partitions into output_root."""
    output_root.mkdir(parents=True, exist_ok=True)
    fragments_root = output_root / "_fragments"
    fragments_root.mkdir(parents=True, exist_ok=True)

    skipped = 0
    built_exchanges: list[str] = []
    merged_source_map: dict[tuple[str, int, int], list[dict[str, str]]] = {}
    for exchange in _normalize_exchanges(exchanges):
        source = _exchange_input_path(input_root, exchange)
        if source_manifest_path is not None:
            files = _resolve_manifest_source_files(source_manifest_path, exchange=exchange)
        elif not source.exists():
            logger.write(f"exchange={exchange} status=missing_source path={source}")
            continue
        else:
            files = _resolve_bhavcopy_inputs(source)
        _processed, exchange_skipped, exchange_source_map = _normalize_exchange_files_to_fragments(
            files,
            exchange=exchange,
            workers=workers,
            show_progress=show_progress,
            fragments_root=fragments_root,
            logger=logger,
        )
        skipped += exchange_skipped
        for key, entries in exchange_source_map.items():
            merged_source_map.setdefault(key, []).extend(entries)
        built_exchanges.append(exchange)

    if not built_exchanges:
        raise FileNotFoundError(f"No bhavcopy inputs found under: {input_root}")

    rows, partitions, manifest_rows = _merge_fragment_partitions(
        fragments_root=fragments_root,
        output_root=output_root,
        logger=logger,
        source_map=merged_source_map,
    )
    shutil.rmtree(fragments_root)
    manifest_path = _write_partition_manifest(manifest_rows, output_root)
    return HistoryBuildResult(
        output_path=output_root,
        log_path=logger.path,
        rows=rows,
        partitions=partitions,
        skipped_non_bhavcopy=skipped,
        exchanges=built_exchanges,
        manifest_path=manifest_path,
    )


def _resolve_manifest_source_files(source_manifest_path: str | Path, *, exchange: str) -> list[Path]:
    manifest = pl.read_parquet(source_manifest_path)
    required = {"exchange", "local_path", "status"}
    missing = required - set(manifest.columns)
    if missing:
        raise ValueError(f"Source manifest is missing columns: {sorted(missing)}")
    local_paths = (
        manifest.filter(
            (pl.col("exchange") == exchange)
            & pl.col("status").is_in(["downloaded", "skipped_existing", "validated"])
            & pl.col("local_path").is_not_null()
        )
        .select("local_path")
        .unique()
        .sort("local_path")
        .get_column("local_path")
        .to_list()
    )
    files = [Path(path) for path in local_paths]
    missing_files = [path for path in files if not path.exists()]
    if missing_files:
        raise FileNotFoundError(f"Source manifest references missing local files: {missing_files[:5]}")
    return files


def _copy_built_partitions(
    *,
    source_root: Path,
    output_root: Path,
    only_missing: bool,
    selected_partition: tuple[str, int, int] | None,
) -> list[dict[str, Any]]:
    source_manifest = pl.read_parquet(_partition_manifest_path(source_root))
    rows: list[dict[str, Any]] = []
    for row in source_manifest.iter_rows(named=True):
        key = (row["exchange"], int(row["year"]), int(row["month"]))
        if selected_partition is not None and key != selected_partition:
            continue
        source_file = Path(row["partition_path"])
        relative = source_file.relative_to(source_root)
        target_file = output_root / relative
        if only_missing and target_file.exists():
            continue
        target_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_file, target_file)
        updated = dict(row)
        updated["partition_path"] = target_file.as_posix()
        updated["file_size_bytes"] = target_file.stat().st_size
        updated["md5"] = file_md5(target_file)
        rows.append(updated)
    if selected_partition is not None and not rows:
        raise FileNotFoundError(f"Repair partition was not produced: {selected_partition}")
    return rows


def build_history_parquet(
    *,
    input_path: str | Path,
    output_path: str | Path,
    exchanges: list[str] | None = None,
    workers: int = 1,
    show_progress: bool = False,
) -> tuple[Path, pl.DataFrame]:
    """Build canonical history partitions and return a combined frame for small/test runs."""
    result = build_history_partitions(
        input_path=input_path,
        output_path=output_path,
        exchanges=exchanges,
        workers=workers,
        show_progress=show_progress,
    )
    parquet_files = sorted(result.output_path.glob("exchange=*/year=*/month=*/part.parquet"))
    combined = pl.read_parquet(parquet_files).sort(["date", "exchange", "symbol", "isin", "series"])
    return result.output_path, combined
