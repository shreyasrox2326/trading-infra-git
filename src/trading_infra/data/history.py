"""Local historical market-data assembly and verification."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from hashlib import sha256
import json
import shutil
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
from tqdm import tqdm

from trading_infra.data.bhavcopy import (
    NonBhavcopyFileError,
    _resolve_bhavcopy_inputs,
    fetch_bhavcopy_archives,
    normalize_bhavcopy_file,
    summarize_canonical_bhavcopy,
)
from trading_infra.data.market_data import DAILY_STOCK_DATA_COLUMNS, DAILY_STOCK_DATA_SCHEMA

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


def _normalize_exchange_files_to_fragments(
    files: list[Path],
    *,
    exchange: str,
    workers: int,
    show_progress: bool,
    fragments_root: Path,
    logger: HistoryBuildLogger,
) -> tuple[int, int]:
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
            _write_fragment_partitions(frame, fragments_root=fragments_root, counters=counters)
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
                _write_fragment_partitions(frame, fragments_root=fragments_root, counters=counters)
                if processed % 100 == 0:
                    logger.write(f"exchange={exchange} phase=normalize_progress processed={processed} skipped={skipped}")

    if not counters:
        raise FileNotFoundError(f"No parseable bhavcopy data files found for exchange={exchange}.")

    logger.write(
        f"exchange={exchange} phase=normalize_done processed={processed} skipped={skipped} "
        f"fragment_partitions={len(counters)}"
    )
    return processed, skipped


def _merge_fragment_partitions(*, fragments_root: Path, output_root: Path, logger: HistoryBuildLogger) -> tuple[int, int]:
    partition_dirs = sorted(path for path in fragments_root.glob("exchange=*/year=*/month=*") if path.is_dir())
    rows = 0
    logger.write(f"phase=merge_start partitions={len(partition_dirs)}")
    for index, partition_dir in enumerate(partition_dirs, start=1):
        files = sorted(partition_dir.glob("fragment-*.parquet"))
        parts = {part.split("=", 1)[0]: part.split("=", 1)[1] for part in partition_dir.parts[-3:]}
        exchange = parts["exchange"]
        year = int(parts["year"])
        month = int(parts["month"])
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
        frame.write_parquet(output_dir / "part.parquet")
        rows += frame.height
        logger.write(
            f"phase=merge_partition_done index={index}/{len(partition_dirs)} exchange={exchange} "
            f"year={year} month={month:02d} rows={frame.height}"
        )
    logger.write(f"phase=merge_done partitions={len(partition_dirs)} rows={rows}")
    return rows, len(partition_dirs)


def _resolve_history_output_path(output_path: str | Path) -> Path:
    path = Path(output_path)
    if path.suffix == ".parquet":
        return path.with_suffix("")
    return path


def resolve_history_parquet_files(path: str | Path) -> list[Path]:
    """Resolve canonical history parquet files from a file or partition directory."""
    source = Path(path)
    if not source.exists() and source.suffix == ".parquet" and source.with_suffix("").exists():
        source = source.with_suffix("")
    if source.is_dir():
        files = sorted(source.glob("exchange=*/year=*/month=*/part.parquet"))
    else:
        files = [source]
    if not files:
        raise FileNotFoundError(f"No history parquet files found under: {source}")
    return files


def build_history_partitions(
    *,
    input_path: str | Path,
    output_path: str | Path,
    exchanges: list[str] | None = None,
    workers: int = 1,
    show_progress: bool = False,
    log_path: str | Path | None = None,
) -> HistoryBuildResult:
    """Build canonical monthly history parquet partitions from raw bhavcopy files."""
    input_root = Path(input_path)
    output_root = _resolve_history_output_path(output_path)
    logger = HistoryBuildLogger(log_path)
    logger.write(
        f"phase=build_start input_path={input_root} output_path={output_root} "
        f"workers={workers} exchanges={_normalize_exchanges(exchanges)}"
    )

    if output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    fragments_root = output_root / "_fragments"
    fragments_root.mkdir(parents=True, exist_ok=True)

    skipped = 0
    built_exchanges: list[str] = []
    for exchange in _normalize_exchanges(exchanges):
        source = _exchange_input_path(input_root, exchange)
        if not source.exists():
            logger.write(f"exchange={exchange} status=missing_source path={source}")
            continue
        files = _resolve_bhavcopy_inputs(source)
        _processed, exchange_skipped = _normalize_exchange_files_to_fragments(
            files,
            exchange=exchange,
            workers=workers,
            show_progress=show_progress,
            fragments_root=fragments_root,
            logger=logger,
        )
        skipped += exchange_skipped
        built_exchanges.append(exchange)

    if not built_exchanges:
        raise FileNotFoundError(f"No bhavcopy inputs found under: {input_root}")

    rows, partitions = _merge_fragment_partitions(
        fragments_root=fragments_root,
        output_root=output_root,
        logger=logger,
    )
    shutil.rmtree(fragments_root)
    logger.write(
        f"phase=build_done output_path={output_root} rows={rows} partitions={partitions} "
        f"skipped_non_bhavcopy={skipped}"
    )
    return HistoryBuildResult(
        output_path=output_root,
        log_path=logger.path,
        rows=rows,
        partitions=partitions,
        skipped_non_bhavcopy=skipped,
        exchanges=built_exchanges,
    )


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


def _json_default(value: Any) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _missing_weekdays(dates: list[date]) -> list[str]:
    if not dates:
        return []
    present = set(dates)
    missing: list[str] = []
    current = min(present)
    end = max(present)
    while current <= end:
        if current.weekday() < 5 and current not in present:
            missing.append(current.isoformat())
        current += timedelta(days=1)
    return missing


def verify_history_frame(frame: pl.DataFrame) -> dict[str, Any]:
    """Return a machine-readable audit for canonical market history."""
    missing = [column for column in DAILY_STOCK_DATA_COLUMNS if column not in frame.columns]
    extra = [column for column in frame.columns if column not in DAILY_STOCK_DATA_COLUMNS]
    normalized = frame.select(
        [pl.col(column).cast(DAILY_STOCK_DATA_SCHEMA[column], strict=False) for column in DAILY_STOCK_DATA_COLUMNS]
    ) if not missing else frame

    required_non_null = ("date", "exchange", "isin", "symbol", "series", "close", "adj_close", "volume")
    null_counts = {
        column: int(normalized.get_column(column).null_count())
        for column in DAILY_STOCK_DATA_COLUMNS
        if column in normalized.columns
    }
    duplicate_count = (
        int(normalized.select(pl.struct(["date", "exchange", "isin", "series"]).is_duplicated().sum()).item())
        if not missing
        else 0
    )
    invalid_ohlc_count = (
        int(
            normalized.select(
                (
                    (pl.col("high") < pl.col("low"))
                    | (pl.col("open") < 0)
                    | (pl.col("high") < 0)
                    | (pl.col("low") < 0)
                    | (pl.col("close") < 0)
                )
                .fill_null(False)
                .sum()
            ).item()
        )
        if not missing
        else 0
    )
    negative_volume_count = (
        int(normalized.select((pl.col("volume") < 0).fill_null(False).sum()).item())
        if not missing
        else 0
    )
    negative_turnover_count = (
        int(normalized.select((pl.col("turnover") < 0).fill_null(False).sum()).item())
        if not missing
        else 0
    )

    by_exchange = []
    by_month = []
    missing_weekdays_by_exchange: dict[str, list[str]] = {}
    if not missing and normalized.height:
        exchange_summary = normalized.group_by("exchange").agg(
            pl.len().alias("rows"),
            pl.col("date").min().alias("date_min"),
            pl.col("date").max().alias("date_max"),
            pl.col("symbol").n_unique().alias("symbols"),
        ).sort("exchange")
        by_exchange = list(exchange_summary.iter_rows(named=True))

        month_summary = (
            normalized.with_columns(
                pl.col("date").dt.year().alias("year"),
                pl.col("date").dt.month().alias("month"),
            )
            .group_by(["exchange", "year", "month"])
            .agg(pl.len().alias("rows"))
            .sort(["exchange", "year", "month"])
        )
        by_month = list(month_summary.iter_rows(named=True))
        for exchange in normalized.get_column("exchange").unique().sort().to_list():
            dates = (
                normalized.filter(pl.col("exchange") == exchange)
                .get_column("date")
                .unique()
                .sort()
                .to_list()
            )
            missing_weekdays_by_exchange[exchange] = _missing_weekdays(dates)

    required_null_columns = [column for column in required_non_null if null_counts.get(column, 0) > 0]
    passed = not (
        missing
        or extra
        or required_null_columns
        or duplicate_count
        or invalid_ohlc_count
        or negative_volume_count
        or negative_turnover_count
    )
    return {
        "passed": passed,
        "rows": normalized.height if not missing else frame.height,
        "missing_columns": missing,
        "unexpected_columns": extra,
        "required_null_columns": required_null_columns,
        "null_counts": null_counts,
        "duplicate_key_count": duplicate_count,
        "invalid_ohlc_count": invalid_ohlc_count,
        "negative_volume_count": negative_volume_count,
        "negative_turnover_count": negative_turnover_count,
        "by_exchange": by_exchange,
        "by_month": by_month,
        "missing_weekdays_by_exchange": missing_weekdays_by_exchange,
        "identity_adjustment": True,
    }


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _merge_date_min(current: date | None, candidate: date | None) -> date | None:
    if current is None:
        return candidate
    if candidate is None:
        return current
    return min(current, candidate)


def _merge_date_max(current: date | None, candidate: date | None) -> date | None:
    if current is None:
        return candidate
    if candidate is None:
        return current
    return max(current, candidate)


def verify_history_partitions(files: list[Path]) -> dict[str, Any]:
    """Verify history parquet files one partition at a time."""
    rows = 0
    missing_columns: set[str] = set()
    unexpected_columns: set[str] = set()
    required_null_columns: set[str] = set()
    null_counts = {column: 0 for column in DAILY_STOCK_DATA_COLUMNS}
    duplicate_key_count = 0
    invalid_ohlc_count = 0
    negative_volume_count = 0
    negative_turnover_count = 0
    exchange_stats: dict[str, dict[str, Any]] = {}
    dates_by_exchange: dict[str, set[date]] = {}
    by_month: list[dict[str, Any]] = []
    partition_summaries: list[dict[str, Any]] = []

    for file in files:
        frame = pl.read_parquet(file)
        audit = verify_history_frame(frame)
        rows += int(audit["rows"])
        missing_columns.update(audit["missing_columns"])
        unexpected_columns.update(audit["unexpected_columns"])
        required_null_columns.update(audit["required_null_columns"])
        for column, count in audit["null_counts"].items():
            null_counts[column] = null_counts.get(column, 0) + int(count)
        duplicate_key_count += int(audit["duplicate_key_count"])
        invalid_ohlc_count += int(audit["invalid_ohlc_count"])
        negative_volume_count += int(audit["negative_volume_count"])
        negative_turnover_count += int(audit["negative_turnover_count"])
        by_month.extend(audit["by_month"])

        if not audit["missing_columns"] and frame.height:
            date_min = frame.get_column("date").min()
            date_max = frame.get_column("date").max()
            symbol_count = frame.get_column("symbol").n_unique()
            for row in audit["by_exchange"]:
                exchange = row["exchange"]
                stats = exchange_stats.setdefault(
                    exchange,
                    {"exchange": exchange, "rows": 0, "date_min": None, "date_max": None, "symbols": set()},
                )
                stats["rows"] += int(row["rows"])
                stats["date_min"] = _merge_date_min(stats["date_min"], row["date_min"])
                stats["date_max"] = _merge_date_max(stats["date_max"], row["date_max"])
                stats["symbols"].update(
                    frame.filter(pl.col("exchange") == exchange).get_column("symbol").unique().to_list()
                )
                dates_by_exchange.setdefault(exchange, set()).update(
                    frame.filter(pl.col("exchange") == exchange).get_column("date").unique().to_list()
                )

        partition_summaries.append(
            {
                "path": file.as_posix(),
                "passed": audit["passed"],
                "rows": audit["rows"],
                "schema_columns": frame.columns,
                "dtypes": {column: str(dtype) for column, dtype in zip(frame.columns, frame.dtypes, strict=False)},
                "date_min": date_min if not audit["missing_columns"] and frame.height else None,
                "date_max": date_max if not audit["missing_columns"] and frame.height else None,
                "symbols": symbol_count if not audit["missing_columns"] and frame.height else 0,
                "file_size_bytes": file.stat().st_size,
                "sha256": _file_sha256(file),
                "missing_columns": audit["missing_columns"],
                "unexpected_columns": audit["unexpected_columns"],
                "required_null_columns": audit["required_null_columns"],
                "null_counts": audit["null_counts"],
                "duplicate_key_count": audit["duplicate_key_count"],
                "invalid_ohlc_count": audit["invalid_ohlc_count"],
                "negative_volume_count": audit["negative_volume_count"],
                "negative_turnover_count": audit["negative_turnover_count"],
            }
        )

    by_exchange = []
    for exchange in sorted(exchange_stats):
        stats = exchange_stats[exchange]
        by_exchange.append(
            {
                "exchange": exchange,
                "rows": stats["rows"],
                "date_min": stats["date_min"],
                "date_max": stats["date_max"],
                "symbols": len(stats["symbols"]),
            }
        )
    missing_weekdays_by_exchange = {
        exchange: _missing_weekdays(sorted(dates))
        for exchange, dates in sorted(dates_by_exchange.items())
    }
    passed = not (
        missing_columns
        or unexpected_columns
        or required_null_columns
        or duplicate_key_count
        or invalid_ohlc_count
        or negative_volume_count
        or negative_turnover_count
    )
    return {
        "passed": passed,
        "rows": rows,
        "missing_columns": sorted(missing_columns),
        "unexpected_columns": sorted(unexpected_columns),
        "required_null_columns": sorted(required_null_columns),
        "null_counts": null_counts,
        "duplicate_key_count": duplicate_key_count,
        "invalid_ohlc_count": invalid_ohlc_count,
        "negative_volume_count": negative_volume_count,
        "negative_turnover_count": negative_turnover_count,
        "by_exchange": by_exchange,
        "by_month": sorted(by_month, key=lambda row: (row["exchange"], row["year"], row["month"])),
        "missing_weekdays_by_exchange": missing_weekdays_by_exchange,
        "partition_summaries": partition_summaries,
        "partitions": len(partition_summaries),
        "identity_adjustment": True,
        "verification_mode": "partition-wise",
    }


def _enforce_history_verify_memory_cap(files: list[Path], max_memory_gb: float | None) -> None:
    if max_memory_gb is None:
        return
    if max_memory_gb <= 0:
        raise ValueError("--max-memory-gb must be positive.")
    largest_file_size = max((file.stat().st_size for file in files), default=0)
    estimated_peak_bytes = largest_file_size * 4
    cap_bytes = max_memory_gb * 1024 * 1024 * 1024
    if estimated_peak_bytes > cap_bytes:
        raise MemoryError(
            f"history-verify estimated peak memory {estimated_peak_bytes} bytes exceeds cap {int(cap_bytes)} bytes"
        )


def write_history_audit(
    *,
    path: str | Path,
    report_path: str | Path,
    max_memory_gb: float | None = None,
) -> dict[str, Any]:
    """Verify a canonical history parquet and write JSON plus a short Markdown report."""
    files = resolve_history_parquet_files(path)
    _enforce_history_verify_memory_cap(files, max_memory_gb)
    audit = verify_history_partitions(files)
    report = Path(report_path)
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(audit, indent=2, default=_json_default) + "\n", encoding="utf-8")
    markdown_path = report.with_suffix(".md")
    lines = [
        "# Historical Market Data Audit",
        "",
        f"- passed: {audit['passed']}",
        f"- rows: {audit['rows']}",
        f"- partitions: {audit['partitions']}",
        f"- verification_mode: {audit['verification_mode']}",
        f"- duplicate_key_count: {audit['duplicate_key_count']}",
        f"- invalid_ohlc_count: {audit['invalid_ohlc_count']}",
        f"- negative_volume_count: {audit['negative_volume_count']}",
        f"- negative_turnover_count: {audit['negative_turnover_count']}",
        f"- missing_columns: {audit['missing_columns']}",
        f"- unexpected_columns: {audit['unexpected_columns']}",
        f"- required_null_columns: {audit['required_null_columns']}",
        f"- missing_weekdays_by_exchange: {audit['missing_weekdays_by_exchange']}",
        "",
        "## By Exchange",
        "",
    ]
    for row in audit["by_exchange"]:
        lines.append(
            f"- {row['exchange']}: rows={row['rows']} date_min={row['date_min']} "
            f"date_max={row['date_max']} symbols={row['symbols']}"
        )
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return audit


def summarize_history_frame(frame: pl.DataFrame) -> dict[str, Any]:
    """Build a concise summary for CLI output."""
    summary = summarize_canonical_bhavcopy(frame)
    return {
        "rows": summary.rows,
        "date_min": summary.date_min,
        "date_max": summary.date_max,
        "exchanges": summary.exchanges,
        "symbols": summary.symbols,
        "missing_deliverable_qty": summary.missing_deliverable_qty,
        "missing_delivery_pct": summary.missing_delivery_pct,
    }
