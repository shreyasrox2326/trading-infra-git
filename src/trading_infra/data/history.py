"""Local historical market-data assembly and verification."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import polars as pl
from tqdm import tqdm

from trading_infra.data.bhavcopy import (
    _resolve_bhavcopy_inputs,
    fetch_bhavcopy_archives,
    normalize_bhavcopy_file,
    summarize_canonical_bhavcopy,
)
from trading_infra.data.market_data import DAILY_STOCK_DATA_COLUMNS, DAILY_STOCK_DATA_SCHEMA

SUPPORTED_HISTORY_EXCHANGES: tuple[str, ...] = ("NSE", "BSE")


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


def _normalize_exchange_files(
    files: list[Path],
    *,
    exchange: str,
    workers: int,
    show_progress: bool,
) -> pl.DataFrame:
    if not files:
        raise FileNotFoundError(f"No bhavcopy CSV or ZIP files found for exchange={exchange}.")

    def normalize_one(path: Path) -> pl.DataFrame:
        return normalize_bhavcopy_file(path, exchange=exchange)

    if workers <= 1:
        iterable = tqdm(files, desc=f"{exchange} build", unit="file") if show_progress else files
        frames = [normalize_one(path) for path in iterable]
    else:
        frames_by_index: dict[int, pl.DataFrame] = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(normalize_one, path): index for index, path in enumerate(files)}
            completed = as_completed(futures)
            if show_progress:
                completed = tqdm(completed, total=len(futures), desc=f"{exchange} build", unit="file")
            for future in completed:
                frames_by_index[futures[future]] = future.result()
        frames = [frames_by_index[index] for index in range(len(files))]

    return pl.concat(frames).select(
        [pl.col(column).cast(DAILY_STOCK_DATA_SCHEMA[column]) for column in DAILY_STOCK_DATA_COLUMNS]
    )


def build_history_parquet(
    *,
    input_path: str | Path,
    output_path: str | Path,
    exchanges: list[str] | None = None,
    workers: int = 1,
    show_progress: bool = False,
) -> tuple[Path, pl.DataFrame]:
    """Build one canonical full-history parquet from raw exchange bhavcopy files."""
    input_root = Path(input_path)
    frames: list[pl.DataFrame] = []
    for exchange in _normalize_exchanges(exchanges):
        source = _exchange_input_path(input_root, exchange)
        if not source.exists():
            continue
        files = _resolve_bhavcopy_inputs(source)
        frames.append(
            _normalize_exchange_files(
                files,
                exchange=exchange,
                workers=workers,
                show_progress=show_progress,
            )
        )

    if not frames:
        raise FileNotFoundError(f"No bhavcopy inputs found under: {input_root}")

    combined = (
        pl.concat(frames)
        .select([pl.col(column).cast(DAILY_STOCK_DATA_SCHEMA[column]) for column in DAILY_STOCK_DATA_COLUMNS])
        .unique(maintain_order=True)
        .sort(["date", "exchange", "symbol", "isin", "series"])
    )
    duplicate_count = combined.select(
        pl.struct(["date", "exchange", "isin", "series"]).is_duplicated().sum()
    ).item()
    if duplicate_count:
        raise ValueError("Canonical history contains duplicate date/exchange/isin/series rows.")

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    combined.write_parquet(output)
    return output, combined


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


def write_history_audit(*, path: str | Path, report_path: str | Path) -> dict[str, Any]:
    """Verify a canonical history parquet and write JSON plus a short Markdown report."""
    frame = pl.read_parquet(path)
    audit = verify_history_frame(frame)
    report = Path(report_path)
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(audit, indent=2, default=_json_default) + "\n", encoding="utf-8")
    markdown_path = report.with_suffix(".md")
    lines = [
        "# Historical Market Data Audit",
        "",
        f"- passed: {audit['passed']}",
        f"- rows: {audit['rows']}",
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
