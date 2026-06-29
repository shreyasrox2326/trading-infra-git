"""Local historical data health checks."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from trading_infra.data.fetch_manifest import default_raw_fetch_manifest_path, read_raw_fetch_manifest
from trading_infra.storage.paths import daily_stock_data_prefix
from trading_infra.storage.r2 import R2Client


@dataclass(frozen=True)
class HistoryDoctorResult:
    """Output paths and report payload for history-doctor."""

    json_path: Path
    markdown_path: Path
    report: dict[str, Any]


def default_history_doctor_output_dir() -> Path:
    return Path("data") / "import" / "audit"


def _weekdays(start_date: date, end_date: date) -> list[date]:
    days = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _partition_key(exchange: str, year: int, month: int) -> str:
    return f"{exchange}:{year}:{month:02d}"


def _local_partition_months(path: Path, exchange: str) -> set[str]:
    if not path.exists():
        return set()
    months = set()
    for file in path.glob(f"exchange={exchange}/year=*/month=*/part.parquet"):
        parts = {part.split("=", 1)[0]: part.split("=", 1)[1] for part in file.parent.parts[-3:]}
        months.add(_partition_key(parts["exchange"], int(parts["year"]), int(parts["month"])))
    return months


def _manifest_months(manifest: pl.DataFrame, exchange: str) -> set[str]:
    if manifest.is_empty():
        return set()
    good_statuses = ["downloaded", "skipped_existing", "validated"]
    dates = (
        manifest.filter((pl.col("exchange") == exchange) & pl.col("status").is_in(good_statuses))
        .select("date")
        .get_column("date")
        .to_list()
    )
    return {_partition_key(exchange, value.year, value.month) for value in dates}


def _r2_partition_months(client: R2Client, exchange: str) -> set[str]:
    keys = client.list_keys(f"data/daily_stock_data/exchange={exchange}/")
    months = set()
    for key in keys:
        if not key.endswith("/part.parquet"):
            continue
        parts = key.split("/")
        try:
            year = int(next(part.split("=", 1)[1] for part in parts if part.startswith("year=")))
            month = int(next(part.split("=", 1)[1] for part in parts if part.startswith("month=")))
        except (StopIteration, ValueError):
            continue
        months.add(_partition_key(exchange, year, month))
    return months


def run_history_doctor(
    *,
    exchange: str,
    raw_manifest_path: str | Path | None = None,
    history_path: str | Path = Path("data") / "import" / "daily_stock_data_full",
    output_dir: str | Path | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    compare_r2: bool = False,
    client: R2Client | None = None,
) -> HistoryDoctorResult:
    """Write local history doctor JSON/Markdown reports."""
    normalized_exchange = exchange.upper()
    manifest_path = Path(raw_manifest_path) if raw_manifest_path else default_raw_fetch_manifest_path(normalized_exchange)
    history_root = Path(history_path)
    output_root = Path(output_dir) if output_dir else default_history_doctor_output_dir()
    output_root.mkdir(parents=True, exist_ok=True)

    manifest = read_raw_fetch_manifest(manifest_path) if manifest_path.exists() else pl.DataFrame()
    exchange_manifest = (
        manifest.filter(pl.col("exchange") == normalized_exchange)
        if not manifest.is_empty()
        else manifest
    )
    if start_date is None and not exchange_manifest.is_empty():
        start_date = exchange_manifest.get_column("date").min()
    if end_date is None and not exchange_manifest.is_empty():
        end_date = exchange_manifest.get_column("date").max()

    expected_weekdays = len(_weekdays(start_date, end_date)) if start_date and end_date else 0
    status_counts = (
        dict(
            exchange_manifest.group_by("status")
            .agg(pl.len().alias("count"))
            .iter_rows()
        )
        if not exchange_manifest.is_empty()
        else {}
    )
    expected_months = _manifest_months(exchange_manifest, normalized_exchange)
    local_months = _local_partition_months(history_root, normalized_exchange)
    r2_months = _r2_partition_months(client or R2Client.from_env(), normalized_exchange) if compare_r2 else set()

    raw_missing = max(0, expected_weekdays - int(exchange_manifest.height)) if expected_weekdays else 0
    parquet_missing = sorted(expected_months - local_months)
    r2_missing = sorted(expected_months - r2_months) if compare_r2 else []
    local_r2_mismatches = sorted(local_months.symmetric_difference(r2_months)) if compare_r2 else []
    fail_reasons = []
    warn_reasons = []
    if status_counts.get("failed", 0) or status_counts.get("rate_limited", 0):
        fail_reasons.append("raw fetch manifest has failed or rate_limited rows")
    if raw_missing:
        warn_reasons.append("raw fetch manifest does not cover all expected weekdays")
    if parquet_missing:
        warn_reasons.append("local parquet partitions are missing for manifest months")
    if compare_r2 and r2_missing:
        warn_reasons.append("R2 partitions are missing for manifest months")

    status = "fail" if fail_reasons else "warn" if warn_reasons else "ok"
    report = {
        "exchange": normalized_exchange,
        "raw_manifest_path": manifest_path.as_posix(),
        "history_path": history_root.as_posix(),
        "expected_trading_weekdays": expected_weekdays,
        "raw_downloaded": int(status_counts.get("downloaded", 0)),
        "raw_missing": raw_missing,
        "raw_rate_limited": int(status_counts.get("rate_limited", 0)),
        "raw_unparseable": int(status_counts.get("parse_failed", 0) + status_counts.get("corrupt_html", 0)),
        "raw_status_counts": {key: int(value) for key, value in status_counts.items()},
        "parquet_partitions_expected": len(expected_months),
        "parquet_partitions_present": len(local_months),
        "parquet_partitions_missing": parquet_missing,
        "r2_partitions_present": len(r2_months) if compare_r2 else None,
        "r2_partitions_missing": r2_missing,
        "local_r2_mismatches": local_r2_mismatches,
        "status": status,
        "fail_reasons": fail_reasons,
        "warn_reasons": warn_reasons,
    }

    json_path = output_root / f"history_doctor_{normalized_exchange}.json"
    markdown_path = output_root / f"history_doctor_{normalized_exchange}.md"
    json_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text(_history_doctor_markdown(report), encoding="utf-8")
    return HistoryDoctorResult(json_path=json_path, markdown_path=markdown_path, report=report)


def _history_doctor_markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# History Doctor {report['exchange']}",
        "",
        f"- status: {report['status']}",
        f"- expected_trading_weekdays: {report['expected_trading_weekdays']}",
        f"- raw_downloaded: {report['raw_downloaded']}",
        f"- raw_missing: {report['raw_missing']}",
        f"- raw_rate_limited: {report['raw_rate_limited']}",
        f"- raw_unparseable: {report['raw_unparseable']}",
        f"- parquet_partitions_expected: {report['parquet_partitions_expected']}",
        f"- parquet_partitions_present: {report['parquet_partitions_present']}",
        f"- parquet_partitions_missing: {report['parquet_partitions_missing']}",
        f"- r2_partitions_present: {report['r2_partitions_present']}",
        f"- r2_partitions_missing: {report['r2_partitions_missing']}",
        f"- local_r2_mismatches: {report['local_r2_mismatches']}",
        "",
    ]
    return "\n".join(lines)
