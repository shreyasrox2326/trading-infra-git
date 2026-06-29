"""Historical data bootstrap orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from trading_infra.data.bhavcopy import fetch_bhavcopy_archives
from trading_infra.data.fetch_manifest import default_raw_fetch_manifest_path, write_raw_fetch_manifest
from trading_infra.data.history import build_history_partitions, write_history_audit
from trading_infra.data.history_doctor import run_history_doctor
from trading_infra.storage.history import upload_verified_history
from trading_infra.storage.r2 import R2Client


@dataclass(frozen=True)
class HistoryBootstrapResult:
    """Summary for one history-bootstrap run."""

    exchange: str
    status: str
    raw_manifest_path: Path
    history_path: Path
    audit_path: Path
    partition_manifest_path: Path
    doctor_json_path: Path
    uploaded_partitions: int
    steps: list[dict[str, Any]]


def run_history_bootstrap(
    *,
    exchange: str,
    start_date: date,
    end_date: date,
    raw_output_path: str | Path,
    history_path: str | Path,
    audit_path: str | Path,
    raw_manifest_path: str | Path | None = None,
    partition_manifest_path: str | Path | None = None,
    resume: bool = True,
    upload: bool = False,
    workers: int = 1,
    retries: int = 3,
    request_sleep_seconds: float = 0.0,
    retry_sleep_seconds: float = 1.0,
    max_memory_gb: float | None = None,
    client: R2Client | None = None,
) -> HistoryBootstrapResult:
    """Run the local historical bootstrap workflow for one exchange."""
    normalized_exchange = exchange.upper()
    raw_manifest = Path(raw_manifest_path) if raw_manifest_path else default_raw_fetch_manifest_path(normalized_exchange)
    output_history = Path(history_path)
    audit = Path(audit_path)
    steps: list[dict[str, Any]] = []

    fetch_results = fetch_bhavcopy_archives(
        exchange=normalized_exchange,
        start_date=start_date,
        end_date=end_date,
        output_path=raw_output_path,
        overwrite=not resume,
        workers=workers,
        retries=retries,
        retry_sleep_seconds=retry_sleep_seconds,
        request_sleep_seconds=request_sleep_seconds,
        show_progress=True,
    )
    write_raw_fetch_manifest(fetch_results, exchange=normalized_exchange, path=raw_manifest)
    fetch_counts: dict[str, int] = {}
    for result in fetch_results:
        fetch_counts[result.status] = fetch_counts.get(result.status, 0) + 1
    steps.append({"step": "fetch", "counts": fetch_counts})

    if fetch_counts.get("failed", 0) or fetch_counts.get("rate_limited", 0):
        return HistoryBootstrapResult(
            exchange=normalized_exchange,
            status="fail",
            raw_manifest_path=raw_manifest,
            history_path=output_history,
            audit_path=audit,
            partition_manifest_path=Path(partition_manifest_path) if partition_manifest_path else output_history.parent / "manifests" / "partition_manifest.parquet",
            doctor_json_path=Path(""),
            uploaded_partitions=0,
            steps=steps,
        )

    build_result = build_history_partitions(
        input_path=raw_output_path,
        output_path=output_history,
        exchanges=[normalized_exchange],
        workers=workers,
        show_progress=True,
        clean=True,
        source_manifest_path=raw_manifest,
    )
    partition_manifest = Path(partition_manifest_path) if partition_manifest_path else build_result.manifest_path
    if partition_manifest != build_result.manifest_path:
        partition_manifest.parent.mkdir(parents=True, exist_ok=True)
        partition_manifest.write_bytes(build_result.manifest_path.read_bytes())
    steps.append({"step": "build", "rows": build_result.rows, "partitions": build_result.partitions})

    audit_payload = write_history_audit(
        path=output_history,
        report_path=audit,
        max_memory_gb=max_memory_gb,
    )
    steps.append({"step": "verify", "passed": audit_payload["passed"], "rows": audit_payload["rows"]})
    if not audit_payload["passed"]:
        return HistoryBootstrapResult(
            exchange=normalized_exchange,
            status="fail",
            raw_manifest_path=raw_manifest,
            history_path=output_history,
            audit_path=audit,
            partition_manifest_path=partition_manifest,
            doctor_json_path=Path(""),
            uploaded_partitions=0,
            steps=steps,
        )

    doctor = run_history_doctor(
        exchange=normalized_exchange,
        raw_manifest_path=raw_manifest,
        history_path=output_history,
    )
    steps.append({"step": "doctor", "status": doctor.report["status"]})
    if doctor.report["status"] == "fail":
        return HistoryBootstrapResult(
            exchange=normalized_exchange,
            status="fail",
            raw_manifest_path=raw_manifest,
            history_path=output_history,
            audit_path=audit,
            partition_manifest_path=partition_manifest,
            doctor_json_path=doctor.json_path,
            uploaded_partitions=0,
            steps=steps,
        )

    uploaded_partitions = 0
    if upload:
        results = upload_verified_history(
            client or R2Client.from_env(),
            path=output_history,
            audit_path=audit,
            exchanges=[normalized_exchange],
            raw_manifest_path=raw_manifest,
            partition_manifest_path=partition_manifest,
        )
        uploaded_partitions = len(results)
        steps.append({"step": "upload", "partitions": uploaded_partitions})

    return HistoryBootstrapResult(
        exchange=normalized_exchange,
        status="ok",
        raw_manifest_path=raw_manifest,
        history_path=output_history,
        audit_path=audit,
        partition_manifest_path=partition_manifest,
        doctor_json_path=doctor.json_path,
        uploaded_partitions=uploaded_partitions,
        steps=steps,
    )
