"""Market-data and storage maintenance CLI command handlers."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date
from pathlib import Path

import polars as pl

from trading_infra.bootstrap import run_history_bootstrap
from trading_infra.data.bhavcopy import BhavcopyRateLimitError, fetch_bhavcopy_archives
from trading_infra.data.bhavcopy_normalize import write_canonical_bhavcopy_parquet
from trading_infra.data.fetch_manifest import (
    combine_raw_fetch_manifests,
    default_raw_fetch_manifest_path,
    mark_raw_fetch_manifest_row,
    select_manifest_dates,
    write_raw_fetch_manifest,
)
from trading_infra.data.formats import inspect_bhavcopy_format
from trading_infra.data.history_build import build_history_partitions
from trading_infra.data.history_doctor import run_history_doctor
from trading_infra.data.history_partitions import refresh_partition_manifest
from trading_infra.data.history_verify import write_history_audit
from trading_infra.storage.history import cleanup_staging_prefix, upload_verified_history
from trading_infra.storage.market_data import list_market_data_partitions, upload_market_data_partitions
from trading_infra.storage.r2 import R2Client
from trading_infra.storage.refresh import refresh_market_data_for_date
from trading_infra.storage.sync import check_r2_sync
from trading_infra.storage.usage import apply_r2_budget, collect_r2_usage, write_r2_usage_snapshot

LOGGER = logging.getLogger("trading_infra.commands.market_data")


def _emit_summary(command: str, *, status: str, **payload) -> None:
    summary = {"command": command, "status": status, **payload}
    LOGGER.info("%s summary status=%s", command, status)
    print(f"{command} summary_json={json.dumps(summary, default=str, sort_keys=True)}")
    print(f"{command} status={status}")


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def market_data_upload(args: argparse.Namespace) -> int:
    date_from = _parse_date(args.date_from) if args.date_from else None
    date_to = _parse_date(args.date_to) if args.date_to else None
    partitions = list_market_data_partitions(args.path, date_from=date_from, date_to=date_to)
    client = R2Client.from_env()
    uploaded = upload_market_data_partitions(
        client,
        paths=args.path,
        date_from=date_from,
        date_to=date_to,
    )
    print(
        f"market-data-upload paths={len(args.path)} partitions={len(uploaded)} "
        f"date_from={date_from} date_to={date_to}"
    )
    for partition in partitions:
        print(
            f"{partition.exchange} year={partition.year} month={partition.month:02d} "
            f"rows={partition.rows} key={partition.key}"
        )
    return 0


def market_data_refresh(args: argparse.Namespace) -> int:
    client = R2Client.from_env()
    result = refresh_market_data_for_date(
        client,
        exchange=args.exchange,
        refresh_date=_parse_date(args.date),
    )
    print(
        f"market-data-refresh exchange={result.exchange} date={result.refresh_date} "
        f"status={result.status} rows={result.rows} staging_key={result.staging_key} "
        f"canonical_key={result.canonical_key} message={result.message}"
    )
    return 1 if result.status == "failed" else 0


def bhavcopy_fetch(args: argparse.Namespace) -> int:
    results = fetch_bhavcopy_archives(
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
        output_path=args.output_path,
        exchange=args.exchange,
        overwrite=args.overwrite,
        workers=args.workers,
        retries=args.retries,
        show_progress=args.progress,
    )
    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    print(
        f"bhavcopy-fetch exchange={args.exchange} start_date={args.start_date} "
        f"end_date={args.end_date} output_path={args.output_path} counts={counts}"
    )
    failures = [result for result in results if result.status == "failed"]
    log_path = Path(args.log_path) if args.log_path else Path(args.output_path) / "bhavcopy-fetch.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(
        "\n".join(
            f"{result.requested_date},{result.status},{result.path.as_posix() if result.path else ''},{result.message}"
            for result in results
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"bhavcopy-fetch log={log_path.as_posix()}")
    for failure in failures[:10]:
        print(f"failed date={failure.requested_date} message={failure.message}")
    return 1 if failures else 0


def bhavcopy_ingest(args: argparse.Namespace) -> int:
    output_path, summary = write_canonical_bhavcopy_parquet(
        input_path=args.input_path,
        output_path=args.output_path,
        exchange=args.exchange,
    )
    print(
        f"bhavcopy-ingest input_path={args.input_path} output_path={output_path.as_posix()} "
        f"rows={summary.rows} date_min={summary.date_min} date_max={summary.date_max} "
        f"exchanges={summary.exchanges} symbols={summary.symbols} "
        f"missing_deliverable_qty={summary.missing_deliverable_qty} "
        f"missing_delivery_pct={summary.missing_delivery_pct}"
    )
    return 0


def history_fetch(args: argparse.Namespace) -> int:
    log_path = Path(args.log_path) if args.log_path else Path(args.output_path) / "history-fetch.log"
    manifest_path = Path(args.manifest_path) if args.manifest_path else default_raw_fetch_manifest_path(args.exchange)
    only_statuses = (
        {status.strip() for status in args.only.split(",") if status.strip()}
        if args.only
        else None
    )
    requested_dates = (
        select_manifest_dates(manifest_path, statuses=only_statuses)
        if only_statuses is not None
        else None
    )
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("", encoding="utf-8")

    def log_result(result) -> None:
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(
                f"{result.requested_date},{result.status},"
                f"{result.path.as_posix() if result.path else ''},{result.message}\n"
            )
            handle.flush()

    aborted_message = ""
    try:
        results = fetch_bhavcopy_archives(
            exchange=args.exchange,
            start_date=_parse_date(args.start_date),
            end_date=_parse_date(args.end_date),
            output_path=args.output_path,
            overwrite=args.overwrite,
            workers=args.workers,
            retries=args.retries,
            retry_sleep_seconds=args.retry_sleep_seconds,
            request_sleep_seconds=args.request_sleep_seconds,
            show_progress=args.progress,
            on_result=log_result,
            requested_dates=requested_dates,
            fail_fast_rate_limit_ratio=args.fail_fast_rate_limit_ratio,
        )
    except BhavcopyRateLimitError as exc:
        results = exc.results
        aborted_message = str(exc)
    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    write_raw_fetch_manifest(results, exchange=args.exchange, path=manifest_path)
    print(
        f"history-fetch exchange={args.exchange.upper()} start_date={args.start_date} "
        f"end_date={args.end_date} output_path={args.output_path} workers={args.workers} "
        f"retries={args.retries} retry_sleep_seconds={args.retry_sleep_seconds} "
        f"request_sleep_seconds={args.request_sleep_seconds} counts={counts} "
        f"only={args.only} log={log_path.as_posix()} manifest={manifest_path.as_posix()}"
    )
    if aborted_message:
        print(f"history-fetch status=fail message={aborted_message}")
    status = "fail" if aborted_message or counts.get("failed", 0) or counts.get("rate_limited", 0) else "ok"
    _emit_summary(
        "history-fetch",
        status=status,
        exchange=args.exchange.upper(),
        counts=counts,
        log_path=log_path.as_posix(),
        manifest_path=manifest_path.as_posix(),
    )
    return 0 if status == "ok" else 1


def history_build(args: argparse.Namespace) -> int:
    repair_partition = (
        (args.repair_partition[0], int(args.repair_partition[1]), int(args.repair_partition[2]))
        if args.repair_partition
        else None
    )
    result = build_history_partitions(
        input_path=args.input_path,
        output_path=args.output_path,
        exchanges=args.exchange,
        workers=args.workers,
        show_progress=args.progress,
        log_path=args.log_path,
        clean=args.clean,
        incremental=args.incremental,
        only_missing=args.only_missing,
        repair_partition=repair_partition,
        source_manifest_path=args.from_manifest,
    )
    print(
        f"history-build input_path={args.input_path} output_path={result.output_path.as_posix()} "
        f"rows={result.rows} partitions={result.partitions} exchanges={result.exchanges} "
        f"skipped_non_bhavcopy={result.skipped_non_bhavcopy} workers={args.workers} "
        f"clean={args.clean} incremental={args.incremental} only_missing={args.only_missing} "
        f"repair_partition={repair_partition} log={result.log_path.as_posix()} "
        f"manifest={result.manifest_path.as_posix()}"
    )
    _emit_summary(
        "history-build",
        status="ok",
        rows=result.rows,
        partitions=result.partitions,
        manifest_path=result.manifest_path.as_posix(),
        log_path=result.log_path.as_posix(),
    )
    return 0


def history_manifest_mark(args: argparse.Namespace) -> int:
    path = mark_raw_fetch_manifest_row(
        args.manifest_path,
        target_date=_parse_date(args.date),
        status=args.status,
        reason=args.reason,
        exchange=args.exchange,
    )
    print(
        f"history-manifest-mark manifest={path.as_posix()} date={args.date} "
        f"status={args.status} exchange={args.exchange or ''} reason={args.reason}"
    )
    return 0


def history_manifest_combine(args: argparse.Namespace) -> int:
    path = combine_raw_fetch_manifests(args.paths, output_path=args.output)
    manifest = pl.read_parquet(path)
    print(
        f"history-manifest-combine output={path.as_posix()} inputs={len(args.paths)} "
        f"rows={manifest.height} exchanges={sorted(set(manifest.get_column('exchange').to_list()))}"
    )
    return 0


def history_partition_manifest_refresh(args: argparse.Namespace) -> int:
    path = refresh_partition_manifest(
        history_path=args.history_path,
        show_progress=args.progress,
        workers=args.workers,
    )
    manifest = pl.read_parquet(path)
    print(
        f"history-partition-manifest-refresh history_path={args.history_path} workers={args.workers} "
        f"manifest={path.as_posix()} rows={manifest.height}"
    )
    return 0


def history_verify(args: argparse.Namespace) -> int:
    audit = write_history_audit(
        path=args.path,
        report_path=args.report_path,
        max_memory_gb=args.max_memory_gb,
        show_progress=args.progress,
    )
    print(
        f"history-verify path={args.path} report_path={args.report_path} "
        f"passed={str(audit['passed']).lower()} rows={audit['rows']} "
        f"partitions={audit['partitions']} verification_mode={audit['verification_mode']} "
        f"duplicate_key_count={audit['duplicate_key_count']} invalid_ohlc_count={audit['invalid_ohlc_count']}"
    )
    status = "ok" if audit["passed"] else "fail"
    _emit_summary(
        "history-verify",
        status=status,
        rows=audit["rows"],
        partitions=audit["partitions"],
        report_path=args.report_path,
    )
    return 0 if audit["passed"] else 1


def history_doctor(args: argparse.Namespace) -> int:
    result = run_history_doctor(
        exchange=args.exchange,
        raw_manifest_path=args.raw_manifest_path,
        history_path=args.history_path,
        output_dir=args.output_dir,
        start_date=_parse_date(args.start_date) if args.start_date else None,
        end_date=_parse_date(args.end_date) if args.end_date else None,
        compare_r2=args.compare_r2,
    )
    report = result.report
    print(
        f"history-doctor exchange={report['exchange']} status={report['status']} "
        f"raw_downloaded={report['raw_downloaded']} raw_skipped_existing={report['raw_skipped_existing']} "
        f"raw_usable={report['raw_usable']} raw_missing={report['raw_missing']} "
        f"raw_rate_limited={report['raw_rate_limited']} raw_unparseable={report['raw_unparseable']} "
        f"parquet_partitions_present={report['parquet_partitions_present']} "
        f"parquet_partitions_missing={len(report['parquet_partitions_missing'])} "
        f"json={result.json_path.as_posix()} markdown={result.markdown_path.as_posix()}"
    )
    _emit_summary(
        "history-doctor",
        status=report["status"],
        exchange=report["exchange"],
        json_path=result.json_path.as_posix(),
        markdown_path=result.markdown_path.as_posix(),
    )
    return 1 if report["status"] == "fail" else 0


def history_bootstrap(args: argparse.Namespace) -> int:
    allow_fetch_statuses = tuple(
        status.strip()
        for status in (args.allow_fetch_status or "").split(",")
        if status.strip()
    )
    result = run_history_bootstrap(
        exchange=args.exchange,
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
        raw_output_path=args.raw_output_path,
        history_path=args.history_path,
        audit_path=args.audit_path,
        raw_manifest_path=args.raw_manifest_path,
        partition_manifest_path=args.partition_manifest_path,
        resume=args.resume,
        upload=args.upload == "true",
        workers=args.workers,
        retries=args.retries,
        request_sleep_seconds=args.request_sleep_seconds,
        retry_sleep_seconds=args.retry_sleep_seconds,
        max_memory_gb=args.max_memory_gb,
        allow_fetch_statuses=allow_fetch_statuses,
    )
    print(
        f"history-bootstrap exchange={result.exchange} status={result.status} "
        f"raw_manifest={result.raw_manifest_path.as_posix()} history_path={result.history_path.as_posix()} "
        f"audit={result.audit_path.as_posix()} partition_manifest={result.partition_manifest_path.as_posix()} "
        f"uploaded_partitions={result.uploaded_partitions}"
    )
    _emit_summary(
        "history-bootstrap",
        status=result.status,
        exchange=result.exchange,
        uploaded_partitions=result.uploaded_partitions,
        steps=result.steps,
    )
    return 0 if result.status == "ok" else 1


def history_upload(args: argparse.Namespace) -> int:
    client = R2Client.from_env()
    budget = apply_r2_budget(collect_r2_usage(client))
    if budget["status"] == "fail":
        print(f"history-upload status=fail budget_status=fail reasons={budget['fail_reasons']}")
        return 1
    results = upload_verified_history(
        client,
        path=args.path,
        audit_path=args.audit_path,
        exchanges=args.exchange,
        raw_manifest_path=args.raw_manifest_path,
        partition_manifest_path=args.partition_manifest_path,
        workers=args.workers,
        show_progress=args.progress,
    )
    print(
        f"history-upload path={args.path} audit_path={args.audit_path} "
        f"budget_status={budget['status']} partitions={len(results)}"
    )
    for result in results:
        print(
            f"{result.exchange} year={result.year} month={result.month:02d} rows={result.rows} "
            f"staging_key={result.staging_key} canonical_key={result.canonical_key}"
        )
    _emit_summary(
        "history-upload",
        status="ok",
        partitions=len(results),
        budget_status=budget["status"],
    )
    return 0


def r2_sync_check(args: argparse.Namespace) -> int:
    client = R2Client.from_env()
    result = check_r2_sync(
        client,
        exchange=args.exchange,
        partition_manifest_path=args.partition_manifest_path,
        show_progress=args.progress,
        workers=args.workers,
    )
    print(f"r2-sync-check exchange={args.exchange.upper()} status={result.status} rows={len(result.rows)}")
    for row in result.rows:
        print(
            f"{row['status']} {row['exchange']} year={row['year']} month={row['month']} "
            f"local_etag={row['local_etag']} r2_etag={row['r2_etag']} "
            f"local_size={row['local_file_size']} r2_size={row['r2_file_size']} key={row['r2_key']}"
        )
    return 0 if result.status == "ok" else 1


def r2_usage(args: argparse.Namespace) -> int:
    client = R2Client.from_env()
    report = collect_r2_usage(client, prefix=args.prefix)
    snapshot = write_r2_usage_snapshot(report, output_dir=args.snapshot_dir)
    print(
        f"r2-usage bucket={report['bucket']} storage_bytes={report['storage_bytes']} "
        f"object_count={report['object_count']} snapshot={snapshot.as_posix()}"
    )
    return 0


def r2_budget_check(args: argparse.Namespace) -> int:
    client = R2Client.from_env()
    report = apply_r2_budget(collect_r2_usage(client, prefix=args.prefix))
    snapshot = write_r2_usage_snapshot(report, output_dir=args.snapshot_dir)
    print(
        f"r2-budget-check bucket={report['bucket']} storage_bytes={report['storage_bytes']} "
        f"object_count={report['object_count']} "
        f"class_a_operations_month_to_date={report['class_a_operations_month_to_date']} "
        f"class_b_operations_month_to_date={report['class_b_operations_month_to_date']} "
        f"estimated_free_tier_remaining={report['estimated_free_tier_remaining']} "
        f"estimated_monthly_cost={report['estimated_monthly_cost']} status={report['status']} "
        f"snapshot={snapshot.as_posix()}"
    )
    return 0 if report["status"] != "fail" else 1


def r2_cleanup_staging(args: argparse.Namespace) -> int:
    client = R2Client.from_env()
    stale_keys = cleanup_staging_prefix(
        client,
        prefix=args.prefix,
        older_than_days=args.older_than_days,
        dry_run=args.dry_run,
    )
    print(
        f"r2-cleanup-staging prefix={args.prefix} older_than_days={args.older_than_days} "
        f"dry_run={str(args.dry_run).lower()} matched={len(stale_keys)}"
    )
    for key in stale_keys[:50]:
        print(key)
    return 0


def format_inspect(args: argparse.Namespace) -> int:
    inspected = inspect_bhavcopy_format(args.exchange, _parse_date(args.date))
    if args.json:
        print(json.dumps(inspected, indent=2))
        return 0
    print(
        f"format-inspect exchange={inspected['exchange']} date={inspected['date']} "
        f"format_id={inspected['format_id']} parser={inspected['parser']} "
        f"filename={inspected['filename']} primary_url={inspected['urls'][0]}"
    )
    print(f"required_columns={inspected['required_columns']}")
    print(f"optional_columns={inspected['optional_columns']}")
    return 0


