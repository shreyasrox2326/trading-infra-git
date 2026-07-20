"""Command-line entrypoints for local backtests and paper dry-runs."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Sequence

import polars as pl

from trading_infra.data.bhavcopy import (
    BhavcopyRateLimitError,
    fetch_bhavcopy_archives,
)
from trading_infra.data.bhavcopy_normalize import (
    write_canonical_bhavcopy_parquet,
)
from trading_infra.bootstrap import run_history_bootstrap
from trading_infra.data.fetch_manifest import (
    combine_raw_fetch_manifests,
    default_raw_fetch_manifest_path,
    mark_raw_fetch_manifest_row,
    select_manifest_dates,
    write_raw_fetch_manifest,
)
from trading_infra.data.formats import inspect_bhavcopy_format
from trading_infra.data.history_build import build_history_parquet, build_history_partitions
from trading_infra.data.history_doctor import run_history_doctor
from trading_infra.data.history_partitions import refresh_partition_manifest
from trading_infra.data.history_verify import write_history_audit
from trading_infra.data.market_data import load_daily_stock_data, load_trading_dates
from trading_infra.performance import compute_strategy_performance, upload_performance_result, write_performance_result
from trading_infra.pipelines.backtest import run_backtest, run_backtest_chunked
from trading_infra.pipelines.paper import run_daily_paper_job, run_daily_paper_job_from_r2
from trading_infra.registry import active_strategy_ids, load_strategy_registry
from trading_infra.storage.decisions import read_decisions_parquet
from trading_infra.storage.decisions import write_decisions_parquet
from trading_infra.storage.market_data import list_market_data_partitions, upload_market_data_partitions
from trading_infra.storage.market_data_remote import load_daily_stock_data_history_from_r2
from trading_infra.storage.history import cleanup_staging_prefix, upload_verified_history
from trading_infra.storage.remote import (
    download_backtest_decisions,
    download_paper_decisions,
    download_strategy_artifacts,
    load_strategy_registry_from_r2,
    upload_backtest_decisions,
    upload_strategy_artifacts,
    upload_strategy_registry,
)
from trading_infra.storage.refresh import refresh_market_data_for_date
from trading_infra.storage.r2 import R2Client
from trading_infra.storage.sync import check_r2_sync
from trading_infra.storage.usage import apply_r2_budget, collect_r2_usage, write_r2_usage_snapshot
from trading_infra.strategy_builder import build_strategy
from trading_infra.strategy_store import load_stored_strategy

LOGGER = logging.getLogger("trading_infra.cli")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def _emit_summary(command: str, *, status: str, **payload) -> None:
    summary = {"command": command, "status": status, **payload}
    LOGGER.info("%s summary status=%s", command, status)
    print(f"{command} summary_json={json.dumps(summary, default=str, sort_keys=True)}")
    print(f"{command} status={status}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="trading_infra")
    subparsers = parser.add_subparsers(dest="command", required=True)

    paper = subparsers.add_parser("paper-dry-run", help="Run the local daily paper workflow.")
    paper.add_argument("--date", required=True)
    paper.add_argument("--base-path", default=".")
    paper.add_argument("--market-data-path")
    paper.add_argument("--use-r2", action="store_true")
    paper.add_argument("--exchange")
    paper.add_argument("--upload-results", action="store_true")

    backtest = subparsers.add_parser("backtest-run", help="Run a local multi-date backtest.")
    backtest.add_argument("--base-path", default=".")
    backtest.add_argument("--strategy-id", required=True)
    backtest.add_argument("--market-data-path")
    backtest.add_argument("--use-r2", action="store_true")
    backtest.add_argument("--exchange")
    backtest.add_argument("--start-date", required=True)
    backtest.add_argument("--end-date", required=True)
    backtest.add_argument("--output-path")
    backtest.add_argument("--chunk-size-days", type=int, default=252)
    backtest.add_argument("--warmup-days", type=int)
    backtest.add_argument("--progress", action="store_true", default=True)
    backtest.add_argument("--no-progress", dest="progress", action="store_false")

    strategy_upload = subparsers.add_parser("strategy-upload", help="Upload local strategy artifacts to R2.")
    strategy_upload.add_argument("--base-path", default=".")
    strategy_upload.add_argument("--strategy-id", required=True)

    registry_upload = subparsers.add_parser("registry-upload", help="Upload a local registry parquet file to R2.")
    registry_upload.add_argument("--path", required=True)

    backtest_upload = subparsers.add_parser("backtest-upload", help="Upload local backtest decisions to R2.")
    backtest_upload.add_argument("--strategy-id", required=True)
    backtest_upload.add_argument("--path", required=True)

    market_data_upload = subparsers.add_parser(
        "market-data-upload",
        help="Upload canonical daily-stock market data to R2 by exchange/year/month partition.",
    )
    market_data_upload.add_argument("--path", action="append", required=True)
    market_data_upload.add_argument("--date-from")
    market_data_upload.add_argument("--date-to")

    market_data_refresh = subparsers.add_parser(
        "market-data-refresh",
        help="Refresh one exchange/date into its monthly R2 market-data partition.",
    )
    market_data_refresh.add_argument("--date", required=True)
    market_data_refresh.add_argument("--exchange", required=True)

    bhavcopy_fetch = subparsers.add_parser("bhavcopy-fetch", help="Fetch NSE equity bhavcopy archives locally.")
    bhavcopy_fetch.add_argument("--exchange", default="NSE")
    bhavcopy_fetch.add_argument("--start-date", required=True)
    bhavcopy_fetch.add_argument("--end-date", required=True)
    bhavcopy_fetch.add_argument("--output-path", required=True)
    bhavcopy_fetch.add_argument("--log-path")
    bhavcopy_fetch.add_argument("--overwrite", action="store_true")
    bhavcopy_fetch.add_argument("--workers", type=int, default=1)
    bhavcopy_fetch.add_argument("--retries", type=int, default=1)
    bhavcopy_fetch.add_argument("--progress", action="store_true")

    bhavcopy_ingest = subparsers.add_parser(
        "bhavcopy-ingest",
        help="Small/manual raw bhavcopy to canonical daily-stock parquet converter.",
    )
    bhavcopy_ingest.add_argument("--input-path", required=True)
    bhavcopy_ingest.add_argument("--output-path", required=True)
    bhavcopy_ingest.add_argument("--exchange", default="NSE")

    history_fetch = subparsers.add_parser("history-fetch", help="Fetch full-range exchange bhavcopies locally.")
    history_fetch.add_argument("--exchange", required=True)
    history_fetch.add_argument("--start-date", required=True)
    history_fetch.add_argument("--end-date", required=True)
    history_fetch.add_argument("--output-path", required=True)
    history_fetch.add_argument("--overwrite", action="store_true")
    history_fetch.add_argument("--workers", type=int, default=1)
    history_fetch.add_argument("--retries", type=int, default=1)
    history_fetch.add_argument("--retry-sleep-seconds", type=float, default=1.0)
    history_fetch.add_argument("--request-sleep-seconds", type=float, default=0.5)
    history_fetch.add_argument("--log-path")
    history_fetch.add_argument("--manifest-path")
    history_fetch.add_argument("--only")
    history_fetch.add_argument("--fail-fast-rate-limit-ratio", type=float)
    history_fetch.add_argument("--progress", action="store_true", default=True)
    history_fetch.add_argument("--no-progress", dest="progress", action="store_false")

    history_build = subparsers.add_parser(
        "history-build",
        help="Build production canonical full-history market-data parquet partitions.",
    )
    history_build.add_argument("--input-path", required=True)
    history_build.add_argument("--output-path", required=True)
    history_build.add_argument("--exchange", action="append")
    history_build.add_argument("--workers", type=int, default=4)
    history_build.add_argument("--log-path")
    history_build.add_argument("--clean", action="store_true")
    history_build.add_argument("--incremental", action="store_true")
    history_build.add_argument("--only-missing", action="store_true")
    history_build.add_argument("--repair-partition", nargs=3, metavar=("EXCHANGE", "YEAR", "MONTH"))
    history_build.add_argument("--from-manifest")
    history_build.add_argument("--progress", action="store_true", default=True)
    history_build.add_argument("--no-progress", dest="progress", action="store_false")

    history_manifest_mark = subparsers.add_parser("history-manifest-mark", help="Mark one raw fetch manifest row after operator review.")
    history_manifest_mark.add_argument("--manifest-path", required=True)
    history_manifest_mark.add_argument("--date", required=True)
    history_manifest_mark.add_argument("--status", required=True)
    history_manifest_mark.add_argument("--reason", required=True)
    history_manifest_mark.add_argument("--exchange")

    history_manifest_combine = subparsers.add_parser("history-manifest-combine", help="Combine per-exchange raw fetch manifests.")
    history_manifest_combine.add_argument("--output", required=True)
    history_manifest_combine.add_argument("paths", nargs="+")

    history_partition_refresh = subparsers.add_parser(
        "history-partition-manifest-refresh",
        help="Regenerate partition_manifest.parquet from existing local partition files.",
    )
    history_partition_refresh.add_argument("--history-path", required=True)
    history_partition_refresh.add_argument("--workers", type=int, default=4)
    history_partition_refresh.add_argument("--progress", action="store_true", default=True)
    history_partition_refresh.add_argument("--no-progress", dest="progress", action="store_false")

    history_verify = subparsers.add_parser("history-verify", help="Verify canonical full-history market-data parquet.")
    history_verify.add_argument("--path", required=True)
    history_verify.add_argument("--report-path", required=True)
    history_verify.add_argument("--partition-wise", action="store_true")
    history_verify.add_argument("--streaming", action="store_true")
    history_verify.add_argument("--max-memory-gb", type=float)
    history_verify.add_argument("--progress", action="store_true", default=True)
    history_verify.add_argument("--no-progress", dest="progress", action="store_false")

    history_doctor = subparsers.add_parser("history-doctor", help="Audit local raw/parquet/R2 history health.")
    history_doctor.add_argument("--exchange", required=True)
    history_doctor.add_argument("--raw-manifest-path")
    history_doctor.add_argument("--history-path", default="data/import/daily_stock_data_full")
    history_doctor.add_argument("--output-dir")
    history_doctor.add_argument("--start-date")
    history_doctor.add_argument("--end-date")
    history_doctor.add_argument("--compare-r2", action="store_true")

    history_bootstrap = subparsers.add_parser("history-bootstrap", help="Run fetch/build/verify/doctor/upload bootstrap.")
    history_bootstrap.add_argument("--exchange", required=True)
    history_bootstrap.add_argument("--start-date", required=True)
    history_bootstrap.add_argument("--end-date", required=True)
    history_bootstrap.add_argument("--raw-output-path", required=True)
    history_bootstrap.add_argument("--history-path", required=True)
    history_bootstrap.add_argument("--audit-path", required=True)
    history_bootstrap.add_argument("--raw-manifest-path")
    history_bootstrap.add_argument("--partition-manifest-path")
    history_bootstrap.add_argument("--resume", action="store_true")
    history_bootstrap.add_argument("--upload", choices=["true", "false"], default="false")
    history_bootstrap.add_argument("--workers", type=int, default=1)
    history_bootstrap.add_argument("--retries", type=int, default=1)
    history_bootstrap.add_argument("--request-sleep-seconds", type=float, default=0.5)
    history_bootstrap.add_argument("--retry-sleep-seconds", type=float, default=1.0)
    history_bootstrap.add_argument("--max-memory-gb", type=float)
    history_bootstrap.add_argument("--allow-fetch-status")

    history_upload = subparsers.add_parser("history-upload", help="Upload verified full-history market data to R2.")
    history_upload.add_argument("--path", required=True)
    history_upload.add_argument("--audit-path", required=True)
    history_upload.add_argument("--raw-manifest-path", required=True)
    history_upload.add_argument("--partition-manifest-path", required=True)
    history_upload.add_argument("--exchange", action="append")
    history_upload.add_argument("--workers", type=int, default=4)
    history_upload.add_argument("--progress", action="store_true", default=True)
    history_upload.add_argument("--no-progress", dest="progress", action="store_false")

    performance_compute = subparsers.add_parser("performance-compute", help="Compute realized performance from decisions and market data.")
    performance_compute.add_argument("--strategy-id", required=True)
    performance_compute.add_argument("--decision-kind", choices=["backtest", "paper"], required=True)
    performance_compute.add_argument("--decisions-path")
    performance_compute.add_argument("--market-data-path")
    performance_compute.add_argument("--use-r2", action="store_true")
    performance_compute.add_argument("--exchange")
    performance_compute.add_argument("--output-dir")
    performance_compute.add_argument("--upload-results", action="store_true")

    performance_refresh = subparsers.add_parser("performance-refresh", help="Compute daily performance for all active R2-backed strategies.")
    performance_refresh.add_argument("--decision-kind", choices=["backtest", "paper"], default="paper")
    performance_refresh.add_argument("--exchange", required=True)
    performance_refresh.add_argument("--upload-results", action="store_true")

    r2_sync_check = subparsers.add_parser("r2-sync-check", help="Compare local partition manifest to R2 market data.")
    r2_sync_check.add_argument("--exchange", required=True)
    r2_sync_check.add_argument("--partition-manifest-path", default="data/import/manifests/partition_manifest.parquet")
    r2_sync_check.add_argument("--workers", type=int, default=8)
    r2_sync_check.add_argument("--progress", action="store_true", default=True)
    r2_sync_check.add_argument("--no-progress", dest="progress", action="store_false")

    r2_usage = subparsers.add_parser("r2-usage", help="Report R2 object inventory usage.")
    r2_usage.add_argument("--prefix", default="")
    r2_usage.add_argument("--snapshot-dir")

    r2_budget = subparsers.add_parser("r2-budget-check", help="Check R2 usage against budget thresholds.")
    r2_budget.add_argument("--prefix", default="")
    r2_budget.add_argument("--snapshot-dir")

    r2_cleanup_staging = subparsers.add_parser("r2-cleanup-staging", help="List or delete old staging objects under one prefix.")
    r2_cleanup_staging.add_argument("--prefix", required=True)
    r2_cleanup_staging.add_argument("--older-than-days", type=int, required=True)
    r2_cleanup_staging.add_argument("--dry-run", action="store_true", default=False)

    format_inspect = subparsers.add_parser("format-inspect", help="Inspect expected bhavcopy format for a date.")
    format_inspect.add_argument("--exchange", required=True)
    format_inspect.add_argument("--date", required=True)
    format_inspect.add_argument("--json", action="store_true")

    return parser


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def paper_dry_run(args: argparse.Namespace) -> int:
    as_of_date = _parse_date(args.date)
    if args.use_r2:
        if not args.exchange:
            raise ValueError("--exchange is required when --use-r2 is set.")
        client = R2Client.from_env()
        results = run_daily_paper_job_from_r2(
            client=client,
            exchange=args.exchange,
            as_of_date=as_of_date,
            upload_results=args.upload_results,
        )
        print(
            f"paper-dry-run date={args.date} source=r2 exchange={args.exchange} "
            f"active_strategies={len(results)} uploaded={str(args.upload_results).lower()}"
        )
        for strategy_id, frame in results.items():
            print(f"{strategy_id} rows={frame.height}")
        return 0

    if not args.market_data_path:
        raise ValueError("--market-data-path is required unless --use-r2 is set.")

    market_data = load_daily_stock_data(args.market_data_path, as_of_date=as_of_date)
    results = run_daily_paper_job(
        base_path=args.base_path,
        market_data=market_data,
        as_of_date=as_of_date,
    )
    print(f"paper-dry-run date={args.date} source=local active_strategies={len(results)}")
    for strategy_id, frame in results.items():
        output_path = Path(args.base_path) / "decisions" / "paper" / strategy_id / "decisions.parquet"
        print(f"{strategy_id} rows={frame.height} output={output_path.as_posix()}")
    return 0


def backtest_run(args: argparse.Namespace) -> int:
    start_date = _parse_date(args.start_date)
    end_date = _parse_date(args.end_date)

    if args.use_r2:
        if not args.exchange:
            raise ValueError("--exchange is required when --use-r2 is set.")
        client = R2Client.from_env()
        market_data = load_daily_stock_data_history_from_r2(
            client,
            exchange=args.exchange,
            end_date=end_date,
        )
        with TemporaryDirectory() as tmpdir:
            download_strategy_artifacts(client, args.strategy_id, tmpdir)
            strategy = build_strategy(load_stored_strategy(tmpdir, args.strategy_id))
            decisions = run_backtest(
                strategy,
                market_data,
                start_date=start_date,
                end_date=end_date,
                show_progress=args.progress,
            )
        source = f"r2 exchange={args.exchange} strategy_source=r2 market_history=full"
    else:
        if not args.market_data_path:
            raise ValueError("--market-data-path is required unless --use-r2 is set.")
        strategy = build_strategy(load_stored_strategy(args.base_path, args.strategy_id))
        exchanges = [args.exchange] if args.exchange else None
        scheduled_dates = load_trading_dates(
            args.market_data_path,
            start_date=start_date,
            end_date=end_date,
            exchanges=exchanges,
        )
        if not scheduled_dates:
            raise ValueError(
                f"No trading dates found for start_date={start_date.isoformat()} end_date={end_date.isoformat()}."
            )
        lookback_days = int(getattr(strategy, "lookback_days", 0) or 0)
        warmup_days = args.warmup_days if args.warmup_days is not None else max(lookback_days, 120)

        def _load_chunk(chunk_start: date | None, chunk_end: date) -> pl.DataFrame:
            return load_daily_stock_data(
                args.market_data_path,
                start_date=chunk_start,
                as_of_date=chunk_end,
                exchanges=exchanges,
            )

        decisions = run_backtest_chunked(
            strategy,
            dates=scheduled_dates,
            load_market_data=_load_chunk,
            warmup_days=warmup_days,
            chunk_size=args.chunk_size_days,
            show_progress=args.progress,
        )
        source = (
            f"local market_data_path={args.market_data_path} strategy_source=local "
            f"chunk_size_days={args.chunk_size_days} warmup_days={warmup_days}"
        )

    output_path = Path(args.output_path) if args.output_path else (
        Path(args.base_path) / "decisions" / "backtest" / args.strategy_id / "decisions.parquet"
    )
    write_decisions_parquet(output_path, decisions)
    print(
        f"backtest-run strategy_id={args.strategy_id} rows={decisions.height} "
        f"start_date={args.start_date} end_date={args.end_date} source={source} output={output_path.as_posix()}"
    )
    return 0


def strategy_upload(args: argparse.Namespace) -> int:
    client = R2Client.from_env()
    upload_strategy_artifacts(client, args.strategy_id, args.base_path)
    print(f"strategy-upload strategy_id={args.strategy_id} source={Path(args.base_path).as_posix()}")
    return 0


def registry_upload(args: argparse.Namespace) -> int:
    registry_path = Path(args.path)
    if not registry_path.exists():
        raise FileNotFoundError(f"Registry parquet not found: {registry_path}")
    registry = load_strategy_registry(registry_path)
    client = R2Client.from_env()
    upload_strategy_registry(client, registry_path)
    print(f"registry-upload rows={registry.height} path={registry_path.as_posix()}")
    return 0


def backtest_upload(args: argparse.Namespace) -> int:
    decisions_path = Path(args.path)
    if not decisions_path.exists():
        raise FileNotFoundError(f"Backtest decisions file not found: {decisions_path}")
    decisions = read_decisions_parquet(decisions_path)
    client = R2Client.from_env()
    upload_backtest_decisions(client, args.strategy_id, decisions_path)
    if decisions.height:
        min_date = decisions.get_column("date").min()
        max_date = decisions.get_column("date").max()
    else:
        min_date = None
        max_date = None
    print(
        f"backtest-upload strategy_id={args.strategy_id} rows={decisions.height} "
        f"date_min={min_date} date_max={max_date} path={decisions_path.as_posix()}"
    )
    return 0


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


def performance_compute(args: argparse.Namespace) -> int:
    if args.use_r2:
        if not args.exchange:
            raise ValueError("--exchange is required with --use-r2.")
        client = R2Client.from_env()
        decisions = (
            download_backtest_decisions(client, args.strategy_id)
            if args.decision_kind == "backtest"
            else download_paper_decisions(client, args.strategy_id)
        )
        market_data = load_daily_stock_data_history_from_r2(client, exchange=args.exchange, end_date=decisions.get_column("date").max() if not decisions.is_empty() else date.today())
    else:
        if not args.decisions_path:
            raise ValueError("--decisions-path is required unless --use-r2 is set.")
        if not args.market_data_path:
            raise ValueError("--market-data-path is required unless --use-r2 is set.")
        decisions = read_decisions_parquet(args.decisions_path)
        market_data = load_daily_stock_data(args.market_data_path)
    result = compute_strategy_performance(
        decisions=decisions,
        market_data=market_data,
        strategy_id=args.strategy_id,
        decision_kind=args.decision_kind,
        primary_exchange=args.exchange,
    )
    output_dir = Path(args.output_dir) if args.output_dir else Path("performance") / args.decision_kind / args.strategy_id
    daily_path, summary_path = write_performance_result(
        result,
        daily_path=output_dir / "daily.parquet",
        summary_path=output_dir / "summary.json",
    )
    if args.upload_results:
        if not args.use_r2:
            client = R2Client.from_env()
        upload_performance_result(client, result)
    print(
        f"performance-compute strategy_id={args.strategy_id} decision_kind={args.decision_kind} "
        f"realized_dates={result.summary['realized_dates']} final_multiple={result.summary['final_multiple']} "
        f"daily={daily_path.as_posix()} summary={summary_path.as_posix()} uploaded={str(args.upload_results).lower()}"
    )
    return 0


def performance_refresh(args: argparse.Namespace) -> int:
    client = R2Client.from_env()
    registry = load_strategy_registry_from_r2(client)
    strategy_ids = active_strategy_ids(registry)
    computed = 0
    for strategy_id in strategy_ids:
        decisions = (
            download_backtest_decisions(client, strategy_id)
            if args.decision_kind == "backtest"
            else download_paper_decisions(client, strategy_id)
        )
        if decisions.is_empty():
            continue
        market_data = load_daily_stock_data_history_from_r2(
            client,
            exchange=args.exchange,
            end_date=decisions.get_column("date").max(),
        )
        result = compute_strategy_performance(
            decisions=decisions,
            market_data=market_data,
            strategy_id=strategy_id,
            decision_kind=args.decision_kind,
            primary_exchange=args.exchange,
        )
        if args.upload_results:
            upload_performance_result(client, result)
        computed += 1
        print(
            f"performance-refresh strategy_id={strategy_id} decision_kind={args.decision_kind} "
            f"realized_dates={result.summary['realized_dates']} final_multiple={result.summary['final_multiple']}"
        )
    _emit_summary(
        "performance-refresh",
        status="ok",
        decision_kind=args.decision_kind,
        exchange=args.exchange,
        strategies=computed,
        uploaded=args.upload_results,
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


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "paper-dry-run":
        return paper_dry_run(args)
    if args.command == "backtest-run":
        return backtest_run(args)
    if args.command == "strategy-upload":
        return strategy_upload(args)
    if args.command == "registry-upload":
        return registry_upload(args)
    if args.command == "backtest-upload":
        return backtest_upload(args)
    if args.command == "market-data-upload":
        return market_data_upload(args)
    if args.command == "market-data-refresh":
        return market_data_refresh(args)
    if args.command == "bhavcopy-fetch":
        return bhavcopy_fetch(args)
    if args.command == "bhavcopy-ingest":
        return bhavcopy_ingest(args)
    if args.command == "history-fetch":
        return history_fetch(args)
    if args.command == "history-build":
        return history_build(args)
    if args.command == "history-manifest-mark":
        return history_manifest_mark(args)
    if args.command == "history-manifest-combine":
        return history_manifest_combine(args)
    if args.command == "history-partition-manifest-refresh":
        return history_partition_manifest_refresh(args)
    if args.command == "history-verify":
        return history_verify(args)
    if args.command == "history-doctor":
        return history_doctor(args)
    if args.command == "history-bootstrap":
        return history_bootstrap(args)
    if args.command == "history-upload":
        return history_upload(args)
    if args.command == "performance-compute":
        return performance_compute(args)
    if args.command == "performance-refresh":
        return performance_refresh(args)
    if args.command == "r2-sync-check":
        return r2_sync_check(args)
    if args.command == "r2-usage":
        return r2_usage(args)
    if args.command == "r2-budget-check":
        return r2_budget_check(args)
    if args.command == "r2-cleanup-staging":
        return r2_cleanup_staging(args)
    if args.command == "format-inspect":
        return format_inspect(args)
    parser.error(f"Unsupported command: {args.command}")
    return 2
