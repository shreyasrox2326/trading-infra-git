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

from trading_infra.commands.market_data import (
    bhavcopy_fetch,
    bhavcopy_ingest,
    format_inspect,
    history_bootstrap,
    history_build,
    history_doctor,
    history_fetch,
    history_manifest_combine,
    history_manifest_mark,
    history_partition_manifest_refresh,
    history_upload,
    history_verify,
    market_data_refresh,
    market_data_upload,
    r2_budget_check,
    r2_cleanup_staging,
    r2_sync_check,
    r2_usage,
)
from trading_infra.data.market_data import load_daily_stock_data, load_trading_dates
from trading_infra.performance import compute_strategy_performance, upload_performance_result, write_performance_result
from trading_infra.pipelines.backtest import run_backtest, run_backtest_chunked
from trading_infra.pipelines.paper import run_daily_paper_job, run_daily_paper_job_from_r2
from trading_infra.registry import active_strategy_ids, load_strategy_registry
from trading_infra.storage.decisions import read_decisions_parquet
from trading_infra.storage.decisions import write_decisions_parquet
from trading_infra.storage.market_data_remote import load_daily_stock_data_history_from_r2
from trading_infra.storage.remote import (
    download_backtest_decisions,
    download_paper_decisions,
    download_strategy_artifacts,
    load_strategy_registry_from_r2,
    upload_backtest_decisions,
    upload_strategy_artifacts,
    upload_strategy_registry,
)
from trading_infra.storage.r2 import R2Client
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
