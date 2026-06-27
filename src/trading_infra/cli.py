"""Command-line entrypoints for local backtests and paper dry-runs."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Sequence

from trading_infra.data.market_data import load_daily_stock_data
from trading_infra.pipelines.backtest import run_backtest
from trading_infra.pipelines.paper import run_daily_paper_job, run_daily_paper_job_from_r2
from trading_infra.registry import load_strategy_registry
from trading_infra.storage.decisions import read_decisions_parquet
from trading_infra.storage.decisions import write_decisions_parquet
from trading_infra.storage.market_data import list_market_data_partitions, upload_market_data_partitions
from trading_infra.storage.remote import (
    download_strategy_artifacts,
    upload_backtest_decisions,
    upload_strategy_artifacts,
    upload_strategy_registry,
)
from trading_infra.storage.remote import load_daily_stock_data_history_from_r2
from trading_infra.storage.r2 import R2Client
from trading_infra.strategy_builder import build_strategy
from trading_infra.strategy_store import load_stored_strategy


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
            )
        source = f"r2 exchange={args.exchange} strategy_source=r2 market_history=full"
    else:
        if not args.market_data_path:
            raise ValueError("--market-data-path is required unless --use-r2 is set.")
        strategy = build_strategy(load_stored_strategy(args.base_path, args.strategy_id))
        market_data = load_daily_stock_data(args.market_data_path, as_of_date=end_date)
        decisions = run_backtest(
            strategy,
            market_data,
            start_date=start_date,
            end_date=end_date,
        )
        source = f"local market_data_path={args.market_data_path} strategy_source=local"

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
    parser.error(f"Unsupported command: {args.command}")
    return 2
