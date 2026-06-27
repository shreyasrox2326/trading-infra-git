"""Command-line entrypoints for local backtests and paper dry-runs."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Sequence

from trading_infra.data.market_data import load_daily_stock_data
from trading_infra.pipelines.backtest import run_backtest
from trading_infra.pipelines.paper import run_daily_paper_job, run_daily_paper_job_from_r2
from trading_infra.storage.decisions import write_decisions_parquet
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
    backtest.add_argument("--market-data-path", required=True)
    backtest.add_argument("--start-date", required=True)
    backtest.add_argument("--end-date", required=True)
    backtest.add_argument("--output-path")

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
        print(f"paper-dry-run date={args.date} source=r2 strategies={len(results)}")
        for strategy_id, frame in results.items():
            print(f"{strategy_id} rows={frame.height} uploaded={str(args.upload_results).lower()}")
        return 0

    if not args.market_data_path:
        raise ValueError("--market-data-path is required unless --use-r2 is set.")

    market_data = load_daily_stock_data(args.market_data_path, as_of_date=as_of_date)
    results = run_daily_paper_job(
        base_path=args.base_path,
        market_data=market_data,
        as_of_date=as_of_date,
    )
    print(f"paper-dry-run date={args.date} source=local strategies={len(results)}")
    for strategy_id, frame in results.items():
        output_path = Path(args.base_path) / "decisions" / "paper" / strategy_id / "decisions.parquet"
        print(f"{strategy_id} rows={frame.height} output={output_path.as_posix()}")
    return 0


def backtest_run(args: argparse.Namespace) -> int:
    strategy = build_strategy(load_stored_strategy(args.base_path, args.strategy_id))
    market_data = load_daily_stock_data(args.market_data_path, as_of_date=_parse_date(args.end_date))
    decisions = run_backtest(
        strategy,
        market_data,
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
    )

    output_path = Path(args.output_path) if args.output_path else (
        Path(args.base_path) / "decisions" / "backtest" / args.strategy_id / "decisions.parquet"
    )
    write_decisions_parquet(output_path, decisions)
    print(
        f"backtest-run strategy_id={args.strategy_id} rows={decisions.height} output={output_path.as_posix()}"
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "paper-dry-run":
        return paper_dry_run(args)
    if args.command == "backtest-run":
        return backtest_run(args)
    parser.error(f"Unsupported command: {args.command}")
    return 2
