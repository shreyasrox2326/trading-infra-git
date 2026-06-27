import json
from datetime import date

import polars as pl
import pytest

from trading_infra.cli import main


def _market_data_frame() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "exchange": "NSE",
                "isin": "INE000000001",
                "symbol": "AAA",
                "series": "EQ",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "prev_close": 99.0,
                "vwap": 100.0,
                "volume": 1000,
                "turnover": 100000.0,
                "trades": 100,
                "deliverable_qty": 500,
                "delivery_pct": 50.0,
                "adj_open": 100.0,
                "adj_high": 101.0,
                "adj_low": 99.0,
                "adj_close": 100.0,
                "adj_factor": 1.0,
            },
            {
                "date": date(2026, 1, 2),
                "exchange": "NSE",
                "isin": "INE000000002",
                "symbol": "BBB",
                "series": "EQ",
                "open": 103.0,
                "high": 104.0,
                "low": 102.0,
                "close": 103.0,
                "prev_close": 95.0,
                "vwap": 103.0,
                "volume": 1000,
                "turnover": 103000.0,
                "trades": 100,
                "deliverable_qty": 500,
                "delivery_pct": 50.0,
                "adj_open": 103.0,
                "adj_high": 104.0,
                "adj_low": 102.0,
                "adj_close": 103.0,
                "adj_factor": 1.0,
            },
        ]
    )


def _write_strategy_files(base_path) -> None:
    registry_root = base_path / "registry"
    registry_root.mkdir(parents=True)
    pl.DataFrame([{"strategy_id": "momentum_v1", "version": "v1", "status": "active"}]).write_parquet(
        registry_root / "strategies.parquet"
    )
    strategy_root = base_path / "strategies" / "momentum_v1"
    strategy_root.mkdir(parents=True)
    (strategy_root / "config.yaml").write_text(
        "strategy_type: top_n_adj_close\nstrategy_id: momentum_v1\ntop_n: 1\n",
        encoding="utf-8",
    )
    (strategy_root / "metadata.json").write_text(
        json.dumps({"strategy_type": "top_n_adj_close", "version": "v1"}),
        encoding="utf-8",
    )


def test_paper_dry_run_local(capsys, tmp_path) -> None:
    _write_strategy_files(tmp_path)
    market_path = tmp_path / "market.parquet"
    _market_data_frame().write_parquet(market_path)

    exit_code = main(
        [
            "paper-dry-run",
            "--base-path",
            str(tmp_path),
            "--date",
            "2026-01-02",
            "--market-data-path",
            str(market_path),
        ]
    )

    captured = capsys.readouterr().out
    assert exit_code == 0
    assert "paper-dry-run date=2026-01-02 source=local strategies=1" in captured
    assert "momentum_v1 rows=1" in captured


def test_backtest_run_writes_output(capsys, tmp_path) -> None:
    _write_strategy_files(tmp_path)
    market_path = tmp_path / "market.parquet"
    _market_data_frame().write_parquet(market_path)

    exit_code = main(
        [
            "backtest-run",
            "--base-path",
            str(tmp_path),
            "--strategy-id",
            "momentum_v1",
            "--market-data-path",
            str(market_path),
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-02",
        ]
    )

    captured = capsys.readouterr().out
    assert exit_code == 0
    assert "backtest-run strategy_id=momentum_v1" in captured


def test_paper_dry_run_requires_market_data_without_r2(tmp_path) -> None:
    _write_strategy_files(tmp_path)

    with pytest.raises(ValueError, match="market-data-path"):
        main(
            [
                "paper-dry-run",
                "--base-path",
                str(tmp_path),
                "--date",
                "2026-01-02",
            ]
        )


def test_paper_dry_run_requires_exchange_for_r2() -> None:
    with pytest.raises(ValueError, match="exchange"):
        main(["paper-dry-run", "--date", "2026-01-02", "--use-r2"])
