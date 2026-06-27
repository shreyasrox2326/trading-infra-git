import json
from datetime import date

import polars as pl
import pytest

from trading_infra.data.market_data import load_daily_stock_data
from trading_infra.pipelines.paper import append_paper_decisions, run_daily_paper_job
from trading_infra.storage.decisions import read_decisions_parquet, write_decisions_parquet


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
                "date": date(2026, 1, 1),
                "exchange": "NSE",
                "isin": "INE000000002",
                "symbol": "BBB",
                "series": "EQ",
                "open": 95.0,
                "high": 96.0,
                "low": 94.0,
                "close": 95.0,
                "prev_close": 94.0,
                "vwap": 95.0,
                "volume": 1000,
                "turnover": 95000.0,
                "trades": 100,
                "deliverable_qty": 500,
                "delivery_pct": 50.0,
                "adj_open": 95.0,
                "adj_high": 96.0,
                "adj_low": 94.0,
                "adj_close": 95.0,
                "adj_factor": 1.0,
            },
            {
                "date": date(2026, 1, 2),
                "exchange": "NSE",
                "isin": "INE000000001",
                "symbol": "AAA",
                "series": "EQ",
                "open": 101.0,
                "high": 102.0,
                "low": 100.0,
                "close": 101.0,
                "prev_close": 100.0,
                "vwap": 101.0,
                "volume": 1000,
                "turnover": 101000.0,
                "trades": 100,
                "deliverable_qty": 500,
                "delivery_pct": 50.0,
                "adj_open": 101.0,
                "adj_high": 102.0,
                "adj_low": 100.0,
                "adj_close": 101.0,
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


def _write_local_strategy(base_path, strategy_id: str, *, strategy_type: str = "top_n_adj_close", top_n: int = 1) -> None:
    strategy_root = base_path / "strategies" / strategy_id
    strategy_root.mkdir(parents=True)
    (strategy_root / "config.yaml").write_text(
        f"strategy_type: {strategy_type}\nstrategy_id: {strategy_id}\ntop_n: {top_n}\n",
        encoding="utf-8",
    )
    (strategy_root / "metadata.json").write_text(
        json.dumps({"strategy_type": strategy_type, "version": "v1"}),
        encoding="utf-8",
    )


def _write_registry(base_path, rows: list[dict]) -> None:
    registry_root = base_path / "registry"
    registry_root.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(registry_root / "strategies.parquet")


def test_append_paper_decisions_is_idempotent() -> None:
    existing = pl.DataFrame(
        [
            {
                "date": date(2026, 1, 2),
                "strategy_id": "momentum_v1",
                "exchange": "NSE",
                "isin": "INE000000002",
                "symbol": "BBB",
                "target_weight": 1.0,
                "rank": 1,
                "score": 103.0,
            }
        ]
    )

    appended = append_paper_decisions(existing, existing)

    assert appended.height == 1


def test_run_daily_paper_job_writes_first_paper_file(tmp_path) -> None:
    _write_registry(tmp_path, [{"strategy_id": "momentum_v1", "version": "v1", "status": "active"}])
    _write_local_strategy(tmp_path, "momentum_v1")
    market_path = tmp_path / "market.parquet"
    _market_data_frame().write_parquet(market_path)
    market_data = load_daily_stock_data(str(market_path), as_of_date=date(2026, 1, 2))

    results = run_daily_paper_job(base_path=tmp_path, market_data=market_data, as_of_date=date(2026, 1, 2))

    assert results["momentum_v1"].get_column("symbol").to_list() == ["BBB"]
    stored = read_decisions_parquet(tmp_path / "decisions" / "paper" / "momentum_v1" / "decisions.parquet")
    assert stored.get_column("symbol").to_list() == ["BBB"]


def test_run_daily_paper_job_appends_new_date(tmp_path) -> None:
    _write_registry(tmp_path, [{"strategy_id": "momentum_v1", "version": "v1", "status": "active"}])
    _write_local_strategy(tmp_path, "momentum_v1")
    market_path = tmp_path / "market.parquet"
    _market_data_frame().write_parquet(market_path)
    market_data = load_daily_stock_data(str(market_path))

    first = pl.DataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "strategy_id": "momentum_v1",
                "exchange": "NSE",
                "isin": "INE000000001",
                "symbol": "AAA",
                "target_weight": 1.0,
                "rank": 1,
                "score": 100.0,
            }
        ]
    )
    write_decisions_parquet(tmp_path / "decisions" / "paper" / "momentum_v1" / "decisions.parquet", first)

    results = run_daily_paper_job(base_path=tmp_path, market_data=market_data, as_of_date=date(2026, 1, 2))

    assert results["momentum_v1"].get_column("date").to_list() == [date(2026, 1, 1), date(2026, 1, 2)]


def test_run_daily_paper_job_rerun_is_idempotent(tmp_path) -> None:
    _write_registry(tmp_path, [{"strategy_id": "momentum_v1", "version": "v1", "status": "active"}])
    _write_local_strategy(tmp_path, "momentum_v1")
    market_path = tmp_path / "market.parquet"
    _market_data_frame().write_parquet(market_path)
    market_data = load_daily_stock_data(str(market_path), as_of_date=date(2026, 1, 2))

    run_daily_paper_job(base_path=tmp_path, market_data=market_data, as_of_date=date(2026, 1, 2))
    second = run_daily_paper_job(base_path=tmp_path, market_data=market_data, as_of_date=date(2026, 1, 2))

    assert second["momentum_v1"].height == 1


def test_run_daily_paper_job_empty_strategy_output_keeps_history(tmp_path) -> None:
    _write_registry(tmp_path, [{"strategy_id": "momentum_v1", "version": "v1", "status": "active"}])
    _write_local_strategy(tmp_path, "momentum_v1", top_n=0)
    market_path = tmp_path / "market.parquet"
    _market_data_frame().write_parquet(market_path)
    market_data = load_daily_stock_data(str(market_path), as_of_date=date(2026, 1, 2))

    existing = pl.DataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "strategy_id": "momentum_v1",
                "exchange": "NSE",
                "isin": "INE000000001",
                "symbol": "AAA",
                "target_weight": 1.0,
                "rank": 1,
                "score": 100.0,
            }
        ]
    )
    write_decisions_parquet(tmp_path / "decisions" / "paper" / "momentum_v1" / "decisions.parquet", existing)

    results = run_daily_paper_job(base_path=tmp_path, market_data=market_data, as_of_date=date(2026, 1, 2))

    assert results["momentum_v1"].get_column("date").to_list() == [date(2026, 1, 1)]


def test_run_daily_paper_job_skips_inactive_strategies(tmp_path) -> None:
    _write_registry(
        tmp_path,
        [
            {"strategy_id": "momentum_v1", "version": "v1", "status": "active"},
            {"strategy_id": "draft_v1", "version": "v1", "status": "draft"},
        ],
    )
    _write_local_strategy(tmp_path, "momentum_v1")
    _write_local_strategy(tmp_path, "draft_v1")
    market_path = tmp_path / "market.parquet"
    _market_data_frame().write_parquet(market_path)
    market_data = load_daily_stock_data(str(market_path), as_of_date=date(2026, 1, 2))

    results = run_daily_paper_job(base_path=tmp_path, market_data=market_data, as_of_date=date(2026, 1, 2))

    assert list(results) == ["momentum_v1"]


def test_run_daily_paper_job_fails_for_missing_registry(tmp_path) -> None:
    market_path = tmp_path / "market.parquet"
    _market_data_frame().write_parquet(market_path)
    market_data = load_daily_stock_data(str(market_path), as_of_date=date(2026, 1, 2))

    with pytest.raises(FileNotFoundError):
        run_daily_paper_job(base_path=tmp_path, market_data=market_data, as_of_date=date(2026, 1, 2))


def test_run_daily_paper_job_fails_for_missing_strategy_artifacts(tmp_path) -> None:
    _write_registry(tmp_path, [{"strategy_id": "momentum_v1", "version": "v1", "status": "active"}])
    market_path = tmp_path / "market.parquet"
    _market_data_frame().write_parquet(market_path)
    market_data = load_daily_stock_data(str(market_path), as_of_date=date(2026, 1, 2))

    with pytest.raises(FileNotFoundError, match="Missing strategy config"):
        run_daily_paper_job(base_path=tmp_path, market_data=market_data, as_of_date=date(2026, 1, 2))


def test_run_daily_paper_job_fails_for_unsupported_strategy_type(tmp_path) -> None:
    _write_registry(tmp_path, [{"strategy_id": "momentum_v1", "version": "v1", "status": "active"}])
    _write_local_strategy(tmp_path, "momentum_v1", strategy_type="unsupported")
    market_path = tmp_path / "market.parquet"
    _market_data_frame().write_parquet(market_path)
    market_data = load_daily_stock_data(str(market_path), as_of_date=date(2026, 1, 2))

    with pytest.raises(ValueError, match="unsupported strategy_type"):
        run_daily_paper_job(base_path=tmp_path, market_data=market_data, as_of_date=date(2026, 1, 2))
