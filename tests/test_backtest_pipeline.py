from datetime import date

import polars as pl

from trading_infra.data.market_data import load_daily_stock_data
from trading_infra.pipelines.backtest import backtest_dates, run_backtest, run_backtest_for_date
from trading_infra.strategies.top_n import TopNByAdjustedCloseStrategy


def test_backtest_pipeline_runs_end_to_end(tmp_path) -> None:
    path = tmp_path / "market.parquet"
    pl.DataFrame(
        [
            {
                "date": date(2026, 1, 2),
                "exchange": "NSE",
                "isin": "INE000000001",
                "symbol": "AAA",
                "series": "EQ",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "prev_close": 98.0,
                "vwap": 100.2,
                "volume": 1000,
                "turnover": 100_200.0,
                "trades": 100,
                "deliverable_qty": 500,
                "delivery_pct": 50.0,
                "adj_open": 100.0,
                "adj_high": 101.0,
                "adj_low": 99.0,
                "adj_close": 100.5,
                "adj_factor": 1.0,
            },
            {
                "date": date(2026, 1, 2),
                "exchange": "NSE",
                "isin": "INE000000002",
                "symbol": "BBB",
                "series": "EQ",
                "open": 110.0,
                "high": 112.0,
                "low": 109.0,
                "close": 111.5,
                "prev_close": 108.0,
                "vwap": 111.0,
                "volume": 900,
                "turnover": 99_900.0,
                "trades": 90,
                "deliverable_qty": 450,
                "delivery_pct": 50.0,
                "adj_open": 110.0,
                "adj_high": 112.0,
                "adj_low": 109.0,
                "adj_close": 111.5,
                "adj_factor": 1.0,
            },
            {
                "date": date(2026, 1, 2),
                "exchange": "NSE",
                "isin": "INE000000003",
                "symbol": "CCC",
                "series": "EQ",
                "open": 95.0,
                "high": 97.0,
                "low": 94.0,
                "close": 96.0,
                "prev_close": 94.0,
                "vwap": 95.8,
                "volume": 1200,
                "turnover": 114_960.0,
                "trades": 110,
                "deliverable_qty": 600,
                "delivery_pct": 50.0,
                "adj_open": 95.0,
                "adj_high": 97.0,
                "adj_low": 94.0,
                "adj_close": 96.0,
                "adj_factor": 1.0,
            },
        ]
    ).write_parquet(path)

    market_data = load_daily_stock_data(str(path), as_of_date=date(2026, 1, 2))
    strategy = TopNByAdjustedCloseStrategy(strategy_id="top_close_v1", top_n=2)

    decisions = run_backtest_for_date(strategy, market_data, date(2026, 1, 2))

    assert decisions.get_column("symbol").to_list() == ["BBB", "AAA"]
    assert decisions.get_column("target_weight").to_list() == [0.5, 0.5]
    assert decisions.get_column("rank").to_list() == [1, 2]


def test_top_n_strategy_ignores_non_eq_series_for_decision_keys(tmp_path) -> None:
    path = tmp_path / "market_series.parquet"
    rows = [
        {
            "date": date(2026, 1, 2),
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
            "turnover": 100_000.0,
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
            "isin": "INE000000001",
            "symbol": "AAA",
            "series": "BL",
            "open": 200.0,
            "high": 201.0,
            "low": 199.0,
            "close": 200.0,
            "prev_close": 199.0,
            "vwap": 200.0,
            "volume": 1000,
            "turnover": 200_000.0,
            "trades": 100,
            "deliverable_qty": 500,
            "delivery_pct": 50.0,
            "adj_open": 200.0,
            "adj_high": 201.0,
            "adj_low": 199.0,
            "adj_close": 200.0,
            "adj_factor": 1.0,
        },
    ]
    pl.DataFrame(rows).write_parquet(path)

    market_data = load_daily_stock_data(str(path), as_of_date=date(2026, 1, 2))
    strategy = TopNByAdjustedCloseStrategy(strategy_id="top_close_v1", top_n=2)

    decisions = run_backtest_for_date(strategy, market_data, date(2026, 1, 2))

    assert decisions.height == 1
    assert decisions.get_column("symbol").to_list() == ["AAA"]
    assert decisions.get_column("score").to_list() == [100.0]


def test_backtest_dates_respects_window(tmp_path) -> None:
    path = tmp_path / "market_dates.parquet"
    pl.DataFrame(
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
                "turnover": 100_000.0,
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
                "isin": "INE000000001",
                "symbol": "AAA",
                "series": "EQ",
                "open": 101.0,
                "high": 102.0,
                "low": 100.0,
                "close": 101.5,
                "prev_close": 100.0,
                "vwap": 101.2,
                "volume": 1000,
                "turnover": 101_200.0,
                "trades": 100,
                "deliverable_qty": 500,
                "delivery_pct": 50.0,
                "adj_open": 101.0,
                "adj_high": 102.0,
                "adj_low": 100.0,
                "adj_close": 101.5,
                "adj_factor": 1.0,
            },
            {
                "date": date(2026, 1, 3),
                "exchange": "NSE",
                "isin": "INE000000001",
                "symbol": "AAA",
                "series": "EQ",
                "open": 102.0,
                "high": 103.0,
                "low": 101.0,
                "close": 102.5,
                "prev_close": 101.5,
                "vwap": 102.2,
                "volume": 1000,
                "turnover": 102_200.0,
                "trades": 100,
                "deliverable_qty": 500,
                "delivery_pct": 50.0,
                "adj_open": 102.0,
                "adj_high": 103.0,
                "adj_low": 101.0,
                "adj_close": 102.5,
                "adj_factor": 1.0,
            },
        ]
    ).write_parquet(path)

    market_data = load_daily_stock_data(str(path))

    scheduled = backtest_dates(
        market_data,
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 3),
    )

    assert scheduled == [date(2026, 1, 2), date(2026, 1, 3)]


def test_run_backtest_replays_across_dates(tmp_path) -> None:
    path = tmp_path / "market_replay.parquet"
    pl.DataFrame(
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
                "turnover": 100_000.0,
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
                "vwap": 95.1,
                "volume": 1000,
                "turnover": 95_100.0,
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
                "turnover": 101_000.0,
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
                "turnover": 103_000.0,
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
    ).write_parquet(path)

    market_data = load_daily_stock_data(str(path))
    strategy = TopNByAdjustedCloseStrategy(strategy_id="top_close_v1", top_n=1)

    decisions = run_backtest(
        strategy,
        market_data,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 2),
    )

    assert decisions.get_column("date").to_list() == [date(2026, 1, 1), date(2026, 1, 2)]
    assert decisions.get_column("symbol").to_list() == ["AAA", "BBB"]
    assert decisions.get_column("target_weight").to_list() == [1.0, 1.0]
