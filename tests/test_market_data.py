from datetime import date

import polars as pl
import pytest

from trading_infra.data.market_data import load_daily_stock_data


def _sample_market_data() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "exchange": "NSE",
                "isin": "INE000000001",
                "symbol": "AAA",
                "series": "EQ",
                "open": 100.0,
                "high": 105.0,
                "low": 99.0,
                "close": 104.0,
                "prev_close": 98.0,
                "vwap": 102.5,
                "volume": 1_000,
                "turnover": 102_500.0,
                "trades": 120,
                "deliverable_qty": 600,
                "delivery_pct": 60.0,
                "adj_open": 100.0,
                "adj_high": 105.0,
                "adj_low": 99.0,
                "adj_close": 104.0,
                "adj_factor": 1.0,
            },
            {
                "date": date(2026, 1, 2),
                "exchange": "NSE",
                "isin": "INE000000001",
                "symbol": "AAA",
                "series": "EQ",
                "open": 104.0,
                "high": 108.0,
                "low": 103.0,
                "close": 107.0,
                "prev_close": 104.0,
                "vwap": 106.0,
                "volume": 1_100,
                "turnover": 116_600.0,
                "trades": 140,
                "deliverable_qty": 650,
                "delivery_pct": 59.1,
                "adj_open": 104.0,
                "adj_high": 108.0,
                "adj_low": 103.0,
                "adj_close": 107.0,
                "adj_factor": 1.0,
            },
            {
                "date": date(2026, 1, 2),
                "exchange": "BSE",
                "isin": "INE000000002",
                "symbol": "BBB",
                "series": "EQ",
                "open": 90.0,
                "high": 92.0,
                "low": 88.0,
                "close": 91.0,
                "prev_close": 89.0,
                "vwap": 90.2,
                "volume": 900,
                "turnover": 81_180.0,
                "trades": 80,
                "deliverable_qty": 450,
                "delivery_pct": 50.0,
                "adj_open": 90.0,
                "adj_high": 92.0,
                "adj_low": 88.0,
                "adj_close": 91.0,
                "adj_factor": 1.0,
            },
        ]
    )


def test_load_daily_stock_data_filters_early(tmp_path) -> None:
    path = tmp_path / "sample.parquet"
    _sample_market_data().write_parquet(path)

    loaded = load_daily_stock_data(
        str(path),
        as_of_date=date(2026, 1, 2),
        exchanges=["NSE"],
        symbols=["AAA"],
        columns=["date", "exchange", "symbol", "adj_close"],
    )

    assert loaded.columns == ["date", "exchange", "symbol", "adj_close"]
    assert loaded.height == 2
    assert loaded.get_column("symbol").to_list() == ["AAA", "AAA"]


def test_load_daily_stock_data_rejects_missing_required_columns(tmp_path) -> None:
    path = tmp_path / "broken.parquet"
    pl.DataFrame(
        [{"date": date(2026, 1, 1), "exchange": "NSE", "symbol": "AAA"}]
    ).write_parquet(path)

    with pytest.raises(ValueError, match="missing required column"):
        load_daily_stock_data(str(path))
