from datetime import date
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import polars as pl
import pytest

from trading_infra.data.bhavcopy import (
    bhavcopy_archive_name,
    fetch_bhavcopy_archive,
    normalize_bhavcopy_inputs,
    trading_weekdays,
    write_canonical_bhavcopy_parquet,
)
from trading_infra.data.market_data import DAILY_STOCK_DATA_COLUMNS, load_daily_stock_data
from trading_infra.storage.market_data import list_market_data_partitions


def _bhavcopy_csv(rows: list[dict]) -> str:
    frame = pl.DataFrame(rows)
    return frame.write_csv()


def _write_bhavcopy_zip(path: Path, rows: list[dict]) -> None:
    with ZipFile(path, "w") as archive:
        archive.writestr(path.with_suffix(".csv").name, _bhavcopy_csv(rows))


def _row(symbol: str = "ABC", isin: str = "INE000000001") -> dict:
    return {
        "SYMBOL": symbol,
        "SERIES": "EQ",
        "OPEN": 100.0,
        "HIGH": 101.0,
        "LOW": 99.0,
        "CLOSE": 100.5,
        "LAST": 100.5,
        "PREVCLOSE": 98.0,
        "TOTTRDQTY": 1000,
        "TOTTRDVAL": 100500.0,
        "TIMESTAMP": "02-JAN-2026",
        "TOTALTRADES": 100,
        "ISIN": isin,
    }


def test_trading_weekdays_skips_weekends() -> None:
    days = trading_weekdays(date(2026, 1, 2), date(2026, 1, 5))

    assert days == [date(2026, 1, 2), date(2026, 1, 5)]


def test_fetch_bhavcopy_archive_without_network(monkeypatch, tmp_path) -> None:
    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b"payload"

    monkeypatch.setattr("trading_infra.data.bhavcopy.urlopen", lambda *_args, **_kwargs: _Response())

    result = fetch_bhavcopy_archive(date(2026, 1, 2), output_path=tmp_path)

    assert result.status == "downloaded"
    assert result.path == tmp_path / bhavcopy_archive_name(date(2026, 1, 2))
    assert result.path.read_bytes() == b"payload"


def test_normalize_bhavcopy_inputs_to_canonical_schema(tmp_path) -> None:
    source = tmp_path / "cm02JAN2026bhav.csv.zip"
    _write_bhavcopy_zip(source, [_row()])

    frame = normalize_bhavcopy_inputs(tmp_path, exchange="NSE")

    assert frame.columns == list(DAILY_STOCK_DATA_COLUMNS)
    assert frame.get_column("date").to_list() == [date(2026, 1, 2)]
    assert frame.get_column("symbol").to_list() == ["ABC"]
    assert frame.get_column("vwap").to_list() == [100.5]
    assert frame.get_column("adj_factor").to_list() == [1.0]
    assert frame.get_column("deliverable_qty").null_count() == 1


def test_normalize_bhavcopy_inputs_rejects_duplicate_keys(tmp_path) -> None:
    source = tmp_path / "cm02JAN2026bhav.csv.zip"
    _write_bhavcopy_zip(source, [_row(), _row()])

    with pytest.raises(ValueError, match="duplicate"):
        normalize_bhavcopy_inputs(tmp_path, exchange="NSE")


def test_bhavcopy_ingest_output_loads_as_market_data(tmp_path) -> None:
    source = tmp_path / "raw" / "cm02JAN2026bhav.csv.zip"
    source.parent.mkdir()
    _write_bhavcopy_zip(source, [_row()])
    output = tmp_path / "daily_stock_data.parquet"

    output_path, summary = write_canonical_bhavcopy_parquet(
        input_path=source.parent,
        output_path=output,
        exchange="NSE",
    )

    loaded = load_daily_stock_data(str(output_path))
    partitions = list_market_data_partitions([output_path])
    assert summary.rows == 1
    assert loaded.get_column("isin").to_list() == ["INE000000001"]
    assert [(partition.exchange, partition.year, partition.month, partition.rows) for partition in partitions] == [
        ("NSE", 2026, 1, 1)
    ]
