from datetime import date
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import polars as pl
import pytest

from trading_infra.data.bhavcopy import (
    bhavcopy_archive_name,
    bhavcopy_archive_url,
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


def _udiff_row(symbol: str = "ABC", isin: str = "INE000000001") -> dict:
    return {
        "TradDt": "2024-07-09",
        "BizDt": "2024-07-09",
        "Sgmt": "CM",
        "Src": "NSE",
        "FinInstrmTp": "STK",
        "FinInstrmId": 1,
        "ISIN": isin,
        "TckrSymb": symbol,
        "SctySrs": "EQ",
        "FinInstrmNm": "ABC LIMITED",
        "OpnPric": 100.0,
        "HghPric": 101.0,
        "LwPric": 99.0,
        "ClsPric": 100.5,
        "LastPric": 100.5,
        "PrvsClsgPric": 98.0,
        "SttlmPric": 100.5,
        "TtlTradgVol": 1000,
        "TtlTrfVal": 100500.0,
        "TtlNbOfTxsExctd": 100,
        "SsnId": "F1",
        "NewBrdLotQty": 1,
    }


def _bse_legacy_row() -> dict:
    return {
        "SC_CODE": "500001",
        "SC_NAME": "BSE DEMO",
        "SC_GROUP": "A",
        "SC_TYPE": "Q",
        "OPEN": 100.0,
        "HIGH": 101.0,
        "LOW": 99.0,
        "CLOSE": 100.5,
        "LAST": 100.5,
        "PREVCLOSE": 98.0,
        "NO_TRADES": 100,
        "NO_OF_SHRS": 1000,
        "NET_TURNOV": 100500.0,
        "TDCLOINDI": "",
    }


def _bse_udiff_row() -> dict:
    row = _udiff_row(symbol="BSEABC", isin="INE000000002")
    row["Src"] = "BSE"
    return row


def test_trading_weekdays_skips_weekends() -> None:
    days = trading_weekdays(date(2026, 1, 2), date(2026, 1, 5))

    assert days == [date(2026, 1, 2), date(2026, 1, 5)]


def test_bhavcopy_archive_name_switches_to_udiff_from_2024_07_08() -> None:
    assert bhavcopy_archive_name(date(2024, 7, 5)) == "cm05JUL2024bhav.csv.zip"
    assert bhavcopy_archive_name(date(2024, 7, 8)) == "BhavCopy_NSE_CM_0_0_0_20240708_F_0000.csv.zip"
    assert bhavcopy_archive_url(date(2024, 7, 8)).endswith(
        "/content/cm/BhavCopy_NSE_CM_0_0_0_20240708_F_0000.csv.zip"
    )


def test_bse_bhavcopy_archive_names_support_legacy_and_udiff() -> None:
    assert bhavcopy_archive_name(date(2024, 7, 29), exchange="BSE") == "EQ290724_CSV.ZIP"
    assert bhavcopy_archive_name(date(2024, 7, 30), exchange="BSE") == "BhavCopy_BSE_CM_0_0_0_20240730_F_0000.CSV"
    assert bhavcopy_archive_url(date(2024, 7, 30), exchange="BSE").endswith(
        "/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_20240730_F_0000.CSV"
    )


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


def test_normalize_udiff_bhavcopy_inputs_to_canonical_schema(tmp_path) -> None:
    source = tmp_path / "BhavCopy_NSE_CM_0_0_0_20240709_F_0000.csv.zip"
    _write_bhavcopy_zip(source, [_udiff_row()])

    frame = normalize_bhavcopy_inputs(tmp_path, exchange="NSE")

    assert frame.columns == list(DAILY_STOCK_DATA_COLUMNS)
    assert frame.get_column("date").to_list() == [date(2024, 7, 9)]
    assert frame.get_column("symbol").to_list() == ["ABC"]
    assert frame.get_column("series").to_list() == ["EQ"]
    assert frame.get_column("vwap").to_list() == [100.5]
    assert frame.get_column("trades").to_list() == [100]


def test_normalize_bse_legacy_bhavcopy_inputs_to_canonical_schema(tmp_path) -> None:
    source = tmp_path / "EQ020126_CSV.ZIP"
    row = _bse_legacy_row()
    row["DATE"] = "2026-01-02"
    _write_bhavcopy_zip(source, [row])

    frame = normalize_bhavcopy_inputs(tmp_path, exchange="BSE")

    assert frame.columns == list(DAILY_STOCK_DATA_COLUMNS)
    assert frame.get_column("date").to_list() == [date(2026, 1, 2)]
    assert frame.get_column("exchange").to_list() == ["BSE"]
    assert frame.get_column("isin").to_list() == ["500001"]
    assert frame.get_column("symbol").to_list() == ["500001"]
    assert frame.get_column("trades").to_list() == [100]
    assert frame.get_column("adj_factor").to_list() == [1.0]


def test_normalize_bse_udiff_bhavcopy_inputs_to_canonical_schema(tmp_path) -> None:
    source = tmp_path / "BhavCopy_BSE_CM_0_0_0_20240730_F_0000.CSV"
    source.write_text(_bhavcopy_csv([_bse_udiff_row()]), encoding="utf-8")

    frame = normalize_bhavcopy_inputs(tmp_path, exchange="BSE")

    assert frame.columns == list(DAILY_STOCK_DATA_COLUMNS)
    assert frame.get_column("date").to_list() == [date(2024, 7, 9)]
    assert frame.get_column("exchange").to_list() == ["BSE"]
    assert frame.get_column("symbol").to_list() == ["BSEABC"]


def test_normalize_bhavcopy_inputs_handles_two_digit_years(tmp_path) -> None:
    source = tmp_path / "cm13JUL2020bhav.csv.zip"
    row = _row()
    row["TIMESTAMP"] = "13-JUL-20"
    _write_bhavcopy_zip(source, [row])

    frame = normalize_bhavcopy_inputs(tmp_path, exchange="NSE")

    assert frame.get_column("date").to_list() == [date(2020, 7, 13)]


def test_normalize_bhavcopy_inputs_deduplicates_identical_rows(tmp_path) -> None:
    source = tmp_path / "cm02JAN2026bhav.csv.zip"
    _write_bhavcopy_zip(source, [_row(), _row()])

    frame = normalize_bhavcopy_inputs(tmp_path, exchange="NSE")

    assert frame.height == 1


def test_normalize_bhavcopy_inputs_rejects_conflicting_duplicate_keys(tmp_path) -> None:
    source = tmp_path / "cm02JAN2026bhav.csv.zip"
    duplicate = _row()
    duplicate["CLOSE"] = 101.0
    _write_bhavcopy_zip(source, [_row(), duplicate])

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
