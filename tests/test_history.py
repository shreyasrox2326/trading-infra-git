from datetime import date
from pathlib import Path
from zipfile import ZipFile

import polars as pl

from trading_infra.cli import main
from trading_infra.data.history import build_history_parquet, verify_history_frame, write_history_audit
from trading_infra.data.market_data import DAILY_STOCK_DATA_COLUMNS


def _write_zip(path: Path, rows: list[dict]) -> None:
    with ZipFile(path, "w") as archive:
        archive.writestr(path.with_suffix(".csv").name, pl.DataFrame(rows).write_csv())


def _nse_row() -> dict:
    return {
        "SYMBOL": "ABC",
        "SERIES": "EQ",
        "OPEN": 100.0,
        "HIGH": 101.0,
        "LOW": 99.0,
        "CLOSE": 100.5,
        "PREVCLOSE": 98.0,
        "TOTTRDQTY": 1000,
        "TOTTRDVAL": 100500.0,
        "TIMESTAMP": "02-JAN-2026",
        "TOTALTRADES": 100,
        "ISIN": "INE000000001",
    }


def _bse_row() -> dict:
    return {
        "DATE": "2026-01-02",
        "SC_CODE": "500001",
        "SC_NAME": "BSE DEMO",
        "SC_GROUP": "A",
        "OPEN": 100.0,
        "HIGH": 101.0,
        "LOW": 99.0,
        "CLOSE": 100.5,
        "PREVCLOSE": 98.0,
        "NO_TRADES": 100,
        "NO_OF_SHRS": 1000,
        "NET_TURNOV": 100500.0,
    }


def test_build_history_parquet_combines_exchange_subdirectories(tmp_path) -> None:
    raw = tmp_path / "raw"
    (raw / "NSE").mkdir(parents=True)
    (raw / "BSE").mkdir(parents=True)
    _write_zip(raw / "NSE" / "cm02JAN2026bhav.csv.zip", [_nse_row()])
    _write_zip(raw / "BSE" / "EQ020126_CSV.ZIP", [_bse_row()])

    output, frame = build_history_parquet(input_path=raw, output_path=tmp_path / "daily_stock_data_full.parquet")

    assert output.exists()
    assert frame.columns == list(DAILY_STOCK_DATA_COLUMNS)
    assert frame.get_column("exchange").to_list() == ["BSE", "NSE"]


def test_verify_history_frame_reports_pass_and_exchange_summary(tmp_path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    _write_zip(raw / "cm02JAN2026bhav.csv.zip", [_nse_row()])
    _, frame = build_history_parquet(
        input_path=raw,
        output_path=tmp_path / "daily_stock_data_full.parquet",
        exchanges=["NSE"],
    )

    audit = verify_history_frame(frame)

    assert audit["passed"] is True
    assert audit["duplicate_key_count"] == 0
    assert audit["by_exchange"][0]["exchange"] == "NSE"
    assert audit["missing_weekdays_by_exchange"] == {"NSE": []}


def test_build_history_parquet_rejects_duplicate_keys(tmp_path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    duplicate = _nse_row()
    duplicate["CLOSE"] = 101.0
    _write_zip(raw / "cm02JAN2026bhav.csv.zip", [_nse_row(), duplicate])

    try:
        build_history_parquet(
            input_path=raw,
            output_path=tmp_path / "daily_stock_data_full.parquet",
            exchanges=["NSE"],
        )
    except ValueError as exc:
        assert "duplicate" in str(exc)
    else:
        raise AssertionError("Expected duplicate-key failure.")


def test_build_history_parquet_preserves_nullable_delivery_and_identity_adjustment(tmp_path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    _write_zip(raw / "cm02JAN2026bhav.csv.zip", [_nse_row()])

    _, frame = build_history_parquet(
        input_path=raw,
        output_path=tmp_path / "daily_stock_data_full.parquet",
        exchanges=["NSE"],
    )

    assert frame.get_column("deliverable_qty").null_count() == 1
    assert frame.get_column("delivery_pct").null_count() == 1
    assert frame.get_column("adj_open").to_list() == frame.get_column("open").to_list()
    assert frame.get_column("adj_close").to_list() == frame.get_column("close").to_list()
    assert frame.get_column("adj_factor").to_list() == [1.0]


def test_verify_history_frame_rejects_invalid_ohlc() -> None:
    frame = pl.DataFrame(
        [
            {
                "date": date(2026, 1, 2),
                "exchange": "NSE",
                "isin": "INE000000001",
                "symbol": "ABC",
                "series": "EQ",
                "open": 100.0,
                "high": 90.0,
                "low": 99.0,
                "close": 100.5,
                "prev_close": 98.0,
                "vwap": 100.5,
                "volume": 1000,
                "turnover": 100500.0,
                "trades": 100,
                "deliverable_qty": None,
                "delivery_pct": None,
                "adj_open": 100.0,
                "adj_high": 90.0,
                "adj_low": 99.0,
                "adj_close": 100.5,
                "adj_factor": 1.0,
            }
        ]
    )

    audit = verify_history_frame(frame)

    assert audit["passed"] is False
    assert audit["invalid_ohlc_count"] == 1


def test_verify_history_frame_rejects_bad_schema() -> None:
    audit = verify_history_frame(pl.DataFrame([{"date": date(2026, 1, 2), "exchange": "NSE"}]))

    assert audit["passed"] is False
    assert "isin" in audit["missing_columns"]


def test_write_history_audit_writes_json_and_markdown(tmp_path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    _write_zip(raw / "cm02JAN2026bhav.csv.zip", [_nse_row()])
    output, _ = build_history_parquet(
        input_path=raw,
        output_path=tmp_path / "daily_stock_data_full.parquet",
        exchanges=["NSE"],
    )
    report = tmp_path / "history_audit.json"

    audit = write_history_audit(path=output, report_path=report)

    assert audit["passed"] is True
    assert report.exists()
    assert report.with_suffix(".md").exists()


def test_history_build_and_verify_cli(tmp_path, capsys) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    _write_zip(raw / "cm02JAN2026bhav.csv.zip", [_nse_row()])
    output = tmp_path / "daily_stock_data_full.parquet"
    report = tmp_path / "history_audit.json"

    build_code = main(
        [
            "history-build",
            "--input-path",
            str(raw),
            "--output-path",
            str(output),
            "--exchange",
            "NSE",
        ]
    )
    verify_code = main(["history-verify", "--path", str(output), "--report-path", str(report)])

    captured = capsys.readouterr().out
    assert build_code == 0
    assert verify_code == 0
    assert "history-build" in captured
    assert "history-verify" in captured
