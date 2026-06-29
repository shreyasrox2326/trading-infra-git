from datetime import date
from pathlib import Path
from zipfile import ZipFile

import polars as pl

from trading_infra.cli import main
from trading_infra.data.bhavcopy import BhavcopyFetchResult
from trading_infra.data.fetch_manifest import write_raw_fetch_manifest
from trading_infra.data.history import (
    build_history_parquet,
    build_history_partitions,
    verify_history_frame,
    verify_history_partitions,
    write_history_audit,
)
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
    assert output == tmp_path / "daily_stock_data_full"
    assert (output / "exchange=NSE" / "year=2026" / "month=01" / "part.parquet").exists()
    assert (output / "exchange=BSE" / "year=2026" / "month=01" / "part.parquet").exists()
    manifest = pl.read_parquet(tmp_path / "manifests" / "partition_manifest.parquet")
    assert set(manifest.get_column("exchange").to_list()) == {"NSE", "BSE"}
    assert set(manifest.columns) >= {
        "exchange",
        "year",
        "month",
        "partition_path",
        "row_count",
        "min_date",
        "max_date",
        "symbols",
        "file_size_bytes",
        "sha256",
        "source_raw_count",
        "source_raw_files",
        "source_sha256s",
        "format_ids",
        "parser_versions",
        "created_at",
        "verified_at",
        "status",
    }
    assert frame.columns == list(DAILY_STOCK_DATA_COLUMNS)
    assert frame.get_column("exchange").to_list() == ["BSE", "NSE"]
    assert manifest.get_column("source_raw_files").str.contains("cm02JAN2026bhav").any()
    assert manifest.get_column("source_sha256s").str.contains(r"[0-9a-f]{64}").any()


def test_build_history_parquet_skips_html_non_bhavcopy_files(tmp_path) -> None:
    raw = tmp_path / "raw"
    (raw / "BSE").mkdir(parents=True)
    (raw / "BSE" / "BhavCopy_BSE_CM_0_0_0_20240815_F_0000.CSV").write_text(
        "<!DOCTYPE html><html><body>holiday</body></html>",
        encoding="utf-8",
    )
    _write_zip(raw / "BSE" / "EQ020126_CSV.ZIP", [_bse_row()])

    _, frame = build_history_parquet(
        input_path=raw,
        output_path=tmp_path / "daily_stock_data_full.parquet",
        exchanges=["BSE"],
        workers=2,
        show_progress=False,
    )

    assert frame.height == 1
    assert frame.get_column("symbol").to_list() == ["500001"]


def test_build_history_parquet_skips_html_payload_saved_as_zip(tmp_path) -> None:
    raw = tmp_path / "raw"
    (raw / "BSE").mkdir(parents=True)
    (raw / "BSE" / "EQ010107_CSV.ZIP").write_text(
        "<!DOCTYPE html><html><body>not zip</body></html>",
        encoding="utf-8",
    )
    _write_zip(raw / "BSE" / "EQ020126_CSV.ZIP", [_bse_row()])

    _, frame = build_history_parquet(
        input_path=raw,
        output_path=tmp_path / "daily_stock_data_full.parquet",
        exchanges=["BSE"],
        workers=2,
        show_progress=False,
    )

    assert frame.height == 1
    assert frame.get_column("symbol").to_list() == ["500001"]


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


def test_history_build_refuses_existing_output_without_update_mode(tmp_path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    _write_zip(raw / "cm02JAN2026bhav.csv.zip", [_nse_row()])
    output = tmp_path / "daily_stock_data_full"
    output.mkdir()

    try:
        build_history_partitions(
            input_path=raw,
            output_path=output,
            exchanges=["NSE"],
            clean=False,
        )
    except FileExistsError as exc:
        assert "--clean" in str(exc)
    else:
        raise AssertionError("Expected existing-output failure.")


def test_history_build_repair_partition_updates_only_selected_partition(tmp_path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    jan = _nse_row()
    feb = _nse_row()
    feb["TIMESTAMP"] = "02-FEB-2026"
    _write_zip(raw / "cm02JAN2026bhav.csv.zip", [jan])
    _write_zip(raw / "cm02FEB2026bhav.csv.zip", [feb])
    output = tmp_path / "daily_stock_data_full"
    build_history_partitions(input_path=raw, output_path=output, exchanges=["NSE"], clean=True)
    jan_part = output / "exchange=NSE" / "year=2026" / "month=01" / "part.parquet"
    jan_before = jan_part.read_bytes()

    feb["CLOSE"] = 101.5
    _write_zip(raw / "cm02FEB2026bhav.csv.zip", [feb])
    result = build_history_partitions(
        input_path=raw,
        output_path=output,
        exchanges=["NSE"],
        clean=False,
        repair_partition=("NSE", 2026, 2),
    )

    feb_frame = pl.read_parquet(output / "exchange=NSE" / "year=2026" / "month=02" / "part.parquet")
    assert result.partitions == 1
    assert jan_part.read_bytes() == jan_before
    assert feb_frame.get_column("close").to_list() == [101.5]


def test_history_build_from_manifest_uses_listed_raw_files(tmp_path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    included = raw / "cm02JAN2026bhav.csv.zip"
    excluded = raw / "cm02FEB2026bhav.csv.zip"
    jan = _nse_row()
    feb = _nse_row()
    feb["TIMESTAMP"] = "02-FEB-2026"
    _write_zip(included, [jan])
    _write_zip(excluded, [feb])
    raw_manifest = tmp_path / "raw_fetch_NSE.parquet"
    write_raw_fetch_manifest(
        [
            BhavcopyFetchResult(date(2026, 1, 2), "downloaded", included),
            BhavcopyFetchResult(date(2026, 2, 2), "failed", excluded),
        ],
        exchange="NSE",
        path=raw_manifest,
    )

    result = build_history_partitions(
        input_path=raw,
        output_path=tmp_path / "daily_stock_data_full",
        exchanges=["NSE"],
        source_manifest_path=raw_manifest,
    )

    manifest = pl.read_parquet(result.manifest_path)
    assert result.partitions == 1
    assert (result.output_path / "exchange=NSE" / "year=2026" / "month=01" / "part.parquet").exists()
    assert not (result.output_path / "exchange=NSE" / "year=2026" / "month=02" / "part.parquet").exists()
    assert "cm02JAN2026bhav" in manifest.get_column("source_raw_files").item()


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
    assert audit["verification_mode"] == "partition-wise"
    assert audit["partitions"] == 1
    assert audit["partition_summaries"][0]["sha256"]
    assert audit["partition_summaries"][0]["schema_columns"] == list(DAILY_STOCK_DATA_COLUMNS)
    assert audit["partition_summaries"][0]["date_min"] == date(2026, 1, 2)
    assert audit["partition_summaries"][0]["symbols"] == 1
    assert report.exists()
    assert report.with_suffix(".md").exists()


def test_verify_history_partitions_aggregates_partition_summaries(tmp_path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    row_jan = _nse_row()
    row_feb = _nse_row()
    row_feb["TIMESTAMP"] = "02-FEB-2026"
    _write_zip(raw / "cm02JAN2026bhav.csv.zip", [row_jan])
    _write_zip(raw / "cm02FEB2026bhav.csv.zip", [row_feb])
    output, _ = build_history_parquet(
        input_path=raw,
        output_path=tmp_path / "daily_stock_data_full.parquet",
        exchanges=["NSE"],
    )
    files = sorted(output.glob("exchange=*/year=*/month=*/part.parquet"))

    audit = verify_history_partitions(files)

    assert audit["passed"] is True
    assert audit["rows"] == 2
    assert audit["partitions"] == 2
    assert [row["month"] for row in audit["by_month"]] == [1, 2]
    assert all(row["sha256"] for row in audit["partition_summaries"])


def test_write_history_audit_enforces_memory_cap(tmp_path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    _write_zip(raw / "cm02JAN2026bhav.csv.zip", [_nse_row()])
    output, _ = build_history_parquet(
        input_path=raw,
        output_path=tmp_path / "daily_stock_data_full.parquet",
        exchanges=["NSE"],
    )

    try:
        write_history_audit(
            path=output,
            report_path=tmp_path / "history_audit.json",
            max_memory_gb=0.000000001,
        )
    except MemoryError as exc:
        assert "estimated peak memory" in str(exc)
    else:
        raise AssertionError("Expected memory cap failure.")


def test_history_build_and_verify_cli(tmp_path, capsys) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    _write_zip(raw / "cm02JAN2026bhav.csv.zip", [_nse_row()])
    output = tmp_path / "daily_stock_data_full.parquet"
    report = tmp_path / "history_audit.json"
    log_path = tmp_path / "history-build.log"

    build_code = main(
        [
            "history-build",
            "--input-path",
            str(raw),
            "--output-path",
            str(output),
            "--exchange",
            "NSE",
            "--workers",
            "2",
            "--log-path",
            str(log_path),
            "--clean",
            "--no-progress",
        ]
    )
    verify_code = main(
        [
            "history-verify",
            "--path",
            str(output),
            "--report-path",
            str(report),
            "--partition-wise",
            "--max-memory-gb",
            "4",
        ]
    )

    captured = capsys.readouterr().out
    assert build_code == 0
    assert verify_code == 0
    assert "history-build" in captured
    assert "workers=2" in captured
    assert "log=" in captured
    assert "manifest=" in captured
    assert "history-verify" in captured
    assert "verification_mode=partition-wise" in captured
    assert log_path.exists()
    assert "phase=build_start" in log_path.read_text(encoding="utf-8")


def test_history_fetch_cli_writes_log_and_uses_workers(monkeypatch, tmp_path, capsys) -> None:
    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b"payload"

    monkeypatch.setattr("trading_infra.data.bhavcopy.urlopen", lambda *_args, **_kwargs: _Response())
    log_path = tmp_path / "fetch.log"
    manifest_path = tmp_path / "raw_fetch_NSE.parquet"

    exit_code = main(
        [
            "history-fetch",
            "--exchange",
            "NSE",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-02",
            "--output-path",
            str(tmp_path / "raw"),
            "--workers",
            "2",
            "--retries",
            "0",
            "--log-path",
            str(log_path),
            "--manifest-path",
            str(manifest_path),
            "--no-progress",
        ]
    )

    captured = capsys.readouterr().out
    assert exit_code == 0
    assert "workers=2" in captured
    assert "retries=0" in captured
    assert f"manifest={manifest_path.as_posix()}" in captured
    assert log_path.exists()
    assert "2026-01-01,downloaded" in log_path.read_text(encoding="utf-8")
    manifest = pl.read_parquet(manifest_path)
    assert manifest.select("exchange", "date", "expected_format_id", "status").rows() == [
        ("NSE", date(2026, 1, 1), "nse_udiff_cm_bhavcopy_v1", "downloaded"),
        ("NSE", date(2026, 1, 2), "nse_udiff_cm_bhavcopy_v1", "downloaded"),
    ]
    assert manifest.get_column("bytes").to_list() == [7, 7]
    assert manifest.get_column("sha256").null_count() == 0


def test_history_fetch_cli_only_repairs_selected_manifest_statuses(monkeypatch, tmp_path, capsys) -> None:
    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b"payload"

    manifest_path = tmp_path / "raw_fetch_NSE.parquet"
    skipped_path = tmp_path / "raw" / "BhavCopy_NSE_CM_0_0_0_20260101_F_0000.csv.zip"
    skipped_path.parent.mkdir()
    skipped_path.write_bytes(b"old")
    write_raw_fetch_manifest(
        [
            BhavcopyFetchResult(date(2026, 1, 1), "downloaded", skipped_path),
            BhavcopyFetchResult(date(2026, 1, 2), "rate_limited", None, "HTTP Error 403"),
        ],
        exchange="NSE",
        path=manifest_path,
    )
    requested_urls = []

    def fake_urlopen(request, **_kwargs):
        requested_urls.append(request.full_url)
        return _Response()

    monkeypatch.setattr("trading_infra.data.bhavcopy.urlopen", fake_urlopen)

    exit_code = main(
        [
            "history-fetch",
            "--exchange",
            "NSE",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-02",
            "--output-path",
            str(tmp_path / "raw"),
            "--manifest-path",
            str(manifest_path),
            "--only",
            "rate_limited,failed",
            "--retries",
            "0",
            "--no-progress",
        ]
    )

    captured = capsys.readouterr().out
    manifest = pl.read_parquet(manifest_path).sort("date")
    assert exit_code == 0
    assert "only=rate_limited,failed" in captured
    assert len(requested_urls) == 1
    assert manifest.get_column("status").to_list() == ["downloaded", "downloaded"]
    assert manifest.get_column("bytes").to_list() == [3, 7]


def test_history_fetch_cli_fails_fast_on_rate_limit_ratio(monkeypatch, tmp_path, capsys) -> None:
    from urllib.error import HTTPError

    def fake_urlopen(request, **_kwargs):
        raise HTTPError(request.full_url, 403, "Forbidden", hdrs=None, fp=None)

    monkeypatch.setattr("trading_infra.data.bhavcopy.urlopen", fake_urlopen)
    manifest_path = tmp_path / "raw_fetch_NSE.parquet"

    exit_code = main(
        [
            "history-fetch",
            "--exchange",
            "NSE",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-02",
            "--output-path",
            str(tmp_path / "raw"),
            "--manifest-path",
            str(manifest_path),
            "--fail-fast-rate-limit-ratio",
            "0.2",
            "--retries",
            "0",
            "--no-progress",
        ]
    )

    captured = capsys.readouterr().out
    manifest = pl.read_parquet(manifest_path)
    assert exit_code == 1
    assert "status=fail" in captured
    assert "rate_limited ratio" in captured
    assert manifest.height == 1
    assert manifest.get_column("status").to_list() == ["rate_limited"]
