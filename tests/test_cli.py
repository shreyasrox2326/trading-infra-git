import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from trading_infra.cli import main
from trading_infra.decisions import decisions_frame
from trading_infra.data.bhavcopy import bhavcopy_archive_name
from trading_infra.storage.config import R2Config
from trading_infra.storage.r2 import R2Client


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


def _upload_strategy_files(fake_client: "_FakeS3Client", base_path, strategy_id: str = "momentum_v1") -> None:
    strategy_root = base_path / "strategy-upload" / strategy_id
    strategy_root.mkdir(parents=True)
    (strategy_root / "config.yaml").write_text(
        f"strategy_type: top_n_adj_close\nstrategy_id: {strategy_id}\ntop_n: 1\n",
        encoding="utf-8",
    )
    (strategy_root / "metadata.json").write_text(
        json.dumps({"strategy_type": "top_n_adj_close", "version": "v1"}),
        encoding="utf-8",
    )
    fake_client.objects[f"strategies/{strategy_id}/config.yaml"] = (strategy_root / "config.yaml").read_bytes()
    fake_client.objects[f"strategies/{strategy_id}/metadata.json"] = (strategy_root / "metadata.json").read_bytes()


class _FakePaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **kwargs):
        prefix = kwargs.get("Prefix", "")
        return [
            {"Contents": [item for item in page.get("Contents", []) if item["Key"].startswith(prefix)]}
            for page in self._pages
        ]


class _FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body, **_kwargs):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        from io import BytesIO

        return {"Body": BytesIO(self.objects[Key])}

    def upload_file(self, Filename, Bucket, Key):
        self.objects[Key] = Path(Filename).read_bytes()

    def download_file(self, Bucket, Key, Filename):
        Path(Filename).parent.mkdir(parents=True, exist_ok=True)
        Path(Filename).write_bytes(self.objects[Key])

    def get_paginator(self, _name):
        contents = [{"Key": key} for key in sorted(self.objects)]
        return _FakePaginator([{"Contents": contents}])


def _patch_r2(monkeypatch) -> _FakeS3Client:
    fake_client = _FakeS3Client()

    class _FakeSession:
        def client(self, *_args, **_kwargs):
            return fake_client

    monkeypatch.setattr("boto3.session.Session", lambda: _FakeSession())
    monkeypatch.setattr(
        R2Client,
        "from_env",
        classmethod(
            lambda cls: cls(
                R2Config(
                    access_key_id="key",
                    secret_access_key="secret",
                    endpoint_url="https://example.r2.cloudflarestorage.com",
                    bucket="bucket-name",
                )
            )
        ),
    )
    return fake_client


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
    assert "paper-dry-run date=2026-01-02 source=local active_strategies=1" in captured
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
    assert "source=local market_data_path=" in captured


def test_backtest_run_supports_r2(capsys, monkeypatch, tmp_path) -> None:
    fake_client = _patch_r2(monkeypatch)
    _upload_strategy_files(fake_client, tmp_path)
    market_path = tmp_path / "market.parquet"
    _market_data_frame().write_parquet(market_path)
    fake_client.objects["data/daily_stock_data/exchange=NSE/year=2026/month=01/part-1.parquet"] = (
        market_path.read_bytes()
    )

    exit_code = main(
        [
            "backtest-run",
            "--base-path",
            str(tmp_path),
            "--strategy-id",
            "momentum_v1",
            "--use-r2",
            "--exchange",
            "NSE",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-02",
        ]
    )

    captured = capsys.readouterr().out
    assert exit_code == 0
    assert "source=r2 exchange=NSE strategy_source=r2 market_history=full" in captured


def test_backtest_run_r2_uses_full_history(monkeypatch, capsys, tmp_path) -> None:
    fake_client = _patch_r2(monkeypatch)
    _upload_strategy_files(fake_client, tmp_path)
    market_path = tmp_path / "market.parquet"
    _market_data_frame().write_parquet(market_path)
    fake_client.objects["data/daily_stock_data/exchange=NSE/year=2026/month=01/part-1.parquet"] = (
        market_path.read_bytes()
    )

    class _HistoryAwareStrategy:
        strategy_id = "momentum_v1"

        def run(self, context):
            dates = context.market_data.get_column("date").unique().sort().to_list()
            if len(dates) < 2:
                return decisions_frame([])
            day = context.market_data.filter(pl.col("date") == context.as_of_date).head(1)
            row = day.iter_rows(named=True).__next__()
            return decisions_frame(
                [
                    {
                        "date": context.as_of_date,
                        "strategy_id": self.strategy_id,
                        "exchange": row["exchange"],
                        "isin": row["isin"],
                        "symbol": row["symbol"],
                        "target_weight": 1.0,
                        "rank": 1,
                        "score": 1.0,
                    }
                ]
            )

    monkeypatch.setattr("trading_infra.cli.build_strategy", lambda _stored: _HistoryAwareStrategy())

    exit_code = main(
        [
            "backtest-run",
            "--base-path",
            str(tmp_path),
            "--strategy-id",
            "momentum_v1",
            "--use-r2",
            "--exchange",
            "NSE",
            "--start-date",
            "2026-01-02",
            "--end-date",
            "2026-01-02",
        ]
    )

    captured = capsys.readouterr().out
    output = pl.read_parquet(tmp_path / "decisions" / "backtest" / "momentum_v1" / "decisions.parquet")
    assert exit_code == 0
    assert "rows=1" in captured
    assert output.get_column("date").to_list() == [date(2026, 1, 2)]


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


def test_strategy_upload(capsys, monkeypatch, tmp_path) -> None:
    fake_client = _patch_r2(monkeypatch)
    _write_strategy_files(tmp_path)

    exit_code = main(["strategy-upload", "--base-path", str(tmp_path), "--strategy-id", "momentum_v1"])

    captured = capsys.readouterr().out
    assert exit_code == 0
    assert "strategy-upload strategy_id=momentum_v1" in captured
    assert "strategies/momentum_v1/config.yaml" in fake_client.objects


def test_registry_upload(capsys, monkeypatch, tmp_path) -> None:
    fake_client = _patch_r2(monkeypatch)
    registry_path = tmp_path / "registry.parquet"
    pl.DataFrame([{"strategy_id": "momentum_v1", "version": "v1", "status": "active"}]).write_parquet(registry_path)

    exit_code = main(["registry-upload", "--path", str(registry_path)])

    captured = capsys.readouterr().out
    assert exit_code == 0
    assert "registry-upload rows=1 path=" in captured
    assert "registry/strategies.parquet" in fake_client.objects


def test_backtest_upload(capsys, monkeypatch, tmp_path) -> None:
    fake_client = _patch_r2(monkeypatch)
    decisions_path = tmp_path / "decisions.parquet"
    pl.DataFrame(
        [
            {
                "date": date(2026, 1, 2),
                "strategy_id": "momentum_v1",
                "exchange": "NSE",
                "isin": "INE000000001",
                "symbol": "AAA",
                "target_weight": 1.0,
                "rank": 1,
                "score": 100.0,
            }
        ]
    ).write_parquet(decisions_path)

    exit_code = main(["backtest-upload", "--strategy-id", "momentum_v1", "--path", str(decisions_path)])

    captured = capsys.readouterr().out
    assert exit_code == 0
    assert "backtest-upload strategy_id=momentum_v1 rows=1" in captured
    assert "decisions/backtest/momentum_v1/decisions.parquet" in fake_client.objects


def test_backtest_upload_rejects_invalid_decisions(monkeypatch, tmp_path) -> None:
    _patch_r2(monkeypatch)
    decisions_path = tmp_path / "decisions.parquet"
    pl.DataFrame(
        [
            {
                "date": date(2026, 1, 2),
                "strategy_id": "momentum_v1",
                "exchange": "NSE",
                "isin": "INE000000001",
                "symbol": "AAA",
                "target_weight": -1.0,
                "rank": 1,
                "score": 100.0,
            }
        ]
    ).write_parquet(decisions_path)

    with pytest.raises(ValueError, match="negative target weights"):
        main(["backtest-upload", "--strategy-id", "momentum_v1", "--path", str(decisions_path)])


def test_market_data_upload(capsys, monkeypatch, tmp_path) -> None:
    fake_client = _patch_r2(monkeypatch)
    market_path = tmp_path / "market.parquet"
    _market_data_frame().write_parquet(market_path)

    exit_code = main(["market-data-upload", "--path", str(market_path)])

    captured = capsys.readouterr().out
    assert exit_code == 0
    assert "market-data-upload paths=1 partitions=1" in captured
    assert "data/daily_stock_data/exchange=NSE/year=2026/month=01/part.parquet" in fake_client.objects


def test_bhavcopy_fetch_cli(capsys, monkeypatch, tmp_path) -> None:
    class _Result:
        def __init__(self):
            self.status = "downloaded"
            self.requested_date = date(2026, 1, 2)
            self.path = tmp_path / "cm02JAN2026bhav.csv.zip"
            self.message = ""

    monkeypatch.setattr("trading_infra.cli.fetch_bhavcopy_archives", lambda **_kwargs: [_Result()])

    exit_code = main(
        [
            "bhavcopy-fetch",
            "--exchange",
            "NSE",
            "--start-date",
            "2026-01-02",
            "--end-date",
            "2026-01-02",
            "--output-path",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr().out
    assert exit_code == 0
    assert "bhavcopy-fetch exchange=NSE" in captured
    assert "downloaded" in captured
    assert (tmp_path / "bhavcopy-fetch.log").exists()


def test_bhavcopy_ingest_cli(capsys, tmp_path) -> None:
    from zipfile import ZipFile

    source = tmp_path / bhavcopy_archive_name(date(2026, 1, 2))
    csv = (
        "SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,"
        "TIMESTAMP,TOTALTRADES,ISIN\n"
        "ABC,EQ,100,101,99,100.5,100.5,98,1000,100500,02-JAN-2026,100,INE000000001\n"
    )
    with ZipFile(source, "w") as archive:
        archive.writestr("cm02JAN2026bhav.csv", csv)
    output = tmp_path / "daily_stock_data.parquet"

    exit_code = main(
        [
            "bhavcopy-ingest",
            "--input-path",
            str(tmp_path),
            "--output-path",
            str(output),
            "--exchange",
            "NSE",
        ]
    )

    captured = capsys.readouterr().out
    assert exit_code == 0
    assert "bhavcopy-ingest input_path=" in captured
    assert pl.read_parquet(output).get_column("symbol").to_list() == ["ABC"]
