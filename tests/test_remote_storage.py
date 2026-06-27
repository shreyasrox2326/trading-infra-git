from datetime import date
from pathlib import Path

import polars as pl

from trading_infra.decisions import example_decision_row
from trading_infra.storage.config import R2Config
from trading_infra.storage.decisions import write_decisions_parquet
from trading_infra.storage.paths import paper_decisions_key, registry_strategies_key
from trading_infra.storage.r2 import R2Client
from trading_infra.storage.remote import (
    download_paper_decisions,
    download_strategy_artifacts,
    list_daily_stock_data_keys,
    load_daily_stock_data_from_r2,
    load_strategy_registry_from_r2,
    upload_backtest_decisions,
)


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
        self.uploaded_files: list[tuple[str, str, str]] = []

    def put_object(self, Bucket, Key, Body, **_kwargs):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        from io import BytesIO

        return {"Body": BytesIO(self.objects[Key])}

    def upload_file(self, Filename, Bucket, Key):
        self.uploaded_files.append((Filename, Bucket, Key))
        self.objects[Key] = Path(Filename).read_bytes()

    def download_file(self, Bucket, Key, Filename):
        Path(Filename).parent.mkdir(parents=True, exist_ok=True)
        Path(Filename).write_bytes(self.objects[Key])

    def get_paginator(self, _name):
        contents = [{"Key": key} for key in sorted(self.objects)]
        return _FakePaginator([{"Contents": contents}])


def _fake_client(monkeypatch) -> R2Client:
    fake_client = _FakeS3Client()

    class _FakeSession:
        def client(self, *_args, **_kwargs):
            return fake_client

    monkeypatch.setattr("boto3.session.Session", lambda: _FakeSession())
    return R2Client(
        R2Config(
            access_key_id="key",
            secret_access_key="secret",
            endpoint_url="https://example.r2.cloudflarestorage.com",
            bucket="bucket-name",
        )
    )


def test_list_daily_stock_data_keys(monkeypatch) -> None:
    client = _fake_client(monkeypatch)
    client.upload_bytes("data/daily_stock_data/exchange=NSE/year=2024/month=01/part-1.parquet", b"a")
    client.upload_bytes("data/daily_stock_data/exchange=NSE/year=2024/month=01/part-2.parquet", b"b")
    client.upload_bytes("data/daily_stock_data/exchange=NSE/year=2024/month=01/readme.txt", b"skip")

    keys = list_daily_stock_data_keys(client, "NSE", 2024, 1)

    assert keys == [
        "data/daily_stock_data/exchange=NSE/year=2024/month=01/part-1.parquet",
        "data/daily_stock_data/exchange=NSE/year=2024/month=01/part-2.parquet",
    ]


def test_load_daily_stock_data_from_r2(monkeypatch, tmp_path) -> None:
    client = _fake_client(monkeypatch)
    partition = tmp_path / "part.parquet"
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
            }
        ]
    ).write_parquet(partition)
    client.upload_file(partition, "data/daily_stock_data/exchange=NSE/year=2026/month=01/part-1.parquet")

    loaded = load_daily_stock_data_from_r2(client, exchange="NSE", year=2026, month=1, as_of_date=date(2026, 1, 2))

    assert loaded.get_column("symbol").to_list() == ["AAA"]


def test_download_strategy_artifacts(monkeypatch, tmp_path) -> None:
    client = _fake_client(monkeypatch)
    client.upload_text("strategies/momentum_v1/config.yaml", "top_n: 5")
    client.upload_text("strategies/momentum_v1/metadata.json", '{"strategy_type": "top_n_adj_close"}')

    root = download_strategy_artifacts(client, "momentum_v1", tmp_path)

    assert (root / "config.yaml").read_text(encoding="utf-8") == "top_n: 5"
    assert (root / "metadata.json").read_text(encoding="utf-8") == '{"strategy_type": "top_n_adj_close"}'


def test_load_strategy_registry_from_r2(monkeypatch, tmp_path) -> None:
    client = _fake_client(monkeypatch)
    registry_path = tmp_path / "strategies.parquet"
    pl.DataFrame([{"strategy_id": "momentum_v1", "version": "v1", "status": "active"}]).write_parquet(registry_path)
    client.upload_file(registry_path, registry_strategies_key())

    loaded = load_strategy_registry_from_r2(client)

    assert loaded.get_column("strategy_id").to_list() == ["momentum_v1"]


def test_download_paper_decisions_returns_empty_when_missing(monkeypatch) -> None:
    client = _fake_client(monkeypatch)

    loaded = download_paper_decisions(client, "momentum_v1")

    assert loaded.is_empty()


def test_download_paper_decisions_and_upload_backtest(monkeypatch, tmp_path) -> None:
    client = _fake_client(monkeypatch)
    decisions_path = tmp_path / "decisions.parquet"
    write_decisions_parquet(decisions_path, pl.DataFrame([example_decision_row()]))
    client.upload_file(decisions_path, paper_decisions_key("momentum_v1"))

    loaded = download_paper_decisions(client, "momentum_v1")
    assert loaded.get_column("strategy_id").to_list() == ["momentum_v1"]

    backtest_path = tmp_path / "backtest.parquet"
    write_decisions_parquet(backtest_path, pl.DataFrame([example_decision_row()]))
    upload_backtest_decisions(client, "momentum_v1", backtest_path)

    assert any(key.endswith("decisions.parquet") for key in client._client.objects)  # type: ignore[attr-defined]
