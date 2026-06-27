from datetime import date
from pathlib import Path

import polars as pl

from trading_infra.storage.config import R2Config
from trading_infra.storage.market_data import (
    list_market_data_partitions,
    scan_market_data_inputs,
    upload_market_data_partitions,
)
from trading_infra.storage.r2 import R2Client


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

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)

    def delete_objects(self, Bucket, Delete):
        for item in Delete.get("Objects", []):
            self.objects.pop(item["Key"], None)

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


def _write_input(path: Path) -> None:
    pl.DataFrame(
        [
            {
                "date": date(2026, 1, 31),
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
                "date": date(2026, 2, 1),
                "exchange": "NSE",
                "isin": "INE000000002",
                "symbol": "BBB",
                "series": "EQ",
                "open": 110.0,
                "high": 111.0,
                "low": 109.0,
                "close": 110.0,
                "prev_close": 108.0,
                "vwap": 110.0,
                "volume": 1000,
                "turnover": 110000.0,
                "trades": 100,
                "deliverable_qty": 500,
                "delivery_pct": 50.0,
                "adj_open": 110.0,
                "adj_high": 111.0,
                "adj_low": 109.0,
                "adj_close": 110.0,
                "adj_factor": 1.0,
            },
        ]
    ).write_parquet(path)


def test_scan_market_data_inputs_validates_required_columns(tmp_path) -> None:
    path = tmp_path / "bad.parquet"
    pl.DataFrame([{"date": date(2026, 1, 1), "exchange": "NSE"}]).write_parquet(path)

    try:
        scan_market_data_inputs([path]).collect()
    except ValueError as exc:
        assert "missing required column" in str(exc).lower()
    else:
        raise AssertionError("Expected missing-column validation failure.")


def test_list_market_data_partitions(tmp_path) -> None:
    path = tmp_path / "market.parquet"
    _write_input(path)

    partitions = list_market_data_partitions([path])

    assert [(p.exchange, p.year, p.month, p.rows) for p in partitions] == [
        ("NSE", 2026, 1, 1),
        ("NSE", 2026, 2, 1),
    ]


def test_upload_market_data_partitions_rewrites_partition_prefix(monkeypatch, tmp_path) -> None:
    client = _fake_client(monkeypatch)
    input_path = tmp_path / "market.parquet"
    _write_input(input_path)
    client.upload_bytes("data/daily_stock_data/exchange=NSE/year=2026/month=01/old-part.parquet", b"stale")

    uploaded = upload_market_data_partitions(client, paths=[input_path])

    assert [(p.exchange, p.year, p.month) for p in uploaded] == [
        ("NSE", 2026, 1),
        ("NSE", 2026, 2),
    ]
    assert "data/daily_stock_data/exchange=NSE/year=2026/month=01/old-part.parquet" not in client._client.objects  # type: ignore[attr-defined]
    assert "data/daily_stock_data/exchange=NSE/year=2026/month=01/part.parquet" in client._client.objects  # type: ignore[attr-defined]
    assert "data/daily_stock_data/exchange=NSE/year=2026/month=02/part.parquet" in client._client.objects  # type: ignore[attr-defined]
