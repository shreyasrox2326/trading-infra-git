from datetime import date
from pathlib import Path
from zipfile import ZipFile

import polars as pl

from trading_infra.data.bhavcopy import BhavcopyFetchResult
from trading_infra.storage.config import R2Config
from trading_infra.storage.refresh import refresh_market_data_for_date
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

    def head_object(self, Bucket, Key):
        return {"ContentLength": len(self.objects[Key]), "ETag": "etag", "LastModified": None}

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


def _market_row(trade_date: date, symbol: str, isin: str, close: float) -> dict:
    return {
        "date": trade_date,
        "exchange": "NSE",
        "isin": isin,
        "symbol": symbol,
        "series": "EQ",
        "open": close,
        "high": close + 1,
        "low": close - 1,
        "close": close,
        "prev_close": close - 1,
        "vwap": close,
        "volume": 1000,
        "turnover": close * 1000,
        "trades": 100,
        "deliverable_qty": None,
        "delivery_pct": None,
        "adj_open": close,
        "adj_high": close + 1,
        "adj_low": close - 1,
        "adj_close": close,
        "adj_factor": 1.0,
    }


def _write_bhavcopy(path: Path, trade_date: date, symbol: str, isin: str, close: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "SYMBOL": symbol,
        "SERIES": "EQ",
        "OPEN": close,
        "HIGH": close + 1,
        "LOW": close - 1,
        "CLOSE": close,
        "PREVCLOSE": close - 1,
        "TOTTRDQTY": 1000,
        "TOTTRDVAL": close * 1000,
        "TIMESTAMP": trade_date.strftime("%d-%b-%Y").upper(),
        "TOTALTRADES": 100,
        "ISIN": isin,
    }
    with ZipFile(path, "w") as archive:
        archive.writestr(path.with_suffix(".csv").name, pl.DataFrame([row]).write_csv())


def test_refresh_market_data_merges_date_without_deleting_rest_of_month(monkeypatch, tmp_path) -> None:
    client = _fake_client(monkeypatch)
    existing_path = tmp_path / "existing.parquet"
    pl.DataFrame([_market_row(date(2026, 1, 1), "AAA", "INE000000001", 100.0)]).write_parquet(existing_path)
    client.upload_file(existing_path, "data/daily_stock_data/exchange=NSE/year=2026/month=01/part.parquet")

    def _fetch(trade_date, *, exchange, output_path, **_kwargs):
        target = Path(output_path) / "cm02JAN2026bhav.csv.zip"
        _write_bhavcopy(target, trade_date, "BBB", "INE000000002", 110.0)
        return BhavcopyFetchResult(trade_date, "downloaded", target)

    monkeypatch.setattr("trading_infra.storage.refresh.fetch_bhavcopy_archive", _fetch)

    result = refresh_market_data_for_date(
        client,
        exchange="NSE",
        refresh_date=date(2026, 1, 2),
        run_id="run-1",
    )

    downloaded = tmp_path / "downloaded.parquet"
    client.download_file("data/daily_stock_data/exchange=NSE/year=2026/month=01/part.parquet", downloaded)
    month = pl.read_parquet(downloaded)
    assert result.status == "refreshed"
    assert month.get_column("date").to_list() == [date(2026, 1, 1), date(2026, 1, 2)]
    assert month.get_column("symbol").to_list() == ["AAA", "BBB"]
    assert "_staging/daily-refresh/run-1/data/daily_stock_data/exchange=NSE/year=2026/month=01/part.parquet" in client._client.objects  # type: ignore[attr-defined]


def test_refresh_market_data_rerun_same_date_is_idempotent(monkeypatch, tmp_path) -> None:
    client = _fake_client(monkeypatch)

    def _fetch(trade_date, *, exchange, output_path, **_kwargs):
        target = Path(output_path) / "cm02JAN2026bhav.csv.zip"
        _write_bhavcopy(target, trade_date, "BBB", "INE000000002", 110.0)
        return BhavcopyFetchResult(trade_date, "downloaded", target)

    monkeypatch.setattr("trading_infra.storage.refresh.fetch_bhavcopy_archive", _fetch)

    refresh_market_data_for_date(client, exchange="NSE", refresh_date=date(2026, 1, 2), run_id="run-1")
    refresh_market_data_for_date(client, exchange="NSE", refresh_date=date(2026, 1, 2), run_id="run-2")

    downloaded = tmp_path / "downloaded.parquet"
    client.download_file("data/daily_stock_data/exchange=NSE/year=2026/month=01/part.parquet", downloaded)
    month = pl.read_parquet(downloaded)
    assert month.height == 1


def test_refresh_market_data_no_file_returns_no_data(monkeypatch) -> None:
    client = _fake_client(monkeypatch)

    def _fetch(trade_date, *, exchange, output_path, **_kwargs):
        return BhavcopyFetchResult(trade_date, "not_available", None, "404")

    monkeypatch.setattr("trading_infra.storage.refresh.fetch_bhavcopy_archive", _fetch)

    result = refresh_market_data_for_date(client, exchange="NSE", refresh_date=date(2026, 1, 3))

    assert result.status == "no_data"
    assert client._client.objects == {}  # type: ignore[attr-defined]
