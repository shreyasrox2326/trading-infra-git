import json
from datetime import date
from hashlib import sha256
from pathlib import Path

import polars as pl
import pytest

from trading_infra.storage.config import R2Config
from trading_infra.storage.history import upload_verified_history
from trading_infra.storage.r2 import R2Client
from trading_infra.storage.sync import check_r2_sync


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
        self.uploaded_keys: list[str] = []

    def put_object(self, Bucket, Key, Body, **_kwargs):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        from io import BytesIO

        return {"Body": BytesIO(self.objects[Key])}

    def upload_file(self, Filename, Bucket, Key):
        self.uploaded_keys.append(Key)
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


def _history_frame() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "date": date(2026, 1, 2),
                "exchange": "NSE",
                "isin": "INE000000001",
                "symbol": "ABC",
                "series": "EQ",
                "open": 100.0,
                "high": 101.0,
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
                "adj_high": 101.0,
                "adj_low": 99.0,
                "adj_close": 100.5,
                "adj_factor": 1.0,
            }
        ]
    )


def _file_sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _write_partition_manifest(path: Path, partition_path: Path) -> None:
    pl.DataFrame(
        [
            {
                "exchange": "NSE",
                "year": 2026,
                "month": 1,
                "partition_path": partition_path.as_posix(),
                "row_count": 1,
                "min_date": date(2026, 1, 2),
                "max_date": date(2026, 1, 2),
                "symbols": 1,
                "file_size_bytes": partition_path.stat().st_size,
                "sha256": _file_sha256(partition_path),
                "source_raw_count": 1,
                "parser_versions": "bhavcopy",
                "created_at": "2026-06-29T00:00:00",
                "verified_at": None,
                "status": "built",
            }
        ]
    ).write_parquet(path)


def test_upload_verified_history_refuses_failed_audit(monkeypatch, tmp_path) -> None:
    client = _fake_client(monkeypatch)
    history_path = tmp_path / "history.parquet"
    audit_path = tmp_path / "audit.json"
    _history_frame().write_parquet(history_path)
    audit_path.write_text(json.dumps({"passed": False}), encoding="utf-8")

    with pytest.raises(ValueError, match="did not pass"):
        upload_verified_history(client, path=history_path, audit_path=audit_path)

    assert client._client.objects == {}  # type: ignore[attr-defined]


def test_upload_verified_history_stages_then_promotes(monkeypatch, tmp_path) -> None:
    client = _fake_client(monkeypatch)
    history_path = tmp_path / "history.parquet"
    audit_path = tmp_path / "audit.json"
    _history_frame().write_parquet(history_path)
    audit_path.write_text(json.dumps({"passed": True}), encoding="utf-8")
    client.upload_bytes("data/daily_stock_data/exchange=NSE/year=2026/month=01/old.parquet", b"stale")

    results = upload_verified_history(
        client,
        path=history_path,
        audit_path=audit_path,
        exchanges=["NSE"],
        run_id="test-run",
    )

    fake = client._client  # type: ignore[attr-defined]
    assert len(results) == 1
    assert "_staging/history-load/test-run/data/daily_stock_data/exchange=NSE/year=2026/month=01/part.parquet" in fake.objects
    assert "data/daily_stock_data/exchange=NSE/year=2026/month=01/part.parquet" in fake.objects
    assert "data/daily_stock_data/exchange=NSE/year=2026/month=01/old.parquet" not in fake.objects
    assert "data/daily_stock_data/_manifest.json" in fake.objects
    manifest = json.loads(fake.objects["data/daily_stock_data/_manifest.json"].decode("utf-8"))
    assert manifest["run_id"] == "test-run"
    assert manifest["exchange_coverage"] == ["NSE"]
    assert manifest["upload_status"] == "promoted"
    assert manifest["partitions"][0]["rows"] == 1
    assert fake.uploaded_keys[0].startswith("_staging/history-load/test-run/")


def test_upload_verified_history_accepts_partition_directory(monkeypatch, tmp_path) -> None:
    client = _fake_client(monkeypatch)
    partition_dir = tmp_path / "history" / "exchange=NSE" / "year=2026" / "month=01"
    partition_dir.mkdir(parents=True)
    audit_path = tmp_path / "audit.json"
    _history_frame().write_parquet(partition_dir / "part.parquet")
    audit_path.write_text(json.dumps({"passed": True}), encoding="utf-8")

    results = upload_verified_history(
        client,
        path=tmp_path / "history",
        audit_path=audit_path,
        exchanges=["NSE"],
        run_id="test-run",
    )

    assert len(results) == 1
    assert "data/daily_stock_data/exchange=NSE/year=2026/month=01/part.parquet" in client._client.objects  # type: ignore[attr-defined]


def test_upload_verified_history_does_not_promote_on_staging_failure(monkeypatch, tmp_path) -> None:
    client = _fake_client(monkeypatch)
    history_path = tmp_path / "history.parquet"
    audit_path = tmp_path / "audit.json"
    _history_frame().write_parquet(history_path)
    audit_path.write_text(json.dumps({"passed": True}), encoding="utf-8")
    client.upload_bytes("data/daily_stock_data/exchange=NSE/year=2026/month=01/part.parquet", b"existing")

    def _fail_verify(*_args, **_kwargs):
        raise ValueError("staging failed")

    monkeypatch.setattr("trading_infra.storage.history._verify_uploaded_size", _fail_verify)

    with pytest.raises(ValueError, match="staging failed"):
        upload_verified_history(
            client,
            path=history_path,
            audit_path=audit_path,
            exchanges=["NSE"],
            run_id="test-run",
        )

    assert client.download_bytes("data/daily_stock_data/exchange=NSE/year=2026/month=01/part.parquet") == b"existing"


def test_r2_sync_check_reports_ok_and_mismatches(monkeypatch, tmp_path) -> None:
    client = _fake_client(monkeypatch)
    partition_path = tmp_path / "part.parquet"
    manifest_path = tmp_path / "partition_manifest.parquet"
    _history_frame().write_parquet(partition_path)
    _write_partition_manifest(manifest_path, partition_path)
    client.upload_file(partition_path, "data/daily_stock_data/exchange=NSE/year=2026/month=01/part.parquet")

    ok = check_r2_sync(client, exchange="NSE", partition_manifest_path=manifest_path)

    assert ok.status == "ok"
    assert ok.rows[0]["status"] == "OK"

    client.upload_bytes("data/daily_stock_data/exchange=NSE/year=2026/month=02/part.parquet", b"not parquet")
    stale_path = tmp_path / "stale.parquet"
    stale_frame = _history_frame().with_columns(pl.lit(101.5).alias("close"))
    stale_frame.write_parquet(stale_path)
    client.upload_file(stale_path, "data/daily_stock_data/exchange=NSE/year=2026/month=01/part.parquet")

    mismatch = check_r2_sync(client, exchange="NSE", partition_manifest_path=manifest_path)

    statuses = {row["status"] for row in mismatch.rows}
    assert "STALE" in statuses
    assert "EXTRA" in statuses
