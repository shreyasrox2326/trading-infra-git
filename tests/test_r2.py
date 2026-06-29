import io
from pathlib import Path

import pytest

from trading_infra.storage.config import R2Config
from trading_infra.storage.paths import (
    daily_stock_data_prefix,
    registry_strategies_key,
    strategy_config_key,
    strategy_metadata_key,
    strategy_model_key,
)
from trading_infra.storage.r2 import R2Client
from trading_infra.storage.usage import R2BudgetThresholds, apply_r2_budget, collect_r2_usage, write_r2_usage_snapshot


class _FakePaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **_kwargs):
        return self._pages


class _FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.uploaded_files: list[tuple[str, str, str]] = []
        self.downloaded_files: list[tuple[str, str, str]] = []

    def put_object(self, Bucket, Key, Body, **_kwargs):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}

    def upload_file(self, Filename, Bucket, Key):
        self.uploaded_files.append((Filename, Bucket, Key))
        self.objects[Key] = Path(Filename).read_bytes()

    def download_file(self, Bucket, Key, Filename):
        self.downloaded_files.append((Bucket, Key, Filename))
        Path(Filename).write_bytes(self.objects[Key])

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)

    def delete_objects(self, Bucket, Delete):
        for item in Delete.get("Objects", []):
            self.objects.pop(item["Key"], None)

    def get_paginator(self, _name):
        contents = [{"Key": key, "Size": len(self.objects[key])} for key in sorted(self.objects)]
        return _FakePaginator([{"Contents": contents}])


def _client_with_fake(fake_client: _FakeS3Client, monkeypatch) -> R2Client:
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


def test_r2_config_from_env(monkeypatch) -> None:
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("R2_S3_API", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("R2_BUCKET_NAME", "bucket-name")

    config = R2Config.from_env()

    assert config.bucket == "bucket-name"
    assert config.region_name == "auto"


def test_r2_config_requires_env(monkeypatch) -> None:
    for name in ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_S3_API", "R2_BUCKET_NAME"):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValueError, match="Missing required R2 environment variables"):
        R2Config.from_env()


def test_r2_client_round_trip(monkeypatch, tmp_path) -> None:
    fake_client = _FakeS3Client()
    client = _client_with_fake(fake_client, monkeypatch)

    client.upload_text("strategies/demo/config.yaml", "top_n: 5")
    assert client.download_text("strategies/demo/config.yaml") == "top_n: 5"

    local_source = tmp_path / "local.bin"
    local_source.write_bytes(b"payload")
    client.upload_file(local_source, "models/demo/model.pkl")

    local_target = tmp_path / "downloaded.bin"
    client.download_file("models/demo/model.pkl", local_target)
    assert local_target.read_bytes() == b"payload"

    assert client.exists("models/demo/model.pkl")
    assert "models/demo/model.pkl" in client.list_keys("models/")

    client.delete_key("models/demo/model.pkl")
    assert not client.exists("models/demo/model.pkl")


def test_r2_usage_and_budget_snapshot(monkeypatch, tmp_path) -> None:
    fake_client = _FakeS3Client()
    client = _client_with_fake(fake_client, monkeypatch)
    client.upload_bytes("data/a.bin", b"12345")
    client.upload_bytes("data/b.bin", b"123")

    usage = collect_r2_usage(client, prefix="data/")
    checked = apply_r2_budget(
        usage,
        thresholds=R2BudgetThresholds(warn_storage_bytes=7, fail_storage_bytes=100),
    )
    snapshot = write_r2_usage_snapshot(checked, output_dir=tmp_path)

    assert usage["bucket"] == "bucket-name"
    assert usage["storage_bytes"] == 8
    assert usage["object_count"] == 2
    assert checked["status"] == "warn"
    assert snapshot.exists()


def test_r2_budget_check_fails_over_threshold(monkeypatch) -> None:
    fake_client = _FakeS3Client()
    client = _client_with_fake(fake_client, monkeypatch)
    client.upload_bytes("data/a.bin", b"12345")

    report = apply_r2_budget(
        collect_r2_usage(client, prefix="data/"),
        thresholds=R2BudgetThresholds(warn_storage_bytes=1, fail_storage_bytes=4),
    )

    assert report["status"] == "fail"
    assert report["fail_reasons"]


def test_storage_paths_match_readme_layout() -> None:
    assert strategy_config_key("momentum_v1") == "strategies/momentum_v1/config.yaml"
    assert strategy_metadata_key("momentum_v1") == "strategies/momentum_v1/metadata.json"
    assert strategy_model_key("momentum_v1") == "strategies/momentum_v1/model.pkl"
    assert registry_strategies_key() == "registry/strategies.parquet"
    assert daily_stock_data_prefix("NSE", 2024, 1) == "data/daily_stock_data/exchange=NSE/year=2024/month=01"
