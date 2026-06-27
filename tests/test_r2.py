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

    def get_paginator(self, _name):
        contents = [{"Key": key} for key in sorted(self.objects)]
        return _FakePaginator([{"Contents": contents}])


def test_r2_config_from_env(monkeypatch) -> None:
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("R2_ENDPOINT_URL", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("R2_BUCKET", "bucket-name")

    config = R2Config.from_env()

    assert config.bucket == "bucket-name"
    assert config.region_name == "auto"


def test_r2_config_requires_env(monkeypatch) -> None:
    for name in ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT_URL", "R2_BUCKET"):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValueError, match="Missing required R2 environment variables"):
        R2Config.from_env()


def test_r2_client_round_trip(monkeypatch, tmp_path) -> None:
    fake_client = _FakeS3Client()

    class _FakeSession:
        def client(self, *_args, **_kwargs):
            return fake_client

    monkeypatch.setattr("boto3.session.Session", lambda: _FakeSession())

    client = R2Client(
        R2Config(
            access_key_id="key",
            secret_access_key="secret",
            endpoint_url="https://example.r2.cloudflarestorage.com",
            bucket="bucket-name",
        )
    )

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


def test_storage_paths_match_readme_layout() -> None:
    assert strategy_config_key("momentum_v1") == "strategies/momentum_v1/config.yaml"
    assert strategy_metadata_key("momentum_v1") == "strategies/momentum_v1/metadata.json"
    assert strategy_model_key("momentum_v1") == "strategies/momentum_v1/model.pkl"
    assert registry_strategies_key() == "registry/strategies.parquet"
    assert daily_stock_data_prefix("NSE", 2024, 1) == "data/daily_stock_data/exchange=NSE/year=2024/month=01"
