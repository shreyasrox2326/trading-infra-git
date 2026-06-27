"""Minimal Cloudflare R2 client for project storage objects."""

from __future__ import annotations

from pathlib import Path
from tempfile import NamedTemporaryFile

import boto3

from trading_infra.storage.config import R2Config


class R2Client:
    """Small wrapper around the S3-compatible R2 API."""

    def __init__(self, config: R2Config) -> None:
        self.config = config
        session = boto3.session.Session()
        self._client = session.client(
            "s3",
            endpoint_url=config.endpoint_url,
            aws_access_key_id=config.access_key_id,
            aws_secret_access_key=config.secret_access_key,
            region_name=config.region_name,
        )

    @classmethod
    def from_env(cls) -> "R2Client":
        """Build an R2 client from environment configuration."""
        return cls(R2Config.from_env())

    def upload_bytes(self, key: str, payload: bytes, *, content_type: str | None = None) -> None:
        """Upload a small object payload."""
        extra_args = {"ContentType": content_type} if content_type else {}
        self._client.put_object(
            Bucket=self.config.bucket,
            Key=key,
            Body=payload,
            **extra_args,
        )

    def download_bytes(self, key: str) -> bytes:
        """Download a small object payload."""
        response = self._client.get_object(Bucket=self.config.bucket, Key=key)
        return response["Body"].read()

    def upload_file(self, local_path: str | Path, key: str) -> None:
        """Upload a local file to the configured bucket."""
        self._client.upload_file(str(local_path), self.config.bucket, key)

    def download_file(self, key: str, local_path: str | Path) -> Path:
        """Download an object into a local file path."""
        target = Path(local_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        self._client.download_file(self.config.bucket, key, str(target))
        return target

    def list_keys(self, prefix: str) -> list[str]:
        """List object keys under a prefix."""
        paginator = self._client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.config.bucket, Prefix=prefix):
            keys.extend(item["Key"] for item in page.get("Contents", []))
        return keys

    def exists(self, key: str) -> bool:
        """Return whether an object exists."""
        return key in self.list_keys(key)

    def download_to_tempfile(self, key: str, *, suffix: str = "") -> Path:
        """Download an object into a temporary file and return its path."""
        payload = self.download_bytes(key)
        with NamedTemporaryFile(delete=False, suffix=suffix) as handle:
            handle.write(payload)
            return Path(handle.name)

    def delete_key(self, key: str) -> None:
        """Delete a single object key."""
        self._client.delete_object(Bucket=self.config.bucket, Key=key)

    def delete_keys(self, keys: list[str]) -> None:
        """Delete multiple object keys."""
        if not keys:
            return
        for start in range(0, len(keys), 1000):
            batch = keys[start : start + 1000]
            self._client.delete_objects(
                Bucket=self.config.bucket,
                Delete={"Objects": [{"Key": key} for key in batch]},
            )

    def upload_text(self, key: str, text: str, *, encoding: str = "utf-8") -> None:
        """Upload a text object."""
        self.upload_bytes(key, text.encode(encoding), content_type="text/plain")

    def download_text(self, key: str, *, encoding: str = "utf-8") -> str:
        """Download a text object."""
        return self.download_bytes(key).decode(encoding)
