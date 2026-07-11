"""Minimal Cloudflare R2 client for project storage objects."""

from __future__ import annotations

from hashlib import md5
from pathlib import Path
from tempfile import NamedTemporaryFile

import boto3

from trading_infra.storage.config import R2Config

DEFAULT_UPLOAD_PART_SIZE = 8 * 1024 * 1024


def file_md5(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Return the hex MD5 digest for a local file."""
    digest = md5()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_multipart_etag(etag: str | None) -> bool:
    """Return whether an object ETag uses the multipart suffix form."""
    if not etag:
        return False
    normalized = etag.strip('"')
    parts = normalized.rsplit("-", 1)
    return len(parts) == 2 and parts[1].isdigit()


def expected_upload_etag(
    path: str | Path,
    *,
    chunk_size: int = DEFAULT_UPLOAD_PART_SIZE,
    md5_hex: str | None = None,
) -> str:
    """Return the S3-style ETag expected from uploading one local file."""
    normalized_path = Path(path)
    part_digests: list[bytes] = []
    with normalized_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            part_digests.append(md5(chunk).digest())
    if not part_digests:
        return f'"{md5(b"").hexdigest()}"'
    if len(part_digests) == 1:
        single_part_md5 = md5_hex or part_digests[0].hex()
        return f'"{single_part_md5}"'
    combined = md5(b"".join(part_digests)).hexdigest()
    return f'"{combined}-{len(part_digests)}"'


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

    def head_object(self, key: str) -> dict:
        """Return metadata for one object key."""
        response = self._client.head_object(Bucket=self.config.bucket, Key=key)
        return {
            "key": key,
            "size": int(response.get("ContentLength", 0)),
            "etag": response.get("ETag"),
            "last_modified": response.get("LastModified"),
        }

    def copy_object(self, source_key: str, destination_key: str) -> None:
        """Server-side copy one object key to another key in the same bucket."""
        self._client.copy_object(
            Bucket=self.config.bucket,
            CopySource={"Bucket": self.config.bucket, "Key": source_key},
            Key=destination_key,
        )

    def list_keys(self, prefix: str) -> list[str]:
        """List object keys under a prefix."""
        return [item["key"] for item in self.list_objects(prefix)]

    def list_objects(self, prefix: str) -> list[dict]:
        """List object keys and sizes under a prefix."""
        paginator = self._client.get_paginator("list_objects_v2")
        objects: list[dict] = []
        for page in paginator.paginate(Bucket=self.config.bucket, Prefix=prefix):
            objects.extend(
                {
                    "key": item["Key"],
                    "size": int(item.get("Size", 0)),
                    "last_modified": item.get("LastModified"),
                }
                for item in page.get("Contents", [])
            )
        return objects

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
