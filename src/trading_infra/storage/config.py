"""Configuration helpers for Cloudflare R2 access."""

from __future__ import annotations

from dataclasses import dataclass
from os import getenv


@dataclass(frozen=True)
class R2Config:
    """Runtime configuration for the private R2 bucket."""

    access_key_id: str
    secret_access_key: str
    endpoint_url: str
    bucket: str
    region_name: str = "auto"

    @classmethod
    def from_env(cls) -> "R2Config":
        """Load R2 configuration from environment variables."""
        values = {
            "access_key_id": getenv("R2_ACCESS_KEY_ID"),
            "secret_access_key": getenv("R2_SECRET_ACCESS_KEY"),
            "endpoint_url": getenv("R2_ENDPOINT_URL"),
            "bucket": getenv("R2_BUCKET"),
            "region_name": getenv("R2_REGION", "auto"),
        }
        missing = [key for key, value in values.items() if key != "region_name" and not value]
        if missing:
            raise ValueError(f"Missing required R2 environment variables for: {missing}")
        return cls(**values)  # type: ignore[arg-type]
