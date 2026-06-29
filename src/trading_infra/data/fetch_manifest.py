"""Raw bhavcopy fetch manifest helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

import polars as pl

from trading_infra.data.bhavcopy import BhavcopyFetchResult
from trading_infra.data.formats import get_bhavcopy_format


FETCH_MANIFEST_COLUMNS: tuple[str, ...] = (
    "exchange",
    "date",
    "expected_format_id",
    "expected_filename",
    "expected_url_primary",
    "local_path",
    "status",
    "http_status",
    "bytes",
    "sha256",
    "attempts",
    "last_attempt_at",
    "last_error",
    "parser_hint",
)


def default_raw_fetch_manifest_path(exchange: str) -> Path:
    """Return the default local manifest path for an exchange raw fetch."""
    return Path("data") / "import" / "manifests" / f"raw_fetch_{exchange.upper()}.parquet"


def _file_sha256(path: Path | None) -> str | None:
    if path is None or not path.exists():
        return None
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_raw_fetch_manifest(results: list[BhavcopyFetchResult], *, exchange: str) -> pl.DataFrame:
    """Build a parquet-ready raw fetch manifest from per-date fetch results."""
    rows = []
    normalized_exchange = exchange.upper()
    for result in results:
        expected_format = get_bhavcopy_format(normalized_exchange, result.requested_date)
        local_path = result.path if result.path is not None else None
        rows.append(
            {
                "exchange": normalized_exchange,
                "date": result.requested_date,
                "expected_format_id": expected_format.format_id,
                "expected_filename": expected_format.filename_for(result.requested_date),
                "expected_url_primary": expected_format.urls_for(result.requested_date)[0],
                "local_path": local_path.as_posix() if local_path else None,
                "status": result.status,
                "http_status": None,
                "bytes": local_path.stat().st_size if local_path and local_path.exists() else None,
                "sha256": _file_sha256(local_path),
                "attempts": None,
                "last_attempt_at": datetime.now(timezone.utc) if result.network_requested else None,
                "last_error": result.message if result.status not in {"downloaded", "skipped_existing"} else "",
                "parser_hint": expected_format.parser,
            }
        )
    return pl.DataFrame(rows).select(FETCH_MANIFEST_COLUMNS)


def write_raw_fetch_manifest(
    results: list[BhavcopyFetchResult],
    *,
    exchange: str,
    path: str | Path,
) -> Path:
    """Write a raw fetch manifest parquet and return its path."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    build_raw_fetch_manifest(results, exchange=exchange).write_parquet(target)
    return target
