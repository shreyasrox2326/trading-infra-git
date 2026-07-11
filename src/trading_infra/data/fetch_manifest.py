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
FETCH_STATUS_VALUES: tuple[str, ...] = (
    "expected",
    "downloaded",
    "skipped_existing",
    "not_available",
    "holiday_or_no_session",
    "rate_limited",
    "failed",
    "corrupt_html",
    "parse_failed",
    "validated",
    "missing",
)
FETCH_MANIFEST_SCHEMA: dict[str, pl.DataType] = {
    "exchange": pl.String,
    "date": pl.Date,
    "expected_format_id": pl.String,
    "expected_filename": pl.String,
    "expected_url_primary": pl.String,
    "local_path": pl.String,
    "status": pl.String,
    "http_status": pl.Int64,
    "bytes": pl.Int64,
    "sha256": pl.String,
    "attempts": pl.Int64,
    "last_attempt_at": pl.Datetime(time_zone="UTC"),
    "last_error": pl.String,
    "parser_hint": pl.String,
}


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
    return pl.DataFrame(rows, schema=FETCH_MANIFEST_SCHEMA).select(FETCH_MANIFEST_COLUMNS)


def read_raw_fetch_manifest(path: str | Path) -> pl.DataFrame:
    """Read a raw fetch manifest parquet."""
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"Raw fetch manifest not found: {source}")
    manifest = pl.read_parquet(source)
    missing = [column for column in FETCH_MANIFEST_COLUMNS if column not in manifest.columns]
    if missing:
        raise ValueError(f"Raw fetch manifest is missing columns: {missing}")
    return manifest.select(FETCH_MANIFEST_COLUMNS).cast(FETCH_MANIFEST_SCHEMA)


def select_manifest_dates(path: str | Path, *, statuses: set[str]) -> list:
    """Return dates whose manifest status is selected for repair."""
    statuses = {"expected" if status == "missing" else status for status in statuses}
    unsupported = sorted(statuses - set(FETCH_STATUS_VALUES))
    if unsupported:
        raise ValueError(f"Unsupported fetch manifest status filters: {unsupported}")
    manifest = read_raw_fetch_manifest(path)
    return (
        manifest.filter(pl.col("status").is_in(sorted(statuses)))
        .select("date")
        .unique()
        .sort("date")
        .get_column("date")
        .to_list()
    )


def merge_raw_fetch_manifest(
    *,
    existing_path: str | Path | None,
    results: list[BhavcopyFetchResult],
    exchange: str,
) -> pl.DataFrame:
    """Merge new result rows into an existing raw fetch manifest."""
    new_rows = build_raw_fetch_manifest(results, exchange=exchange)
    if existing_path is None or not Path(existing_path).exists():
        return new_rows
    existing = read_raw_fetch_manifest(existing_path)
    replacement_dates = new_rows.select("date").unique().get_column("date").to_list()
    retained = existing.filter(~pl.col("date").is_in(replacement_dates))
    return pl.concat([retained, new_rows], how="vertical_relaxed").sort("date").select(FETCH_MANIFEST_COLUMNS)


def write_raw_fetch_manifest(
    results: list[BhavcopyFetchResult],
    *,
    exchange: str,
    path: str | Path,
) -> Path:
    """Write a raw fetch manifest parquet and return its path."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    merge_raw_fetch_manifest(existing_path=target, results=results, exchange=exchange).write_parquet(target)
    return target


def mark_raw_fetch_manifest_row(
    path: str | Path,
    *,
    target_date,
    status: str,
    reason: str,
    exchange: str | None = None,
) -> Path:
    """Mark one manifest row with an explicit reviewed status and reason."""
    normalized_status = "expected" if status == "missing" else status
    if normalized_status not in FETCH_STATUS_VALUES:
        raise ValueError(f"Unsupported fetch manifest status: {status}")
    target = Path(path)
    manifest = read_raw_fetch_manifest(target)
    filters = [pl.col("date") == target_date]
    if exchange:
        filters.append(pl.col("exchange") == exchange.upper())
    predicate = filters[0]
    for item in filters[1:]:
        predicate = predicate & item
    matched = manifest.filter(predicate)
    if matched.height != 1:
        raise ValueError(
            f"Expected exactly one manifest row to mark for date={target_date}"
            + (f" exchange={exchange.upper()}" if exchange else "")
            + f"; found {matched.height}."
        )
    updated = manifest.with_columns(
        pl.when(predicate).then(pl.lit(normalized_status)).otherwise(pl.col("status")).alias("status"),
        pl.when(predicate).then(pl.lit(reason)).otherwise(pl.col("last_error")).alias("last_error"),
        pl.when(predicate).then(pl.lit(datetime.now(timezone.utc))).otherwise(pl.col("last_attempt_at")).alias("last_attempt_at"),
    )
    updated.write_parquet(target)
    return target


def combine_raw_fetch_manifests(
    paths: list[str | Path],
    *,
    output_path: str | Path,
) -> Path:
    """Combine per-exchange raw fetch manifests into one deduplicated parquet."""
    if not paths:
        raise ValueError("At least one manifest path is required.")
    frames = [read_raw_fetch_manifest(path) for path in paths]
    combined = (
        pl.concat(frames, how="vertical_relaxed")
        .sort(["exchange", "date"])
        .unique(subset=["exchange", "date"], keep="last", maintain_order=True)
        .select(FETCH_MANIFEST_COLUMNS)
        .cast(FETCH_MANIFEST_SCHEMA)
    )
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    combined.write_parquet(target)
    return target
