"""Strategy registry loading and active-strategy filtering."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl


REGISTRY_REQUIRED_COLUMNS: tuple[str, ...] = (
    "strategy_id",
    "version",
    "status",
)


@dataclass(frozen=True)
class RegistryEntry:
    """Typed view of a strategy registry row."""

    strategy_id: str
    version: str
    status: str
    raw: dict[str, Any]


def validate_strategy_registry(frame: pl.DataFrame) -> pl.DataFrame:
    """Validate the thin strategy registry contract."""
    missing = [column for column in REGISTRY_REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Strategy registry is missing required columns: {missing}")

    normalized = frame.with_columns(
        pl.col("strategy_id").cast(pl.Utf8),
        pl.col("version").cast(pl.Utf8),
        pl.col("status").cast(pl.Utf8),
    )

    if normalized.select(pl.col("strategy_id").is_null().any()).item():
        raise ValueError("Strategy registry cannot contain null strategy_id values.")

    return normalized


def load_strategy_registry(path_or_frame: str | Path | pl.DataFrame) -> pl.DataFrame:
    """Load and validate the strategy registry."""
    frame = (
        path_or_frame
        if isinstance(path_or_frame, pl.DataFrame)
        else pl.read_parquet(Path(path_or_frame))
    )
    return validate_strategy_registry(frame)


def registry_entries(path_or_frame: str | Path | pl.DataFrame) -> list[RegistryEntry]:
    """Return typed registry entries."""
    frame = load_strategy_registry(path_or_frame)
    return [
        RegistryEntry(
            strategy_id=row["strategy_id"],
            version=row["version"],
            status=row["status"],
            raw=row,
        )
        for row in frame.iter_rows(named=True)
    ]


def active_strategies(path_or_frame: str | Path | pl.DataFrame) -> pl.DataFrame:
    """Return only active strategy rows."""
    frame = load_strategy_registry(path_or_frame)
    return frame.filter(pl.col("status") == "active")


def active_strategy_ids(path_or_frame: str | Path | pl.DataFrame) -> list[str]:
    """Return active strategy ids in registry order."""
    return active_strategies(path_or_frame).get_column("strategy_id").to_list()
