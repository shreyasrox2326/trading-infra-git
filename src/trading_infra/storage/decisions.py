"""Parquet IO helpers for decision logs."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from trading_infra.decisions import empty_decisions_frame, validate_decisions_frame


def write_decisions_parquet(path: str | Path, frame: pl.DataFrame) -> Path:
    """Validate and write a decisions parquet file."""
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    validated = validate_decisions_frame(frame)
    validated.write_parquet(output_path)
    return output_path


def read_decisions_parquet(path: str | Path) -> pl.DataFrame:
    """Read and validate a decisions parquet file."""
    input_path = Path(path)
    if not input_path.exists():
        return empty_decisions_frame()
    frame = pl.read_parquet(input_path)
    return validate_decisions_frame(frame)
