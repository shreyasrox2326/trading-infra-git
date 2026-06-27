"""Shared decision-log schema for backtest and paper workflows."""

from __future__ import annotations

from datetime import date

import polars as pl

DECISION_COLUMNS: tuple[str, ...] = (
    "date",
    "strategy_id",
    "exchange",
    "isin",
    "symbol",
    "target_weight",
    "rank",
    "score",
)

DECISION_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "strategy_id": pl.Utf8,
    "exchange": pl.Utf8,
    "isin": pl.Utf8,
    "symbol": pl.Utf8,
    "target_weight": pl.Float64,
    "rank": pl.Int64,
    "score": pl.Float64,
}

REQUIRED_COLUMNS: tuple[str, ...] = DECISION_COLUMNS[:-1]


def empty_decisions_frame() -> pl.DataFrame:
    """Return an empty frame with the canonical decision schema."""
    return pl.DataFrame(
        {name: pl.Series(name=name, values=[], dtype=dtype) for name, dtype in DECISION_SCHEMA.items()}
    )


def decisions_frame(records: list[dict]) -> pl.DataFrame:
    """Build a decision frame using the canonical schema."""
    if not records:
        return empty_decisions_frame()
    return pl.DataFrame(records, schema=DECISION_SCHEMA, strict=False).select(DECISION_COLUMNS)


def validate_decisions_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """Validate and normalize a decision frame for storage."""
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Decision frame is missing required columns: {missing}")

    extra = [column for column in frame.columns if column not in DECISION_COLUMNS]
    if extra:
        raise ValueError(f"Decision frame contains unexpected columns: {extra}")

    normalized = frame.with_columns(
        pl.col("date").cast(pl.Date),
        pl.col("strategy_id").cast(pl.Utf8),
        pl.col("exchange").cast(pl.Utf8),
        pl.col("isin").cast(pl.Utf8),
        pl.col("symbol").cast(pl.Utf8),
        pl.col("target_weight").cast(pl.Float64),
        pl.col("rank").cast(pl.Int64),
        pl.col("score").cast(pl.Float64, strict=False) if "score" in frame.columns else pl.lit(None, dtype=pl.Float64).alias("score"),
    )

    if normalized.height == 0:
        return normalized.select(DECISION_COLUMNS)

    date_values = normalized.get_column("date")
    if date_values.null_count() > 0:
        raise ValueError("Decision frame cannot contain null dates.")

    if normalized.select(pl.col("target_weight").is_null().any()).item():
        raise ValueError("Decision frame cannot contain null target weights.")

    if normalized.select((pl.col("target_weight") < 0).any()).item():
        raise ValueError("Decision frame cannot contain negative target weights.")

    if normalized.select(pl.col("rank").is_null().any()).item():
        raise ValueError("Decision frame cannot contain null ranks.")

    duplicate_count = normalized.select(
        pl.struct(["date", "strategy_id", "exchange", "isin", "symbol"]).is_duplicated().sum()
    ).item()
    if duplicate_count:
        raise ValueError("Decision frame contains duplicate decision rows.")

    return normalized.select(DECISION_COLUMNS).sort(["date", "strategy_id", "rank", "symbol"])


def example_decision_row() -> dict[str, date | float | int | str | None]:
    """Provide a sample row for documentation and tests."""
    return {
        "date": date(2026, 1, 2),
        "strategy_id": "momentum_v1",
        "exchange": "NSE",
        "isin": "INE000000000",
        "symbol": "ABC",
        "target_weight": 0.1,
        "rank": 1,
        "score": 0.87,
    }
