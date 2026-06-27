"""Canonical object-key builders for project storage layout."""

from __future__ import annotations

from pathlib import PurePosixPath


def strategy_prefix(strategy_id: str) -> str:
    """Return the base strategy prefix under the R2 bucket."""
    return str(PurePosixPath("strategies") / strategy_id)


def backtest_decisions_key(strategy_id: str) -> str:
    """Return the canonical backtest decisions object key."""
    return str(PurePosixPath("decisions") / "backtest" / strategy_id / "decisions.parquet")


def paper_decisions_key(strategy_id: str) -> str:
    """Return the canonical paper decisions object key."""
    return str(PurePosixPath("decisions") / "paper" / strategy_id / "decisions.parquet")
