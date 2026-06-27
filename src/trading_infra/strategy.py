"""Strategy protocol for blackbox decision generators."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol

import polars as pl


@dataclass(frozen=True)
class StrategyContext:
    """Inputs shared by local backtest and online paper execution."""

    strategy_id: str
    as_of_date: date
    market_data: pl.DataFrame


class Strategy(Protocol):
    """Blackbox strategy that emits final decision rows for a date."""

    strategy_id: str

    def run(self, context: StrategyContext) -> pl.DataFrame:
        """Return decision rows for the requested date."""
