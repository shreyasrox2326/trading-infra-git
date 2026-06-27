"""Simple example strategies for local development."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from trading_infra.decisions import decisions_frame
from trading_infra.strategy import StrategyContext


@dataclass(frozen=True)
class TopNByAdjustedCloseStrategy:
    """Select the top-N symbols by adjusted close on the requested date."""

    strategy_id: str
    top_n: int = 3

    def run(self, context: StrategyContext) -> pl.DataFrame:
        current_day = (
            context.market_data.filter(pl.col("date") == context.as_of_date)
            .sort(["adj_close", "symbol"], descending=[True, False])
            .head(self.top_n)
        )
        if current_day.is_empty():
            return decisions_frame([])

        target_weight = 1.0 / current_day.height
        records = []
        for rank, row in enumerate(current_day.iter_rows(named=True), start=1):
            records.append(
                {
                    "date": context.as_of_date,
                    "strategy_id": self.strategy_id,
                    "exchange": row["exchange"],
                    "isin": row["isin"],
                    "symbol": row["symbol"],
                    "target_weight": target_weight,
                    "rank": rank,
                    "score": float(row["adj_close"]),
                }
            )
        return decisions_frame(records)
