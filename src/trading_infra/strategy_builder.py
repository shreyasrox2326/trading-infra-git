"""Build runnable strategies from stored strategy artifacts."""

from __future__ import annotations

from typing import cast

from trading_infra.strategies.top_n import TopNByAdjustedCloseStrategy
from trading_infra.strategy import Strategy
from trading_infra.strategy_store import StoredStrategy


def strategy_type(stored_strategy: StoredStrategy) -> str:
    """Return the declared strategy type."""
    value = stored_strategy.config.get("strategy_type", stored_strategy.metadata.get("strategy_type"))
    if not value:
        raise ValueError(f"Strategy {stored_strategy.strategy_id} is missing strategy_type.")
    return str(value)


def build_strategy(stored_strategy: StoredStrategy) -> Strategy:
    """Instantiate a runnable strategy from stored artifacts."""
    declared_type = strategy_type(stored_strategy)

    if declared_type == "top_n_adj_close":
        top_n = stored_strategy.config.get("top_n")
        if top_n is None:
            raise ValueError(f"Strategy {stored_strategy.strategy_id} is missing required config field: top_n")
        return cast(
            Strategy,
            TopNByAdjustedCloseStrategy(
                strategy_id=stored_strategy.config.get("strategy_id", stored_strategy.strategy_id),
                top_n=int(top_n),
                lookback_days=(
                    int(stored_strategy.config["lookback_days"])
                    if stored_strategy.config.get("lookback_days") is not None
                    else 0
                ),
            ),
        )

    raise ValueError(
        f"Strategy {stored_strategy.strategy_id} uses unsupported strategy_type: {declared_type}"
    )
