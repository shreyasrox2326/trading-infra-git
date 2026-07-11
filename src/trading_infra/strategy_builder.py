"""Build runnable strategies from stored strategy artifacts."""

from __future__ import annotations

from typing import cast

from trading_infra.strategies.private_pickle import PrivatePickleStrategy
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

    if declared_type == "private_pickle_v1":
        if stored_strategy.model_path is None:
            raise FileNotFoundError(f"Strategy {stored_strategy.strategy_id} is missing required private artifact: model.pkl")
        runtime_contract = stored_strategy.config.get("runtime_contract", "private_pickle_v1")
        if runtime_contract != "private_pickle_v1":
            raise ValueError(
                f"Strategy {stored_strategy.strategy_id} declares unsupported runtime_contract: {runtime_contract}"
            )
        lookback_days = stored_strategy.config.get("lookback_days")
        if lookback_days is None:
            raise ValueError(f"Strategy {stored_strategy.strategy_id} is missing required config field: lookback_days")
        return cast(
            Strategy,
            PrivatePickleStrategy(
                strategy_id=stored_strategy.config.get("strategy_id", stored_strategy.strategy_id),
                lookback_days=int(lookback_days),
                artifact_path=stored_strategy.model_path,
                feature_config=stored_strategy.feature_config,
            ),
        )

    raise ValueError(
        f"Strategy {stored_strategy.strategy_id} uses unsupported strategy_type: {declared_type}"
    )
