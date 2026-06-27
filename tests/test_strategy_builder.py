from pathlib import Path

import pytest

from trading_infra.strategies.top_n import TopNByAdjustedCloseStrategy
from trading_infra.strategy_builder import build_strategy
from trading_infra.strategy_store import StoredStrategy


def _stored_strategy(config: dict, metadata: dict | None = None) -> StoredStrategy:
    return StoredStrategy(
        strategy_id="momentum_v1",
        root=Path("/tmp/strategies/momentum_v1"),
        config=config,
        metadata=metadata or {},
    )


def test_build_strategy_supports_top_n_adj_close() -> None:
    strategy = build_strategy(
        _stored_strategy(
            {
                "strategy_type": "top_n_adj_close",
                "strategy_id": "momentum_v1",
                "top_n": 5,
            }
        )
    )

    assert isinstance(strategy, TopNByAdjustedCloseStrategy)
    assert strategy.top_n == 5


def test_build_strategy_reads_type_from_metadata() -> None:
    strategy = build_strategy(
        _stored_strategy(
            {
                "strategy_id": "momentum_v1",
                "top_n": 3,
            },
            metadata={"strategy_type": "top_n_adj_close"},
        )
    )

    assert isinstance(strategy, TopNByAdjustedCloseStrategy)
    assert strategy.top_n == 3


def test_build_strategy_rejects_unsupported_type() -> None:
    with pytest.raises(ValueError, match="unsupported strategy_type"):
        build_strategy(_stored_strategy({"strategy_type": "unknown", "top_n": 5}))


def test_build_strategy_requires_strategy_type() -> None:
    with pytest.raises(ValueError, match="missing strategy_type"):
        build_strategy(_stored_strategy({"top_n": 5}))


def test_build_strategy_requires_top_n() -> None:
    with pytest.raises(ValueError, match="missing required config field: top_n"):
        build_strategy(_stored_strategy({"strategy_type": "top_n_adj_close"}))
