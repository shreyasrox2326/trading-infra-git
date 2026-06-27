from pathlib import Path

import polars as pl
import pytest

from trading_infra.registry import (
    REGISTRY_REQUIRED_COLUMNS,
    active_strategies,
    active_strategy_ids,
    load_strategy_registry,
    registry_entries,
)


def _registry_frame() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {"strategy_id": "momentum_v1", "version": "v1", "status": "active", "strategy_name": "Momentum"},
            {"strategy_id": "mean_revert_v1", "version": "v1", "status": "draft", "strategy_name": "Mean Revert"},
        ]
    )


def test_load_strategy_registry_from_frame() -> None:
    loaded = load_strategy_registry(_registry_frame())

    assert all(column in loaded.columns for column in REGISTRY_REQUIRED_COLUMNS)
    assert loaded.height == 2


def test_load_strategy_registry_from_path(tmp_path) -> None:
    path = tmp_path / "strategies.parquet"
    _registry_frame().write_parquet(path)

    loaded = load_strategy_registry(path)

    assert loaded.get_column("strategy_id").to_list() == ["momentum_v1", "mean_revert_v1"]


def test_active_strategies_filters_status() -> None:
    filtered = active_strategies(_registry_frame())

    assert filtered.get_column("strategy_id").to_list() == ["momentum_v1"]
    assert active_strategy_ids(_registry_frame()) == ["momentum_v1"]


def test_registry_entries_returns_typed_records() -> None:
    entries = registry_entries(_registry_frame())

    assert entries[0].strategy_id == "momentum_v1"
    assert entries[0].raw["strategy_name"] == "Momentum"


def test_load_strategy_registry_rejects_missing_columns() -> None:
    frame = pl.DataFrame([{"strategy_id": "momentum_v1", "status": "active"}])

    with pytest.raises(ValueError, match="missing required columns"):
        load_strategy_registry(frame)


def test_load_strategy_registry_rejects_null_strategy_id() -> None:
    frame = pl.DataFrame([{"strategy_id": None, "version": "v1", "status": "active"}])

    with pytest.raises(ValueError, match="null strategy_id"):
        load_strategy_registry(frame)
