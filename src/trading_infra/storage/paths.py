"""Canonical object-key builders for project storage layout."""

from __future__ import annotations

from pathlib import PurePosixPath


def strategy_prefix(strategy_id: str) -> str:
    """Return the base strategy prefix under the R2 bucket."""
    return str(PurePosixPath("strategies") / strategy_id)


def strategy_config_key(strategy_id: str) -> str:
    """Return the canonical config key for a strategy."""
    return str(PurePosixPath(strategy_prefix(strategy_id)) / "config.yaml")


def strategy_metadata_key(strategy_id: str) -> str:
    """Return the canonical metadata key for a strategy."""
    return str(PurePosixPath(strategy_prefix(strategy_id)) / "metadata.json")


def strategy_model_key(strategy_id: str, *, filename: str = "model.pkl") -> str:
    """Return the canonical model key for a strategy."""
    return str(PurePosixPath(strategy_prefix(strategy_id)) / filename)


def strategy_feature_config_key(strategy_id: str) -> str:
    """Return the canonical feature-config key for a strategy."""
    return str(PurePosixPath(strategy_prefix(strategy_id)) / "feature_config.yaml")


def strategy_artifact_keys(strategy_id: str) -> dict[str, str]:
    """Return canonical strategy artifact keys by logical name."""
    return {
        "config": strategy_config_key(strategy_id),
        "metadata": strategy_metadata_key(strategy_id),
        "model": strategy_model_key(strategy_id),
        "feature_config": strategy_feature_config_key(strategy_id),
    }


def registry_strategies_key() -> str:
    """Return the strategy registry object key."""
    return str(PurePosixPath("registry") / "strategies.parquet")


def daily_stock_data_prefix(exchange: str, year: int, month: int) -> str:
    """Return the canonical prefix for monthly daily-stock parquet data."""
    return str(
        PurePosixPath("data")
        / "daily_stock_data"
        / f"exchange={exchange}"
        / f"year={year}"
        / f"month={month:02d}"
    )


def backtest_decisions_key(strategy_id: str) -> str:
    """Return the canonical backtest decisions object key."""
    return str(PurePosixPath("decisions") / "backtest" / strategy_id / "decisions.parquet")


def paper_decisions_key(strategy_id: str) -> str:
    """Return the canonical paper decisions object key."""
    return str(PurePosixPath("decisions") / "paper" / strategy_id / "decisions.parquet")


def performance_daily_key(strategy_id: str, *, decision_kind: str) -> str:
    """Return the canonical daily performance object key."""
    return str(PurePosixPath("performance") / decision_kind / strategy_id / "daily.parquet")


def performance_summary_key(strategy_id: str, *, decision_kind: str) -> str:
    """Return the canonical summary performance object key."""
    return str(PurePosixPath("performance") / decision_kind / strategy_id / "summary.json")
