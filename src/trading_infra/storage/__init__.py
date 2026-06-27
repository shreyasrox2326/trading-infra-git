"""Storage helpers for canonical project paths and parquet artifacts."""

from trading_infra.storage.config import R2Config
from trading_infra.storage.decisions import read_decisions_parquet, write_decisions_parquet
from trading_infra.storage.paths import (
    backtest_decisions_key,
    daily_stock_data_prefix,
    paper_decisions_key,
    registry_strategies_key,
    strategy_config_key,
    strategy_metadata_key,
    strategy_model_key,
)
from trading_infra.storage.r2 import R2Client

__all__ = [
    "R2Client",
    "R2Config",
    "backtest_decisions_key",
    "daily_stock_data_prefix",
    "paper_decisions_key",
    "read_decisions_parquet",
    "registry_strategies_key",
    "strategy_config_key",
    "strategy_metadata_key",
    "strategy_model_key",
    "write_decisions_parquet",
]
