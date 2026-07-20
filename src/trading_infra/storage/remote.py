"""Higher-level R2 helpers for strategy artifacts, registries, and decisions."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl

from trading_infra.decisions import empty_decisions_frame
from trading_infra.registry import load_strategy_registry
from trading_infra.storage.decisions import read_decisions_parquet, write_decisions_parquet
from trading_infra.storage.market_data_remote import (
    list_daily_stock_data_keys,
    list_daily_stock_data_months,
    load_daily_stock_data_from_r2,
    load_daily_stock_data_history_from_r2,
    load_daily_stock_data_range_from_r2,
)
from trading_infra.storage.paths import (
    backtest_decisions_key,
    paper_decisions_key,
    registry_strategies_key,
    strategy_artifact_keys,
    strategy_prefix,
)
from trading_infra.storage.r2 import R2Client


def download_strategy_artifacts(client: R2Client, strategy_id: str, target_dir: str | Path) -> Path:
    """Download a strategy folder into a local target directory."""
    target_root = Path(target_dir) / "strategies" / strategy_id
    target_root.mkdir(parents=True, exist_ok=True)
    prefix = strategy_prefix(strategy_id)
    keys = client.list_keys(prefix)
    if not keys:
        raise FileNotFoundError(f"No strategy artifacts found for strategy_id={strategy_id}.")

    for key in keys:
        relative_parts = Path(key).parts[2:]
        client.download_file(key, target_root.joinpath(*relative_parts))

    return target_root


def load_strategy_registry_from_r2(client: R2Client) -> pl.DataFrame:
    """Download and validate the strategy registry from R2."""
    with TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "strategies.parquet"
        client.download_file(registry_strategies_key(), local_path)
        return load_strategy_registry(local_path)


def upload_strategy_artifacts(client: R2Client, strategy_id: str, base_path: str | Path) -> None:
    """Upload a local strategy folder to its canonical R2 location."""
    strategy_root = Path(base_path) / "strategies" / strategy_id
    keys = strategy_artifact_keys(strategy_id)
    required = {
        "config": strategy_root / "config.yaml",
        "metadata": strategy_root / "metadata.json",
    }
    optional = {
        "model": strategy_root / "model.pkl",
        "feature_config": strategy_root / "feature_config.yaml",
    }

    missing = [name for name, path in required.items() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required strategy artifacts for {strategy_id}: {missing}")

    for logical_name, path in required.items():
        client.upload_file(path, keys[logical_name])
    for logical_name, path in optional.items():
        if path.exists():
            client.upload_file(path, keys[logical_name])


def upload_strategy_registry(client: R2Client, local_path: str | Path) -> None:
    """Upload a validated local strategy registry parquet file to R2."""
    validated = load_strategy_registry(local_path)
    with TemporaryDirectory() as tmpdir:
        normalized_path = Path(tmpdir) / "strategies.parquet"
        validated.write_parquet(normalized_path)
        client.upload_file(normalized_path, registry_strategies_key())


def upload_backtest_decisions(client: R2Client, strategy_id: str, local_path: str | Path) -> None:
    """Upload a validated local backtest decisions parquet file to R2."""
    validated = read_decisions_parquet(local_path)
    with TemporaryDirectory() as tmpdir:
        normalized_path = Path(tmpdir) / "decisions.parquet"
        validated.write_parquet(normalized_path)
        client.upload_file(normalized_path, backtest_decisions_key(strategy_id))


def upload_paper_decisions(client: R2Client, strategy_id: str, local_path: str | Path) -> None:
    """Upload a validated local paper decisions parquet file to R2."""
    validated = read_decisions_parquet(local_path)
    with TemporaryDirectory() as tmpdir:
        normalized_path = Path(tmpdir) / "decisions.parquet"
        write_decisions_parquet(normalized_path, validated)
        client.upload_file(normalized_path, paper_decisions_key(strategy_id))


def download_paper_decisions(client: R2Client, strategy_id: str) -> pl.DataFrame:
    """Download paper decisions for a strategy if they exist."""
    key = paper_decisions_key(strategy_id)
    if not client.exists(key):
        return empty_decisions_frame()
    with TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "decisions.parquet"
        client.download_file(key, local_path)
        return read_decisions_parquet(local_path)


def download_backtest_decisions(client: R2Client, strategy_id: str) -> pl.DataFrame:
    """Download backtest decisions for a strategy if they exist."""
    key = backtest_decisions_key(strategy_id)
    if not client.exists(key):
        return empty_decisions_frame()
    with TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "decisions.parquet"
        client.download_file(key, local_path)
        return read_decisions_parquet(local_path)
