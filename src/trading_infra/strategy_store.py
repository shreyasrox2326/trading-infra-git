"""Local strategy artifact loading for versioned strategy folders."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from trading_infra.storage.paths import strategy_config_key, strategy_metadata_key


@dataclass(frozen=True)
class StoredStrategy:
    """Strategy artifacts loaded from a versioned strategy folder."""

    strategy_id: str
    root: Path
    config: dict[str, Any]
    metadata: dict[str, Any]


def strategy_root(base_path: str | Path, strategy_id: str) -> Path:
    """Return the local root folder for a strategy."""
    return Path(base_path) / "strategies" / strategy_id


def load_stored_strategy(base_path: str | Path, strategy_id: str) -> StoredStrategy:
    """Load local config and metadata for a stored strategy."""
    root = strategy_root(base_path, strategy_id)
    config_path = root / Path(strategy_config_key(strategy_id)).name
    metadata_path = root / Path(strategy_metadata_key(strategy_id)).name

    if not config_path.exists():
        raise FileNotFoundError(f"Missing strategy config: {config_path}")
    if not metadata_path.exists():
        raise FileNotFoundError(f"Missing strategy metadata: {metadata_path}")

    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    if not isinstance(config, dict):
        raise ValueError(f"Strategy config must deserialize to a mapping: {config_path}")
    if not isinstance(metadata, dict):
        raise ValueError(f"Strategy metadata must deserialize to a mapping: {metadata_path}")

    return StoredStrategy(
        strategy_id=strategy_id,
        root=root,
        config=config,
        metadata=metadata,
    )
