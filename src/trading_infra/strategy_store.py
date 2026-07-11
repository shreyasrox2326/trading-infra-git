"""Local strategy artifact loading for versioned strategy folders."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from trading_infra.storage.paths import (
    strategy_config_key,
    strategy_feature_config_key,
    strategy_metadata_key,
    strategy_model_key,
)


@dataclass(frozen=True)
class StoredStrategy:
    """Strategy artifacts loaded from a versioned strategy folder."""

    strategy_id: str
    root: Path
    config: dict[str, Any]
    metadata: dict[str, Any]
    model_path: Path | None = None
    feature_config: dict[str, Any] = field(default_factory=dict)


def strategy_root(base_path: str | Path, strategy_id: str) -> Path:
    """Return the local root folder for a strategy."""
    return Path(base_path) / "strategies" / strategy_id


def load_stored_strategy(base_path: str | Path, strategy_id: str) -> StoredStrategy:
    """Load local config and metadata for a stored strategy."""
    root = strategy_root(base_path, strategy_id)
    config_path = root / Path(strategy_config_key(strategy_id)).name
    metadata_path = root / Path(strategy_metadata_key(strategy_id)).name
    model_path = root / Path(strategy_model_key(strategy_id)).name
    feature_config_path = root / Path(strategy_feature_config_key(strategy_id)).name

    if not config_path.exists():
        raise FileNotFoundError(f"Missing strategy config: {config_path}")
    if not metadata_path.exists():
        raise FileNotFoundError(f"Missing strategy metadata: {metadata_path}")

    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    feature_config = (
        yaml.safe_load(feature_config_path.read_text(encoding="utf-8")) or {}
        if feature_config_path.exists()
        else {}
    )

    if not isinstance(config, dict):
        raise ValueError(f"Strategy config must deserialize to a mapping: {config_path}")
    if not isinstance(metadata, dict):
        raise ValueError(f"Strategy metadata must deserialize to a mapping: {metadata_path}")
    if not isinstance(feature_config, dict):
        raise ValueError(f"Strategy feature_config must deserialize to a mapping: {feature_config_path}")

    return StoredStrategy(
        strategy_id=strategy_id,
        root=root,
        config=config,
        metadata=metadata,
        model_path=model_path if model_path.exists() else None,
        feature_config=feature_config,
    )
