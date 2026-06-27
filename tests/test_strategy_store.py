import json

import pytest

from trading_infra.strategy_store import load_stored_strategy, strategy_root


def test_load_stored_strategy(tmp_path) -> None:
    root = strategy_root(tmp_path, "momentum_v1")
    root.mkdir(parents=True)
    (root / "config.yaml").write_text("top_n: 5\nrebalance: daily\n", encoding="utf-8")
    (root / "metadata.json").write_text(
        json.dumps({"strategy_name": "Momentum", "version": "v1", "status": "active"}),
        encoding="utf-8",
    )

    stored = load_stored_strategy(tmp_path, "momentum_v1")

    assert stored.strategy_id == "momentum_v1"
    assert stored.config["top_n"] == 5
    assert stored.metadata["status"] == "active"


def test_load_stored_strategy_requires_files(tmp_path) -> None:
    with pytest.raises(FileNotFoundError, match="Missing strategy config"):
        load_stored_strategy(tmp_path, "missing_v1")


def test_load_stored_strategy_requires_mapping(tmp_path) -> None:
    root = strategy_root(tmp_path, "broken_v1")
    root.mkdir(parents=True)
    (root / "config.yaml").write_text("- not\n- a\n- mapping\n", encoding="utf-8")
    (root / "metadata.json").write_text(json.dumps({"status": "draft"}), encoding="utf-8")

    with pytest.raises(ValueError, match="config must deserialize to a mapping"):
        load_stored_strategy(tmp_path, "broken_v1")
