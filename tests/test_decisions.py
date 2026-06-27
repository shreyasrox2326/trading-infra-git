from datetime import date

import polars as pl
import pytest

from trading_infra.decisions import DECISION_COLUMNS, example_decision_row, validate_decisions_frame
from trading_infra.storage.decisions import read_decisions_parquet, write_decisions_parquet
from trading_infra.storage.paths import backtest_decisions_key, paper_decisions_key


def test_validate_decisions_frame_accepts_valid_rows() -> None:
    frame = pl.DataFrame([example_decision_row()])

    validated = validate_decisions_frame(frame)

    assert validated.columns == list(DECISION_COLUMNS)
    assert validated.shape == (1, len(DECISION_COLUMNS))


def test_validate_decisions_frame_adds_optional_score_column() -> None:
    row = example_decision_row()
    row.pop("score")

    validated = validate_decisions_frame(pl.DataFrame([row]))

    assert validated.get_column("score").to_list() == [None]


def test_validate_decisions_frame_rejects_duplicates() -> None:
    row = example_decision_row()
    frame = pl.DataFrame([row, row])

    with pytest.raises(ValueError, match="duplicate"):
        validate_decisions_frame(frame)


def test_validate_decisions_frame_rejects_negative_weights() -> None:
    row = example_decision_row()
    row["target_weight"] = -0.2

    with pytest.raises(ValueError, match="negative"):
        validate_decisions_frame(pl.DataFrame([row]))


def test_validate_decisions_frame_rejects_unexpected_columns() -> None:
    row = example_decision_row()
    row["extra"] = "nope"

    with pytest.raises(ValueError, match="unexpected"):
        validate_decisions_frame(pl.DataFrame([row]))


def test_decision_storage_round_trip(tmp_path) -> None:
    frame = pl.DataFrame([example_decision_row()])
    output_path = tmp_path / "decisions.parquet"

    write_decisions_parquet(output_path, frame)
    loaded = read_decisions_parquet(output_path)

    assert loaded.equals(validate_decisions_frame(frame))


def test_missing_decision_file_returns_empty_frame(tmp_path) -> None:
    loaded = read_decisions_parquet(tmp_path / "missing.parquet")

    assert loaded.columns == list(DECISION_COLUMNS)
    assert loaded.is_empty()


def test_storage_keys_match_repo_layout() -> None:
    assert backtest_decisions_key("momentum_v1") == "decisions/backtest/momentum_v1/decisions.parquet"
    assert paper_decisions_key("momentum_v1") == "decisions/paper/momentum_v1/decisions.parquet"
