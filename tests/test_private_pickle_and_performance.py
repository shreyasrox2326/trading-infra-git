import json
from datetime import date
from pathlib import Path

import cloudpickle
import polars as pl
import pytest
import yaml

from trading_infra.cli import main
from trading_infra.decisions import empty_decisions_frame
from trading_infra.data.market_data import load_daily_stock_data as _load_daily_stock_data_impl
from trading_infra.performance import compute_strategy_performance
from trading_infra.storage.paths import performance_daily_key, performance_summary_key
from trading_infra.strategy_builder import build_strategy
from trading_infra.strategy_store import load_stored_strategy


def _market_frame() -> pl.DataFrame:
    rows = [
        {"date": date(2026, 1, 1), "exchange": "NSE", "isin": "INEAAA", "symbol": "AAA", "series": "EQ", "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "prev_close": 99.0, "vwap": 100.0, "volume": 1000, "turnover": 100000.0, "trades": 10, "deliverable_qty": None, "delivery_pct": None, "adj_open": 100.0, "adj_high": 101.0, "adj_low": 99.0, "adj_close": 100.0, "adj_factor": 1.0},
        {"date": date(2026, 1, 1), "exchange": "NSE", "isin": "INEBBB", "symbol": "BBB", "series": "EQ", "open": 90.0, "high": 91.0, "low": 89.0, "close": 90.0, "prev_close": 89.0, "vwap": 90.0, "volume": 1000, "turnover": 90000.0, "trades": 10, "deliverable_qty": None, "delivery_pct": None, "adj_open": 90.0, "adj_high": 91.0, "adj_low": 89.0, "adj_close": 90.0, "adj_factor": 1.0},
        {"date": date(2026, 1, 2), "exchange": "NSE", "isin": "INEAAA", "symbol": "AAA", "series": "EQ", "open": 110.0, "high": 111.0, "low": 109.0, "close": 110.0, "prev_close": 100.0, "vwap": 110.0, "volume": 1000, "turnover": 110000.0, "trades": 10, "deliverable_qty": None, "delivery_pct": None, "adj_open": 110.0, "adj_high": 111.0, "adj_low": 109.0, "adj_close": 110.0, "adj_factor": 1.0},
        {"date": date(2026, 1, 2), "exchange": "NSE", "isin": "INEBBB", "symbol": "BBB", "series": "EQ", "open": 95.0, "high": 96.0, "low": 94.0, "close": 95.0, "prev_close": 90.0, "vwap": 95.0, "volume": 1000, "turnover": 95000.0, "trades": 10, "deliverable_qty": None, "delivery_pct": None, "adj_open": 95.0, "adj_high": 96.0, "adj_low": 94.0, "adj_close": 95.0, "adj_factor": 1.0},
        {"date": date(2026, 1, 3), "exchange": "NSE", "isin": "INEAAA", "symbol": "AAA", "series": "EQ", "open": 105.0, "high": 106.0, "low": 104.0, "close": 105.0, "prev_close": 110.0, "vwap": 105.0, "volume": 1000, "turnover": 105000.0, "trades": 10, "deliverable_qty": None, "delivery_pct": None, "adj_open": 105.0, "adj_high": 106.0, "adj_low": 104.0, "adj_close": 105.0, "adj_factor": 1.0},
        {"date": date(2026, 1, 3), "exchange": "NSE", "isin": "INEBBB", "symbol": "BBB", "series": "EQ", "open": 98.0, "high": 99.0, "low": 97.0, "close": 98.0, "prev_close": 95.0, "vwap": 98.0, "volume": 1000, "turnover": 98000.0, "trades": 10, "deliverable_qty": None, "delivery_pct": None, "adj_open": 98.0, "adj_high": 99.0, "adj_low": 97.0, "adj_close": 98.0, "adj_factor": 1.0},
        {"date": date(2026, 1, 4), "exchange": "NSE", "isin": "INEAAA", "symbol": "AAA", "series": "EQ", "open": 106.0, "high": 107.0, "low": 105.0, "close": 106.0, "prev_close": 105.0, "vwap": 106.0, "volume": 1000, "turnover": 106000.0, "trades": 10, "deliverable_qty": None, "delivery_pct": None, "adj_open": 106.0, "adj_high": 107.0, "adj_low": 105.0, "adj_close": 106.0, "adj_factor": 1.0},
        {"date": date(2026, 1, 4), "exchange": "NSE", "isin": "INEBBB", "symbol": "BBB", "series": "EQ", "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "prev_close": 98.0, "vwap": 100.0, "volume": 1000, "turnover": 100000.0, "trades": 10, "deliverable_qty": None, "delivery_pct": None, "adj_open": 100.0, "adj_high": 101.0, "adj_low": 99.0, "adj_close": 100.0, "adj_factor": 1.0},
    ]
    return pl.DataFrame(rows)


def _write_private_strategy(root: Path, strategy_id: str) -> None:
    class _DemoPrivateStrategy:
        def decision(self, as_of_date, runtime):
            data = runtime.market_data(
                exchange="NSE",
                as_of_date=as_of_date,
                symbols=["AAA", "BBB"],
                series="EQ",
                columns=["date", "exchange", "isin", "symbol", "adj_close"],
            )
            latest = data.sort_values(["date", "symbol"]).groupby("symbol", as_index=False).tail(1)
            row = latest.sort_values(["adj_close", "symbol"], ascending=[False, True]).iloc[0]
            return [
                {
                    "exchange": row["exchange"],
                    "isin": row["isin"],
                    "symbol": row["symbol"],
                    "target_weight": 1.0,
                    "score": float(row["adj_close"]),
                }
            ]

    strategy_root = root / "strategies" / strategy_id
    strategy_root.mkdir(parents=True, exist_ok=True)
    (strategy_root / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "strategy_type": "private_pickle_v1",
                "strategy_id": strategy_id,
                "runtime_contract": "private_pickle_v1",
                "lookback_days": 10,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (strategy_root / "metadata.json").write_text(
        json.dumps({"strategy_name": "Demo Private", "version": "v1"}, indent=2),
        encoding="utf-8",
    )
    (strategy_root / "feature_config.yaml").write_text(
        yaml.safe_dump(
            {
                "required_features": ["market_data_slice"],
                "required_aggregates": [],
                "max_lookback_days": 10,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    with (strategy_root / "model.pkl").open("wb") as handle:
        cloudpickle.dump(_DemoPrivateStrategy(), handle)


def _write_feature_runtime_strategy(root: Path, strategy_id: str) -> None:
    class _FeatureStrategy:
        def decision(self, as_of_date, runtime):
            latest = runtime.latest_features(
                exchange="NSE",
                as_of_date=as_of_date,
                symbols=["AAA", "BBB"],
                series="EQ",
                columns=["exchange", "isin", "symbol", "ret_20"],
            )
            if latest.empty:
                return []
            ranked = latest.sort_values(["ret_20", "symbol"], ascending=[False, True])
            row = ranked.iloc[0]
            return [
                {
                    "exchange": row["exchange"],
                    "isin": row["isin"],
                    "symbol": row["symbol"],
                    "target_weight": 1.0,
                    "score": float(row["ret_20"]) if row["ret_20"] == row["ret_20"] else 0.0,
                }
            ]

    strategy_root = root / "strategies" / strategy_id
    strategy_root.mkdir(parents=True, exist_ok=True)
    (strategy_root / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "strategy_type": "private_pickle_v1",
                "strategy_id": strategy_id,
                "runtime_contract": "private_pickle_v1",
                "lookback_days": 30,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (strategy_root / "metadata.json").write_text(
        json.dumps({"strategy_name": "Feature Runtime", "version": "v1"}, indent=2),
        encoding="utf-8",
    )
    with (strategy_root / "model.pkl").open("wb") as handle:
        cloudpickle.dump(_FeatureStrategy(), handle)


def _write_registry(root: Path, strategy_id: str) -> None:
    registry_root = root / "registry"
    registry_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            {
                "strategy_id": strategy_id,
                "version": "1",
                "status": "active",
            }
        ]
    ).write_parquet(registry_root / "strategies.parquet")


def test_private_pickle_strategy_runs_in_backtest(tmp_path) -> None:
    strategy_id = "private_demo"
    _write_private_strategy(tmp_path, strategy_id)
    stored = load_stored_strategy(tmp_path, strategy_id)
    strategy = build_strategy(stored)

    frame = strategy.run(
        type("Context", (), {"strategy_id": strategy_id, "as_of_date": date(2026, 1, 2), "market_data": _market_frame()})()
    )

    assert frame.height == 1
    assert frame.get_column("symbol").to_list() == ["AAA"]
    assert frame.get_column("strategy_id").to_list() == [strategy_id]


def test_private_pickle_strategy_runs_in_local_paper_flow(tmp_path) -> None:
    strategy_id = "private_demo"
    _write_private_strategy(tmp_path, strategy_id)
    _write_registry(tmp_path, strategy_id)
    market_path = tmp_path / "market.parquet"
    _market_frame().write_parquet(market_path)

    code = main(
        [
            "paper-dry-run",
            "--base-path",
            str(tmp_path),
            "--market-data-path",
            str(market_path),
            "--date",
            "2026-01-02",
        ]
    )

    output_path = tmp_path / "decisions" / "paper" / strategy_id / "decisions.parquet"
    decisions = pl.read_parquet(output_path)
    assert code == 0
    assert decisions.height == 1
    assert decisions.get_column("symbol").to_list() == ["AAA"]


def test_private_pickle_strategy_runs_in_r2_paper_flow_and_uploads(monkeypatch, tmp_path, capsys) -> None:
    strategy_id = "private_demo"
    uploads: list[tuple[str, Path]] = []

    class _FakeClient:
        pass

    def _from_env():
        return _FakeClient()

    def _registry(_client):
        return pl.DataFrame([{"strategy_id": strategy_id, "version": "1", "status": "active"}])

    def _download_strategy_artifacts(_client, requested_strategy_id, target_dir):
        assert requested_strategy_id == strategy_id
        _write_private_strategy(Path(target_dir), strategy_id)
        return Path(target_dir) / "strategies" / strategy_id

    def _download_paper_decisions(_client, requested_strategy_id):
        assert requested_strategy_id == strategy_id
        return empty_decisions_frame()

    def _load_range(_client, *, exchange, start_date, end_date, symbols=None, columns=None):
        assert exchange == "NSE"
        assert end_date == date(2026, 1, 2)
        return _market_frame()

    def _upload_paper_decisions(_client, requested_strategy_id, local_path):
        uploads.append((requested_strategy_id, Path(local_path)))

    monkeypatch.setattr("trading_infra.cli.R2Client.from_env", _from_env)
    monkeypatch.setattr("trading_infra.pipelines.paper.load_strategy_registry_from_r2", _registry)
    monkeypatch.setattr("trading_infra.pipelines.paper.download_strategy_artifacts", _download_strategy_artifacts)
    monkeypatch.setattr("trading_infra.pipelines.paper.download_paper_decisions", _download_paper_decisions)
    monkeypatch.setattr("trading_infra.pipelines.paper.load_daily_stock_data_range_from_r2", _load_range)
    monkeypatch.setattr("trading_infra.pipelines.paper.upload_paper_decisions", _upload_paper_decisions)

    code = main(
        [
            "paper-dry-run",
            "--date",
            "2026-01-02",
            "--use-r2",
            "--exchange",
            "NSE",
            "--upload-results",
        ]
    )

    captured = capsys.readouterr().out
    assert code == 0
    assert "paper-dry-run date=2026-01-02 source=r2 exchange=NSE active_strategies=1 uploaded=true" in captured
    assert uploads and uploads[0][0] == strategy_id


def test_backtest_run_loads_local_market_data_in_chunks(monkeypatch, tmp_path, capsys) -> None:
    strategy_id = "private_demo"
    _write_private_strategy(tmp_path, strategy_id)
    market_path = tmp_path / "market.parquet"
    _market_frame().write_parquet(market_path)

    calls: list[tuple[date | None, date | None]] = []

    def _recording_loader(path, *, start_date=None, as_of_date=None, exchanges=None, symbols=None, columns=None):
        calls.append((start_date, as_of_date))
        return _load_daily_stock_data_impl(
            path,
            start_date=start_date,
            as_of_date=as_of_date,
            exchanges=exchanges,
            symbols=symbols,
            columns=columns,
        )

    monkeypatch.setattr("trading_infra.cli.load_daily_stock_data", _recording_loader)

    output_path = tmp_path / "decisions.parquet"
    code = main(
        [
            "backtest-run",
            "--base-path",
            str(tmp_path),
            "--strategy-id",
            strategy_id,
            "--market-data-path",
            str(market_path),
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-04",
            "--chunk-size-days",
            "2",
            "--warmup-days",
            "1",
            "--no-progress",
            "--output-path",
            str(output_path),
        ]
    )

    captured = capsys.readouterr().out
    decisions = pl.read_parquet(output_path)
    assert code == 0
    assert "chunk_size_days=2 warmup_days=1" in captured
    assert decisions.height == 4
    assert len(calls) == 2
    assert calls[0] == (date(2025, 12, 31), date(2026, 1, 2))
    assert calls[1] == (date(2026, 1, 2), date(2026, 1, 4))


def test_private_feature_runtime_builds_once_per_chunk(monkeypatch, tmp_path) -> None:
    strategy_id = "feature_runtime_demo"
    _write_feature_runtime_strategy(tmp_path, strategy_id)
    market_path = tmp_path / "market.parquet"
    _market_frame().write_parquet(market_path)

    build_calls = 0
    from trading_infra.strategies import private_pickle as private_pickle_module

    original = private_pickle_module._build_feature_frame

    def _counting_build(frame):
        nonlocal build_calls
        build_calls += 1
        return original(frame)

    monkeypatch.setattr(private_pickle_module, "_build_feature_frame", _counting_build)

    code = main(
        [
            "backtest-run",
            "--base-path",
            str(tmp_path),
            "--strategy-id",
            strategy_id,
            "--market-data-path",
            str(market_path),
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-04",
            "--chunk-size-days",
            "2",
            "--warmup-days",
            "30",
            "--no-progress",
            "--output-path",
            str(tmp_path / "feature_runtime_decisions.parquet"),
        ]
    )

    assert code == 0
    assert build_calls == 2


def test_performance_computation_fills_missing_cash_days() -> None:
    decisions = pl.DataFrame(
        [
            {"date": date(2026, 1, 1), "strategy_id": "demo", "exchange": "NSE", "isin": "INEAAA", "symbol": "AAA", "target_weight": 1.0, "rank": 1, "score": 1.0},
            {"date": date(2026, 1, 3), "strategy_id": "demo", "exchange": "NSE", "isin": "INEBBB", "symbol": "BBB", "target_weight": 0.5, "rank": 1, "score": 1.0},
        ]
    )
    result = compute_strategy_performance(
        decisions=decisions,
        market_data=_market_frame(),
        strategy_id="demo",
        decision_kind="backtest",
        primary_exchange="NSE",
    )

    assert result.daily.get_column("date").to_list() == [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)]
    assert result.daily.get_column("daily_return").to_list()[0] == pytest.approx(0.10)
    assert result.daily.get_column("daily_return").to_list()[1] == 0.0
    assert result.daily.get_column("cash_weight").to_list()[-1] == 0.5


def test_performance_compute_cli_writes_outputs(tmp_path, capsys) -> None:
    decisions_path = tmp_path / "decisions.parquet"
    market_path = tmp_path / "market.parquet"
    pl.DataFrame(
        [
            {"date": date(2026, 1, 1), "strategy_id": "demo", "exchange": "NSE", "isin": "INEAAA", "symbol": "AAA", "target_weight": 1.0, "rank": 1, "score": 1.0},
        ]
    ).write_parquet(decisions_path)
    _market_frame().write_parquet(market_path)

    code = main(
        [
            "performance-compute",
            "--strategy-id",
            "demo",
            "--decision-kind",
            "backtest",
            "--decisions-path",
            str(decisions_path),
            "--market-data-path",
            str(market_path),
            "--output-dir",
            str(tmp_path / "performance"),
        ]
    )

    captured = capsys.readouterr().out
    assert code == 0
    assert "performance-compute strategy_id=demo" in captured
    assert (tmp_path / "performance" / "daily.parquet").exists()
    assert (tmp_path / "performance" / "summary.json").exists()


def test_performance_refresh_cli_uploads_daily_and_summary(monkeypatch, capsys) -> None:
    uploads: list[str] = []

    class _FakeClient:
        pass

    def _from_env():
        return _FakeClient()

    def _registry(_client):
        return pl.DataFrame([{"strategy_id": "demo", "version": "1", "status": "active"}])

    def _decisions(_client, strategy_id):
        assert strategy_id == "demo"
        return pl.DataFrame(
            [
                {
                    "date": date(2026, 1, 1),
                    "strategy_id": "demo",
                    "exchange": "NSE",
                    "isin": "INEAAA",
                    "symbol": "AAA",
                    "target_weight": 1.0,
                    "rank": 1,
                    "score": 1.0,
                }
            ]
        )

    def _history(_client, *, exchange, end_date, symbols=None, columns=None):
        assert exchange == "NSE"
        assert end_date == date(2026, 1, 1)
        return _market_frame()

    def _upload(_client, result):
        uploads.extend(
            [
                performance_daily_key(result.strategy_id, decision_kind=result.decision_kind),
                performance_summary_key(result.strategy_id, decision_kind=result.decision_kind),
            ]
        )

    monkeypatch.setattr("trading_infra.cli.R2Client.from_env", _from_env)
    monkeypatch.setattr("trading_infra.cli.load_strategy_registry_from_r2", _registry)
    monkeypatch.setattr("trading_infra.cli.download_paper_decisions", _decisions)
    monkeypatch.setattr("trading_infra.cli.load_daily_stock_data_history_from_r2", _history)
    monkeypatch.setattr("trading_infra.cli.upload_performance_result", _upload)

    code = main(
        [
            "performance-refresh",
            "--decision-kind",
            "paper",
            "--exchange",
            "NSE",
            "--upload-results",
        ]
    )

    captured = capsys.readouterr().out
    assert code == 0
    assert "performance-refresh strategy_id=demo decision_kind=paper" in captured
    assert "performance-refresh status=ok" in captured
    assert uploads == [
        "performance/paper/demo/daily.parquet",
        "performance/paper/demo/summary.json",
    ]
