import json
from datetime import date
from pathlib import Path

import polars as pl

from trading_infra.data.market_data import load_daily_stock_data
from trading_infra.decisions import decisions_frame
from trading_infra.pipelines.backtest import run_backtest
from trading_infra.pipelines.paper import run_daily_paper_job_from_r2
from trading_infra.storage.config import R2Config
from trading_infra.storage.decisions import read_decisions_parquet, write_decisions_parquet
from trading_infra.storage.paths import paper_decisions_key
from trading_infra.storage.r2 import R2Client
from trading_infra.storage.remote import (
    upload_backtest_decisions,
    upload_strategy_artifacts,
    upload_strategy_registry,
)
from trading_infra.strategy_builder import build_strategy
from trading_infra.strategy_store import load_stored_strategy


class _FakePaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **kwargs):
        prefix = kwargs.get("Prefix", "")
        return [
            {"Contents": [item for item in page.get("Contents", []) if item["Key"].startswith(prefix)]}
            for page in self._pages
        ]


class _FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body, **_kwargs):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        from io import BytesIO

        return {"Body": BytesIO(self.objects[Key])}

    def upload_file(self, Filename, Bucket, Key):
        self.objects[Key] = Path(Filename).read_bytes()

    def download_file(self, Bucket, Key, Filename):
        Path(Filename).parent.mkdir(parents=True, exist_ok=True)
        Path(Filename).write_bytes(self.objects[Key])

    def get_paginator(self, _name):
        contents = [{"Key": key} for key in sorted(self.objects)]
        return _FakePaginator([{"Contents": contents}])


def _fake_client(monkeypatch) -> R2Client:
    fake_client = _FakeS3Client()

    class _FakeSession:
        def client(self, *_args, **_kwargs):
            return fake_client

    monkeypatch.setattr("boto3.session.Session", lambda: _FakeSession())
    return R2Client(
        R2Config(
            access_key_id="key",
            secret_access_key="secret",
            endpoint_url="https://example.r2.cloudflarestorage.com",
            bucket="bucket-name",
        )
    )


def test_local_to_r2_to_paper_flow(monkeypatch, tmp_path) -> None:
    client = _fake_client(monkeypatch)

    strategy_root = tmp_path / "strategies" / "top_n_adj_close_v1"
    strategy_root.mkdir(parents=True)
    (strategy_root / "config.yaml").write_text(
        "strategy_type: top_n_adj_close\nstrategy_id: top_n_adj_close_v1\ntop_n: 1\n",
        encoding="utf-8",
    )
    (strategy_root / "metadata.json").write_text(
        json.dumps({"strategy_type": "top_n_adj_close", "version": "v1"}),
        encoding="utf-8",
    )

    registry_root = tmp_path / "registry"
    registry_root.mkdir(parents=True)
    registry_path = registry_root / "strategies.parquet"
    pl.DataFrame([{"strategy_id": "top_n_adj_close_v1", "version": "v1", "status": "active"}]).write_parquet(
        registry_path
    )

    market_path = tmp_path / "market.parquet"
    pl.DataFrame(
        [
            {
                "date": date(2026, 1, 31),
                "exchange": "NSE",
                "isin": "INE000000001",
                "symbol": "AAA",
                "series": "EQ",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "prev_close": 99.0,
                "vwap": 100.0,
                "volume": 1000,
                "turnover": 100000.0,
                "trades": 100,
                "deliverable_qty": 500,
                "delivery_pct": 50.0,
                "adj_open": 100.0,
                "adj_high": 101.0,
                "adj_low": 99.0,
                "adj_close": 100.0,
                "adj_factor": 1.0,
            },
            {
                "date": date(2026, 2, 1),
                "exchange": "NSE",
                "isin": "INE000000002",
                "symbol": "BBB",
                "series": "EQ",
                "open": 110.0,
                "high": 111.0,
                "low": 109.0,
                "close": 110.0,
                "prev_close": 100.0,
                "vwap": 110.0,
                "volume": 1000,
                "turnover": 110000.0,
                "trades": 100,
                "deliverable_qty": 500,
                "delivery_pct": 50.0,
                "adj_open": 110.0,
                "adj_high": 111.0,
                "adj_low": 109.0,
                "adj_close": 110.0,
                "adj_factor": 1.0,
            },
        ]
    ).write_parquet(market_path)

    strategy = build_strategy(load_stored_strategy(tmp_path, "top_n_adj_close_v1"))
    market_data = load_daily_stock_data(str(market_path), as_of_date=date(2026, 2, 1))
    backtest = run_backtest(
        strategy,
        market_data,
        start_date=date(2026, 1, 31),
        end_date=date(2026, 2, 1),
    )
    backtest_path = tmp_path / "decisions" / "backtest" / "top_n_adj_close_v1" / "decisions.parquet"
    write_decisions_parquet(backtest_path, backtest)

    upload_strategy_artifacts(client, "top_n_adj_close_v1", tmp_path)
    upload_strategy_registry(client, registry_path)
    upload_backtest_decisions(client, "top_n_adj_close_v1", backtest_path)

    january = tmp_path / "january.parquet"
    february = tmp_path / "february.parquet"
    load_daily_stock_data(str(market_path)).filter(pl.col("date") == date(2026, 1, 31)).write_parquet(january)
    load_daily_stock_data(str(market_path)).filter(pl.col("date") == date(2026, 2, 1)).write_parquet(february)
    client.upload_file(january, "data/daily_stock_data/exchange=NSE/year=2026/month=01/part-1.parquet")
    client.upload_file(february, "data/daily_stock_data/exchange=NSE/year=2026/month=02/part-1.parquet")

    results = run_daily_paper_job_from_r2(
        client=client,
        exchange="NSE",
        as_of_date=date(2026, 2, 1),
        upload_results=True,
    )

    assert results["top_n_adj_close_v1"].get_column("strategy_id").to_list() == ["top_n_adj_close_v1"]

    downloaded = client.download_to_tempfile(
        "decisions/paper/top_n_adj_close_v1/decisions.parquet",
        suffix=".parquet",
    )
    paper = read_decisions_parquet(downloaded)
    assert paper.columns == backtest.columns


def test_r2_paper_flow_preserves_existing_history(monkeypatch, tmp_path) -> None:
    client = _fake_client(monkeypatch)

    strategy_root = tmp_path / "strategies" / "top_n_adj_close_v1"
    strategy_root.mkdir(parents=True)
    (strategy_root / "config.yaml").write_text(
        "strategy_type: top_n_adj_close\nstrategy_id: top_n_adj_close_v1\ntop_n: 1\n",
        encoding="utf-8",
    )
    (strategy_root / "metadata.json").write_text(
        json.dumps({"strategy_type": "top_n_adj_close", "version": "v1"}),
        encoding="utf-8",
    )

    registry_root = tmp_path / "registry"
    registry_root.mkdir(parents=True)
    registry_path = registry_root / "strategies.parquet"
    pl.DataFrame([{"strategy_id": "top_n_adj_close_v1", "version": "v1", "status": "active"}]).write_parquet(
        registry_path
    )

    upload_strategy_artifacts(client, "top_n_adj_close_v1", tmp_path)
    upload_strategy_registry(client, registry_path)

    existing_path = tmp_path / "existing-paper.parquet"
    write_decisions_parquet(
        existing_path,
        pl.DataFrame(
            [
                {
                    "date": date(2026, 1, 31),
                    "strategy_id": "top_n_adj_close_v1",
                    "exchange": "NSE",
                    "isin": "INE000000001",
                    "symbol": "AAA",
                    "target_weight": 1.0,
                    "rank": 1,
                    "score": 100.0,
                }
            ]
        ),
    )
    client.upload_file(existing_path, paper_decisions_key("top_n_adj_close_v1"))

    january = tmp_path / "january.parquet"
    february = tmp_path / "february.parquet"
    pl.DataFrame(
        [
            {
                "date": date(2026, 1, 31),
                "exchange": "NSE",
                "isin": "INE000000001",
                "symbol": "AAA",
                "series": "EQ",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "prev_close": 99.0,
                "vwap": 100.0,
                "volume": 1000,
                "turnover": 100000.0,
                "trades": 100,
                "deliverable_qty": 500,
                "delivery_pct": 50.0,
                "adj_open": 100.0,
                "adj_high": 101.0,
                "adj_low": 99.0,
                "adj_close": 100.0,
                "adj_factor": 1.0,
            }
        ]
    ).write_parquet(january)
    pl.DataFrame(
        [
            {
                "date": date(2026, 2, 1),
                "exchange": "NSE",
                "isin": "INE000000002",
                "symbol": "BBB",
                "series": "EQ",
                "open": 110.0,
                "high": 111.0,
                "low": 109.0,
                "close": 110.0,
                "prev_close": 100.0,
                "vwap": 110.0,
                "volume": 1000,
                "turnover": 110000.0,
                "trades": 100,
                "deliverable_qty": 500,
                "delivery_pct": 50.0,
                "adj_open": 110.0,
                "adj_high": 111.0,
                "adj_low": 109.0,
                "adj_close": 110.0,
                "adj_factor": 1.0,
            }
        ]
    ).write_parquet(february)
    client.upload_file(january, "data/daily_stock_data/exchange=NSE/year=2026/month=01/part-1.parquet")
    client.upload_file(february, "data/daily_stock_data/exchange=NSE/year=2026/month=02/part-1.parquet")

    results = run_daily_paper_job_from_r2(
        client=client,
        exchange="NSE",
        as_of_date=date(2026, 2, 1),
        upload_results=True,
    )

    assert results["top_n_adj_close_v1"].get_column("date").to_list() == [date(2026, 1, 31), date(2026, 2, 1)]
    downloaded = client.download_to_tempfile(paper_decisions_key("top_n_adj_close_v1"), suffix=".parquet")
    paper = read_decisions_parquet(downloaded)
    assert paper.get_column("date").to_list() == [date(2026, 1, 31), date(2026, 2, 1)]


def test_r2_paper_flow_loads_full_history_for_strategy(monkeypatch, tmp_path) -> None:
    client = _fake_client(monkeypatch)

    strategy_root = tmp_path / "strategies" / "history_v1"
    strategy_root.mkdir(parents=True)
    (strategy_root / "config.yaml").write_text(
        "strategy_type: top_n_adj_close\nstrategy_id: history_v1\ntop_n: 1\n",
        encoding="utf-8",
    )
    (strategy_root / "metadata.json").write_text(
        json.dumps({"strategy_type": "top_n_adj_close", "version": "v1"}),
        encoding="utf-8",
    )

    registry_root = tmp_path / "registry"
    registry_root.mkdir(parents=True)
    registry_path = registry_root / "strategies.parquet"
    pl.DataFrame([{"strategy_id": "history_v1", "version": "v1", "status": "active"}]).write_parquet(registry_path)

    upload_strategy_artifacts(client, "history_v1", tmp_path)
    upload_strategy_registry(client, registry_path)

    january = tmp_path / "january.parquet"
    february = tmp_path / "february.parquet"
    pl.DataFrame(
        [
            {
                "date": date(2026, 1, 31),
                "exchange": "NSE",
                "isin": "INE000000001",
                "symbol": "AAA",
                "series": "EQ",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "prev_close": 99.0,
                "vwap": 100.0,
                "volume": 1000,
                "turnover": 100000.0,
                "trades": 100,
                "deliverable_qty": 500,
                "delivery_pct": 50.0,
                "adj_open": 100.0,
                "adj_high": 101.0,
                "adj_low": 99.0,
                "adj_close": 100.0,
                "adj_factor": 1.0,
            }
        ]
    ).write_parquet(january)
    pl.DataFrame(
        [
            {
                "date": date(2026, 2, 1),
                "exchange": "NSE",
                "isin": "INE000000002",
                "symbol": "BBB",
                "series": "EQ",
                "open": 110.0,
                "high": 111.0,
                "low": 109.0,
                "close": 110.0,
                "prev_close": 100.0,
                "vwap": 110.0,
                "volume": 1000,
                "turnover": 110000.0,
                "trades": 100,
                "deliverable_qty": 500,
                "delivery_pct": 50.0,
                "adj_open": 110.0,
                "adj_high": 111.0,
                "adj_low": 109.0,
                "adj_close": 110.0,
                "adj_factor": 1.0,
            }
        ]
    ).write_parquet(february)
    client.upload_file(january, "data/daily_stock_data/exchange=NSE/year=2026/month=01/part-1.parquet")
    client.upload_file(february, "data/daily_stock_data/exchange=NSE/year=2026/month=02/part-1.parquet")

    class _HistoryAwareStrategy:
        strategy_id = "history_v1"

        def run(self, context):
            dates = context.market_data.get_column("date").unique().sort().to_list()
            if len(dates) < 2:
                return decisions_frame([])
            day = context.market_data.filter(pl.col("date") == context.as_of_date).head(1)
            row = next(day.iter_rows(named=True))
            return decisions_frame(
                [
                    {
                        "date": context.as_of_date,
                        "strategy_id": self.strategy_id,
                        "exchange": row["exchange"],
                        "isin": row["isin"],
                        "symbol": row["symbol"],
                        "target_weight": 1.0,
                        "rank": 1,
                        "score": 1.0,
                    }
                ]
            )

    monkeypatch.setattr("trading_infra.pipelines.paper.build_strategy", lambda _stored: _HistoryAwareStrategy())

    results = run_daily_paper_job_from_r2(
        client=client,
        exchange="NSE",
        as_of_date=date(2026, 2, 1),
        upload_results=False,
    )

    assert results["history_v1"].get_column("symbol").to_list() == ["BBB"]
