"""Compute realized performance from decision logs and market data."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl

from trading_infra.storage.paths import performance_daily_key, performance_summary_key
from trading_infra.storage.r2 import R2Client


PERFORMANCE_COLUMNS: tuple[str, ...] = (
    "date",
    "strategy_id",
    "decision_kind",
    "daily_return",
    "invested_weight",
    "cash_weight",
    "cumulative_multiple",
    "drawdown",
)

PERFORMANCE_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "strategy_id": pl.Utf8,
    "decision_kind": pl.Utf8,
    "daily_return": pl.Float64,
    "invested_weight": pl.Float64,
    "cash_weight": pl.Float64,
    "cumulative_multiple": pl.Float64,
    "drawdown": pl.Float64,
}


@dataclass(frozen=True)
class PerformanceResult:
    strategy_id: str
    decision_kind: str
    daily: pl.DataFrame
    summary: dict[str, object]


def _empty_performance_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {name: pl.Series(name=name, values=[], dtype=dtype) for name, dtype in PERFORMANCE_SCHEMA.items()}
    )


def compute_strategy_performance(
    *,
    decisions: pl.DataFrame,
    market_data: pl.DataFrame,
    strategy_id: str,
    decision_kind: str,
    primary_exchange: str | None = None,
) -> PerformanceResult:
    if decisions.is_empty():
        daily = _empty_performance_frame()
        return PerformanceResult(
            strategy_id=strategy_id,
            decision_kind=decision_kind,
            daily=daily,
            summary={
                "strategy_id": strategy_id,
                "decision_kind": decision_kind,
                "rows": 0,
                "realized_dates": 0,
                "latest_realized_date": None,
                "final_multiple": 1.0,
                "max_drawdown": 0.0,
            },
        )

    strategy_decisions = decisions.filter(pl.col("strategy_id") == strategy_id).sort(["date", "rank", "symbol"])
    if strategy_decisions.is_empty():
        return compute_strategy_performance(
            decisions=pl.DataFrame(strategy_decisions.schema),
            market_data=market_data,
            strategy_id=strategy_id,
            decision_kind=decision_kind,
            primary_exchange=primary_exchange,
        )

    trading_calendar_exchange = primary_exchange or strategy_decisions.get_column("exchange").drop_nulls().unique().sort().to_list()[0]

    returns = (
        market_data
        .sort(["exchange", "isin", "date"])
        .with_columns(
            pl.col("adj_close").shift(-1).over(["exchange", "isin"]).alias("next_adj_close"),
        )
        .with_columns(
            ((pl.col("next_adj_close") / pl.col("adj_close")) - 1.0).alias("next_close_to_close_return"),
        )
        .select(["date", "exchange", "isin", "symbol", "next_adj_close", "next_close_to_close_return"])
    )

    realized_rows = (
        strategy_decisions
        .join(returns, on=["date", "exchange", "isin", "symbol"], how="left")
        .filter(pl.col("next_adj_close").is_not_null())
        .with_columns((pl.col("target_weight") * pl.col("next_close_to_close_return")).alias("weighted_return"))
    )

    if realized_rows.is_empty():
        daily = _empty_performance_frame()
        return PerformanceResult(
            strategy_id=strategy_id,
            decision_kind=decision_kind,
            daily=daily,
            summary={
                "strategy_id": strategy_id,
                "decision_kind": decision_kind,
                "rows": int(strategy_decisions.height),
                "realized_dates": 0,
                "latest_realized_date": None,
                "final_multiple": 1.0,
                "max_drawdown": 0.0,
            },
        )

    realized_by_date = (
        realized_rows
        .group_by("date")
        .agg(
            pl.col("weighted_return").sum().alias("daily_return"),
            pl.col("target_weight").sum().alias("invested_weight"),
        )
        .sort("date")
    )

    start_date = realized_by_date.get_column("date").min()
    end_date = realized_by_date.get_column("date").max()
    calendar = (
        market_data
        .filter((pl.col("exchange") == trading_calendar_exchange) & (pl.col("date") >= start_date) & (pl.col("date") <= end_date))
        .select(pl.col("date").unique().sort())
        .sort("date")
    )

    daily = (
        calendar
        .join(realized_by_date, on="date", how="left")
        .with_columns(
            pl.lit(strategy_id).alias("strategy_id"),
            pl.lit(decision_kind).alias("decision_kind"),
            pl.col("daily_return").fill_null(0.0),
            pl.col("invested_weight").fill_null(0.0),
        )
        .with_columns((1.0 - pl.col("invested_weight")).clip(lower_bound=0.0).alias("cash_weight"))
    )

    cumulative: list[float] = []
    drawdowns: list[float] = []
    equity = 1.0
    peak = 1.0
    for ret in daily.get_column("daily_return").to_list():
        equity *= 1.0 + float(ret)
        peak = max(peak, equity)
        cumulative.append(equity)
        drawdowns.append(equity / peak - 1.0)

    daily = daily.with_columns(
        pl.Series("cumulative_multiple", cumulative),
        pl.Series("drawdown", drawdowns),
    ).select(PERFORMANCE_COLUMNS)

    summary = {
        "strategy_id": strategy_id,
        "decision_kind": decision_kind,
        "rows": int(strategy_decisions.height),
        "realized_dates": int(daily.height),
        "latest_realized_date": daily.get_column("date").max(),
        "final_multiple": float(cumulative[-1]) if cumulative else 1.0,
        "max_drawdown": float(min(drawdowns)) if drawdowns else 0.0,
        "primary_exchange": trading_calendar_exchange,
    }
    return PerformanceResult(strategy_id=strategy_id, decision_kind=decision_kind, daily=daily, summary=summary)


def write_performance_result(
    result: PerformanceResult,
    *,
    daily_path: str | Path,
    summary_path: str | Path,
) -> tuple[Path, Path]:
    daily_output = Path(daily_path)
    summary_output = Path(summary_path)
    daily_output.parent.mkdir(parents=True, exist_ok=True)
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    result.daily.write_parquet(daily_output)
    summary_output.write_text(json.dumps(result.summary, default=str, indent=2) + "\n", encoding="utf-8")
    return daily_output, summary_output


def upload_performance_result(client: R2Client, result: PerformanceResult) -> None:
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        daily_path = root / "daily.parquet"
        summary_path = root / "summary.json"
        write_performance_result(result, daily_path=daily_path, summary_path=summary_path)
        client.upload_file(daily_path, performance_daily_key(result.strategy_id, decision_kind=result.decision_kind))
        client.upload_file(summary_path, performance_summary_key(result.strategy_id, decision_kind=result.decision_kind))
