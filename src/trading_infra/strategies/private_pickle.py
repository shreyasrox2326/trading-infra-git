"""Runtime helpers for private pickle-backed strategies."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any

import cloudpickle
import numpy as np
import pandas as pd
import polars as pl

from trading_infra.decisions import decisions_frame
from trading_infra.strategy import StrategyContext

_MARKET_COLUMNS = [
    "date",
    "exchange",
    "isin",
    "symbol",
    "series",
    "open",
    "high",
    "low",
    "close",
    "prev_close",
    "vwap",
    "volume",
    "turnover",
    "trades",
    "deliverable_qty",
    "delivery_pct",
    "adj_open",
    "adj_high",
    "adj_low",
    "adj_close",
    "adj_factor",
]

_FEATURE_COLUMNS = [
    *_MARKET_COLUMNS,
    "ret_5",
    "ret_20",
    "ret_60",
    "ema_20",
    "ema_trend_20",
    "drawdown_20",
    "drawdown_60",
    "rsi_14",
    "adx_14",
    "turnover_median_20",
    "turnover_median_60",
    "daily_return",
    "realized_vol_20",
    "is_above_ema_20",
]


def _rsi(series: pd.Series, window: int = 14) -> pd.Series:
    delta = series.diff()
    gains = delta.clip(lower=0.0)
    losses = -delta.clip(upper=0.0)
    avg_gain = gains.ewm(alpha=1 / window, min_periods=window, adjust=False).mean()
    avg_loss = losses.ewm(alpha=1 / window, min_periods=window, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    return 100 - (100 / (1 + rs))


def _adx(frame: pd.DataFrame, window: int = 14) -> pd.Series:
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    close = frame["close"].astype(float)
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    tr = pd.concat(
        [
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / window, min_periods=window, adjust=False).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / window, min_periods=window, adjust=False).mean() / atr.replace(0.0, np.nan)
    minus_di = 100 * minus_dm.ewm(alpha=1 / window, min_periods=window, adjust=False).mean() / atr.replace(0.0, np.nan)
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)) * 100
    return dx.ewm(alpha=1 / window, min_periods=window, adjust=False).mean()


def _build_feature_frame(market_frame: pd.DataFrame) -> pd.DataFrame:
    ordered = market_frame.sort_values(["exchange", "symbol", "date"]).copy()
    grouped = ordered.groupby(["exchange", "symbol"], group_keys=False)
    ordered["ret_5"] = grouped["adj_close"].pct_change(5)
    ordered["ret_20"] = grouped["adj_close"].pct_change(20)
    ordered["ret_60"] = grouped["adj_close"].pct_change(60)
    ordered["ema_20"] = grouped["adj_close"].transform(lambda s: s.ewm(span=20, adjust=False).mean())
    ordered["ema_trend_20"] = ordered["adj_close"] / ordered["ema_20"] - 1.0
    ordered["drawdown_20"] = grouped["adj_close"].transform(lambda s: s / s.rolling(20, min_periods=5).max() - 1.0)
    ordered["drawdown_60"] = grouped["adj_close"].transform(lambda s: s / s.rolling(60, min_periods=10).max() - 1.0)
    ordered["rsi_14"] = grouped["adj_close"].transform(_rsi)
    ordered["adx_14"] = grouped[["high", "low", "close"]].apply(_adx).sort_index()
    ordered["turnover_median_20"] = grouped["turnover"].transform(lambda s: s.rolling(20, min_periods=5).median())
    ordered["turnover_median_60"] = grouped["turnover"].transform(lambda s: s.rolling(60, min_periods=10).median())
    ordered["daily_return"] = grouped["adj_close"].pct_change().fillna(0.0)
    ordered["realized_vol_20"] = grouped["daily_return"].transform(lambda s: s.rolling(20, min_periods=10).std())
    ordered["is_above_ema_20"] = (ordered["adj_close"] > ordered["ema_20"]).astype(float)
    return ordered


class PrivateStrategyRuntimeV1:
    """Small in-process runtime exposed to private artifacts."""

    def __init__(self, market_data_frame: pl.DataFrame) -> None:
        self.market_data_frame = market_data_frame
        self._market_pandas: pd.DataFrame | None = None
        self._feature_pandas: pd.DataFrame | None = None
        self._latest_feature_cache: dict[tuple[date, str | None, tuple[str, ...] | None, tuple[str, ...] | None], pd.DataFrame] = {}

    def _market_frame(self) -> pd.DataFrame:
        if self._market_pandas is None:
            self._market_pandas = (
                self.market_data_frame
                .select(_MARKET_COLUMNS)
                .sort(["date", "exchange", "symbol"])
                .to_pandas()
            )
            if not self._market_pandas.empty:
                self._market_pandas["date"] = pd.to_datetime(self._market_pandas["date"])
        return self._market_pandas

    def _feature_frame(self) -> pd.DataFrame:
        if self._feature_pandas is None:
            self._feature_pandas = _build_feature_frame(self._market_frame())
        return self._feature_pandas

    def _filter_pandas(
        self,
        frame: pd.DataFrame,
        *,
        exchange: str | None = None,
        as_of_date: date | None = None,
        start_date: date | None = None,
        lookback_days: int | None = None,
        symbols: list[str] | None = None,
        series: str | list[str] | None = None,
    ) -> pd.DataFrame:
        filtered = frame
        if exchange is not None:
            filtered = filtered[filtered["exchange"] == exchange]
        if as_of_date is not None:
            filtered = filtered[filtered["date"] <= pd.Timestamp(as_of_date)]
        if start_date is not None:
            filtered = filtered[filtered["date"] >= pd.Timestamp(start_date)]
        if lookback_days is not None:
            if as_of_date is None:
                raise ValueError("lookback_days requires as_of_date.")
            filtered = filtered[filtered["date"] >= pd.Timestamp(as_of_date - timedelta(days=int(lookback_days)))]
        if symbols:
            filtered = filtered[filtered["symbol"].isin(symbols)]
        if series is not None:
            allowed_series = [series] if isinstance(series, str) else list(series)
            filtered = filtered[filtered["series"].isin(allowed_series)]
        return filtered

    def market_data(
        self,
        *,
        exchange: str | None = None,
        as_of_date: date | None = None,
        start_date: date | None = None,
        lookback_days: int | None = None,
        symbols: list[str] | None = None,
        columns: list[str] | None = None,
        series: str | list[str] | None = None,
    ) -> pd.DataFrame:
        frame = self._filter_pandas(
            self._market_frame(),
            exchange=exchange,
            as_of_date=as_of_date,
            start_date=start_date,
            lookback_days=lookback_days,
            symbols=symbols,
            series=series,
        )
        selected = columns if columns is not None else _MARKET_COLUMNS
        missing = [column for column in selected if column not in frame.columns]
        if missing:
            raise ValueError(f"Requested unsupported runtime market-data columns: {missing}")
        return frame.loc[:, selected].sort_values(["date", "exchange", "symbol"]).reset_index(drop=True)

    def feature_data(
        self,
        *,
        exchange: str | None = None,
        as_of_date: date | None = None,
        start_date: date | None = None,
        lookback_days: int | None = None,
        symbols: list[str] | None = None,
        columns: list[str] | None = None,
        series: str | list[str] | None = None,
    ) -> pd.DataFrame:
        frame = self._filter_pandas(
            self._feature_frame(),
            exchange=exchange,
            as_of_date=as_of_date,
            start_date=start_date,
            lookback_days=lookback_days,
            symbols=symbols,
            series=series,
        )
        selected = columns if columns is not None else _FEATURE_COLUMNS
        missing = [column for column in selected if column not in frame.columns]
        if missing:
            raise ValueError(f"Requested unsupported runtime feature columns: {missing}")
        return frame.loc[:, selected].sort_values(["date", "exchange", "symbol"]).reset_index(drop=True)

    def latest_features(
        self,
        *,
        as_of_date: date,
        exchange: str | None = None,
        symbols: list[str] | None = None,
        columns: list[str] | None = None,
        series: str | list[str] | None = None,
    ) -> pd.DataFrame:
        symbol_key = tuple(symbols) if symbols else None
        series_key = tuple([series] if isinstance(series, str) else series) if series is not None else None
        cache_key = (as_of_date, exchange, symbol_key, series_key)
        if cache_key not in self._latest_feature_cache:
            latest = self.feature_data(
                exchange=exchange,
                as_of_date=as_of_date,
                start_date=as_of_date,
                symbols=symbols,
                series=series,
            )
            self._latest_feature_cache[cache_key] = latest
        frame = self._latest_feature_cache[cache_key]
        selected = columns if columns is not None else _FEATURE_COLUMNS
        missing = [column for column in selected if column not in frame.columns]
        if missing:
            raise ValueError(f"Requested unsupported runtime latest-feature columns: {missing}")
        return frame.loc[:, selected].reset_index(drop=True)

    def trading_dates(
        self,
        *,
        exchange: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[date]:
        frame = self.market_data_frame
        if exchange is not None:
            frame = frame.filter(pl.col("exchange") == exchange)
        if start_date is not None:
            frame = frame.filter(pl.col("date") >= pl.lit(start_date))
        if end_date is not None:
            frame = frame.filter(pl.col("date") <= pl.lit(end_date))
        return frame.select(pl.col("date").unique().sort()).get_column("date").to_list()


class PrivatePickleStrategy:
    """Public wrapper that executes a private artifact against a runtime."""

    requires_historical_slice = False

    def __init__(
        self,
        *,
        strategy_id: str,
        lookback_days: int,
        artifact_path: Path,
        feature_config: dict[str, Any] | None = None,
    ) -> None:
        self.strategy_id = strategy_id
        self.lookback_days = lookback_days
        self.artifact_path = artifact_path
        self.feature_config = feature_config or {}
        self._artifact = self._load_artifact()
        self._runtime_cache_id: int | None = None
        self._runtime_cache: PrivateStrategyRuntimeV1 | None = None

    def _load_artifact(self) -> Any:
        with self.artifact_path.open("rb") as handle:
            return cloudpickle.load(handle)

    def _runtime(self, market_data: pl.DataFrame) -> PrivateStrategyRuntimeV1:
        market_id = id(market_data)
        if self._runtime_cache is None or self._runtime_cache_id != market_id:
            self._runtime_cache = PrivateStrategyRuntimeV1(market_data_frame=market_data)
            self._runtime_cache_id = market_id
        return self._runtime_cache

    def run(self, context: StrategyContext) -> pl.DataFrame:
        runtime = self._runtime(context.market_data)
        if hasattr(self._artifact, "decision"):
            result = self._artifact.decision(context.as_of_date, runtime)
        elif hasattr(self._artifact, "run"):
            result = self._artifact.run(context.as_of_date, runtime)
        else:
            raise ValueError(
                f"Private artifact for {self.strategy_id} must expose decision(as_of_date, runtime) or run(as_of_date, runtime)."
            )
        return _normalize_private_decisions(
            result,
            strategy_id=self.strategy_id,
            as_of_date=context.as_of_date,
        )


def _normalize_private_decisions(
    payload: Any,
    *,
    strategy_id: str,
    as_of_date: date,
) -> pl.DataFrame:
    if payload is None:
        return decisions_frame([])
    if isinstance(payload, pl.DataFrame):
        frame = payload
    elif isinstance(payload, pd.DataFrame):
        frame = pl.from_pandas(payload, include_index=False)
    elif isinstance(payload, list):
        frame = pl.DataFrame(payload) if payload else decisions_frame([])
    else:
        raise ValueError(f"Unsupported private decision payload type: {type(payload)!r}")

    if frame.height == 0:
        return decisions_frame([])

    required = {"exchange", "isin", "symbol", "target_weight"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Private decision payload is missing required columns: {sorted(missing)}")

    rank_values = list(range(1, frame.height + 1))
    normalized = frame.with_columns(
        pl.lit(as_of_date).alias("date") if "date" not in frame.columns else pl.col("date"),
        pl.lit(strategy_id).alias("strategy_id") if "strategy_id" not in frame.columns else pl.col("strategy_id"),
        pl.col("score").cast(pl.Float64, strict=False)
        if "score" in frame.columns
        else pl.lit(None, dtype=pl.Float64).alias("score"),
    )
    if "rank" not in normalized.columns:
        normalized = normalized.with_columns(pl.Series("rank", rank_values))
    elif normalized.get_column("rank").null_count() > 0:
        normalized = normalized.with_columns(
            pl.Series(
                "rank",
                [
                    int(value) if value is not None else fallback
                    for fallback, value in zip(rank_values, normalized.get_column("rank").to_list(), strict=False)
                ],
            )
        )
    return decisions_frame(normalized.to_dicts())
