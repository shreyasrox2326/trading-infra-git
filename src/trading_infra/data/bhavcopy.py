"""Fetch and normalize NSE equity bhavcopy files."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zipfile import ZipFile

import polars as pl

from trading_infra.data.market_data import DAILY_STOCK_DATA_COLUMNS, DAILY_STOCK_DATA_SCHEMA

NSE_ARCHIVE_BASE_URL = "https://archives.nseindia.com/content/historical/EQUITIES"
NSE_MONTHS = {
    1: "JAN",
    2: "FEB",
    3: "MAR",
    4: "APR",
    5: "MAY",
    6: "JUN",
    7: "JUL",
    8: "AUG",
    9: "SEP",
    10: "OCT",
    11: "NOV",
    12: "DEC",
}


@dataclass(frozen=True)
class BhavcopyFetchResult:
    """Outcome for one requested bhavcopy date."""

    requested_date: date
    status: str
    path: Path | None
    message: str = ""


@dataclass(frozen=True)
class BhavcopyIngestSummary:
    """Summary for canonical bhavcopy ingestion output."""

    rows: int
    date_min: date | None
    date_max: date | None
    exchanges: list[str]
    symbols: int
    missing_deliverable_qty: int
    missing_delivery_pct: int


def bhavcopy_archive_name(trade_date: date) -> str:
    """Return the canonical NSE equity bhavcopy archive filename."""
    month = NSE_MONTHS[trade_date.month]
    return f"cm{trade_date:%d}{month}{trade_date:%Y}bhav.csv.zip"


def bhavcopy_archive_url(trade_date: date) -> str:
    """Return the canonical NSE historical bhavcopy archive URL."""
    month = NSE_MONTHS[trade_date.month]
    filename = bhavcopy_archive_name(trade_date)
    return f"{NSE_ARCHIVE_BASE_URL}/{trade_date:%Y}/{month}/{filename}"


def trading_weekdays(start_date: date, end_date: date) -> list[date]:
    """Return weekday dates in an inclusive range."""
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date.")

    days: list[date] = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def fetch_bhavcopy_archive(
    trade_date: date,
    *,
    output_path: str | Path,
    overwrite: bool = False,
    timeout_seconds: int = 30,
) -> BhavcopyFetchResult:
    """Fetch one NSE equity bhavcopy archive into local operator state."""
    output_root = Path(output_path)
    output_root.mkdir(parents=True, exist_ok=True)
    target = output_root / bhavcopy_archive_name(trade_date)
    if target.exists() and not overwrite:
        return BhavcopyFetchResult(trade_date, "skipped_existing", target)

    request = Request(
        bhavcopy_archive_url(trade_date),
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/zip,text/csv,*/*",
        },
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            payload = response.read()
    except HTTPError as exc:
        if exc.code == 404:
            return BhavcopyFetchResult(trade_date, "not_available", None, str(exc))
        return BhavcopyFetchResult(trade_date, "failed", None, str(exc))
    except URLError as exc:
        return BhavcopyFetchResult(trade_date, "failed", None, str(exc))

    target.write_bytes(payload)
    return BhavcopyFetchResult(trade_date, "downloaded", target)


def fetch_bhavcopy_archives(
    *,
    start_date: date,
    end_date: date,
    output_path: str | Path,
    overwrite: bool = False,
) -> list[BhavcopyFetchResult]:
    """Fetch NSE equity bhavcopy archives for all weekdays in a date range."""
    return [
        fetch_bhavcopy_archive(trade_date, output_path=output_path, overwrite=overwrite)
        for trade_date in trading_weekdays(start_date, end_date)
    ]


def _resolve_bhavcopy_inputs(input_path: str | Path) -> list[Path]:
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Bhavcopy input path not found: {path}")
    if path.is_dir():
        files = sorted(
            candidate
            for candidate in path.rglob("*")
            if candidate.suffix.lower() in {".csv", ".zip"}
        )
    else:
        files = [path]
    if not files:
        raise FileNotFoundError(f"No bhavcopy CSV or ZIP files found under: {path}")
    return files


def _read_bhavcopy_file(path: Path) -> pl.DataFrame:
    if path.suffix.lower() == ".zip":
        with ZipFile(path) as archive:
            csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if not csv_names:
                raise ValueError(f"Bhavcopy archive has no CSV file: {path}")
            payload = archive.read(csv_names[0])
        frame = pl.read_csv(BytesIO(payload), ignore_errors=False)
    else:
        frame = pl.read_csv(path, ignore_errors=False)

    return frame.rename({column: column.strip().upper().replace(" ", "") for column in frame.columns})


def _column_expr(frame: pl.DataFrame, *names: str, default: pl.Expr | None = None) -> pl.Expr:
    for name in names:
        if name in frame.columns:
            return pl.col(name)
    if default is not None:
        return default
    raise ValueError(f"Bhavcopy input is missing required column; tried: {names}")


def normalize_bhavcopy_inputs(input_path: str | Path, *, exchange: str) -> pl.DataFrame:
    """Normalize raw NSE bhavcopy files into canonical daily stock data."""
    frames = [_read_bhavcopy_file(path) for path in _resolve_bhavcopy_inputs(input_path)]
    normalized_frames: list[pl.DataFrame] = []

    for frame in frames:
        timestamp = _column_expr(frame, "TIMESTAMP", "DATE", "TRADEDATE")
        open_price = _column_expr(frame, "OPEN")
        high = _column_expr(frame, "HIGH")
        low = _column_expr(frame, "LOW")
        close = _column_expr(frame, "CLOSE")
        volume = _column_expr(frame, "TOTTRDQTY", "TTL_TRD_QNTY", "VOLUME")
        turnover = _column_expr(frame, "TOTTRDVAL", "TURNOVER")

        normalized = frame.select(
            timestamp.cast(pl.Utf8).str.strptime(pl.Date, "%d-%b-%Y", strict=False).alias("date"),
            pl.lit(exchange).alias("exchange"),
            _column_expr(frame, "ISIN").cast(pl.Utf8).alias("isin"),
            _column_expr(frame, "SYMBOL").cast(pl.Utf8).alias("symbol"),
            _column_expr(frame, "SERIES", default=pl.lit("EQ")).cast(pl.Utf8).alias("series"),
            open_price.cast(pl.Float64).alias("open"),
            high.cast(pl.Float64).alias("high"),
            low.cast(pl.Float64).alias("low"),
            close.cast(pl.Float64).alias("close"),
            _column_expr(frame, "PREVCLOSE", "PREV_CLOSE").cast(pl.Float64).alias("prev_close"),
            (turnover.cast(pl.Float64) / volume.cast(pl.Float64)).alias("vwap"),
            volume.cast(pl.Int64).alias("volume"),
            turnover.cast(pl.Float64).alias("turnover"),
            _column_expr(frame, "TOTALTRADES", "TRADES", default=pl.lit(None)).cast(pl.Int64).alias("trades"),
            _column_expr(frame, "DELIV_QTY", "DELIVERABLEQTY", default=pl.lit(None)).cast(pl.Int64).alias("deliverable_qty"),
            _column_expr(frame, "DELIV_PER", "DELIVERYPCT", default=pl.lit(None)).cast(pl.Float64).alias("delivery_pct"),
            open_price.cast(pl.Float64).alias("adj_open"),
            high.cast(pl.Float64).alias("adj_high"),
            low.cast(pl.Float64).alias("adj_low"),
            close.cast(pl.Float64).alias("adj_close"),
            pl.lit(1.0).alias("adj_factor"),
        )
        normalized_frames.append(normalized)

    combined = pl.concat(normalized_frames).select(
        [pl.col(column).cast(DAILY_STOCK_DATA_SCHEMA[column]) for column in DAILY_STOCK_DATA_COLUMNS]
    )
    return validate_canonical_bhavcopy_frame(combined)


def validate_canonical_bhavcopy_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """Validate canonical bhavcopy output before writing parquet."""
    missing = [column for column in DAILY_STOCK_DATA_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Canonical bhavcopy output is missing columns: {missing}")

    required_non_null = ("date", "exchange", "isin", "symbol", "series", "close", "adj_close", "volume")
    null_columns = [column for column in required_non_null if frame.select(pl.col(column).is_null().any()).item()]
    if null_columns:
        raise ValueError(f"Canonical bhavcopy output has null values in required columns: {null_columns}")

    duplicate_count = frame.select(
        pl.struct(["date", "exchange", "isin", "series"]).is_duplicated().sum()
    ).item()
    if duplicate_count:
        raise ValueError("Canonical bhavcopy output contains duplicate date/exchange/isin/series rows.")

    return frame.select(DAILY_STOCK_DATA_COLUMNS).sort(["date", "exchange", "symbol", "isin", "series"])


def write_canonical_bhavcopy_parquet(
    *,
    input_path: str | Path,
    output_path: str | Path,
    exchange: str,
) -> tuple[Path, BhavcopyIngestSummary]:
    """Normalize bhavcopy files and write canonical parquet."""
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    frame = normalize_bhavcopy_inputs(input_path, exchange=exchange)
    frame.write_parquet(output)
    return output, summarize_canonical_bhavcopy(frame)


def summarize_canonical_bhavcopy(frame: pl.DataFrame) -> BhavcopyIngestSummary:
    """Build a concise summary for canonical bhavcopy output."""
    return BhavcopyIngestSummary(
        rows=frame.height,
        date_min=frame.get_column("date").min() if frame.height else None,
        date_max=frame.get_column("date").max() if frame.height else None,
        exchanges=frame.get_column("exchange").unique().sort().to_list() if frame.height else [],
        symbols=frame.get_column("symbol").n_unique() if frame.height else 0,
        missing_deliverable_qty=frame.get_column("deliverable_qty").null_count(),
        missing_delivery_pct=frame.get_column("delivery_pct").null_count(),
    )
