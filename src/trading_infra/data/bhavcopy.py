"""Fetch and normalize exchange equity bhavcopy files."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zipfile import ZipFile

import polars as pl
from tqdm import tqdm

from trading_infra.data.market_data import DAILY_STOCK_DATA_COLUMNS, DAILY_STOCK_DATA_SCHEMA

NSE_ARCHIVE_BASE_URL = "https://archives.nseindia.com/content/historical/EQUITIES"
NSE_UDIFF_BASE_URL = "https://archives.nseindia.com/content/cm"
NSE_UDIFF_START_DATE = date(2024, 7, 8)
BSE_ARCHIVE_BASE_URL = "https://www.bseindia.com/download/BhavCopy/Equity"
BSE_UDIFF_START_DATE = date(2024, 7, 30)
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
NSE_MONTH_NUMBERS = {month: f"{number:02d}" for number, month in NSE_MONTHS.items()}


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


def _normalize_exchange(exchange: str) -> str:
    normalized = exchange.upper()
    if normalized not in {"NSE", "BSE"}:
        raise ValueError(f"Unsupported bhavcopy exchange: {exchange}")
    return normalized


def bhavcopy_archive_name(trade_date: date, *, exchange: str = "NSE") -> str:
    """Return the canonical exchange equity bhavcopy filename."""
    exchange = _normalize_exchange(exchange)
    if exchange == "NSE":
        if trade_date >= NSE_UDIFF_START_DATE:
            return f"BhavCopy_NSE_CM_0_0_0_{trade_date:%Y%m%d}_F_0000.csv.zip"
        month = NSE_MONTHS[trade_date.month]
        return f"cm{trade_date:%d}{month}{trade_date:%Y}bhav.csv.zip"

    if trade_date >= BSE_UDIFF_START_DATE:
        return f"BhavCopy_BSE_CM_0_0_0_{trade_date:%Y%m%d}_F_0000.CSV"
    return f"EQ{trade_date:%d%m%y}_CSV.ZIP"


def bhavcopy_archive_url(trade_date: date, *, exchange: str = "NSE") -> str:
    """Return the canonical exchange historical bhavcopy URL."""
    exchange = _normalize_exchange(exchange)
    if exchange == "NSE" and trade_date >= NSE_UDIFF_START_DATE:
        filename = bhavcopy_archive_name(trade_date)
        return f"{NSE_UDIFF_BASE_URL}/{filename}"
    if exchange == "NSE":
        month = NSE_MONTHS[trade_date.month]
        filename = bhavcopy_archive_name(trade_date)
        return f"{NSE_ARCHIVE_BASE_URL}/{trade_date:%Y}/{month}/{filename}"

    filename = bhavcopy_archive_name(trade_date, exchange=exchange)
    return f"{BSE_ARCHIVE_BASE_URL}/{filename}"


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
    exchange: str = "NSE",
    overwrite: bool = False,
    timeout_seconds: int = 30,
    retries: int = 3,
    retry_sleep_seconds: float = 1.0,
) -> BhavcopyFetchResult:
    """Fetch one exchange equity bhavcopy archive into local operator state."""
    exchange = _normalize_exchange(exchange)
    output_root = Path(output_path)
    output_root.mkdir(parents=True, exist_ok=True)
    target = output_root / bhavcopy_archive_name(trade_date, exchange=exchange)
    if target.exists() and not overwrite:
        return BhavcopyFetchResult(trade_date, "skipped_existing", target)

    request = Request(
        bhavcopy_archive_url(trade_date, exchange=exchange),
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/zip,text/csv,*/*",
        },
    )
    attempts = max(1, retries + 1)
    last_error = ""
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                payload = response.read()
            target.write_bytes(payload)
            return BhavcopyFetchResult(trade_date, "downloaded", target)
        except HTTPError as exc:
            if exc.code == 404:
                return BhavcopyFetchResult(trade_date, "not_available", None, str(exc))
            last_error = str(exc)
        except URLError as exc:
            last_error = str(exc)

        if attempt < attempts:
            sleep(retry_sleep_seconds * attempt)

    return BhavcopyFetchResult(trade_date, "failed", None, last_error)


def fetch_bhavcopy_archives(
    *,
    start_date: date,
    end_date: date,
    output_path: str | Path,
    exchange: str = "NSE",
    overwrite: bool = False,
    workers: int = 1,
    retries: int = 3,
    timeout_seconds: int = 30,
    show_progress: bool = False,
) -> list[BhavcopyFetchResult]:
    """Fetch exchange equity bhavcopy archives for all weekdays in a date range."""
    days = trading_weekdays(start_date, end_date)

    def fetch_one(trade_date: date) -> BhavcopyFetchResult:
        return fetch_bhavcopy_archive(
            trade_date,
            output_path=output_path,
            exchange=exchange,
            overwrite=overwrite,
            timeout_seconds=timeout_seconds,
            retries=retries,
        )

    if workers <= 1:
        iterable = tqdm(days, desc=f"{exchange.upper()} bhavcopy", unit="file") if show_progress else days
        return [fetch_one(trade_date) for trade_date in iterable]

    results_by_date: dict[date, BhavcopyFetchResult] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_one, trade_date): trade_date for trade_date in days}
        completed = as_completed(futures)
        if show_progress:
            completed = tqdm(completed, total=len(futures), desc=f"{exchange.upper()} bhavcopy", unit="file")
        for future in completed:
            trade_date = futures[future]
            results_by_date[trade_date] = future.result()

    return [results_by_date[trade_date] for trade_date in days]


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

    return frame.rename(
        {
            column: column.strip().upper().replace(" ", "").replace("_", "")
            for column in frame.columns
        }
    )


def _column_expr(frame: pl.DataFrame, *names: str, default: pl.Expr | None = None) -> pl.Expr:
    for name in names:
        if name in frame.columns:
            return pl.col(name)
    if default is not None:
        return default
    raise ValueError(f"Bhavcopy input is missing required column; tried: {names}")


def _parse_bhavcopy_date(expr: pl.Expr) -> pl.Expr:
    """Parse NSE bhavcopy date strings with two- or four-digit years."""
    text = expr.cast(pl.Utf8)
    iso_date = text.str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    day = text.str.slice(0, 2)
    month = text.str.slice(3, 3).str.to_uppercase().replace(NSE_MONTH_NUMBERS)
    raw_year = text.str.slice(7)
    year = pl.when(raw_year.str.len_chars() == 2).then(pl.lit("20") + raw_year).otherwise(raw_year)
    legacy_date = (year + pl.lit("-") + month + pl.lit("-") + day).str.strptime(
        pl.Date, "%Y-%m-%d", strict=False
    )
    return pl.coalesce(iso_date, legacy_date)


def _vwap_expr(turnover: pl.Expr, volume: pl.Expr, fallback: pl.Expr | None = None) -> pl.Expr:
    calculated = turnover.cast(pl.Float64) / volume.cast(pl.Float64)
    if fallback is None:
        return calculated
    return pl.coalesce(fallback.cast(pl.Float64), calculated)


def normalize_bhavcopy_frame(frame: pl.DataFrame, *, exchange: str) -> pl.DataFrame:
    """Normalize one raw exchange bhavcopy frame into canonical daily stock data."""
    exchange = _normalize_exchange(exchange)

    timestamp = _column_expr(frame, "TIMESTAMP", "DATE", "TRADEDATE", "TRADDT", "BIZDT")
    open_price = _column_expr(frame, "OPEN", "OPNPRIC")
    high = _column_expr(frame, "HIGH", "HGHPRIC")
    low = _column_expr(frame, "LOW", "LWPRIC")
    close = _column_expr(frame, "CLOSE", "CLSPRIC")
    prev_close = _column_expr(frame, "PREVCLOSE", "PRVSCLSGPRIC")
    volume = _column_expr(frame, "TOTTRDQTY", "TTLTRDQNTY", "NOOFSHRS", "VOLUME", "TTLTRADGVOL")
    turnover = _column_expr(frame, "TOTTRDVAL", "NETTURNOV", "TURNOVER", "TTLTRFVAL")
    isin = _column_expr(frame, "ISIN", "SC CODE", "SCCODE", "FININSTRMID")
    symbol = _column_expr(frame, "SYMBOL", "TCKRSYMB", "SCCODE", "SCNAME", "FININSTRMNM")
    series = _column_expr(frame, "SERIES", "SCTYSRS", "SCGROUP", default=pl.lit("EQ"))
    vwap = _vwap_expr(turnover, volume, _column_expr(frame, "VWAP", "AVGPERIC", default=pl.lit(None)))

    return frame.select(
        _parse_bhavcopy_date(timestamp).alias("date"),
        pl.lit(exchange).alias("exchange"),
        isin.cast(pl.Utf8).alias("isin"),
        symbol.cast(pl.Utf8).alias("symbol"),
        series.cast(pl.Utf8).alias("series"),
        open_price.cast(pl.Float64).alias("open"),
        high.cast(pl.Float64).alias("high"),
        low.cast(pl.Float64).alias("low"),
        close.cast(pl.Float64).alias("close"),
        prev_close.cast(pl.Float64).alias("prev_close"),
        vwap.alias("vwap"),
        volume.cast(pl.Int64).alias("volume"),
        turnover.cast(pl.Float64).alias("turnover"),
        _column_expr(
            frame,
            "TOTALTRADES",
            "NOOFTRADES",
            "NOTRADES",
            "TRADES",
            "TTLNBOFTXSEXCTD",
            default=pl.lit(None),
        )
        .cast(pl.Int64)
        .alias("trades"),
        _column_expr(frame, "DELIVQTY", "DELIVERABLEQTY", default=pl.lit(None)).cast(pl.Int64).alias("deliverable_qty"),
        _column_expr(frame, "DELIVPER", "DELIVERYPCT", default=pl.lit(None)).cast(pl.Float64).alias("delivery_pct"),
        open_price.cast(pl.Float64).alias("adj_open"),
        high.cast(pl.Float64).alias("adj_high"),
        low.cast(pl.Float64).alias("adj_low"),
        close.cast(pl.Float64).alias("adj_close"),
        pl.lit(1.0).alias("adj_factor"),
    )


def normalize_bhavcopy_file(path: str | Path, *, exchange: str) -> pl.DataFrame:
    """Normalize one raw exchange bhavcopy file into canonical daily stock data."""
    return normalize_bhavcopy_frame(_read_bhavcopy_file(Path(path)), exchange=exchange)


def normalize_bhavcopy_inputs(input_path: str | Path, *, exchange: str) -> pl.DataFrame:
    """Normalize raw exchange bhavcopy files into canonical daily stock data."""
    normalized_frames = [
        normalize_bhavcopy_file(path, exchange=exchange)
        for path in _resolve_bhavcopy_inputs(input_path)
    ]

    combined = pl.concat(normalized_frames).select(
        [pl.col(column).cast(DAILY_STOCK_DATA_SCHEMA[column]) for column in DAILY_STOCK_DATA_COLUMNS]
    ).unique(maintain_order=True)
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
