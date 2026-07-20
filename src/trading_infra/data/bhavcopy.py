"""Fetch exchange equity bhavcopy files and expose bhavcopy compatibility imports."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from time import sleep
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from tqdm import tqdm

from trading_infra.data.formats import get_bhavcopy_format
from trading_infra.data.bhavcopy_normalize import (
    BhavcopyIngestSummary,
    NonBhavcopyFileError,
    _infer_bhavcopy_format_from_filename,
    _resolve_bhavcopy_inputs,
    normalize_bhavcopy_file,
    normalize_bhavcopy_frame,
    normalize_bhavcopy_inputs,
    summarize_canonical_bhavcopy,
    validate_canonical_bhavcopy_frame,
    write_canonical_bhavcopy_parquet,
)

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
    network_requested: bool = False


class BhavcopyRateLimitError(RuntimeError):
    """Raised when rate-limited fetch results exceed the configured guardrail."""

    def __init__(self, message: str, results: list[BhavcopyFetchResult]) -> None:
        super().__init__(message)
        self.results = results


def _normalize_exchange(exchange: str) -> str:
    normalized = exchange.upper()
    if normalized not in {"NSE", "BSE"}:
        raise ValueError(f"Unsupported bhavcopy exchange: {exchange}")
    return normalized


def bhavcopy_archive_name(trade_date: date, *, exchange: str = "NSE") -> str:
    """Return the canonical exchange equity bhavcopy filename."""
    exchange = _normalize_exchange(exchange)
    return get_bhavcopy_format(exchange, trade_date).filename_for(trade_date)


def bhavcopy_archive_url(trade_date: date, *, exchange: str = "NSE") -> str:
    """Return the canonical exchange historical bhavcopy URL."""
    return bhavcopy_archive_urls(trade_date, exchange=exchange)[0]


def bhavcopy_archive_urls(trade_date: date, *, exchange: str = "NSE") -> list[str]:
    """Return official exchange bhavcopy URLs to try for one trade date."""
    exchange = _normalize_exchange(exchange)
    return get_bhavcopy_format(exchange, trade_date).urls_for(trade_date)


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


def _looks_like_html(payload: bytes) -> bool:
    stripped = payload.lstrip()[:64].lower()
    return stripped.startswith(b"<!doctype html") or stripped.startswith(b"<html")


def fetch_bhavcopy_archive(
    trade_date: date,
    *,
    output_path: str | Path,
    exchange: str = "NSE",
    overwrite: bool = False,
    timeout_seconds: int = 30,
    retries: int = 1,
    retry_sleep_seconds: float = 1.0,
) -> BhavcopyFetchResult:
    """Fetch one exchange equity bhavcopy archive into local operator state."""
    exchange = _normalize_exchange(exchange)
    output_root = Path(output_path)
    output_root.mkdir(parents=True, exist_ok=True)
    target = output_root / bhavcopy_archive_name(trade_date, exchange=exchange)
    if target.exists() and not overwrite:
        if target.stat().st_size == 0:
            target.unlink()
        else:
            return BhavcopyFetchResult(trade_date, "skipped_existing", target)

    attempts = max(1, retries + 1)
    last_error = ""
    saw_forbidden = False
    for attempt in range(1, attempts + 1):
        for url in bhavcopy_archive_urls(trade_date, exchange=exchange):
            request = Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/zip,text/csv,*/*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://www.nseindia.com/",
                },
            )
            try:
                with urlopen(request, timeout=timeout_seconds) as response:
                    payload = response.read()
                if not payload:
                    last_error = "Empty response instead of bhavcopy data."
                    continue
                if _looks_like_html(payload):
                    last_error = "HTML response instead of bhavcopy data."
                    continue
                target.write_bytes(payload)
                return BhavcopyFetchResult(trade_date, "downloaded", target, url, network_requested=True)
            except HTTPError as exc:
                if exc.code == 404:
                    last_error = str(exc)
                    continue
                if exc.code == 403:
                    saw_forbidden = True
                last_error = str(exc)
            except URLError as exc:
                last_error = str(exc)

        if attempt < attempts:
            sleep(retry_sleep_seconds * attempt)

    if last_error.startswith("HTTP Error 404") or last_error == "HTML response instead of bhavcopy data.":
        return BhavcopyFetchResult(trade_date, "not_available", None, last_error, network_requested=True)
    if saw_forbidden:
        return BhavcopyFetchResult(trade_date, "rate_limited", None, last_error, network_requested=True)
    return BhavcopyFetchResult(trade_date, "failed", None, last_error, network_requested=True)


def fetch_bhavcopy_archives(
    *,
    start_date: date,
    end_date: date,
    output_path: str | Path,
    exchange: str = "NSE",
    overwrite: bool = False,
    workers: int = 1,
    retries: int = 1,
    retry_sleep_seconds: float = 1.0,
    request_sleep_seconds: float = 0.5,
    timeout_seconds: int = 30,
    show_progress: bool = False,
    on_result: Callable[[BhavcopyFetchResult], None] | None = None,
    requested_dates: list[date] | None = None,
    fail_fast_rate_limit_ratio: float | None = None,
) -> list[BhavcopyFetchResult]:
    """Fetch exchange equity bhavcopy archives for all weekdays in a date range."""
    days = requested_dates if requested_dates is not None else trading_weekdays(start_date, end_date)
    if fail_fast_rate_limit_ratio is not None and not 0 <= fail_fast_rate_limit_ratio <= 1:
        raise ValueError("fail_fast_rate_limit_ratio must be between 0 and 1.")

    def maybe_abort_for_rate_limits(results: list[BhavcopyFetchResult]) -> None:
        if fail_fast_rate_limit_ratio is None or not results:
            return
        rate_limited = sum(1 for result in results if result.status == "rate_limited")
        ratio = rate_limited / len(results)
        if ratio > fail_fast_rate_limit_ratio:
            raise BhavcopyRateLimitError(
                f"rate_limited ratio {ratio:.3f} exceeded threshold {fail_fast_rate_limit_ratio:.3f}",
                results,
            )

    def fetch_one(trade_date: date) -> BhavcopyFetchResult:
        return fetch_bhavcopy_archive(
            trade_date,
            output_path=output_path,
            exchange=exchange,
            overwrite=overwrite,
            timeout_seconds=timeout_seconds,
            retries=retries,
            retry_sleep_seconds=retry_sleep_seconds,
        )

    if workers <= 1:
        iterable = tqdm(days, desc=f"{exchange.upper()} bhavcopy", unit="file") if show_progress else days
        results = []
        for trade_date in iterable:
            result = fetch_one(trade_date)
            if on_result is not None:
                on_result(result)
            results.append(result)
            maybe_abort_for_rate_limits(results)
            if request_sleep_seconds > 0 and result.network_requested:
                sleep(request_sleep_seconds)
        return results

    results_by_date: dict[date, BhavcopyFetchResult] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_one, trade_date): trade_date for trade_date in days}
        completed = as_completed(futures)
        if show_progress:
            completed = tqdm(completed, total=len(futures), desc=f"{exchange.upper()} bhavcopy", unit="file")
        for future in completed:
            trade_date = futures[future]
            result = future.result()
            if on_result is not None:
                on_result(result)
            results_by_date[trade_date] = result
            maybe_abort_for_rate_limits(list(results_by_date.values()))
            if request_sleep_seconds > 0 and result.network_requested:
                sleep(request_sleep_seconds)

    return [results_by_date[trade_date] for trade_date in days]

