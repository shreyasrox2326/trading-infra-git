"""Bhavcopy format registry and date-based inspection helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from importlib.resources import files
from typing import Any

import yaml


MONTHS = {
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
class BhavcopyFormat:
    """One registered exchange bhavcopy format period."""

    exchange: str
    format_id: str
    start_date: date
    end_date: date | None
    filename_pattern: str
    url_patterns: tuple[str, ...]
    required_columns: tuple[str, ...]
    optional_columns: tuple[str, ...]
    parser: str
    known_quirks: tuple[str, ...]
    fixture_refs: tuple[str, ...]

    def filename_for(self, trade_date: date) -> str:
        return render_format_pattern(self.filename_pattern, trade_date)

    def urls_for(self, trade_date: date) -> list[str]:
        return [render_format_pattern(pattern, trade_date) for pattern in self.url_patterns]

    def inspect(self, trade_date: date) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "date": trade_date.isoformat(),
            "format_id": self.format_id,
            "filename": self.filename_for(trade_date),
            "urls": self.urls_for(trade_date),
            "parser": self.parser,
            "required_columns": list(self.required_columns),
            "optional_columns": list(self.optional_columns),
            "known_quirks": list(self.known_quirks),
            "fixture_refs": list(self.fixture_refs),
        }


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _format_from_raw(raw: dict[str, Any]) -> BhavcopyFormat:
    start_date = _parse_date(raw["from"])
    if start_date is None:
        raise ValueError(f"Format {raw.get('format_id')} is missing a start date.")
    return BhavcopyFormat(
        exchange=str(raw["exchange"]).upper(),
        format_id=str(raw["format_id"]),
        start_date=start_date,
        end_date=_parse_date(raw.get("to")),
        filename_pattern=str(raw["filename_pattern"]),
        url_patterns=tuple(str(value) for value in raw.get("url_patterns", [])),
        required_columns=tuple(str(value) for value in raw.get("required_columns", [])),
        optional_columns=tuple(str(value) for value in raw.get("optional_columns", [])),
        parser=str(raw["parser"]),
        known_quirks=tuple(str(value) for value in raw.get("known_quirks", [])),
        fixture_refs=tuple(str(value) for value in raw.get("fixture_refs", [])),
    )


@lru_cache(maxsize=1)
def load_bhavcopy_formats() -> tuple[BhavcopyFormat, ...]:
    """Load all registered bhavcopy format periods."""
    resource = files("trading_infra.data").joinpath("formats.yaml")
    payload = yaml.safe_load(resource.read_text(encoding="utf-8"))
    formats = tuple(_format_from_raw(raw) for raw in payload.get("formats", []))
    if not formats:
        raise ValueError("No bhavcopy formats are registered.")
    return tuple(sorted(formats, key=lambda item: (item.exchange, item.start_date)))


def get_bhavcopy_format(exchange: str, trade_date: date) -> BhavcopyFormat:
    """Return the registered format for an exchange/date."""
    normalized_exchange = exchange.upper()
    for item in load_bhavcopy_formats():
        if item.exchange != normalized_exchange:
            continue
        if item.start_date <= trade_date and (item.end_date is None or trade_date <= item.end_date):
            return item
    raise ValueError(f"No bhavcopy format registered for exchange={exchange} date={trade_date}.")


def inspect_bhavcopy_format(exchange: str, trade_date: date) -> dict[str, Any]:
    """Return a machine-readable format inspection for one exchange/date."""
    return get_bhavcopy_format(exchange, trade_date).inspect(trade_date)


def render_format_pattern(pattern: str, trade_date: date) -> str:
    """Render date tokens used by registered filename and URL patterns."""
    month = MONTHS[trade_date.month]
    replacements = {
        "YYYYMMDD": f"{trade_date:%Y%m%d}",
        "YYYY": f"{trade_date:%Y}",
        "YY": f"{trade_date:%y}",
        "MON": month,
        "MM": f"{trade_date:%m}",
        "DD": f"{trade_date:%d}",
    }
    rendered = pattern
    for token, value in replacements.items():
        rendered = rendered.replace(token, value)
    return rendered
