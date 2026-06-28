# NSE Pre-2016 Fetch Fix

Date: 2026-06-28

Context:

- Local full-history fetch showed NSE dates from 1994 through 2015 failing with `HTTP Error 403: Forbidden`.
- The official NSE historical archive files are still reachable for tested pre-2016 dates when using browser-compatible headers.

Change:

- NSE legacy fetch now sends `Accept-Language` and NSE `Referer` headers.
- NSE legacy fetch now tries both official archive hosts:
  - `https://archives.nseindia.com/content/historical/EQUITIES`
  - `https://nsearchives.nseindia.com/content/historical/EQUITIES`
- Fetch logs record the URL used in the result message for downloaded files.

Verification:

- `2015-12-31` downloaded from the official NSE archive and normalized to the canonical schema.
- The one-day canonical partition had 1,606 rows, no duplicate keys, no invalid OHLC rows, and passed `history-verify`.
- Early archive probe found `1994-01-03` unavailable by direct file URL, while `1994-11-03`, `1995-01-02`, `2000-01-03`, `2005-01-03`, `2010-01-04`, and `2015-12-31` were reachable.

Operator note:

- Re-run NSE `history-fetch` for the pre-2016 range with `--overwrite` only if existing failed/missing raw files need replacement. Existing successfully downloaded ZIPs can be reused.
