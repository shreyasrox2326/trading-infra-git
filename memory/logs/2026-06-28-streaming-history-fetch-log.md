# Streaming History Fetch Log

Date: 2026-06-28

Context:

- `history-fetch` previously wrote its CSV status log only after the full fetch completed.
- Long full-history downloads need a log that can be followed with `tail -f` while the command is still running.

Change:

- `fetch_bhavcopy_archives` now accepts an `on_result` callback.
- `history-fetch` uses that callback to append and flush one log line as each date completes.
- Parallel fetch still returns results in date order for callers, while log lines reflect completion order.

Verification:

- Unit tests cover the per-result callback.
- CLI smoke for NSE `2015-12-31` wrote the fetch log line during the command path.
- Full test suite passed.
