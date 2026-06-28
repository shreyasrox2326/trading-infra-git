# NSE Rate Limit Handling

Date: 2026-06-28

Context:

- NSE legacy archive requests started returning HTTP 403 for all tested dates after repeated probes.
- A full-range fetch should not continue producing thousands of ordinary `failed` rows when the exchange is rejecting the client.

Change:

- NSE/Bhavcopy fetch now reports all-host HTTP 403 as `rate_limited`.
- `history-fetch` exits nonzero when any `rate_limited` rows occur.
- `history-fetch` exposes `--retry-sleep-seconds` for long backoff between retry attempts.
- Operator runbook now recommends conservative NSE historical fetch settings: `--workers 1 --retries 5 --retry-sleep-seconds 60`.

Operator note:

- If the log shows repeated `rate_limited` rows, stop the run and wait before resuming.
- Existing downloaded ZIPs are reused by default, so a later resumed run does not need to redownload successful dates.
