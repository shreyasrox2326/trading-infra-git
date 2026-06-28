# NSE Fetch Throttle Tuning

Date: 2026-06-28

Context:

- The NseIndiaApi README documents NSE throttle expectations for bulk downloads: keep request rates low and add an extra short sleep for high-volume report downloads.
- Our previous `history-fetch` retry sleep only applied after errors; it did not slow successful date-to-date fetches.

Change:

- `history-fetch` now defaults to `--workers 1`.
- Added `--request-sleep-seconds` to sleep between completed fetch requests.
- The NSE bootstrap runbook now uses `--request-sleep-seconds 1` plus low concurrency.
- BSE can still be run faster by explicitly passing higher `--workers`.

Operator note:

- Use `--request-sleep-seconds` for normal pacing.
- Use `--retry-sleep-seconds` for backoff after failed/rate-limited attempts.
