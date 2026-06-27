# Historical NSE R2 Load

Date: 2026-06-28

Summary:

- Loaded NSE daily market history into R2 canonical monthly parquet partitions.
- Source files are local ignored operator state under `data/raw/bhavcopy/`.
- Canonical parquet is local ignored operator state at `data/import/daily_stock_data.parquet`.
- Verified R2 market data: 126 month partitions, 5,786,247 rows, date range 2016-01-01 to 2026-06-25, zero duplicate `date + exchange + isin + series` keys.
- Legacy NSE bhavcopy files are used through 2024-07-05; UDiFF common bhavcopy files are used from 2024-07-08 onward.
- Identity adjustment is still used: `adj_* = raw OHLC`, `adj_factor = 1.0`.
- Delivery fields remain null because the selected bhavcopy source does not include delivery quantities/percentages.
- Uploaded validated backtest decisions for `top_n_adj_close_v1`: 12,930 rows, 2016-01-01 to 2026-06-25.
- Deleted stale R2 paper sample decisions for `top_n_adj_close_v1`; dry-run paper now returns only 5 current-day rows for 2026-06-25 and does not upload results.

Remaining gaps:

- GitHub Actions still needs a daily market-data refresh step before paper execution.
- Corporate-action adjustment source is not integrated.
- ML strategy runtime/model loading is not implemented.
