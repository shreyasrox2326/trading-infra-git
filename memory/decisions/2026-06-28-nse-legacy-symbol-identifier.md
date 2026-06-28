# NSE Legacy Symbol Identifier

Date: 2026-06-28

Decision:

- For NSE legacy bhavcopy files that do not contain `ISIN`, populate canonical `isin` with the source `SYMBOL`.

Reason:

- Available NSE legacy bhavcopies from 1994 through at least 2011 contain `SYMBOL`, `SERIES`, OHLC, volume, turnover, and timestamp, but do not include `ISIN`.
- The README canonical schema requires a non-null `isin` column and uses `date + exchange + isin + series` as the key.
- Using `SYMBOL` preserves a source-local identifier and allows full-history canonical assembly without silently dropping older rows.

Implication:

- For old NSE rows before `ISIN` appears in the source file, canonical `isin` is an exchange-symbol identifier, not a true ISIN.
- Strategy logic should treat `isin` as the canonical security identifier column, but audits and documentation must remember this historical-source limitation.
