# R2 Budget Thresholds

Date: 2026-06-29

Decision:

- Default warning thresholds:
  - storage > 8 GiB
  - Class A operations > 800,000/month
  - Class B operations > 8,000,000/month
- Default failure thresholds:
  - storage > 9.5 GiB
  - Class A operations > 950,000/month
  - Class B operations > 9,500,000/month

Implementation:

- `r2-usage` reports object inventory storage and object count from S3-compatible listing.
- `r2-budget-check` applies the thresholds and writes timestamped snapshots.
- Class A/Class B operation fields are present and nullable until Cloudflare analytics integration is configured.
- `history-upload` runs a budget check before bulk upload.
