# Local Paper Readiness

Date: 2026-06-27

## Decisions

- Reach online deployment only after local daily paper flow is runnable end to end.
- Use `registry/strategies.parquet` as the activation source for paper runs.
- Instantiate strategies from versioned local or downloaded strategy folders.
- Reuse the same paper runner for local dry-runs and the GitHub Actions daily workflow.
- Keep online paper processing CPU-only and limited to one decision date at a time.
