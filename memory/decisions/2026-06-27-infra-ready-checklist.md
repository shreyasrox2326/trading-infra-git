# Infrastructure Ready Checklist

Date: 2026-06-27

## Decisions

- Treat the codebase as operationally ready only after real R2 bucket setup and GitHub Actions secret configuration are completed.
- Use `docs/strategy-contract.md` as the human-facing contract for strategy folders and registry rows.
- Use `docs/operator-runbook.md` as the execution checklist for local backtests, R2 publishing, and GitHub Actions validation.
- Keep local-to-R2 publishing explicit through CLI commands rather than automatic side effects.
