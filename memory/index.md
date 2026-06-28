# Memory Index

This file is the navigation map for repo memory.

`AGENTS.md` is stable agent instruction. `README.md` remains the current source of truth for project architecture. `memory/index.md` is the memory map. `memory/decisions/` contains durable decisions. `memory/logs/` contains timestamped session notes.

## Current Source-Of-Truth Decisions

- [Infrastructure baseline](decisions/2026-06-27-infra-baseline.md)
- [Tech stack](decisions/2026-06-27-tech-stack.md)
- [Local paper readiness](decisions/2026-06-27-local-paper-readiness.md)
- [Infrastructure ready checklist](decisions/2026-06-27-infra-ready-checklist.md)
- [Market data publishing](decisions/2026-06-28-market-data-publishing.md)
- [Progress tracking](decisions/2026-06-28-progress-tracking.md)

## Session Logs

- [Initial session](logs/2026-06-27-initial-session.md)
- [Paper readiness buildout](logs/2026-06-27-paper-readiness-buildout.md)
- [Infrastructure ready implementation](logs/2026-06-27-infra-ready-implementation.md)
- [Market data publishing](logs/2026-06-28-market-data-publishing.md)
- [README gap audit](logs/2026-06-28-readme-gap-audit.md)
- [Historical NSE R2 load](logs/2026-06-28-historical-nse-r2-load.md)
- [Full README alignment implementation](logs/2026-06-28-full-readme-alignment-implementation.md)
- [History fetch optimization](logs/2026-06-28-history-fetch-optimization.md)
- [History build optimization](logs/2026-06-28-history-build-optimization.md)
- [BSE ragged CSV fix](logs/2026-06-28-bse-ragged-csv-fix.md)
- [Strict bhavcopy row quality](logs/2026-06-28-strict-bhavcopy-row-quality.md)
- [Partition-first history build](logs/2026-06-28-partition-first-history-build.md)
- [NSE pre-2016 fetch fix](logs/2026-06-28-nse-pre2016-fetch-fix.md)

## Open Questions

- Corporate-action-adjusted prices still need a source decision; current historical load uses identity adjustment.

## Active Plan Checklists

- [Historical NSE data plan checklist](2026-06-28-historical-nse-plan-checklist.md)
- [Full README alignment checklist](2026-06-28-full-readme-alignment-checklist.md)

## Future Update Rules

- Read `README.md` before changing agent memory.
- Add durable decisions to `memory/decisions/`.
- Add timestamped notes to `memory/logs/`.
- Keep entries short and practical.
- Update this index whenever new memory files are added.
- Do not use memory files to replace or contradict `README.md`.
