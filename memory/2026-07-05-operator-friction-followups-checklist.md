# Operator Friction Follow-Ups Checklist

Plan: Reduce repeat operator/agent friction from the 2026-07-05 full-history and daily-run review, without contradicting `README.md`.

Status syntax:

- `- [ ]` not done
- `- [x]` done

Execution rule:

- Do planning first.
- Prefer small, surgical patches over large edits in `storage/history.py` and adjacent CLI wiring.
- Keep operator docs aligned with actual CLI behavior.
- Add or update tests alongside behavior changes where practical.

## Phase 1: Confirm Scope And Current Behavior

- [x] Read `README.md`.
- [x] Read `memory/index.md`.
- [x] Read `memory/logs/2026-07-05-operator-friction-and-followups.md`.
- [x] Inspect current CLI commands, docs, and workflow files touched by the noted friction points.
- [x] Map each friction point to one of: code fix, doc fix, workflow fix, agent/process fix, or deliberate deferral.

## Phase 2: Operator Flow Documentation

- [x] Add or revise a runbook section that gives a clear end-to-end operator flow for full history bootstrap.
- [x] Cover exact phases: fetch, manifest review, build, verify, doctor, upload readiness, upload, post-upload checks.
- [x] Document the difference between `history-verify` and `history-doctor`.
- [x] Replace shell examples that assume `source .env` under `/bin/sh` with POSIX `. ./.env` where appropriate.
- [x] Make the next step after each command explicit so the runbook is less like a command catalog.

## Phase 3: Manifest Override And Combination Tooling

- [x] Add an auditable CLI command to mark manifest rows with an explicit status and reason.
- [x] Decide command shape for manifest override, likely `history-manifest-mark`.
- [x] Add tests covering override of a single row and preservation of untouched rows.
- [x] Add a CLI command to combine per-exchange raw fetch manifests into one combined manifest.
- [x] Decide command shape for manifest combine, likely `history-manifest-combine`.
- [x] Add tests covering stable schema and deterministic combined output ordering.

## Phase 4: Bootstrap Tolerance For Known Gaps

- [x] Inspect current `history-bootstrap` failure conditions around `failed` and `rate_limited`.
- [x] Add an explicit incomplete-fetch override mode that records ignored statuses in the summary.
- [x] Ensure the override is opt-in and visible in logs/output.
- [x] Add tests for default strict mode and opt-in override mode.
- [x] Document when it is appropriate to proceed despite accepted archive gaps.

## Phase 5: Partition Manifest And Clean-Scope Safety

- [x] Reproduce or inspect the `history-build --incremental --exchange NSE` partition-manifest mismatch behavior.
- [x] Fix incremental build behavior so partition manifest stays consistent with the full output tree, or add a safe refresh command if that is the better design.
- [x] Add a command to regenerate `partition_manifest.parquet` from partition files if needed.
- [x] Revisit `history-build --clean --exchange X` semantics so single-exchange clean does not surprise operators.
- [x] If behavior remains dangerous, require a stronger confirmation flag or fail loudly.
- [x] Add tests for mixed-exchange trees and clean/incremental edge cases.

## Phase 6: Doctor Clarity Improvements

- [x] Inspect current `history-doctor` summary fields and failure messages.
- [x] Add clearer counters including `raw_usable` and `raw_skipped_existing` if supported by the data model.
- [x] Make resume-heavy fetch results less misleading in terminal summaries.
- [x] Clarify doctor fail reasons when parquet validity is fine but pipeline completeness is not.
- [x] Add or update tests for summary rendering and failure classification.

## Phase 7: R2 Upload And Staging Follow-Ups

- [x] Inspect current historical upload promotion flow and staging cleanup behavior.
- [x] Decide whether to implement staging cleanup now or document it as explicit operator debt for a later pass.
- [x] Add `R2Client.head_object` support if missing.
- [x] Add `R2Client.copy_object` support if missing.
- [x] Evaluate replacing download-size-verify-reupload promotion with metadata verification plus server-side copy.
- [x] Add a cleanup command or cleanup mode for old staging objects if implementation scope is acceptable now.
- [x] Add tests around new R2 client methods and promotion logic where possible.

## Phase 8: GitHub Actions Reliability Follow-Ups

- [x] Inspect scheduled workflow defaults for paper decision upload behavior.
- [x] Confirm the observed July 3, 2026 behavior in workflow config: scheduled run had `UPLOAD_RESULTS=false`, `market-data-refresh` succeeded for NSE and BSE, and `paper-dry-run` reported `uploaded=false` for both exchanges.
- [x] Fix scheduled-run defaults so paper decisions upload unless intentionally disabled.
- [x] Inspect current install step for dependency drift risk.
- [x] Replace floating `python -m pip install -e .` CI install with a lock-style or pinned-constraints install path.
- [x] Decide whether to use an existing lockfile, pinned constraints, or document the current tradeoff if no lock artifact exists yet.
- [x] Inspect action versions and runner warnings: on July 3, 2026 the job emitted Node 20 deprecation warnings for `actions/checkout@v4` and `actions/setup-python@v5` while running on Node 24.
- [x] Decide whether action-version updates are available now or whether the warning should be documented as hosted-action ecosystem noise outside repo control.
- [x] Update workflow docs or comments to match actual behavior.

## Phase 9: Agent/Process Guardrails

- [x] Review `AGENTS.md`, operator docs, or contribution docs for shell quoting guidance that would reduce future agent/operator friction.
- [x] Document the reliable `docker exec -i ... python -` stdin pattern if container-inline Python remains a normal workflow.
- [x] Add a note to prefer smaller surgical patches in large files when agent tooling is brittle.
- [x] Only add agent guidance that is stable and repo-relevant, not transient tooling complaints.

## Phase 10: Memory And Closeout

- [x] Add a durable decision file only if implementation changes establish a lasting policy or command contract.
- [x] Add a session log summarizing the fixes that were actually implemented.
- [x] Update `memory/index.md` with any new memory files created during implementation.
- [x] Run targeted tests and checks for touched areas.
- [x] Review `git status` and summarize remaining follow-ups that were intentionally deferred.

## Proposed First Execution Order

- [x] Start with operator docs and verify/doctor clarification because they reduce friction fastest.
- [x] Next implement manifest mark/combine tooling because it removes manual one-off scripts.
- [x] Then fix partition-manifest and clean-scope safety because they are easy to misuse.
- [x] Then improve bootstrap tolerance and doctor summaries.
- [x] Then handle workflow and R2 promotion/cleanup follow-ups based on remaining scope.

## Stop Conditions

- [ ] Stop and ask before changing any behavior that could alter R2 object layout or deletion semantics in a non-backward-compatible way.
- [ ] Stop and ask before introducing a lockfile or dependency management policy that affects local developer workflow.
- [ ] Stop and ask if the repo already contains user changes in the same files that materially conflict with these follow-ups.
- [ ] Stop and ask if the safest fix requires splitting this work into multiple commits or phases larger than originally implied.
