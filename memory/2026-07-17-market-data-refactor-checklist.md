# Market Data Refactor Checklist

Date: 2026-07-17

Goal: make the market-data codebase modern, maintainable, understandable, and aligned with the actual workflows, while keeping behavior and operator commands stable.

## Phase 0: Small Immediate Fix

- [ ] Add tqdm progress to `history-verify`.
- [ ] Add `--progress` / `--no-progress` to `history-verify` if needed for consistency.
- [ ] Run focused tests for history verification.

## Phase 1: Clarify R2 Helpers

- [ ] Identify market-data functions currently in `storage/remote.py`.
- [ ] Move market-data R2 loading helpers to a market-data-specific module.
- [ ] Leave strategy, registry, and decision helpers behavior unchanged.
- [ ] Update imports and tests.

## Phase 2: Split Raw Fetch And Normalize

- [ ] Separate raw bhavcopy fetch logic from canonical normalization logic.
- [ ] Keep format lookup behavior unchanged.
- [ ] Keep `bhavcopy-ingest` available as a small/manual conversion command.
- [ ] Document that `history-build` is the production full-history builder.
- [ ] Run bhavcopy and CLI tests.

## Phase 3: Split Historical Build And Verify

- [ ] Move partition-first build logic into a build-focused module.
- [ ] Move partition manifest generation/refresh into its own module.
- [ ] Move parquet verification/audit logic into a verify-focused module.
- [ ] Preserve partition-first behavior and existing audit output.
- [ ] Run history, history upload, and market-data tests.

## Phase 4: Slim CLI

- [ ] Move market-data command handlers out of `cli.py`.
- [ ] Keep command names and flags stable.
- [ ] Keep `cli.py` as parser/router rather than business-logic owner.
- [ ] Run CLI tests and workflow tests.

## Phase 5: Final Cleanup

- [ ] Remove or rename ambiguous internal modules only after imports are stable.
- [ ] Update docs if operator-facing behavior or module names matter.
- [ ] Run the focused market-data test suite.
- [ ] Run the full test suite before merging.

## Focused Test Set

```bash
python -m pytest \
  tests/test_bhavcopy.py \
  tests/test_history.py \
  tests/test_history_upload.py \
  tests/test_market_data.py \
  tests/test_market_data_refresh.py \
  tests/test_remote_storage.py \
  tests/test_cli.py
```

## Non-Goals For This Refactor

- [ ] Do not change R2 object layout.
- [ ] Do not change canonical market-data schema.
- [ ] Do not introduce a global hard memory limiter.
- [ ] Do not rename operator commands without a separate migration decision.
- [ ] Do not refactor strategy runtime until market-data boundaries are clean.
