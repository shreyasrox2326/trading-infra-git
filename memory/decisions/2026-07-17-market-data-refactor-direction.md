# Market Data Refactor Direction

Date: 2026-07-17

## Decision

Refactor the market-data side of the codebase in small, behavior-preserving steps. The goal is a modern, maintainable, understandable codebase with clear workflow boundaries, stable operator commands, and no unnecessary rewrites.

The market-data subsystem should read as these flows:

```text
Historical publish:
  raw exchange files
  -> raw fetch manifest and reviewed exceptions
  -> canonical monthly parquet partitions
  -> partition manifest
  -> verify and doctor reports
  -> optional R2 sync check
  -> staged R2 upload

Daily refresh:
  one exchange/date raw bhavcopy
  -> canonical rows
  -> merge into affected R2 monthly partition
  -> dedupe and publish

Repair and maintenance:
  resume fetch rows
  -> mark reviewed exceptions
  -> rebuild missing or selected partitions
  -> regenerate partition manifest
  -> compare local manifest to R2
  -> clean stale staging objects

Read/query:
  local or R2 parquet
  -> filtered canonical Polars DataFrame
```

## Refactor Principles

- Keep existing CLI commands stable until there is an explicit operator migration.
- Prefer mechanical moves before behavior changes.
- Split modules by workflow intent: fetch, normalize, build, verify, publish, refresh, load.
- Keep storage boring: R2 client, object paths, usage/budget helpers.
- Avoid broad abstractions unless they remove real coupling.
- Preserve parallelism and tqdm progress bars for long operations.
- Do not add a global memory hard limiter now; the known history-verify crash issue is no longer the active problem.
- Add tqdm progress to `history-verify` as the immediate small fix.

## Desired Shape

Target direction, not necessarily one commit:

```text
src/trading_infra/
  market_data/
    schema.py
    formats.py
    fetch.py
    normalize.py
    manifest.py
    partition_manifest.py
    build.py
    verify.py
    doctor.py
    publish.py
    refresh.py
    load.py

  storage/
    r2.py
    paths.py
    usage.py

  commands/
    market_data.py
    strategy.py
    performance.py
    r2.py

  cli.py
```

## Current Pain Points

- `cli.py` mixes parser wiring and command execution logic.
- `data/bhavcopy.py` mixes fetch, file reading, normalization, and validation.
- `data/history.py` mixes build, partition manifest, verification, and audit rendering.
- `storage/remote.py` mixes market data, strategies, registries, and decisions.
- `bhavcopy-ingest` is a small/manual conversion command and should not be confused with the production full-history builder.

## Initial Implementation Order

1. Add tqdm progress to `history-verify`.
2. Split market-data R2 load helpers out of `storage/remote.py`.
3. Split `data/bhavcopy.py` into fetch and normalize modules.
4. Split `data/history.py` into build, partition manifest, and verify modules.
5. Slim `cli.py` by moving command handlers into domain command modules.

## Acceptance Criteria

- Existing operator commands still work.
- Existing tests pass after each refactor step.
- Market-data modules can be understood from filenames without reading the CLI.
- Full-history work remains partition-first and progress-visible.
- R2 storage layout remains unchanged.
- Documentation and memory do not contradict `README.md`.
