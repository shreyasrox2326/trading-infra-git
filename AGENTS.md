# AI Agent Instructions

## Project Purpose

Project name: `trading-infra-git`

This repo supports pure technical, individual-stock backtesting and paper-trading strategies.

Read `README.md` first for the current project architecture and notes. Do not overwrite or contradict it. Use `memory/` for durable decisions and timestamped session notes. commit-rules.md contains commit rules. 

## Repo Path

Container path:

```text
/workspaces/code/trading-infra-git
```

Host mount:

```text
Host:      e:\code
Container: /workspaces/code
```

Container:

```text
name = infallible_turing
id   = ee7d490cbe5e
```

## Execution Rule

- Run project commands only inside the container `infallible_turing` (`ee7d490cbe5e`).
- Use the container workspace path `/workspaces/code/trading-infra-git` for repo operations.
- Do not run project build, test, or tooling commands on the host machine unless the user explicitly asks for host execution.

## Shell And Container Guidance

- The operator shell is often Windows PowerShell while project commands must run inside container `sh`/`python`.
- Prefer simple `docker exec infallible_turing sh -lc 'cd /workspaces/code/trading-infra-git && ...'` commands for short repo operations.
- Avoid deep nested quoting chains such as PowerShell -> Docker -> `sh -lc` -> inline Python when a stdin script is enough.
- For inline Python inside the container, prefer piping the script over stdin:

```text
@'
print("hello from container")
'@ | docker exec -i infallible_turing python -
```

- When shell features must work under container `/bin/sh`, use POSIX syntax. Prefer `. ./.env` over `source .env` unless Bash is explicitly required.
- Prefer `rg` for search when available, but fall back to portable tools (`find`, `grep`, Python) if the container image does not have `rg`.
- Keep one shell end-to-end for a command when possible instead of mixing host-side path expansion with container-side path assumptions.

## Core Context

Cloudflare R2 is the private source of truth for persistent data, strategy files, model files, registries, and decision logs.

GitHub Actions runs daily scheduled processing.

The local machine is used for strategy research, full backtests, training, and approved strategy uploads.

Strategies are treated as blackboxes:

```text
market data up to date t -> strategy -> final decision rows for date t
```

Backtest decisions and paper decisions are symmetric. Both produce `decisions.parquet` with the same schema.

Performance is computed on demand from:

```text
decision log + market data + strategy behavior
```

## Memory Files

Memory lives in:

```text
memory/
  index.md
  decisions/
  logs/
```

- `AGENTS.md` is stable agent instruction.
- `memory/index.md` is the memory map.
- `memory/decisions/` contains durable decisions.
- `memory/logs/` contains timestamped session notes.

At the start of a future session, load context in this order:

1. Read `README.md` for project architecture.
2. Read `memory/index.md` for the current memory map.
3. Read relevant files in `memory/decisions/` for durable decisions.
4. Read relevant files in `memory/logs/` for recent session notes.

## How To Update Memory

- Update `README.md` only when project architecture notes themselves change.
- Add durable decisions under `memory/decisions/YYYY-MM-DD-topic.md`.
- Add session notes under `memory/logs/YYYY-MM-DD-topic.md`.
- Keep memory short and practical.
- Link new decision and log files from `memory/index.md`.
- Do not add architecture decisions that contradict `README.md`.

## Local Run Rules

- Use `tmux` for runs expected to be long.
- Keep terminal output minimal.
- Use `tqdm` or progress bars for long-running loops.
- Redirect verbose logs to temp/log files instead of flooding the terminal.
- Avoid Python row-by-row loops for large data.
- Prefer Polars, DuckDB, NumPy, and PyTorch optimized operations.
- Do not introduce Rust unless profiling proves a bottleneck.


## Git Usage

Use Git CLI as needed.

Before committing:

    git status
    git diff --staged

Stage specific files when possible:

    git add <file1> <file2>

Use `git add .` only when all changes should be committed.

Commit format:

Use a commit message file inside the container instead of nested `git commit -m`
quoting from Windows PowerShell:

    cat > /tmp/commit-msg <<'EOF'
    <type>: <summary>

    <description>
    EOF
    git commit -F /tmp/commit-msg


### What Not To Commit

Do not commit:

- secrets
- R2 credentials
- private market data
- large private model files
- large generated backtest outputs unless explicitly intended
