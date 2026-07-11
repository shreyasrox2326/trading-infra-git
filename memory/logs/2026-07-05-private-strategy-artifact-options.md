# Private Strategy Artifact Options

Date: 2026-07-05

Context: User does not want real strategy logic exposed in GitHub. Current repo stores public code in Git and private persistent artifacts in R2.

## Current State

- Strategy upload/download paths already support:
  - `config.yaml`
  - `metadata.json`
  - optional `model.pkl`
  - optional `feature_config.yaml`
- Current runtime only executes strategy types implemented in `strategy_builder.py`.
- Current runtime does **not** execute ML or private artifact strategy logic just because a `model.pkl` exists.

## Privacy Requirement

Need a deployment pattern where:

- public GitHub does not contain the actual strategy logic
- strategy artifacts in R2 remain private
- daily paper and local backtest can still execute deployed strategies

## Loader / Artifact Options

### Option A: Public Generic Interpreter + Private Rule Artifact

Shape:

- GitHub contains one generic interpreter strategy type such as `rule_graph_v1`
- R2 stores a private artifact like:
  - `rules.json`
  - `rules.yaml`
  - `model.pkl`
- Runtime downloads artifact from R2 and generic interpreter executes it

Pros:

- Keeps actual strategy logic out of GitHub
- Supports auditable structured rule logic
- Better than pickle for diffability if JSON/YAML based

Cons:

- Requires designing a rule schema expressive enough for real strategies
- Complex nested logic may become awkward
- Still requires runtime support for private artifact execution

### Option B: Public Generic Feature Engine + Private Serialized Decision Function

Shape:

- GitHub contains shared feature builders only
- R2 stores serialized strategy object or learned model
- Runtime loads artifact and runs `.predict` / `.decide`

Pros:

- Keeps strategy logic private
- Can hide thresholds, trees, weights, etc.
- Natural if strategy eventually becomes model-like

Cons:

- `pickle` is opaque and brittle
- Harder to inspect, diff, and audit
- Security/safety concerns around arbitrary deserialization
- Not ideal for hand-authored rule policies

### Option C: Public Minimal Runtime + Private Python Module Bundle

Shape:

- R2 stores zipped Python strategy package
- Runtime downloads and imports private module dynamically

Pros:

- Closest to current repo-native Python strategy authoring
- Preserves full flexibility

Cons:

- Effectively remote code execution
- Highest security and maintainability risk
- Hardest to make robust and auditable

### Option D: Public Thin Wrapper + Private Precomputed Daily Decisions

Shape:

- Local/private system computes final decisions
- R2 stores final `decisions.parquet`
- Infra only reads/uploads/serves them

Pros:

- Maximum privacy
- No strategy logic in GitHub
- Simplest runtime

Cons:

- Breaks requirement for repo-native backtestable strategy execution
- Daily paper becomes an external dependency, not self-contained

## Best-Fit Direction

If privacy must hold and repo code should stay mostly stable, the least-bad path is:

- **Option A**: public generic interpreter + private structured rule artifact in R2

Reason:

- More auditable than pickle
- Safer than dynamic private Python imports
- Closer to deterministic strategy execution than opaque model files

## Important Constraint

Even Option A is still an architecture change.

It is smaller than “expand the whole infra,” but it still requires:

- a new strategy type in `strategy_builder.py`
- artifact loading beyond current `config.yaml` / `metadata.json`
- a private artifact schema and validator

## Practical Recommendation

If the user wants privacy first, do **not** port real strategies as public Python classes.

Instead:

1. Define one private artifact format.
2. Define one generic runtime loader/interpreter.
3. Keep strategy-specific logic/data in R2.
4. Only then port private strategies into that artifact format.
