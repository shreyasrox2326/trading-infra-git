# Private Flattened Pickle Strategy Direction

Date: 2026-07-05

## Decision

Preferred privacy-preserving direction for non-public strategies:

- Public repo computes market data inputs and shared features.
- Private strategy artifact is a flattened blackbox object stored in R2.
- Artifact may be serialized as a pickle if it is self-contained enough to run with a stable public runtime interface.
- Public runtime passes computed features into the private artifact and receives final decision rows back.

## Intended Split

Public code:

- market-data loading
- on-demand feature and raw-data service exposed through a stable runtime interface
- strategy artifact download from R2
- execution harness
- decision validation/storage
- backtest and paper orchestration

Private artifact:

- flattened strategy logic
- all nested sleeve/selector logic required for that strategy
- optional private parameters/model state
- requests whatever raw data slices or derived indicators it needs through the public runtime interface

## Constraints

- Top-level selectors like current `V23` and `V28` are not useful as standalone pickle artifacts unless flattened first.
- Pickle is acceptable only if runtime compatibility is controlled and the artifact does not depend on hidden missing code at load time.
- Public code should not encode private branch logic.

## Refined Execution Model

Preferred runtime shape:

- private artifact exposes something like `decision(as_of_date, runtime)`
- `runtime` is a public host object supplied by infra
- private artifact asks the runtime for:
  - raw market-data slices
  - derived features / indicators
  - helper computations such as rolling return, drawdown, breadth, liquidity, regime inputs
- runtime computes those values locally inside the current execution environment
- private artifact returns final decision rows only

Important clarification:

- The pickle should not literally call GitHub Actions as an external service.
- Instead, the same public runtime contract should work in both environments:
  - local backtest runtime
  - GitHub Actions daily-paper runtime

So “GitHub computes it” really means “the public infra code running inside GitHub Actions computes it during daily execution.”
