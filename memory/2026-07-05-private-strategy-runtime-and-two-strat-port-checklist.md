# Private Strategy Runtime And Two-Strategy Port Checklist

Plan: Flatten `V23 h5 dd6 hard 1Cr` and `V28 V18 rotation or shock hard 1Cr` into private long-only blackbox strategies compatible with current `trading-infra-git` backtest and daily paper flows, without exposing strategy logic in GitHub.

Status syntax:

- `- [ ]` not done
- `- [x]` done

## Phase 1: Freeze Current Contract Decisions

- [x] Confirm current decision schema is long-only and rejects negative `target_weight`.
- [x] Confirm current public repo should not contain real private strategy logic.
- [x] Confirm preferred artifact direction is flattened private pickle strategy artifact in R2.
- [x] Confirm private artifact should call a public in-process Python runtime interface, not a network API.
- [x] Confirm one private `.pkl` artifact per strategy.
- [x] Confirm full-cash days are represented as zero decision rows for that strategy/date.
- [x] Confirm partial cash is represented by invested rows summing to less than `1.0`.
- [x] Confirm `feature_config.yaml` should list only coarse required feature families / aggregates / max lookback, not exact params.
- [x] Confirm naming convention:
  - `v1_<revised_description>` for the V23 descendant
  - `v2_<revised_description>` for the V28 descendant
- [x] Write down the exact strategy runtime contract version for the first private artifact implementation.

## Phase 2: Decide What “Compatible” Means

- [x] Define the final execution contract for private strategies:
  - `decision(as_of_date, runtime)` or equivalent
- [x] Define what the private artifact must return:
  - final decisions in current canonical schema only
- [x] Confirm no schema expansion for shorts, sleeve actions, or auxiliary decision columns.
- [x] Confirm how empty / cash decisions are represented under the current schema.
- [x] Decide whether strategy artifacts are versioned by `strategy_id` only or by `strategy_id` plus runtime contract version metadata.

## Phase 3: Public Runtime Interface

- [x] Design a minimal stable Python runtime interface for private artifacts.
- [x] Define runtime methods for:
  - market-data slices
  - trading-date lookup
- [x] Decide whether the runtime API is row-oriented, vectorized DataFrame-oriented, or mixed.
- [x] Prefer vectorized DataFrame-oriented outputs for performance.
- [x] Define the error behavior when a private strategy requests unsupported features.
- [ ] Define caching rules inside one backtest/paper run so repeated feature requests are cheap.

## Phase 4: Flattening Scope For V23 And V28

- [x] Re-map the source `trading-git` dependency chain into a flattened long-only variant.
- [x] Explicitly remove short-only behavior and short fallback behavior.
- [x] Explicitly remove public nested sleeve references from the private artifact.
- [x] Decide what replaces incompatible short lineage inside flattened `V23`.
- [x] Decide whether `V18` remains a long-only internal sleeve inside the flattened private objects.
- [x] Decide whether `V28` should consume flattened internal `V23` and `V18` logic directly inside one artifact.
- [x] Document every source rule kept, dropped, or reinterpreted.

## Phase 5: Feature Support Needed From Public Runtime

- [x] List all required raw inputs for flattened `V23`.
- [x] List all required raw inputs for flattened `V28`.
- [x] List all required derived features from old source lineage, including likely items such as:
  - rolling returns
  - rolling drawdown
  - RSI
  - ADX or any retained equivalents
  - turnover and turnover median
  - breadth-style aggregates
  - leadership features
  - realized volatility
  - EMA trend features
- [x] Separate features into:
  - per-symbol
  - per-exchange aggregate
  - cross-universe aggregate
- [x] Confirm each needed feature can be computed from current parquet market data or identify any impossible ones.

## Phase 6: Strategy Artifact Packaging

- [x] Decide artifact layout under strategy folder / R2 for private strategies.
- [x] Decide whether `model.pkl` is the actual strategy artifact filename or whether a new private artifact filename is cleaner.
- [x] Decide how private artifacts declare the runtime contract version they expect.
- [x] Decide how config and metadata reference the private artifact.
- [x] Decide whether one artifact contains one strategy or shared internal sub-strategies too.
- [ ] Decide whether pickle serialization is stable enough with the intended Python/runtime version.

## Phase 7: Local Backtest Runtime Support

- [x] Add public runtime implementation for local backtests.
- [x] Ensure the local runtime can serve all supported feature requests from parquet market data.
- [x] Wire private strategy loading into existing backtest execution flow.
- [x] Ensure output decisions still use current validation/storage paths unchanged.
- [x] Add tests for one mock private strategy artifact using the runtime.

## Phase 8: GitHub Daily Paper Runtime Support

- [x] Add the same public runtime implementation for R2-backed daily paper execution.
- [x] Ensure private artifact download from R2 works in the paper path.
- [x] Ensure daily paper runtime can compute the same requested features as local backtest runtime.
- [x] Ensure decisions upload path stays unchanged after private strategy execution.
- [x] Add tests for one mock private strategy artifact in the R2-backed paper path.

## Phase 9: Port V23

- [x] Finalize `v1_<revised_description>` strategy_id for the V23 descendant.
  - `v1_regime_sideways_etf_rotation_long`
- [x] Build the flattened long-only private `V23` artifact.
- [x] Make its dependencies self-contained inside the artifact except for public runtime feature requests.
- [x] Validate that the artifact emits final long-only decision rows under the current schema.
- [ ] Add backtest comparison checks against source behavior where a comparison is still meaningful after removing short logic.
- [x] Register and test `V23` in local backtest flow.
- [x] Register and test `V23` in daily paper flow.

## Phase 10: Port V28

- [x] Finalize `v2_<revised_description>` strategy_id for the V28 descendant.
  - `v2_rotation_or_shock_overlay_long`
- [x] Build the flattened long-only private `V28 V18 rotation or shock` artifact.
- [x] Make it self-contained inside the artifact except for public runtime feature requests.
- [x] Validate that the artifact emits final long-only decision rows under the current schema.
- [ ] Add backtest comparison checks against source behavior where meaningful after long-only trimming.
- [x] Register and test `V28` in local backtest flow.
- [x] Register and test `V28` in daily paper flow.

## Phase 11: Verification And Documentation

- [x] Add a durable decision file if the runtime contract is finalized.
- [x] Add a session log summarizing the implementation path taken.
- [x] Update `memory/index.md` with any new memory files created during execution.
- [x] Run targeted tests for runtime loading, backtest execution, paper execution, and performance computation.
- [x] Review `git status` and summarize any remaining deferred items.

## Key Risks To Watch

- [ ] Pickle artifact is not actually self-contained and still requires private code imports at runtime.
- [ ] Required features cannot be derived cleanly from current parquet schema alone.
- [ ] Flattened long-only versions drift too far from source strategy intent to be worth calling V23/V28 descendants.
- [ ] Runtime API grows ad hoc instead of becoming a stable contract.
