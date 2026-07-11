# Post-Sync Zero Materialization Note

Date: April 6, 2026

## Change Summary

The current branch includes a production-path change in [live_engine.rs](<repo-root>/src/runtime/live_engine.rs#L6439) introduced by commit `659a27e` on April 1, 2026 at 11:49 PM EDT.

The change adds `materialize_authoritative_zero_post_position(...)`, which converts:

- `post_position = None`

into:

- `post_position = Some(Position::new(condition_id))`

when all of the following are true:

- the hedge resolution result was marked `success=true`
- the post-sync source was one of:
  - `first_sync`
  - `retry_sync`
  - `position_manager`

The call site in the fill-handler path is [live_engine.rs:5335](<repo-root>/src/runtime/live_engine.rs#L5335).

## Why This Matters

Before this change, a missing post-sync position row remained `None`, which preserved the distinction between:

- no position object was found
- a flat position was positively confirmed

After this change, some successful hedge paths now collapse those two cases into a concrete zero-inventory position.

That synthetic zero is not used only for reporting. It flows into normal runtime handling:

- reconciliation baseline update at [live_engine.rs:5350](<repo-root>/src/runtime/live_engine.rs#L5350)
- neutrality emission / interpretation inputs at [live_engine.rs:5358](<repo-root>/src/runtime/live_engine.rs#L5358)

## Risk Hypothesis

If the upstream positions truth is truly authoritative and omission means flat, this is a reasonable improvement.

If omission can also mean lag, incomplete sync visibility, or a transient missing row, then the runtime can become too optimistic:

- internal state may record `(YES=0, NO=0)` earlier than justified
- reconciliation may treat the market as already neutral
- later recovery or re-hedging may be suppressed because the baseline was zeroed

This does not directly place new orders, but it can change later bot behavior by changing runtime state.

## What To Look For First If Something Breaks

If a future hedge failure shows any of the following:

- production says `no_exit_needed`
- production says post-sync inventory was neutral
- direct funded-wallet truth still shows residual inventory
- reconciliation appears to stand down too early after a successful hedge

then this change is an early suspect.

Focus first on:

- [live_engine.rs:5335](<repo-root>/src/runtime/live_engine.rs#L5335)
- [live_engine.rs:5350](<repo-root>/src/runtime/live_engine.rs#L5350)
- [live_engine.rs:5358](<repo-root>/src/runtime/live_engine.rs#L5358)
- [live_engine.rs:6439](<repo-root>/src/runtime/live_engine.rs#L6439)

## Current Assessment

- Confidence this is a real production-behavior change, not just monitor-only plumbing: **high**
- Confidence this could suppress recovery only in specific success-path / missing-position cases, not broadly across all fills: **high**
- Confidence this is a sensible first place to inspect if a hedge run reports internal neutrality while direct truth remains directional: **high**
