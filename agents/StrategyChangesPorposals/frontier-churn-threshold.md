# ~~Frontier Churn — Minimum Improvement Threshold~~ COMPLETED (2026-04-11)

**Date filed:** 2026-04-11
**Completed:** 2026-04-11
**Priority:** Medium-High
**Type:** Code change
**Source:** operator-follow-ups.md item #5 / GitHub #17A
**Branch:** `fix/frontier-churn-threshold`

## Problem

The frontier comparator in `select_frontier_rotation()` used pure ranking superiority with no minimum improvement threshold. The bot rotated capital for sub-penny daily improvements ($0.0011/day, $0.0093/day) that couldn't cover the operational cost of the rotation.

## What Was Done

### Config (`src/config.rs`)
- Added `min_frontier_improvement: Decimal` to `StrategyConfig` with `#[serde(default)]`
- Default: `$0.05/day` (5 cents) via `default_min_frontier_improvement()`
- Added to `Config::default()` impl

### Threshold gate (`src/runtime/live_engine.rs`)
- In `select_frontier_rotation()`, after the `compare_rank_keys` check (line ~2823), added:
  ```rust
  let improvement = entrant_rank_key.estimated_reward - loser_rank_key.estimated_reward;
  if improvement < self.config.strategy.min_frontier_improvement {
      trace!(..., "Frontier rotation skipped: improvement below threshold");
      continue;
  }
  ```
- Added `trace` to tracing imports

### Config (`config.json`)
- Added `"min_frontier_improvement": "0.05"` to strategy section

### Documentation (`STRATEGY.md`)
- Section 2.3: Added bullet explaining the minimum improvement threshold
- Section 9 config table: Added `strategy.min_frontier_improvement | $0.05`

### Tests (`tests/unit/config_tests.rs`)
- Added `min_frontier_improvement` default assertion to `strategy_config_defaults` test

## Behavior

- Rotations with `entrant_reward - loser_reward < $0.05/day` are now skipped
- Trace-level log fires when a rotation is skipped, including the improvement delta and threshold (useful for tuning)
- The `compare_rank_keys` check still runs first (filters non-improvements), so the threshold only applies to genuine but small improvements
- Configurable via `config.json` — can be tuned up or down based on observed operational costs
