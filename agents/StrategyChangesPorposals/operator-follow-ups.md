# Operator Follow-Ups — Pending Config & Code Changes

**Date:** 2026-04-10
**Source:** Identified during STRATEGY.md alignment audit (see `strategy-alignment.md`)
**Status:** None actioned yet

---

## ~~1. `config.json risk.cash_reserve`: Operator override to 10~~ — RESOLVED (2026-04-11)

Operator intentionally set `cash_reserve` to `"10"` in `config.json:37`. The code default is `50` (in `config.rs:130`), but the JSON value properly overrides it via serde. The value propagates through a single path: `config.risk.cash_reserve` → `OrderManager` constructor → `available_budget()` subtraction at `order_manager.rs:559`. No code paths fall back to the default. Emergency hedges (`available_hedge_resolution_usdc()`) intentionally bypass the reserve by design.

---

## ~~2. `config.json discovery.poll_interval_secs`: 60 → 61~~ — DONE (2026-04-11)

Applied. `config.json:5` changed to `"poll_interval_secs": 61`. Now aligned with STRATEGY.md and code default.

---

## 3. Strip `max_position_size` enforcement from `risk.rs` — DEFERRED

**Status:** Separate issue filed at `agents/StrategyChangesPorposals/184shareCap.md`
**Decision:** Cap is no longer needed. Bot self-determines hedge sufficiency. Will tackle as a separate issue.

---

## 4. Evaluate hedge mutex timeout remaining concerns — DEFERRED

**Status:** Separate issue filed at `agents/StrategyChangesPorposals/hedge-mutex-timeout.md`
**Decision:** Will tackle as a separate issue.

---

## ~~5. Frontier churn on tiny deltas (GitHub #17A)~~ — DONE (2026-04-11)

**Status:** Implemented in branch `fix/frontier-churn-threshold`. See `agents/StrategyChangesPorposals/frontier-churn-threshold.md` for details.
Added `strategy.min_frontier_improvement` config parameter (default $0.05/day). Rotations below threshold are skipped with trace log.

---

## ~~6. Admission/retention misalignment (GitHub #17B)~~ — DONE (2026-04-11)

**Status:** Implemented in branch `fix/frontier-churn-threshold`. See `agents/StrategyChangesPorposals/pre-admission-hedge-check.md` for details.
Pre-admission hedge depth check added in `evaluate_market_on_books_with_context()`. Bids are rejected before placement if opposite-side depth within slippage is below `min_size`.
