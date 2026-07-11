# ~~184-Share Per-Market Position Cap — Removal~~ COMPLETED (2026-04-11)

**Date filed:** 2026-04-11
**Completed:** 2026-04-11
**Priority:** Medium
**Type:** Code change
**Source:** operator-follow-ups.md item #3

## Decision

Cap is no longer needed. The bot self-determines whether there are enough shares to hedge its position via budget constraints (`available_budget()`) and hedge depth checks. The 184-share cap was vestigial, had two bugs (USDC vs shares mismatch, disproportionate aggression on cheap markets), and was redundant with the budget system.

## What Was Done

### Source code changes

| File | Change |
|------|--------|
| `src/trading/risk.rs` | Removed force-halt on overshoot (old lines 83-96 in `update_market_exposure()`) and pre-trade position cap rejection (old lines 232-241 in `pre_trade_check()`) |
| `src/config.rs` | Removed `max_position_size` field from `RiskConfig` struct, its doc comment, and its default value (`Decimal::from(300)`) in `Config::default()`. Removed from 2 inline test JSON fixtures. |
| `src/runtime/orchestrator.rs` | Replaced `self.config.risk.max_position_size` with `Decimal::from(10_000)` hardcoded ceiling in shadow-mode `evaluate_market()`. Shadow mode is evaluation-only (no real trades), so an unconstrained ceiling is appropriate. |
| `src/runtime/live_engine.rs` | Removed `.min(self.config.risk.max_position_size)` from 4 inventory ask sizing lines (old lines 3339, 3361, 5932, 5953). Ask size now uses full position inventory. |

### Test changes

| File | Change |
|------|--------|
| `tests/unit/trading/risk_tests.rs` | Removed `max_position_size` from `test_config()`. Removed 3 tests: `update_market_exposure_halts_on_exceed`, `pre_trade_check_rejects_position_cap_exceeded`, `pre_trade_check_allows_hedge_over_cap`. |
| `tests/unit/helpers.rs` | Removed `max_position_size` from `make_risk_config()`. |
| `tests/unit/config_tests.rs` | Removed `max_position_size` from JSON fixtures in `risk_config_defaults`, `risk_config_explicit_values`, and `full_config_json_roundtrip` tests. |
| `src/watchdog/mod.rs` | Removed `max_position_size` from `test_risk_manager()` helper. |

### Config and documentation changes

| File | Change |
|------|--------|
| `config.json` | Removed `"max_position_size": "184"` line from risk section. |
| `CONFIG.md` | Removed `risk.max_position_size` reference line. |
| `STRATEGY.md` | Replaced implementation gap note with removal note: "The vestigial per-market position cap has been fully removed." |

## What Still Constrains Position Size (unchanged)

1. **`available_budget()`** (`order_manager.rs:559`) — can only buy what the account can afford
2. **`compute_dynamic_size()`** (`live_engine.rs:1656`) — sizes orders from actual `whole_share_budget`, not a static cap
3. **Hedge depth checks** — `check_hedge_depth()` cancels orders if opposite-side depth is insufficient
4. **Score proxy sizing** — proportional to market conditions
5. **Cash reserve** — $10 always held back
6. **Kill switch** — 10s unhedged exposure timeout still active

## Verification

- Zero `max_position_size` references remain in `src/` or `tests/`
- `spreadeater-core` sub-crate checked clean via `cargo check`
- All remaining risk tests (global halt, market halt, hedge timeout, balance checks) unaffected
