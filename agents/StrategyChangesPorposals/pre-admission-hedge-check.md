# ~~Pre-Admission Hedge Depth Check~~ COMPLETED (2026-04-11)

**Date filed:** 2026-04-11
**Completed:** 2026-04-11
**Priority:** Medium
**Type:** Code change
**Source:** operator-follow-ups.md item #6 / GitHub #17B
**Branch:** `fix/frontier-churn-threshold`

## Problem

Markets were admitted and bids placed without pre-checking hedge depth. The `check_hedge_depth()` function runs on a separate ~2-second interval and cancels orders after the fact when it finds insufficient opposite-side depth. This produced wasteful 3-5 second submit-cancel pairs.

## What Was Done

### Pre-admission check (`src/runtime/live_engine.rs`)
Added a hedge depth gate inside the existing hedgeability loop in `evaluate_market_on_books_with_context()` (line ~1678). After `apply_hedgeability_gate()` runs, approved bid candidates get an additional check:

```rust
if candidate.status == QuoteStatus::Approved && candidate.leg.is_bid() {
    let opposite_book = match candidate.leg {
        QuoteLeg::YesBid => no_book,
        QuoteLeg::NoBid => yes_book,
        _ => unreachable!(),
    };
    let hedgeable = max_hedgeable_within_slippage(
        opposite_book, true, self.config.strategy.max_slippage_bps,
    );
    if hedgeable < market.reward_config.min_size {
        candidate.status = QuoteStatus::Rejected;
        candidate.reason = Some(format!("Hedge depth {:.0} below min_size {:.0}", ...));
    }
}
```

- Uses the same `max_hedgeable_within_slippage()` function that `check_hedge_depth()` uses
- Rejects at the candidate level — rejected candidates are skipped by `place_quotes()`
- Debug-level log fires when a bid is rejected for insufficient depth
- Existing `check_hedge_depth()` remains as a safety net for depth that evaporates after evaluation

### Test helper fix (`src/strategy/viability.rs`)
Added `min_frontier_improvement` field to `test_strategy_config()` to fix build (from frontier churn change).

## Behavior

- Bids are no longer placed on markets where opposite-side depth within slippage tolerance is below `min_size`
- No 3-5 second submit-cancel pairs for unhedgeable markets
- Eliminates unnecessary API calls (place + cancel)
- Eliminates brief unhedgeable exposure windows
- Markets with insufficient depth will be re-evaluated on the next discovery cycle when depth may have improved
