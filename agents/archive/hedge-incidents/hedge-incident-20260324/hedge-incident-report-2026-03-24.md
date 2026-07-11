# Hedge Incident Report - March 24, 2026

## Scope

This report covers the unhedged fill incident on:

- Market: `Will Trump visit China by April 30?`
- Condition ID: `<redacted-id>`
- Run: `run_20260324_185903`
- Primary event log: `data/events/run_20260324_185903/events.jsonl`

This document is intended as a factual incident report first. It is not a fix plan.

## Executive Summary

The bot did not behave according to the intended strategy.

Confirmed failures:

- The fill was missed by the real-time hedge path.
- The fill was only discovered later by reconciliation.
- The reconciliation hedge path attempted a full-size BUY hedge at `0.99`, which was rejected for insufficient balance/allowance.
- The bot did not properly resolve the resulting one-sided exposure.
- The reconciliation path kept rediscovering and retrying the same fill for minutes.
- The market halt/cleanup behavior was not sufficient to guarantee immediate flattening.

Overall confidence that this was a real hedge-system failure and not just adverse market conditions: **high**.

## Intended Strategy Reference

The intended behavior documented in `STRATEGY.md` is:

- Immediately hedge every fill to stay delta-neutral.
- If a partial hedge cannot fully resolve exposure, sell back the unhedged remainder.
- If exposure remains unresolved, kill the market and flatten.

Relevant references:

- `STRATEGY.md` section 4
- `STRATEGY.md` section 4.5
- `STRATEGY.md` section 4.6
- `STRATEGY.md` section 4.7

Confidence that the observed incident violates the documented strategy: **high**.

## Confirmed Timeline

All times below are UTC, with EDT equivalents in parentheses.

### Order lifecycle before the fill

- `2026-03-24T20:11:16.802Z` (`4:11:16 PM EDT`)
  - `OrderSubmitted`
  - `NO_BID @ 0.73 x 373`
  - Order ID: `<redacted-id>`

- `2026-03-24T20:21:23.410Z` (`4:21:23 PM EDT`)
  - `OrderCancelled`
  - Same order canceled for `QuoteDrift`

- `2026-03-24T20:21:23.968Z` (`4:21:23 PM EDT`)
  - `OrderSubmitted`
  - Replacement `NO_BID @ 0.74 x 373`
  - Order ID: `<redacted-id>`

Confirmed there is no real-time fill-handling sequence tied to that replacement order in this run: `0.99`.

### First fill discovery

- `2026-03-24T20:27:43.383Z` (`4:27:43 PM EDT`)
  - `FillDetected`
  - Source: `reconciliation`
  - `fallback_match = true`
  - `match_source = reconciliation`
  - Fill: `BUY NO 373 @ 0.739998`

This means the fill was first discovered by the imbalance/reconciliation path, not by the dedicated real-time fill handler.

Confidence: **high**.

### First hedge failure

- `2026-03-24T20:27:43.383Z`
  - `HedgeIntentCreated`
  - Source: `reconciliation`
  - Trigger leg: `NO_BID`
  - Hedge side: `BUY`
  - Hedge size: `373`

- `2026-03-24T20:27:43.383Z`
  - `HedgeResultRecorded`
  - Source: `reconciliation`
  - Hedge price recorded: `0.99`
  - Result: failed
  - Failure:
    - `Order placement failed (400 Bad Request): {"error":"not enough balance / allowance"}`

Confidence that the first hedge attempt failed before it ever hit the book: **high**.

### Post-failure behavior

- `2026-03-24T20:28:45.309Z`
  - `OrderSubmitted`
  - `NO_ASK @ 0.75 x 184`
  - Origin: `inventory_ask`

- `2026-03-24T20:29:18.553Z`
  - `OrderCancelled`
  - Same ask canceled for `RiskHalt`

- `2026-03-24T20:29:18.715Z`
  - `RiskStateChanged`
  - Reason: `Position size 373.004836 exceeds cap 184`

Then the bot repeatedly rediscovered the same fill and retried the same failing reconciliation hedge:

- `20:28:51Z`
- `20:29:29Z`
- `20:30:28Z`
- `20:31:28Z`
- `20:32:29Z`
- `20:33:30Z`
- `20:34:27Z`
- `20:35:32Z`
- `20:36:29Z`
- `20:37:28Z`
- `20:38:32Z`
- `20:39:28Z`
- `20:40:27Z`
- `20:41:28Z`
- `20:42:27Z`
- `20:43:27Z`

Every one of those retries produced the same reconciliation hedge failure:

- BUY hedge at `0.99`
- rejected with `not enough balance / allowance`

Confidence that the system kept retrying the same unresolved fill for roughly 16 minutes: **high**.

## Account State During the Incident

Before fill discovery:

- `api_balance_usd = 373.775766`
- `available_budget_usd = 0.775766`
- `order_committed_usd = 276.02`
- `position_committed_usd = 0`

After fill discovery:

- `api_balance_usd = 97.755766`
- `available_budget_usd = 0`
- `order_committed_usd = 276.02`
- `position_committed_usd = 276.022832630328`
- `total_committed_usd = 552.042832630328`

This indicates the system believed it had both:

- the filled position, and
- stale committed order capital

at the same time.

Confidence that this indicates stale local order/accounting state after the missed fill: **high**.

## What We Can Confirm Happened

### 1. The real-time fill path did not process this fill

There are no `fill_handler` events for this market in the incident window, and no pending-fill-fallback queue events in this run.

Confidence: **high**.

### 2. The reconciliation path attempted an unsafe full-size BUY hedge

Current code in `src/trading/hedge_executor.rs`:

- BUY hedges use `buy_hedge_limit_price()`
- that function returns `0.99`
- `execute_buy_gtc_cancel(...)` places the full requested BUY hedge at `0.99`
- there is no book-backed affordability planning
- there is no partial affordability fallback

Confidence: **high**.

### 3. Tight capital was part of the problem, but not the whole problem

At the time of the missed fill:

- remaining balance was about `$97.76`
- complementary full hedge at roughly `0.27 x 373` would cost about `$100.71`

So a full complementary hedge may indeed have been slightly unaffordable.

However:

- the bot did not fail while attempting a smart partial hedge
- it failed while attempting `373 @ 0.99`, which would require about `$369.27`

So the system failed well before it reached the subtle "slightly underfunded for full hedge" case.

Confidence: **high**.

### 4. The bot did not properly flatten after hedge failure

The intended strategy says unresolved residual exposure should be sold back aggressively and/or flattened after kill.

Current reconciliation logic in `src/runtime/live_engine.rs` does not reliably do that:

- it retries and only escalates after 3 consecutive failures
- on "success", it patches local state as if the hedge size was obtained
- on repeated failure, it halts the market and removes it from `managed_markets`
- it does not route through the same full resolution behavior expected for immediate fill handling

Confidence: **high**.

## Root Cause Analysis

### Primary root cause

The most direct cause of the financial loss is:

- missed real-time fill detection
- followed by an unsafe reconciliation hedge implementation that tried a full BUY hedge at `0.99`
- followed by inadequate unresolved-exposure cleanup

Confidence: **high**.

### Secondary root causes

#### Reconciliation path is structurally weaker than the real-time path

The reconciliation path:

- does not share the full intended hedge-resolution behavior
- retries for too long
- does not immediately drive the market through the safest kill/flatten path

Confidence: **high**.

#### Stale local tracking likely worsened cleanup

Status snapshots strongly suggest stale local order commitment remained after the fill, which likely interfered with cleanup and budget accounting.

Confidence: **high**.

#### BUY hedge affordability logic is fundamentally unsafe

Using a fixed `0.99` BUY hedge price makes the hedge path rejectable on balance grounds even when a smaller affordable hedge exists deeper in the opposite book.

Confidence: **high**.

## Answers to Specific Questions

### "Did we not have enough money to hedge because the spread moved?"

Partly yes, but only partly.

A full perfect complementary hedge likely required slightly more cash than remained. But the bot never attempted the correct degraded behavior:

- affordable partial BUY hedge, then
- forced sell-back/flatten of the unresolved remainder

Instead it attempted an obviously unaffordable `0.99` full-size BUY hedge.

Confidence: **high**.

### "Why didn't we get out of the bid before we got filled?"

For normal reward-collecting bids, there is no strategy rule or config rule that says:

- never be the top bid
- never become the inside bid
- always cancel if we become the most likely resting bid to fill

What the bot does have is:

- passive `post_only` normal orders
- quote refresh every 5 seconds
- cancel-replace when quote drift exceeds threshold
- a bid-pricing formula that often lands at or below the current best bid rather than aggressively improving it

But that is not the same as "avoid ever being top bid."

The documented strategy for normal bids is to place them at a reward-seeking depth derived from:

- midpoint
- reward floor
- configured `bid_depth_pct`

See:

- `STRATEGY.md` section 3.1
- `src/strategy/quote_engine.rs`
- `src/trading/order_manager.rs`

That means a normal bid can absolutely still become the best visible bid, as long as it remains passive and does not cross the ask. So the bot did not "fail to get out" of the bid because some special anti-top-bid safety failed. That safety does not appear to exist.

However, there is an important implicit tendency:

- with both sides of the book present, bid price is computed from midpoint and reward floor
- with default `bid_depth_pct = 0.50`, the quote is placed halfway between midpoint and the reward floor
- because of tick rounding down, many common spread shapes naturally cause the computed price to equal the current best bid or sit just below it
- if we join the current best bid price, we can still appear 2nd or 3rd in queue at that price level

So repeated observations of "we are usually 2nd or 3rd" are consistent with the current formula, even without an explicit anti-top-of-book rule.

Confidence: **high**.

### "Do we have safety measures against being the top bid or touching mid on normal orders?"

Only limited ones.

For normal non-hedge entry bids, the protections appear to be:

- `post_only = true`, so the order should rest passively and not cross the ask
- price generation based on midpoint and reward floor, rather than simply bidding the ask

However, I do not see any explicit protection that forces:

- bid price < current best bid
- bid price to remain away from the inside quote
- auto-cancel purely because we become top bid

So the real answer is:

- yes, there is protection against taking liquidity immediately
- no, there is not protection against becoming the top passive bid

There is also an implicit non-aggressive effect from the current pricing formula:

- the quote engine does not "chase" the inside bid directly
- it targets a reward-depth-derived price
- in many books that target, after rounding, ends up joining the best bid rather than stepping ahead of it

That explains why the bot may often look non-top-of-book in practice without having a formal rule requiring that outcome.

Confidence: **high**.

### "Was this a whale collapsing the book?"

Because there is no confirmed "never top bid" safety, a whale is not required to explain this fill.

A normal seller hitting the inside bid could have filled us if our resting order was the best or near-best bid at the time. The logs do not prove a whale-sized sweep here.

So:

- a large taker is possible
- a whale is not necessary to explain the fill
- the hedge failure itself still happened before any hedge order reached the book

Confidence: **high**.

## Why This Sample Is Still Useful

The user correctly noted that the sample is partially tainted by tight capital. That is true.

But the incident is still highly useful because it exposed multiple genuine system failures:

- missed real-time fill handling
- unaffordable full-size BUY hedge logic
- lack of graceful partial hedge handling
- repeated reconciliation rediscovery/retry
- weak halt/flatten behavior under stale local tracking

So even if capital had been slightly higher, this sample still reveals real defects that need fixing.

Confidence: **high**.

## Final Conclusions

### Confirmed

- The fill was missed by the real-time fill path.  
  Confidence: **high**

- The first hedge attempt happened only via reconciliation.  
  Confidence: **high**

- That hedge attempt was a full-size BUY hedge at `0.99`.  
  Confidence: **high**

- The hedge was rejected for `not enough balance / allowance`.  
  Confidence: **high**

- The bot then failed to promptly resolve the exposure and kept retrying the same reconciliation fill.  
  Confidence: **high**

- The current behavior violated the documented intended strategy.  
  Confidence: **high**

### Most likely system-level diagnosis

This was not one isolated affordability miss. It was a compound hedge-system failure:

1. fill detection failure
2. unsafe BUY hedge execution
3. inadequate reconciliation safety behavior
4. likely stale local order/accounting cleanup

Confidence: **high**.

## Out of Scope for This Report

This report intentionally does not prescribe the final fix sequence. A separate fix plan should cover:

- immediate safety fixes
- minimum viable hedge-path hardening
- reconciliation path redesign/alignment
- stale tracking cleanup behavior
- validation plan
