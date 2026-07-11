# Open-Order Confirmation Safety Report

## Purpose
- Preserve the current understanding of the duplicate-order regression that reappeared while trying to fix the filtered `/data/orders?market=...` `401 Unauthorized` warning.
- Record the deeper architectural issue so this can be resumed later without relying on chat history.
- Frame this as an improvement opportunity rather than an urgent reward blocker: the bot can still earn rewards in its current baseline state, but a correct fix here would improve capital truth and reduce avoidable missed efficiency.

## Executive Summary
- The most recent attempts to fix the `401` warning reintroduced a severe regression: live tracked bids were pruned even though the exchange still had them open, which freed budget and allowed duplicate same-leg bids to be entered.
- Confidence that the attempted `401` fix caused the observed duplicate-order regression: **high**.
- The deeper issue is broader than the specific auth experiment:
  - the bot currently treats **absence from open-order snapshots** as sufficiently trustworthy evidence that a tracked order vanished
  - live exchange snapshots appear to be incomplete enough that this assumption is unsafe
  - a second fetch from the same endpoint is not meaningfully independent evidence
- Confidence in that deeper architectural diagnosis: **high**.

## What Was Being Fixed
- The warning was:
  - `Observe-only market sync failed while confirming missing tracked bids ... Get orders failed (401 Unauthorized): {"error":"Unauthorized/Invalid api key"}`
- That warning came from the market-scoped confirmation step added for the Phase 2 missing-order safety logic.
- Intended purpose of that confirmation step:
  - when a tracked bid disappears from the global `/data/orders` snapshot, confirm whether that specific market still has the order live before pruning it or freeing capital
- Operational importance of the warning:
  - moderate, not catastrophic
  - it means the bot failed to fetch one piece of exchange truth used to confirm whether a missing tracked bid is still live
  - in the safe failure mode, the bot should retain the tracked order and keep capital reserved
- Confidence in the above severity framing: **high**.

## What Went Wrong

### First auth attempt
- The first auth fix attempt changed L2 signing behavior to remove `POLY_NONCE` and later changed `/data/orders` signing behavior further.
- That path caused the runtime to begin successfully using a confirmation flow that still did not reliably return the live order.
- Result: the bot treated those confirmations as real disappearance evidence and pruned still-live tracked bids.

### Safe fallback attempt
- A second attempt avoided `GET /data/orders?market=...` and instead used a second global `GET /data/orders` snapshot filtered locally by `condition_id`.
- That removed the `401`, but it still relied on **absence from open-order snapshots** as the core confirmation signal.
- Result: the same regression reappeared, which proves the deeper issue is not only filtered-endpoint auth.

## Evidence From Runs

### Broken run with attempted fix
- Run: `data/events/run_20260328_035236/events.jsonl`
- Sequence:
  1. At `2026-03-28T03:53:00Z`, two bids were submitted:
     - John Cornyn `YES_BID` `225 @ 0.28`
     - Spider-Man `YES_BID` `134 @ 0.39`
  2. Status snapshots through `03:53:11Z` still showed:
     - `order_committed_usd = 115.26`
     - `available_budget_usd = 0.414266`
  3. At `03:53:13Z`, without any `OrderCancelled` events, status abruptly flipped to:
     - `order_committed_usd = 0`
     - `available_budget_usd = 359.414266`
  4. The bot then entered the same markets again, producing duplicates.
- Interpretation:
  - the bot did not actually cancel those bids
  - it lost local tracking, freed capital, and re-entered
- Confidence: **high**.

### Baseline comparison run without the bad local diff
- Run: `data/events/run_20260328_035544/events.jsonl`
- Sequence:
  1. At `2026-03-28T03:56:10Z`, two bids were submitted:
     - John Cornyn `YES_BID` `200 @ 0.28`
     - Spider-Man `YES_BID` `159 @ 0.39`
  2. Status snapshots remained stable with:
     - `order_committed_usd = 118.01`
     - `available_budget_usd = 0.414266`
  3. Later state changes happened only after explicit `OrderCancelled` events:
     - `hedge_depth` cancellation on one market
     - `quote_refresh` drift cancellation on the other
  4. Replacement activity followed those real cancels, not silent tracking loss.
- Interpretation:
  - the duplicate-order regression was not present in this baseline run
  - the attempted `401` fix path is the differentiator
- Confidence: **high**.

## Deeper Root Cause

### The unsafe assumption
- The missing-order logic currently assumes:
  - if a tracked order is absent from one global open-orders snapshot
  - and then absent again from a second confirmation snapshot
  - and there is no corroborating position increase
  - then it is safe to prune the tracked order and free capital

That assumption is not safe enough.

### Why it is unsafe
- The exchange’s open-orders snapshot appears to be incomplete or inconsistent enough that a live order can be absent from repeated snapshots.
- Two fetches from the same endpoint are not independent confirmation.
- When both snapshots miss the order, the bot reaches a false conclusion:
  - “order is gone”
- It then executes the harmful part:
  - `remove_order(order_id)`
  - capital is freed
  - duplicate same-leg entry is allowed on the next cycle

### Concrete code path
- In `src/runtime/live_engine.rs`, `detect_missed_fills_from_exchange()`:
  - fetches global live orders
  - builds `disappeared_by_market`
  - fetches another global confirmation snapshot
  - passes locally filtered orders into market sync
  - when no fill delta is corroborated, it increments missing-order confirmations
  - on the second confirmed miss, it removes the tracked order
- In `src/trading/order_manager.rs`, `sync_market_open_orders_from_live_orders(...)`:
  - uses the caller-supplied live orders to decide which tracked orders are “missing”
  - in `Reconcile` mode, absence from that supplied set directly drives pruning
- Confidence that this is the operative failure path: **high**.

## Why This Is A Deeper Issue, Not Just A Bad Auth Fix
- The filtered-endpoint `401` was real, but it was not the core safety problem.
- The core safety problem is:
  - **absence-based pruning from open-order snapshots is too strong a conclusion**
- The auth experiment exposed it one way.
- The unfiltered-only confirmation experiment exposed it another way.
- Both fail for the same underlying reason:
  - snapshots are trustworthy for **positive evidence**
  - snapshots are not trustworthy enough for **negative evidence**
- Confidence: **high**.

## Current Practical Impact
- In the bot’s last known-safe baseline, this issue does not appear to be actively preventing reward earning.
- The bot can still quote, hold bids, and generate rewards.
- That is why this should be treated as an improvement opportunity rather than the top urgent blocker if other priorities are more important.
- However, if revisited, it matters because a correct solution would improve:
  - capital truth
  - duplicate-order safety
  - confidence in incident handling
  - ability to reclaim truly stale capital without dangerous side effects
- Confidence in this priority framing: **high**.

## Recommended Future Direction

### Design principle
- Use open-order snapshots only for **positive confirmation**, not for pruning by absence alone.

### Safe uses of open-order snapshots
- Good uses:
  - confirm a tracked order is still live
  - confirm `size_matched` increased
  - import still-live orders that are visible
  - detect duplicate live same-leg bids that are positively visible

### Unsafe uses to avoid
- Avoid:
  - freeing capital just because an order is missing from one or two snapshots
  - pruning a tracked order purely from absence in snapshot data

### Safer future options
- Option A: fail closed on missing-order confirmation
  - if a tracked bid disappears from snapshots but there is no explicit fill/cancel evidence, keep the tracked order and capital reserved
  - downside: capital may remain tied up longer than ideal
  - upside: no duplicate live bid risk
- Option B: require stronger evidence before pruning
  - explicit cancel confirmation
  - reliable per-order lookup by order id, if Polymarket exposes one that proves stable in live use
  - corroborating account/position truth that can distinguish vanished order vs retained resting order
- Option C: shadow-mode diagnostics first
  - if filtered market-scoped open-order requests are revisited, use them in audit-only mode first
  - compare them against global snapshot truth
  - do not let them drive capital release until proven reliable

### Recommended default
- If this work is resumed later, the safest next implementation should be:
  - restore baseline behavior
  - keep snapshot-based confirmation additive/observational only
  - do not prune tracked bids on snapshot absence alone
- Confidence: **high**.

## Suggested Acceptance Criteria For A Future Fix
- A live tracked bid that is omitted from one or more open-order snapshots must not free budget unless there is stronger corroborating evidence than snapshot absence.
- Status snapshots must never drop `order_committed_usd` to zero while visible live bids remain open on the exchange.
- Duplicate same-leg live bids must not accumulate across refresh cycles.
- Any future attempt to use filtered open-order endpoints must first run in shadow mode and prove parity before it affects tracking or capital.

## Recommendation If This Is Deferred
- Defer this work safely by leaving the bot on the last known-good baseline rather than any of the attempted `401` fixes.
- Treat the missing confirmation warning as acceptable technical debt for now, provided the failure mode remains conservative.
- Confidence: **high**.
