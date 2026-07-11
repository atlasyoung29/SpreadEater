# Hedge Incident Report - 2026-03-27

## Summary

The most recent live-run Shai incident was not handled properly end-to-end.

- The bot did detect a missed `YES` buy fill and did attempt to hedge the same market by buying `NO`.
- The initial split decision was directionally plausible under the current planner.
- The failure was in post-resolution execution/verification and follow-on control flow, not in choosing the market to hedge.
- The exchange-history evidence materially changes the diagnosis: the `NO` hedge buy really did execute on exchange, even though the bot later recorded the hedge result as failed and still believed exposure remained at `197.5` shares.

Overall confidence: **high**.

## Sources Reviewed

- `data/events/run_20260327_174658/events.jsonl`
- `data/error_log.jsonl`
- Exchange-history screenshot for the Shai market

## Market

- Question: `Will Shai Gilgeous-Alexander win the 2025–2026 NBA MVP?`
- Condition ID: `<redacted-id>`

## What Happened

### 1. Reconciliation detected a missed fill

At `2026-03-27 18:38:27Z` (`2:38:27 PM EDT`), the bot emitted:

- `FillDetected`
  - source: `reconciliation`
  - side: `BUY`
  - outcome: `YES`
  - fill size: `197.50`
  - fill price: `0.71`
  - trade id: `reconciliation-recon-b13fa8e7-3189-47eb-bb22-02a48f640bcb`

Confidence this represents the missed-fill incident trigger: **high**.

### 1a. Why reconciliation had to catch it

The missed `YES` buy most likely fell out of the normal real-time hedge path before the bot could anchor it to a tracked bid order.

Important evidence:

- by `2026-03-27 18:38:06Z`, the run already showed one-sided position exposure on Shai while `order_committed_usd` was `0`
- the websocket still appeared healthy around that time, so this does not look like a broad user-stream outage
- the fast exchange-truth detector only synthesizes fills from still-tracked bids, so once the tracked bid is gone the position reconciliation pass becomes the only remaining safety net

That narrows the likely failure mode to a cancel-race, event-loss, or order-to-fill match-loss around the original `YES_BID`, not a full websocket disconnect.

Confidence on this narrowed mechanism: **high**.
Confidence on the exact micro-cause: **medium**.

### 2. The bot planned a split hedge resolution

Immediately after, the bot emitted `HedgeIntentCreated` with:

- planned `NO` hedge buy: `163.53` shares at `0.31`
- planned `YES` sell-back: `33.97` shares at `0.71`
- unresolved shares: `0`

This split is directionally reasonable under the current resolution planner, which compares hedge cost versus sell-back cost and prefers the cheaper route per share.

Confidence the planner itself was not irrational: **high**.

One clarification: the planned `NO` hedge buy was not passive. In the current implementation, BUY hedges are sent as executable GTC orders at a crossing limit price. In this incident, that appears to have been aggressive enough, because the screenshot shows the `NO` buy really filled.

Confidence: **high**.

### 3. The exchange screenshot confirms the hedge buy really happened

The Polymarket History screenshot shows these relevant fills:

- `Bought Yes 71c, 197.5 shares`
- `Bought No 30c, 163.5 shares`
- `Sold Yes 69c, 197.5 shares`
- `Sold No 27c, 163.5 shares`
- plus a small `10.1`-share `YES` round-trip

This is strong evidence that the `NO` hedge buy executed on exchange.

Confidence: **high**.

### 4. The bot still recorded the hedge result as failed

At `2026-03-27 18:38:29Z` (`2:38:29 PM EDT`), `HedgeResultRecorded` said:

- `result_status = failed`
- `hedge_leg_status = unverified`
- `sellback_leg_status = failed`
- `failure_reason = Sell-back placement failed ... FOK ... post_sync_net_exposure=197.5 exceeds tolerance 0.50`

The error log matches this:

- `Reconciliation hedge resolution failed: Sell-back placement failed ... post_sync_net_exposure=197.5 exceeds tolerance 0.50`

This means the bot's post-resolution truth was wrong or stale relative to the exchange, because the screenshot shows the `NO` hedge buy did happen.

Confidence: **high**.

### 5. The later sells look like emergency flatten, not the original plan

The screenshot shows the bot later sold:

- the `YES` side (`197.5 @ 69c`)
- the `NO` side (`163.5 @ 27c`)

That does not match the original planned split of:

- `NO` buy `163.53`
- `YES` sell-back `33.97`

The most likely interpretation is:

- the bot believed the hedge resolution failed,
- halted the market,
- then unwound both sides to flatten.

Confidence: **high**.

## Loss Breakdown From Screenshot

Approximate realized loss from the six visible exchange trades:

- `YES` bulk round-trip: bought `197.5 @ 0.71`, sold `197.5 @ 0.69` -> about `-$3.35`
- `NO` hedge round-trip: bought `163.5 @ 0.30`, sold `163.5 @ 0.27` -> about `-$5.08`
- small `YES` round-trip: bought `10.1 @ 0.71`, sold `10.1 @ 0.70` -> about `-$0.10`

Approximate total: `-$8.53`

Confidence: **high**.

## Evidence Of Still-Broken Behavior

### 1. Post-hedge verification/accounting is wrong

The bot believed it still had full `197.5` share net exposure after resolution, but the screenshot shows the `NO` hedge buy actually executed.

This strongly suggests a reconciliation/post-sync truth bug.

Confidence: **high**.

### 2. The sell-back policy is brittle and likely contributed directly

The implementation currently uses the worst visible sell-back bid as the FOK limit price. In this incident, that was `0.71`.

Current implementation:

- `src/trading/hedge_executor.rs` sets `sellback_limit_price = worst_sellback_bid`

Strategy doc:

- `STRATEGY.md` says unhedged remainder should be sold back aggressively via FOK at `$0.01`

This mismatch made the sell-back much more likely to fail.

Clarification:

- the problem here was not that the hedge BUY was too passive
- the hedge BUY appears to have executed
- the problematic leg was the sell-back FOK, because `SELL 33.97 @ 0.71 FOK` means "fill the entire size immediately at `0.71` or better, or fail the whole order"
- if even part of the available size was only bid at `0.70` or `0.69`, the sell-back would be rejected in full

So the likely cascade was:

1. reconciliation detected the missed `YES` buy
2. the `NO` hedge BUY executed
3. the strict `YES` sell-back FOK at `0.71` failed
4. the bot still believed full exposure remained
5. the market was halted and later fully unwound

This is materially different from "the bot did not bid aggressively enough on the hedge." The evidence points to the sell-back leg being too strict, not the hedge BUY being too passive.

Confidence this materially contributed to the incident: **high**.

### 3. The fallback fill matcher misattributed a giant fill to a tiny cancelled ask

The bot had a cancelled `YES_ASK` order:

- order id: `<redacted-id>`
- size: `10.06`
- submitted at `18:37:56Z`
- cancelled at `18:38:01Z`

Later, the fill handler used that same cancelled `10.06`-share order to emit:

- a `SELL YES 197.5 @ 0.71`
- then a `SELL YES 197.5 @ 0.69`

That is impossible as logged and is strong evidence the `recently_cancelled_fallback` matcher is still broken or too permissive.

Confidence: **high**.

### 4. Duplicate internal fill detection occurred after the halt

The same trade id:

- `f21a9975-9e44-42d6-986c-0c3501db17df`

was emitted multiple times as `FillDetected` for `SELL YES 197.5 @ 0.69`.

That is duplicate internal detection noise, not multiple independent exchange trades.

Confidence: **high**.

### 5. Halt handling re-entered repeatedly instead of converging cleanly

The error log and events show repeated nested halt reasons:

- `Market halted: Market halted: ...`

This indicates the halted-market path is still reprocessing follow-on signals instead of becoming idempotent once the market is already halted.

Confidence: **high**.

## Interpretation

### Did the bot hedge the correct market?

Yes. It hedged the Shai market and chose the correct opposite side (`NO`) for a `YES` fill.

Confidence: **high**.

### Did it perform properly overall?

No.

- Initial hedge side selection: mostly reasonable
- End-to-end resolution, verification, and follow-on flatten behavior: not correct

Confidence: **high**.

### Was there noise on top of the core incident?

Yes.

- A small `10.1`-share inventory-ask round-trip added a little noise
- The much larger issue was internal duplicate/misattributed fill handling after the failure

Confidence: **high**.

## Current Best Diagnosis

The current incident exposed three main problems:

1. A missed fill can still escape the real-time path and only surface later as one-sided position truth.
2. Reconciliation/post-resolution position truth is not reliable enough once the resolution starts.
3. `recently_cancelled_fallback` can misattribute later fills to the wrong cancelled order.
4. The sell-back policy is too brittle relative to the intended "get flat first" strategy.

Overall confidence in this diagnosis: **high**.
