# Frontier Churn And Admission-Retention Report

## Purpose
- Preserve the two reward/performance improvement areas that are still unplanned.
- Explicitly separate them from the already-written same-cycle handoff plan in [frontier-same-cycle-handoff-plan.md](<repo-root>/agents/improvements/frontier-same-cycle-handoff-plan.md).
- Capture enough evidence and implementation direction that this can be resumed later without reconstructing the investigation.

## Scope
- This report covers the two still-unplanned findings from the March 28, 2026 live-run review:
  1. frontier churn is too eager
  2. admission and retention are still misaligned
- This report does **not** re-plan same-cycle frontier handoff.
  - That topic is already covered by [frontier-same-cycle-handoff-plan.md](<repo-root>/agents/improvements/frontier-same-cycle-handoff-plan.md).

## Related Context
- Current branch during analysis: `feature/frontier-allocator-checkpoint`
- Relevant live run used for evidence:
  - [run_20260328_041537/events.jsonl](<repo-root>/data/events/run_20260328_041537/events.jsonl)
- Comparison run used for sanity checks:
  - [run_20260327_152236/events.jsonl](<repo-root>/data/events/run_20260327_152236/events.jsonl)

## Summary
- Frontier ranking itself looked broadly coherent in the March 28 run.
- The remaining reward drag appears more operational than purely ranking-related.
- Two distinct improvement opportunities remain:
  1. the bot rotates out of markets for gains that are often too small to justify the churn
  2. the bot still enters some bids that the later hedge-depth or refresh checks invalidate almost immediately

Confidence in that summary: **high**.

## Improvement 1: Frontier Churn Is Too Eager

### Description
- The bot currently rotates for modeled improvements that are sometimes real but economically too small once operational costs are considered.
- Those costs include:
  - cancel latency
  - next-cycle or delayed entrant placement
  - time spent with no bid live
  - immediate invalidation risk on the entrant

### Why This Matters
- Even if the entrant is technically better than the loser at the moment of evaluation, a tiny expected reward advantage may be wiped out by the churn required to realize it.
- This is especially important because the current allocator already has real idle windows around frontier replacement.

### Evidence
- In [run_20260328_041537/events.jsonl](<repo-root>/data/events/run_20260328_041537/events.jsonl), the frontier comparisons were generally coherent, but many later swaps were small:
  - `2026-03-28T05:06:24Z`
    - entrant modeled about `$0.19295/day`
    - loser modeled about `$0.18266/day`
    - improvement about `$0.01029/day`
  - `2026-03-28T05:18:28Z`
    - entrant modeled about `$0.02262/day`
    - loser modeled about `$0.01943/day`
    - improvement about `$0.00319/day`
  - `2026-03-28T05:24:27Z`
    - improvement about `$0.00144/day`
  - `2026-03-28T05:29:26Z`
    - improvement about `$0.00119/day`
- These are positive differences, but they are probably too small to justify a cancel-and-replace handoff.

Confidence that this is a real improvement area: **high**.

### Likely Root Cause
- `select_frontier_rotation()` in [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) currently appears to use relative ranking superiority without a meaningful minimum-improvement threshold.
- That means a slightly better entrant can displace a loser even when the net operational payoff is negligible.

Confidence in that likely root cause: **high**.

### Recommended Direction
- Add a minimum frontier-improvement threshold or hysteresis rule before a loser is displaced.
- Candidate threshold forms:
  - minimum absolute daily reward delta
  - minimum ranking-metric delta
  - hybrid rule requiring both
- Strong preference:
  - use an absolute expected daily reward delta first
  - optionally add a metric delta floor second

### Example Policy Shapes
- Do not rotate unless:
  - `entrant_expected_daily_reward - loser_expected_daily_reward >= X`
- Or require both:
  - `entrant_expected_daily_reward - loser_expected_daily_reward >= X`
  - `entrant_metric - loser_metric >= Y`

### Acceptance Criteria
- Frontier rotation should not trigger on tiny deltas that are unlikely to cover handoff costs.
- In live runs, loser cancellations caused by frontier replacement should correspond to meaningfully better entrants.
- The number of `frontier_rebalance` cancels should decrease without obviously trapping the bot in bad markets.

## Improvement 2: Admission And Retention Are Still Misaligned

### Description
- Discovery sometimes admits a market and places a bid, but shortly afterward the hedge-depth or refresh path invalidates that same bid.
- This means the entry decision and the later retention decision are not using a sufficiently aligned definition of viability.

### Why This Matters
- The bot spends time and churn entering orders it was already going to reject moments later.
- This hurts reward capture even when the market ranking itself is reasonable.
- It also makes frontier replacement less effective because a reserved entrant may immediately fail after taking over the freed capital.

### Evidence
- In [run_20260328_041537/events.jsonl](<repo-root>/data/events/run_20260328_041537/events.jsonl):
  - `2026-03-28T05:30:25.579Z`
    - Shai `NO_BID` submitted via `frontier_reservation`
  - `2026-03-28T05:30:26.066Z`
    - same order canceled by `hedge_depth / HedgeDepthBelowMinimum`
  - elapsed time about `0.49s`
- Similar John Cornyn cases occurred repeatedly:
  - `04:24:25Z` submit
  - `04:24:26Z` hedge-depth cancel
  - `04:25:29Z` submit
  - `04:25:30Z` hedge-depth cancel
  - `04:33:27Z` submit
  - `04:33:27Z` hedge-depth cancel
- There were also quote-refresh deadmits shortly after reservation handoff, for example:
  - Avengers reserved entrant submitted at `04:31:24Z`
  - canceled as `quote_refresh_non_viable / MarketDeadmitted` at `04:31:25Z`

Confidence that this issue is still active: **high**.

### Likely Root Cause
- Entry approval uses a market evaluation snapshot that is not strict enough relative to what `check_hedge_depth()` and refresh maintenance later enforce.
- In practice, entry can still pass while later logic determines:
  - opposite-side hedgeable depth is below minimum size
  - quote drift or viability constraints no longer support retention

### Relevant Code Areas
- Entry / evaluation path:
  - [live_engine.rs](<repo-root>/src/runtime/live_engine.rs)
  - especially `evaluate_market(...)`
- Retention / post-entry invalidation path:
  - [check_hedge_depth()](<repo-root>/src/runtime/live_engine.rs#L1682)
  - refresh maintenance logic in [live_engine.rs](<repo-root>/src/runtime/live_engine.rs)

Confidence in that architectural framing: **high**.

### Recommended Direction
- Make admission use a stricter approximation of the same constraints that later invalidate bids.
- Goal:
  - if a market is highly likely to be rejected by hedge-depth or refresh within seconds, it should fail admission instead of briefly entering live state

### Candidate Approaches
- Tighten admission on hedgeable size:
  - require effective hedgeable depth to meet the same minimum-size semantics used by `check_hedge_depth()`
- Add a “retention safety margin” to entry:
  - require slightly more than the bare minimum hedgeable depth at admission time
- Prevent frontier reservation activation if the entrant is already borderline on the same depth constraint that later causes immediate cancellation

### Acceptance Criteria
- A market that enters via `new_quote` or `frontier_reservation` should not be canceled by hedge-depth or refresh within seconds under the same market conditions.
- Immediate submit-then-cancel pairs should materially decrease in live runs.
- Replacement entrants should survive long enough to actually capture rewards after frontier handoff.

## Relationship To Same-Cycle Handoff Plan
- The already-written same-cycle handoff plan addresses the separate issue that loser cancellation currently leaves idle time before entrant placement.
- These two improvements complement that plan:
  - same-cycle handoff improves replacement speed
  - frontier thresholding reduces unnecessary replacements
  - admission/retention alignment makes replacements more likely to stick once placed

Confidence in that decomposition: **high**.

## Recommended Future Order
- If revisited after hedge testing:
  1. implement same-cycle handoff plan
  2. add frontier minimum-improvement threshold / hysteresis
  3. align admission with hedge-depth / retention constraints

Reason:
- same-cycle handoff is already designed and likely to improve yield quickly
- thresholding is the next cleanest reduction in churn
- admission/retention alignment is important but slightly more coupled to quote/hedge mechanics

Confidence in this prioritization: **high**.

## Final Takeaway
- These two issues are still worth fixing even if the hedge path becomes the top active focus.
- The bot can still earn rewards without them, but solving them should improve reward retention and reduce operational waste once hedge confidence is high enough to return to allocator work.
