# SpreadEater — Strategy Breakdown

> This document describes the complete trading strategy: what the bot does, why, and how.

---

## 1. Core Thesis

SpreadEater is a **fully hedged liquidity-rewards market maker** on Polymarket.

- Post passive limit orders on both sides of binary prediction markets to earn Polymarket's liquidity reward program
- Immediately hedge every fill to stay **delta-neutral** — no directional exposure, ever
- Merge YES + NO token pairs on-chain via the standard CTF contract or the Neg Risk Adapter to lock in profit as USDC

**Net = liquidity rewards − hedge costs − sellback losses − fees − operational losses**

The bot does not take directional bets. Rewards are the only positive term; everything else is a leak.

---

## 2. Market Selection Pipeline

### 2.1 Discovery (every 61 seconds)

1. Poll the Polymarket Discovery/Gamma API for all active binary markets
2. Filter to markets that:
   - Have daily liquidity rewards **≥ $10**
   - Expire **> 24 hours** from now
   - Are active, not closed/archived, and accepting orders
   - Are binary (YES/NO outcomes only) with distinct token IDs

### 2.2 Evaluation (per market)

For each discovered market:

1. **Reject cheap outcomes** — skip any outcome with mid-price < $0.20 (avoids extreme-tail markets)
2. **Compute quote set** — generate 4 candidate legs: YesBid, YesAsk, NoBid, NoAsk
3. **Check hedgeability** — walk the opposite book to verify:
   - Full hedge size is available in depth
   - Slippage ≤ 80 bps
   - Before admission, approved bid size is clamped down to currently hedgeable opposite-book depth; if even `min_size` is not hedgeable, that bid is rejected
4. **Estimate reward share** using Polymarket's scoring formula:
   ```
   S(v, s) = ((v − s) / v)² × size
   ```
   where `v` = max_spread, `s` = distance from mid-price
   - Estimate competitor score from visible book depth × 1.5× conservative multiplier
   - Our estimated share = `our_score / (our_score + competitor_score)`
   - Clamped to [0.01%, 25%]
  - Calibration samples compare actual scoring status only against orders whose current fresh-book, competition-adjusted evaluation still approves that exact quote; recent observed non-scoring on an unchanged order suppresses repeated false positives until the quote meaningfully changes, and stale or missing book truth is skipped
5. **Viability gate** — market is viable if the legacy edge/share threshold passes:
   - `estimated_reward = daily_reward × score_share × discount_factor` (default discount: 0.70)
   - `estimated_edge = estimated_reward − estimated_hedge_cost`
   -
   - `shares_committed` is hedge-aware: sum of approved bid sizes, with `effective_quote_size` as the fallback when there are no approved bids
   - This viability/admission check still includes hedge economics in the numerator

### 2.3 Ranking & Budget Allocation

- All viable markets sorted by **reward_per_share = estimated_reward / shares_committed** (highest first)
- Hedge economics are still part of the viability gate, but they no longer boost ranking once a market is viable
- Budget allocated top-down until exhausted
- On the **60s discovery cycle only**, the bot also runs a strict **bid-rotation frontier pass**:
  - Only resting **bid** capital is reclaimable; asks and held inventory are never rotated for rank reasons
  - A held bid market is only evictable once its continuous bid presence is at least `poll_interval_secs + 1` seconds old (currently **62s**)
  - The bot evaluates non-held markets against `actual_free_budget + one loser market's reclaimable bid capital`
  - At most **one held bid market** is displaced per discovery cycle
  - Rotation only triggers if the entrant's daily reward exceeds the loser's by at least `min_frontier_improvement` (default $0.05/day) — prevents churn on sub-penny deltas that can't cover operational costs
  - If a better entrant is found, the loser market's bids are canceled in that cycle, then the bot attempts a **same-cycle handoff**: polls for cancel verification every 250ms within a bounded window (`frontier_handoff_window_secs`, default 5s). Once verified, a **fresh evaluation** of the current cycle's admitted/evaluated non-held markets is performed with current books and budget, not the historical `known_markets` cache, and the best viable market is placed immediately — this may differ from the originally reserved entrant if conditions changed. If the cancel is not verified in time, the capital is reserved and unrelated new bid entries are frozen until the **next** discovery cycle gives the reserved entrant first claim
- The existing `would_trade` concept remains immediate-actionable only: “can this market be entered right now with actually free capital?”

---

## 3. Quote Pricing

### 3.1 Bids (passive — earning rewards)

- **Price** = `mid − (bid_depth_pct × available_range)` where `bid_depth_pct = 0.50`
  - "Available range" = distance from mid to the reward floor (where the Polymarket score function drops to zero)
  - 50% depth means we place halfway between mid and the floor — balancing fill probability against reward score
- **Size** = dynamically computed per market using score-share targeting:
  1. Estimate visible competitor score from book depth (× 1.5 conservative multiplier)
  2. Solve for the size needed to capture the configured target share of the reward pool
  3. Clamp to `[min_size, max_size]` and round to whole shares
  4. Clamp approved bid size again to currently hedgeable opposite-book depth; reject it if hedgeable depth is below `min_size`
  5. Cap by available budget
- Only placed on outcomes with mid ≥ $0.20

### 3.2 Asks (reward quoting only)

- Asks are generated as part of the 4-leg candidate quote set but are **not used as an exit mechanism** — these are *reward asks* for liquidity scoring purposes only
- **Note:** Separate from reward asks, the bot also places *inventory asks* as a fallback exit mechanism when CTF merge fails, is unconfigured, or during reconciliation. See Section 5 (CTF Merge) and Section 6.6 (Reconciliation) for details.
- After a bid fill, the bot exits via one of two paths chosen per-share by cost comparison:
  1. **Hedge + CTF merge** — buy the opposite token, merge YES+NO pairs on-chain for $1.00
  2. **Sellback** — sell the filled token back into the bid book
- See Section 4.5 for how this decision is made
- Price = `best_bid + max_spread` or `best_ask`, whichever is lower

### 3.3 Quote Refresh

- Every 5 seconds, check all resting orders against current book
- Cancel-replace any order that has drifted **> 30 bps** from its target price
- Separate from this drift refresh, fresh market-book WebSocket updates may also trigger the existing hedge-depth safety guard immediately for the affected managed market only; this reuses the same hedge-depth predicates and does not run broader viability or drift logic

---

## 4. Hedge Execution (Critical Path)

This is the most latency-sensitive part of the bot. Hedges run on a **dedicated async task** that is never blocked by discovery, evaluation, or refresh cycles.

### 4.1 Trigger

- Real-time WebSocket (UserStream) detects a fill on one of our resting bid orders
- Fill events arrive via an unbounded channel to the dedicated FillHandler task
- Primary anchoring uses exchange order IDs; when maker/taker IDs are absent, the bot may still anchor a trade immediately if there is exactly one active or recently cancelled tracked order with the same market, token, side, price, and sufficient size. Otherwise the trade is deferred to reconciliation rather than guessed.

### 4.2 Side Selection

| We got filled on | Hedge action |
|-----------------|--------------|
| YES Bid (bought YES) | Buy NO |
| NO Bid (bought NO) | Buy YES |
| YES Ask (sold YES) | Sell NO |
| NO Ask (sold NO) | Sell YES |

### 4.3 Hedge Order Types

- **BUY hedges**: GTC limit at a dynamic price computed by `plan_fill_resolution()` — set to `worst_hedge_ask + tick_size` to capture the full intended depth without overpaying
  1. Place GTC order for the resolved hedge size at the dynamic limit
  2. Wait 500ms (exchange matching latency)
  3. Cancel unfilled remainder
  4. Verify actual fills via `GET /order` API
  - Rationale: Polymarket BUY FOK interprets `size` as notional USDC spend, not shares. GTC preserves exact-share semantics.

- **SELL hedges**: FOK at **$0.01** (accept any price)
  - Immediate fill-or-kill — no cancellation needed
  - FOK SELL correctly interprets `size` as shares

- **BUY-resolution sellbacks**: FOK limit at the computed `sellback_limit_price`
  1. Place FOK sellback for the resolved sellback shares at the planner-computed bid limit
  2. If the sellback fully fills, continue normally
  3. If it misses because books moved, sync current position truth once, refresh books/balance once, recompute the residual resolution once, then either resolve or kill the market
  - Rationale: keeps sellback share-sized and bounded like the current system, but removes the planner-vs-executor price divergence for BUY-side resolution sellbacks

### 4.4 Hedge Sizing

- Size = **residual exposure** (current position imbalance), not the raw fill size
- Normalized to 2 decimal places (truncated toward zero)
- This prevents overhedging when multiple fills arrive in quick succession

### 4.5 Fill Resolution (Hedge vs Sellback)

When a BUY-side hedge is needed, the bot runs a **greedy per-share cost comparison** to decide how to neutralize each share of exposure. This happens in a single pass before any orders are placed.

For each share, two costs are compared:
- `hedge_cost = fill_price + hedge_ask - $1.00` — net cost to buy the opposite token (accounting for the $1.00 CTF merge recovery)
- `sellback_cost = fill_price - sellback_bid` — net loss from selling back into the filled-side bid book

The algorithm walks both order books simultaneously, consuming depth level by level. Each share is routed to whichever exit is cheaper at the current depth.

**Tie-breaking:** Ties (`hedge_cost == sellback_cost`) route to sellback, which reclaims capital immediately without depending on CTF merge execution.

**Affordability gate:** Hedge allocation is capped by `max_hedge_usdc` — the available USDC balance at resolution time. Resolution prep first waits for market-order cancels, reconciles exchange order truth for that market, and retries pending cancels before refreshing balance and computing `max_hedge_usdc`, so stale filled/cancelled bids do not keep consuming hedge budget. On BUY-side resolution only, if that still leaves `max_hedge_usdc` below the residual exposure size, the bot cancels other resting bid capital globally, waits through the same bounded cancel window, refreshes balance once more, and then computes the final `max_hedge_usdc` used by the planner. When the hedge budget is exhausted, remaining shares are rerouted to sellback regardless of cost comparison. This prevents the bot from attempting hedges it can't fund.

**SELL-side hedges** bypass this algorithm entirely and use the legacy FOK path (see Section 4.3).

### 4.6 Post-Hedge Verification

- After hedge resolves, check `net_exposure ≤ 0.5 shares` tolerance
- If exceeded → **kill market** (cancel all orders + flatten position)

### 4.7 Hedge Timeout

- If a fill goes unhedged for 10 seconds, kill the market
- 10s is the realistic upper bound for API order + fill time
- Longer timeouts cause quote drift while we're exposed

### 4.8 Trade Lifecycle (End-to-End)

Complete flow from fill to flat:

1. **Fill detected** via WebSocket → dedicated FillHandler task (4.1)
2. **Side selection** — determine hedge token and direction (4.2)
3. **Sizing** — compute residual exposure, not raw fill size (4.4)
4. **Resolution** — greedy per-share cost comparison allocates shares to hedge and/or sellback (4.5)
5. **Execution** — BUY hedges placed as GTC, BUY-resolution sellbacks as real-price FOK, SELL hedges remain legacy FOK @ $0.01 (4.3)
6. **CTF merge** — if hedge created YES+NO pairs, merge on-chain for $1.00/pair (Section 5)
7. **Verification** — confirm net exposure ≤ 0.5 shares; kill market if exceeded (4.6)
8. **Timeout guard** — if any step takes >10 seconds from fill, kill market (4.7)

---

## 5. CTF Merge

- When we hold both YES and NO tokens for the same market, merge them on-chain through the venue that matches the market class:
  - standard markets (`neg_risk=false`) merge via the CTF (Conditional Token Framework) contract
  - neg-risk markets (`neg_risk=true`) merge the paired YES+NO inventory via the Neg Risk Adapter
- Merging converts YES + NO pairs → USDC at exactly $1.00/pair
- Executed gaslessly through Polymarket's SAFE relayer flow using the bot's existing signer and SAFE wallet
- This branch supports only the bot's normal paired single-question exit path. It does **not** add event-level neg-risk `convertPositions(...)` support.

### Merge Prerequisites

- Requires `POLY_PRIVATE_KEY`, `RELAYER_API_KEY`, and `RELAYER_API_KEY_ADDRESS`. The SAFE wallet address comes from `POLY_FUNDER` when set, otherwise the signer address is used. If any required relayer credential is missing, the CTF merger is disabled and all post-hedge exits fall back to inventory asks (no merge attempt). This is logged at startup but produces no further warnings.
- Standard markets merge directly through the CTF contract; no ERC-1155 operator approval is needed because the caller burns its own paired positions inside `mergePositions(...)`.
- On the **first neg-risk merge of a session**, the bot submits a SAFE `setApprovalForAll` on the CTF ERC-1155 contract for the Neg Risk Adapter, because the adapter first transfers the caller's YES/NO tokens into itself before invoking CTF merge.
  Approval success is cached separately per venue for the rest of the session. Merges will fail if the required approval step fails.

### Merge Timing

- Merge is triggered **after the hedge attempt is fully resolved** — not on partial fills
- The bot waits until resolution is complete (both hedge and sellback orders settled) before merging
- This prevents merging partial pairs while the hedge is still in progress
- Merge operates on **whole pairs only** — `min(YES, NO).floor()`. Fractional remainders (e.g., 0.5 shares on each side) stay as inventory.

### Merge Confirmation

- After submitting the SAFE transaction to Polymarket's relayer, the bot polls relayer transaction state up to **30 times at 2-second intervals** (60 seconds max) and treats `STATE_MINED` / `STATE_CONFIRMED` as success.
- The relayer client makes only **bounded** recovery attempts before giving up: transient relayer readiness/submit failures (`408/429/5xx` or transport timeouts on SAFE deployment checks, nonce reads, and exact-payload submit requests) are retried a few times, and terminal on-chain SAFE `STATE_FAILED` executions are resubmitted with a fresh nonce up to **2 additional attempts** before the bot falls back to inventory asks. Success criteria do **not** change: a merge still counts only after relayer success plus post-merge truth convergence.
- Once relayer success is confirmed, the bot refreshes collateral balance and performs one immediate positions sync.
- Because the direct `/positions` API can lag confirmed merge settlement, the bot then starts a bounded post-merge truth observer: poll `/positions` every **1 second** for up to **30 seconds**, treat a missing row as flat zero inventory, compare on the bot's normalized **2-decimal share precision**, and require **2 consecutive matching snapshots** before calling the direct truth converged.
- In production this observer is detached and observability-only so the fill-handler queue and per-market hedge lock are not held open by visibility lag. If truth still has not converged after the bounded wait, the bot emits a degraded warning event and log, but does **not** liquidate, freeze, or otherwise override the confirmed merge outcome.
- The manual/harness merge probe waits on this same bounded observer before reporting merge success, so a green `standard_pass` now means the production truth source itself actually caught up rather than just the relayer succeeding first.
- This relayer polling plus post-merge truth convergence window is **not covered by the 10-second hedge timeout** (which only wraps `execute_resolution_plan`). The full fill-to-flat lifecycle can exceed 10s when merge observability is included.

---

## 6. Risk Controls

### 6.1 Budget Management

- Every cycle, call the API to get current cash balance
- Available budget = `API_balance − cash_reserve`
- The reserve is a configurable amount we never touch (safety buffer)
- **Default reserve: $50** (`risk.cash_reserve`)
- **No secondary cap** — the old `max_total_exposure` static cap is fully replaced. Budget is purely balance-driven.
- Position size per market is derived from this available budget

### 6.2 Hedge Timeout — Kill Switch

- If a fill remains unhedged for 10 seconds → kill that market
- Kill = cancel all orders + flatten all positions in that market
- Prevents accumulating unhedged exposure and quote drift

### 6.3 Depth Check

- Every 2 seconds, for each managed market, verify the opposite book still has hedging depth
- If **partial depth**: scale down our bid size proportionally
- If **no depth**: cancel our bids entirely (can't hedge, shouldn't be bidding)
- The same guard can also run immediately on fresh market-book WebSocket updates for the affected managed market only, using the same predicates as the 2-second pass

### 6.4 Slippage & Hedge Cost Limits

- Maximum acceptable slippage on hedge execution: **80 bps**
- Maximum acceptable hedge cost: **80 bps**
- If we can only partially fill within these limits:
  1. Fill what we can
  2. Sell the unhedged remainder back to the book
  3. CTF merge the hedged portion

### 6.5 Book Staleness

- If a managed market's book data goes stale (> 30 seconds without update), this is a **critical error**
- Response: immediately **halt that market**, cancel all tracked orders, and start the shared cleanup/flatten flow
- The halted market stays quarantined until cleanup verifies that orders are drained and post-sync exposure is flat
- Before cleanup defers on "pending order drain", it retries pending cancels and reconciles exchange order truth for the halted market so stale tracked orders do not keep the market wedged in deferred cleanup
- For stale-book halts specifically, the bot auto-resumes only after **two consecutive fresh-book confirmations** following verified cleanup; duplicate stale-book kills are deduplicated to a single halt transition
- Other markets with fresh books continue trading normally
- The book should effectively never be stale — we maintain a constant WebSocket connection

### 6.6 Reconciliation (Missed Hedge Safety Net)

- Every cycle, scan for one-sided positions (e.g., YES > 0 but NO = 0)
- These indicate a missed hedge (WS event lost, API error, etc.)
- Execute the same hedge-resolve flow as normal fills (shared `execute_resolution_plan_with_sellback_recompute`), but **exit via inventory asks instead of CTF merge**
  - Reconciliation is a recovery path — it hedges to balance the position, then exits via inventory asks rather than CTF merge
  - This avoids adding an on-chain transaction — with its own failure modes — to a recovery path where something already went wrong
  - Inventory asks are a simpler, exchange-level exit that doesn't depend on Polygon transaction execution
- **Post-hedge exposure verification is enforced** — reconciliation still checks `net_exposure ≤ 0.5` after the shared hedge-resolution flow and will halt the market on first aggregate failure. Because reconciliation reuses the same shared resolution executor as the WS fill handler, both paths can do one bounded post-sync retry for execution-confirmed sellbacks and, when hedge truth is already confirmed but sellback truth still lags, temporarily derive exposure from that sellback to avoid false failures.
- Late user-stream trades that match our own already-verified resolution sellbacks are treated as duplicate observability, not as new fills, so reconciliation does not re-open a market that post-sync already proved flat.
- On **first failure**, the market is **killed** (cancel all orders + flatten) — no retry escalation
- **Critical**: uses a **per-market mutex/lock** shared with the WS fill handler
  - If the WS fill handler is hedging market X, recon waits
  - If recon is hedging market X, WS fill handler waits
  - **Prevents double-hedging** — this was a real incident (the "Lyon incident")
- **Note:** A failure counter (`recon_failure_counts`) exists in code but is effectively dead code — reconciliation kills the market on first hedge failure, so the counter never accumulates.

### 6.7 Duplicate Prevention

- Enforced by the per-market mutex (6.6 above)
- Additionally, hedge order IDs are tracked to prevent re-hedging from late WS events

---

## 7. Startup & Reconnect Behavior

1. On startup (or WebSocket reconnect):
   - Sync all positions from the Polymarket Data API
   - Check for any unhedged (one-sided) positions
2. **Hedge immediately** — before placing any new orders
   - Uses the reconciliation path: hedge-resolve flow with inventory asks exit (no CTF merge). See Section 6.6.
3. Only after all positions are hedged: begin normal discovery/evaluation cycles

---

## 8. Operational Lifecycle

```
STARTUP
  └─ Sync positions from API
      └─ Hedge any unhedged positions
          └─ Begin normal cycles

DISCOVERY CYCLE (every 61s)
  ├─ Refresh API balance → compute available budget (balance − reserve)
  ├─ Discover & filter markets ($10+ rewards, >24h expiry, binary, active)
  ├─ For each market (ranked by reward_per_share, then estimated daily reward):
  │   ├─ Evaluate: quote pricing, hedgeability, score proxy, viability
  │   ├─ If viable → place or refresh bids
  │   └─ If not viable → cancel bids
  ├─ Frontier bid rotation:
  │   ├─ Treat only older resting bids as reclaimable capital
  │   ├─ Compare one better non-held entrant against one lower-ranked held bid market
  │   └─ If entrant wins → cancel loser bids now, reserve that capital, freeze unrelated new bid entries, then enter the reserved winner next discovery cycle after cancel verification
  └─ Reconcile any one-sided positions (with per-market mutex)

DEPTH CHECK (every 2s)
  └─ For each managed market:
      ├─ Walk opposite book to verify hedge depth
      ├─ Scale down bids if partial depth available
      └─ Cancel bids entirely if no depth

QUOTE REFRESH (every 5s)
  └─ For each resting order:
      └─ If drifted > 30bps from target → cancel-replace

FILL HANDLER (real-time, dedicated async task)
  └─ WebSocket fill detected
      ├─ Acquire per-market mutex
      ├─ Compute residual exposure
      ├─ Run plan_fill_resolution() — greedy per-share cost comparison (see 4.5)
      ├─ Execute: hedge order (GTC at dynamic limit), sellback order (FOK)
      ├─ CTF merge all complete YES+NO pairs → USDC
      ├─ Verify net_exposure ≤ 0.5 shares (else kill market)
      └─ Release mutex

RECONCILIATION (per cycle, mutex-protected)
  └─ Scan for one-sided positions
      ├─ Acquire per-market mutex (waits if fill handler is active)
      ├─ Execute hedge-resolve flow (shared resolution pipeline, no CTF merge)
      ├─ Place inventory asks on remaining exposure to flatten
      └─ On first failure → kill market (no retry escalation)
```

---

## 9. Configuration Reference

| Parameter | Value | Description |
|-----------|-------|-------------|
| `discovery.min_daily_reward` | $10 | Minimum daily reward pool to consider a market |
| `discovery.poll_interval_secs` | 61 | How often to discover new markets |
| `strategy.bid_depth_pct` | 0.50 | How deep below mid to place bids (50% of available range) |
| `strategy.ask_depth_pct` | 0.20 | How far above mid to place asks |
| `strategy.score_proxy.target_share` | (configured) | Target share of reward pool used to compute dynamic bid size |
| `strategy.quote_refresh_secs` | 5 | Cancel-replace interval |
| `strategy.quote_drift_bps` | 30 | Drift threshold for cancel-replace |
| `strategy.min_outcome_price` | 0.20 | Minimum mid-price to quote on |
| `strategy.min_est_daily` | $0.25 | Minimum estimated daily edge |
| `strategy.min_return_pct` | 0.25% | Minimum return per dollar committed (R_dollar_effective − hedge_cost_per_dollar) |
| `strategy.reward_discount_factor` | 0.70 | Uncertainty discount on reward-per-dollar estimate (range 0.5–0.8) |
| `strategy.max_slippage_bps` | 80 | Max slippage on hedge execution |
| `strategy.max_hedge_cost_bps` | 80 | Max acceptable hedge cost |
| `strategy.min_frontier_improvement` | $0.05 | Minimum daily reward improvement to justify frontier rotation |
| `strategy.frontier_handoff_window_secs` | 5 | Seconds to poll for cancel verification before deferring to next cycle (0 = disable) |
| `strategy.score_proxy.competition_multiplier` | 1.5 | Conservative inflation on competitor estimate |
| `risk.hedge_timeout_secs` | 10 | Kill market if unhedged |
| `risk.hedge_exposure_tolerance` | 0.5 | Shares of residual allowed post-hedge |
| `risk.cash_reserve` | $50 | Cash to always keep in account |
| `books.max_book_age_secs` | 30 | Book staleness threshold (kill trigger) |
| Depth check interval | 2s | Opposite book depth verification |

*Live operating values may drift from these defaults; refer to `config.json` for currently deployed values.*

---

## 10. Implementation Status

Known gaps between this document and the running system:

- **Legacy SELL-hedge execution price (4.3):** SELL hedges still use the legacy FOK @ $0.01 path. This increment only aligns BUY-resolution sellback execution with the planner-computed `sellback_limit_price`.
- **Timeout scope (4.7):** The 10-second timeout does not cover the full fill-to-flat lifecycle. It starts around `execute_resolution_plan`, missing the preparation phase (book fetch, cancel wait) before it and CTF merge/verification after it.
- **`risk.max_position_size` removed (2026-04-11):** The vestigial per-market position cap has been fully removed. The bot's own hedge-depth and budget logic determines appropriate position sizing without an arbitrary share cap.
- **Hedge timeout clock starts on discovery cycle, not on fill:** The `unhedged_since` clock that drives `RiskManager.check_hedge_timeouts()` is only set inside `update_market_exposure()`, which runs on the discovery cycle (~once per 61s). A fill arriving 1 second after a discovery cycle won't start its 10-second countdown for ~60 seconds, so the realized kill-switch latency is 10–~70s, not 10s. This is separate from the timeout-scope note above.
- **API-hang mutex hold (per `agents/hedge-timeout-gap.md`):** `client.rs:89-90` now has a 15-second request timeout, which mitigates unbounded HTTP hangs. However, the per-market hedge mutex stays held for the full duration of the HTTP request (up to 15s). The 10-second market-level kill switch fires correctly (the market gets halted), but the mutex isn't released until the request completes or times out, blocking subsequent fills on the same market during that window.
