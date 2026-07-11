# Polymarket Hedged Liquidity Rewards Bot — PRD

## 1. Summary

Build a local-first, high-performance Rust system for Polymarket that progresses in two layers:

* **Full product:** a live, fully hedged, four-quote market-making bot for binary reward-eligible markets.
* **MVP deliverable:** a **real-data shadow mode** that discovers markets, ingests live books and account-relevant state, computes candidate quotes, computes hedgeability using opposite-outcome depth, estimates reward viability, and reports which trades would be attractive without sending live orders.

The strategy is reward-first and non-directional:

* discover reward-eligible binary markets,
* filter to markets with **total daily rewards >= $20**,
* maintain a four-leg quote model (`YES bid`, `YES ask`, `NO bid`, `NO ask`),
* post or simulate only quotes whose **full fill size is immediately hedgeable** using opposite-outcome liquidity,
* prefer passive reward capture,
* only execute the hedge if a resting order is filled,
* bound exposure tightly and fail closed on stale data, auth failure, or hedge failure.

Polymarket’s documented surfaces support this design: sampling-market responses expose `rewards_daily_rate`, `min_size`, and `max_spread`; public order-book APIs expose token books plus market details; the market and user WebSocket channels provide live book/trade/order activity; and official Rust SDK support exists for CLOB market data, auth, and order management. ([Polymarket Documentation][1])

## 2. Problem & Context

Polymarket’s liquidity rewards program rewards makers for qualifying liquidity based on spread/size criteria, and the docs describe an order scoring function plus market-side scoring methodology. Sampling-market responses expose the daily reward rate and reward configuration for markets, while the order-scoring endpoint confirms whether a specific order is currently scoring. ([Polymarket Documentation][2])

The target strategy is not directional speculation. It is a **fully hedged liquidity provision strategy** that attempts to earn maker rewards while minimizing directional event risk. The system should only participate when:

* the market is binary and reward-eligible,
* the total daily reward is at least $20,
* the books are valid and internally consistent,
* the quote size is fully hedgeable against opposite-outcome depth,
* the estimated reward opportunity exceeds estimated hedge/slippage/adverse-selection cost.

A reviewed documentation pass confirmed a documented **per-order** scoring endpoint, but did not identify a documented endpoint that returns a market-level aggregate “total score from market makers.” The bot should therefore treat aggregate market score as a **derived internal estimate**, not a guaranteed exchange-provided field. ([Polymarket Documentation][3])

## 3. Goals, Non-Goals, and Success Metrics

### Goals

* Build a production-capable Rust bot for Polymarket binary liquidity rewards markets.
* Start with a real-data shadow MVP that validates selection, quote math, and hedgeability before live trading.
* Support four-sided quoting logic: `YES bid`, `YES ask`, `NO bid`, `NO ask`.
* Require **pre-trade immediate hedgeability** for the full intended fill size of each quote.
* Prefer passive reward capture and only cross/take liquidity when a fill requires an offsetting hedge.
* Persist live data for audit, replay, and future research.

### Non-Goals

* Directional/speculative trading.
* Multi-tenant SaaS.
* Heavy frontend/UI.
* Multi-outcome or negative-risk market making in the first live version.
* Dependence on pre-existing historical Polymarket order-book archives.
* Immediate cloud deployment as a requirement.

### Success Metrics

* 100% of candidate traded markets MUST be binary, reward-eligible, and satisfy total daily rewards `>= 20`.
* 100% of quoted or simulated quotes MUST pass metadata/token/book consistency checks before admission.
* 100% of quoted or simulated quotes MUST pass the hedgeability check for full configured size.
* Shadow-mode quote evaluation SHOULD recompute within 50 ms p95 after a relevant market event, excluding external network latency.
* Live-mode fill-to-hedge-intent creation SHOULD complete within 150 ms p95, excluding external network latency.
* The MVP MUST produce reports that explain why a market was selected, what size was considered hedgeable, what quote set would be posted, and why a trade would or would not be attractive.
* The live bot MUST record zero unresolved unhedged exposure incidents beyond the configured timeout in normal operation.

## 4. Users & Personas

### Persona 1 — Owner-Operator

* Runs the bot locally on a Mac Mini-class machine.
* Wants maximum backend performance and minimal operational complexity.
* Needs explicit controls, kill switches, and clear reporting.

### Persona 2 — Strategy Researcher

* Wants to validate math and improve the strategy using real captured data.
* Needs normalized market, book, reward, quote, fill, and hedgeability datasets.

## 5. MVP Scope

### MVP Scope — Real-Data Shadow Mode

The MVP MUST:

* use real Polymarket market metadata and live order-book data,
* discover all binary reward-eligible markets meeting the reward threshold,
* normalize and verify market identity across sources,
* maintain local YES/NO books,
* compute four candidate quote legs,
* compute **immediate opposite-outcome hedgeability** using depth-weighted liquidity,
* estimate reward viability,
* report which quotes/markets would be traded,
* avoid sending live orders.

### Full Product Scope — Live Hedged Bot

The full product adds:

* L1/L2 auth,
* account/order/position ingestion,
* live order placement and cancel-replace,
* fill-triggered offsetting hedge execution,
* inventory split/merge support,
* kill switches and exposure management.

### In Scope

* Rust backend/core.
* Public market discovery and reward ingestion.
* REST bootstrap and WebSocket book maintenance.
* Rule-based market universe: all binary markets meeting the reward threshold.
* Shadow mode for real-data strategy validation.
* Live mode for actual trading after shadow validation.
* Postgres operational storage plus research archive files.

### Out of Scope

* Heavy frontend.
* Manual market allowlists by type/name as a required filter.
* Multi-outcome and negative-risk MM in initial live release.
* Cross-exchange hedging.
* Predictive alpha models.

## 6. User Journeys & Key Flows

### Journey 1 — Shadow Discovery and Selection

1. System fetches candidate markets and reward data.
2. System filters to binary, active, accepting-order, reward-eligible markets with total daily rewards `>= 20`.
3. System reconciles condition IDs, token IDs, outcomes, and reward settings.
4. System bootstraps YES and NO books.
5. System computes hedgeable size, reward viability, and candidate quote set.
6. System reports which markets look tradable and why.

**Edge cases**

* Market appears reward-eligible but books are stale.
* Token mapping mismatch across sources.
* Market passes reward filter but fails hedgeability.
* Reward config changes while monitored.

### Journey 2 — Shadow Quote Evaluation

1. System maintains live books using REST bootstrap plus WebSocket updates.
2. Quote planner proposes four candidate quotes.
3. Hedgeability engine computes full-size immediate opposite-outcome offset cost using level-by-level depth.
4. Strategy engine scores the opportunity.
5. Report engine records whether the market would be traded.

**Edge cases**

* Top-of-book looks good, but deeper liquidity makes full hedge too expensive.
* A quote is valid on price/tick grounds but fails full-size hedgeability.
* One or more quote legs are suppressed because asks would require owned inventory later in live mode.

### Journey 3 — Live Order Lifecycle

1. System posts approved passive limit orders.
2. One or more resting orders fill.
3. Exposure engine computes deviation from target inventory/exposure.
4. Hedge engine executes the pre-modeled opposite-outcome hedge if required.
5. System returns to target posture or kill-switches the market.

**Edge cases**

* Partial fill.
* Opposite-side depth disappears between quote time and fill time.
* Account state lags user feed.
* Market transitions state mid-session.

### Journey 4 — Session Review

1. System generates summary reports.
2. Researcher reviews selected vs rejected markets.
3. Researcher reviews reward-vs-cost estimates and hedgeability decisions.
4. Later live trading is enabled only after shadow outputs look correct.

## 7. Functional Requirements (Epics → Stories → AC)

### Epic A — Market Discovery and Reward Intake

**FR-1 (P0) — Discover candidate markets**
Story: The system MUST discover candidate markets from Polymarket public market surfaces.
Acceptance Criteria:

* Given a discovery cycle, when the system runs, then it fetches candidate binary markets and their reward metadata from public market endpoints. ([Polymarket Documentation][1])
* Given a candidate market, when normalized, then it includes condition ID, market slug/question, outcomes, token IDs, active/closed/archive/accepting-orders status, and reward configuration if present. ([Polymarket Documentation][1])
* Given a market with more than two outcomes, when evaluated, then it is rejected for this strategy version.

**FR-2 (P0) — Reward-threshold market universe**
Story: The system MUST use a rules-based universe instead of a manual name/type allowlist.
Acceptance Criteria:

* Given discovered markets, when eligibility is computed, then the system includes only binary reward-eligible markets with summed `rewards_daily_rate >= 20`. ([Polymarket Documentation][1])
* Given reward entries, when multiple rates exist, then the system sums them into one `daily_reward_total`.
* Given reward data is missing or malformed, when eligibility is computed, then the market is rejected.

**FR-3 (P0) — Canonical market identity**
Story: The system MUST reconcile one canonical market record across metadata and book surfaces.
Acceptance Criteria:

* Given multiple sources, when reconciliation runs, then mismatched condition IDs, token IDs, or outcome labels cause rejection.
* Given successful reconciliation, when stored, then one canonical record maps `condition_id -> YES token -> NO token`.
* Given canonical identity changes after admission, when detected, then the market is quarantined.

### Epic B — Live Market Data Foundation

**FR-4 (P0) — REST bootstrap**
Story: The system MUST bootstrap each YES/NO book from REST before live streaming.
Acceptance Criteria:

* Given an admitted market, when bootstrapping begins, then the system fetches both YES and NO order books by token ID. ([Polymarket Documentation][4])
* Given a returned book, when normalized, then bids, asks, timestamp, market/asset IDs, and last-trade context if available are stored. ([Polymarket Documentation][4])
* Given no successful bootstrap, when quote evaluation is attempted, then the market remains non-tradable.

**FR-5 (P0) — WebSocket maintenance**
Story: The system MUST maintain local state using market WebSocket updates and controlled resync.
Acceptance Criteria:

* Given subscribed asset IDs, when the market channel emits book/price/trade/lifecycle updates, then the local book state is updated atomically. ([Polymarket Documentation][5])
* Given detected drift or suspected gaps, when triggered, then the system performs a REST resync.
* Given degraded state, when safety cannot be guaranteed, then the market is withheld from quoting.

**FR-6 (P1) — User/account stream intake**
Story: The live system MUST ingest authenticated user order/trade events.
Acceptance Criteria:

* Given live mode, when the user channel emits order/trade events, then local order and exposure state is updated. ([Polymarket Documentation][6])
* Given reconnect, when state may be incomplete, then the system reconciles against authenticated open-order endpoints. ([Polymarket Documentation][7])
* Given shadow mode, when no trading is enabled, then user-channel ingestion may be disabled.

### Epic C — Strategy Math and Quote Planning

**FR-7 (P0) — Four-quote model**
Story: The system MUST model four quote legs per admitted market.
Acceptance Criteria:

* Given a market state, when planning runs, then the engine evaluates `YES bid`, `YES ask`, `NO bid`, and `NO ask`.
* Given a leg is not viable, when outputs are produced, then the system records a suppression reason.
* Given shadow mode, when asks are modeled, then inventory assumptions MAY be simulated but must be labeled as such.

**FR-8 (P0) — Pre-trade full-size hedgeability**
Story: The system MUST only approve a quote size that is immediately hedgeable in full using opposite-outcome liquidity.
Acceptance Criteria:

* Given a candidate quote with size `q`, when hedgeability is computed, then the engine walks opposite-outcome book depth across price levels until `q` size is satisfied or liquidity is exhausted.
* Given sufficient depth, when cost is computed, then the system calculates weighted-average hedge price, total hedge cost, and slippage for the full `q`.
* Given insufficient depth or excessive hedge cost, when evaluated, then the quote is rejected or downsized.
* Given shadow reports, when displayed, then the output includes the maximum immediately hedgeable size.

**FR-9 (P0) — Reward-vs-cost viability gate**
Story: The system MUST only approve quotes when expected reward opportunity exceeds estimated hedge and execution cost.
Acceptance Criteria:

* Given a candidate quote set, when viability is computed, then the engine estimates reward opportunity, hedge cost, and adverse-selection penalty.
* Given estimated costs exceed configured threshold, when evaluated, then the quote set is rejected.
* Given uncertainty is high, when data is insufficient, then the market is downgraded or rejected.

**FR-10 (P0) — Passive-first behavior**
Story: The strategy MUST prefer passive reward capture and hedge only when a resting order fills.
Acceptance Criteria:

* Given no fill, when the market remains stable, then the system does not cross the spread to pre-hedge.
* Given a resting order fills, when directional imbalance appears, then the hedge engine is allowed to execute.
* Given shadow mode, when reporting, then the system indicates what hedge would have been executed only after the hypothetical fill.

### Epic D — Live Trading, Inventory, and Risk

**FR-11 (P1) — Trading auth**
Story: The live bot MUST support Polymarket’s documented L1/L2 authentication flow.
Acceptance Criteria:

* Given live mode, when auth initializes, then the system loads or derives API credentials and signs authenticated requests correctly. ([Polymarket Documentation][8])
* Given invalid credentials, when auth fails, then trading does not start.
* Given shadow mode, when no live orders are sent, then auth MAY be optional.

**FR-12 (P1) — Inventory awareness**
Story: The live bot MUST distinguish owned inventory from external market liquidity.
Acceptance Criteria:

* Given a YES ask, when live approval is checked, then the system verifies that owned YES inventory exists or can be intentionally created.
* Given a NO ask, when live approval is checked, then the system verifies that owned NO inventory exists or can be intentionally created.
* Given bids, when live approval is checked, then the system verifies sufficient collateral is available.
* Given inventory creation is enabled, when needed, then split/merge workflows may be used to create or collapse complete sets. Polymarket documents splitting USDC.e into equal YES/NO tokens and merging equal YES/NO tokens back into USDC.e. ([Polymarket Documentation][9])

**FR-13 (P1) — Offsetting hedge execution**
Story: The live bot MUST execute an offsetting opposite-outcome hedge when a fill creates unacceptable imbalance.
Acceptance Criteria:

* Given a resting order fill, when exposure deviates beyond tolerance, then the system creates an offsetting hedge intent immediately.
* Given the modeled opposite-outcome liquidity is still available, when the hedge executes, then the system returns toward target exposure/inventory.
* Given the hedge cannot be completed within cost/time limits, when detected, then the system de-risks or kill-switches the market.

**FR-14 (P1) — Order lifecycle**
Story: The live bot MUST manage passive limit orders for approved quote intents.
Acceptance Criteria:

* Given live mode, when orders are sent, then they are submitted as limit orders and tracked locally. Polymarket documents that all orders are expressed as limit orders; marketable behavior is achieved by using a marketable limit. ([Polymarket Documentation][10])
* Given quote drift, when thresholds are exceeded, then the bot cancel-replaces.
* Given market disablement, when triggered, then open orders for that market are canceled.

**FR-15 (P1) — Hard risk controls**
Story: The system MUST enforce hard limits in shadow and live modes.
Acceptance Criteria:

* Given stale books, auth failure, identity mismatch, or unresolved drift, when detected, then the market is halted.
* Given live mode and unhedged exposure duration exceeds timeout, when detected, then the market is kill-switched.
* Given capital limits are exceeded, when detected, then no new quotes are approved.

### Epic E — Persistence, Reporting, and Research

**FR-16 (P0) — Durable event capture**
Story: The system MUST persist market, reward, book, quote, hedgeability, and decision events.
Acceptance Criteria:

* Given any relevant runtime event, when persisted, then it includes event time and ingest time.
* Given restart, when recovery runs, then monitored state can be rebuilt from storage plus fresh exchange sync.
* Given a finished session, when reported, then all selected/rejected markets and reasons are reconstructable.

**FR-17 (P0) — Shadow reports**
Story: The MVP MUST report what the bot would do using real data.
Acceptance Criteria:

* Given a market is evaluated, when reporting occurs, then the system outputs reward total, book summary, candidate quotes, max hedgeable size, estimated hedge cost, and final trade/no-trade decision.
* Given a market is rejected, when reporting occurs, then the system outputs explicit reasons.
* Given a hypothetical fill, when simulated, then the system outputs the offsetting hedge that would be attempted.

**FR-18 (P1) — Score proxy and calibration**
Story: The system SHOULD estimate reward competitiveness locally and calibrate when possible.
Acceptance Criteria:

* Given local books and reward configuration, when estimation runs, then the engine computes a local score proxy using documented reward logic as guidance. ([Polymarket Documentation][2])
* Given per-order scoring checks are available in live mode, when sampled, then the system uses them to calibrate assumptions. ([Polymarket Documentation][3])
* Given no official aggregate market-maker score endpoint is documented in the reviewed docs, when reporting occurs, then aggregate score estimates are labeled approximate.

## 8. Non-Functional Requirements

**NFR-1 (P0)** The system MUST use Rust for latency-sensitive components, and SHOULD use official Polymarket Rust SDK support where it reduces integration risk. ([Polymarket Documentation][11])

**NFR-2 (P0)** Market-data ingestion from WebSocket receipt to normalized in-memory update SHOULD complete in under 100 ms p95 on local hardware, excluding external network latency.

**NFR-3 (P0)** Shadow-mode recomputation after a relevant market event SHOULD complete in under 50 ms p95, excluding external network latency.

**NFR-4 (P1)** Fill-to-hedge-intent creation in live mode SHOULD complete in under 150 ms p95, excluding external network latency.

**NFR-5 (P0)** The system MUST prefer WebSocket-first ingestion over aggressive polling for live updates. Polymarket documents real-time market and user channels and publishes API rate limits. ([Polymarket Documentation][12])

**NFR-6 (P0)** Secrets MUST never be logged.

**NFR-7 (P0)** The system MUST fail closed on auth inconsistency or market-identity inconsistency.

**NFR-8 (P1)** Tracing SHOULD be optional and low-overhead.

## 9. Data Model & Integrations

### Core Entities

* **Market**

  * `condition_id`
  * `market_slug`
  * `question`
  * `active`
  * `closed`
  * `archived`
  * `accepting_orders`
  * `is_binary`
* **OutcomeToken**

  * `token_id`
  * `condition_id`
  * `outcome` (`YES`, `NO`)
  * `last_price`
* **RewardConfig**

  * `condition_id`
  * `daily_reward_rates[]`
  * `daily_reward_total`
  * `min_size`
  * `max_spread`
* **OrderBookSnapshot**

  * `token_id`
  * `exchange_ts`
  * `ingest_ts`
  * `bids[]`
  * `asks[]`
  * `best_bid`
  * `best_ask`
  * `mid`
* **QuoteCandidate**

  * `condition_id`
  * `leg`
  * `price`
  * `size`
  * `status`
  * `reason`
* **HedgeabilityReport**

  * `condition_id`
  * `trigger_leg`
  * `candidate_size`
  * `opposite_token_id`
  * `opposite_depth_available`
  * `max_hedgeable_size`
  * `weighted_avg_hedge_price`
  * `estimated_hedge_cost`
  * `slippage_bps`
  * `is_approved`
* **DecisionReport**

  * `condition_id`
  * `daily_reward_total`
  * `score_proxy`
  * `quote_set_status`
  * `would_trade`
  * `reasons[]`
* **Position**

  * `condition_id`
  * `yes_inventory`
  * `no_inventory`
  * `collateral_balance`
  * `net_exposure`

### External Integrations

* Public market metadata / candidate market surfaces. ([Polymarket Documentation][13])
* Public order-book REST. ([Polymarket Documentation][4])
* Public market WebSocket. ([Polymarket Documentation][5])
* Authenticated user channel and order endpoints for live mode. ([Polymarket Documentation][6])
* Split/merge inventory operations for live inventory management. ([Polymarket Documentation][9])

## 10. API Contracts

### External Contract A — Candidate Market Discovery

**Purpose:** fetch candidate binary reward markets.

**Normalized request**

```json
{
  "min_daily_reward": 20
}
```

**Normalized response**

```json
{
  "markets": [
    {
      "condition_id": "0xabc",
      "market_slug": "example-market",
      "question": "Example question?",
      "tokens": [
        { "token_id": "123", "outcome": "YES" },
        { "token_id": "456", "outcome": "NO" }
      ],
      "reward": {
        "daily_reward_total": 25,
        "min_size": 50,
        "max_spread": 0.03
      }
    }
  ]
}
```

### External Contract B — Book Bootstrap

**Purpose:** fetch initial YES/NO books.

**Request**

```json
{
  "token_id": "123"
}
```

**Response**

```json
{
  "token_id": "123",
  "timestamp": "1234567890",
  "bids": [
    { "price": "0.45", "size": "80" },
    { "price": "0.44", "size": "150" }
  ],
  "asks": [
    { "price": "0.46", "size": "100" },
    { "price": "0.47", "size": "90" }
  ],
  "best_bid": "0.45",
  "best_ask": "0.46"
}
```

### Internal Contract C — Hedgeability Check

**Purpose:** compute full-size immediate opposite-outcome hedgeability.

**Request**

```json
{
  "condition_id": "0xabc",
  "trigger_leg": "YES_BID",
  "candidate_size": 100,
  "opposite_book": {
    "levels": [
      { "price": 0.54, "size": 40 },
      { "price": 0.55, "size": 30 },
      { "price": 0.56, "size": 50 }
    ]
  },
  "constraints": {
    "max_hedge_cost": 56.0,
    "max_slippage_bps": 80
  }
}
```

**Response**

```json
{
  "condition_id": "0xabc",
  "trigger_leg": "YES_BID",
  "candidate_size": 100,
  "max_hedgeable_size": 100,
  "weighted_avg_hedge_price": 0.551,
  "estimated_hedge_cost": 55.1,
  "slippage_bps": 73,
  "is_approved": true
}
```

### Internal Contract D — Shadow Decision Report

**Purpose:** explain whether the bot would trade.

**Response**

```json
{
  "condition_id": "0xabc",
  "daily_reward_total": 25,
  "candidate_quotes": [
    { "leg": "YES_BID", "price": 0.45, "size": 100, "status": "APPROVED" },
    { "leg": "YES_ASK", "price": 0.46, "size": 100, "status": "SIMULATED_ONLY" },
    { "leg": "NO_BID", "price": 0.54, "size": 100, "status": "APPROVED" },
    { "leg": "NO_ASK", "price": 0.55, "size": 100, "status": "SIMULATED_ONLY" }
  ],
  "reward_viability": {
    "estimated_reward": 3.8,
    "estimated_hedge_cost": 1.2,
    "estimated_edge": 2.6
  },
  "would_trade": true,
  "reasons": []
}
```

## 11. UX Notes (IA, screens, states)

Primary interface is CLI plus generated reports.

### Modes

* **Shadow mode:** real data, no live orders.
* **Live mode:** real data plus trading.

### Runtime states

* startup
* syncing
* shadow-ready
* live-ready
* degraded
* halted
* shutdown

### Key outputs

* selected markets
* rejected markets with reasons
* per-market quote candidates
* per-leg max hedgeable size
* hypothetical hedge path
* live risk status

## 12. Observability & Analytics

### Logs

* structured JSON logs
* market IDs, token IDs, quote IDs, decision IDs
* no secrets

### Metrics

* discovery cycle latency
* book staleness
* resync count
* quote recompute latency
* hedgeability compute latency
* markets passing threshold
* markets rejected by reason
* live hedge timeout incidents
* score-proxy confidence

### Analytics events

* `market_discovered`
* `market_rejected`
* `market_selected`
* `quote_candidate_generated`
* `hedgeability_passed`
* `hedgeability_failed`
* `shadow_trade_approved`
* `shadow_trade_rejected`
* `live_order_posted`
* `live_fill_received`
* `live_offsetting_hedge_started`
* `live_offsetting_hedge_completed`
* `market_killswitched`

## 13. Security, Privacy, and Compliance

* Shadow mode SHOULD work without live trading credentials where possible.
* Live mode MUST use Polymarket’s documented auth model. ([Polymarket Documentation][8])
* Private keys and API secrets MUST remain local-only.
* The bot MUST fail closed on auth failure.
* The bot SHOULD separate read-only config from signing secrets.
* No unsupported bypasses or non-documented auth shortcuts are permitted.

## 14. Rollout Plan

### Stage 1 — MVP: Real-Data Shadow Mode

* market discovery
* reward thresholding
* market reconciliation
* REST + WebSocket books
* four-leg quote modeling
* hedgeability engine
* reward-vs-cost reports
* persistence

### Stage 2 — Live Trading Foundation

* auth
* account/order state
* inventory state
* user stream
* live-safe dry run against owned account

### Stage 3 — Live Bot

* passive order placement
* cancel-replace
* fill-triggered offsetting hedge execution
* hard kill switches
* small-cap pilot

### Stage 4 — Refinement

* score proxy calibration
* replay tooling
* parameter tuning

## 15. Risks, Dependencies, Open Questions

### Risks

* No documented aggregate market-maker score endpoint identified in the reviewed docs.
  Impact: reward-share forecasting may be approximate.
  Mitigation: local score proxy plus conservative thresholds. ([Polymarket Documentation][3])

* Opposite-side liquidity can disappear between evaluation and fill.
  Impact: live hedge cost may exceed modeled hedge cost.
  Mitigation: conservative size caps, slippage caps, timeout-based kill switches.

* Four-sided live quoting requires owned inventory for asks.
  Impact: some modeled quote sets may not be live-deployable without split inventory.
  Mitigation: separate shadow modeling from live inventory eligibility; support split/merge. ([Polymarket Documentation][9])

* Excess polling can create recovery or throttling issues.
  Impact: degraded market state.
  Mitigation: WebSocket-first runtime and bounded REST resync. ([Polymarket Documentation][14])

### Dependencies

* Polymarket public market and book APIs.
* Polymarket WebSocket market stream.
* Polymarket auth and user endpoints for live mode.
* Local Postgres instance.
* Local filesystem for research archive.

### Open Questions

* What exact max hedge-cost rule should gate approval?
* What exact slippage cap should gate approval?
* Should asks be fully modeled in shadow mode even when live inventory is not present?
* What is the initial per-market capital cap in live mode?
* What hedge timeout should trigger a kill switch?

## 16. Assumptions

* Rust is the best balance of speed, safety, and implementation practicality for this system, and Polymarket documents official Rust support. ([Polymarket Documentation][11])
* The full product is a live bot, but MVP is shadow mode on real data.
* Markets are selected by rule, not by manual type/name allowlist.
* Binary markets only for this version.
* A quote is only valid if its full fill size is immediately hedgeable using opposite-outcome depth at acceptable cost.
* The system should not pre-hedge; it should hedge only after a fill.
* Inventory means **owned YES/NO tokens and available collateral**, not external market depth.
* Historical research begins with data captured by this system.

## 17. Appendix (Glossary, References)

### Glossary

* **Shadow mode:** real data, no live order submission.
* **Offsetting hedge execution:** the concrete opposite-outcome trade taken after a fill to restore target exposure/inventory.
* **Immediate hedgeability:** the ability to offset the full intended fill size right away using current opposite-outcome depth.
* **Inventory:** owned YES, owned NO, and collateral balances.
* **Complete set:** one YES plus one NO for the same market. Polymarket documents split/merge workflows around YES/NO complete sets. ([Polymarket Documentation][9])

### References

* Polymarket liquidity rewards docs. ([Polymarket Documentation][2])
* Polymarket sampling markets docs. ([Polymarket Documentation][1])
* Polymarket order-book docs. ([Polymarket Documentation][4])
* Polymarket WebSocket overview and market/user channels. ([Polymarket Documentation][12])
* Polymarket authentication docs. ([Polymarket Documentation][8])
* Polymarket clients & SDKs docs. ([Polymarket Documentation][11])
* Polymarket inventory docs. ([Polymarket Documentation][9])
* Polymarket rate limits docs. ([Polymarket Documentation][14])

The next revision that would add the most value is locking the actual formulas for quote placement, quote sizing, and the precise hedge-cost threshold.

[1]: https://docs.polymarket.com/api-reference/markets/get-sampling-markets "Get sampling markets"
[2]: https://docs.polymarket.com/market-makers/liquidity-rewards "Liquidity Rewards"
[3]: https://docs.polymarket.com/api-reference/trade/get-order-scoring-status "Get order scoring status"
[4]: https://docs.polymarket.com/api-reference/market-data/get-order-book "Get order book"
[5]: https://docs.polymarket.com/market-data/websocket/market-channel "Market Channel"
[6]: https://docs.polymarket.com/market-data/websocket/user-channel "User Channel"
[7]: https://docs.polymarket.com/api-reference/trade/get-user-orders "Get user orders"
[8]: https://docs.polymarket.com/api-reference/authentication "Authentication"
[9]: https://docs.polymarket.com/market-makers/inventory "Inventory Management"
[10]: https://docs.polymarket.com/trading/orders/overview "Overview - Polymarket Documentation"
[11]: https://docs.polymarket.com/api-reference/clients-sdks "Clients & SDKs"
[12]: https://docs.polymarket.com/market-data/websocket/overview "Overview - Polymarket Documentation"
[13]: https://docs.polymarket.com/api-reference/markets/list-markets "List markets"
[14]: https://docs.polymarket.com/api-reference/rate-limits "Rate Limits"
