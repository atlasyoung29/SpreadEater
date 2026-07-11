# SpreadEater Monitor — Handoff Guide

## 1. Overview

SpreadEater Monitor is a local-first structured observability system that explains trading decisions and order lifecycles without forcing operators to reconstruct behavior from fragmented raw logs.

**MVP Horizon:** Non-blocking domain event backbone, append-only event log, Postgres index, terminal operator view, and local live dashboard.

### Problem

The bot already emits tracing logs and decision reports, but selection, sizing, cancellations, fills, hedges, and neutrality outcomes are not modeled as one correlated lifecycle. The new system must be local-first, performance-safe, and usable both live and post-run.

### Goals

- Emit structured domain events for decision, order, fill, hedge, position, risk, and monitor-health activity.
- Power a terminal operator view, append-only event logs plus Postgres index, and a separate local live dashboard from the same source of truth.
- Allow operators to inspect market-level summaries and per-trade lifecycle traces.
- Show expected and realized outcome metrics separately.
- Make hedge neutrality an explicit audited state with before and after exposure snapshots.
- Protect trading performance by keeping the producer path bounded and non-blocking.

### Non-Goals (MVP)

- Internet-facing multi-user monitoring.
- Dependence on the dashboard being online for trading to continue.
- Remote control plane behavior.
- Changes to SpreadEater trading strategy logic.
- Using the monitor as the legal or accounting system of record.

---

## 2. Architecture

```
SpreadEater Bot                          Monitor App
┌──────────────────┐                    ┌──────────────────────┐
│  Trading Logic   │                    │  Log Ingestor        │
│        │         │                    │        │             │
│  Event Producer  │──► Append-Only ───►│  Postgres Projector  │
│  (non-blocking,  │    Event Logs      │        │             │
│   bounded queue) │    (source of      │  ┌─────┴──────┐      │
└──────────────────┘     truth)         │  │  REST API  │      │
                                        │  │  WebSocket │      │
                                        │  └─────┬──────┘      │
                                        └────────┼─────────────┘
                                                 │
                                        ┌────────┴─────────┐
                                        │ Terminal View (P0)│
                                        │ Dashboard    (P1) │
                                        └──────────────────┘
```

**Key design decisions:**

- Event logs are the durable source of truth. Postgres is a rebuildable read model.
- The monitor app lives in the same monorepo as a separate process.
- The bot never blocks on the monitor — observer failures degrade safely.
- Access is local single-operator only in MVP.

---

## 3. Domain Event Model

### Event Envelope

Every event uses one canonical envelope schema:

| Field | Type | Notes |
|---|---|---|
| `event_id` | uuid | Unique event identifier |
| `schema_version` | string | Versioned contract for consumers |
| `event_type` | string | Canonical domain event name |
| `priority` | string | `critical`, `high`, `normal`, or `debug` |
| `occurred_at` | timestamp | When the source event happened |
| `recorded_at` | timestamp | When the event was serialized or flushed |
| `run_id` | string | Bot process session identifier |
| `cycle_id` | string? | Discovery or refresh cycle if applicable |
| `trace_id` | string? | Lifecycle correlation identifier |
| `source_component` | string | Discovery, order manager, hedge executor, risk manager, etc. |
| `mode` | string | `shadow`, `dry-run`, or `live` |
| `condition_id` | string? | Canonical market identifier |
| `market_slug` | string? | Human-friendly market slug |
| `question` | string? | Display name for operator readability |
| `order_id` | string? | Order linkage when relevant |
| `asset_id` | string? | Token linkage when relevant |
| `hedge_id` | string? | Hedge lifecycle linkage when relevant |
| `payload` | jsonb | Event-specific structured content |

### Event Types

The backbone covers these domain event families:

| Family | Events |
|---|---|
| Decision | `decision_evaluated`, `quote_approved`, `quote_rejected` |
| Order | `order_submitted`, `order_resized`, `order_cancelled` |
| Fill | `fill_detected` |
| Hedge | `hedge_intent_created`, `hedge_result_recorded` |
| Position | `neutrality_evaluated` |
| Monitor health | `monitor_degraded`, `projection_rebuilt` |

---

## 4. Event Priority & Producer Path

### Priority Classes

- **Critical** — Fill, hedge result, neutrality evaluation, risk breaches. Must flush to disk within 250 ms p95.
- **High** — Order placement, cancellation, decision events.
- **Normal** — Status updates, position syncs.
- **Debug** — Verbose diagnostics, book snapshots.

### Non-Blocking Producer

- Enqueue overhead target: **< 1 ms p95** on healthy local hardware.
- Bounded queue with priority ordering — critical events are never dropped.
- Under overload, low-priority status events may be coalesced or dropped.
- On observer or DB failure, trading continues and a `monitor_degraded` signal is emitted.

---

## 5. Trade Lifecycle Tracing

The primary audit unit is the **trace** — a correlated lifecycle from decision through neutrality evaluation.

```
Decision → Order Placement → [Resize / Cancel-Replace] → Fill Detection
    → Hedge Intent → Hedge Execution → Neutrality Evaluation
```

- A `trace_id` is assigned when the first order is created for an approved quote.
- All subsequent events in that lifecycle reference the same `trace_id`.
- Multiple simultaneous lifecycles on one market are independently traceable.

### Operator Journeys

1. **Understand market selection** — Inspect active and recently evaluated markets with names, IDs, expected edge, expected yield, and status. Drill into candidate quote legs, approval status, hedgeability, dynamic size, and reasons.

2. **Follow one trade lifecycle** — Select a trace by ID, order ID, or fill event. Walk through decision, placement, resize/cancel, fill, hedge intent, hedge execution, and neutrality. Inspect before/after exposure and complete sets.

3. **Explain cancellations and resizes** — Filter to cancellation/resize events, group by reason code, inspect old order vs. replacement, size delta, and capital delta.

4. **Audit expected vs. realized outcome** — Compare expected reward, hedge cost, edge, and edge percent against fills, hedges, and realized post-hedge state.

---

## 6. Data Model

### DecisionEventPayload

| Field | Type | Notes |
|---|---|---|
| `candidate_quotes` | array | Quote legs with prices, sizes, and statuses |
| `reasons` | array\<string\> | Approved and rejected reasoning |
| `effective_quote_size` | decimal | Dynamic size used for evaluation |
| `expected_reward_usd_day` | decimal | Estimated reward opportunity |
| `expected_hedge_cost_usd` | decimal | Estimated hedge cost |
| `expected_edge_usd` | decimal | Net expected edge |
| `expected_edge_pct` | decimal | Expected edge over committed capital |
| `committed_capital_usd` | decimal | Capital basis for percentages |
| `score_share` | decimal | Estimated reward share |
| `max_hedgeable_size` | decimal | Maximum immediately hedgeable size |

### OrderLifecycleProjection

| Field | Type | Notes |
|---|---|---|
| `order_id` | string | Primary order identifier |
| `trace_id` | string | Lifecycle linkage |
| `leg` | string | YES bid, YES ask, NO bid, or NO ask |
| `price` | decimal | Placed or updated price |
| `size` | decimal | Placed or updated size |
| `matched_size` | decimal | Filled amount |
| `state` | string | Live, matched, cancelled, invalid, etc. |
| `cancel_reason_code` | string? | Normalized reason taxonomy |
| `replacement_order_id` | string? | Link to replacement on resize/cancel-replace |
| `committed_capital_delta_usd` | decimal | Capital effect of this order change |

### HedgeLifecycleProjection

| Field | Type | Notes |
|---|---|---|
| `hedge_id` | string | Hedge lifecycle identifier |
| `trace_id` | string | Links back to originating lifecycle |
| `trigger_order_id` | string | Order that led to hedge intent |
| `fill_size` | decimal | Trigger fill size |
| `fill_price` | decimal | Trigger fill price |
| `hedge_token_id` | string | Target hedge instrument |
| `hedge_side` | string | BUY or SELL |
| `hedge_order_id` | string? | Actual hedge order identifier when placed |
| `result_status` | string | Accepted, failed, reconciled, etc. |
| `failure_reason` | string? | Structured failure detail |

### NeutralityEvaluation

| Field | Type | Notes |
|---|---|---|
| `trace_id` | string | Lifecycle linkage |
| `pre_yes_size` | decimal | Position before hedge |
| `pre_no_size` | decimal | Position before hedge |
| `post_yes_size` | decimal | Position after hedge and reconciliation |
| `post_no_size` | decimal | Position after hedge and reconciliation |
| `residual_exposure` | decimal | Net exposure after reconciliation |
| `complete_sets` | decimal | Paired inventory in the market |
| `tolerance` | decimal | Configured neutrality tolerance (default 0.001) |
| `is_neutral` | boolean | Inventory-neutral plus reconciled verdict |

### RunProjection

| Field | Type | Notes |
|---|---|---|
| `run_id` | string | Bot process session identifier |
| `mode` | string | `shadow`, `dry-run`, or `live` |
| `started_at` | timestamp | Run start time |
| `ended_at` | timestamp? | Run end time |
| `observer_health` | string | `healthy`, `lagging`, `degraded`, or `rebuilding` |
| `producer_lag_ms` | integer | Producer-side observability lag |
| `index_lag_ms` | integer | Projection catch-up lag |

---

## 7. Cancellation Reason Taxonomy

All order cancellations and resizes carry a normalized reason code from a stable taxonomy:

| Reason | Description |
|---|---|
| Quote drift | Price drifted beyond `quote_drift_bps`, cancel-replace triggered |
| Hedge depth below minimum | Opposite-side depth below `min_size`, bid cancelled entirely |
| Hedge depth partial downsize | Opposite-side depth below order size but above `min_size`, bid scaled down |
| Market de-admitted | Market no longer meets admission criteria |
| Risk halt | Risk manager triggered market halt or kill switch |
| External cancel | Order cancelled externally (UI, API) |

Each cancellation event includes:
- Short reason code and human-readable explanation
- Old order ID and replacement order ID (for cancel-replace flows)
- Size delta and capital delta

---

## 8. Hedge Neutrality Audit

Hedge neutrality is an explicit audited state, not implied from positions.

**Evaluation flow:**
1. Fill detected on a tracked order → hedge intent created with trigger details, target instrument, and fill size.
2. Hedge execution completes or fails → result recorded with order outcome, failure reason if any, and timing.
3. Post-hedge positions synced → neutrality evaluated with pre-state, post-state, residual exposure, complete-set size, tolerance, and `is_neutral` verdict.

**Edge cases:**
- Fallback fill matching on missing order IDs
- Cancel-replace race with recently cancelled orders
- Hedge failure triggering kill switch
- Reconciliation hedge after downtime or reconnect gap

**Formulas:**
- `expected_edge_pct = expected_edge_usd / committed_capital_usd * 100`
- `realized_outcome_pct = realized_pnl_usd / committed_capital_usd * 100`
- Neutrality tolerance defaults to `0.001`

---

## 9. Operator Surfaces

### Terminal View (P0 — Primary live surface)

Live session display:
- Run health, lag, capital, active markets, open orders, unhedged markets, recent critical events
- Market drill-down: name, IDs, approved/rejected reasons, expected metrics, current orders, positions
- Trace drill-down: decision → order → fill → hedge → reconciliation event timeline

### Browser Dashboard (P1 — Secondary live surface)

- Local read-only dashboard on localhost (bound to localhost by default, explicit opt-in for non-local)
- Views: overview, market detail, trace detail, health
- Live updates within 1 second p95 freshness target
- Dashboard being offline has zero impact on trading behavior

### Search, Filter, and Export (P1)

- Query by market, order ID, trace ID, or reason code
- Export filtered timelines as machine-readable output
- Supports both human audit and AI audit workflows

---

## 10. API Reference

### GET /api/v1/overview

Returns run-level summary.

```json
{
  "run_id": "run_20260308_153000",
  "mode": "live",
  "observer_health": "healthy",
  "producer_lag_ms": 12,
  "index_lag_ms": 84,
  "active_markets": 6,
  "open_orders": 10,
  "committed_capital_usd": "342.50",
  "unhedged_markets": 0
}
```

Errors: `503 monitor not initialized`, `500 projection query failed`

### GET /api/v1/markets/{condition_id}

Returns market detail with optional timeline.

Query params: `include_timeline=true`

```json
{
  "condition_id": "0xabc",
  "market_slug": "election-example",
  "question": "Will example happen?",
  "decision_status": "approved",
  "expected_edge_usd": "1.42",
  "expected_edge_pct": "0.41",
  "expected_reward_usd_day": "2.05",
  "open_order_notional_usd": "88.00",
  "yes_size": "44",
  "no_size": "44",
  "net_exposure": "0",
  "is_neutral": true,
  "recent_events": []
}
```

Errors: `404 market not found`, `500 projection query failed`

### GET /api/v1/traces/{trace_id}

Returns full trade lifecycle trace.

```json
{
  "trace_id": "trace_01H",
  "market": {
    "condition_id": "0xabc",
    "market_slug": "election-example",
    "question": "Will example happen?"
  },
  "decision": {},
  "orders": [],
  "fills": [],
  "hedges": [],
  "neutrality": {
    "is_neutral": true,
    "residual_exposure": "0",
    "complete_sets": "44",
    "tolerance": "0.001"
  }
}
```

Errors: `404 trace not found`, `500 projection query failed`

### GET /api/v1/events

Returns filtered event list with cursor pagination.

Query params: `trace_id`, `event_type`, `limit` (default 200)

Errors: `400 invalid filter`, `500 projection query failed`

### GET /ws/live

WebSocket live stream.

Channels: `overview`, `market`, `trace`, `alerts`

Errors: `503 live stream unavailable`

---

## 11. Analytics Events

| Event | Trigger | Key Properties |
|---|---|---|
| `decision_evaluated` | Market evaluation completes | run_id, condition_id, market_slug, decision_status, expected_edge_usd, expected_edge_pct, max_hedgeable_size, reasons |
| `quote_approved` | Quote leg approved | trace_id, condition_id, leg, price, size, committed_capital_usd |
| `quote_rejected` | Quote leg rejected | trace_id, condition_id, leg, reason_code, reason_text |
| `order_submitted` | Order submission succeeds | trace_id, order_id, condition_id, leg, side, price, size |
| `order_resized` | Order resized or replaced | trace_id, old_order_id, new_order_id, old_size, new_size, reason_code |
| `order_cancelled` | Order cancelled or removed | trace_id, order_id, condition_id, reason_code, reason_text |
| `fill_detected` | Tracked fill matched | trace_id, order_id, trade_id, fill_price, fill_size, fallback_match |
| `hedge_intent_created` | Fill creates hedge intent | trace_id, trigger_order_id, condition_id, fill_size, fill_price, hedge_token_id, hedge_side |
| `hedge_result_recorded` | Hedge execution completes/fails | trace_id, hedge_order_id, result_status, failure_reason, latency_ms |
| `neutrality_evaluated` | Post-hedge reconciliation | trace_id, pre_yes_size, pre_no_size, post_yes_size, post_no_size, residual_exposure, complete_sets, is_neutral |
| `monitor_degraded` | Observer path enters degraded state | run_id, component, degraded_reason, queue_depth, index_lag_ms |
| `projection_rebuilt` | Postgres projection rebuilt from logs | rebuild_id, run_id, events_processed, duration_ms, status |

---

## 12. Success Metrics

| Metric | Target | Notes |
|---|---|---|
| Critical event identity completeness | 100% include condition_id, market name, run_id, timestamp, correlation ID | Makes traces usable by humans and agents |
| Cancellation reason coverage | 100% carry a normalized reason code | Supports operational debugging |
| Fill to hedge trace linkage | 100% link to hedge trace or explicit failure trace | Covers hot-path auditability |
| Neutrality evaluation coverage | 100% record before/after exposure + is_neutral | Makes hedge integrity explicit |
| Producer enqueue overhead | < 1 ms p95 | Protects trading performance |
| Critical event durability latency | < 250 ms p95 to local event log | Critical and high priority events |
| UI freshness | < 1 second p95 from event to render | Local live visibility |
| Trading isolation | 0 trading path blocks from monitor/Postgres outages | Observer failures degrade safely |

---

## 13. Non-Functional Requirements

| ID | Requirement | Priority |
|---|---|---|
| NFR-1 | Producer path bounded and non-blocking for trading-critical code | P0 |
| NFR-2 | Trading continues when monitor app, dashboard, or Postgres is unavailable | P0 |
| NFR-3 | Critical events flush to local logs within 250 ms p95 | P0 |
| NFR-4 | Terminal and dashboard freshness under 1 second p95 | P1 |
| NFR-5 | Event payloads must NOT include private keys, API secrets, passphrases, or raw auth headers | P0 |
| NFR-6 | Event schemas versioned and backward-compatible within one minor version | P0 |
| NFR-7 | Projection rebuild from logs deterministic and idempotent | P1 |
| NFR-8 | Dashboard binds to localhost by default, explicit opt-in for non-local | P1 |
| NFR-9 | Operator surfaces support keyboard navigation and high-contrast readable states | P1 |
| NFR-10 | Implementation portable across local developer environments | P1 |

---

## 14. Risks & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Event volume spikes overwhelm rendering paths | Terminal and dashboard lag or omit context | Priority classes, bounded queues, coalescing, explicit lag metrics |
| Schema churn breaks consumers or rebuilds | Monitor and stored data drift apart | Version schemas, backward-compatible minor revisions, validate replay in CI |
| Realized yield/PnL formulas become misleading | Operators make incorrect decisions | Explicit formulas, label expected vs. realized separately, extension points for exit accounting |
| Position sync lag delays neutrality verdicts | Hedge status appears uncertain briefly | Record execution result and reconciled neutrality as separate states with timestamps |
| Local Postgres introduces operational overhead | Monitor harder to run or recover | Event logs as source of truth, projections rebuildable, bot continues without Postgres |

---

## 15. Personas

| Persona | Job | Key Pain Points |
|---|---|---|
| **Owner-Operator** | Run SpreadEater locally, understand live behavior during trading | Fragmented logs, hard to track capital and hedge state, cancellation reasons hard to audit |
| **Strategy Researcher** | Review runs post-fact, compare expected vs. realized quality | Decision archives incomplete, expected/realized not together, filtering cumbersome |
| **AI Auditor** | Consume machine-readable traces to debug and guide changes | Raw logs expensive to parse, missing reason codes and correlation IDs, no auditable lifecycle object |

---

## 16. Durable Capture & Indexing

### Append-Only Event Logs (Source of Truth)

- Events written as append-only structured records partitioned by run or time bucket.
- New runs create new session contexts without corrupting prior logs.
- Logs are sufficient to replay and rebuild all projection state.

### Postgres Projections (Read Model)

- Normalized tables: runs, markets, traces, orders, hedges, cancellations, positions.
- Updates are idempotent on duplicate ingestion.
- Live UI queries read from projections, never raw bot logs.
- Full rebuild from event logs supported for recovery and schema evolution.

---

## 17. Assumptions

- The monitor app lives in the same monorepo as a separate app and process.
- Event logs are the durable source of truth; Postgres is a rebuildable read model.
- Access is local single-operator only in MVP.
- `expected_edge_pct = expected_edge_usd / committed_capital_usd * 100`
- `realized_outcome_pct = realized_pnl_usd / committed_capital_usd * 100` (for the traced lifecycle)
- Neutrality tolerance defaults to `0.001` until a stricter market-driven unit is introduced.
- Low-priority status events may be coalesced under load; critical lifecycle events are always prioritized.
- The dashboard is read-only in MVP.

---

## 18. Open Questions

- Should the monitor eventually support operator actions (halt, cancel, acknowledge)?
- What retention and pruning policy should become the long-term default after real usage patterns are observed?
- Should existing decision report JSON files remain first-class outputs or become fully derived from the event stream?
- How should realized yield be extended once inventory exit and redemption flows are fully accounted for?
