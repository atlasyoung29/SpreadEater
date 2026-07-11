# SpreadEater Monitor — PRD

## 1. Summary

Build a local-first observability system for SpreadEater that introduces a structured domain event backbone and uses it to power three operator surfaces from one source of truth:

- a terminal-first operator view,
- an append-only event log plus Postgres index,
- a separate live dashboard app in the same monorepo.

The system MUST answer, in real time and after the fact:

- why a market was selected or rejected,
- why an order size was chosen or resized,
- how much capital is tied up in open orders and inventory,
- what expected yield or edge a trade offers,
- why an order was cancelled,
- what fill triggered what hedge,
- whether a hedge resulted in reconciled neutral inventory.

The design MUST protect trading performance. SpreadEater MUST emit events through a prioritized, non-blocking producer path. Heavy work such as indexing, querying, rendering, and dashboard updates MUST happen outside the trading hot path.

### Delivery Phases

- MVP: event contracts, producer queue, append-only event logs, Postgres projections, terminal operator view, minimal local dashboard, trace and export support.
- Phase 2: richer dashboard analytics, alerting, historical comparisons, deeper replay integration.
- Phase 3: optional operator controls, remote access hardening, advanced analytics.

## 2. Problem & Context

SpreadEater currently emits useful `tracing` logs across discovery, order placement, fills, hedging, and status, but the runtime story is fragmented:

- many logs use `condition_id` without a consistent market display name,
- the operator must reconstruct a lifecycle by reading unrelated lines in sequence,
- selection, sizing, cancellation, fill, and hedge decisions are not modeled as one traceable object,
- hedge success is not elevated to a first-class audited neutrality result,
- existing decision report archives cover market evaluation but not the full live lifecycle.

The repo already contains useful seams:

- live decision reports and archives,
- a generic `save_event` hook in the file archive layer,
- a dormant database config field,
- existing runtime modules for discovery, order management, fills, positions, and hedging.

The new monitoring system MUST unify those seams into one machine-readable event model instead of expanding raw human-readable logs.

## 3. Goals, Non-Goals, and Success Metrics

### Goals

- SpreadEater MUST emit structured domain events for decision, order, fill, hedge, position, risk, and monitor-health activity.
- The same event stream MUST power a terminal operator surface, a durable audit log, and a local live dashboard.
- Operators MUST be able to inspect both market-level summaries and per-trade lifecycle traces.
- The system MUST show expected and realized outcome metrics separately.
- The system MUST make hedge neutrality an explicit audited state, not an inferred side effect.
- The system MUST remain local-first and single-operator in MVP.

### Non-Goals

- The MVP MUST NOT introduce internet-facing multi-user monitoring.
- The MVP MUST NOT depend on the dashboard being online for trading to continue.
- The MVP MUST NOT replace the trading engine with a remote control plane.
- The MVP MUST NOT add strategy logic that changes order selection or hedge behavior.
- The MVP MUST NOT become the legal or accounting system of record.

### Success Metrics

- 100% of critical lifecycle events MUST include `condition_id`, market display name, event timestamp, source component, run ID, and correlation ID when applicable.
- 100% of order cancellations and resizes MUST have a normalized reason code.
- 100% of fills on tracked orders MUST link to either a hedge trace or an explicit failure trace.
- 100% of hedge traces MUST record a reconciled before/after exposure snapshot and a neutrality verdict.
- Producer enqueue overhead SHOULD be under 1 ms p95 on a healthy local host.
- Critical event flush to local event log SHOULD be under 250 ms p95 on a healthy local disk.
- Terminal and dashboard freshness SHOULD be under 1 second p95 from event occurrence to display.
- Monitor failure MUST NOT block order placement, cancellation, fill handling, or hedge execution.

## 4. Users & Personas

### Owner-Operator

- Runs SpreadEater locally.
- Needs to understand current behavior quickly during live trading.
- Needs immediate clarity on risk, open capital, cancellations, and hedge integrity.

### Strategy Researcher

- Reviews runs after the fact.
- Needs structured exports, filters, and comparisons.
- Needs expected-versus-realized data to debug strategy quality and execution quality separately.

### AI Auditor

- Consumes machine-readable logs and indexed traces.
- Needs deterministic event schemas, stable reason codes, and correlation IDs.
- Uses the data for debugging, replay analysis, and implementation iteration.

## 5. MVP Scope

### In Scope

- Shared domain event contracts in the monorepo.
- A prioritized, non-blocking producer inside SpreadEater.
- Append-only local event logs as the durable source of truth.
- A separate monitor app that tails or ingests logs and materializes Postgres projections.
- A terminal operator view built on the monitor data model.
- A local browser dashboard with live summaries and trace drill-down.
- Search, filtering, and export for runs, markets, traces, orders, and hedge outcomes.
- Expected and realized yield/edge metrics.
- Explicit hedge neutrality evaluation.

### Out of Scope

- Internet-facing hosting.
- Multi-user auth and RBAC.
- Cloud-only deployment.
- Automated alerting integrations to third-party services in MVP.
- Bot control actions from the UI in MVP.
- Rewrite of current trading strategy logic.

### MVP Boundaries

The MVP MUST deliver all three output surfaces in usable form, but they may differ in depth:

- terminal view: full operator-grade summary and trace inspection,
- event log + Postgres index: full-fidelity storage and query path,
- dashboard: read-only live visibility and drill-down, lighter than the terminal view.

## 6. User Journeys & Key Flows

### Journey 1 — Understand why a market was selected or rejected

1. Operator opens the terminal view or dashboard.
2. Operator sees active and recently evaluated markets with names, IDs, reward context, expected edge, expected yield, and status.
3. Operator selects a market.
4. System shows candidate quote legs, approved/rejected status, dynamic size, hedgeability, score share, and reasons.

Edge cases:

- market rejected due to hedgeability failure,
- market rejected due to reward viability,
- market halted by risk controls,
- market removed from management after reward eligibility changes.

### Journey 2 — Follow one trade lifecycle end to end

1. Operator selects a trace by order ID, fill event, or trace ID.
2. System shows decision event, order placement, resize/cancel activity, fill detection, hedge intent, hedge execution, and post-hedge reconciliation.
3. System highlights whether inventory became neutral within tolerance.
4. Operator exports the trace if needed.

Edge cases:

- fill matched via fallback logic,
- cancel-replace race with a recently cancelled order,
- hedge fails and kill switch triggers,
- reconciliation hedge occurs after downtime or WS gap.

### Journey 3 — Understand why an order was cancelled or resized

1. Operator filters to cancellations or resizes.
2. System groups them by reason code.
3. Operator opens a cancellation event and sees old size, new size if applicable, reason code, free-text explanation, and affected capital.

Edge cases:

- quote drift cancel-replace,
- hedge depth below minimum size,
- hedge depth partial downsize,
- market de-admitted,
- risk halt,
- external cancel or stale tracked order prune.

### Journey 4 — Audit expected versus realized outcome

1. Researcher opens a run or a specific trace.
2. System shows expected reward, expected hedge cost, expected edge, expected edge percent, committed capital, realized fill details, realized hedge details, and realized post-hedge position state.
3. Researcher compares decision quality against execution quality.

Edge cases:

- expected positive edge but failed hedge,
- expected edge was positive but realized outcome degraded,
- realized position is neutral but inventory exit orders remain open.

## 7. Functional Requirements (Epics → Stories → AC)

### Epic A — Domain Event Backbone

**FR-1 (P0) — Canonical event envelope**  
Story: SpreadEater MUST emit every monitoring event in one canonical envelope.
Acceptance Criteria:
- Given any emitted event, when serialized, then it includes `event_id`, `schema_version`, `event_type`, `priority`, `occurred_at`, `run_id`, `source_component`, and `mode`.
- Given any market-linked event, when serialized, then it includes `condition_id`, `market_slug`, and `question`.
- Given any trace-linked event, when serialized, then it includes `trace_id` and parent linkage when applicable.

**FR-2 (P0) — Domain event coverage**  
Story: The event backbone MUST cover decision, order, fill, hedge, position, risk, and monitor-health events.
Acceptance Criteria:
- Given a market evaluation, when completed, then a decision event is emitted with quote candidates, reasons, and expected metrics.
- Given order lifecycle activity, when it occurs, then placement, update, resize, cancellation, and failure events are emitted.
- Given fill or hedge lifecycle activity, when it occurs, then the system emits fill, hedge intent, hedge result, and reconciliation events.

**FR-3 (P0) — Lifecycle correlation**  
Story: The system MUST correlate one decision-to-order-to-fill-to-hedge lifecycle as a trace.
Acceptance Criteria:
- Given a newly approved quote, when the first order is created, then a trace ID is assigned or propagated.
- Given subsequent events for that lifecycle, when emitted, then they reference the same trace ID.
- Given one market has multiple simultaneous lifecycles, when queried, then each lifecycle is independently inspectable.

**FR-4 (P0) — Prioritized non-blocking producer path**  
Story: SpreadEater MUST emit monitoring events without blocking trading-critical work.
Acceptance Criteria:
- Given normal operation, when events are emitted, then enqueue is non-blocking and bounded.
- Given temporary overload, when low-priority status events accumulate, then they may be coalesced or dropped before critical events.
- Given observer or DB failure, when trading continues, then the bot remains operational and emits a monitor-degraded signal.

### Epic B — Durable Capture and Indexing

**FR-5 (P0) — Append-only event logs**  
Story: The bot MUST durably write append-only local event logs that act as the source of truth.
Acceptance Criteria:
- Given emitted events, when flushed, then they are written as append-only structured records partitioned by run or time bucket.
- Given a restart, when a new run begins, then a new run/session context is created without corrupting prior logs.
- Given later analysis, when rebuilding projections, then the event logs are sufficient to replay index state.

**FR-6 (P0) — Postgres projection index**  
Story: The monitor app MUST materialize Postgres projections for fast querying and UI reads.
Acceptance Criteria:
- Given new event logs, when ingested, then normalized tables for runs, markets, traces, orders, hedges, cancellations, and positions are updated.
- Given duplicate ingestion, when replayed, then projection updates are idempotent.
- Given a live UI query, when executed, then it reads from projections rather than raw bot logs.

**FR-7 (P1) — Rebuild and backfill**  
Story: The monitor app MUST be able to rebuild its Postgres index from event logs.
Acceptance Criteria:
- Given a fresh or corrupted index, when rebuild is triggered, then the monitor recreates projections from stored event logs.
- Given partial ingestion progress, when resumed, then processing continues deterministically.
- Given a completed rebuild, when compared to prior output, then trace and market summaries are consistent.

### Epic C — Operator Surfaces

**FR-8 (P0) — Terminal operator view**  
Story: The monitor MUST provide a terminal-first operator view optimized for live trading.
Acceptance Criteria:
- Given a live session, when the terminal view is opened, then it shows run health, lag, capital, active markets, open orders, unhedged markets, and recent critical events.
- Given a selected market, when inspected, then it shows market name, IDs, approved/rejected reasons, expected metrics, current orders, and current positions.
- Given a selected trace, when inspected, then the operator can follow decision, order, fill, hedge, and reconciliation events in order.

**FR-9 (P1) — Local live dashboard**  
Story: The monitor MUST provide a local read-only browser dashboard for live visibility.
Acceptance Criteria:
- Given the monitor app is running, when the operator opens localhost, then overview, market detail, trace detail, and health views are available.
- Given live events arrive, when the dashboard is open, then updates appear within the defined freshness target.
- Given the dashboard is not open, when the bot runs, then trading behavior is unchanged.

**FR-10 (P1) — Search, filter, and export**  
Story: The monitor MUST support targeted debugging and audit export.
Acceptance Criteria:
- Given a query by market, order ID, trace ID, or reason code, when executed, then matching data is returned.
- Given a filtered timeline, when exported, then the output is machine-readable.
- Given an AI or human audit flow, when reading data, then all event payloads and normalized fields are available.

### Epic D — Strategy Audit Semantics

**FR-11 (P0) — Selection, sizing, and yield explanation**  
Story: The monitor MUST explain why a market or order was selected and what the expected economics were.
Acceptance Criteria:
- Given a market evaluation, when recorded, then the event includes candidate quote legs, approved and rejected statuses, reasons, effective quote size, score share, expected reward, expected hedge cost, expected edge, and expected edge percent.
- Given capital is committed, when displayed, then open order notional and position notional are shown.
- Given expected metrics are shown, when rendered, then USD and percentage values are both available.

**FR-12 (P0) — Cancellation and resize reason taxonomy**  
Story: The monitor MUST normalize cancellation and resize reasons.
Acceptance Criteria:
- Given any cancellation or resize, when recorded, then a reason code is attached from a stable taxonomy.
- Given a cancellation reason, when displayed, then the UI shows both a short code and a human-readable explanation.
- Given a replace flow, when inspected, then old order ID and replacement order ID are linked.

**FR-13 (P0) — Hedge outcome and neutrality evaluation**  
Story: The monitor MUST record hedge outcome as an auditable lifecycle with reconciled neutrality status.
Acceptance Criteria:
- Given a fill on a tracked order, when hedge intent is created, then trigger details, target hedge instrument, and fill size are recorded.
- Given hedge execution completes or fails, when recorded, then the result includes order outcome, failure reason if any, and timing.
- Given post-hedge positions are synced, when evaluated, then the system records pre-state, post-state, residual exposure, complete-set size, tolerance, and `is_neutral`.

**FR-14 (P0) — Capital and exposure accounting**  
Story: The monitor MUST show how much money is in open orders, inventory, and hedged positions.
Acceptance Criteria:
- Given live tracked orders, when summarized, then open order notional is shown by market and globally.
- Given inventory exists, when summarized, then YES size, NO size, net exposure, and complete sets are shown.
- Given a market is fully hedged, when displayed, then the total paired size in the hedged market is shown.

### Epic E — Integration and Compatibility

**FR-15 (P1) — Coexistence with current logs and reports**  
Story: The new monitoring system MUST coexist with current logs and archive outputs during rollout.
Acceptance Criteria:
- Given the new monitor is enabled, when the bot logs, then human-readable logs remain concise and may reference trace IDs instead of repeating full detail.
- Given existing decision reports are still needed, when generated, then they may be preserved as derived outputs from the new event model.
- Given the monitor is disabled by config, when the bot runs, then existing trading behavior remains intact.

## 8. Non-Functional Requirements

- **NFR-1 (P0)** The producer path MUST be bounded and non-blocking for trading-critical code paths.
- **NFR-2 (P0)** Trading MUST continue when the monitor app, dashboard, or Postgres index is unavailable.
- **NFR-3 (P0)** Critical events SHOULD flush to local event logs within 250 ms p95 on healthy local hardware.
- **NFR-4 (P1)** Terminal and dashboard freshness SHOULD stay under 1 second p95 from event occurrence to render.
- **NFR-5 (P0)** Event payloads MUST NOT include private keys, API secrets, passphrases, or raw auth headers.
- **NFR-6 (P0)** Event schemas MUST be versioned and backward-compatible within one minor version.
- **NFR-7 (P1)** Projection rebuild from logs MUST be deterministic and idempotent.
- **NFR-8 (P1)** The dashboard MUST bind to localhost by default and require explicit opt-in for non-local exposure.
- **NFR-9 (P1)** The operator surfaces SHOULD support keyboard navigation and high-contrast readable states.
- **NFR-10 (P1)** The implementation SHOULD remain portable across local developer environments used by SpreadEater.

## 9. Data Model & Integrations

### Core Entities

**EventEnvelope**
- `event_id: uuid`
- `schema_version: string`
- `event_type: enum`
- `priority: enum(critical|high|normal|debug)`
- `occurred_at: timestamp`
- `recorded_at: timestamp`
- `run_id: string`
- `cycle_id: string?`
- `trace_id: string?`
- `parent_event_id: string?`
- `source_component: string`
- `mode: enum(shadow|dry-run|live)`
- `condition_id: string?`
- `market_slug: string?`
- `question: string?`
- `order_id: string?`
- `asset_id: string?`
- `hedge_id: string?`
- `payload: jsonb`

**DecisionEventPayload**
- market eligibility result
- quote candidates
- approved/rejected legs
- rejection reasons
- effective quote size
- score share
- expected reward USD/day
- expected hedge cost USD
- expected edge USD
- expected edge percent of committed capital
- committed capital USD
- max hedgeable size

**OrderEventPayload**
- leg, side, price, size, matched size
- order status
- order type
- replace linkage
- cancel reason code
- cancel reason text
- committed capital delta USD

**FillEventPayload**
- fill price
- fill size
- maker/taker order IDs
- matched trace linkage
- fallback-match flag

**HedgeEventPayload**
- trigger order/fill linkage
- hedge token ID
- hedge side
- hedge order ID
- hedge price/size
- success/failure
- failure reason
- latency fields

**NeutralityEvaluationPayload**
- pre-hedge YES size
- pre-hedge NO size
- post-hedge YES size
- post-hedge NO size
- net exposure before/after
- complete sets after
- neutrality tolerance
- `is_neutral`

**RunProjection**
- run ID, start/end time, mode, monitor health, lag metrics

**MarketProjection**
- market identifiers
- last decision status
- current expected metrics
- current open capital
- current position state
- current health flags

### Projection Tables

- `runs`
- `markets`
- `traces`
- `orders`
- `fills`
- `hedges`
- `neutrality_evaluations`
- `cancellations`
- `positions_latest`
- `events_raw_ingest`

### Integrations

- SpreadEater runtime emits events from discovery, strategy, order manager, user stream, hedge executor, risk manager, and position sync.
- Event logs are the durable source of truth.
- Postgres stores rebuildable read projections.
- The monitor app serves the terminal and dashboard views from projections plus live tail state.

## 10. API Contracts

### Local Monitor Read API

**GET `/api/v1/overview`**
Request:
```json
{}
```
Response:
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

**GET `/api/v1/markets/{condition_id}`**
Request:
```json
{
  "include_timeline": true
}
```
Response:
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

**GET `/api/v1/traces/{trace_id}`**
Request:
```json
{}
```
Response:
```json
{
  "trace_id": "trace_01H...",
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

**GET `/api/v1/events`**
Request:
```json
{
  "trace_id": "trace_01H...",
  "event_type": "order_cancelled",
  "limit": 200
}
```
Response:
```json
{
  "items": [],
  "next_cursor": null
}
```

### Local Live Stream

**GET `/ws/live`**
- Server MUST publish overview refreshes, critical alerts, market updates, and trace updates.
- The dashboard MAY subscribe only to the channels it renders.

## 11. UX Notes (IA, screens, states)

### Terminal View

Primary regions:
- global run header,
- market table,
- selected market detail,
- selected trace timeline,
- critical alerts strip.

Key columns:
- market name,
- condition ID,
- decision status,
- expected edge USD,
- expected edge %,
- open order capital,
- YES size,
- NO size,
- net exposure,
- neutrality status,
- last critical event.

States:
- healthy,
- observer lagging,
- DB catch-up,
- no live data,
- market halted,
- hedge failed,
- monitor degraded.

### Dashboard

Core pages:
- overview,
- market detail,
- trace detail,
- health/degraded state,
- exports.

The dashboard SHOULD feel secondary to the terminal view in MVP. It MUST remain read-only.

## 12. Observability & Analytics

### Internal Metrics

The system MUST expose:
- producer queue depth by priority,
- enqueue failures and coalesced event counts,
- flush batch size and flush latency,
- event log write errors,
- ingest lag,
- projection lag,
- dashboard/live stream lag,
- rebuild throughput,
- monitor degraded state transitions.

### Analytics Events

- `decision_evaluated`
- `quote_approved`
- `quote_rejected`
- `order_submitted`
- `order_resized`
- `order_cancelled`
- `fill_detected`
- `hedge_intent_created`
- `hedge_result_recorded`
- `neutrality_evaluated`
- `monitor_degraded`
- `projection_rebuilt`

Each event SHOULD include stable reason codes and correlation IDs where applicable.

## 13. Security, Privacy, and Compliance

- MVP access model is local single-operator only.
- The monitor MUST bind to localhost by default.
- Event logs and projections MUST exclude secrets and sensitive auth material.
- The system SHOULD record identifiers needed for debugging but MUST avoid raw request signatures and private credentials.
- The monitor is an operational tool, not a compliance ledger or tax accounting system.

## 14. Rollout Plan

### Milestone 1 — Event Backbone
- Shared contracts defined.
- Bot emits structured events behind config/feature flag.
- Append-only log writer works.
Validation:
- Demo a live run where raw JSON events can reconstruct one full trace.

### Milestone 2 — Postgres Index
- Monitor app ingests event logs.
- Rebuild/backfill works.
Validation:
- Drop the DB and rebuild it from logs with matching trace counts.

### Milestone 3 — Terminal Operator View
- Terminal summary and trace inspection work live.
Validation:
- Operator can answer selection, cancellation, capital, and hedge-neutrality questions without reading raw logs.

### Milestone 4 — Local Dashboard
- Overview, market detail, and trace detail render from monitor projections.
Validation:
- Browser view updates within freshness target during a live run.

### Phase 2+
- richer realized analytics,
- alerting,
- run-to-run comparison,
- replay overlays,
- optional operator controls.

## 15. Risks, Dependencies, Open Questions

### Risks

- Event volume spikes may overwhelm low-priority rendering paths.
- Schema churn may fragment consumers if versioning is weak.
- Realized P&L and yield may become misleading if formulas are inconsistent.
- Hedge neutrality may appear delayed if position sync lags exchange reality.
- Local Postgres adds operational complexity compared with file-only logging.

### Dependencies

- Existing SpreadEater runtime modules for decision, orders, fills, hedges, and positions.
- Local filesystem durability for append-only logs.
- Local Postgres availability for indexed queries.
- Monorepo workspace support for separate app plus shared contracts.

### Open Questions

- Should the monitor eventually support operator actions such as halt, cancel, or acknowledge?
- What retention and pruning policy should be the long-term default once production usage patterns are known?
- Should existing decision report JSON files remain first-class outputs, or become fully derived artifacts from the event stream?
- How should realized yield be extended once inventory exit and redemption flows are fully accounted for?

## 16. Assumptions

- The monitor app will live in the same monorepo as a separate app/process.
- Event logs are the durable source of truth; Postgres is a rebuildable read model.
- Access is local single-operator in MVP.
- Expected edge percent is defined as `expected_edge_usd / committed_capital_usd * 100`.
- Realized outcome percent is defined as `realized_pnl_usd / committed_capital_usd * 100` for the traced lifecycle.
- Neutrality tolerance defaults to a small configurable value, assumed `0.001`, until a stricter market-driven unit is introduced.
- Low-priority status events may be coalesced under load; critical lifecycle events are prioritized.
- The dashboard is read-only in MVP.

## 17. Appendix (Glossary, References)

### Glossary

- **Run ID**: unique identifier for one bot process session.
- **Trace ID**: correlation ID for one decision-to-order-to-fill-to-hedge lifecycle.
- **Critical event**: monitoring event that materially affects auditability of orders, fills, hedges, risk, or neutrality.
- **Neutrality**: reconciled post-hedge state where YES and NO inventory are balanced within tolerance.
- **Complete sets**: paired YES and NO inventory in the same market.

### Engineering Notes

Suggested monorepo layout:
- root package remains `spreadeater`
- `apps/monitor/` for the monitor app
- `crates/observability_contract/` for shared event types and reason codes

Suggested config additions:
- `observability.enabled`
- `observability.event_log_dir`
- `observability.flush_interval_ms`
- `observability.queue_capacity`
- `observability.pg_url`
- `observability.dashboard_bind`
- `observability.neutrality_tolerance`

Suggested environment variables:
- `SE_MONITORING_ENABLED`
- `SE_MONITORING_DIR`
- `SE_MONITORING_PG_URL`
- `SE_MONITORING_BIND`
- `SE_MONITORING_QUEUE_CAPACITY`

### Definition of Done

- Structured events exist for decision, order, fill, hedge, risk, position, and monitor health.
- One full trade lifecycle can be reconstructed by trace ID.
- Operators can answer the original monitoring questions without reading raw bot logs.
- Append-only logs persist events durably.
- Postgres projections rebuild from logs.
- Terminal operator view and local dashboard both run against the same event model.
- Trading continues when the monitor app or Postgres is unavailable.
```
