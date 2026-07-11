# SpreadEater Monitor — MVP Implementation Plan

This plan breaks the MVP defined in `prd.json` and `HANDOFF.md` into 7 independently testable increments, ordered by dependency chain. Each increment builds on the previous one.

**Source documents:** `agents/logging/prd.json`, `agents/archive/logging/HANDOFF.md`
**Bot operations guide:** `handoff.md` (project root)

---

## Increment Overview

| # | Name | Scope | Status |
|---|------|-------|--------|
| 1 | Workspace Setup + Core Types | Cargo workspace, shared event model crate | Phase A |
| 2 | Non-Blocking Producer + JSONL Writer | Event queue, file-based persistence | Phase A |
| 3 | Bot Instrumentation | Emit all 12 event types at decision points | Phase A |
| 4 | Postgres Projections | Log ingestor, projection materializer | Phase B |
| 5 | REST API + WebSocket | Axum HTTP server for projection data | Phase B |
| 6 | Terminal Operator View (TUI) | Ratatui live dashboard | Phase B |
| 7 | Browser Dashboard + Rebuild | Web UI, projection rebuild command | Phase B |

**Increments 1-3** cover PRD functional requirements FR-1 through FR-5 (domain event backbone + durable capture).
**Increments 4-7** cover FR-6 through FR-14 (indexing, operator surfaces, strategy audit semantics).

---

## Architecture Decision: Cargo Workspace

The project is converted to a 3-crate Cargo workspace:

- **`spreadeater-core`** — Shared types: EventEnvelope, payloads, reason codes, producer/writer traits
- **`spreadeater`** (existing bot) — Depends on core, adds `src/monitor/` submodule for producer + emitters
- **`spreadeater-monitor`** — Separate binary: log ingestor, Postgres projector, TUI, REST API, dashboard

```
Cargo.toml                    (workspace root, also bot package)
crates/
  spreadeater-core/
    Cargo.toml                (serde, chrono, uuid, rust_decimal, async-trait, serde_json, tokio)
    src/
      lib.rs
      envelope.rs             EventEnvelope, EventType, Priority, SchemaVersion
      payloads/
        mod.rs
        decision.rs           DecisionEventPayload
        order.rs              OrderSubmittedPayload, OrderCancelledPayload, OrderResizedPayload, FillDetectedPayload
        hedge.rs              HedgeIntentPayload, HedgeResultPayload, NeutralityPayload
        monitor.rs            MonitorDegradedPayload
      reason_codes.rs         CancelReasonCode enum
      producer.rs             EventProducer trait, QueueDepthSnapshot, ProducerError
      writer.rs               EventWriter trait, WriterHealth, WriterError
  spreadeater-monitor/        (created in Increment 4)
    Cargo.toml                (core + sqlx + axum + ratatui + tokio)
    src/
      main.rs                 Monitor CLI entry point
      ingestor/               JSONL tailing + parse
      projector/              Postgres upserts (idempotent)
      api/                    Axum REST + WebSocket
      tui/                    Ratatui terminal view
    migrations/               sqlx Postgres schema
src/                          (existing bot code, workspace member)
  monitor/                    (NEW submodule, created in Increment 2-3)
    mod.rs
    producer.rs               BoundedEventQueue implementation
    emitters.rs               EventEnvelope builder functions from bot domain types
    log_writer.rs             JSONL append-only file writer
tests/                        (NEW)
  unit/
  integration/
```

---

## Increment 1: Workspace Setup + Core Types

**PRD coverage:** FR-1 (canonical envelope), FR-12 (cancellation reason taxonomy)
**Goal:** Create workspace, define all shared types. Bot still compiles and runs identically.

### Steps

1. Convert root `Cargo.toml` to workspace:
   - Add `[workspace]` section with `members = [".", "crates/spreadeater-core"]`
   - Add `resolver = "2"`
   - Add `spreadeater-core = { path = "crates/spreadeater-core" }` to `[dependencies]`

2. Create `crates/spreadeater-core/Cargo.toml`:
   - Dependencies: serde, serde_json, chrono (with serde), uuid (v4), rust_decimal (serde-with-str), async-trait, tokio (sync feature), anyhow

3. Create core type files:

   **`envelope.rs`** — The canonical event envelope (HANDOFF section 3):
   ```rust
   EventEnvelope {
       event_id: Uuid,
       schema_version: SchemaVersion,  // (major: u16, minor: u16), initial: (1, 0)
       event_type: EventType,
       priority: Priority,
       occurred_at: DateTime<Utc>,
       recorded_at: DateTime<Utc>,
       run_id: String,
       cycle_id: Option<String>,
       trace_id: Option<String>,
       source_component: String,
       mode: String,                   // "shadow", "dry-run", "live"
       condition_id: Option<String>,
       market_slug: Option<String>,
       question: Option<String>,
       order_id: Option<String>,
       asset_id: Option<String>,
       hedge_id: Option<String>,
       payload: serde_json::Value,
   }
   ```

   **`EventType`** enum — 12 variants:
   - `DecisionEvaluated`, `QuoteApproved`, `QuoteRejected`
   - `OrderSubmitted`, `OrderResized`, `OrderCancelled`
   - `FillDetected`
   - `HedgeIntentCreated`, `HedgeResultRecorded`
   - `NeutralityEvaluated`
   - `MonitorDegraded`, `ProjectionRebuilt`

   **`Priority`** enum — `Critical`, `High`, `Normal`, `Debug` (with Ord derive for priority ordering)

   **`payloads/decision.rs`** — DecisionEventPayload (HANDOFF section 6):
   - candidate_quotes (Vec of quote leg summaries), reasons, effective_quote_size
   - expected_reward_usd_day, expected_hedge_cost_usd, expected_edge_usd, expected_edge_pct
   - committed_capital_usd, score_share, max_hedgeable_size

   **`payloads/order.rs`** — Order lifecycle payloads:
   - `OrderSubmittedPayload`: leg, side, price, size, token_id, neg_risk
   - `OrderCancelledPayload`: reason_code, reason_text, old_size, capital_delta
   - `OrderResizedPayload`: old_order_id, new_order_id, old_size, new_size, old_price, new_price, reason_code
   - `FillDetectedPayload`: trade_id, fill_price, fill_size, side, outcome, fallback_match

   **`payloads/hedge.rs`** — Hedge lifecycle payloads:
   - `HedgeIntentPayload`: trigger_order_id, trigger_leg, fill_size, fill_price, hedge_token_id, hedge_side
   - `HedgeResultPayload`: hedge_order_id, result_status, hedge_price, failure_reason, latency_ms
   - `NeutralityPayload`: pre_yes_size, pre_no_size, post_yes_size, post_no_size, residual_exposure, complete_sets, tolerance, is_neutral

   **`payloads/monitor.rs`** — Monitor health payloads:
   - `MonitorDegradedPayload`: component, degraded_reason, queue_depth, index_lag_ms

   **`reason_codes.rs`** — CancelReasonCode enum (HANDOFF section 7):
   - `QuoteDrift` — Price drifted beyond quote_drift_bps
   - `HedgeDepthBelowMinimum` — Opposite-side depth below min_size
   - `HedgeDepthPartialDownsize` — Depth below order size but above min_size
   - `MarketDeadmitted` — Market no longer meets admission criteria
   - `RiskHalt` — Risk manager triggered halt or kill switch
   - `ExternalCancel` — Order cancelled externally (UI, API)
   - Each variant has `code()` → short string and `description()` → human-readable text

   **`producer.rs`** — EventProducer trait:
   ```rust
   pub trait EventProducer: Send + Sync {
       fn emit(&self, event: EventEnvelope) -> Result<bool, ProducerError>;
       fn queue_depth(&self) -> QueueDepthSnapshot;
       fn is_degraded(&self) -> bool;
   }
   ```

   **`writer.rs`** — EventWriter trait:
   ```rust
   #[async_trait]
   pub trait EventWriter: Send + Sync {
       async fn write_batch(&self, events: &[EventEnvelope]) -> Result<usize, WriterError>;
       async fn flush(&self) -> Result<(), WriterError>;
       fn health(&self) -> WriterHealth;
   }
   ```

4. Create `tests/unit/core/` with serde round-trip tests for every type

### Verification
- `cargo build` succeeds for entire workspace
- `cargo test` passes all new serialization tests
- Bot runs unchanged: `cargo run -- show-config` works

---

## Increment 2: Non-Blocking Producer + JSONL Writer

**PRD coverage:** FR-4 (non-blocking producer), FR-5 (append-only event logs), NFR-1, NFR-3
**Goal:** Implement the queue and file-based event persistence.

### Steps

1. **`src/monitor/mod.rs`** — Module declaration

2. **`src/monitor/producer.rs`** — `BoundedEventQueue` implementing `EventProducer`:
   - Two `tokio::sync::mpsc` bounded channels:
     - Critical channel (capacity 256) — for Critical priority events
     - Normal channel (capacity 4096) — for High/Normal/Debug events
   - `emit()` uses `try_send()` — never blocks, never awaits
     - Returns `Ok(true)` if enqueued
     - Returns `Ok(false)` if dropped (queue full)
     - Critical events use the critical channel (never dropped unless truly full)
   - Background flusher task spawned via `tokio::spawn`:
     - Drains critical channel first (priority), then normal channel
     - Batches events (up to 100 per batch)
     - Calls `EventWriter::write_batch()` then `EventWriter::flush()`
     - Tick interval: 100ms, but critical events trigger immediate drain
   - `is_degraded()` tracks writer error state
   - `queue_depth()` returns per-priority counts

3. **`src/monitor/log_writer.rs`** — `JsonlFileWriter` implementing `EventWriter`:
   - On construction: creates `{event_log_dir}/{run_id}/` directory
   - `write_batch()`: serializes each event as one JSON line, appends to `events.jsonl`
   - `flush()`: calls `file.sync_data()` for durability
   - File handle kept open for duration of run
   - Health tracking: healthy if last write succeeded, degraded on IO errors

4. **`src/config.rs`** — Add `ObservabilityConfig`:
   ```rust
   pub struct ObservabilityConfig {
       pub enabled: bool,          // default: false
       pub event_log_dir: String,  // default: "./data/events"
   }
   ```
   Add `#[serde(default)]` field `pub observability: ObservabilityConfig` to `Config`

5. Tests:
   - Unit: BoundedEventQueue accepts events up to capacity, drops when full
   - Unit: Critical events use separate channel and survive normal channel saturation
   - Unit: emit() is synchronous and returns immediately (benchmark < 1ms)
   - Integration: JsonlFileWriter creates directory and file, appends valid JSONL
   - Integration: Full round-trip — emit → queue → flush → read back from JSONL → deserialize

### Verification
- Unit tests pass, producer benchmark < 1ms
- Integration test creates valid JSONL files
- Bot still compiles and runs with `observability.enabled = false` (default)

---

## Increment 3: Bot Instrumentation

**PRD coverage:** FR-2 (event type coverage), FR-3 (trace correlation), FR-11 (strategy audit), FR-13 (hedge neutrality)
**Goal:** Emit all 12 domain event types at correct decision points in the trading pipeline.

### Steps

1. **`src/monitor/emitters.rs`** — Pure builder functions (no side effects, easily testable):
   - `build_decision_evaluated(run_id, cycle_id, mode, market, report) -> EventEnvelope`
   - `build_quote_approved(run_id, cycle_id, trace_id, mode, market, candidate) -> EventEnvelope`
   - `build_quote_rejected(run_id, cycle_id, trace_id, mode, market, candidate) -> EventEnvelope`
   - `build_order_submitted(run_id, trace_id, mode, tracked_order) -> EventEnvelope`
   - `build_order_cancelled(run_id, trace_id, mode, tracked_order, reason_code) -> EventEnvelope`
   - `build_order_resized(run_id, trace_id, mode, old_order, new_order, reason_code) -> EventEnvelope`
   - `build_fill_detected(run_id, trace_id, mode, trade_event, order_id, fallback) -> EventEnvelope`
   - `build_hedge_intent(run_id, trace_id, hedge_id, mode, intent) -> EventEnvelope`
   - `build_hedge_result(run_id, trace_id, hedge_id, mode, result, latency_ms) -> EventEnvelope`
   - `build_neutrality_evaluated(run_id, trace_id, mode, condition_id, pre_pos, post_pos) -> EventEnvelope`
   - `build_monitor_degraded(run_id, component, reason, queue_depth) -> EventEnvelope`

2. **Modify `LiveEngine`** (`src/runtime/live_engine.rs`):
   - Add fields: `event_producer: Option<Arc<dyn EventProducer>>`, `run_id: String`, `trace_map: HashMap<String, String>` (order_id → trace_id)
   - `new()`: generate `run_id = format!("run_{}", Utc::now().format("%Y%m%d_%H%M%S"))`, init producer if config.observability.enabled
   - After `evaluate_market()`: emit `decision_evaluated`
   - After each approved/rejected quote candidate: emit `quote_approved` / `quote_rejected`
   - In `handle_user_event()` on fill match: emit `fill_detected`
   - After HedgeIntent construction: emit `hedge_intent_created`
   - After `execute_hedge()` returns: emit `hedge_result_recorded`
   - After post-hedge position sync: compute pre/post positions, emit `neutrality_evaluated`

3. **Modify `OrderManager`** (`src/trading/order_manager.rs`):
   - Add field: `event_producer: Option<Arc<dyn EventProducer>>`, `run_id: String`, `mode: String`
   - In `place_order()` success path: emit `order_submitted`
   - All cancel paths: emit `order_cancelled` with appropriate `CancelReasonCode`:
     - `cancel_replace_if_drifted()` → `QuoteDrift`
     - `cancel_all()` / `cancel_bids_only()` → accept reason parameter (MarketDeadmitted or RiskHalt)
     - External cancellation from WS → `ExternalCancel`
   - In `resize_order()`: emit `order_resized`

4. **Modify `main.rs`**: construct producer, pass to LiveEngine (and transitively to OrderManager)

5. **Trace ID flow**:
   - Generated at order placement time (UUID v4) in OrderManager
   - Stored in `LiveEngine::trace_map: HashMap<order_id, trace_id>`
   - On cancel-replace: replacement order inherits original's trace_id
   - On fill detection: look up trace_id from order_id → propagate to hedge intent, hedge result, neutrality
   - On fallback fill match (no tracked order_id): assign new trace_id

6. **Cancel reason code mapping** (exact code paths):
   | Bot code path | CancelReasonCode |
   |---|---|
   | `cancel_replace_if_drifted()` — drift exceeds threshold | `QuoteDrift` |
   | `check_hedge_depth()` — hedgeable < min_size | `HedgeDepthBelowMinimum` |
   | `check_hedge_depth()` — hedgeable < order size but >= min_size | `HedgeDepthPartialDownsize` |
   | `run_cycle()` — stale_cids (market no longer reward-eligible) | `MarketDeadmitted` |
   | `run_cycle()` — `!report.would_trade` (cancel bids) | `MarketDeadmitted` |
   | `kill_market()` — risk halt or hedge failure | `RiskHalt` |
   | WS OrderEvent with Cancellation type | `ExternalCancel` |

### Verification
- Unit tests: each emitter produces correct EventEnvelope with all required fields
- Unit tests: trace_id propagation (order → fill → hedge → neutrality)
- Unit tests: cancel reason code assigned correctly for each path
- Integration: run `cargo run -- dry-run-loop` with `observability.enabled = true`
- Integration: parse the JSONL output, verify all 12 event types present
- Integration: reconstruct at least one full trace (decision → order → fill → hedge → neutrality)

### Key files modified
- `Cargo.toml` — workspace + core dependency
- `src/config.rs` — ObservabilityConfig
- `src/main.rs` — producer construction
- `src/runtime/live_engine.rs` — producer injection, event emission (~10 points)
- `src/trading/order_manager.rs` — producer injection, reason codes on cancel methods

---

## Increment 4: Postgres Projections

**PRD coverage:** FR-6 (Postgres projections), FR-7 (rebuild capability)
**Goal:** Ingest JSONL event logs and materialize read-model tables in Postgres.

### Prerequisites from Increments 1-3
- `crates/spreadeater-core/` exists with all shared types
- Bot emits JSONL event files to `./data/events/{run_id}/events.jsonl`
- All 12 event types are being written with valid EventEnvelope schema

### Steps

1. Create `crates/spreadeater-monitor/` crate:
   - `Cargo.toml`: depends on `spreadeater-core`, `sqlx` (postgres, runtime-tokio, chrono), `tokio`, `clap`, `anyhow`, `serde_json`, `tracing`

2. SQL migrations (`crates/spreadeater-monitor/migrations/`):
   - `runs` table: run_id, mode, started_at, ended_at, observer_health, producer_lag_ms, index_lag_ms
   - `events_raw` table: sequential id, event_id (unique), all envelope fields, payload (JSONB)
   - `markets` table: condition_id + run_id composite PK, decision metrics, last_evaluated_at
   - `traces` table: trace_id PK, run_id, condition_id, market info, status, timestamps
   - `orders` table: order_id PK, trace_id, leg, side, price, size, matched_size, state, cancel_reason
   - `fills` table: fill_id PK, trace_id, order_id, price, size, fallback_match
   - `hedges` table: hedge_id PK, trace_id, trigger details, result_status, failure_reason, latency
   - `neutrality_evaluations` table: serial id, trace_id, pre/post positions, residual, complete_sets, is_neutral
   - `cancellations` table: serial id, order_id, reason_code, reason_text, size delta, capital delta
   - `positions_latest` table: condition_id + run_id PK, yes/no size, net exposure, complete_sets
   - Indexes on: run_id, trace_id, condition_id, event_type, occurred_at, reason_code

3. `PostgresProjector` implementing the `EventProjector` pattern:
   - Dispatches on `event_type` to specific handler functions
   - Each handler runs `INSERT ... ON CONFLICT DO UPDATE` (idempotent)
   - Batch processing with transaction wrapping
   - Tracks last processed event sequence number

4. `LogIngestor`:
   - Tails a JSONL file using `tokio::fs::File` + `BufReader` + `lines()`
   - Deserializes each line into `EventEnvelope`
   - Feeds batches to `PostgresProjector`
   - Handles file rotation (new run_id directories)
   - Tracks file offset for resume after restart

5. Monitor CLI (`main.rs`) with subcommands:
   - `ingest` — tail and project event logs
   - `rebuild` — truncate projections, re-ingest all logs (Increment 7)

### Verification
- Run bot in dry-run, generate JSONL events
- Run `spreadeater-monitor ingest`, verify all projection tables populated
- SQL queries: reconstruct a trace, list markets, check neutrality evaluations
- Idempotency: ingest same file twice, verify row counts unchanged

---

## Increment 5: REST API + WebSocket

**PRD coverage:** FR-9 (dashboard backend), FR-10 (search/filter/export), FR-14 (capital tracking)
**Goal:** Serve projection data through HTTP for operator surfaces.

### Steps

1. Axum app in `crates/spreadeater-monitor/src/api/`:
   - Bind to `127.0.0.1:{port}` (NFR-8: localhost only by default)

2. Endpoints (matching HANDOFF section 10):
   - `GET /api/v1/overview` — run summary: run_id, mode, health, lag, active_markets, open_orders, committed_capital, unhedged_markets
   - `GET /api/v1/markets/{condition_id}` — market detail + optional `?include_timeline=true`
   - `GET /api/v1/traces/{trace_id}` — full trace: decision, orders, fills, hedges, neutrality
   - `GET /api/v1/events?trace_id=&event_type=&limit=` — filtered event list with cursor pagination
   - `GET /ws/live` — WebSocket live stream with channels: overview, market, trace, alerts

3. WebSocket broadcasts via `tokio::sync::broadcast` channel, fed by the ingestor as new events are projected

### Verification
- Seed Postgres with test data, hit each endpoint with curl
- WebSocket client connects and receives live updates
- Error responses match spec (404, 400, 500, 503)

---

## Increment 6: Terminal Operator View (TUI)

**PRD coverage:** FR-8 (terminal-first operator view)
**Goal:** Live terminal dashboard using ratatui.

### Steps

1. Add `ratatui` and `crossterm` dependencies to monitor crate

2. `crates/spreadeater-monitor/src/tui/`:
   - Main layout: header (run health, lag, capital) + market table + detail panel + alerts strip
   - Market table columns: name, condition_id, decision status, expected edge, open capital, YES/NO size, net exposure, neutrality
   - Market drill-down: candidate quotes, approval reasons, hedgeability, positions
   - Trace drill-down: timeline of decision → order → fill → hedge → neutrality events
   - Keyboard navigation: j/k (rows), Enter (drill-down), q (quit), / (search), Tab (panel switch)

3. Data source: direct Postgres queries (same connection as projector)

4. Update loop: poll at 500ms or subscribe to internal broadcast channel

### Verification
- Run TUI alongside dry-run bot
- Verify all 4 operator journeys from HANDOFF section 5 are walkable
- Keyboard navigation works correctly

---

## Increment 7: Browser Dashboard + Rebuild

**PRD coverage:** FR-7 (projection rebuild), FR-9 (browser dashboard)
**Goal:** Web UI and projection rebuild command.

### Steps

1. Rebuild command in monitor CLI:
   - Truncates all projection tables
   - Re-ingests all JSONL files from event log directory
   - Emits `projection_rebuilt` event
   - Deterministic and idempotent (NFR-7)

2. Browser dashboard:
   - Minimal frontend (HTML + vanilla JS or htmx) served by the Axum app
   - Pages: overview, market detail, trace detail, health
   - Live updates via WebSocket
   - Read-only, no authentication (localhost only)
   - Dashboard being offline has zero impact on trading (NFR-2)

### Verification
- Populate DB, truncate, rebuild from logs — verify identical projection state
- Dashboard pages render with real data
- Compare trace counts before/after rebuild

---

## Critical NFRs Across All Increments

| NFR | Requirement | Covered By |
|-----|------------|------------|
| NFR-1 | Producer non-blocking, < 1ms p95 | Increment 2: `try_send()` with benchmark test |
| NFR-2 | Trading continues without monitor | Increment 2-3: `Option<Arc<dyn EventProducer>>`, config-gated |
| NFR-3 | Critical events flush < 250ms p95 | Increment 2: immediate flush on critical priority |
| NFR-5 | No secrets in event payloads | Increment 3: emitter functions never accept credential types |
| NFR-6 | Versioned event schemas | Increment 1: SchemaVersion(1, 0) on every envelope |

---

## Handoff Notes for Increments 4-7

### What Exists After Increments 1-3

1. **Cargo workspace** with `spreadeater-core` crate containing all shared types
2. **EventEnvelope** and all payload types in `crates/spreadeater-core/src/`
3. **EventProducer trait** and **EventWriter trait** in the core crate
4. **BoundedEventQueue** implementation in `src/monitor/producer.rs`
5. **JsonlFileWriter** in `src/monitor/log_writer.rs`
6. **Emitter functions** in `src/monitor/emitters.rs`
7. **JSONL event files** written to `./data/events/{run_id}/events.jsonl`
8. **Tests** in `tests/unit/` and `tests/integration/`

### Remaining Deliverables

1. **`crates/spreadeater-monitor/`** — Entire monitor binary crate (Increment 4+)
2. **Postgres migrations** — All projection tables
3. **Ingestor + Projector** — Read JSONL, materialize Postgres
4. **Axum API** — REST + WebSocket endpoints
5. **Ratatui TUI** — Terminal dashboard
6. **Browser dashboard** — Static HTML/JS served by Axum

### Key Integration Points

- The monitor reads `EventEnvelope` from JSONL files — use `serde_json::from_str()` per line
- All event types are in `spreadeater_core::envelope::EventType`
- All payload types are in `spreadeater_core::payloads::*`
- The `EventProjector` trait pattern is defined but not implemented — implement `PostgresProjector`
- The workspace root `Cargo.toml` already has `members = [".", "crates/spreadeater-core"]` — add `"crates/spreadeater-monitor"` to the members list
- Database URL comes from `config.json` field `persistence.database_url` or environment variable
