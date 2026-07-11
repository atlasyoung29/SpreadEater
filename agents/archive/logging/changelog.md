# Changelog

## 2026-03-08 — Monitor MVP Increment 1: Workspace + Core Types

### Cargo Workspace Conversion (Cargo.toml)
- Root `Cargo.toml` converted to workspace with `members = [".", "crates/spreadeater-core"]` and `resolver = "2"`.
- `spreadeater-core = { path = "crates/spreadeater-core" }` added as bot dependency.
- `rust_decimal_macros = "1"` added as dev-dependency for test `dec!()` macros.

### New Crate: `crates/spreadeater-core`
- **`envelope.rs`**: `EventEnvelope` (17 fields), `EventType` (12 variants), `Priority` (4 levels with `Ord`), `SchemaVersion` struct with `V1_0` constant. Builder pattern via `with_*` methods.
- **`payloads/decision.rs`**: `DecisionEventPayload`, `QuoteLegSummary`.
- **`payloads/order.rs`**: `OrderSubmittedPayload`, `OrderCancelledPayload`, `OrderResizedPayload`, `FillDetectedPayload`.
- **`payloads/hedge.rs`**: `HedgeIntentPayload`, `HedgeResultPayload`, `NeutralityPayload`.
- **`payloads/monitor.rs`**: `MonitorDegradedPayload`.
- **`reason_codes.rs`**: `CancelReasonCode` enum (6 variants) with `code()` and `description()` methods.
- **`producer.rs`**: `EventProducer` trait (non-blocking `emit`, `queue_depth`, `is_degraded`), `QueueDepthSnapshot`, `ProducerError`.
- **`writer.rs`**: `EventWriter` async trait (`write_batch`, `flush`, `health`), `WriterHealth`, `WriterError`.

### Tests (`tests/`)
- `tests/core_types.rs` integration test harness with `tests/unit/core/` module tree.
- 25 unit tests covering serde round-trips for all types, display impls, builder methods, JSONL round-trip, priority ordering. All 25 pass.

### Verification
- `cargo build` — workspace compiles clean.
- `cargo test --test core_types` — 25/25 pass.
- `cargo run -- show-config` — bot runs identically, no behaviour change.

## 2026-03-08 — Hedge reconciliation + cancel-replace race fix

### Unhedged Position Reconciliation (live_engine.rs)
- New `reconcile_unhedged_positions` runs every cycle after position/order sync.
- Detects one-sided inventory (e.g. YES but no NO) with no resting bid — sign of a missed fill.
- Bootstraps fresh books and executes FOK hedge via HedgeExecutor.
- Catches fills that occurred during bot downtime (restart/deploy) or WebSocket gaps.

### Cancel-Replace Race Condition (order_manager.rs)
- Orders cancelled during cancel-replace/resize now move to a `recently_cancelled` grace buffer instead of being immediately deleted.
- `get_tracked_order` and `find_tracked_order` both check the grace buffer, so in-flight fill events arriving after cancel can still trigger hedges.
- Buffer auto-cleaned after 30 seconds each cycle.
- `find_tracked_order` fallback now also scans recently-cancelled orders.

## 2026-03-08 — Fix est_daily estimation + post-only crossing guard

### est_daily Estimation (live_engine.rs)
- `estimate_market_daily_reward` no longer uses `two_sided_q_min` for scoring. Each order earns independently — simple sum of per-order scores replaces Q1/Q2 split.
- Single-leg markets now show non-zero `est_daily` values (previously always $0.00).

### Post-Only Crossing Guard (order_manager.rs)
- "crosses book" 400 errors downgraded from `ERROR` to `WARN` since they're expected on fast-moving books.
- Bot already handled this gracefully (continues to next candidate); just reduced log noise.

## 2026-03-08 — WS parse logging + status log cleanup

### WebSocket Parse Logging (user_stream.rs)
- `parse_user_message` now logs every parse failure at `warn!` level (JSON errors, missing fields, invalid trade/order data) instead of silently returning None.
- Successfully parsed trade/order events logged at `info!` level with full context (`>>> WS TRADE EVENT received`, `>>> WS ORDER EVENT received`).
- Raw incoming messages logged at `debug!` level for troubleshooting.

### Status Log Cleanup (live_engine.rs)
- Status log now only shows markets with resting orders or inventory (skips idle markets).
- Summary line shows `active` and `idle` counts for visibility into total monitored set.

## 2026-03-08 — Fix fill detection + aggressive hedge execution

### Fill Detection (live_engine.rs)
- **Conditional UserStream re-subscription**: Only tear down/recreate WebSocket when managed market set actually changes. Prevents losing fills during reconnect gap that occurred every 5-min cycle.
- **Fallback fill matching**: `find_tracked_order` now falls back to matching by (condition_id, asset_id, side) when maker/taker order IDs are missing from trade events.
- **Diagnostic logging**: Unmatched trade events now emit a `warn!` with full context instead of being silently dropped.

### Aggressive Hedge Pricing (hedge_executor.rs)
- FOK hedge orders now use aggressive limit prices (0.99 for buys, 0.01 for sells) instead of tight slippage-buffer pricing.
- Depth walk kept as sanity check (rejects if zero liquidity), but no longer constrains the limit price.
- Guarantees fill on binary markets whenever any depth exists.

## 2026-03-08 — Fix duplicate orders on re-scan

- Added pagination support to `get_open_orders` (cursor-based)
- Fixed 401 from `LTE=` cursor encoding
- Conservative reconciliation: exchange-aware dedup guard

## 2026-03-08 — Score proxy and status logging

- Estimated daily reward per market in status logs
- Score proxy functions made public for use in LiveEngine

## 2026-03-09 — Monitor MVP increments 4-7

### New workspace crate: `crates/spreadeater-monitor`
- Added the monitor binary crate and workspace member with `serve`, `rebuild`, and `tui` commands.
- Added Postgres migrations for raw events, runs, markets, traces, orders, fills, hedges, neutrality, cancellations, positions, and ingestion offsets.
- Implemented an idempotent projector over the existing JSONL event logs plus resumable ingestion from `./data/events`.

### Monitor HTTP / live surface
- Added the Axum API:
  - `GET /api/v1/overview`
  - `GET /api/v1/markets/{condition_id}`
  - `GET /api/v1/traces/{trace_id}`
  - `GET /api/v1/events`
  - `GET /ws/live`
- WebSocket frames now broadcast `overview`, `market`, `trace`, and `alerts` payloads from projected state.

### Operator clients
- Added a ratatui TUI that consumes the monitor API and live WebSocket feed.
- Added a React + Vite + TypeScript dashboard under `crates/spreadeater-monitor/web` with overview, market, and trace routes.
- Built SPA assets are served by the monitor process from `web/dist`, and `package-lock.json` is now tracked.

### Local bootstrap / validation
- Added `docker-compose.monitor.yml` for local Postgres on `postgres://postgres:postgres@127.0.0.1:54329/spreadeater_monitor`.
- Rebuild smoke replayed 3898 events across 14 run logs.
- `cargo test -p spreadeater-monitor`, `cargo build`, and `npm run build` all pass.

## 2026-03-09 — Postgres integration suite for monitor MVP

### Dedicated monitor integration coverage
- Added `crates/spreadeater-monitor/tests/postgres_integration.rs` as an ignored-by-default suite for real Postgres verification.
- The suite provisions a fresh database per test, writes deterministic JSONL fixtures, and covers migration smoke, duplicate replay idempotency, rebuild consistency, REST status/error paths, and WebSocket overview/alert delivery.
- Added `cargo test -p spreadeater-monitor --test postgres_integration -- --ignored --nocapture` to the README monitor workflow.

### Behavioral fix found while hardening tests
- `runs.observer_health` no longer flips from `degraded` back to `healthy` just because a later non-degraded event, such as `projection_rebuilt`, is processed.
- Exposed small testability hooks by making one-shot ingestion callable (`LogIngestor::ingest_once`) and by factoring API router construction into `build_app(...)`.
