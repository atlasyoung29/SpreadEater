# SpreadEater - Project Summary

## Overview
Polymarket hedged liquidity rewards bot written in Rust. Discovers reward-eligible binary markets, places two-sided quotes (bids + asks) to earn liquidity rewards, and automatically hedges fills on the opposite outcome to remain market-neutral.

## Architecture

### Core Modules
- **`src/auth/`** - API credentials (env vars), HMAC-SHA256 L2 request signing, EIP-712 order signing
- **`src/books/`** - REST order book bootstrap, WebSocket delta maintenance, in-memory BookManager
- **`src/config.rs`** - Layered config with defaults (strategy, risk, discovery, books, persistence)
- **`src/discovery/`** - REST client for Polymarket sampling-markets API, filter/reconcile pipeline
- **`src/models/`** - Market, OrderBook, Quote, Hedge, Decision, Order, Position, Events structs
- **`src/persistence/`** - File archive (JSON decision reports)
- **`src/reporting/`** - Shadow mode decision reports, logging
- **`src/runtime/`** - Shadow orchestrator, LiveEngine (full live trading), ReplayEngine
- **`src/strategy/`** - Quote engine (4-leg), hedgeability depth-walk, reward viability, score proxy, calibration
- **`src/trading/`** - TradingClient, PositionManager, UserStream (WS), RiskManager, OrderManager, HedgeExecutor

### Key Flows
1. **Discovery cycle** (every 5 min): fetch sampling-markets, filter by reward threshold, evaluate each market, place/update quotes
2. **Quote refresh** (every N seconds): re-read cached books, cancel-replace drifted orders
3. **Hedge depth check** (every 15s): verify opposite-side liquidity supports resting bids, scale down if needed
4. **Fill handling** (real-time): UserStream WebSocket detects fills, HedgeExecutor fires hedge (BUY: GTC+cancel, SELL: FOK) based on residual exposure
5. **Hedge reconciliation** (every cycle): detects unhedged inventory from missed fills (offline/WS gaps), auto-hedges
5. **Risk management**: per-market halt, global halt, hedge timeout tracking, exposure limits

### CLI Commands
`once`, `run`, `show-config`, `auth-check`, `dry-run`, `dry-run-loop`, `live`, `replay`

## Tech Stack
Rust 1.94.0, tokio, reqwest, serde, tokio-tungstenite, chrono, clap, rust_decimal, tracing, hmac/sha2, k256/sha3 (EIP-712)

### Monitor Observability (`crates/spreadeater-core/`, `tests/`)
- **`crates/spreadeater-core`** — Shared event model crate (workspace member since 2026-03-08)
  - `EventEnvelope` canonical schema (17 fields, schema v1.0)
  - `EventType`: 12 variants (DecisionEvaluated → ProjectionRebuilt)
  - `Priority`: Debug/Normal/High/Critical with `Ord` derive
  - Full payload type hierarchy: Decision, Order (4 types), Hedge (3 types), Monitor
  - `CancelReasonCode`: 6 variants with machine codes and human descriptions
  - `EventProducer` trait (non-blocking) and `EventWriter` async trait
- 25 serde round-trip unit tests in `tests/unit/core/` — all passing

### Monitor App (`crates/spreadeater-monitor/`, `docker-compose.monitor.yml`)
- **`crates/spreadeater-monitor`** — Monitor binary crate for MVP increments 4-7 (2026-03-09)
  - CLI surface:
    - `serve` — tails JSONL logs, projects to Postgres, serves REST + WebSocket + built SPA
    - `rebuild` — truncates projections, deterministically replays `./data/events`, emits `projection_rebuilt`
    - `tui` — ratatui client for the monitor API and live stream
  - Postgres migrations for:
    - `runs`, `events_raw`, `markets`, `traces`, `orders`, `fills`, `hedges`
    - `neutrality_evaluations`, `cancellations`, `positions_latest`, `ingestion_offsets`
  - Projector + ingestor:
    - stores every envelope idempotently in `events_raw`
    - maintains market, trace, order, fill, hedge, neutrality, cancellation, and position projections
    - resumes from `ingestion_offsets` and replays historical runs from disk
  - Axum monitor surface:
    - `GET /api/v1/overview`
    - `GET /api/v1/markets/{condition_id}`
    - `GET /api/v1/traces/{trace_id}`
    - `GET /api/v1/events`
    - `GET /ws/live`
  - Operator clients:
    - ratatui TUI that consumes REST + WebSocket rather than raw logs
    - React + Vite + TypeScript SPA in `crates/spreadeater-monitor/web`
    - local static asset serving from `web/dist` with SPA fallback
  - Local infra:
    - `docker-compose.monitor.yml` boots Postgres on `127.0.0.1:54329`
    - `package-lock.json` committed for the web app

### Monitor API / UI Validation
- `cargo run -p spreadeater-monitor -- rebuild --database-url postgres://postgres:postgres@127.0.0.1:54329/spreadeater_monitor`
  - replayed 14 event-log files and projected 3898 events
- `cargo test -p spreadeater-monitor`
  - 4 unit tests passing (`TuiConfig` WS URL derivation + event-type filter normalization)
- `cargo test -p spreadeater-monitor --test postgres_integration -- --ignored --nocapture`
  - 4 Postgres-backed integration tests passing (migrations, ingest/idempotency, rebuild consistency, REST/WS)
- `cargo build`
  - workspace build passes; existing root warnings remain in `src/trading/mod.rs`
- `npm run build` in `crates/spreadeater-monitor/web`
  - SPA build passes and `web/dist` is present
- Browser smoke with Playwright against the compiled monitor server
  - overview route rendered 19 market cards
  - market detail route loaded a real question title
  - trace detail route loaded directly and rendered lifecycle panels

## Current Status
- Stages 1-4 complete (shadow, live trading, hedging, score proxy refinement)
- Live mode operational with fill detection, residual-based hedging (BUY GTC+cancel / SELL FOK), conditional WS re-subscription
- Hedge reconciliation catches missed fills from downtime/restarts; cancel-replace race condition fixed
- Monitor MVP Increment 1 complete: Cargo workspace + `spreadeater-core` shared types (2026-03-08)
- Monitor MVP Increment 2-3 now includes non-blocking JSONL event capture plus producer-side fallbacks for partial fills missed by the user WebSocket
- Order updates and position-delta reconciliation both reduce tracked remaining size; when the originating order context is gone, the bot now emits a synthetic reconciliation fill trace and attaches reconciliation hedge events to that same trace
- Validated on 2026-03-09 with `cargo test` and `cargo build` using `CARGO_TARGET_DIR=.rust-build-tests`; remaining warnings are the pre-existing unused re-exports in `src/trading/mod.rs`
- Monitor MVP increments 4-7 are now implemented locally: rebuildable Postgres projections, read-only Axum API + WebSocket, ratatui TUI client, and a React/Vite browser dashboard served by the monitor process
- Monitor MVP stabilization now includes a dedicated ignored-by-default Postgres integration suite in `crates/spreadeater-monitor/tests/postgres_integration.rs`; it provisions per-test databases, verifies migrations, duplicate replay idempotency, rebuild consistency, API error/success cases, and WebSocket alert delivery
