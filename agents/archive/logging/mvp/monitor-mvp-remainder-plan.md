# SpreadEater Monitor MVP Remainder (Increments 4-7)

## Summary
Implement the remaining MVP as a new workspace member, `crates/spreadeater-monitor`, delivered in four sequential checkpoints: Postgres projections, Axum API/WebSocket, Ratatui TUI, and a React + Vite browser dashboard. Use Docker Compose for local Postgres bootstrap, keep the bot unchanged as the event producer, and make the monitor app the consumer of `./data/events` JSONL logs.

## Key Changes
### Monitor app foundation
- Add `crates/spreadeater-monitor` to the workspace with three operator-facing CLI commands:
  - `serve`: tail logs, project into Postgres, expose REST + WS, serve built SPA assets
  - `rebuild`: truncate projection tables, replay all event logs deterministically, repopulate projections
  - `tui`: terminal client that consumes the monitor API/WS, not raw logs
- Add repo-managed Docker Compose for Postgres only.
  - Default local URL: `postgres://postgres:postgres@127.0.0.1:54329/spreadeater_monitor`
  - Commit the compose file and document a one-command local bootstrap path
- Use monitor-specific CLI/env config, not the bot’s `config.json`.
  - `--database-url` or `DATABASE_URL`
  - `--event-log-dir` default `./data/events`
  - `--bind` default `127.0.0.1:8080`
  - `--web-dist` default `crates/spreadeater-monitor/web/dist`

### Checkpoint 1: Postgres projections
- Create SQLx migrations for:
  - `runs`
  - `events_raw`
  - `markets`
  - `traces`
  - `orders`
  - `fills`
  - `hedges`
  - `neutrality_evaluations`
  - `cancellations`
  - `positions_latest`
  - `ingestion_offsets`
- Store every ingested envelope in `events_raw` keyed by `event_id` for idempotency.
- Implement a log ingestor that tails `./data/events/<run_id>/events.jsonl`, tracks per-file offsets in `ingestion_offsets`, and survives restarts.
- Implement projector handlers per event family using `INSERT ... ON CONFLICT` upserts.
- Treat event logs as source of truth; `rebuild` replays from disk and recreates all derived tables.
- Record rebuild status in projections and emit `projection_rebuilt` as a projection-side event record.

### Checkpoint 2: REST API + WebSocket
- Add Axum endpoints exactly matching the handoff contract:
  - `GET /api/v1/overview`
  - `GET /api/v1/markets/{condition_id}?include_timeline=true|false`
  - `GET /api/v1/traces/{trace_id}`
  - `GET /api/v1/events`
  - `GET /ws/live`
- Add stable response DTOs for overview, market detail, trace detail, and filtered event lists.
- Use cursor pagination for `/api/v1/events` via `before_id` on `events_raw.id`.
- Broadcast WS frames as `{ channel, payload }` for `overview`, `market`, `trace`, and `alerts`.
- Bind to localhost by default and keep the API read-only.

### Checkpoint 3: TUI
- Build the TUI as an API/WS client so it can run alongside `serve` without duplicating ingestion.
- Ratatui layout:
  - header: run health, lag, capital, active markets, unhedged markets
  - left pane: active/recent markets
  - right pane: selected market or trace detail
  - bottom pane: recent critical events and alerts
- Keyboard navigation only for MVP: market list movement, detail toggle, trace drill-down, refresh/reconnect.
- Show explicit degraded states when API/WS is unavailable.

### Checkpoint 4: Browser dashboard
- Add `crates/spreadeater-monitor/web` as a React + Vite + TypeScript SPA with committed `package-lock.json`.
- Routes:
  - `/` overview
  - `/markets/:conditionId`
  - `/traces/:traceId`
- Use REST for initial fetch and WS for live updates.
- Serve the built SPA from Axum using static files from `web/dist` with `index.html` fallback.
- Keep the dashboard read-only and local-first; no auth, no operator actions.

## Public Interfaces / Additions
- New workspace crate: `spreadeater-monitor`
- New CLI surface:
  - `cargo run -p spreadeater-monitor -- serve`
  - `cargo run -p spreadeater-monitor -- rebuild`
  - `cargo run -p spreadeater-monitor -- tui`
- New HTTP surface:
  - `/api/v1/overview`
  - `/api/v1/markets/{condition_id}`
  - `/api/v1/traces/{trace_id}`
  - `/api/v1/events`
  - `/ws/live`
- New frontend app under `crates/spreadeater-monitor/web`
- New local infra file for Postgres bootstrap via Docker Compose

## Test Plan
- Projection layer:
  - migration smoke test against local Postgres
  - ingest a fixture JSONL run and assert all projection tables populate
  - replay the same fixture twice and assert row counts and trace state remain unchanged
  - run `rebuild` and assert output matches fresh ingestion
- API/WS:
  - integration tests for 200/400/404/503 cases on all endpoints
  - WS test that subscribing after ingestion receives overview and alert updates
- TUI:
  - unit tests for view-model shaping and state transitions from API/WS payloads
  - manual smoke: run `serve`, then `tui`, verify market drill-down and live updates
- SPA:
  - `npm run build` must succeed
  - minimal component/store tests for overview, market detail, and trace detail hydration
  - manual smoke: run `serve`, open dashboard, verify live updates and trace navigation
- End-to-end:
  - generate bot events with observability enabled
  - start Docker Postgres
  - run `serve`
  - verify REST, TUI, browser dashboard, then `rebuild`

## Assumptions and Defaults
- The remainder means implementing increments 4-7, not stopping after the backend.
- Delivery is sequential by checkpoint, with each checkpoint independently runnable before the next.
- Browser dashboard stack is React + Vite + TypeScript.
- Local Postgres is bootstrapped by a committed Docker Compose path.
- TUI is a client of the monitor API/WS, not a second ingestor.
- The bot’s existing event schema in `spreadeater-core` is the stable input contract for the monitor app.
- `agents/summary.md` is updated after each checkpoint with what was built and validated.
