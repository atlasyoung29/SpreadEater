# Hedge Mutex Timeout — Remaining Gap

**Date filed:** 2026-04-11
**Priority:** Low-Medium
**Type:** Code change (evaluation needed)
**Source:** operator-follow-ups.md item #4

## Background

The hedge system uses a per-market mutex to prevent double-hedging between the FillHandler and reconciliation paths. Three layers of timeout protection have been implemented:

| Layer | Location | Status |
|-------|----------|--------|
| HTTP timeout (15s) | `src/trading/client.rs:89-90` | Implemented |
| Resolution-level `tokio::timeout` (10s) | `src/runtime/live_engine.rs:6355-6390` | Implemented |
| Market kill switch (10s unhedged) | `src/trading/risk.rs:107-129` | Active |

## Current Mutex Behavior

The per-market hedge mutex is defined at `live_engine.rs:123`:
```rust
hedge_locks: Arc<RwLock<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
```

Acquired at two sites:
- **FillHandler:** `live_engine.rs:5230-5233`
- **Reconciliation:** `live_engine.rs:3951-3953`

### Hold Duration
- **Normal case:** 1-3 seconds (API responds quickly)
- **Worst case:** ~45-50 seconds (3 sequential HTTP timeouts at 15s each: place_order + cancel + verify)

### What Happens During Hold
Full hedge execution flow while mutex is locked:
1. Pre-execution checks (position sync, market checks) — ~50-200ms
2. HTTP request `place_order` — up to 15s
3. 500ms sleep (`hedge_executor.rs:247`)
4. Cancel order — up to 15s
5. Verification GET request — up to 15s
6. Post-execution position sync and logging

## Existing Mitigations

The `tokio::timeout` wrapper at `live_engine.rs:6355-6390` wraps the entire `execute_resolution_plan` (hedge + sellback + position sync) with a 10-second timeout controlled by `config.risk.hedge_timeout_secs`. On timeout, it:
- Returns a failure result with `post_sync_source: "timeout"`
- Releases the mutex (the guard drops when the future is cancelled)
- Logs "Hedge execution timed out — releasing mutex"

The kill switch at `risk.rs:107-129` fires at 10s of unhedged exposure, halting the market. But it only sets a flag — it does NOT cancel the in-flight task or release the mutex.

## Remaining Concern

The 10-second `tokio::timeout` on the resolution plan DOES release the mutex, making the gap narrower than originally thought. However:

1. The individual `execute_hedge` call within the resolution plan is NOT separately wrapped
2. If the resolution includes both hedge + sellback, the 10s budget is shared between them
3. A slow but successful hedge (e.g., 8s) leaves only 2s for sellback before the resolution timeout fires

## Evaluation Needed

- Is the 10s resolution timeout sufficient, or should `execute_hedge` get its own dedicated timeout?
- Should the 500ms sleep in `hedge_executor.rs:247` be reduced or removed to free budget?
- Are there edge cases where the mutex could be held past 10s despite the resolution timeout?

## Files Involved
- `src/runtime/live_engine.rs` — mutex definition (123), acquisition (5230-5233, 3951-3953), resolution timeout (6355-6390)
- `src/trading/client.rs` — HTTP timeout (89-90)
- `src/trading/risk.rs` — kill switch (107-129)
- `src/trading/hedge_executor.rs` — 500ms sleep (247)
