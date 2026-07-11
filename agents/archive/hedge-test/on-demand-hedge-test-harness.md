# On-Demand Hedge Test Harness — Layer 1 Implementation

**Date:** 2026-03-28
**Issue:** #19
**Branch:** `On-Demand-Hedge-Test-Harness`

## Purpose

Hedge correctness is the highest-risk part of the bot, but live validation opportunities are rare and expensive. This harness enables deterministic, on-demand testing of the full hedge pipeline — from fill detection through resolution planning, hedge execution, and position verification — without placing real orders or waiting for organic fills.

## What Was Built

### New CLI Command

```bash
cargo run -- hedge-test --scenario fixtures/hedge_scenarios/yes_bid_buy_hedge.json
```

Runs a deterministic hedge test scenario that:
1. Loads a JSON scenario file defining market state, fill event, and expected outcome
2. Spins up a mock HTTP server serving canned API responses
3. Constructs the real `FillHandler` with all production dependencies wired to the mock
4. Injects a synthetic fill into `handle_fill()` — the same code path used by the live bot
5. Captures the hedge execution result and compares against expectations
6. Reports PASS/FAIL with details

### New Files

| File | Purpose | Lines |
|------|---------|-------|
| `src/runtime/hedge_test.rs` | Full harness module — scenario types, mock server, harness construction, execution, reporting | ~600 |
| `fixtures/hedge_scenarios/yes_bid_buy_hedge.json` | Standard scenario: 373 YES shares filled at $0.74, ample NO depth → full hedge | 50 |
| `fixtures/hedge_scenarios/thin_book_split.json` | Thin book scenario: partial NO depth → split between hedge buy and sellback | 50 |

### Modified Files

| File | Change |
|------|--------|
| `src/runtime/live_engine.rs` | Promoted `FillHandler`, `FillWorkItem`, `ResolutionExecutionResult`, `SellbackExecutionResult`, `HedgeSignal` and key functions to `pub(crate)` — zero logic changes |
| `src/runtime/mod.rs` | Added `pub mod hedge_test;` |
| `src/main.rs` | Added `HedgeTest` CLI variant and match arm |
| `agents/changelog.md` | Added changelog entry |
| `agents/summary.md` | Updated recent work and test counts |

## Architecture

### Scenario Format (JSON)

```json
{
  "name": "yes_bid_buy_hedge",
  "description": "Standard YES bid fill → buy NO hedge",
  "market": {
    "condition_id": "0xabc123",
    "question": "Will Bitcoin reach $100k?",
    "yes_token_id": "tok-yes",
    "no_token_id": "tok-no",
    "daily_reward_total": "50.0",
    "max_spread": "0.04",
    "tick_size": "0.01"
  },
  "tracked_order": {
    "order_id": "order-1",
    "leg": "YesBid",
    "price": "0.74",
    "size": "373",
    "matched_size": "0"
  },
  "fill": {
    "trade_id": "trade-1",
    "side": "BUY",
    "price": "0.74",
    "size": "373"
  },
  "position": { "yes_size": "0", "no_size": "0" },
  "balance": "500.0",
  "yes_book": {
    "bids": [["0.73", "500"]],
    "asks": [["0.76", "500"]]
  },
  "no_book": {
    "bids": [["0.24", "500"]],
    "asks": [["0.26", "500"]]
  },
  "expected": {
    "success": true,
    "hedge_side": "Buy",
    "hedge_shares": "373"
  }
}
```

### Mock HTTP Server

Adapted from the existing `MockExchangeApiState` pattern in `live_engine.rs` tests:

| Route | Response |
|-------|----------|
| `GET /data/orders` | Empty orders (no resting orders) |
| `GET /positions` | Scenario position data |
| `GET /balance-allowance` | Scenario balance in atomic USDC |
| `GET /neg-risk/balance-allowance` | Same as above (for neg-risk markets) |
| `GET /book?token_id=<id>` | Scenario order book snapshot |
| `POST /order` | Mock order result (synthetic order ID) |
| `DELETE /order/<id>` | Mock cancel confirmation |

### Harness Construction

`HedgeTestHarness::from_scenario()` wires all 16 `FillHandler` dependencies:

1. **Mock server** → spawned on random port
2. **Config** → default with URLs pointed at mock
3. **TradingClient** → dry-run mode (place_order returns synthetic results, GET endpoints hit mock)
4. **BookManager** → pre-populated with scenario books
5. **BookRestClient** → pointed at mock for REST book fetches
6. **PositionManager** → pointed at mock for position sync
7. **RiskManager** → default config, balance pre-loaded
8. **OrderManager** → wired to dry-run TradingClient
9. **HedgeExecutor** → wired to TradingClient + BookManager
10. **Managed/Known markets** → pre-populated with scenario market
11. **ErrorLogger** → temp directory
12. **EventProducer** → None (lightweight)
13. **CTF Merger** → None (skip on-chain merge)
14. **Hedge locks, signals, baselines** → empty defaults

### Execution Flow

```
from_scenario() → build FillWorkItem → handle_fill()
    → apply_trade_fill (warn: no tracked order, continue)
    → acquire hedge lock
    → check risk (not halted)
    → look up market metadata
    → get pre-position
    → compute hedge params
    → prepare_market_for_resolution (hits mock for balance, orders, books)
    → plan_fill_resolution (book-aware cost-benefit analysis)
    → execute_resolution_plan_with_timeout
        → execute_hedge (dry-run: synthetic order)
    → post-resolution position sync (hits mock)
    → compare result against expected
```

## Visibility Promotions

The following items were promoted from private to `pub(crate)` in `live_engine.rs` with **zero logic changes**:

**Structs (with all fields):**
- `FillHandler` (20 fields)
- `FillWorkItem` (7 fields)
- `ResolutionExecutionResult` (7 fields)
- `SellbackExecutionResult` (4 fields)
- `HedgeSignal` (2 fields)

**Functions/Methods:**
- `FillHandler::handle_fill()`
- `FillHandler::emit_event()`
- `execute_resolution_plan_with_timeout()`
- `execute_resolution_plan()`
- `get_hedge_lock()`
- `hedge_exposure_tolerance()`

## Tests

11 new inline tests in `src/runtime/hedge_test.rs`:
- Scenario JSON round-trip deserialization
- Work item construction from scenario data
- Canonical market conversion
- Order book snapshot conversion
- Mock server endpoint verification (balance, neg-risk, orders, positions, books)
- Harness construction wiring
- Full harness run-through

**Total test count:** 533 passing (169 inline + 364 integration)

## Design Decisions

1. **Dry-run TradingClient** — avoids needing a private key for order signing while still exercising GET endpoints against the mock. `place_order` returns synthetic results; the hedge planning and resolution logic still runs fully.

2. **No pre-registration of tracked order** — `handle_fill()` warns but continues when `apply_trade_fill` finds no tracked order. The `FillWorkItem` carries all data needed for hedge execution, so pre-registering is unnecessary.

3. **Separate from existing Replay command** — the existing `Replay` CLI replays discovery/quote decisions. This harness tests the hedge execution path, which is a fundamentally different code path.

4. **JSON scenario files** — human-readable, easy to create from incident logs, version-controllable, and extensible for Layer 2 (event-sequence replay).

## Future Work (Layers 2 & 3)

- **Layer 2: Event-Sequence Replay** — replay raw upstream signals (delayed trades, duplicate IDs, cancellation races) to test fill attribution and reconciliation
- **Layer 3: Live-Execution Probe** — inject synthetic trigger but use real Polymarket execution for highest-fidelity validation
