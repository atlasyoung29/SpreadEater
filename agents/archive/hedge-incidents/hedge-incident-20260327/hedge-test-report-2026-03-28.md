# Hedge Test Report - 2026-03-28

## Purpose

This report captures the current recommendation for an on-demand hedge test capability.

The goal is to make hedge behavior testable without waiting for organic live fills, while still preserving as much real Polymarket execution fidelity as possible.

This is motivated by the recent hedge incidents and by the current reality that hedge correctness is still the highest-risk part of the bot, but live validation opportunities are rare and expensive.

## 2026-03-28 Layer 3 Implementation Status

Layer 3 is now implemented on `On-Demand-Hedge-Test-Harness` as an **operator-only live-execution probe**.

2026-03-29 correction:
- Layer 3 no longer starts from delayed `/positions` truth, REST-synthesized trigger evidence, or a manually authored post-attribution work item
- the earlier share-sized BUY `FOK` trigger experiment was removed; the shared production validator now rejects BUY `FOK/FAK` with share semantics again
- trigger acquisition now uses an aggressive share-sized BUY `GTC` plus bounded cancel, so Layer 3 no longer weakens a shared production safety rail just to create the probe trigger
- Layer 3 now starts an authenticated user-stream subscription before placing the trigger order and waits for the real websocket `UserEvent::Trade(...)` for that tracked trigger order before hedging
- instead of manually constructing a `FillWorkItem`, the probe now seeds the acquired trigger order into `OrderManager`, dispatches the actual user-stream trade/order events through the existing replay helpers, and drains the real fill queue so `build_fill_work_item(...)` remains the Layer 3 attribution boundary
- live REST order/trade/position data is now diagnostics, cleanup, and final-flatness verification only; it is no longer allowed to synthesize or start the hedge trigger path
- Layer 3 trigger success now requires the normalized cumulative matching user-stream trigger shares to equal the requested trigger size exactly; partial trigger fills are reported as `trigger_partial_fill` and overshoots as `trigger_overshoot`
- if placement/REST evidence suggests a fill but the user stream never delivers a matching trigger trade, the probe flattens any acquired trigger inventory and fails conservatively instead of inventing a hedge signal from REST or position truth
- this keeps Layer 3 aligned with the normal bot trigger source semantics wherever feasible: authenticated user-stream trade first, positions later
- the main Layer 3 verdict now records three separate outcomes:
  - `meta_pass`: whether the run stayed on the production-faithful path after trigger placement
  - `standard_pass`: whether the hedge behavior matched the scenario expectations
  - `cleanup_result`: whether the out-of-band safety cleanup succeeded afterward
- cleanup now happens strictly after the main verdict and is excluded from both `meta_pass` and `standard_pass`
- the current mock-backed production-mirror baseline is regression-green:
  - `cargo test --test hedge_live_probe -- --nocapture` (`21/21`)
  - `cargo test --test hedge_test_harness -- --nocapture` (`8/8`)
  - `cargo test --test hedge_replay_harness -- --nocapture` (`9/9`)
  - `cargo test --bin spreadeater -- --nocapture` (`165/165`)
  - `cargo test`
- confidence that this is the right final Layer 3 boundary without changing `LiveEngine::run()` or adding meaningful hot-path overhead: **high**

Current behavior:
- adds the explicit `hedge-live-probe --scenario <path>` CLI
- acquires the trigger-side inventory live first with an aggressive share-sized `GTC` BUY and explicitly cancels any remainder after the bounded observation window
- starts the authenticated user stream first, lets it connect asynchronously like production, and only starts hedging if a real matching websocket `UserEvent::Trade(...)` is observed for the tracked trigger order
- requires `SPREADEATER_HEDGE_LIVE_PROBE_ARM=I_UNDERSTAND_REAL_ORDERS` before it can place live orders
- performs one-market preflight against live discovery, open orders, positions, balance, and both books
- aborts by default unless the target market is clean: no open orders and no directional inventory beyond normal hedge tolerance
- computes a bounded preflight trigger+hedge plan, enforces operator safety caps before any `POST /order`, and prints a warning banner with trigger leg, trigger size, trigger limit, planned hedge shares, sell-back shares, hedge notional, and CTF-merge status
- seeds one market into the real downstream engine state, runs a scoped production-mirror runtime that uses the normal `process_user_event(...)` path, real fill handling in `FillHandlerPostHedgeMode::Normal`, and the normal background market maintenance hooks without calling the full multi-market `LiveEngine::run()`
- records a separated verdict:
  - `meta_pass`
  - `standard_pass`
  - `cleanup_result`
- exits non-zero on any safety or standard/meta expectation mismatch, and also exits non-zero if the post-verdict cleanup fails

Explicit Layer 3 fidelity boundaries:
- the probe still creates the trigger fill itself instead of waiting for an organic passive quote fill
- the probe does not run the normal discovery/quoting loop
- cleanup remains probe-owned rather than long-lived runtime-owned, but it is now explicitly out-of-band and excluded from the main verdict
- within those boundaries, the hedge trigger and downstream hedge path now follow the live bot path as closely as possible; ordinary no-fill scenarios are standard failures, not meta-failures

Current live probe fixture shape:
- `name`
- `description`
- `market`
- `trigger`
  - `leg`
  - `shares`
  - `max_trigger_limit_price`
- `safety`
  - `max_trigger_notional_usdc`
  - `max_cleanup_notional_usdc`
- `expected`
  - bounded hedge expectations
  - cleanup fields may still exist in fixtures for compatibility, but they are no longer part of the main standard verdict

Current operator templates:
- `template_small_yes_buy_probe.json`
- `template_small_no_buy_probe.json`
- `scotus_mail_ballots_buy_probe_5.json`

Layer 3 validation on 2026-03-28:
- `cargo test --test hedge_live_probe -- --nocapture`
- `cargo test --test hedge_replay_harness -- --nocapture`
- `cargo test --test hedge_test_harness -- --nocapture`
- `cargo test --bin spreadeater -- --nocapture`
- `cargo test`

Layer 3 trigger-path validation on 2026-03-29:
- `cargo check --quiet`
- `cargo test --test hedge_live_probe -- --nocapture`
- `cargo test --test hedge_test_harness -- --nocapture`
- `cargo test --test hedge_replay_harness -- --nocapture`
- `cargo test --bin spreadeater -- --nocapture`
- `cargo test`

Layer 3 isolated websocket-trigger validation on 2026-03-29:
- the mock exchange now exposes a mock user websocket endpoint alongside REST
- `tests/hedge_live_probe.rs` uses the real `UserStream` parser/subscription code against that mock websocket
- the isolated trigger-path baseline now covers connect ACK, delayed trades, duplicate trades, order-only events, no-fill timeout, and partial-fill failure

Manual live validation was intentionally not run during implementation because it can place real orders and therefore can cost money.

Confidence that Layer 3 is implemented without adding meaningful overhead to the normal bot path: **high**.

## 2026-03-28 Layer 2 Implementation Status

Layer 2 is now implemented on `On-Demand-Hedge-Test-Harness` as a **pre-attribution event-sequence replay harness**.

Current behavior:
- adds the explicit `hedge-replay --scenario <path>` CLI
- replays raw `UserEvent::Trade` / `UserEvent::Order` sequences plus refresh-time checkpoints
- drives the real `LiveEngine` attribution path, including:
  - `build_fill_work_item(...)`
  - pending-fill fallback queuing + flushing
  - exchange-truth missed-fill detection during `refresh_quotes()`
  - orphaned-position recovery and reconciliation hedging
- drains the real fill-work queue into the real `FillHandler::handle_fill(...)` path
- asserts an ordered critical-event subsequence plus final hedge/risk outcome
- exits non-zero on any expected-vs-actual mismatch

Current replay fixture shape:
- `market`
- `setup`
  - `tracked_orders`
  - `recently_cancelled_orders`
  - `positions`
  - `cached_balance`
- `sequence`
  - `user_connected`
  - `user_trade`
  - `user_order_update`
  - `user_order_cancellation`
  - `refresh_quotes`
  - `flush_pending_fill_fallbacks`
  - `recover_orphaned_positions`
  - `reconcile_unhedged_positions`
- `exchange`
- `expected`
  - `critical_events`
  - final hedge / halt / exposure assertions

Current deterministic Layer 2 fixtures:
- `raw_trade_immediate_attribution.json`
- `order_update_fallback_partial_accounted.json`
- `exchange_sync_missing_fill.json`
- `reconciliation_orphan_recovery.json`
- `cancelled_order_not_misattributed.json`
- `duplicate_trade_id_deduped.json`

Layer 2 validation on 2026-03-28:
- `cargo test --test hedge_replay_harness -- --nocapture`
- `cargo test hedge_size_for_accounted_fill_ -- --nocapture`
- `cargo run -- hedge-replay --scenario fixtures/hedge_replay_scenarios/raw_trade_immediate_attribution.json`
- `cargo run -- hedge-replay --scenario fixtures/hedge_replay_scenarios/order_update_fallback_partial_accounted.json`
- `cargo run -- hedge-replay --scenario fixtures/hedge_replay_scenarios/exchange_sync_missing_fill.json`

Confidence that Layer 2 now validates the old “production fill-work-item sizing” concern in the correct place: **high**.

## 2026-03-28 Layer 1 Implementation Status

Layer 1 is now implemented on PR 27 as a **post-attribution deterministic harness**.

Current behavior:
- injects an explicit `trigger.work_item` instead of replaying raw trade attribution
- reuses the real `FillHandler::handle_fill(...)` path
- uses a real `TradingClient` pointed at an in-process mutable mock exchange
- asserts outcomes from emitted hedge/neutrality events plus final risk state
- exits non-zero on any expected-vs-actual mismatch

Current fixture shape:
- `market`
- `trigger.work_item`
- `exchange`
  - `books`
  - `balances`
  - `positions`
  - `global_open_orders`
  - `market_open_orders`
  - `order_lookup`
  - `actions`
- `expected`

Current deterministic fixtures:
- `clean_full_buy_hedge.json`
- `thin_book_split.json`
- `delayed_truth_confirmation.json`
- `resolution_failure_halts_market.json`

## 2026-03-28 Layer 3 Status

Layer 3 is implemented.

The current state is:
- Layer 1 implemented
- Layer 2 implemented
- Layer 3 implemented

## Core Goal

Create an **on-demand hedge test** that can:

- trigger the real hedge pipeline deliberately
- exercise current hedge logic with repeatable scenarios
- optionally use real Polymarket execution for highest-fidelity operator validation
- avoid changing the default behavior of the normal live bot

## Key Design Conclusion

The best design is **not** “only a fully simulated harness” and **not** “only a live-execution harness.”

The best design is a **layered harness**:

1. deterministic simulated replay
2. noisy event-sequence replay
3. optional live-execution probe

Overall confidence in this architecture choice: **high**.

## Why One Harness Is Not Enough

### Fully simulated only

Pros:
- repeatable
- fast
- cheap
- easy to debug

Cons:
- cannot fully prove exchange quirks
- cannot prove real order verification / sync behavior
- cannot prove downstream live execution against Polymarket

### Live-only probe

Pros:
- highest fidelity
- real books, real orders, real verification, real timing noise

Cons:
- nondeterministic
- slower to iterate
- riskier
- expensive in time and potentially money
- poor as the first line of debugging

## Recommended Architecture

### Layer 1: Deterministic Resolution Harness

This is the first thing to build.

Behavior:
- inject a synthetic fill **after attribution**
- enter the same downstream path the bot normally uses after a real fill is already recognized
- reuse the real:
  - fill handling flow
  - hedge planning
  - hedge execution orchestration
  - sell-back logic
  - reconciliation / post-sync truth logic
  - halt / escalation logic

Data sources:
- fixture books
- fixture positions
- fixture open-order responses
- fixture `get_order` responses
- fixture timing / lag behavior

This mode should not run the normal market discovery or passive bid placement loop.

Confidence this is the highest-leverage first increment: **high**.

### Layer 2: Event-Sequence Replay Harness

This is now implemented.

Behavior:
- replay a sequence of raw upstream signals instead of injecting only the post-attribution fill
- simulate the kinds of noise seen in real incidents:
  - delayed or missing trade events
  - delayed order updates
  - duplicate trade IDs
  - cancellation races
  - inconsistent open-order snapshots
  - delayed position truth

This tests the earlier part of the system:
- fill attribution
- fallback matching
- reconciliation trigger behavior
- exchange-truth missed-fill detection

This mode also does not need to run the normal passive bidding loop.

Confidence that this remains the correct second layer: **high**.

### Layer 3: Live-Execution Probe

This is now implemented as the highest-fidelity hedge validation mode.

Behavior:
- acquire a small real trigger-side position first with a bounded share-sized `FOK` BUY
- wait for the authenticated user stream to deliver the real trigger `UserEvent::Trade(...)` for that probe-owned order
- let the existing attribution path derive the real post-attribution fill trigger through `build_fill_work_item(...)`
- then let the downstream path use:
  - real books
  - real trading client
  - real order verification
  - real position sync
  - real Polymarket execution
- then require probe-owned cleanup to merge or explicitly flatten back to a clean target-market state

This mode is for operator validation, not for routine debugging. It is intentionally guarded by an arm env var plus bounded safety caps because it can place real orders.

Confidence that this is the highest-fidelity final layer: **high**.

## Important Clarification

These are **special modes**, not normal production bot runs.

That means:
- they should not do the bot’s ordinary discovery-and-quote job
- they should not search markets and place passive bids unless explicitly designed to do so
- they should focus only on testing the hedge path

However, they do **not** have to be fake at every layer.

In the live-execution probe:
- the trigger acquisition is real
- the downstream hedge execution is real
- the cleanup is probe-owned and also real

## Control Surface Recommendation

The primary control should be **explicit CLI subcommands**, not ambient environment variables.

Recommended pattern:
- add new CLI commands in `src/main.rs`
- keep them completely separate from `Live`, `DryRun`, `Run`, etc.
- do not let env vars silently alter the behavior of normal `live`

Why:
- explicit CLI modes are safer
- harder to trigger accidentally
- easier to test
- easier to keep operationally separate

Env vars are still appropriate for:
- normal API credentials
- optional fixture paths
- explicit dev-only guardrails

Confidence in this control-surface recommendation: **high**.

## Existing Code Paths To Reuse

The harness should avoid building a parallel fake hedge engine.

The existing runtime already has useful boundaries:

- `build_fill_work_item(...)` in `src/runtime/live_engine.rs`
- `FillWorkItem`
- `FillHandler`
- `FillHandler::handle_fill(...)`
- `execute_resolution_plan(...)`

There is also already a `Replay` command in `src/main.rs`, but it is currently aimed at discovery/session replay rather than hedge replay.

The best implementation should reuse the current hedge path rather than copy it.

Confidence: **high**.

## Recommended Scenario Format

A hedge-test scenario should be able to define:

- tracked order state before the fill
- market metadata
- pre-fill position state
- trigger type:
  - raw trade event
  - order update
  - post-attribution fill work item
- fill size and price
- book states at one or more steps
- open-order snapshots
- `get_order` responses
- position-sync responses
- timing gaps / lag between steps

This allows scenarios such as:
- clean immediate hedge
- hedge buy fills but post-sync truth lags
- sell-back fails but paired inventory proves success
- missed fill only appears in reconciliation
- duplicate trade IDs
- stale or incomplete open-order snapshots

## Best Use Of Recent Incident Files

The incident logs from March 24 and March 27 are useful, but they should be used as:

- scenario inspiration
- timing templates
- noise templates
- regression targets

They should **not** be copied blindly as expected behavior because some of those runs contain failure modes that have already been patched.

Recommended use:
- lift the event ordering, timing, and external-noise characteristics
- update the expected outcomes to current intended behavior

Confidence: **high**.

## Safety Requirements

### For all modes
- normal `live` behavior must remain unchanged
- harness modes must be opt-in only
- the feature must be impossible to trigger accidentally from ordinary live runs

### For simulated modes
- no real order placement
- no dependency on live Polymarket execution

### For live-execution probe mode
- explicit CLI mode only
- obvious console warning that real orders may be placed
- tiny-size / operator-controlled scenarios only
- ideally one-market scoped
- no passive discovery loop running in the background

## Suggested Implementation Order

1. Add a dev-only CLI command for deterministic post-attribution hedge replay.
2. Define a scenario/fixture format.
3. Add a mock-backed runner that executes the real hedge path from that scenario.
4. Seed fixtures from the recent hedge incidents.
5. Add a second replay mode that starts before attribution and replays noisy event sequences.
6. Only then add an optional live-execution probe mode.

Confidence in this sequence: **high**.

## Acceptance Criteria

### Layer 1 acceptance
- a known scenario can trigger the hedge path on demand
- the real planner and resolution code run
- expected result can be asserted deterministically

### Layer 2 acceptance
- noisy incident-like input sequences can be replayed
- attribution and reconciliation behavior can be validated repeatably

### Layer 3 acceptance
- operator can deliberately trigger one hedge test against real Polymarket execution
- the test remains tightly scoped and clearly separated from normal live mode

## Why This Matters

Without an on-demand hedge test, hedge confidence depends on waiting for organic fills.

That creates three problems:
- long feedback cycles
- weak confidence between incidents
- pressure to infer too much from too little live evidence

This harness is the cleanest way to shorten the hedge-debug loop while preserving the option for high-fidelity real-exchange validation.

## Final Recommendation

Proceed with a **shared hedge-replay architecture** with:

- deterministic simulated replay first
- event-sequence replay second
- optional live-execution probe last

That gives:
- repeatability for debugging
- realism where it matters most
- a clean separation from the bot’s normal live quoting job

Overall confidence: **high**.
