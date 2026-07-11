# Hedge Testing Suite

This document explains how to validate the full hedge pipeline in SpreadEater.

The suite is intentionally layered:

1. Layer 1: deterministic post-attribution hedge harness
2. Layer 2: pre-attribution event-sequence replay harness
3. Layer 3: operator-only live hedge probe

Use the cheapest, safest layer that can answer the question you have. Do not jump straight to Layer 3 unless you specifically need live exchange behavior.

## What Each Layer Proves

### Layer 1: `hedge-test`

Purpose:
- validate downstream hedge resolution after attribution is already known
- validate hedge planning, hedge execution, sell-back behavior, post-sync truth, halt behavior, and deterministic outcome assertions

Fidelity boundary:
- Layer 1 is as close to production as possible after attribution
- it intentionally starts from an explicit post-attribution `FillWorkItem`
- it is not trying to prove the pre-attribution matching or sizing path

What it does not prove:
- raw trade attribution
- order-update fallback matching
- exchange-sync missed-fill detection

Code and fixtures:
- runtime: `src/runtime/hedge_test.rs`
- tests: `tests/hedge_test_harness.rs`
- fixtures: `fixtures/hedge_scenarios/`

### Layer 2: `hedge-replay`

Purpose:
- validate the real upstream event path before `FillWorkItem` exists
- validate `build_fill_work_item(...)`, pending-fill fallback, exchange-sync missed-fill detection, orphan recovery, and reconciliation routing

Fidelity boundary:
- Layer 2 is as close to production as possible before and through attribution
- it is the layer that proves `UserEvent -> build_fill_work_item(...) -> handle_fill(...)`
- it should absorb attribution-path concerns instead of pushing them into Layer 1

What it does not prove:
- real Polymarket execution timing
- real live orders

Code and fixtures:
- runtime: `src/runtime/hedge_replay.rs`
- tests: `tests/hedge_replay_harness.rs`
- fixtures: `fixtures/hedge_replay_scenarios/`

### Layer 3: `hedge-live-probe`

Purpose:
- validate the real downstream hedge path against live books, live balance, live open-order state, live position sync, live order submission, and live verification
- validate it against a real trigger-side position that the probe acquires live first with an aggressive share-sized `GTC` BUY plus bounded cancel, while starting the hedge from the same authenticated user-stream trade signal the live bot uses
- separately report whether the run stayed on the production-faithful path (`meta_pass`) and whether the actual hedge behavior matched the scenario (`standard_pass`)

What it can do:
- place real orders
- cost money
- interfere with a live trading run if used carelessly

What is intentionally still non-production scaffolding:
- the probe creates the trigger fill itself instead of waiting for an organic passive quote fill
- pre-run safety guards can abort before any live order
- cleanup is an out-of-band operational safety step after the verdict, not part of the production-faithful hedge verdict

Current Layer 3 trigger exactness rules:
- matching websocket trigger trades are fed into the engine immediately, even if they arrive in multiple fragments
- the trigger only counts as a Layer 3 success if the normalized cumulative matching websocket trigger shares equal the requested trigger shares exactly
- cumulative trigger shares below the request are reported as `trigger_partial_fill`
- cumulative trigger shares above the request are reported as `trigger_overshoot`

Code and fixtures:
- runtime: `src/runtime/hedge_live_probe.rs`
- tests: `tests/hedge_live_probe.rs`
- templates: `fixtures/hedge_live_probe_scenarios/`

## Safety Rules

These rules are not optional for Layer 3:

- Do not run `hedge-live-probe` while `cargo run -- live` is already active on the same account.
- Use a quiet market and very small share caps.
- Keep `require_clean_market: true` unless you have a deliberate reason not to.
- Treat the warning banner as the final preflight approval step.
- If the banner shows a larger hedge than expected, stop and fix the scenario instead of continuing.

Money risk by layer:
- Layer 1: no real orders, no money risk
- Layer 2: no real orders, no money risk
- Layer 3: real orders possible, real money risk exists

## Layer 3 Verdict Semantics

Layer 3 should be read as three results, not one:

- `meta_pass`: the run stayed on the production-faithful path after trigger placement
- `standard_pass`: the observed hedge behavior matched the scenario expectations
- `cleanup_result`: the out-of-band safety cleanup succeeded or failed afterward

Examples:

- `meta_pass=true`, `standard_pass=false`
  The live run used the production-faithful path, but the hedge behavior failed the scenario expectations.
- `meta_pass=false`, `standard_pass=false`
  The live run did not stay on the production-faithful path, so the scenario cannot be trusted as a hedge verdict.
- `meta_pass=true`, `standard_pass=true`, `cleanup_result=failure`
  The live hedge path behaved as expected, but the after-the-fact safety cleanup still failed and needs operator attention.

## Recommended Validation Order

Run the suite in this order:

1. Compile and typecheck
2. Run Layer 1 deterministic tests
3. Run Layer 2 replay tests
4. Run Layer 3 mock-backed tests
5. Run the broader runtime suite
6. Run a real Layer 3 probe only if you actually need live confirmation

Recommended command set:

```powershell
cargo check --quiet
```

```powershell
cargo test --bin spreadeater layer1_ -- --nocapture
```

```powershell
cargo test --bin spreadeater layer2_ -- --nocapture
```

```powershell
cargo test --bin spreadeater live_probe_ -- --nocapture
```

```powershell
cargo test --bin spreadeater -- --nocapture
```

```powershell
cargo test --workspace --all-targets -- --nocapture
```

## Layer 1: Deterministic Harness

### When To Use It

Use Layer 1 when the suspected bug is in:
- hedge side selection
- hedge share sizing after attribution is already known
- split hedge vs sell-back behavior
- final neutrality / halt outcome
- post-sync truth handling

### How To Run It

Run the checked-in Layer 1 fixture coverage:

```powershell
cargo test --bin spreadeater layer1_ -- --nocapture
```

Target an individual Layer 1 scenario by its test name when needed:

```powershell
cargo test --bin spreadeater layer1_clean_full_buy_hedge_matches_expected_outcome -- --nocapture
```

The old standalone `cargo run -- hedge-test --scenario ...` CLI is no longer exposed by the app binary.

Failure fixture coverage:

```powershell
cargo test --bin spreadeater layer1_resolution_failure_halts_market_when_no_resolution_path_exists -- --nocapture
```

### Current Layer 1 Fixtures

- `clean_full_buy_hedge.json`
- `thin_book_split.json`
- `delayed_truth_confirmation.json`
- `resolution_failure_halts_market.json`

### Layer 1 Fixture Shape

Top-level keys:
- `market`
- `trigger.work_item`
- `exchange`
- `expected`

Use Layer 1 when you already know what the post-attribution `FillWorkItem` should be.

## Layer 2: Replay Harness

### When To Use It

Use Layer 2 when the suspected bug is in:
- raw trade attribution
- `maker_order_id` or `taker_order_id` anchoring
- pending-fill fallback behavior
- exchange-order sync missed-fill detection
- cancelled-order misattribution
- duplicate trade handling
- orphan recovery or reconciliation routing

### How To Run It

Run the checked-in Layer 2 replay coverage:

```powershell
cargo test --bin spreadeater layer2_ -- --nocapture
```

Target an individual Layer 2 replay scenario by its test name when needed:

```powershell
cargo test --bin spreadeater layer2_raw_trade_immediate_attribution_matches_fixture -- --nocapture
```

Additional examples:

```powershell
cargo test --bin spreadeater layer2_order_update_fallback_respects_residual_exposure -- --nocapture
```

The old standalone `cargo run -- hedge-replay --scenario ...` CLI is no longer exposed by the app binary.

### Current Layer 2 Fixtures

- `raw_trade_immediate_attribution.json`
- `order_update_fallback_partial_accounted.json`
- `exchange_sync_missing_fill.json`
- `reconciliation_orphan_recovery.json`
- `cancelled_order_not_misattributed.json`
- `duplicate_trade_id_deduped.json`

### Layer 2 Fixture Shape

Top-level keys:
- `market`
- `setup`
- `sequence`
- `exchange`
- `expected`

Use Layer 2 when the bug depends on the exact order of raw user events, refresh checkpoints, or reconciliation triggers.

## Layer 3: Live Probe

### When To Use It

Use Layer 3 only when you need proof against live exchange behavior:
- live order verification behavior
- live balance usage
- live book routing
- live post-sync position truth
- live downstream hedge execution under real exchange responses

Do not use Layer 3 as your first debugging tool.

### Preconditions

Required environment:
- valid live API credentials
- `POLY_PRIVATE_KEY`
- `SPREADEATER_HEDGE_LIVE_PROBE_ARM=I_UNDERSTAND_REAL_ORDERS`

Strongly recommended:
- no concurrent live bot run
- no open orders on the target market
- no residual inventory on the target market
- tiny share caps

### Live Probe Templates

- `fixtures/hedge_live_probe_scenarios/template_small_yes_buy_probe.json`
- `fixtures/hedge_live_probe_scenarios/template_small_no_buy_probe.json`
- `fixtures/hedge_live_probe_scenarios/scotus_mail_ballots_buy_probe_5.json`

### Layer 3 Scenario Shape

Top-level keys:
- `name`
- `description`
- `market`
- `trigger`
- `safety`
- `expected`

Trigger fields:
- `leg`
- `shares`
- `max_trigger_limit_price`

Key safety fields:
- `require_clean_market`
- `max_planned_hedge_shares`
- `max_planned_sellback_shares`
- `max_planned_hedge_notional_usdc`
- `max_post_sync_net_exposure`
- `max_trigger_notional_usdc`
- `max_cleanup_notional_usdc`

Key expected fields:
- `success`
- `halted`
- `hedge_side`
- `critical_event_types`
- `result_status`
- `hedge_leg_status`
- `sellback_leg_status`
- `cleanup_status`
- `clean_end_state`

### How To Run A Live Probe

Set the arm env var:

```powershell
$env:SPREADEATER_HEDGE_LIVE_PROBE_ARM="I_UNDERSTAND_REAL_ORDERS"
```

Then run a tiny scenario:

```powershell
cargo run -- hedge-live-probe --scenario fixtures/hedge_live_probe_scenarios/template_small_yes_buy_probe.json
```

Expected behavior:
- the command prints a warning banner before any real order is placed
- the probe first acquires the trigger-side inventory live with a bounded share-sized `FOK` BUY
- the probe starts the authenticated user stream before trigger placement, waits for the real matching websocket `UserEvent::Trade(...)`, replays that exact event, and then enters the normal attribution + downstream hedge path
- after the hedge path, the probe attempts `merge_or_flatten` cleanup and only passes if the market returns to a clean end state
- the banner includes trigger leg, trigger shares, best ask, trigger limit, planned hedge shares, sell-back shares, hedge notional, and CTF-merge status
- the command exits non-zero if the preflight safety checks fail
- the command exits non-zero if the observed outcome violates the expected bounds

Current Layer 3 v1 restrictions:
- only `trigger.leg = YesBid` or `NoBid` is supported
- trigger acquisition is an immediate taker `FOK` BUY, not a passive wait
- if REST suggests the trigger filled but the user stream never confirms it, the probe fails conservatively and flattens instead of hedging from REST-derived truth
- success requires a clean end state on the target market, not merely a successful hedge leg

Layer 3 fidelity boundaries:
- the probe creates the trigger fill itself instead of waiting for an organic passive fill
- the probe does not run the normal discovery/quoting loop
- cleanup is probe-owned instead of long-lived runtime-owned
- within those explicit boundaries, the hedge-start input path is the same as the live bot wherever feasible: real authenticated user-stream trade -> `build_fill_work_item(...)` -> `handle_fill(...)`

### Layer 3 Cost And Risk

Layer 3 can place real orders.

That means:
- yes, it can cost money
- yes, it can interact with live account state
- yes, it can conflict with a separate live bot process if both are active

If you do not need live exchange confirmation, stop at Layer 1 or Layer 2.

## How To Add A New Hedge Regression

Use this decision rule:

- If the issue starts after attribution is already known, add a Layer 1 fixture.
- If the issue depends on raw event ordering or missed-fill detection, add a Layer 2 fixture.
- If the issue only appears on live exchange behavior, add a Layer 3 operator template and, if possible, a mock-backed Layer 3 test.

Recommended workflow for a new incident:

1. Reproduce it as a Layer 1 fixture if the downstream resolution is the main concern.
2. Reproduce it as a Layer 2 fixture if the trigger path is suspect.
3. Only after both pass, decide whether a live Layer 3 probe is worth the cost.

## Pass / Fail Rules

All three harness commands are intended to behave the same way at the CLI level:

- exit code `0` means the scenario passed
- exit code `1` means the scenario failed expectations or safety checks
- a non-zero error can also mean the scenario was invalid or setup failed

Do not treat "the command ran" as success. Treat only an explicit PASS with exit code `0` as success.

## Troubleshooting

### The command hangs or behaves strangely

- Make sure another live bot run is not already active.
- Make sure you are not reusing a market with open orders or residual inventory for Layer 3.
- Re-run the deterministic layers first before blaming the live probe.

### A mock-backed harness unexpectedly touches live credentials

The test suite is supposed to isolate child-process CLI runs from the repo `.env`. If that changes, fix the test harness before trusting the result.

### A live probe preflight aborts

That is usually good. Common causes:
- target market not clean
- planned hedge size too large for your safety caps
- discovery metadata disagrees with the scenario
- not enough balance for the bounded plan

Fix the scenario or account state first. Do not weaken the caps just to force a run.

## Current Validation Baseline

As of 2026-04-25, the hedge testing suite validates with:

```powershell
cargo test --bin spreadeater layer1_ -- --nocapture
```

```powershell
cargo test --bin spreadeater layer2_ -- --nocapture
```

```powershell
cargo test --bin spreadeater live_probe_ -- --nocapture
```

```powershell
cargo test --bin spreadeater -- --nocapture
```

```powershell
cargo test --workspace --all-targets -- --nocapture
```

Current local baseline:
- `cargo test --workspace --all-targets -- --nocapture`: green on 2026-04-25
- main bot inline/unit coverage: `383` passing tests
- monitor unit coverage: `12` passing tests
- monitor Postgres integration tests remain ignored unless a local Postgres service is running

## Bottom Line

Use the hedge suite like this:

- Layer 1 for deterministic downstream hedge behavior
- Layer 2 for attribution and missed-fill path correctness
- Layer 3 for small, deliberate, operator-approved live confirmation

That sequence gives the highest confidence at the lowest cost.
