# Same-Cycle Verified Frontier Handoff Plan

## Purpose
- Preserve the exact implementation plan and context for the next frontier allocator increment.
- This is intended to be sufficient for a brand new session to implement the change without needing the prior conversation.

## Current Branch / Checkpoint Context
- Current working branch at the time of writing: `feature/frontier-allocator-checkpoint`
- Remote checkpoint branch exists and was pushed.
- Checkpoint commit:
  - `afa51a0d1cb13e9c34f8bb65d1db04a9a2859201`
- That checkpoint intentionally does **not** include the remote-only watchdog commit:
  - `f32be19 Add watchdog module with health checks, status polling, and kill trigger`

## Why This Change Is Needed
- Frontier rotation currently cancels a loser market during the `60s` discovery cycle.
- After cancel verification, replacement is deferred until the **next** discovery cycle.
- This creates an idle-cash window that can approach most of a minute.
- Reservation/freeze already fixed the earlier bug where freed capital leaked into unrelated same-cycle new bids.
- The remaining concern is idle capital during the gap between loser cancellation and next-cycle reserved entrant placement.

## Existing Baseline Behavior

### Discovery / Refresh Cadence
- Discovery cycle runs every `60s`.
  - Source of truth in config: `config.json`
  - Current key: `discovery.poll_interval_secs`
- Quote refresh runs every `5s`.
  - Current key: `strategy.quote_refresh_secs`

### Frontier Rotation Today
- Frontier selection only runs on the discovery cycle.
- Current model is still:
  - one loser
  - one entrant
  - one swap at most per discovery cycle
- Current behavior:
  1. discover better frontier candidate
  2. cancel loser bid(s)
  3. arm reservation
  4. freeze unrelated new bid entries
  5. on the next discovery cycle, attempt the reserved entrant first
- Current next-cycle reservation logic already does a **fresh** evaluation of the reserved entrant on the next cycle.
- It does **not** blindly reuse stale cycle-start entrant evaluation.

### Important Constraint
- Only **resting bids** are reclaimable/displaceable for frontier rotation.
- Asks and held inventory are out of scope for cancellation/rotation.

## Agreed Design Decisions

### What Stays The Same
- Keep the `61s` minimum bid hold rule.
  - This was discussed as “cancel protection”.
- Keep the existing one-loser / one-entrant model for now.
- Keep the `5s` refresh path unchanged.
- Keep unrelated new bid entries frozen once frontier handoff begins.
- Keep the existing next-cycle reservation path as the fallback if same-cycle handoff does not complete in time.

### What Changes
- Add a **same-cycle verified handoff** path inside the discovery cycle.
- After loser cancellation is selected:
  - wait for verified cancel in a bounded handoff window
  - if verified in time, do a **fresh post-cancel evaluation**
  - place the **best currently placeable non-held market**
  - this market is not required to be the originally reserved entrant
- If verification does not complete in time:
  - keep the current reservation behavior
  - defer placement to the next discovery cycle

### Critical Behavioral Rule
- Do **not** reuse the originally selected frontier entrant blindly after cancel verification.
- After the loser is truly gone and the budget is truly free:
  - re-evaluate using current books and current actual budget
  - pick the best currently placeable non-held market under the normal ranking comparator

This was an explicit agreement point:
- The originally reserved entrant may no longer be available or may no longer be best.
- In that case, the engine should place the best market it can actually buy into at that point, subject to all normal gates.

## Intended Same-Cycle Handoff Behavior

### High-Level Flow
1. Discovery cycle starts and performs normal evaluation.
2. Frontier selector finds loser + preferred frontier entrant.
3. If loser satisfies the `61s` minimum hold rule, cancel loser bid(s).
4. Arm the usual reservation immediately.
5. Enter a bounded same-cycle handoff window.
6. During that window:
   - check whether loser bid orders are fully gone / cancel verified
   - if not yet clear, keep waiting until timeout
7. If loser clears before timeout:
   - perform a fresh post-cancel evaluation
   - select the best currently placeable **non-held** market
   - place exactly one new bid market in that same cycle
   - clear the reservation
   - keep unrelated new bid entries frozen for the remainder of the cycle
8. If loser does not clear before timeout:
   - keep the reservation
   - do not place any same-cycle entrant
   - allow the existing next-cycle reservation activation path to handle it later

### Why This Design
- Reduces idle cash materially.
- Preserves the safe verified-cancel requirement.
- Keeps the current next-cycle fallback behavior.
- Avoids the dumb case where cancel verifies moments after the original cycle and capital stays idle anyway.

## Config Addition

### New Config Knob
- Add to `strategy` in `src/config.rs`:
  - `frontier_handoff_window_secs: u64`
- Default:
  - `5`

### Why Config, Not Constant
- The user explicitly wanted this to be tunable rather than hard-coded.
- It is strategy/trading behavior, not discovery cadence, so it belongs under `strategy`.

## Recommended Internal Polling Behavior
- During the same-cycle handoff window, use a short internal verification polling cadence:
  - `250ms`
- This is only for the bounded handoff wait after a loser cancel.
- It is acceptable because:
  - it is short-lived
  - only occurs on frontier swaps
  - the change is specifically about reducing idle cash on those swaps

## Files Expected To Change

### 1. `src/config.rs`
- Add `frontier_handoff_window_secs` to `StrategyConfig`
- Update defaults
- Add config parsing tests if needed

### 2. `config.json`
- Optionally add the new setting explicitly once implementation is ready
- If omitted, defaulting should still work

### 3. `src/runtime/live_engine.rs`
- Main implementation file
- This is where nearly all of the new logic belongs

Expected additions:
- helper to run a bounded same-cycle handoff after loser cancel
- helper to detect whether loser bid orders are fully clear
- helper to perform fresh post-cancel selection using real freed budget
- integration into the existing discovery-cycle flow after loser cancel / reservation arming
- logs for handoff progress and outcome

### 4. Possibly `src/trading/order_manager.rs`
- Only if existing helpers are insufficient
- There are already useful helpers in this area for:
  - pending cancel retries
  - counting bid order states
  - checking whether a market still has bid orders or pending cancels

Use existing helpers first.

### 5. Docs / Notes
- `STRATEGY.md`
- `agents/summary.md`
- `agents/changelog.md`

## Existing Code Facts To Remember

### Live Engine
- Frontier rotation logic already exists in `src/runtime/live_engine.rs`.
- It currently includes:
  - frontier selection
  - reservation arming
  - reservation activation on next cycle
  - freeze of unrelated new bid entries
- This is the right place to extend.

### Order Manager
- Existing order-manager behavior already exposes enough machinery for cancel verification decisions.
- Useful existing concepts/helpers include:
  - whether market still has active bid orders
  - whether bid cancels are still pending/retrying
  - bid-only cancellation helpers

## Exact Behavioral Requirements

### Must Preserve
- `61s` minimum hold/cancel-protection rule
- no inventory liquidation for frontier reasons
- no ask-side frontier liquidation
- same-market maintenance still works:
  - quote refresh
  - hedge-depth downsizing
  - deadmission handling
- no unrelated new bid entries after frontier handoff begins

### Must Add
- same-cycle post-cancel placement opportunity
- only after verified loser cancel
- must use fresh evaluation, not stale cycle-start choice
- must choose the best currently placeable non-held market
- if nothing qualifies, do not force placement

### Must Not Do
- must not place entrant before verified loser cancel
- must not reopen the full general new-bid loop after same-cycle placement
- must not perform multi-loser funding
- must not redesign the allocator into a portfolio solver in this pass

## Preferred Implementation Shape

### Proposed New Helper Responsibilities In `live_engine.rs`

#### `run_same_cycle_frontier_handoff(...)`
Responsibilities:
- called only after loser bids were canceled and reservation armed
- waits up to `frontier_handoff_window_secs`
- polls every `250ms`
- exits early when loser is fully clear
- if clear:
  - refreshes/re-evaluates
  - selects best current non-held entrant
  - places it
  - clears reservation
  - returns success state
- if timeout:
  - leaves reservation armed
  - returns fallback state

#### `select_best_post_cancel_market(...)`
Responsibilities:
- use actual free budget after verified cancel
- evaluate only **non-held** markets
- require normal gates:
  - quote approval
  - hedgeability
  - viability
  - min-size
  - budget
- ordering:
  - same current comparator as frontier ranking
  - `reward_per_share`
  - then `estimated_reward`
  - then stable `condition_id`

### Reservation Semantics
- Reservation remains the authoritative fallback record if same-cycle handoff times out.
- Same reservation model should continue to support next-cycle activation if same-cycle handoff fails.
- After successful same-cycle placement:
  - clear the reservation
  - keep unrelated new bid entries frozen until cycle ends

## Logging Requirements

Add explicit logs for:
- `Frontier same-cycle handoff started`
- `Frontier same-cycle handoff waiting`
- `Frontier same-cycle handoff verified`
- `Frontier same-cycle handoff placed`
- `Frontier same-cycle handoff timed out`
- `Frontier same-cycle handoff no_placeable_market`

Also log whether the placed market was:
- the originally reserved entrant
- or a different fresh best market

No monitor DB/schema/UI changes are needed for this pass.

## Testing Requirements

### Unit / Integration Coverage
- loser cancel verifies inside the handoff window and reserved entrant is still best:
  - same-cycle placement succeeds
- loser cancel verifies inside the handoff window and reserved entrant is no longer best:
  - best fresh currently placeable market is chosen instead
- loser cancel does not verify before timeout:
  - reservation remains for next cycle
- after successful same-cycle handoff:
  - unrelated new bid entries remain frozen for the rest of the cycle
- same-market maintenance still works while freeze is active
- inventory asks still function while freeze is active
- if no placeable market exists after cancel verification:
  - reservation clears
  - no same-cycle new bid is placed

### Config Tests
- new config knob defaults to `5`
- config parses successfully when field is omitted

### Validation Command
- `cargo test --workspace`

### Manual Validation Checklist
1. Let a held bid age past the hold window.
2. Confirm frontier loser is selected and canceled.
3. Confirm same-cycle handoff log starts.
4. If cancel verifies quickly:
   - confirm same-cycle placement happens
   - confirm no unrelated bid is placed later in that cycle
5. If cancel does not verify quickly:
   - confirm timeout log appears
   - confirm reservation remains armed
   - confirm next-cycle reservation activation still works

## Known Non-Goals
- No multi-loser funding (`A + B -> C`) in this pass
- No threshold/hysteresis tuning in this pass
- No global frontier portfolio solver
- No changes to monitor UI
- No changes to 5-second refresh cadence

## Important Strategic Context
- The user’s priority is currently reducing idle cash more than worrying about ping-pong behavior.
- Ping-pong may still exist after this change.
- That is acceptable for now as long as the system no longer keeps capital idle unnecessarily between frontier cancels and replacements.

## Confidence
- `0.96` this is the right next increment for the frontier allocator
- `0.93` it should materially reduce the idle-cash window
- `0.99` it does not address the separate multi-loser allocation limitation
