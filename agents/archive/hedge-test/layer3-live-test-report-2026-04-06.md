# Layer 3 Live Test Report

Date: April 6, 2026

## Scope

This report covers a fresh armed Layer 3 live probe run on the current branch using:

`powershell -ExecutionPolicy Bypass -File scripts/run_hedge_live_probe.ps1 -Scenario scotus_mail_ballots_yes_bid_probe_under_10`

The goal of this run was not to prove the strategy is fixed. The goal was to produce a real current-state hedge trace that is detailed enough to guide the next production hedge-engine fixes.

## Execution Result

- Scenario: `scotus_mail_ballots_yes_bid_probe_under_10`
- Clean-baseline guard: passed
- Real order placed: yes
- Real trade observed: yes
- Real Layer 3 payload produced: yes
- Runtime crash / harness crash: no

Result:

- `meta_pass=true`
- `standard_pass=false`
- `cleanup_pass=false`

Trigger identifiers:

- `trigger_order_id=<redacted-id>`
- `trigger_trade_id=be941a51-c548-45e5-800d-9f72943d1a24`

Confidence this run is a valid live-money meta-pass and a usable foundation for production fixes: **high**

## Primary Findings

### 1. Production decision path still contradicts both Layer 3 planner snapshots

The strongest repeated finding remains the production decision contradiction:

- planner snapshots: `hedge=0`, `sellback=5`
- production decision event: `hedge=5`, `sellback=0`
- `production_decision_mode=buy_side_resolution`
- `production_decision_reason_code=tie_prefers_hedge`

Layer 3 verdict:

- `decision_audit_status=failed`
- `decision_audit_reason="both planner snapshots agreed on hedge=0 sellback=5, but production decision event recorded hedge=5 sellback=0 decision_mode=buy_side_resolution reason_code=tie_prefers_hedge"`

Confidence this is a real production decision-path bug, not a harness interpretation issue: **high**

### 2. Production still reports neutral internal post-sync truth while direct truth remains directional

Production-side exit reporting said:

- `production_exit_path_status=no_exit_needed`
- `merge_status=not_needed`
- `fallback_status=not_needed`
- `post_sync_net_exposure=0`

But direct funded-wallet truth after the decision stage stayed directional for the entire observation window:

- `post_decision_direct_yes_size=5.007`
- `post_decision_direct_no_size=0`
- `post_decision_direct_observed_for_secs=8`

Layer 3 verdict:

- `truth_reconciliation_status=failed`
- `truth_reconciliation_reason="production exit event recorded neutral post-sync inventory yes=0 no=0 net=0, but stage=post_decision direct_yes=5.007 direct_no=0 observed_for=8s remained directional net=5.007"`

Confidence this is now the main production truth-path failure to fix: **high**

### 3. Hedge verification is no longer a blind spot, but it still does not prove final hedge fill state

This pass did close the old `lookup_unavailable` gap. The current run tells us exactly what production observed:

- `hedge_leg_status=unverified`
- `hedge_verification_state=production_lookup_missing_after_cancel_unknown`
- `production_hedge_cancel_status=rejected`
- `production_hedge_cancel_reason="matched orders can't be canceled"`
- `production_hedge_lookup_status=missing`
- `production_hedge_lookup_matched_shares=null`
- `production_hedge_lookup_error=null`
- `production_hedge_trade_ids=null`

Interpretation:

- production attempted to cancel the hedge order
- exchange replied that matched orders cannot be canceled
- the subsequent production lookup still did not produce a terminal order record
- production therefore could not prove the exact final hedge execution state

This is no longer an observability blind spot in the old sense. It is now a narrower unresolved production state.

Confidence the remaining hedge-leg ambiguity is narrower and acceptable to move past for initial production fixes: **high**

### 4. Cleanup still leaves residual inventory

Cleanup did not restore flatness:

- `cleanup_direct_yes_size=5.007`
- `cleanup_direct_no_size=5`
- `cleanup_direct_observed_for_secs=35`
- `cleanup_status="residual_inventory stage=cleanup direct_yes=5.007 direct_no=5 observed_for=35s baseline_yes=0 baseline_no=0 user=<redacted-wallet>"`

This final cleanup state is not pure production evidence because it is post-harness cleanup. But it still confirms the run ended in a bad state.

Confidence this remains important but secondary to the earlier production decision and truth-path failures: **high**

## Relevance To The `post_position None -> zero` Change

The specific production-path change documented in [post-sync-zero-materialization-note-2026-04-06.md](<repo-root>/agents/archive/hedge-test/post-sync-zero-materialization-note-2026-04-06.md) is consistent with this run’s failure pattern, but this run does not prove that it is the root cause.

Why it is relevant:

- this run reports internal post-sync neutrality:
  - `production_exit_path_status=no_exit_needed`
  - `post_sync_net_exposure=0`
  - truth-reconciliation message says production recorded `yes=0 no=0 net=0`
- direct truth still showed `YES=5.007`, `NO=0`
- the `None -> zero` change is exactly the change that can convert missing final post-sync position truth into an explicit flat position in success cases

Why this run does not prove it:

- the Layer 3 result does not expose whether production’s `yes=0 no=0` came from:
  - a real synced zero position row, or
  - a missing position row that was materialized to zero by [live_engine.rs:6439](<repo-root>/src/runtime/live_engine.rs#L6439)
- `post_sync_net_exposure=0` is computed earlier in the resolution path and can already reflect an internal truth error even before the final fill-handler materialization step

Current assessment:

- Confidence the `None -> zero` change is a legitimate first-look suspect for this run’s internal-flat/direct-directional contradiction: **high**
- Confidence the broader internal post-sync truth path was already problematic even before considering that change: **high**

## Artifact Note

The live probe produced a real result payload, but the newly created event archive directory:

- [run_20260407_003014](<repo-root>/data/events/run_20260407_003014)

contains an empty `events.jsonl` in this workspace snapshot. That means this report is grounded primarily in the probe’s printed result payload and the known field semantics already implemented in Layer 3.

Confidence the printed result payload is still sufficient for the conclusions in this report: **high**

## Most Actionable Production Fix Targets

Ordered by priority:

1. BUY-side resolution decision selection
   - production chose `hedge=5 / sellback=0`
   - both planner snapshots independently said `hedge=0 / sellback=5`
   - Confidence this should be the first production fix target: **high**

2. Internal post-sync truth / neutral-exit classification
   - production said `no_exit_needed` and neutral
   - direct truth stayed directional
   - Confidence this should be the second production fix target: **high**

3. Hedge terminal verification state after cancel rejection plus missing lookup
   - now well explained, but still unresolved
   - Confidence this is worth fixing after the two core production contradictions above: **high**

## Bottom Line

This run is good enough to serve as the foundation for upcoming production hedge-engine fixes.

What it proves:

- the live Layer 3 meta-pass is real on the current branch
- the run reaches a real result payload without the old runtime crash blocker
- the main remaining failures are production behavior failures, not harness ambiguity

What it does not yet prove:

- whether the internal neutral post-sync truth is wrong because of the newly added `None -> zero` conversion specifically, or because of an earlier sync/truth defect upstream of that conversion

Final confidence that Layer 3 is now sufficiently explanatory to move on to production hedge-engine fixes: **high**
