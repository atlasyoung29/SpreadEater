# Layer 3 Meta-Pass Observability Report

Date: April 6, 2026

## Purpose

This report replaces the prior April 6 version. The old report described the harness-only Layer 3 cleanup pass. This version reflects the follow-up production-evidence pass that specifically targeted the remaining hedge verification gap:

- old gap: `hedge_verification_state=lookup_unavailable` with `hedge_lookup_status=missing`
- goal: stop asking Layer 3 to guess from a later empty lookup when production already had cancel/lookup facts on the hedge path

The intent of this pass was still observability-only:
- no new hedge orders
- no new waits
- no new websocket subscriptions
- no new REST calls
- no strategy changes

Confidence this remained observability-only from a trading-behavior perspective: **high**

## Scope

Scenario used for live validation:
- `scotus_mail_ballots_yes_bid_probe_under_10`

Runner:
- [run_hedge_live_probe.ps1](<repo-root>/scripts/run_hedge_live_probe.ps1)

Scenario file:
- [scotus_mail_ballots_yes_bid_probe_under_10.json](<repo-root>/fixtures/hedge_live_probe_scenarios/scotus_mail_ballots_yes_bid_probe_under_10.json)

Files changed in this pass:
- [hedge_executor.rs](<repo-root>/src/trading/hedge_executor.rs)
- [hedge.rs](<repo-root>/crates/spreadeater-core/src/payloads/hedge.rs)
- [emitters.rs](<repo-root>/src/monitor/emitters.rs)
- [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs)
- [TERMINOLOGY.md](<repo-root>/tests/support/hedge/TERMINOLOGY.md)

Files intentionally not changed in this pass:
- [live_engine.rs](<repo-root>/src/runtime/live_engine.rs)
- production hedge decision logic
- production hedge execution logic
- event schema version

## What Changed

### 1. Production now emits the hedge verification facts it already had

The existing BUY-side hedge path in [hedge_executor.rs](<repo-root>/src/trading/hedge_executor.rs) already did three things:
- placed the hedge order
- attempted a cancel
- performed a post-cancel `get_order` verification lookup

Before this pass, the runtime kept only the coarse result:
- `hedge_leg_status=unverified`

The finer evidence was discarded.

After this pass, `hedge_result_recorded` now carries additive fields sourced from those existing runtime observations:
- `hedge_cancel_status`
- `hedge_cancel_reason`
- `hedge_lookup_status`
- `hedge_lookup_matched_shares`
- `hedge_lookup_error`
- `hedge_trade_ids`

This was done by extending [HedgeResultPayload](<repo-root>/crates/spreadeater-core/src/payloads/hedge.rs) and wiring the existing runtime data through [emitters.rs](<repo-root>/src/monitor/emitters.rs).

Confidence this closes the specific “production knew more than Layer 3 could see” gap: **high**

### 2. Layer 3 now prefers production hedge verification evidence

[live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs) now distinguishes:
- production hedge verification evidence
- harness-side fallback lookup evidence

New additive result fields:
- `production_hedge_cancel_status`
- `production_hedge_cancel_reason`
- `production_hedge_lookup_status`
- `production_hedge_lookup_matched_shares`
- `production_hedge_lookup_error`
- `production_hedge_trade_ids`

Existing fallback fields remain:
- `hedge_lookup_status`
- `hedge_lookup_matched_shares`

The resolver order is now:
1. raw production `hedge_leg_status`
2. production hedge verification evidence from `hedge_result_recorded`
3. bounded harness-side external lookup

Confidence this ordering is correct: **high**

### 3. `lookup_unavailable` is no longer the primary failure bucket

For current-schema traces with a hedge order id, Layer 3 can now produce more specific production-led verdicts:
- `production_fill_confirmed`
- `production_zero_fill_confirmed`
- `production_lookup_missing_after_cancel_confirmed`
- `production_lookup_missing_after_cancel_unknown`
- `production_lookup_error`

The old `lookup_unavailable` bucket still exists, but only as a last resort when both production evidence and fallback lookup remain insufficient.

Confidence this is the right residual use of `lookup_unavailable`: **high**

## Validation

### Targeted tests

Passed:
- `cargo test hedge_result_payload_serde_roundtrip -- --nocapture`
- `cargo test hedge_verification_metadata_ -- --nocapture`
- `cargo test hedge_result_event_carries_verification_metadata -- --nocapture`
- `cargo test resolve_hedge_verification_ -- --nocapture`

These now cover:
- payload round-trip for the new hedge verification fields
- hedge executor metadata retention
- event emission of verification metadata
- Layer 3 production-first hedge verification classification

Confidence the new code path is covered at the intended seams: **high**

### Full workspace

Passed:
- `cargo test --workspace`

Confidence the pass did not introduce broader regressions: **high**

## Latest Live Run

Armed live rerun date:
- April 6, 2026

Trigger:
- `trigger_order_id=<redacted-id>`
- `trigger_trade_id=fe5ef465-f3f4-4165-a1d7-a2d41caac761`

High-level result:
- `meta_pass=true`
- `standard_pass=false`
- `cleanup_pass=false`

Hedge verification result:
- `hedge_leg_status=unverified`
- `production_hedge_cancel_status=rejected`
- `production_hedge_cancel_reason="matched orders can't be canceled"`
- `production_hedge_lookup_status=missing`
- `production_hedge_lookup_matched_shares=null`
- `production_hedge_lookup_error=null`
- `production_hedge_trade_ids=null`
- `hedge_lookup_status=null`
- `hedge_lookup_matched_shares=null`
- `hedge_verification_state=production_lookup_missing_after_cancel_unknown`

This is the key outcome of the pass:
- the run no longer ends at `lookup_unavailable`
- the harness now reports what production actually saw
- the remaining ambiguity is narrower and explicit

Confidence the old hedge-verification observability gap is closed enough to move on from harness ambiguity: **high**

## What The Live Result Means

### Decision-side failure remains real

The live rerun still showed:
- `decision_audit_status=failed`
- `decision_audit_reason="both planner snapshots agreed on hedge=0 sellback=5, but production decision event recorded hedge=5 sellback=0 ... tie_prefers_hedge"`

That remains a production decision-path contradiction, not a harness issue.

Confidence: **high**

### Exit-path failure remains real

Production still reported:
- `production_exit_path_status=no_exit_needed`

But direct truth still reported:
- `post_decision YES=5.007`
- `post_decision NO=0`

So the internal-vs-direct truth mismatch remains real.

Confidence: **high**

### Hedge verification is now narrower, but not fully terminal

The new evidence says:
- production attempted to cancel the hedge order
- production got a rejection: `"matched orders can't be canceled"`
- production then could not fetch the order back (`lookup_status=missing`)

That is materially more informative than the old state, but it is still not the same as a hard explicit fill confirmation.

This run therefore narrows the hedge ambiguity to:
- likely matched or terminal on exchange
- not provably filled from the currently emitted production fields alone

Confidence this is a real narrowing, not a cosmetic relabel: **high**

## Remaining Observability Gaps

### Gap 1: Production still does not emit a final explicit hedge fill verdict

We now have the runtime’s cancel and lookup evidence, but production still does not emit a direct terminal statement like:
- hedge filled
- hedge zero-filled
- hedge matched before cancel rejection

So Layer 3 can explain the branch better, but not always prove final hedge execution from production data alone.

Confidence: **high**

### Gap 2: `production_lookup_missing_after_cancel_unknown` is still an intentionally conservative bucket

The live result shows this exact case. The reason text strongly suggests a matched/terminal order:
- `"matched orders can't be canceled"`

But the current classifier intentionally does not collapse that into a hard fill-confirmed verdict without more explicit production evidence.

That is defensible, but it is still a remaining observability limit.

Confidence: **high**

### Gap 3: The main remaining failures are now production-behavior failures

After this pass, the major remaining problems are no longer “Layer 3 can’t explain what happened.”

They are:
- decision contradiction on BUY-side resolution
- internal post-sync neutrality vs direct funded-wallet truth mismatch
- cleanup still ending with residual inventory

Confidence these are now the dominant issues: **high**

## Bottom Line

What is now true:
- `meta_pass` is confirmed on a fresh live run
- the old hedge-verification `lookup_unavailable` gap is closed in the live result
- Layer 3 now reports the runtime’s actual hedge cancel/lookup evidence
- the remaining failures are primarily production-behavior issues, not missing hedge-test observability

What is not yet true:
- hedge execution is still not proven from production data alone in the exact `cancel rejected + lookup missing` case
- the app still fails the scenario for real reasons after the observability improvement

Overall assessment:
- enough monitoring to move on and fix the app with clear causes: `yes`, confidence **high**
- hedge verification observability fully perfect: `no`, confidence **high**
- old `lookup_unavailable` harness blind spot still blocking interpretation: `no`, confidence **high**
