# Hedge Harness Terminology

This note defines the terms and result fields used by the hedge validation harness.

## Probe Families

### Hedge live probe

The hedge live probe validates the real production fill-triggered hedge/sellback path.

It:
- creates one live trigger fill
- waits for the real production hedge-resolution path to run
- audits the production decision and exit path
- keeps cleanup as a separate operational verdict

### Merge live probe

The merge live probe validates CTF merge plumbing on a deliberately self-acquired YES/NO pair.

It:
- self-acquires a small equal YES/NO pair under capped prices
- verifies the pair through direct funded-wallet truth plus synced engine truth
- calls the harness-only pair-exit seam that reuses the existing production merge/fallback helper
- requires merge success and pair disappearance before cleanup

Operational note:
- the merge live probe now performs a harmless relayer-readiness preflight before it buys anything
- a configured merger object is not enough; the probe also requires valid relayer auth, a deployed SAFE wallet, and a readable SAFE nonce
- if that preflight fails, the probe should stop before acquisition and still leave cleanup clean

This probe is intentionally separate from the hedge live probe. A green merge live probe proves merge plumbing works on a pre-created pair; it does not, by itself, prove that a live hedge scenario naturally reached a merge-favored market state.

## Production Observability Events

### `hedge_decision_evaluated`

Explicit production event emitted when the hedge path decides how to resolve the trigger fill.

This is the canonical source for:
- the actual BUY-side split between hedge and sellback
- whether the path was `buy_side_resolution` or `sell_side_direct`
- why production chose that path through `decision_reason_code`

Layer 3 should prefer this event over reverse-engineering the decision from `hedge_intent_created`.

### `hedge_exit_path_recorded`

Explicit production event emitted after post-sync inventory truth is known and after the merge/fallback branch resolves.

This is the canonical source for:
- post-sync YES/NO sizes and net exposure
- whether complete sets existed after sync
- which exit path production itself took
- whether merge was configured, attempted, successful, or failed
- whether fallback asks were attempted, placed, or failed

Layer 3 should prefer this event over inferring exit flow from generic order events.

## Core Verdicts

### `meta_pass`

The harness successfully exercised the intended production hedge path.

Examples:
- A real or replayed trigger trade was observed
- The current production attribution path ran
- The current production hedge-resolution path ran

Question answered: did we actually test the real bot path we meant to test?

### `meta_fail`

The run did not meaningfully exercise the intended production path.

Examples:
- Harness bug
- Probe never reached the real hedge path
- User-stream/connectivity failure before the decision logic ran
- Invalid setup that prevented the real path from being exercised

### `standard_pass`

The bot did what it is trying to do in production for that scenario.

This is about strategy correctness plus intended production follow-through, not probe hygiene.

For BUY-side hedge resolution, this includes:
- Did it choose the correct split between buying the opposite side and selling back the filled side?
- If paired inventory was created, did it follow the intended production flow after that?
- If the intended strategy is to merge or place fallback asks, did one of those paths happen?

Question answered: did the bot make the right production decision and follow the intended production flow?

`standard_pass` may still carry a warning when Layer 3 confirmed that production sold before cleanup, but the short post-decision positions snapshot lagged and still looked one-sided.

### `standard_fail`

The real production path ran, but the bot's behavior did not match intended production strategy.

Examples:
- Wrong hedge-vs-sellback decision
- Wrong share split between hedge and sellback
- Intended merge/fallback exit did not happen when strategy says it should
- Pair left idle when the strategy expected merge or fallback asks
- Directional residual remained after the hedge path
- Unconfirmed sellback left Layer 3 unable to prove production flattened before cleanup

### `cleanup_pass`

The probe returned the wallet or target market to its exact starting baseline after the run.

This is a separate operational verdict. A run can be `standard_pass=true` and `cleanup_pass=false`.

### `cleanup_fail`

The probe left probe-created state behind.

Examples:
- Residual one-sided inventory
- Residual paired inventory
- Residual open orders

## Strategy Audit Fields

### `decision_audit_status`

How confidently Layer 3 can audit the live branch choice against live books.

Values:
- `confirmed`: the observed planned hedge/sellback split matches the computed audit plan
- `inconclusive`: live audit snapshots do not support a single unambiguous conclusion
- `failed`: the observed split clearly contradicts the audit plan
- `not_applicable`: used for SELL-side live cases where the buy-side hedge-vs-sellback decision does not apply

### `decision_audit_reason`

Human-readable explanation of why the decision audit was classified that way.

### `planned_hedge_shares`

Observed planned shares that production chose to hedge by buying the opposite side.

### `planned_sellback_shares`

Observed planned shares that production chose to resolve by selling back the filled side.

### `production_decision_mode`

The explicit production branch type from `hedge_decision_evaluated`.

Typical values:
- `buy_side_resolution`
- `sell_side_direct`

### `production_decision_reason_code`

The explicit production reason code from `hedge_decision_evaluated`.

Typical values:
- `hedge_cheaper`
- `sellback_cheaper`
- `tie_prefers_sellback`
- `budget_rerouted_to_sellback`
- `no_resolution_available`
- `sell_side_direct`

### `hedge_leg_status`

Observed status of the hedge leg from production results.

Typical values:
- `success`
- `skipped`
- `unverified`
- `failed`

### `sellback_leg_status`

Observed status of the sellback leg from production results.

Typical values:
- `success`
- `skipped`
- `unverified`
- `failed`

## Follow-Through Audit Fields

### `flow_status`

How the post-decision production flow actually ended before harness cleanup.

Values:
- `sellback_completed`: resolution finished through sellback without paired inventory needing a merge/fallback path
- `merge_completed`: paired inventory existed and was observed to decrease through merge behavior
- `fallback_asks_placed`: paired inventory remained, but production placed real inventory asks as the intended exit path
- `pair_left_idle`: paired inventory remained with no merge evidence and no fallback ask evidence
- `directional_residual`: one-sided inventory remained after the hedge path
- `flow_inconclusive`: the harness could not prove which post-decision path happened

For the narrow confirmed-sellback warning case, `sellback_completed` remains the correct flow status even if the post-decision positions snapshot still looks briefly one-sided.

### `production_exit_path_status`

The explicit production exit branch from `hedge_exit_path_recorded`.

Typical values:
- `sellback_complete`
- `merge_attempted`
- `merge_succeeded`
- `merge_failed`
- `fallback_asks_placed`
- `fallback_asks_failed`
- `pair_left_idle`
- `directional_residual`
- `no_exit_needed`

### `merge_status`

Layer 3 summary of the production merge branch, sourced from `hedge_exit_path_recorded` first and only inferred when the event is missing.

Typical values:
- `succeeded`
- `failed`
- `attempted`
- `not_attempted`
- `not_configured`
- `not_needed`

### `merge_failure_reason`

Human-readable production explanation for a failed merge attempt, when present.

### `fallback_status`

Layer 3 summary of the fallback-ask branch, sourced from `hedge_exit_path_recorded` first and only inferred when the event is missing.

Typical values:
- `placed`
- `failed`
- `attempted`
- `skipped`
- `not_needed`

### `fallback_failure_reason`

Human-readable production explanation for a failed fallback-ask attempt, when present.

### `merge_observed`

Boolean indicating whether paired inventory was observed decreasing before manual cleanup, consistent with a production merge.

### `fallback_asks_observed`

Boolean indicating whether production submitted inventory asks and those asks were still visible in tracked or live open-order truth before cleanup.

### `cleanup_status`

Human-readable cleanup verdict details. This explains why `cleanup_pass` is true or false.

This field must describe the cleanup-stage direct snapshot explicitly rather than reusing the post-decision snapshot.

### `post_decision_direct_yes_size`

Canonical YES inventory seen in the direct funded-wallet truth snapshot used for post-decision reconciliation.

### `post_decision_direct_no_size`

Canonical NO inventory seen in the direct funded-wallet truth snapshot used for post-decision reconciliation.

### `post_decision_direct_observed_for_secs`

How long the post-decision direct snapshot remained stable before Layer 3 used it for reconciliation.

### `cleanup_direct_yes_size`

Canonical YES inventory seen in the direct funded-wallet truth snapshot used for cleanup validation.

### `cleanup_direct_no_size`

Canonical NO inventory seen in the direct funded-wallet truth snapshot used for cleanup validation.

### `cleanup_direct_observed_for_secs`

How long the cleanup direct snapshot remained stable before Layer 3 used it for cleanup validation.

### `truth_reconciliation_status`

Layer 3 verdict for whether production's own post-sync event truth is consistent with direct funded-wallet truth.

Typical values:
- `confirmed`
- `failed`
- `event_missing`

For current-schema successful hedge traces, `hedge_exit_path_recorded` is required. If a successful `hedge_result_recorded` trace is missing that exit event, Layer 3 must mark reconciliation as `failed` with an explicit observability reason rather than tolerating a normal fallback.

`event_missing` is reserved for failed traces or legacy traces where the explicit exit event is not required.

Direct funded-wallet truth remains the final external authority for hard failures. The one narrow exception is a confirmed pre-cleanup sellback where Layer 3 independently verifies execution and the only contradiction is a short one-sided post-decision positions lag; that case stays `confirmed` and is surfaced through dedicated warning fields instead of `failed`.

### `truth_reconciliation_reason`

Human-readable explanation of any mismatch between internal event truth and direct funded-wallet truth.

This field must describe the `post_decision` direct snapshot explicitly so it cannot be confused with cleanup-stage truth.

### `production_sellback_confirmation_status`

Layer 3 answer to: did production confirm the sellback before cleanup ran?

Typical values:
- `confirmed_before_cleanup`
- `unconfirmed_before_cleanup`
- `not_applicable`

`confirmed_before_cleanup` must come from pre-cleanup confirmation before cleanup, not from generic response-only sellback evidence alone. Acceptable proof sources are:
- production sellback trade IDs
- production sellback lookup status or full matched shares
- harness-side authenticated order lookup showing full fill
- post-decision funded-wallet truth already flat before `manual_live_probe_cleanup`
- the narrow production `hedge_exit_path_recorded.post_sync_source=execution_confirmed_sellback` + `exit_path_status=sellback_complete` + neutral internal post-sync inventory case

`hedge_exit_path_recorded.post_sync_source=execution_confirmed_sellback` is not enough by itself when all you have is generic response-only sellback evidence. It is sufficient for the narrow production proof path where the exit event already records `sellback_complete` with neutral internal post-sync inventory before cleanup.

### `production_sellback_confirmation_reason`

Human-readable explanation of the proof source used for the pre-cleanup sellback verdict.

This may cite:
- production trade IDs
- production sellback lookup status or matched shares
- harness-side authenticated order lookup

### `truth_reconciliation_warning_status`

Optional Layer 3 warning for non-failing reconciliation contradictions.

Typical values:
- `positions_lag_after_confirmed_execution`

### `truth_reconciliation_warning_reason`

Human-readable warning detail for a reconciled-but-not-failing contradiction.

This must describe the `post_decision` direct snapshot explicitly and explain why Layer 3 kept the run as a warning instead of a hard failure.

### `hedge_verification_state`

Layer 3 verdict for the hedge leg after combining:
- raw production hedge status
- production hedge verification evidence already observed by the runtime
- bounded harness-side external lookup as a fallback only

This is not a raw copy of `hedge_leg_status`.

Typical values:
- `verified_filled`
- `verified_zero_fill`
- `skipped`
- `production_fill_confirmed`
- `production_zero_fill_confirmed`
- `production_lookup_missing_after_cancel_confirmed`
- `production_lookup_missing_after_cancel_unknown`
- `production_lookup_error`
- `external_fill_confirmed`
- `resting_open`
- `external_zero_fill`
- `missing_order_id`
- `lookup_unavailable`

`lookup_unavailable` is now a last-resort verdict. For current-schema traces with a hedge order id, Layer 3 must use production hedge verification evidence first and only fall back to this bucket when both production evidence and bounded external lookup remain insufficient.

### `production_hedge_cancel_status`

What production itself observed from the existing hedge cancel attempt on BUY-side hedge flows.

Typical values:
- `confirmed`
- `rejected`
- `unknown`

### `production_hedge_cancel_reason`

Reason text returned by production’s existing hedge cancel path when a rejection or unknown outcome was observed.

### `production_hedge_lookup_status`

Status from production’s existing post-cancel `get_order` verification lookup for the hedge order.

Typical values:
- `matched`
- `live`
- `cancelled`
- `invalid`
- `missing`
- `error`

### `production_hedge_lookup_matched_shares`

Matched shares returned by production’s existing hedge verification lookup when available.

### `production_hedge_lookup_error`

Lookup error text captured by production when the existing hedge verification `get_order` call failed.

### `production_hedge_trade_ids`

Trade ids returned by the existing hedge order placement response when production observed them.

### `hedge_lookup_status`

Layer 3 summary of the harness’s own later external hedge-order lookup status when the raw production hedge leg stayed `unverified` and production evidence alone did not fully classify the hedge.

Typical values:
- `matched`
- `live`
- `cancelled`
- `invalid`
- `missing`
- `error`

### `hedge_lookup_matched_shares`

Matched share amount returned by the bounded external hedge-order lookup when that fallback lookup was performed.

## Setup And Harness Terms

### Precondition failure

The test should not be considered valid because the required starting assumptions were not true.

Examples:
- Market was not flat when the probe required a flat baseline
- Credentials or funding were missing
- Spend caps or arming conditions were not satisfied

### Harness failure

The failure came from the harness or test plumbing rather than from production bot behavior.

Examples:
- Probe waited on the wrong websocket condition
- Test-only wiring broke
- Mock/live adapter logic misreported the outcome

## Short Version

- `meta_pass`: we exercised the intended real bot path
- `standard_pass`: the bot did the right thing for that scenario and followed the intended production flow
- `cleanup_pass`: the probe cleaned up after itself

These verdicts are related, but they are not the same.
