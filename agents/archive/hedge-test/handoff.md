# Handoff: Hedge Harness Recovery + Prod Revert

## Primary Goal

Build a **working hedge test harness** that **perfectly emulates real production behavior** wherever feasible.

The key standard is:

- **Meta-pass**: the test uses the same real code path and behavior as production, except for the explicitly accepted wrapper/scaffolding.
- **Standard pass/fail**: given that real path, the hedge either succeeds or fails.

## Immediate Task

**Do not continue iterating on the harness first.**

Immediate task is:

- **revert the production-facing changes that came in via PR #27**
- restore production to the last known good behavior
- then rebuild/re-land the harness with strict separation between:
  - harness/test-only code
  - true production code

## User Constraints / Non-Negotiables

These were stated clearly and should be treated as hard requirements:

- The harness must emulate production **as closely as possible**
- If production uses user stream, the test should use user stream
- If something cannot match production, that must be explicitly called out
- The harness should adapt to production behavior, **not** the other way around
- Production code should not be changed just to make tests work, unless there is a clearly justified real prod fix
- Synthetic trigger creation for Layer 3 is acceptable as wrapper behavior
- Safety scaffolding is acceptable as wrapper behavior
- But once the trigger exists, behavior should match production

## Current Situation

### What broke

After PR `#27` was merged into `main`, your partner reported that production is now:

- **making bids over the hedge-aware max amount available**

Your view is that production was basically fine before, except:

- hedging itself still had issues
- bidding was not yet optimized

That bias is reasonable and should be the working assumption until disproven:

- **assume PR #27 introduced prod regressions unless proven otherwise**

### Key merged commit

The risky merge is:

- `c76bef3`  
  `Merge pull request #27 from atlasyoung29/On-Demand-Hedge-Test-Harness`

A later commit on `main` is **doc-only** and not the production break:

- `0175228`
  `Archive hedge reports and add standard fail note`

So the prod regression should be assumed to come from the PR #27 merge payload, not the later docs commit.

## Why Revert-First Is The Best Move

Best approach is **not** “logs first” and **not** “surgical patch first”.

Best approach is:

1. **Restore production**
   - revert the production-facing changes introduced by PR `#27`
   - validate that the hedge-aware bid oversizing disappears

2. **Then rebuild the harness correctly**
   - keep harness-only changes
   - reapply any shared production changes only if they are independently justified as real prod fixes

Reason:

- PR #27 touched too much shared runtime surface area
- this is not a good situation for guessing one-line fixes
- if bidding/sizing is wrong in prod, prod safety comes first

## Review Finding Still Relevant

This unresolved review concern matters and fits the same philosophy:

> `build_work_item()` hardcodes `size_to_apply` and `hedge_size` to the raw fill size instead of reusing `LiveEngine::build_fill_work_item()`, which applies synthetic-fill deduplication, pending fallback accounting, and residual-exposure sizing.

Interpretation:

- harness is still not fully respecting production fill sizing
- this is another sign that the harness should be brought to production, not production bent toward the harness

## What PR #27 Changed

PR #27 was **not** test-only.

It changed shared production/runtime files, including:

- `src/runtime/live_engine.rs`
- `src/trading/order_manager.rs`
- `src/trading/client.rs`
- `src/trading/user_stream.rs`
- `src/trading/hedge_executor.rs`
- `src/config.rs`
- `src/auth/order_signer.rs`
- `src/models/order.rs`
- `src/runtime/mod.rs`
- `src/trading/mod.rs`

It also added harness/test files, including:

- `src/runtime/hedge_live_probe.rs`
- `src/runtime/hedge_replay.rs`
- `src/runtime/hedge_test.rs`
- `src/runtime/hedge_harness_support.rs`
- `tests/hedge_live_probe.rs`
- `tests/hedge_replay_harness.rs`
- `tests/hedge_test_harness.rs`
- `fixtures/hedge_*`
- docs

## Most Suspicious Production Areas

Given the reported prod symptom, the first files to suspect are:

- `src/trading/order_manager.rs`
- `src/runtime/live_engine.rs`

Specifically, inspect:

- hedge-aware available budget calculations
- gross balance updates
- quote/bid capping
- any path involving:
  - `available_hedge_resolution_usdc`
  - `cap_buy_size_to_budget`
  - remaining hedge-aware budget
  - gross balance / cash reserve interaction

## Recommendation For The Next Branch

Start from `main` and use a branch dedicated to reverting the prod-facing damage.

A branch was already created in the main checkout for this purpose:

- `fix/revert-prod-changes-from-hedge-harness`

Use that branch or recreate it fresh if preferred.

## Recommended Execution Plan

### Phase 1: Prod Recovery

1. Branch from current `main`
2. Revert **production-facing changes from PR #27**
3. Re-test the live bot behavior that was reported broken
4. Confirm bid sizing returns to expected hedge-aware limits

### Phase 2: Harness Recovery

After prod is restored:

1. Reintroduce harness/test-only files
2. Remove unnecessary shared production changes
3. Re-add only genuine prod fixes as separate reviewed commits

## Practical Revert Strategy

Preferred order:

### Best / safest

- revert the **production-facing files** from PR #27
- keep docs/tests/harness files out of the revert if possible

### If that becomes messy

- revert the whole merge in a hotfix branch
- then selectively restore harness-only files

The reason I prefer file-focused revert first is:

- the harness/docs/tests may still be useful
- the prod break appears tied to shared runtime changes, not the test files themselves

## Important Harness Context For Later

Layer 3 eventually reached a real **meta-pass** on live runs.

That means:

- trigger matched
- user-stream trade observed
- downstream hedge path executed
- harness was capable of reaching the real production hedge path

But the run still exposed real production-facing problems:

- `hedge_leg_status = unverified` even though the hedge really happened
- post-hedge cleanup left paired residue behind

Those are important later, but **not the immediate task right now**.

Immediate task is still:

- restore production safety and sane bid sizing first

## Standard-Fail Context For Later

There is already a report written for the later prod hedge verification issue:

- `retired standard-failure report`

Use that later when returning to:

- hedge verification
- post-hedge cleanup
- standard-fail production fixes

Do **not** let that distract from the current revert-first task.

## What The Next Instance Should Do First

Open the main checkout, not the old worktree.

Repo path:

- `<repo-root>`

Then:

1. confirm current `main` behavior / failing prod symptom
2. inspect diff from PR #27 in shared production files
3. revert prod-facing changes first
4. validate live/runtime sizing behavior
5. only then resume harness redesign

## One-Sentence Guidance

**Treat PR #27 as having overreached into production; restore prod first, then rebuild the hedge harness so it follows production rather than modifying it.**
