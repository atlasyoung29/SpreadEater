# Branch Story - 2026-04-19

This note captures the branch lineage that started from the post-`721d5b1` frontier fixes and now lands on the current integration branch `fix/fontier-churn-admission-and-handoff`.

Important scope note:
- the current checked-out branch is `fix/fontier-churn-admission-and-handoff`
- the current local branch tip is `c9ec0ef`
- that tip already includes the full downstream stabilization chain from `fix/reconciliation-hedge-resolution`, `feature/control-plane-productivity-followup`, and `feature/neg-risk-merge-safety`
- this note is meant to describe what is actually fixed, validated, and still open before the final `origin/main` refresh and merge decision

## Baseline

- local `main` currently points at `034bac2` (`Merge pull request #41 from atlasyoung29/frontier-same-cycle-handoff`)
- `origin/main` currently points at `9360377` (the webhook test revert on top of the temporary webhook test commit)
- the main historical anchor before the frontier/same-cycle review cycle is `721d5b1` (`Fix START.py cargo PATH resolution on Windows`)
- the first reviewed defect family after `721d5b1` was same-cycle frontier handoff: write-lock scope blocked hedge metadata reads, and post-cancel selection could consult the historical ghost-market cache. Confidence: **high**

## Branch Lineage

### 1. `fix/fontier-churn-admission-and-handoff` at `f37b0f2`

Purpose:
- stop same-cycle frontier handoff from holding the `managed_markets` write lock across waits, polling, and fresh evaluation
- restrict same-cycle post-cancel selection to current-cycle evaluated candidates instead of the historical `known_markets` cache

Current verdict:
- keep this patch
- later live runs did not produce evidence that this patch caused hedge failures
- later live runs also did not generate enough real frontier rotation activity to fully live-close `#16` / `#17A`

Confidence:
- lock-scope fix is correct: `0.95`
- current-cycle-only candidate scope fix is correct: `0.94`
- fully live-validated under real frontier rotation: `0.63`

### 2. `fix/reconciliation-hedge-resolution` at `c762e6d` then `9828883`

Purpose:
- fix mixed hedge/sellback truth-lag handling
- fix false merge-harness failure when relayer success precedes `/positions` convergence

Commits:
- `c762e6d` `Fix mixed hedge-sellback truth-lag resolution fallback`
- `9828883` `Fix post-merge truth convergence after confirmed pair exits`

Current verdict:
- these fixes remain the base for the current stabilization work
- no later evidence contradicted either fix

Confidence:
- mixed hedge/sellback truth-lag fix is directionally correct: `0.90`
- post-merge truth convergence fix is correct: `0.93`

### 3. `feature/control-plane-productivity-followup` at `18eb7bf`

Original purpose:
- fix false user-WS silence classification by treating authenticated raw socket activity as liveness
- fix calibration non-convergence by making `predicted_scoring` depend on the current competition-adjusted evaluation path instead of a stale boolean rule
- harden the live-probe fixtures and validation flow

Landed at that stage:
- user-stream handling emits watchdog-only raw activity for authenticated `PING`, `PONG`, and non-business text frames
- watchdog liveness resets on raw user-stream activity without creating order/fill side effects
- calibration prediction recomputes viability and quote compatibility using the current competition multiplier and fresh-book evaluation path

Confidence that the original branch goal was technically correct: **high**

### 4. `feature/neg-risk-merge-safety` at `4b6ca36`

Purpose:
- fix paired `neg_risk=true` merge routing and venue-safe approvals before long live stabilization

What that branch established:
- pair exits route `neg_risk=false` markets to the standard CTF contract and `neg_risk=true` markets to the Neg Risk Adapter
- SAFE approval remains on the CTF ERC-1155 contract, but the approved operator is venue-specific and cached separately for standard vs neg-risk
- production no longer guesses a standard merge when market metadata is missing
- the harness fails clearly when it cannot resolve market metadata for venue selection

Confidence the original neg-risk merge gap is solved for paired single-question exits: **high**

### 5. `feature/control-plane-productivity-followup` after the later April 17 stabilization work

The current uncommitted worktree now carries the following later stabilization work on top of the earlier branch state:

- paired neg-risk merge support and venue-safe approval behavior from the child branch
- fill anchoring exact-signature fallback for unattributed user-stream trades against active or recently cancelled tracked orders
- suppression of late user-stream echoes for already-verified resolution sellbacks
- order-truth reconciliation before hedge-affordability checks and halted-market cleanup
- order-level calibration hardening so unchanged sampled orders that were just observed as non-scoring do not keep ratcheting the multiplier upward on the same false-positive sample
- scoring endpoint error handling so failed scoring probes are skipped instead of silently counted as `actual_scoring=false`
- SAFE relayer retry hardening for deployment / nonce / submit / lookup failures plus bounded fresh-nonce retries after terminal `STATE_FAILED`
- live-probe fixture refresh from the stale China standard-merge candidate to the newer Playboi standard-merge candidate
- harness expectation updates so split hedge+sellback residual pairs truthfully exit through fallback inventory asks when merge is unconfigured or fails

Confidence that this later stabilization work is internally coherent: **high**

### 6. Current integration branch state at `c9ec0ef`

Current truth:
- the child stabilization work has already been merged up into `fix/reconciliation-hedge-resolution`
- `fix/reconciliation-hedge-resolution` was reconciled with its remote-only event-log commit and pushed as `c9ec0ef`
- the current branch `fix/fontier-churn-admission-and-handoff` has been fast-forwarded to that same `c9ec0ef` tip

What this means:
- the current branch now represents the full safety/stabilization chain that sits one integration step below `main`
- this is no longer a child worktree-only story; it is now the active frontier-parent branch state

Confidence current integration branch state is coherent: **high**

## What Has Been Validated On The Current Local Worktree

### Automated validation

Validated on the current integrated code tree:
- `cargo test --bin spreadeater -- --nocapture`
- `cargo test --workspace --no-run`
- focused hedge harness layers 0/1/2
- focused merge harness slices
- focused ctf-merge retry coverage

Current read:
- automated runtime and hedge harness coverage is green on the integrated branch tip

Confidence: **high**

### Scripted live validation

Validated successfully on the integrated branch tip:

1. User stream smoke on `Will no Fed rate cuts happen in 2026?`
   - passed
   - confirms raw-frame liveness reporting is working in the scripted path
   - Confidence: **high**

2. Hedge live probe on `Will no Fed rate cuts happen in 2026?`
   - passed
   - `meta_pass=true`, `standard_pass=true`, `cleanup_pass=true`
   - Confidence: **high**

3. Neg-risk merge live probe on `Will no Fed rate cuts happen in 2026?`
   - passed on the final validation pass
   - `meta_pass=true`, `standard_pass=true`, `cleanup_pass=true`
   - `pair_exit_status=merge_succeeded`
   - Confidence: **high**

4. Standard merge live probe on `New Playboi Carti Album before GTA VI?`
   - passed on the final validation pass
   - `meta_pass=true`, `standard_pass=true`, `cleanup_pass=true`
   - `pair_exit_status=merge_succeeded`
   - Confidence: **high**

What this means:
- the earlier neg-risk merge blocker recorded in this branch story is no longer the current worktree truth
- merge-path remaining instability looked primarily like relayer/service fragility, and the current worktree hardens the bot against that without weakening success criteria

Confidence: **high**

### Latest passive live-run check

The later one-hour-plus live run inspected from:
- `data/events/run_20260417_184100/events.jsonl`

What that run showed:
- no fills
- no hedge executions
- no merge attempts
- no stale-book halt spam
- no risk-state halt spiral
- calibration multiplier stayed at `1.5` the whole run
- one brief reconnect/recover episode on the user stream produced noisy watchdog reconnect warnings and two `kill_pending` verdicts, but no actual kill, no flatten, and no downstream trading damage

Current read:
- nothing in that run looked broken relative to the branch's intended stabilization goals
- that run does not itself live-validate fill/merge resolution paths because no fills occurred

Confidence:
- no obvious active regression in the current branch goals: `0.88`
- fill/merge paths fully validated by that passive run alone: `0.45`

## What Is Still Open

### 1. Watchdog reconnect policy is still noisy

Current truth:
- the false "silent but actually alive" user-WS classification appears fixed
- reconnect-window escalation can still look aggressive/noisy during short reconnect bursts

Current verdict:
- worth tightening later
- not a current trading-path blocker from the evidence gathered so far

Confidence: **high**

### 2. `managed_markets` is still a misleading productivity proxy

Current truth:
- recent runs continue to show the familiar early `67 -> 2` style collapse in `managed_markets`
- on the latest checked run, calibration did not move, so that specific `managed_markets` behavior cannot be blamed on the old calibration feedback-loop bug
- the stronger hypothesis now is that `managed_markets` is a loose runtime-control-plane set and a poor operator proxy for "active productive bid markets"

Current verdict:
- this is still worth investigating
- it is an operator-clarity / productivity-diagnosis issue, not a current hedge/merge safety blocker

Confidence: **medium**

### 3. Frontier same-cycle behavior still lacks rich live exercise

Current truth:
- the code-level same-cycle handoff fixes remain in place
- the later live sessions still have not given a strong enough real frontier-rotation sample to fully close `#16` / `#17A` with live evidence

Current verdict:
- keep the fix
- do not treat this as newly broken
- recognize that live closure remains partial

Confidence: **medium**

## Current Code / Strategy Alignment

Current branch truth:
- the runtime now supports normal quoting/trading on `neg_risk` markets and paired single-question merge routing for both standard and neg-risk paths
- `STRATEGY.md` has been updated during the later April 16-17 work to describe neg-risk adapter routing, order-level calibration truth, fill anchoring fallback, halted-cleanup reconciliation, and bounded relayer retry behavior

Current verdict:
- I do not currently see the earlier branch-story code/strategy misalignment around neg-risk merge behavior in the current integrated branch state

Confidence: **high**

## Bottom Line

- the original frontier safety fixes remain the correct base for this line. Confidence: **high**
- the reconciliation, merge-truth, WS-liveness, calibration, fill-truth, relayer-hardening, and neg-risk merge fixes are now integrated together on this branch. Confidence: **high**
- this branch has already passed the available local suite plus the scripted live matrix on the integrated code tree. Confidence: **high**
- there are still worthwhile follow-up investigations around watchdog reconnect noise, `managed_markets` semantics, and broader productivity economics, but they do not currently look like reasons to hold this branch. Confidence: **high**

Recommended next move from the current branch state:
1. push `fix/fontier-churn-admission-and-handoff`
2. merge `origin/main`
3. rerun the local and live validation matrix on the merged branch
4. if that stays green, treat this branch as the ready candidate to close upward into `main`
