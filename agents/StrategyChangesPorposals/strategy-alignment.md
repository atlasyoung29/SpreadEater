# STRATEGY.md Alignment Update — 2026-04-10

This document explains every change made to STRATEGY.md to align it with the actual codebase behavior. Each change is independently reviewable.

---

## Change 1: Section 6.6 — Reconciliation Flow Corrected

**What was wrong:**
STRATEGY.md said reconciliation uses "the same hedge-resolve-merge flow as a normal fill." In the code (`live_engine.rs:4349-4409`, `finalize_reconciliation_resolution_success`), reconciliation skips CTF merge entirely and exits via `place_inventory_asks()` at line 4401.

**What was changed:**
- Replaced "Execute the same hedge-resolve-merge flow" with accurate description: reconciliation uses the shared hedge-resolve pipeline but exits via inventory asks, not CTF merge
- Added rationale: reconciliation is a recovery path — the goal is to flatten back to neutral, not create new paired positions. Buying more opposite tokens to merge would add risk during a risk-recovery event.
- Added: on first failure, the market is killed (no retry escalation) — per `handle_reconciliation_resolution_failure` at line 4445

**Why this is intentional (not a bug):**
FillHandler merges because YES+NO pairs are a natural byproduct of a clean hedge. Reconciliation detects something already went wrong (missed fill, partial hedge, overhedge). The correct response is to shed one-sided exposure via asks, not compound the situation by creating new positions.

**Code references:**
- `src/runtime/live_engine.rs:4401` — `place_inventory_asks()` call (no merge)
- `src/runtime/live_engine.rs:5548-5630` — FillHandler merge path (for comparison)
- `src/runtime/live_engine.rs:4445` — kill market on first failure

---

## Change 2: Section 10 — Fallback Asks Reclassified as Functional

**What was wrong:**
Line 348 said: "Code attempts to place asks on merge failure, but they are silently filtered by the sellable inventory check when positions are fully hedged. Asks are effectively non-functional as an exit mechanism."

This is incorrect. Fallback inventory asks ARE functional. They work when inventory exists and activate in three scenarios:
1. FillHandler when CTF merge fails (`live_engine.rs:5584-5629`)
2. FillHandler when CTF merger is not configured (`live_engine.rs:5632-5661`)
3. Reconciliation — always (`live_engine.rs:4401`)

They naturally stop producing orders when inventory depletes. That's not "silent filtering" — it's having nothing left to sell.

**What was changed:**
Rewrote the bullet to accurately describe fallback inventory asks as a real secondary exit mechanism with the three activation scenarios listed above.

---

## Change 3: Section 5 — CTF Merge Approval Check Added

**What was undocumented:**
The first merge in a session requires an on-chain approval check (`ensure_approval()` in `ctf_merge.rs:116-176`). It calls `isApprovedForAll(Safe, CTF)` on the ERC-1155 contract. If not approved, it submits a `setApprovalForAll` Safe transaction. This is a one-time check per session, guarded by an atomic bool at `ctf_merge.rs:26`. If this step fails, all subsequent merges will also fail.

**What was changed:**
Added a "Merge Prerequisites" subsection to Section 5 documenting the approval check, its one-time-per-session behavior, and the failure implication.

---

## Change 4: Section 5 — Receipt Polling Timeout Added

**What was undocumented:**
After submitting a merge transaction to Polygon, the bot polls `eth_getTransactionReceipt` up to 30 times at 2-second intervals (60 seconds max) at `ctf_merge.rs:426-460`. If no receipt is found after 60 seconds, it logs a warning but returns `Ok()` — meaning the merge is treated as successful even though the transaction may still be pending on-chain.

This 60-second window is NOT covered by the 10-second hedge timeout, which only wraps `execute_resolution_plan` at `live_engine.rs:6198`. The full fill-to-flat lifecycle can therefore exceed 10 seconds.

**What was changed:**
Added a "Merge Confirmation" subsection to Section 5 documenting the polling behavior, timeout, and its relationship to the hedge timeout.

---

## Change 5: Section 5 — Unconfigured Merger Fallback Added

**What was undocumented:**
If `POLY_PRIVATE_KEY` or `POLY_FUNDER` environment variables are not set, the CTF merger initializes as `None` (`live_engine.rs:407-430`). When `ctf_merger` is `None`, all post-hedge exits silently fall back to inventory asks with no merge attempt (`live_engine.rs:5632-5661`). The only indication is a startup log line.

**What was changed:**
Added to the "Merge Prerequisites" subsection noting the env var requirements and the silent fallback behavior.

---

## Change 6: Section 5 — Fractional Pair Handling Added

**What was undocumented:**
`merge_eligible_pairs()` at `live_engine.rs:6749-6755` computes `min(yes_size, no_size).floor()`. This means fractional pairs are never merged. For example, holding 10.5 YES + 10.5 NO only merges 10 pairs — the 0.5 remainder on each side stays as inventory.

**What was changed:**
Added a bullet to "Merge Timing" noting whole-pair-only behavior and fractional remainder handling.

---

## Change 7: Section 3.2 — Ask Types Distinguished

**What was wrong:**
Section 3.2 said asks are "not used as an exit mechanism." This is true for *reward asks* (the 4-leg candidate quote set), but the bot also places *inventory asks* which ARE an exit mechanism. The doc conflated two different ask types, creating confusion when readers encountered the fallback ask behavior in Sections 5 and 6.6.

**What was changed:**
- Clarified that the Section 3.2 statement applies to *reward asks* specifically
- Added a note distinguishing inventory asks (fallback exit mechanism) from reward asks, with cross-references to Section 5 and Section 6.6

---

## Change 8: Section 6.6 — No Post-Hedge Exposure Verification

**What was undocumented:**
FillHandler checks `net_exposure <= 0.5` after hedge execution (`live_engine.rs:5453`) and sells the remainder if exceeded. Reconciliation does NOT perform this check — it accepts whatever exposure results from the hedge attempt and relies on inventory asks to handle remaining exposure passively. This is a meaningful behavioral difference between the two paths.

**What was changed:**
Added a bullet to Section 6.6 noting the absence of post-hedge exposure verification and the rationale (inventory asks handle residual exposure passively).

---

## Change 9: Section 8 — Lifecycle Diagram Updated

**What was wrong:**
The reconciliation block in the lifecycle diagram said "Execute same hedge-resolve-merge flow" — same incorrect claim as Section 6.6.

**What was changed:**
Updated to reflect the actual flow: hedge-resolve (no merge), place inventory asks, kill on first failure.

---

## Post-Review Polish (Items 10-14)

The following 5 changes were applied after a second-pass review identified correctness and clarity issues in Changes 1-9.

---

## Change 10: Section 7 — Startup "Same Flow" Claim Fixed

**What was wrong:**
Section 7 said startup follows "the same hedge-resolve-merge flow as normal fills." Code verification confirmed startup calls `reconcile_unhedged_positions()` (line 885) → `execute_reconciliation_hedge()` → `finalize_reconciliation_resolution_success()` → `place_inventory_asks()` at line 4401. No CTF merge. This is the same reconciliation path, not the FillHandler path.

This was the same error already fixed in Section 6.6 (Change 1) and Section 8 (Change 9), but Section 7 was missed in the original pass.

**What was changed:**
Replaced "Follow the same hedge-resolve-merge flow as normal fills" with "Uses the reconciliation path: hedge-resolve flow with inventory asks exit (no CTF merge). See Section 6.6."

---

## Change 11: Section 6.6 — Rationale Reworded

**What was wrong:**
The original rationale said "buying more of the opposite token just to merge would add risk." This is misleading — reconciliation literally buys opposite tokens (that's the hedge step). The distinction isn't about buying tokens; it's about what happens AFTER the hedge: CTF merge (on-chain Polygon tx with its own failure modes) vs. inventory asks (exchange-level, simpler).

**What was changed:**
Replaced the rationale bullets with: reconciliation hedges to balance the position, then exits via inventory asks rather than CTF merge. This avoids adding an on-chain transaction — with its own failure modes — to a recovery path where something already went wrong. Inventory asks are a simpler, exchange-level exit that doesn't depend on Polygon transaction execution.

---

## Change 12: Section 6.6 — `recon_failure_counts` Note Fixed

**What was wrong:**
Change 1 added "on first failure, the market is killed (no retry escalation)." But the existing `recon_failure_counts` note said "reconciliation simply retries each cycle." These contradict — if the market is killed on first failure, the counter never accumulates and retries never happen.

**What was changed:**
Replaced the note with: "A failure counter (`recon_failure_counts`) exists in code but is effectively dead code — reconciliation kills the market on first hedge failure, so the counter never accumulates."

---

## Change 13: Section 10 — Fallback Asks Efficiency Caveat Added

**What was missing:**
Without context on the cost difference, a reader might wonder why the bot bothers with CTF merge when inventory asks work fine.

**What was changed:**
Added one sentence to the fallback asks bullet: "Inventory asks recover market price for each token sold, which is typically less than the $1.00/pair that CTF merge guarantees — they are a viable but costlier fallback exit."

---

## Change 14: Section 5 — State-Divergence Window Documented

**What was missing:**
The 60-second receipt polling that returns Ok() with no receipt creates a window where internal state (tokens merged, USDC expected) may diverge from on-chain reality (tokens may still exist). Relevant for debugging post-merge balance discrepancies.

**What was changed:**
Added to the Merge Confirmation subsection: "This creates a brief window where the bot's internal position state may diverge from on-chain reality (tokens considered merged internally but may still exist on-chain). The next API position sync resolves the discrepancy."

---

## Remaining Edits from `discrepanies_from_StrategyPlan.md` (2026-04-08)

These 5 edits were proposed in the older discrepancy review and applied on 2026-04-10 after re-verifying all claims against current code. One (Edit 2C) was adjusted because the original claim was stale.

---

## Change 15: Section 2.3 — Held-Bid Eviction Cutoff Phrasing

**What was wrong:**
Section 2.3 hardcoded "62 seconds" but the code computes `poll_interval_secs + 1` at `live_engine.rs:2715`. If `poll_interval_secs` ever changes, the doc would silently become wrong.

**What was changed:**
Replaced "at least **62 seconds** old" with "at least `poll_interval_secs + 1` seconds old (currently **62s**)".

---

## Change 16: Section 10 — `max_position_size` Legacy Bandaid

**What was undocumented:**
A vestigial per-market cap (`risk.max_position_size`) is still enforced in `risk.rs:83-96` (force-halts markets) and `risk.rs:232-241` (rejects non-hedge entries). This contradicts Section 6.1's "no secondary cap" statement. Two latent bugs: (a) documented as USDC in `config.rs:117` but compared against share counts, (b) disproportionately aggressive on cheap-outcome markets.

**What was changed:**
Added a new bullet to Section 10 documenting the legacy cap, its contradiction with Section 6.1, and the two latent bugs.

---

## Change 17: Section 10 — Hedge Timeout Clock Starts on Discovery Cycle

**What was undocumented:**
The `unhedged_since` clock is only set inside `update_market_exposure()`, which runs on the ~61s discovery cycle. A fill arriving 1 second after a cycle won't start its 10-second countdown for ~60 seconds, making the realized kill-switch latency 10–~70s. This is separate from the existing timeout-scope note (which covers the `execute_resolution_plan` wrapper).

**What was changed:**
Added a new bullet to Section 10 documenting the discovery-cycle dependency.

---

## Change 18: Section 10 — API-Hang Mutex Hold (Adjusted)

**Original claim (stale):** `client.rs` constructs `reqwest::Client` with no request timeout.
**Current reality:** A 15-second timeout exists at `client.rs:89-90` (likely added via `hedge-timeout-gap.md` Option C).

The mutex-hold concern is partially mitigated but still valid: the per-market hedge mutex stays held for up to 15 seconds during an HTTP request. The market-level kill switch fires correctly, but the mutex blocks subsequent fills on the same market until the request completes.

**What was changed:**
Added a new bullet to Section 10 with the adjusted claim (acknowledging the 15s timeout exists, documenting the remaining mutex-hold concern).

---

## Change 19: Section 9 — Config-Drift Footnote

**What was missing:**
No indication that live `config.json` values may differ from the documented defaults in the config table.

**What was changed:**
Added footnote after the config table: "Live operating values may drift from these defaults; refer to `config.json` for currently deployed values."

---

## Bonus: Section 10 — Removed Stale `recon_failure_counts` Bullet

Section 10 had a bullet saying "`recon_failure_counts` exists but is not wired up. No automatic kill after repeated failures." This contradicted the corrected Section 6.6 (Change 12) which says reconciliation kills on first failure. The Section 10 bullet was removed since Section 6.6 is now the authoritative location for this information.

---

## Operator Follow-Ups (Not Actioned — Tracked Here for Reference)

These are config/code changes identified during the discrepancy review. They are NOT doc edits and have NOT been applied:

1. **`config.json risk.cash_reserve`: 0 → 50** — The bot may be trading without its safety buffer. STRATEGY.md documents $50 as the intended value.
2. **`config.json discovery.poll_interval_secs`: 60 → 61** — Config says 60, doc and code default say 61.
3. **Strip `max_position_size` enforcement from `risk.rs`** — Requires separate code review. Need to verify no hedged position relies on the kill-switch as a backstop before removing.
4. **Evaluate `hedge-timeout-gap.md` remaining concerns** — The 15s reqwest timeout was added, but the mutex-hold duration concern remains. Assess whether a `tokio::timeout` wrapper on `execute_hedge` is still warranted.
