# Issue #16: Same-Cycle Frontier Handoff — Implementation Notes

**Status:** PAUSED (2026-04-12) — work preserved in `stash@{0}` and branch `frontier-same-cycle-handoff` (commit `90141db`). To be resumed after verifying #17A and #17B fixes on main.

## Context

Frontier rotation currently cancels a loser bid in cycle N and defers entrant placement to cycle N+1 (~61 seconds later). The 1.5-hour live run (`data/events/run_20260411_215143`) confirmed this creates a **systematic ~60-second idle-cash window** on every rotation:

- **12 rotations** in 1.5 hours
- **Every single one** had a 59.5–61.0 second idle gap
- **$1,349 total capital** cycled through idle windows
- **11 minutes** cumulative idle time

Fix: after cancelling the loser, poll for cancel verification within the same cycle. Once confirmed, fresh-evaluate the best non-held market with current books/budget and place immediately.

## Where the Work Is Preserved

- **Git stash:** `stash@{0}: On frontier-same-cycle-handoff: WIP #16 frontier same-cycle handoff` — full working tree at time of branch switch, includes all implementation + START.py fix
- **Git branch:** `frontier-same-cycle-handoff` at commit `90141db` "WIP #16 frontier same-cycle handoff" (missing START.py fix)
- **GitHub issue:** #16 has a detailed comment with investigation findings

## Files Changed (from the stash)

### 1. `src/config.rs`

Add after the `min_frontier_improvement` field in `StrategyConfig`:

```rust
    /// Seconds to wait for loser cancel verification before deferring entrant
    /// placement to the next discovery cycle. Set to 0 to disable same-cycle handoff.
    #[serde(default = "default_frontier_handoff_window_secs")]
    pub frontier_handoff_window_secs: u64,
```

Add default function after `default_min_frontier_improvement()`:

```rust
fn default_frontier_handoff_window_secs() -> u64 {
    5
}
```

Update the test/default `StrategyConfig` initializer at line ~241 to include:

```rust
    frontier_handoff_window_secs: default_frontier_handoff_window_secs(),
```

### 2. `src/strategy/viability.rs`

Update `test_strategy_config()` at line ~133 to include:

```rust
    frontier_handoff_window_secs: 5,
```

### 3. `config.json`

Add under `strategy`:

```json
"frontier_handoff_window_secs": 5,
```

### 4. `src/runtime/live_engine.rs` — Core implementation

**A. Add new enum near the other frontier structs (near `MarketRankKey`):**

```rust
#[derive(Debug)]
enum SameCycleHandoffResult {
    Placed(String),
    TimedOut,
    Disabled,
    NoReservation,
    NoPlaceableMarket,
    Failed,
}
```

**B. Add two new methods before `activate_frontier_reservation`:**

```rust
async fn run_same_cycle_frontier_handoff(
    &self,
    _cycle_id: &str,
    managed: &mut HashMap<String, CanonicalMarket>,
) -> SameCycleHandoffResult {
    let window_secs = self.config.strategy.frontier_handoff_window_secs;
    if window_secs == 0 {
        return SameCycleHandoffResult::Disabled;
    }

    let window = StdDuration::from_secs(window_secs);
    let poll_interval_ms = 250;
    let deadline = Instant::now() + window;

    let loser_id = {
        let res = self.frontier_reservation.read().await;
        match res.as_ref() {
            Some(r) => r.loser_condition_id.clone(),
            None => return SameCycleHandoffResult::NoReservation,
        }
    };

    info!(
        loser = %loser_id,
        window_secs = window_secs,
        "Frontier same-cycle handoff started"
    );

    while Instant::now() < deadline {
        self.order_manager.retry_pending_cancels().await;

        if !self
            .order_manager
            .has_bid_orders_or_pending_cancels(&loser_id)
            .await
        {
            info!("Frontier same-cycle handoff: loser cancel verified");

            match self.select_best_post_cancel_market(managed).await {
                Some((market, quote_set, _report, trace_ids)) => {
                    let position = self
                        .position_manager
                        .get_position(&market.condition_id)
                        .await;
                    let min_size = market.reward_config.min_size;

                    if let Err(e) = self
                        .order_manager
                        .place_quotes(
                            &market,
                            &quote_set,
                            position.as_ref(),
                            min_size,
                            Some(&trace_ids),
                            "frontier_reservation",
                            None,
                        )
                        .await
                    {
                        warn!(
                            condition_id = %market.condition_id,
                            error = %e,
                            "Same-cycle handoff placement failed"
                        );
                        self.clear_frontier_reservation("same_cycle_placement_failed")
                            .await;
                        return SameCycleHandoffResult::Failed;
                    }

                    let has_bids = self
                        .order_manager
                        .get_market_orders(&market.condition_id)
                        .await
                        .iter()
                        .any(|o| o.leg.is_bid());

                    if has_bids {
                        let entrant_id = market.condition_id.clone();
                        let was_original = {
                            let res = self.frontier_reservation.read().await;
                            res.as_ref()
                                .map(|r| r.entrant_condition_id == entrant_id)
                                .unwrap_or(false)
                        };
                        managed.insert(entrant_id.clone(), market);
                        self.clear_frontier_reservation("same_cycle_placed").await;
                        info!(
                            entrant = %entrant_id,
                            was_original_reservation = was_original,
                            "Frontier same-cycle handoff placed"
                        );
                        return SameCycleHandoffResult::Placed(entrant_id);
                    } else {
                        self.clear_frontier_reservation("same_cycle_place_no_bids")
                            .await;
                        return SameCycleHandoffResult::NoPlaceableMarket;
                    }
                }
                None => {
                    info!("Frontier same-cycle handoff: no viable market after fresh evaluation");
                    self.clear_frontier_reservation("same_cycle_no_viable_market")
                        .await;
                    return SameCycleHandoffResult::NoPlaceableMarket;
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
    }

    info!("Frontier same-cycle handoff timed out — deferring to next cycle");
    SameCycleHandoffResult::TimedOut
}

async fn select_best_post_cancel_market(
    &self,
    _managed: &HashMap<String, CanonicalMarket>,
) -> Option<(
    CanonicalMarket,
    QuoteSet,
    DecisionReport,
    HashMap<QuoteLeg, String>,
)> {
    let known = self.known_markets.read().await;

    let mut candidates: Vec<(
        CanonicalMarket,
        QuoteSet,
        DecisionReport,
        HashMap<QuoteLeg, String>,
        MarketRankKey,
    )> = Vec::new();

    for market in known.values() {
        let existing = self
            .order_manager
            .get_market_orders(&market.condition_id)
            .await;
        if existing.iter().any(|o| o.leg.is_bid()) {
            continue;
        }

        if !self
            .risk_manager
            .is_market_tradable(&market.condition_id)
            .await
        {
            continue;
        }

        match self.evaluate_market(market).await {
            Ok((_yes_book, _no_book, quote_set, report)) => {
                if !report.would_trade {
                    continue;
                }
                let rank = market_rank_key(&quote_set, &report);
                let trace_ids = build_quote_trace_ids(&quote_set);
                candidates.push((market.clone(), quote_set, report, trace_ids, rank));
            }
            Err(e) => {
                debug!(
                    condition_id = %market.condition_id,
                    error = %e,
                    "Skipping market in post-cancel evaluation"
                );
                continue;
            }
        }
    }

    if candidates.is_empty() {
        return None;
    }

    candidates.sort_by(|a, b| {
        compare_rank_keys(a.4, &a.0.condition_id, b.4, &b.0.condition_id)
    });

    candidates
        .into_iter()
        .next()
        .map(|(market, quote_set, report, trace_ids, _rank)| {
            (market, quote_set, report, trace_ids)
        })
}
```

**C. Integration point** — in Phase 3, after the frontier loser block (after the `managed.insert`/`managed.remove` block ending with `continue;`), insert **before** the `continue;`:

```rust
    // Same-cycle handoff: poll for cancel verification and place entrant immediately
    let handoff_result = self
        .run_same_cycle_frontier_handoff(&cycle_id, &mut managed)
        .await;
    if let SameCycleHandoffResult::Placed(ref entrant_id) = handoff_result {
        reservation_processed_condition_id = Some(entrant_id.clone());
    }
```

### 5. STRATEGY.md

**Section 2.3** — replace the final rotation bullet with:

```
- If a better entrant is found, the loser market's bids are canceled in that cycle, then the bot attempts a **same-cycle handoff**: polls for cancel verification every 250ms within a bounded window (`frontier_handoff_window_secs`, default 5s). Once verified, a **fresh evaluation** of all non-held markets is performed with current books and budget, and the best viable market is placed immediately — this may differ from the originally reserved entrant if conditions changed. If the cancel is not verified in time, the capital is reserved and unrelated new bid entries are frozen until the **next** discovery cycle gives the reserved entrant first claim
```

**Section 9** — add new row to config table:

```
| `strategy.frontier_handoff_window_secs` | 5 | Seconds to poll for cancel verification before deferring to next cycle (0 = disable) |
```

### 6. `agents/changelog.md`

Prepend under the `# Changelog` header:

```
## 2026-04-11 — Same-cycle frontier handoff to eliminate idle-cash window

- Added `frontier_handoff_window_secs` config parameter (default 5s) to `StrategyConfig` in [config.rs](src/config.rs)
- Implemented `run_same_cycle_frontier_handoff()` in [live_engine.rs](src/runtime/live_engine.rs): after cancelling a frontier loser, polls for cancel verification every 250ms within the handoff window. Calls `retry_pending_cancels()` during polling to push verification forward.
- Implemented `select_best_post_cancel_market()` in [live_engine.rs](src/runtime/live_engine.rs): once cancel is confirmed, fresh-evaluates all non-held candidate markets with current books and budget, ranks by `reward_per_share`, and places the best viable market. Does not blindly reuse the stale evaluation from earlier in the cycle.
- If the handoff window times out, falls back to the existing next-cycle reservation activation (no behavior change from baseline).
- Live run evidence: 12 rotations in 1.5 hours, every one had ~60s idle gap. This change reduces the gap to <5s.
- Updated STRATEGY.md Section 2.3 and Section 9 config table.
```

### 7. `agents/summary.md`

Add to the dated entries list:

```
- 2026-04-11: added same-cycle frontier handoff to eliminate ~60-second idle-cash windows during frontier rotation. After cancelling the loser, the bot now polls for cancel verification within a configurable window (`frontier_handoff_window_secs`, default 5s). Once confirmed, it fresh-evaluates all non-held markets with current books and budget and places the best viable market immediately — reducing the idle gap from ~60s to <5s. Falls back to next-cycle reservation if cancel is not verified in time. See GitHub #16.
```

## Key Design Decisions

1. **Fresh re-evaluation after cancel confirmation** — does NOT reuse stale cycle-start evaluations. Calls `evaluate_market()` on candidates with current books to ensure hedgeability, depth, and viability are up to date.

2. **Best market, not necessarily the reserved entrant** — the original reservation is a hint, not a commitment. After cancel, the best currently viable non-held market is selected.

3. **`retry_pending_cancels()` called inside the poll loop** — pushes cancel verification forward during the handoff window rather than passively waiting.

4. **250ms poll interval** — fast enough to catch sub-second cancel confirmations, infrequent enough to not hammer the API.

5. **5-second default window** — cancel verification typically takes <2 seconds. 5 seconds is generous while being well under the 61-second cycle. Configurable via `frontier_handoff_window_secs`. Set to 0 to disable.

6. **Freeze remains active** — even after same-cycle placement, `freeze_new_bid_entries` stays true for the rest of the cycle. Prevents other new bids from racing.

7. **Next-cycle fallback is untouched** — if same-cycle fails or times out, existing behavior works exactly as before.

## Build Verification

When resumed, this is verified to build cleanly:

```bash
PATH="$HOME/.cargo/bin:/c/msys64/mingw64/bin:$PATH" CARGO_TARGET_DIR="/c/rust-build/spreadeater" cargo check
```

Produces only 2 pre-existing warnings (both from `risk.rs` unrelated to this work).

## To Resume

When ready:

```bash
git checkout frontier-same-cycle-handoff
git stash pop  # restores the full working tree including START.py fix
# ...continue work or commit as-is
```

Or manually re-apply the changes above.
