# Per-Share Ranking Revert — Implementation Plan

## Executive Summary

**What:** Revert the viability/ranking denominator from per-dollar (posted notional = `price * size`) back to per-share (hedge-aware = `size` alone).

**Why:** The bot's actual budget consumption model is `$1/share`, as confirmed by `order_manager.rs` line 377-407: `committed_exposure()` sums `order.size` (not `price * size`). In binary markets, `order_cost(price) + hedge_cost(1 - price) = $1`, so each share costs exactly $1 of account capacity regardless of bid price. Ranking by `price * size` (posted notional) systematically over-rewards cheap bids that don't actually consume less account capacity, producing suboptimal market selection.

**Scope:** 4 files changed, 1 test file updated. No new config fields needed. The `min_return_pct` threshold semantics change from "per dollar committed" to "per share committed" but the numeric value (0.0025 = 0.25%) remains appropriate because the denominator gets larger for cheap bids (shares > dollars when price < $1), making the threshold easier to meet — which is correct.

---

## Change 1: `src/strategy/viability.rs` — Revert denominator to per-share

**Current code (lines 50-66):**
```rust
    // Capital committed = sum of (price × size) for approved bid legs.
    // Falls back to effective_quote_size if no approved bids (e.g. ask-only).
    let bid_capital: Decimal = quote_set
        .candidates
        .iter()
        .filter(|c| {
            c.status == crate::models::QuoteStatus::Approved && c.leg.is_bid()
        })
        .map(|c| c.price * c.size)
        .sum();
    let capital = if bid_capital > Decimal::ZERO {
        bid_capital
    } else {
        effective_quote_size
    }
    .max(Decimal::ONE); // prevent div-by-zero
    let return_pct = estimated_edge / capital;
```

**New code:**
```rust
    // Capital committed = sum of size for approved bid legs (hedge-aware: $1/share).
    // In binary markets, bid + hedge ≈ $1, so shares ≈ true account capacity consumed.
    // Falls back to effective_quote_size if no approved bids (e.g. ask-only).
    let bid_shares: Decimal = quote_set
        .candidates
        .iter()
        .filter(|c| {
            c.status == crate::models::QuoteStatus::Approved && c.leg.is_bid()
        })
        .map(|c| c.size)
        .sum();
    let capital = if bid_shares > Decimal::ZERO {
        bid_shares
    } else {
        effective_quote_size
    }
    .max(Decimal::ONE); // prevent div-by-zero
    let return_pct = estimated_edge / capital;
```

**Also update the doc comment** at the top of the function (line 19):
- Current: `/// - return_per_dollar = estimated_edge / capital_committed`
- New: `/// - return_per_share = estimated_edge / shares_committed  (hedge-aware: $1/share)`

---

## Change 2: `src/models/decision.rs` — Rename field for clarity

**Current code (line 14-16):**
```rust
    /// Return per dollar of committed capital (estimated_edge / capital_committed).
    #[serde(default)]
    pub return_per_dollar: Decimal,
```

**New code:**
```rust
    /// Return per share committed (estimated_edge / shares_committed).
    /// Hedge-aware: each share costs $1 total (bid + hedge), so this equals return per dollar of true account capacity.
    #[serde(default, alias = "return_per_dollar")]
    pub return_per_share: Decimal,
```

The `alias = "return_per_dollar"` ensures backward-compatible deserialization of any persisted JSON payloads.

---

## Change 3: `src/strategy/viability.rs` — Update struct field assignment

**Current (line 82):**
```rust
        return_per_dollar: return_pct,
```

**New:**
```rust
        return_per_share: return_pct,
```

---

## Change 4: `src/runtime/live_engine.rs` — Update ranking sort and logging

### 4a: Phase 2 ranking sort (lines 830-845)

**Current:**
```rust
        // === Phase 2: Rank by return per dollar committed (highest first) ===
        evaluations.sort_by(|a, b| {
            let a_rpd = a.report.reward_viability.as_ref()
                .map(|v| v.return_per_dollar).unwrap_or(Decimal::ZERO);
            let b_rpd = b.report.reward_viability.as_ref()
                .map(|v| v.return_per_dollar).unwrap_or(Decimal::ZERO);
            b_rpd.cmp(&a_rpd)
        });
```

**New:**
```rust
        // === Phase 2: Rank by return per share committed (highest first) ===
        evaluations.sort_by(|a, b| {
            let a_rps = a.report.reward_viability.as_ref()
                .map(|v| v.return_per_share).unwrap_or(Decimal::ZERO);
            let b_rps = b.report.reward_viability.as_ref()
                .map(|v| v.return_per_share).unwrap_or(Decimal::ZERO);
            b_rps.cmp(&a_rps)
        });
```

### 4b: Status log per-market capital computation (lines 1971-1978)

Rename `capital_committed` → `shares_committed`, `reward_per_dollar` → `reward_per_share`, `reward_per_dollar_eff` → `reward_per_share_eff`.

Change the capital computation from:
```rust
let capital_committed: Decimal = orders.iter().map(|o| o.price * o.size).sum();
```
To:
```rust
let shares_committed: Decimal = orders.iter()
    .filter(|o| o.side == Side::Buy)
    .map(|o| o.size)
    .sum();
```

### 4c: MarketStatus struct (lines 1936-1947)

Rename fields:
- `capital_committed` → `shares_committed`
- `reward_per_dollar` → `reward_per_share`
- `reward_per_dollar_eff` → `reward_per_share_eff`

### 4d: Status log output formatting (lines 2040, 2067-2068, 2073-2078, 2085-2086)

Update all `r_per_dollar` → `r_per_share`, change format labels from `¢/$` to `¢/sh`, rename `avg_r_per_dollar` → `avg_r_per_share`.

---

## Change 5: `src/monitor/emitters.rs` — Update test fixture

**Line 618:**
- Current: `return_per_dollar: dec!(0.175),`
- New: `return_per_share: dec!(0.175),`

---

## Change 6: Update tests

### 6a: `src/strategy/viability.rs` inline tests (lines 88-258)

All `.return_per_dollar` references → `.return_per_share`.

**Test A** (`different_prices_yield_different_per_dollar_returns`): Under per-share model, two bids with same size but different prices will have equal return (same hedge cost, same shares). **Rewrite assertion to `assert_eq!`** and rename test to `same_size_same_hedge_cost_yields_same_per_share_return`.

**Test B** (`higher_absolute_reward_can_have_lower_per_dollar_return`): Same issue — same size = same per-share return. **Rewrite with different sizes** to demonstrate meaningful ranking differences.

**Test C** (`estimated_reward_includes_discount_factor`): Rename `.return_per_dollar` → `.return_per_share`. Assertion should still pass since the numerator (reward) hasn't changed.

### 6b: `tests/unit/strategy/reward_per_dollar_tests.rs`

Rename to `reward_per_share_tests.rs` or update in-place. Change all `reward_per_dollar` function names and references to `reward_per_share`. Remove `capital_committed_accounts_for_price` test (validates price-based returns which no longer apply). Update `tests/unit/strategy/mod.rs` module declaration.

---

## Change 7: Documentation

### 7a: `agents/changelog.md` — Add entry at top
Document the revert with rationale.

### 7b: `agents/summary.md` — Update
Reflect that ranking is now per-share again.

### 7c: `src/config.rs` line 59
- Current comment: `/// 0.0025 = 0.25%. Return = estimated_edge / capital_committed.`
- New: `/// 0.0025 = 0.25%. Return = estimated_edge / shares_committed.`

---

## Order of Operations

1. **Change 2** (rename `RewardViability` field) — compiler will flag every site to update
2. **Change 3** (fix struct assignment in viability.rs)
3. **Change 1** (revert denominator)
4. **Change 4** (fix all live_engine.rs references)
5. **Change 5** (fix emitters.rs test fixture)
6. `cargo build` — should compile cleanly
7. **Change 6** (update all tests)
8. `cargo test` — all tests should pass
9. **Change 7** (documentation)

---

## Verification Checklist

- [ ] `cargo build` compiles cleanly (after steps 1-5)
- [ ] `cargo test --lib` passes (after step 6)
- [ ] `cargo test --test '*'` passes (after step 6)
- [ ] `cargo clippy` has no new warnings
- [ ] `grep -rn "return_per_dollar" src/` returns zero results
- [ ] Verify ranking log line includes `return_per_share`

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Persisted JSON with `return_per_dollar` fails to deserialize | Medium | Low | `serde(alias = "return_per_dollar")` handles this |
| `min_return_pct` threshold too loose with larger denominator | Low | Medium | For prices < $1, per-share denominator is larger, making threshold easier to meet — this is correct behavior. Monitor in first live session. |
| Test A and B assertions fail due to equal returns | Certain | Low | Tests must be rewritten as described. Verify manually. |
| Status log format change breaks log parsing | Low | Low | Cosmetic only: `¢/$` → `¢/sh`. No automated parsing exists. |

---

## Downstream Effects

1. **Event emission** (`emitters.rs`): `RewardViability` serializes as `return_per_share`. Alias handles deserialization.
2. **Orchestrator** (`orchestrator.rs`): Passes args through, no change needed.
3. **Config** (`config.rs`): `min_return_pct` name is generic, only comment needs updating.
4. **`committed_exposure()`** in `order_manager.rs`: Already uses `order.size` — consistent with per-share model. No changes needed.
