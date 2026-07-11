# Task: Fix Per-Dollar Reward Estimation Mismatches in SpreadEater

All changes are on the `feature/hedging-fix` branch. Do not modify `STRATEGY.md` or external project documentation.

---

## Fix 1: Apply discount factor to status `est_daily` logging

**File:** `src/runtime/live_engine.rs`

In the function `estimate_market_daily_reward()` (around line 2124):
- Current line 2154 returns: `daily_reward_total * score_proxy.estimated_share`
- Change it to: `daily_reward_total * score_proxy.estimated_share * self.config.strategy.reward_discount_factor`

This makes status logs consistent with the discounted selection math in `compute_viability()`.

---

## Fix 2: Rank viable markets by `return_per_dollar` instead of absolute `estimated_reward`

**File:** `src/runtime/live_engine.rs`

In Phase 2 ranking (around line 830-845):
- Currently sorts by `v.estimated_reward` (absolute daily $)
- Change the sort key to `v.return_per_dollar`
- Update the comment on line 830 from "Rank by estimated reward" to "Rank by return per dollar committed"

This ensures the most capital-efficient market gets budget first, which matters when capital is spread across multiple markets.

---

## Fix 3: Use actual committed capital (`price * size`) as viability denominator

**File:** `src/strategy/viability.rs`

The function `compute_viability()` needs a price input to compute committed capital:
1. Add a `bid_price: Decimal` parameter to the function signature (around line 20)
2. Change line 52 from: `let capital = effective_quote_size.max(Decimal::ONE);`
   To: `let capital = (bid_price * effective_quote_size).max(Decimal::ONE);`
3. Update the comment on lines 50-51 to reflect: "Capital committed = bid price * size (actual USDC exposure on the bid side)"

Then update the call site:

**File:** `src/runtime/live_engine.rs`

Find where `compute_viability()` is called (search for `compute_viability`). Pass the bid price from the approved quote candidate. The price is available on the quote candidate struct (`c.price` for the approved bid leg). If multiple legs are approved, use the primary bid leg's price, or compute a weighted average.

---

## Fix 4: Add regression tests against real production functions

**File:** `tests/unit/strategy/reward_per_dollar_tests.rs`

Add tests that call the actual `compute_viability()` function (not just standalone arithmetic) with:
- **Test case A:** Two markets, same share size, different bid prices → verify different `return_per_dollar` values
- **Test case B:** Two markets where Market A has higher absolute reward but lower per-dollar return than Market B → verify Market B would rank first under the new sort
- **Test case C:** Verify `estimated_reward` includes the discount factor

---

## Order of Operations

1. **Fix 1 first** (trivial, isolated)
2. **Fix 3 next** (changes the denominator — Fix 2 depends on correct `return_per_dollar`)
3. **Fix 2 after Fix 3** (ranking now uses the corrected metric)
4. **Fix 4 last** (validates everything)

Run `cargo test` after each fix to verify nothing breaks.
