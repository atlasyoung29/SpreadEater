# Hedge Resolution Redesign — Book-Aware Cost-Benefit Analysis

## Problem

When a fill is detected (real-time or via reconciliation), the current hedge executor places a BUY hedge at a hardcoded limit price of $0.99. This caused an incident on 2026-03-24 where:

- A NO_BID fill of 373 shares @ $0.74 required a BUY YES hedge
- The exchange locks collateral based on limit price: 373 x $0.99 = ~$369
- Account balance was ~$97 — order rejected for "not enough balance / allowance"
- A book-aware hedge at ~$0.27 would have required only ~$100, nearly affordable
- The system retried the same unaffordable $0.99 hedge for 16 minutes without resolving

The hardcoded $0.99 limit price wastes ~3.7x the collateral per share compared to book-aware pricing.

---

## Solution: Two-Option Cost-Benefit Analysis

When a fill creates unhedged exposure, the system walks BOTH books to determine the cheapest way to resolve each share of exposure.

### Option A — Hedge (BUY opposite token from the ask side)

```
cost_per_share = fill_price + hedge_ask_price - 1.00
```

- Result: paired position (YES + NO) worth $1.00 at resolution
- If cost < 0: locked-in profit (pair costs less than $1.00)
- If cost > 0: we pay the difference to close the exposure

### Option B — Sell back (SELL the filled token into the bid side)

```
cost_per_share = fill_price - sellback_bid_price
```

- Result: flat (no position)
- We lose the spread between our fill price and the current bid

### Decision Rule

For each share of exposure, compare hedge_cost vs sellback_cost:

- If `hedge_cost <= sellback_cost` → hedge that share
- If `sellback_cost < hedge_cost` → sell back that share

The crossover simplifies to: **hedge when `hedge_ask <= 1.00 - sellback_bid`**.

On ties, prefer hedge — paired inventory is eligible for CTF merge ($1.00 payout).

---

## Book Walk Algorithm

### Pre-Walk Steps

1. Cancel all resting orders for this market (frees committed capital, removes our orders from the book)
2. Refresh balance
3. Fetch both books fresh:
   - Opposite token ask side (for hedging)
   - Filled token bid side (for selling back)

### Walk Logic

Walk both books level-by-level, consuming depth from the cheapest option first:

```
Input:
  fill_price:    Decimal         (price we were filled at)
  hedge_asks:    [(price, size)] (opposite token ask levels, best-first)
  sellback_bids: [(price, size)] (filled token bid levels, best-first)
  total_size:    Decimal         (shares to resolve)

Output:
  HedgeResolution {
    hedge_shares:        Decimal
    hedge_limit_price:   Decimal  (worst ask level consumed + 1 tick buffer)
    sellback_shares:     Decimal
    sellback_limit_price: Decimal (worst bid level consumed)
  }

Algorithm:
  remaining = total_size
  hedge_shares = 0
  sellback_shares = 0
  hedge_ptr = 0  (index into hedge_asks)
  sell_ptr = 0   (index into sellback_bids)

  while remaining > 0:
    hedge_available = hedge_asks[hedge_ptr] if hedge_ptr < len else None
    sell_available  = sellback_bids[sell_ptr] if sell_ptr < len else None

    if both None:
      break  (no more depth on either side)

    if hedge_available is None:
      consume from sell_available, advance sell_ptr when exhausted
      sellback_shares += consumed
      remaining -= consumed
      continue

    if sell_available is None:
      consume from hedge_available, advance hedge_ptr when exhausted
      hedge_shares += consumed
      remaining -= consumed
      continue

    hedge_cost   = fill_price + hedge_available.price - 1.00
    sellback_cost = fill_price - sell_available.price

    if hedge_cost <= sellback_cost:
      consume from hedge_available (min of remaining, level size)
      hedge_shares += consumed
    else:
      consume from sell_available (min of remaining, level size)
      sellback_shares += consumed

    remaining -= consumed
    advance pointer if level exhausted

  hedge_limit_price   = worst consumed hedge level + 1 tick
  sellback_limit_price = worst consumed sell level
```

### Post-Walk: Affordability Gate

Even with book-aware pricing, the hedge may exceed available balance:

1. Compute required capital: `hedge_shares x hedge_limit_price`
2. If required > available_balance:
   - Reduce hedge_shares to `floor(available_balance / hedge_limit_price)`
   - Move excess shares from hedge bucket to sellback bucket
3. This is a secondary safety net — book-aware pricing makes this case rare

### Execution

1. Execute hedge order (GTC, cancel after 500ms) for `hedge_shares` at `hedge_limit_price`
2. Execute sell-back order (FOK) for `sellback_shares` at `sellback_limit_price`
3. Any remaining unfilled shares after both orders → fall through to existing residual check → kill_market path

---

## Worked Example

Fill: NO_BID 373 @ $0.74. Balance: $97.76.

**YES ask book (opposite — for hedging):**

| Price | Size |
|-------|------|
| 0.26  | 200  |
| 0.27  | 150  |
| 0.30  | 100  |

**NO bid book (filled token — for selling back):**

| Price | Size |
|-------|------|
| 0.73  | 300  |
| 0.72  | 200  |

**Walk:**

| Shares | Hedge ask | Hedge cost | Sellback bid | Sellback cost | Winner |
|--------|-----------|------------|--------------|---------------|--------|
| 200    | 0.26      | 0.00       | 0.73         | 0.01          | Hedge  |
| 150    | 0.27      | 0.01       | 0.73         | 0.01          | Hedge (tie → prefer hedge) |
| 23     | 0.30      | 0.04       | 0.73         | 0.01          | Sell back |

**Result:** Hedge 350 shares at limit $0.28 (0.27 + 1 tick). Sell back 23 shares FOK at $0.73.

**Capital required:** 350 x $0.28 = $98.00. Balance = $97.76. Affordability gate triggers — reduce hedge to `floor(97.76 / 0.28)` = 349 shares. Move 1 share to sellback.

**Final:** Hedge 349 at $0.28. Sell back 24 at $0.73.

Compare to current system: 373 x $0.99 = $369.27 → rejected entirely. Zero shares resolved.

---

## Integration Points

### Hedge Executor (`src/trading/hedge_executor.rs`)

- Add `HedgeResolution` struct
- Add `compute_hedge_resolution(fill_price, hedge_asks, sellback_bids, total_size) -> HedgeResolution`
- Modify `execute_buy_gtc_cancel` to accept computed limit price instead of calling `buy_hedge_limit_price()`
- Remove (or deprecate) the hardcoded `buy_hedge_limit_price()` returning 0.99

### Live Engine — Fill Handler (`src/runtime/live_engine.rs`)

- Before calling `execute_hedge`:
  1. Cancel resting orders for this market
  2. Refresh balance
  3. Fetch both books fresh
  4. Run `compute_hedge_resolution()`
- Execute hedge order for `hedge_shares`
- Execute sell-back order for `sellback_shares`
- Fall through to existing residual check for any unfilled remainder

### Live Engine — Reconciliation (`src/runtime/live_engine.rs`)

- Same book walk logic — reconciliation already fetches books at L3296-3313
- Pass computed resolution to hedge executor instead of raw fill_size at hardcoded 0.99
- Add balance refresh before resolution computation

### Config (`src/config.rs`)

- No new config fields needed for core logic
- Tie-breaking rule (prefer hedge) is hardcoded — correct default for CTF merge eligibility

---

## Future Consideration: Maximum Acceptable Loss Threshold

Currently the system resolves ALL shares via whichever option is cheaper (hedge or sell-back). There is no hard cap on how much loss per share is acceptable.

In extreme scenarios — thin books on BOTH sides — both options could be expensive:

```
Hedge ask jumped to $0.50  → hedge_cost   = $0.24/share
Filled token bid at $0.50  → sellback_cost = $0.24/share
```

The system would still execute (choosing the cheaper of two bad options), which may be the right behavior — resolving exposure immediately is almost always better than holding unhedged directional risk.

However, a future enhancement could add a `max_acceptable_loss_per_share` config field. If BOTH options exceed this threshold for remaining shares:

- Execute what can be resolved at acceptable prices
- Kill the market for the remainder
- Leave the position for manual intervention rather than locking in a large loss

**Not implemented in the current plan.** Rationale: an unresolved position carries unlimited directional risk, while a known bounded loss (even a large one) is safer. Revisit after observing real-world book conditions during hedge events.

---

## Verification Plan

### Unit Tests (`compute_hedge_resolution`)

- Perfect hedge: hedge_ask + fill = $1.00 → all shares hedge, zero cost
- Hedge cheaper: deep opposite book → all shares hedge
- Sellback cheaper: thin opposite book, wide spread → all shares sell back
- Mixed split: cheap hedge depth exhausted, then sellback cheaper → correct split
- Empty opposite book → all shares sell back
- Empty filled book → all shares hedge
- Tie → prefer hedge (CTF merge eligibility)
- Both books empty → zero resolution, remaining escalates to kill_market

### Unit Tests (Affordability Gate)

- Budget covers full hedge → no adjustment
- Budget covers partial → excess moves to sellback bucket
- Zero budget → all sellback

### Integration Tests

- Mock both books with known depth, trigger fill → verify correct split decision
- Verify orders placed at correct prices and sizes
- Verify affordability gate interacts correctly with book walk output

### Manual Validation

- Shadow run with synthetic fill at low balance
- Verify event log shows: book walk decision, computed prices, split, order placements
