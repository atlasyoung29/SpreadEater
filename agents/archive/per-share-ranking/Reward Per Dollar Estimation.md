---
tags:
  - spreadeater
  - strategy
  - reward-estimation
parent: "[[SpreadEater/Potential Tweaks]]"
proposed: 2026-03-22
---

# Estimating Reward Per Dollar Committed (R_dollar) — Practical Framework

## Objective

Estimate how many **cents per dollar of committed capital** you earn from liquidity rewards.

This normalizes for price — a $0.80 share ties up 4x more capital than a $0.20 share, so raw "per share" comparisons are misleading.

This is the key variable used in the decision rule:

```
R_dollar_effective > hedge_cost_per_dollar
```

---

## Step 1 — Identify Reward Pool

Example:

Reward Pool = $10,000 per day

---

## Step 2 — Estimate Your Score Share

Approximate your share of the total market score using:

- visible book depth (competitor orders)
- conservative multiplier on competitor estimate (default 1.5x)
- the Polymarket scoring formula: `S(v, s) = ((v - s) / v)^2 * size`

The score function uses **shares** as the size unit — this is correct because Polymarket scores in shares. The per-dollar conversion happens at the reward step.

Example:

score_share ≈ 5% of total pool

---

## Step 3 — Compute Capital Committed

Capital committed = sum of `price * size` for all resting bid orders.

Example:

100 shares @ $0.50 = $50
100 shares @ $0.80 = $80
Total capital = $130

Note: In binary markets with hedging, total cost per hedged share ≈ $1 (bid_price + hedge_price ≈ 1.00), but capital **committed** is only the bid side.

---

## Step 4 — Compute Reward Per Dollar

```
R_dollar = (score_share * daily_reward_pool) / capital_committed
```

Example:

R_dollar = (0.05 * 10,000) / 130 = $500 / $130 = $3.85 per dollar per day

Or equivalently: 385 cents per dollar per day.

---

## Step 5 — Compute Hedge Cost Per Dollar

```
hedge_cost_per_dollar = total_hedge_cost / capital_committed
```

Where `total_hedge_cost = sum((entry_price + hedge_price - 1.00) * size)` for each bid leg.

Example:

If entry at $0.50, hedge at $0.52: cost per share = 0.50 + 0.52 - 1.00 = $0.02
For 100 shares: $2.00 hedge cost
hedge_cost_per_dollar = $2.00 / $50 = $0.04 per dollar

---

## Step 6 — Apply Uncertainty Discount (Critical)

Reward estimates are uncertain — score share is approximate, competitor behavior changes.

```
R_dollar_effective = R_dollar * discount_factor
```

Default discount: **0.70** (configurable, range 0.5–0.8).

---

## Step 7 — Decision Rule

A market is viable if:

```
R_dollar_effective > hedge_cost_per_dollar
```

With a minimum threshold: `return_per_dollar >= min_return_pct` (default 0.25%).

This replaces the old `P_Y + P_N - R < 100` rule.

---

## Shortcut (Used in Practice)

```
R_dollar ≈ (score_share%) * reward_pool / capital_committed
```

This is implemented directly in `compute_viability()` as:
```
estimated_reward = daily_reward * score_share * discount_factor
return_pct = (estimated_reward - estimated_hedge_cost) / capital
```

---

## Final Intuition

Always think:

"How many cents per dollar of committed capital am I being paid to provide liquidity?"

Then compare directly to:

- hedge cost per dollar
- minimum return threshold

---

## TLDR

- Estimate score share (from book depth + competition multiplier)
- Compute capital committed (price * size for all bids)
- R_dollar = (score_share * reward_pool) / capital_committed
- Discount for uncertainty (default 0.70)
- Viable if R_dollar_effective > hedge_cost_per_dollar and return >= 0.25%
