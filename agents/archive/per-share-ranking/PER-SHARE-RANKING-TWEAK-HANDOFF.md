# Per-Share Ranking Tweak Handoff

## Executive summary

The current branch now ranks markets by `return_per_dollar`, where the denominator is posted order notional (`price * size`).

That is implemented here:
- [live_engine.rs](<repo-root>/src/runtime/live_engine.rs#L830)
- [viability.rs](<repo-root>/src/strategy/viability.rs#L50)

However, the bot's actual account budget is still consumed on a hedge-aware full-exposure basis, effectively `$1 per share`, not `price * size`.

That is implemented here:
- [order_manager.rs](<repo-root>/src/trading/order_manager.rs#L398)

Because of that mismatch, the current ranking can prefer cheap bids even when they do not maximize reward relative to total hedge-aware account capacity consumed.

If the intended optimization target is "best reward for the account capacity this bot actually uses," then a per-share / full-exposure denominator is the more aligned model.

## What was observed

In the current live run, the bot is sitting in two low-priced YES bids:
- `Callum Turner announced as next James Bond?`
- `Will Gavin Newsom win the 2028 Democratic presidential nomination?`

The status logs show roughly:
- posted order notional around `$79.92`
- but hedge-aware exposure consumed around `363` shares / dollars of account capacity
- total estimated daily reward around `$0.13`

So the bot is close to fully allocated from the account's perspective, while only a relatively small amount of posted notional is actually resting on the book.

## Diagnosis

### 1. Current ranking is based on posted dollars, not hedge-aware capacity

Current viability math:
- numerator: expected edge / expected reward logic
- denominator: `sum(price * size)` for approved bid legs

See:
- [viability.rs](<repo-root>/src/strategy/viability.rs#L50)

### 2. Actual capital usage is still per-share / full exposure

Current budget math:
- remaining budget = gross balance - committed exposure - reserve
- committed exposure for BUY orders is effectively `sum(size)`

See:
- [order_manager.rs](<repo-root>/src/trading/order_manager.rs#L398)

The code comment is explicit:
- in binary markets, buy share total cost is `price + hedge_cost = 1`
- so account commitment is treated as `$1/share`

### 3. Exchange reward docs appear share-based

Polymarket's liquidity rewards docs define:
- `BidSize` / `AskSize` as share-denominated quantity
- reward scoring terms multiply by size, not by `price * size`

Docs:
- [Polymarket Liquidity Rewards](https://docs.polymarket.com/market-makers/liquidity-rewards)

Polymarket's pricing docs also describe a matched YES and NO pair as becoming `1 YES` and `1 NO` share.

Docs:
- [How Are Prices Calculated?](https://help.polymarket.com/en/articles/13364488-how-are-prices-calculated)

This supports the view that reward opportunity is fundamentally share-based, with price affecting quote quality and hedge economics, but not directly converting rewards into a posted-notional basis.

## Proposed fix explained simply

If the intended objective is:

"Use capital where it produces the best reward relative to the account capacity this bot actually consumes,"

then the ranking denominator should be brought back into alignment with the budget model:

- use per-share / full-exposure committed capital
- not posted notional `price * size`

In plain terms:
- today, a cheap `YES @ 0.20` can look much more attractive than an expensive `NO @ 0.80` because it uses fewer posted dollars
- but the bot still spends about the same total hedge-aware account capacity on both
- so the ranking should probably compare them on that same full-capacity basis

This is a narrow tweak:
- it does not require changing exchange interaction
- it does not require changing the reward numerator
- it primarily changes the denominator used for market ranking / viability

## Example scenario

Assume two candidate quotes are equally good in reward terms:

- Market A: `YES @ $0.20`, `200` shares, expected reward `$1.00/day`
- Market B: `NO @ $0.80`, `200` shares, expected reward `$1.00/day`

### Under the current posted-dollar metric

- A committed dollars = `0.20 * 200 = $40`
- B committed dollars = `0.80 * 200 = $160`

So:
- A return = `$1.00 / $40 = 2.5%`
- B return = `$1.00 / $160 = 0.625%`

Current ranking strongly prefers A.

### Under a per-share / hedge-aware metric

Both positions consume about the same total account capacity:
- A uses about `$200`
- B uses about `$200`

So:
- A return = `$1.00 / $200 = 0.5%`
- B return = `$1.00 / $200 = 0.5%`

Under that model, they tie.

This is the core ranking difference:
- posted-dollar ranking systematically favors cheap bids
- per-share / hedge-aware ranking aligns with how the bot actually runs out of capital

## Confidence by point

- High confidence: current ranking denominator is posted notional (`price * size`).
- High confidence: current budget consumption is still effectively `$1/share` hedge-aware exposure.
- High confidence: these two models can produce different market rankings.
- Medium-high confidence: Polymarket reward mechanics are better approximated by a share-based denominator than a posted-notional denominator.
- Medium confidence: changing the denominator back to per-share is the right strategy choice for this bot.

The last point is intentionally left as a strategy decision for review, not a coding conclusion.

## Open question for strategy decision

The key decision is:

What should "capital efficiency" mean for this bot?

Two reasonable interpretations:
- reward per posted resting dollar
- reward per total hedge-aware account capacity consumed

Given the current budget model and exchange reward docs, the second interpretation appears more internally consistent, but that choice should be made explicitly at the strategy level.
