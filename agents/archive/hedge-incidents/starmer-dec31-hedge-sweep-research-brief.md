# Research Brief For Web Preventing Sweep-Driven Exit Losses In A Fully Hedged Polymarket Reward Maker

This document is a self-contained research brief. Treat it as the authoritative summary of the system, incident, current logic, and constraints.

The objective is to produce a practical, evidence-based recommendation set for reducing losses from sweep-style fills in a Polymarket liquidity-reward market-making bot.

This is **not** a generic trading bot. It is a specific type of passive, reward-seeking, fully hedged binary market maker.

Recommendations should preserve earnings where possible. Broad controls such as tighter size caps, quotes farther from mid, or excluding thin markets require evidence that the risk reduction justifies the yield impact, along with more targeted alternatives where available.

## Mission

Research and recommend ways to reduce the chance and severity of this failure mode:

- the bot posts a passive bid to earn liquidity rewards
- a large market participant sweeps the book or otherwise creates an adverse fill
- by the time the bot resolves the fill, the executable exit prices are much worse than at entry
- the bot flattens correctly but realizes a painful immediate loss that wipes out a lot of earned yield

The deliverable should rank concrete mitigations and include tradeoffs, implementation ideas, and pre-deployment measurements.

## Short System Description

This bot is a **fully hedged Polymarket liquidity-reward market maker** for binary markets.

Its objective is:

- earn Polymarket liquidity rewards by resting passive orders
- stay neutral, not directional
- exit fills via the cheapest available flattening path

Its profit model is roughly:

`liquidity rewards - hedge/exit costs - fees`

It is **not** supposed to take directional bets on outcomes.

## Important Market / Product Context

This is a binary prediction market environment with YES and NO tokens.

For one binary question:

- YES settles to $1 if the event happens, $0 otherwise
- NO settles to $1 if the event does **not** happen, $0 otherwise
- holding one YES and one NO can be merged into $1.00 of collateral

So if the bot:

- buys NO at $0.37
- later buys YES at $0.66
- and merges the pair for $1.00

then the gross economic result is:

- total paid = $1.03
- merge value = $1.00
- gross loss = $0.03/share

If instead it sells the filled NO back at $0.31:

- entry = $0.37
- exit = $0.31
- gross loss = $0.06/share

That basic choice, hedge+merge vs sellback, is central to this incident.

## Core Strategy Summary

The bot continuously discovers and evaluates Polymarket binary markets and only trades markets that seem reward-positive **after accounting for expected exit economics**.

### Market selection summary

The bot:

1. polls for active binary markets
2. filters out markets that are too low quality for this strategy
3. builds candidate quotes
4. checks whether bids are hedgeable
5. estimates reward share
6. ranks viable markets and allocates budget

### Important current selection / quoting parameters

Use these as current system assumptions:

- only active binary markets
- minimum daily liquidity reward threshold: `$10`
- market must expire more than `24 hours` away
- skip outcomes whose mid-price is below `$0.20`
- quote refresh every `5 seconds`
- book hedge-depth check every `2 seconds`
- discovery cycle every `61 seconds`
- quote drift threshold: `30 bps`
- max slippage / hedgeability tolerance: `80 bps`
- bot uses one live order per leg, not layered queue tactics

### Current bid philosophy

The bot primarily rests passive **bids** to earn rewards.

Bids are placed relative to mid and the reward floor rather than simply crossing or sitting at random.

The rough design intent is:

- not too close to mid, because that hurts adverse-selection economics
- not too far from mid, because that hurts fill probability and reward share

So "just quote farther away" is not a satisfying answer unless you can show a smarter variant that preserves most yield.

### Current sizing philosophy

The bot does **not** just place a flat fixed size everywhere.

It sizes dynamically based on:

- expected reward share
- hedgeability
- budget
- market competitiveness

There are already budget controls in the system, including market-level exposure controls and budget-aware sizing. A broad size reduction is insufficient unless existing sizing logic can be shown to miss a structural risk dimension.

## Current Fill Resolution Logic

This part matters a lot.

When a passive bid fill occurs and the bot needs to flatten, it has two main exit choices:

1. **Hedge + merge**
   - buy the opposite token
   - end up holding one YES and one NO
   - merge for $1.00

2. **Sellback**
   - sell the filled token back into the bid book

### Current buy-side resolution planner

For buy-side resolution, the bot uses a planner that walks **both books** level by level.

For each marginal share, it compares:

- `hedge_cost = fill_price + opposite_ask_price - 1.00`
- `sellback_cost = fill_price - filled_side_bid_price`

Then it allocates shares to whichever path is cheaper at that depth.

### Important nuance

The actual planner **does walk full executable depth**.

However, some event telemetry and reason labeling are based only on **top-of-book** best bid / best ask snapshots. That means observability can sometimes describe a situation as a "tie" even though deeper full-size execution economics are more nuanced.

For research purposes, assume this:

- **planner**: full-depth, executable, book-walking logic
- **some monitoring labels**: top-of-book simplification

Do not misdiagnose the system as "only using top-of-book for the actual planner." That is not accurate.

### Current exact tie policy

On an exact cost tie, the current system prefers **sellback** rather than hedge.

The rationale is immediate capital reclamation and avoiding dependence on merge execution.

### Current hedge budget constraint

Even if hedging would be economically attractive, hedge allocation is capped by the bot's available hedge budget at resolution time.

This matters a lot in the incident below.

## Current Order / Book / Queue Behavior

The bot currently has:

- full order book snapshots
- price-level deltas
- periodic refresh and replacement logic

But it does **not** appear to maintain a true queue-position model such as:

- exact ahead-of-us size when we joined
- exact queue rank
- explicit same-price joiner state
- a formal policy like "if someone joins at our price, immediately cancel and requeue later"

So if you recommend queue-aware behavior, you should treat it as a **new feature**, not something already implemented.

## Current Refresh / Maintenance Behavior

The bot already does several things that matter for risk, but they are periodic rather than omniscient:

- refreshes quotes every `5 seconds`
- cancel-replaces on price drift
- also cancel-replaces on large size drift
- checks hedge depth every `2 seconds`
- can scale down or cancel bids if opposite-book hedge depth deteriorates

This means the bot already has some stale-quote protection, but it does **not** guarantee protection against a single large, fast sweep that occurs between maintenance checks.

## Exact Incident To Analyze

This is the incident under analysis.

### Market

- Public market question: `Starmer out by December 31, 2026?`
- Internal market identifier used by the bot: `condition_id=<redacted-id>`

The internal identifier is included only for specificity. It is not important to the research except as a way to uniquely refer to the market.

### Operator description of the incident

The operator's description was roughly:

- the bot probably did not do anything "wrong" in the narrow sense
- a whale or very large aggressor appears to have swept a large chunk of the book very quickly
- the bot got caught in that sweep
- the bot likely sold back instead of hedging on what looked like a tie
- the sellback happened at a very poor price
- the realized loss wiped out a meaningful chunk of recent yield

### Observed incident facts from log analysis

These are the relevant observed facts.

1. The bot had a resting passive `NO_BID` at `0.37`.

2. The actual live resting order size on that leg was `232` shares at `0.37`.

3. An unattributed large trade hit the same asset:
   - side: `SELL`
   - token: `NO`
   - trade price: `0.36`
   - trade size: about `11,763.94` shares
   - it was not confidently anchored to the bot immediately
   - it was deferred to reconciliation

4. Reconciliation later recovered the bot's actual fill as:
   - `232 shares`
   - at `0.37`
   - on the `NO_BID`

5. At resolution time, the bot's decision telemetry showed:
   - mode: buy-side resolution
   - reason label: `tie_prefers_sellback`
   - available hedge budget: `0`
   - planned hedge shares: `0`
   - planned sellback shares: `232`
   - planned sellback price: `0.31`

6. The sellback completed successfully at `0.31`.

7. So the realized gross sellback loss was:
   - `(0.37 - 0.31) * 232 = $13.92`

8. At the same decision moment, top-of-book telemetry showed:
   - best bid on the filled side: `0.34` with size `15`
   - best ask on the opposite side: `0.66` with size `15`

9. At top-of-book only:
   - hedge cost looked like `0.37 + 0.66 - 1.00 = 0.03/share`
   - sellback cost looked like `0.37 - 0.34 = 0.03/share`
   - so the top-of-book label looked like a tie

10. But the actual executable full-size sellback for `232` shares ended up at `0.31`, which is materially worse than the top bid of `0.34`.

11. The actual planner uses full-depth book walking, so the bigger structural problem appears to be:
   - hedge budget at resolution time had fallen to zero
   - therefore the planner could not allocate any shares to the hedge path, even if deeper hedge would have become cheaper than deeper sellback

### Economic significance

From the bot's own market-local telemetry around the time of the incident:

- expected reward for that market at that time was only about `$0.393/day`

So the single realized `$13.92` loss is economically huge relative to that market's own expected daily contribution.

The human operator also described it as wiping out about `48 hours` of broader strategy yield.

## What Seems Most Structurally Important

Pressure-test these hypotheses with current research rather than accepting them at face value.

### Hypothesis 1

The main issue is **not** that the planner failed to walk the books.

The main issue may be that by the time resolution happened, available hedge budget was `0`, which forced all shares into sellback.

### Hypothesis 2

Current sizing may be insufficiently aware of **sweep-loss severity** on forced or near-forced exit paths.

The current system already reasons about hedgeability and slippage, but that may not be enough to protect against sudden large adverse fills followed by unfavorable sellback economics.

### Hypothesis 3

Queue-aware ideas like:

- if someone joins our bid, cancel and re-place so we sit later in queue

might help a little, but probably do **not** solve the worst-case whale sweep by themselves.

### Hypothesis 4

The strategy may benefit more from:

- dynamic emergency hedge budget availability
- better stressed-size-aware admission / sizing
- temporary stress withdrawal / shrink logic

than from simplistic global risk caps.

## Insufficient Standalone Recommendations

These controls may be appropriate, but they are insufficient as primary recommendations without strong evidence and more targeted variants.

1. **Place bids farther from mid.**
   - The whole strategy already depends on a reward-vs-fill tradeoff.
   - Always moving farther from mid hurts fill probability and reward share.

2. **Apply tighter market-wide size caps.**
   - The bot already has market sizing and budget controls.
   - Prefer controls targeted to this incident class over a blunt reduction applied everywhere.

3. **Exclude all thin markets.**
   - That may reduce risk but can also gut opportunity.
   - If you suggest filtering, it needs to be more nuanced than simply avoiding thin books entirely.

4. **Hold a large permanent idle cash reserve.**
   - Some reserve may make sense, but leaving a lot of capital idle all the time can destroy yield.

5. **Use a hard loss or kill cap as the main control.**
   - Prefer controls that prevent or reduce the incident, not only truncate losses after the fact.

## Research Questions

Use current web research to answer these.

1. What are the best-known practical ways passive FIFO-style market makers reduce adverse-selection losses from thin-book sweeps **without** giving up too much queue value or spread capture?

2. What mechanisms are used by serious market makers to remain mostly passive in normal conditions but become more defensive in fragile or toxic microstructure regimes?

3. Are there robust methods for keeping **emergency hedge capacity** available on demand without leaving too much capital permanently idle?

4. Are there market-making approaches that dynamically reclaim or rotate capital when a fill happens, so the bot can still choose hedge over sellback when hedge becomes relatively attractive deeper in the books?

5. Are there known heuristics or models for sizing passive quotes based on:
   - executable opposite-book depth
   - executable sellback depth
   - fragility of top-of-book
   - expected toxicity / sweep probability

6. How do practitioners estimate when a passive fill is likely to be "toxic" in thin event markets or similar order books?

7. Is there good evidence that queue-aware requeue tactics help in thin FIFO books, and under what conditions do they help versus hurt?

8. What does the literature or practitioner guidance say about choosing between:
   - hedging
   - unwinding back into the same side
   - mixed or staged exits
   after an adverse passive fill?

9. Are there known methods to compute or approximate a "worst reasonable flatten cost" for a resting passive order at admission time?

10. If you were designing safeguards specifically for Polymarket or similar prediction CLOBs, what would you ship first?

## Areas To Research

I want broad but relevant research across these buckets:

### Exchange-agnostic microstructure

Research things like:

- adverse selection for passive makers
- toxic flow detection
- sweep risk
- queue dynamics in FIFO books
- optimal passive order sizing under fragile depth
- short-horizon microstructure fragility indicators
- dynamic spread / skew / quote withdrawal logic

### Prediction-market-specific or thin-binary-book behavior

Research things like:

- market making in binary options / prediction markets
- paired-token hedge mechanics
- behavior of thin event books
- idiosyncratic risks of reward-driven liquidity provision

### Polymarket-specific

Research anything you can find on:

- Polymarket CLOB mechanics
- reward formulas and incentives
- binary token pairing / merge mechanics
- any public discussion from traders / makers about adverse selection, queueing, or exit behavior on Polymarket

If a claim is Polymarket-specific, cite it carefully.

## Important Design Constraints For Recommendations

Please optimize for recommendations that fit these constraints:

1. **Yield preservation matters.**
   - The strategy exists to monetize reward programs.
   - Recommendations that materially reduce fill rate or reward share require strong justification.

2. **Main hot path overhead should remain very low.**
   - Rare-path logic is more acceptable than constant heavy computation.
   - Book-local, event-local, or resolution-local logic is more acceptable than global continuous modeling if the latter is expensive.

3. **The bot should remain largely delta-neutral.**
   - Directional holding is a last resort, not the goal.

4. **Incremental rollout is preferred.**
   - Good answers distinguish low-risk immediate heuristics from more sophisticated medium-term ideas.

5. **The best answer may be a combination of multiple mitigations.**
   - For example:
     - a better emergency hedge-budget rule
     - a better stressed-size admission rule
     - a modest toxicity-triggered quote defense

## What I Want In Your Final Answer

Please produce:

### 1. Executive Summary

A short, blunt summary of:

- what you think the real problem is
- what you think the most promising fixes are
- what is probably a distraction

### 2. Ranked Recommendations

Give a ranked list of mitigation ideas. For each one include:

- what it is
- why it specifically helps this incident class
- likely yield drag
- implementation complexity
- data / telemetry needed
- failure modes
- whether it fits the constraints above

### 3. Best Immediate Changes

Your top 3 lowest-regret changes that could plausibly be shipped first.

### 4. Higher-Upside But Riskier Ideas

Ideas that may be powerful but need more modeling, experimentation, or data.

### 5. What To Measure Before Shipping

Tell me what I should measure offline or in shadow mode before rolling out any fix, such as:

- fill rate
- reward share
- flatten cost distribution
- hedge budget utilization
- forced sellback frequency
- realized loss per adverse fill
- time-to-flat
- idle capital side effects

### 6. Suggested Replay / Backtest Methodology

I want a practical methodology for validating changes offline using historical event logs or book replays. Include:

- what data would be needed
- what counterfactuals to simulate
- what metrics matter most
- what would constitute a real improvement versus a fake one

### 7. Explicit Tradeoff Callouts

Please explicitly state if any recommendation mostly works by sacrificing one of these:

- reward yield
- capital efficiency
- market coverage
- latency

## Citation Rules

1. Use **current web research**, not memory only.
2. Cite sources with URLs.
3. Separate clearly:
   - exchange-agnostic research
   - prediction-market-specific research
   - Polymarket-specific research
4. If you infer something from multiple sources rather than reading it directly in one source, label it as an **inference**.
5. Do **not** invent Polymarket mechanics. If uncertain, say so.

## One Final Instruction

Please do not default to the easiest answer.

I already know that blunt risk reduction usually works if you are willing to ruin the economics enough. What I want is careful research into the best ways to keep this strategy economically alive while making incidents like this materially less damaging.

The bar for a good answer is:

- grounded in market-microstructure reality
- specific to this failure mode
- aware of the current system design
- honest about tradeoffs
- still trying to preserve yield rather than kill the strategy
