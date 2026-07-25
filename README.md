# SpreadEater

> A local-first Rust market-making bot for Polymarket that earns liquidity rewards while working to stay delta-neutral through post-fill hedging.

SpreadEater posts passive two-sided limit orders on binary prediction markets to earn Polymarket's liquidity rewards; when a fill is detected, it attempts to resolve the resulting exposure via an opposite-token hedge or sellback to minimize directional exposure. It is designed to be non-directional — profit comes from rewards, net of hedge costs and fees — though residual exposure can remain when a hedge misses or an on-chain merge fails.

**Profit = liquidity rewards − hedge costs − fees**

---

## Overview

Polymarket pays liquidity rewards to makers who post resting orders close to the mid-price on eligible markets. SpreadEater automates capturing those rewards while minimizing event-outcome risk:

- **Post passively** — resting limit orders on both sides (bids and reward asks) of eligible binary markets, priced to score well against Polymarket's reward function.
- **Hedge on fill** — when a fill is detected, SpreadEater attempts to resolve the resulting exposure through an opposite-token hedge or sellback, neutralizing the position as far as market conditions allow.
- **Merge to cash** — when both YES and NO tokens are held for the same market, *attempt* to merge the pair on-chain into USDC at par via the Conditional Token Framework (CTF) contract or the Neg Risk Adapter. Merging requires a configured private key and relayer; if those are unavailable or a merge fails, the position is exited via inventory asks instead.

The bot never pre-hedges: capital is only put at risk after a confirmed fill. It runs entirely on the operator's own machine against Polymarket's public CLOB, Gamma, and data APIs.

| | |
| --- | --- |
| **Type** | Automated market-making bot + monitor dashboard |
| **Purpose** | Earn Polymarket maker rewards without directional exposure |
| **Strategy** | Two-sided passive quotes with immediate hedge-on-fill |
| **Language** | Rust (bot + monitor API/TUI); TypeScript + React (monitor web UI) |

> **Status — research project, not a live earner.** SpreadEater was built and analyzed as a study of Polymarket rewards market-making. It works as engineering, but it was never shown to have a positive net edge after real costs — see [Why it doesn't work](#why-it-doesnt-work).

---

## Why Polymarket pays makers

Polymarket runs a **liquidity rewards program**: for each eligible market it sets aside a daily reward pool and splits it among makers according to how much tight, useful liquidity each one posts. That subsidy — not a spread captured from takers — is the reason SpreadEater exists.

Each resting order is scored by a spread-utility function of how close it sits to the midpoint, relative to the market's max reward spread:

```text
S(v, s) = ((v - s) / v)^2 * b
```

- `v` — the market's **max spread** from the midpoint, in cents, within which orders earn rewards (`max_incentive_spread`)
- `s` — the order's spread (distance) from the size-cutoff-adjusted midpoint
- `b` — an in-game size multiplier

Orders closer to the mid (smaller `s`) score higher; orders beyond `v` score zero. Per-side book scores combine into a per-sample maker score `Q_min`, with a two-sidedness rule: for mid-range markets (midpoint roughly 0.10–0.90) single-sided liquidity still scores but at a reduced rate (divided by a scaling factor `c`, currently 3.0), while near the 0/1 tails liquidity must be two-sided to score at all.

A maker's reward is a **normalized, time-weighted share** of that pool — not a single instantaneous ratio. Polymarket:

1. samples each maker's score `Q_min` **every minute**,
2. normalizes it against all makers in that sample — `Q_normal = Q_min / Σ Q_min`,
3. sums those across the epoch — `Q_epoch = Σ Q_normal` over the **10,080** one-minute samples in an epoch (~7 days),
4. normalizes once more across all makers — `Q_final = Q_epoch / Σ Q_epoch`,

and the payout is:

```text
reward = Q_final * market reward pool
```

Rewards are distributed to maker addresses **daily at midnight UTC**, subject to a documented minimum payout threshold (smaller accruals are not paid). It is an allocation from a shared pool, not a guaranteed return — a maker's share scales with the pool and shrinks as competing liquidity grows.

> Formula and terminology per Polymarket's [Liquidity Rewards documentation](https://docs.polymarket.com/market-makers/liquidity-rewards) (as of July 2026). Venue reward mechanics can change; treat the official docs as the source of truth.

---

## How it works

A concise summary of the strategy. See [`STRATEGY.md`](STRATEGY.md) for the complete, authoritative breakdown.

```mermaid
flowchart TD
    A["1. Discover eligible markets"] --> B["2. Evaluate: quote, hedgeability, reward score"]
    B --> C["3. Rank by reward-per-share and allocate budget"]
    C --> D["4. Quote: post passive two-sided orders"]
    D --> E["5. Hedge on fill: resolve exposure"]
    E -->|loop| A
    R["6. Risk controls and watchdog oversee order placement and hedging"] -.-> D
    R -.-> E
```

1. **Discovery** — On a recurring interval, poll Polymarket's discovery/Gamma/data APIs for active binary markets and filter to reward-eligible candidates (minimum daily reward, sufficient time to expiry, actively accepting orders, distinct YES/NO token IDs).
2. **Evaluation** — For each candidate market, skip cheap tail outcomes, compute a 4-leg candidate quote set (YES bid, YES ask, NO bid, NO ask), verify the opposite book has enough depth to hedge within slippage limits, and estimate the expected reward share using Polymarket's scoring formula.
3. **Ranking & budget** — Sort viable markets by estimated reward per share and allocate available budget top-down. A bid-rotation "frontier" pass on the discovery cycle can displace at most one weaker market per cycle, and only when the improvement clears a configurable threshold (prevents churn on tiny deltas).
4. **Quoting** — Place passive bids priced a configurable fraction of the way from mid toward the reward floor, sized via score-share targeting and clamped by hedgeable depth and available budget. Resting orders are refreshed on an interval and cancel-replaced when they drift beyond a basis-point threshold.
5. **Hedge on fill** — A dedicated async fill-handler task (never blocked by discovery/refresh work) detects fills over the user WebSocket stream and neutralizes exposure. A greedy per-share cost comparison routes each share of exposure to whichever exit is cheaper: **hedge** (buy the opposite token, then CTF-merge the pair to USDC) or **sellback** (sell the filled token back into its bid book).
6. **Risk controls** — Per-market hedge timeout kill switch, opposite-book depth checks, slippage/hedge-cost caps, a cash reserve held back from the trading budget, and a two-layer watchdog (in-process Rust + external Python sidecar) that can halt and flatten on WebSocket/API failure.

### Fill resolution

When a fill is detected, the exposure is routed to whichever exit is cheaper per share, with explicit fallbacks if an on-chain merge is unavailable or fails:

```mermaid
flowchart TD
    F["Fill detected on a resting bid"] --> G{"Cheaper per-share exit?"}
    G -->|hedge| H["Buy opposite outcome token"]
    G -->|sellback| S["Sell filled token back into its bid book"]
    H --> M{"Both YES and NO held and relayer configured?"}
    M -->|yes| MG["Attempt on-chain CTF merge to USDC"]
    M -->|no| IA["Fallback inventory ask placed - residual exposure may remain"]
    MG -->|merge ok| U["USDC realized at par per completed pair"]
    MG -->|merge fails| IA
    S --> V["Resolution attempted - verify exchange position"]
    IA --> V
    U --> V
```

---

## Market selection, ranking & liquidity guards

SpreadEater is deliberately conservative about *where* it quotes. Two ideas drive it: rank markets by reward efficiency, and never post liquidity it can't hedge. The exact thresholds live in [`STRATEGY.md`](STRATEGY.md) and [`CONFIG.md`](CONFIG.md); the design is below.

### Ranking

Each discovery cycle, candidate markets that pass the viability gate are sorted by **reward-per-share** — estimated reward divided by the shares the bot would have to commit — so capital flows to the markets that pay the most *per share of quoted size*, not simply the ones with the largest reward pools. Budget is then allocated top-down across that ranking.

To stop the book from thrashing, a conservative **frontier rotation** runs on the discovery cycle: it can displace at most one weaker market per cycle, only reclaims resting *bid* capital (never held inventory), and only fires when the incoming market's daily reward beats the outgoing one by a configurable margin — so the bot doesn't churn orders chasing sub-penny improvements that wouldn't cover costs.

### Liquidity guardrails — won't quote what it can't hedge

Because every fill has to be hedgeable, the bot refuses markets and orders it can't safely exit:

- **Eligibility filters** — only binary, actively-quoting markets with a real daily reward pool and enough time to expiry are even considered.
- **No cheap tails** — outcomes priced below a floor are skipped, avoiding the extreme-tail markets where a small adverse move is proportionally huge.
- **Hedgeability admission gate** — before a bid is placed, the bot walks the *opposite* book and clamps the order down to the size it can actually hedge there; if not even the minimum size is hedgeable, the bid is rejected outright.
- **Continuous depth checks** — while orders rest, opposite-book hedge depth is re-checked on a short interval: if it thins, the resting bid is scaled down proportionally; if hedge depth disappears, the bids are cancelled entirely (can't hedge → shouldn't be quoting).

---

## Why it doesn't work

SpreadEater implements the full market-neutral loop — quoting both sides, then attempting to resolve detected fills back toward flat. Two things undercut it. First, live incidents showed that fill detection, hedging, verification, and flattening can each fail. Second, even when the loop runs cleanly, it never demonstrated a **positive net edge after real costs** — and that second problem is structural.

```text
Net edge  =  rewards  -  hedge costs  -  sellback losses  -  fees  -  operational losses
```

Rewards are the only positive term, and they are small. Everything to the right is a leak. What follows is what the fill record shows about which leaks mattered. The figures come from Polymarket's public trade tape plus a read-only pass over the bot's own event archive; this is a post-mortem on a few hundred fills — directionally clear, and not a statistically significant result. One caveat on the raw counts: roughly half of the second funding wallet's fills were a live QA harness deliberately triggering the hedge path, so any raw fill-count statistic over that wallet is dominated by test activity rather than by the strategy.

### The maker legs worked; the minutes after them did not

On the legs where the bot did what it was built to do — rest a passive quote and get hit — it captured spread, and it captured it consistently. And then the price moved, and took it back.

*The life of one maker fill: the spread is earned in one step and handed back in the next.*

```mermaid
flowchart LR
    QUOTE["Resting quote<br/>at or just behind the touch"] -->|hit by a taker| EARN["EARNED at fill<br/>+328 bp spread<br/>positive on 95.3% of fills"]
    EARN --> DRIFT["FIRST 30 SECONDS<br/>about 70% of the 60 minute adverse move"]
    DRIFT --> BACK["GIVEN BACK by 60 min<br/>102% to 128% of the spread earned"]
    BACK --> NET["NET after one hour<br/>about -88 bp"]
```

Both wallets show this independently. This is not a latency defect. Faster repricing would not have recovered the money — orders that had rested longest captured *more* edge, not less. A resting quote is an option written to the rest of the market, and this is what exercising it costs. *Why* the price moved is not observable from this data: the tape shows fills, not identities or motives, and no claim is made about what any counterparty knew or intended. What is measurable is that price moved against the fill, quickly, on most fills. That is the cost of standing in a book, and it is not something engineering removes.

### Getting flat means crossing the book

The hedge leg is a taker order by construction — to get flat, the bot has to cross. Too often it crossed with an order larger than the book in front of it, and paid for the extra size on the way down.

*One hedge order eating through several price levels on its way to flat, what that cost, and the two fixable causes behind it.*

```mermaid
flowchart LR
    HEDGE["Hedge order<br/>a taker order by construction"] --> LV1["Level 1<br/>best offer"]
    LV1 --> LV2["Level 2"]
    LV2 --> LV3["Level 3<br/>and deeper"]
    LV3 --> COST["Cost of walking the book<br/>105.6 bp of taker notional<br/>54.6% of measured at-fill cost, on about 20% of fills"]
    FREQ["Crossed 2 or more levels on 38.1% of crossings<br/>vs a 13.8% base rate for a typical aggressor, about 2.8x"] -.-> LV2
    FIX1["Fixable: early sizing bug hedged 2 to 4.8x the needed size<br/>fixed, median ratio afterward exactly 1.00x"] -.-> HEDGE
    FIX2["Fixable: median hedge latency 240 s<br/>against a move about 70% complete at 30 s"] -.-> HEDGE
```

### The arithmetic underneath

Beneath both of the above sits a simpler problem, and it rests on no claim about what the market did: what a fill earns is smaller than what getting flat costs.

*The two magnitudes side by side — rewards in, one required crossing out.*

```mermaid
flowchart LR
    FILL["Every fill"] --> REW["IN: rewards earned<br/>about 0.2% to 0.4% of notional"]
    FILL --> CROSS["OUT: the one required crossing<br/>about 0.6% to 0.7% of face value"]
    REW --> VERDICT["OUT is the larger number<br/>and at least one crossing is required per fill"]
    CROSS --> VERDICT
```

> **Unit caveat, stated because the comparison is otherwise too tidy:** rewards are measured against traded notional while the crossing cost is measured against a share's face value, so this is an order-of-magnitude argument about which term is larger, not a precise ratio.

### The conclusion

A delta-neutral rewards bot is buildable, and this one was built and run. But *hedged* is not the same as *profitable*, and **getting flat is not the same as getting flat cheaply.**

The maker legs did their job. Post-fill drift took it back within the hour, on both wallets independently — a cost of standing in a book that a wider spread, better market selection or inventory-aware skewing might reduce, but that no amount of latency engineering touches. The bot's own hedge orders carried the majority of measurable execution cost, which *was* fixable. And underneath both, the arithmetic: the reward earned per unit of notional was smaller than the cost of the one crossing the design required to get flat.

**The record does not demonstrate positive lifetime net P&L**, and the strategy's own retrospective treats further scaling as unproven. The engineering verdict is separate and it stands — replayable test harnesses, a large unit-test suite, forensic-grade event logs good enough to reconstruct a resting order's lifecycle months later, a watchdog, and a real structural invariant.

---

## Architecture

SpreadEater is a Cargo workspace with three crates:

| Crate | Role |
| --- | --- |
| `spreadeater` (workspace root) | Main bot binary — CLI, discovery, strategy, trading/hedge engine, watchdog |
| `spreadeater-core` | Shared types consumed by both the bot and the monitor |
| `spreadeater-monitor` | Monitor dashboard binary — Axum HTTP/WebSocket API, terminal UI (ratatui), and the React web frontend |

### Bot module tree (`src/`)

| Module | Responsibility |
| --- | --- |
| `auth/` | API credentials, L2 HMAC-SHA256 request signing, EIP-712 order signing |
| `books/` | Order-book ingestion — REST bootstrap plus live WebSocket updates and caching |
| `config.rs` | Config loading and defaults |
| `discovery/` | Market discovery and reward-eligibility filtering |
| `models/` | Core domain types (market, order, quote, hedge, position) |
| `monitor/` | Event emission — the append-only JSONL producer and error logger |
| `persistence/` | Session and decision archives, startup retention pruning |
| `reporting/` | Shadow reports and CSV export |
| `runtime/` | Orchestrator, live engine, replay engine, run metadata |
| `strategy/` | Quote engine, hedgeability checks, viability gating, reward-score proxy |
| `trading/` | Trading client, order manager, hedge executor, risk controls, trade parsing |
| `watchdog/` | Watchdog manager, WebSocket health tracking, status-page poller, kill trigger |

---

## Documentation index

| Document | Contents |
| --- | --- |
| [`STRATEGY.md`](STRATEGY.md) | Complete strategy breakdown — core thesis, market selection, quote pricing, hedge execution, CTF merge, and risk controls. |
| [`CONFIG.md`](CONFIG.md) | Reference for every field in the JSON config file. |

---

## Status & caveats

- **Research project, not a live earner.** SpreadEater was built and analyzed as a study of market-neutral rewards farming on Polymarket. It was not shown to clear its own costs (see [Why it doesn't work](#why-it-doesnt-work)) and is not run as a production earner.
- **Polymarket-specific.** It targets Polymarket's CLOB V2 (EIP-712 order signing, match-time fees) and reward program; it is not a general-purpose exchange bot.
- **On the post-mortem numbers.** They come from public on-chain data reconstructed after the fact, not from the project's own accounting. Three limitations travel with them: the maker/taker split relies on a venue query parameter that Polymarket exposes but does not document in writing (corroborated four independent structural ways, but still an inference); **only fills are observable**, so orders placed, rested and cancelled leave no trace anywhere in the data; and the maker samples are small — 100 and 29 fills — so "no effect" means "no large effect," not "exactly zero."
- **No claim is made about counterparties.** Nothing in this data observes who traded against the bot, what they knew, or why. Only that price moved against the fill afterward.
- **No guarantees, not financial advice.** Reward, hedge, and merge behavior all depend on live venue conditions, and the strategy's own analysis concluded the edge was never demonstrated. Nothing here is a recommendation to trade.

---

## Acknowledgments

SpreadEater was built in collaboration with **[@gabrielsalazar777](https://github.com/gabrielsalazar777)**, who contributed to the strategy design and led the testing and validation infrastructure, observability and benchmarking, and the monitoring dashboard. Much of the design was worked out in conversation rather than in writing, so any division of labor is descriptive rather than exact.
