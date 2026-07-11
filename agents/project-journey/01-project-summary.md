# SpreadEater Project Summary

## Human Summary

SpreadEater was built as a Rust-based Polymarket liquidity-rewards bot.

The core idea was to earn maker rewards without taking directional bets.

The bot discovered reward-eligible binary markets, placed passive orders, hedged fills on the opposite outcome, and attempted to return paired YES/NO inventory to collateral through CTF merge.

The intended economic equation was: liquidity rewards minus hedge costs, sellback losses, fees, and operational failures.

The project began as a shadow-mode research system and became a live trading system with real order placement, WebSocket fill detection, hedge execution, position reconciliation, monitoring, replay harnesses, live probes, watchdogs, and CLOB V2 migration work.

The main strategy stayed consistent: do not predict market outcomes; earn the reward program while staying neutral.

The main engineering challenge was that "staying neutral" was harder in production than it looked in design.

Real exchange behavior introduced missed fills, stale position truth, delayed order status, book staleness, API hangs, merge transport failures, and emergency flatten timing problems.

The project became a long sequence of safety hardening loops after live-money incidents exposed gaps between documented intent and runtime behavior.

By the later phase, the system was substantially more robust, but also much more complex.

The strongest strategic lesson was that a safe delta-neutral rewards bot and the highest-yield reward-farming playbooks were not the same thing.

The competitor analysis suggested many top earners accepted one-sided inventory, low-priced longshot exposure, or event-farming concentration that SpreadEater intentionally avoided.

No code or strategy behavior changed in this documentation increment.

STRATEGY.md remains aligned with this summary at the conceptual level.

Confidence in this summary: 0.88.

---

## AI Detail

### Source Basis

This project summary is based on:

| Source | Why it matters |
|---|---|
| `STRATEGY.md` | Defines the current intended Standard strategy. |
| `agents/summary.md` | Captures recent implementation state and validated work. |
| `agents/changelog.md` | Provides the chronological engineering journey. |
| `agents/archive/prd.md` | Shows the original product intent and staged rollout. |
| `agents/archive/handoff.md` | Shows the early operations guide after live mode was working. |
| `agents/archive/hedge-incidents/...` | Documents live-money failures and hardening motivations. |
| `private competitor-analysis source` | Explains reward leaderboard patterns and strategy mismatch. |
| Aggregate-exposure design archive | Documents the later scaling thesis and go/no-go framework. |

### Original Product Intent

The original PRD framed SpreadEater as a local-first, high-performance Rust bot for Polymarket binary markets.

The first deliverable was shadow mode:

| Shadow-mode goal | Operational meaning |
|---|---|
| Discover reward-eligible binary markets | Find markets where Polymarket was paying makers. |
| Maintain live books | Read current YES/NO depth through REST and WebSocket. |
| Compute four candidate quotes | YES bid, YES ask, NO bid, NO ask. |
| Check immediate hedgeability | Only quote if the opposite outcome had enough depth. |
| Estimate reward viability | Compare expected reward against hedge and execution costs. |
| Report decisions | Explain why a market would or would not be traded. |

The full product then added live orders, user-stream fills, account state, hedges, inventory management, and kill switches.

### Mature Strategy

The mature strategy in `STRATEGY.md` is:

| Strategy element | Mature behavior |
|---|---|
| Market universe | Active binary Polymarket markets with daily rewards at least $10, expiry greater than 24 hours, and valid YES/NO token identity. |
| Entry filter | Skip cheap outcomes below the minimum mid-price floor, currently documented as $0.20. |
| Quote placement | Place passive reward-seeking bids at a configured depth from mid toward the reward floor. |
| Sizing | Use score-share targeting, competitor score estimates, hedge-aware budget, and book depth. |
| Fill handling | A dedicated async fill handler processes user-stream fills outside discovery and refresh work. |
| Hedge resolution | Choose per share between buying the opposite token and selling back the filled token based on cost. |
| Pair exit | Merge complete YES/NO pairs through CTF when configured and available. |
| Fallback exit | Use inventory asks or sellback when merge is unavailable or not appropriate. |
| Risk controls | Book staleness gates, hedge timeout, per-market mutex, reconciliation, kill/flatten paths, and watchdogs. |

### Engineering Arc

The project moved through several broad phases:

| Phase | Description |
|---|---|
| Design and PRD | Defined the non-directional, reward-first strategy and staged rollout. |
| Shadow mode | Built market discovery, book ingestion, quote math, hedgeability, reward estimation, and reports. |
| Live foundation | Added auth, order placement, order tracking, user WebSocket handling, positions, and live dry-run support. |
| First live bot | Placed real passive bids and hedged fills. |
| Incident hardening | Fixed fill misses, stale accounting, unsafe hedge requests, reconciliation retries, and weak flattening. |
| Observability | Added event logs, monitor stack, replay harnesses, live probes, detailed decision archives, and watchdogs. |
| Merge hardening | Moved from RPC-based merge assumptions to preflighted SAFE relayer merge with live proof. |
| Scaling design | Designed an aggregate-exposure proposal as a possible higher-reward strategy variant, gated by thesis validation. |
| Strategic stop | The risk/reward profile no longer clearly justified continued expansion. |

### What Made The Project Difficult

The hard part was not quote math by itself.

The hard part was making a live exchange bot keep the strategy invariant under imperfect information.

The invariant was:

| Invariant | Real-world challenge |
|---|---|
| Every fill must be hedged quickly. | User-stream events can be missed, delayed, malformed, or unmatched. |
| Position truth must be reliable. | Polymarket position data can lag execution truth. |
| A successful hedge must be recognized. | Order status and associated trades can materialize after delay. |
| Kill switches must actually remove risk. | Cancel and flatten paths can be slow, ambiguous, or sequential. |
| Reward estimates must guide allocation. | Competitor reaction is not observable from historical replay. |
| Merge should lock in pairs. | CTF merge depends on external relayer and chain-facing transport. |

### Strategic Lesson

The project proved that a hedged reward bot can be engineered, but it also showed that engineering correctness is only one side of the problem.

The economic edge depended on Polymarket's reward program, competitor behavior, fill rates, sellback losses, merge reliability, and the opportunity cost of excluding riskier but richer market zones.

That combination made the final risk/reward decision more strategic than technical.

Confidence that this captures the project journey accurately: 0.88.
