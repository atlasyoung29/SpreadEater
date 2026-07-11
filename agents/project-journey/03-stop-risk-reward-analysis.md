# SpreadEater Stop And Risk Reward Analysis

## Human Summary

I did not find a single final document that explicitly says "we stopped the project because X."

This analysis is therefore an evidence-based reconstruction from the strategy docs, incident reports, competitor analysis, and aggregate-exposure go/no-go documents.

The most likely stop reason is that the risk/reward equation stopped being attractive.

SpreadEater became safer over time, but the safe version had limited upside.

The highest reward opportunities appeared to live in market zones SpreadEater intentionally avoided: low-priced longshots, sports/event farming, wide multi-leg exposure, and sometimes one-sided inventory holding.

The project could chase those rewards only by weakening its original non-directional safety posture or by building an aggregate-exposure expansion.

The aggregate-exposure proposal assumed that rewards could scale faster than sellback losses when the bot rests in more markets.

The proposal documents correctly treated that assumption as unverified and required replay plus live micro-probes before committing.

The risk side was concrete: correlated fills, insufficient hedge cash, sellback misses, cascade market kills, merge backlog, passive global halt timing, and slow emergency flattening.

The reward side was uncertain: score proxy estimates, competitor reaction, reward-program changes, and market concentration all made projected capture hard to trust.

So the project appears to have reached a rational stop point: the engineering could continue, but the expected economic edge was not clearly worth the operational and capital risk.

Plain English: the safest bot might not make enough, and the version that might make enough was no longer the same low-risk bot.

Confidence in this stop explanation: 0.86.

---

## AI Detail

### Important Caveat

No single final stop memo was found in the reviewed files.

The conclusion here is reconstructed from:

| Evidence | Interpretation |
|---|---|
| `STRATEGY.md` | Standard mode remained explicitly delta-neutral and reward-program-dependent. |
| `private competitor-analysis source` | Top reward earners used patterns SpreadEater did not fully participate in. |
| `aggregate-exposure strategy document` | Scaling required accepting aggregate exposure beyond available balance. |
| `aggregate-exposure validation document` | Scaling economics were explicitly unverified and needed go/no-go tests. |
| `aggregate-exposure prerequisites document` | Several correctness, safety, and scaling gates were required before live expansion. |
| Hedge incident reports | Live production risk was real, not theoretical. |
| `agents/changelog.md` | The project had already accumulated heavy safety and integration complexity. |

Confidence in the evidence base: 0.88.

### Reward Side Of The Equation

The reward thesis was:

| Reward source | What it required |
|---|---|
| Polymarket liquidity rewards | Resting qualifying orders near enough to mid and large enough to score. |
| Multiple markets | Capital deployed across more reward pools. |
| CTF merge | Complete YES/NO pairs returned to collateral at $1 per pair. |
| Low hedge costs | Fill plus hedge or sellback cost lower than reward income. |

The upside was real but uncertain.

Key uncertainty:

| Uncertainty | Why it mattered |
|---|---|
| Competitor score | The bot estimated competitor score from visible book depth, but did not know true aggregate reaction. |
| Competitor response | Historical replay sees markets without our orders; it cannot prove how competitors react when we enter. |
| Reward program changes | The strategy depends on Polymarket continuing the same reward rules and pools. |
| Market selection | Highest-rate markets may be outside the Standard filter set. |
| Fill mix | More fills can mean more sellbacks, not more CTF merges. |

Confidence that reward upside was uncertain rather than guaranteed: 0.91.

### Competitor Analysis Signal

The competitor analysis found that the top rewards leaderboard did not map cleanly to SpreadEater's strategy.

Important patterns:

| Competitor pattern | SpreadEater mismatch |
|---|---|
| Low-priced longshot farming at 0.02 to 0.10 | Standard `min_outcome_price = 0.20` excludes this zone. |
| Sports and tournament event farming | SpreadEater's greedy allocation did not naturally spread across every leg of a tournament. |
| One-sided geopolitics inventory holding | SpreadEater wanted immediate neutrality rather than holding exposure. |
| Dual-mode maker plus directional accounts | SpreadEater avoided directional speculation. |
| Large persistent maker size | Smaller capital and strict hedging reduced reward share. |

This does not mean SpreadEater was wrong.

It means SpreadEater was pursuing a safer edge than the one many top earners appeared to exploit.

Confidence this was a strategic mismatch: 0.87.

### Risk Side Of The Equation

The operational risks were concrete and repeatedly observed.

| Risk | Evidence or source | Operational meaning |
|---|---|---|
| Missed fills | March 24 and March 27 incident docs | Bot can become one-sided before the hot path sees the fill. |
| Unsafe hedge request shape | March 24 incident | Bot can fail before reaching the true book economics. |
| Stale position truth | Shai and Cuba analyses | Bot can believe it has exposure it already resolved, or miss proof of a hedge. |
| Sellback miss | Sellback recompute work | Getting flat through FOK sellback can fail when books move. |
| API hangs | `retired hedge-timeout analysis` | A hedge task can hold a mutex longer than risk design expects. |
| CTF merge dependency | Merge probe and relayer work | Pair exit relies on external relayer and chain-facing flows. |
| Emergency flatten delay | Aggregate-exposure documents | Global halt and kill flatten may leave exposure during the worst moment. |
| Correlated shocks | aggregate-exposure risk profile | Many markets can fill together exactly when hedge depth disappears. |

Confidence that these risks were material: 0.92.

### Standard Mode Risk Reward

Standard mode became safer over time.

It stayed within balance-aware constraints and aimed to hedge every fill.

But safety created opportunity limits:

| Safety choice | Reward tradeoff |
|---|---|
| Minimum outcome price floor | Avoided extreme tails but skipped rich longshot reward zones. |
| Hedge-aware budget | Prevented overcommitting but limited market count. |
| Immediate hedge or sellback | Reduced directional risk but removed upside from positions that moved favorably. |
| Binary-only focus | Avoided complex market structures but skipped some reward-farming patterns. |
| Strict risk halts | Protected capital but reduced uptime during edge cases. |

Likely conclusion:

Standard mode was the cleaner engineering product, but possibly too low-yield relative to maintenance and tail risk.

Confidence: 0.82.

### Aggregate-Exposure Risk Reward

The aggregate-exposure proposal was the proposed scaling path.

Aggregate-exposure thesis:

| Claim | Meaning |
|---|---|
| Rewards scale with market count | More resting bids across more reward pools could earn more. |
| Sellback losses are tolerable | If concurrent fills exceed hedge cash, selling back still leaves net positive rewards. |
| Aggregate exposure can exceed balance under caps | The bot can rest more notional than it can hedge all at once. |

Aggregate-exposure risk:

| Failure chain | Operational result |
|---|---|
| Correlated news shock hits multiple markets | Many passive bids fill within seconds. |
| Opposite-book depth disappears | Hedges become expensive or unavailable. |
| Hedge cash is insufficient | Planner routes excess to sellback. |
| Sellback FOK misses | Residual exposure remains. |
| Multiple markets fail together | Cascade kill, reduced uptime, manual intervention risk. |
| Merge backlog grows | Collateral return slows, reducing hedge capacity. |

The Aggregate-exposure documents required:

| Gate | Purpose |
|---|---|
| Phase 0a replay | Prove best case is worth considering. |
| Phase 0b correctness gates | Fix known constraint and timeout gaps before live probe. |
| Phase 0c live micro-probe | Measure competitor reaction and real sellback economics. |
| Phase 1 safety gates | Improve global halt, flatten speed, mutex behavior. |
| Phase 2 constrained live | Only after empirical positive economics. |
| Phase 3 scaling | Only after Phase 2 success. |

This was the right design discipline.

It also shows why stopping was rational: the aggregate-exposure proposal required more proof and more engineering before the upside was known.

Confidence: 0.88.

### Why Continuing Was Not Obviously Worth It

The decision can be framed as a portfolio tradeoff:

| Continue building | Stop or pause |
|---|---|
| Possible larger reward capture. | Avoids more live-money edge cases. |
| Better monitoring and hardening. | Avoids compounding code complexity. |
| The aggregate-exposure proposal could discover a scalable edge. | Aggregate-exposure thesis was unverified. |
| More probes could clarify competitor response. | Competitor patterns suggested safer strategy may be structurally lower-yield. |
| More safety gates could reduce tail risk. | Each safety gate added maintenance and integration burden. |

The project appears to have stopped because the marginal next phase was no longer "ship one fix."

It was:

1. Validate whether the economics still worked.
2. Add correctness gates.
3. Add safety gates.
4. Run live probes.
5. Potentially redesign market selection.
6. Potentially accept more exposure than the original thesis allowed.

That is a much larger commitment than continuing a known-profitable simple bot.

Confidence: 0.86.

### Final Stop Statement

SpreadEater likely stopped because the remaining path split in two:

| Path | Problem |
|---|---|
| Keep Standard safe | Lower expected reward and limited market universe. |
| Chase higher rewards | More exposure, more correlated fills, more sellbacks, more operational complexity, and a less purely neutral strategy. |

The project had already proven that production-safe hedged market making is possible but expensive to maintain.

It had not proven that the net reward was large enough to justify continuing into more complex scaling modes.

Final confidence score: 0.86.
