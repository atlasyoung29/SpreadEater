# SpreadEater Journey Visualization

## Human Summary

This document turns the SpreadEater journey into compact tables for quick review.

The project moved from thesis, to shadow mode, to live trading, to incident hardening, to merge validation, to scaling analysis, and finally to a risk/reward stop point.

The most important visual pattern is that each phase increased both capability and operational complexity.

Early work answered "can we find and quote reward markets?"

Middle work answered "can we place real orders and hedge fills?"

Later work answered "can we prove the bot is neutral when exchange truth is delayed or inconsistent?"

The final question became "is the expected reward large enough to justify the remaining risk and complexity?"

The answer appears to have been no, or at least not without a new validation phase.

Confidence in this visualization: 0.87.

---

## AI Detail

### Journey Timeline

| Phase | Core Question | What Was Built | Main Lesson | Confidence |
|---|---|---|---|---|
| 1. Thesis | Can Polymarket rewards be farmed without directional bets? | Strategy concept, PRD, reward-first non-directional design. | The thesis was clear and coherent. | 0.92 |
| 2. Shadow MVP | Can the bot identify attractive markets without trading? | Discovery, book ingestion, quote math, hedgeability, reward reports. | Research layer was practical and useful. | 0.90 |
| 3. Live foundation | Can it place real orders safely? | Auth, order placement, dry-run, live engine, user stream, order tracking. | Live exchange semantics matter more than paper design. | 0.91 |
| 4. First live hedging | Can fills be hedged quickly? | Dedicated fill handler, hedge executor, position sync, risk manager. | Fill-to-hedge path needed isolation from cycle work. | 0.93 |
| 5. Incident hardening | Can the bot survive missed or stale truth? | Reconciliation redesign, residual sizing, bounded sellback recompute, idempotent halts. | Neutrality must be proven from multiple evidence sources. | 0.94 |
| 6. Observability | Can failures be diagnosed after the fact? | Event logs, monitor API/UI, replay fixtures, live probes, archive improvements. | Event logs became mandatory for serious investigation. | 0.94 |
| 7. CTF merge proof | Can paired inventory return to collateral? | Merger interface, SAFE relayer, preflight, signature fix, live merge probe. | Merge was its own external integration, not a small helper. | 0.90 |
| 8. Competitor analysis | Are top rewards compatible with our safe strategy? | Leaderboard and market-pattern analysis. | Highest reward playbooks appeared riskier than SpreadEater's default posture. | 0.87 |
| 9. Aggregate-exposure design | Can rewards scale by entering more markets? | Aggregate exposure thesis, gates, rug-pull rules, validation plan. | Scaling changed the risk profile materially. | 0.88 |
| 10. Stop point | Is the next phase worth it? | Risk/reward reconstruction. | The likely edge did not clearly justify more complexity and live risk. | 0.86 |

### Capability Versus Complexity

| Capability Added | Benefit | Complexity Added | Net Read |
|---|---|---|---|
| Shadow market evaluation | No-risk strategy research | Low | Strongly positive |
| Live order placement | Real reward earning possible | Medium | Necessary |
| Dedicated fill handling | Faster hedges | Medium | Strongly positive |
| Reconciliation | Recovers missed fills | High | Necessary but dangerous if stale |
| CTF merge | Locks paired inventory back to collateral | High | Valuable but external-dependency-heavy |
| Monitor stack | Operator visibility | Medium | Strongly positive |
| Replay/live probes | High-confidence validation | Medium | Strongly positive |
| Watchdog and kill flatten | Emergency protection | High | Necessary but not free |
| Aggregate exposure | Potential reward scaling | Very high | Unverified |

### Incident-To-Fix Map

| Incident Or Problem | Bot Believed | Actual / Risk | Fix Direction |
|---|---|---|---|
| Cheap bid fill before filters | Old or imported orders could remain live | Bot could trade outside intended floor | Cheap bid cancellation and min-outcome enforcement |
| March 24 missed fill | Reconciliation could recover safely | Full BUY hedge at 0.99 failed and retried | Shared resolution, residual sizing, safer flatten |
| March 27 Shai | Hedge result failed, exposure remained | Hedge likely executed, truth lagged | Stronger execution proof and post-sync logic |
| Stale book halt | Book was unsafe | Cache could be stale due WS parsing/protocol issue | WS protocol update and REST rescue |
| Sellback miss | One strict FOK should resolve | Book moved and full FOK missed | Real limit pricing plus one bounded recompute |
| Merge RPC failure | Merge code existed | Transport/env could be dead | Preflight before acquisition |
| SAFE relayer 400 | Merge submitted | Signature encoding wrong | EIP-191 SafeTx signing and v-byte normalization |
| Cuba orphan halt | 32 YES exposure existed | Sellback had already flattened; API truth lagged | Settlement barrier / recent-resolution guard recommended |

### Strategy Tradeoff Matrix

| Design Choice | Safety Effect | Reward Effect | Final Interpretation |
|---|---|---|---|
| Delta-neutral only | Reduces directional risk | Gives up one-sided upside | Aligned with original thesis |
| Min outcome price floor | Avoids extreme-tail fills | Excludes rich low-price longshot markets | Safe but opportunity-limiting |
| Hedge-aware budget | Prevents overcommitment | Limits number of markets | Correct for Standard |
| Immediate sellback on cheaper path | Reduces exposure quickly | Realizes losses instead of waiting | Necessary but affects net edge |
| CTF merge primary exit | Locks complete pairs to collateral | Requires relayer/chain path | Valuable but fragile |
| Aggregate-exposure cap | More market coverage | More concurrent-fill and cascade risk | Needed separate validation |

### Risk Reward Snapshot

| Category | Upside | Downside |
|---|---|---|
| Standard mode | Safer, cleaner, closer to original thesis | Lower market count and possible lower reward capture |
| Low-price longshot markets | High reward density shown in competitor patterns | Excluded by safety filter and higher tail/adverse-selection concern |
| One-sided inventory farming | Top earners appeared to use it | Violates SpreadEater's non-directional goal |
| Aggregate-exposure scaling | Could multiply reward capture | Correlated fills, insufficient hedge cash, sellback misses, cascade halts |
| More engineering | Could close more edge cases | More maintenance, more code paths, more proof burden |

### One-Screen Story

| Chapter | Short Version |
|---|---|
| Start | Build a safe Rust bot to earn Polymarket maker rewards without betting on outcomes. |
| Build | Create discovery, quote math, hedgeability, live orders, user-stream fills, and risk controls. |
| Reality | Real exchange state is delayed, ambiguous, and sometimes contradictory. |
| Harden | Add dedicated fill handling, reconciliation, replay, live probes, CTF merge proof, and monitor tooling. |
| Compete | Learn that top reward farmers often use riskier markets or one-sided exposure. |
| Scale | Design an aggregate-exposure variant, but require go/no-go proof before accepting aggregate exposure risk. |
| Stop | The next phase needed more risk and complexity than the proven reward justified. |

### Final Visual Summary

| What SpreadEater Proved | What SpreadEater Did Not Fully Prove |
|---|---|
| A hedged Polymarket reward bot can be engineered. | That the safest version has enough net reward to justify ongoing operation. |
| Live incidents can be diagnosed with strong event logs. | That competitor reaction would leave enough reward capture at scale. |
| The hedge path can be hardened significantly. | That aggregate-exposure scaling is safe under correlated shocks. |
| CTF merge can be validated through a relayer path. | That merge, sellback, and reward capture remain profitable under high fill volume. |
| Standard strategy is conceptually aligned. | That top leaderboard strategies can be replicated without accepting their risk profile. |

Confidence in the final visual summary: 0.87.
