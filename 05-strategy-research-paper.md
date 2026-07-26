# SpreadEater: A Strategy Research Paper On Hedged Liquidity Rewards

## Update — 2026-07-26

This paper's economic conclusion was reasoned from architecture and contemporaneous logs, before the fill record itself had been analysed.

A later forensic pass over the on-chain fills found a simpler and stronger reason the strategy does not close: liquidity rewards were recovered on the order of 0.2-0.4% of traded notional, while crossing from the mid to the touch cost roughly 0.6-0.7% of a share's $1.00 redemption value, and the hedge leg is a taker order by construction, so every fill pays that crossing.

Because the crossing is required once per fill rather than incurred by scale, the shortfall is structural rather than a scaling or tuning question, and no parameter setting closes it.

The comparison is an order-of-magnitude argument rather than a precise ratio, because rewards are measured against traded notional while the crossing cost is measured against a share's $1.00 face.

The "not proven scalable" framing below is superseded by that finding. The original reasoning is left in place as a record of what the evidence supported at the time.

## Human Summary

This paper frames SpreadEater as a strategy research project, not just a software build.

The central research question was whether a local Rust trading system could earn Polymarket liquidity rewards while remaining delta-neutral.

The strategy was intentionally non-directional: post passive reward-scoring orders, hedge every fill, and return paired inventory to collateral through merge when possible.

The project tested that thesis through shadow research, live trading, event-log analysis, replay fixtures, live probes, and incident-driven hardening.

The main technical stack was Rust, Tokio, Polymarket CLOB APIs, market and user WebSockets, EIP-712 order signing, HMAC L2 auth, position reconciliation, CTF merge, SAFE relayer execution, event archives, replay harnesses, and a monitor dashboard.

Decision-making was evidence-driven: read the event log, reconstruct what the bot believed, compare it to exchange truth, identify the strategy mismatch, implement the narrow fix, add regression coverage, then validate through tests or live probes.

The research found that the hardest problem was not calculating quotes; it was proving the bot actually returned to neutral under delayed or contradictory exchange state.

Major failure modes included missed fills, stale position truth, unsafe BUY hedge semantics, strict sellback behavior, stale books, CTF merge transport failures, and reconciliation re-processing already resolved exposure.

The project substantially improved the live system, but each safety gain exposed more operational complexity.

Competitor research showed that high reward earners often used lower-priced longshot markets, event farming, or one-sided inventory patterns that SpreadEater intentionally avoided.

The final strategic issue was therefore not whether the bot could be improved; it was whether the safe version of the strategy had enough upside to justify continued engineering and live-money risk.

The proposed scaling path, Operation Normandy, could have increased market count and reward capture, but it changed the risk profile by allowing aggregate exposure beyond immediately hedgeable balance.

The rational stop point was reached when the project split into two paths: keep Standard mode safe but lower-yield, or chase higher rewards with materially higher correlated-fill and operational risk.

Conclusion: SpreadEater proved that a hedged rewards bot can be engineered, but the reward earned per fill is smaller than the crossing that same fill must pay to get flat, and that shortfall is structural rather than a question of scale.

Confidence in this paper's reconstruction: 0.87.

---

## AI Detail

## Abstract

SpreadEater was a strategy-engineering project designed to test whether Polymarket liquidity rewards could be harvested by a fully hedged market-making bot. The system's research thesis was that passive, reward-scoring orders on binary prediction markets could produce positive expected return if every fill was neutralized quickly through an opposite-outcome hedge, sellback, or CTF merge.

The project progressed from shadow-mode research to live execution and then into a long safety-hardening phase driven by event-log investigations. Its key finding was that a theoretically hedged strategy becomes operationally complex in production because exchange state is delayed, incomplete, and sometimes contradictory. The final strategic conclusion was that the safe form of the strategy was likely too constrained to justify continued expansion, while the higher-yield scaling path required accepting risks that conflicted with the original low-risk thesis.

Confidence: 0.87.

## 1. Research Question

The project investigated one primary question:

Can a local-first trading system earn Polymarket liquidity rewards while maintaining near-zero directional exposure?

That question decomposed into four sub-questions:

| Sub-question | Why it mattered |
|---|---|
| Can reward-eligible markets be discovered and evaluated systematically? | Without reliable market selection, the bot cannot allocate capital intelligently. |
| Can each resting order be hedgeable before admission? | The strategy depends on neutralizing fills, not predicting outcomes. |
| Can fills be hedged quickly and proven flat? | A delayed or unverified hedge creates live-money directional exposure. |
| Can net rewards exceed hedge costs, sellback losses, fees, and operational losses? | Engineering success only matters if the economics are positive. |

The project answered the first three questions increasingly well over time. The fourth question remained the limiting factor.

Confidence: 0.86.

## 2. Strategy Thesis

SpreadEater's Standard strategy was a fully hedged liquidity-rewards market maker.

The simplified economic model was:

```text
Net edge = liquidity rewards - hedge costs - sellback losses - fees - operational losses
```

The bot was not designed to forecast event outcomes. It was designed to earn from the reward program.

### 2.1 Standard Strategy

| Strategy component | Standard behavior |
|---|---|
| Market universe | Active binary markets with daily rewards above the configured threshold. |
| Quote type | Passive reward-seeking limit orders. |
| Entry discipline | Quote only where the opposite book can support a hedge. |
| Fill response | Route each share to either hedge or sellback based on cost. |
| Pair exit | Merge complete YES/NO inventory back to collateral when possible. |
| Fallback exit | Use sellback or inventory asks when merge is unavailable or inappropriate. |
| Risk posture | Fail closed on stale books, unresolved exposure, hedge timeout, or reconciliation failure. |

### 2.2 Strategic Constraint

The strategy intentionally avoided directional exposure. That constraint made it safer, but it also limited the markets and playbooks the bot could exploit.

| Safety constraint | Economic consequence |
|---|---|
| Minimum outcome price floor | Avoids extreme tails, but skips low-priced longshot reward zones. |
| Hedge-aware budget | Prevents overcommitment, but reduces market count. |
| Immediate neutralization | Reduces event risk, but gives up upside from holding positions. |
| Binary-only focus | Simplifies hedge logic, but excludes more complex reward opportunities. |

Confidence: 0.89.

## 3. Technical Stack

SpreadEater's technical design was shaped by the need for low-latency fill handling, deterministic auditability, and safe external integration boundaries.

| Layer | Technology or module | Role |
|---|---|---|
| Core language | Rust | Performance, type safety, and reliable async systems programming. |
| Async runtime | Tokio | Concurrent discovery, refresh, WebSocket handling, fill handling, and risk tasks. |
| Market data | Polymarket REST books and market WebSocket | Bootstrap and maintain YES/NO order books. |
| User events | Polymarket authenticated user WebSocket | Detect fills and order updates. |
| Trading API | Polymarket CLOB API | Place, cancel, and inspect orders. |
| Authentication | HMAC L2 headers and EIP-712 order signing | Sign authenticated requests and orders. |
| Strategy engine | Quote engine, score proxy, viability, hedgeability | Evaluate markets, estimate rewards, and approve quotes. |
| Hedge engine | Hedge planner and executor | Resolve fills through opposite-token buys, sellbacks, and verification. |
| Position truth | PositionManager and reconciliation | Sync account positions and detect missed one-sided exposure. |
| Pair exit | CTF merge and SAFE relayer | Convert complete YES/NO pairs back to collateral. |
| Observability | Event logs, decision archives, monitor API/UI | Preserve evidence for diagnosis and operator review. |
| Testing | Unit tests, replay fixtures, live probes | Validate deterministic scenarios and selected real-money paths. |
| Emergency control | RiskManager, watchdog, kill/flatten scripts | Halt markets and flatten residual exposure. |

### 3.1 Why Rust And Tokio Fit

The strategy needed several tasks to operate independently:

| Task | Reason for independence |
|---|---|
| Discovery cycle | Can take multiple REST calls and must not block hedges. |
| Quote refresh | Runs frequently and touches many markets. |
| Book updates | Must process streaming data continuously. |
| Fill handler | Must react to fills immediately. |
| Reconciliation | Must recover missed fills without duplicating active hedge work. |
| Watchdog | Must observe system health independently of trading loops. |

Tokio allowed the project to separate fill handling from the slower discovery and refresh loops. That separation became one of the most important live-risk improvements.

Confidence: 0.91.

## 4. Market Selection And Quote Research Workflow

The bot's decision process was designed as a pipeline.

| Step | Decision made | Evidence used |
|---|---|---|
| Discovery | Is this market in universe? | Reward amount, binary status, active status, expiry. |
| Book validation | Are books usable? | REST snapshot, WebSocket freshness, YES/NO token mapping. |
| Outcome filter | Is the outcome too cheap? | Mid-price threshold. |
| Hedgeability | Can the fill be neutralized? | Opposite-book depth and slippage. |
| Reward estimation | Is reward capture worth it? | Score proxy, visible competitor depth, discount factor. |
| Ranking | Which market gets budget first? | Reward per hedge-aware share, then estimated reward. |
| Placement | Can the order be sent now? | Available budget, active orders, post-only constraints. |
| Refresh | Should orders be updated? | Drift threshold, book changes, hedge-depth checks. |

### 4.1 Decision Principle

The bot should not place an order merely because reward exists. It should place an order only when the order is:

1. reward-eligible,
2. hedgeable,
3. budget-compatible,
4. not stale,
5. not outside strategy constraints,
6. expected to produce positive net return.

Confidence: 0.90.

## 5. Incident-Driven Research Method

The project's practical research method was incident-driven.

When live behavior diverged from intended strategy, the team did not treat the error as a generic bug. It reconstructed the trading state transition.

### 5.1 Investigation Workflow

| Step | Question | Output |
|---|---|---|
| 1. Read event log | What did the bot observe and when? | Timeline of fills, hedges, risk events, and position snapshots. |
| 2. Compare exchange truth | What actually happened on Polymarket? | Confirmation from order status, trades, position API, screenshots, or live probes. |
| 3. Identify belief mismatch | Where did bot truth diverge from exchange truth? | Root cause category. |
| 4. Map to strategy invariant | Which strategy rule was violated? | STRATEGY.md alignment or misalignment. |
| 5. Propose fix | What narrow change addresses the root cause? | Implementation plan. |
| 6. Add regression | How do we prevent recurrence? | Unit, integration, replay, or live-probe coverage. |
| 7. Validate increment | Did the fix work under relevant conditions? | Test result, replay result, or controlled live result. |

### 5.2 Why Event Logs Became Central

The bot's failures were often belief failures.

Examples:

| Bot belief | Possible reality |
|---|---|
| A hedge failed. | The hedge executed, but position truth lagged. |
| A one-sided position exists. | The position was already sold back, but the positions API was stale. |
| A fill is new. | It is a late event for a resolution sellback already verified. |
| A book is stale. | The WebSocket parser failed to interpret a changed protocol payload. |
| A market is safe to rotate out. | Cancel verification has not completed, so capital is not actually free. |

Without event logs, these cases look similar. With event logs, the project could separate execution failures from truth-confirmation failures.

Confidence: 0.93.

## 6. Empirical Findings From Live Incidents

### 6.1 March 24 Hedge Incident

The March 24 incident showed that a missed fill could reach reconciliation first. Reconciliation then attempted an unsafe full-size BUY hedge at a highly aggressive fixed limit and failed for insufficient balance or allowance.

Research finding:

| Finding | Confidence |
|---|---|
| Real-time fill path missed the fill. | 0.99 |
| Reconciliation attempted an unsafe full-size BUY hedge. | 0.99 |
| The behavior violated the documented strategy. | 0.99 |
| The root cause was a compound hedge-system failure, not just market movement. | 0.97 |

Resulting design direction:

- use residual exposure rather than raw fill size,
- align reconciliation with the main resolution flow,
- fail closed faster,
- avoid unaffordable hedge requests,
- improve flatten behavior.

### 6.2 March 27 Shai Incident

The Shai incident showed that execution and proof can diverge. The exchange evidence suggested the hedge buy executed, but the bot believed exposure remained and unwound positions.

Research finding:

| Finding | Confidence |
|---|---|
| The bot selected the correct opposite side. | 0.96 |
| The post-resolution truth was wrong or stale. | 0.95 |
| Sellback strictness likely contributed. | 0.89 |
| Duplicate or misattributed fill handling added noise. | 0.97 |

Resulting design direction:

- preserve associated trade IDs,
- parse delayed order statuses correctly,
- strengthen post-sync truth,
- treat execution-confirmed sellbacks as a narrow valid completion source,
- make halt handling idempotent.

### 6.3 May 1 Cuba Incident

The Cuba incident showed a different stale-truth failure. The bot bought YES, sold it back successfully, recorded neutrality, and then orphan recovery re-discovered the same stale position as if sellback had not happened.

Research finding:

| Finding | Confidence |
|---|---|
| The bot sold back to neutral before orphan recovery fired. | 0.95 |
| Stale post-sellback truth was the likely primary cause. | 0.84 |
| Orphan recovery lacked a recent-resolution settlement barrier. | 0.78 |
| Calibration was not material to the incident. | 0.92 |

Resulting design direction:

- add settlement barriers around recently resolved sellbacks,
- avoid re-processing positions inside the known lag window,
- consider recent-resolution dedupe keys.

Confidence in the incident research synthesis: 0.91.

## 7. Validation Framework

The project used multiple layers of validation because no single test type could prove live-trading safety.

| Validation layer | Purpose |
|---|---|
| Unit tests | Prove local functions and state transitions. |
| Integration tests | Prove component interactions. |
| Replay fixtures | Reconstruct event-sequence failures deterministically. |
| Live probes | Validate selected paths against real exchange behavior with controlled exposure. |
| Event-log review | Diagnose actual production runs. |
| Strategy-doc review | Confirm code matches intended trading behavior. |

### 7.1 Why Temporary Scripts Were Not Enough

Temporary scripts can show that an API call works once. They cannot prove that the bot's strategy lifecycle is safe.

The meaningful validation question was:

Can the production path detect a fill, resolve exposure, prove neutrality, and clean up without relying on stale or synthetic truth?

That required replay harnesses, live probes, event assertions, and STRATEGY.md alignment.

Confidence: 0.90.

## 8. Competitive Landscape

Competitor research changed the interpretation of the opportunity.

The top reward accounts did not appear to be clean replicas of SpreadEater's strategy. Many looked like event farmers, low-priced longshot makers, geopolitics inventory holders, or dual-mode accounts that mixed making with directional exposure.

| Observed competitor pattern | Strategic implication |
|---|---|
| Low-priced sports or tournament longshots | High reward zones may sit below SpreadEater's price floor. |
| Geopolitics NO inventory | Some makers accept one-sided exposure. |
| Large multi-market persistent quoting | Capital and operational scale matter. |
| Dormant historical farmers | Leaderboards can overstate current competition. |
| Dual-mode accounts | Reward income and directional PnL may be mixed. |

### 8.1 SpreadEater's Strategic Divergence

SpreadEater diverged from top-earner patterns in a deliberate way.

| Top-earner behavior | SpreadEater posture |
|---|---|
| Accept one-sided inventory | Stay delta-neutral. |
| Quote very low-priced longshots | Enforce minimum outcome price. |
| Farm many event legs | Allocate budget greedily by reward per share. |
| Hold positions through market movement | Hedge or sell back quickly. |
| Potentially tolerate wider risk | Fail closed on unresolved exposure. |

This divergence made SpreadEater safer but probably reduced its maximum reward capture.

Confidence: 0.87.

## 9. Operation Normandy As A Scaling Hypothesis

Operation Normandy was the proposed answer to Standard mode's limited reward capture.

The hypothesis was:

Rewards scale with market count faster than sellback losses increase.

Normandy would allow aggregate resting exposure to exceed immediately available balance under defined caps. That would let the bot rest in more markets and potentially earn more rewards.

### 9.1 Why Normandy Was Not Just A Feature

Normandy changed the risk model.

| Standard mode | Normandy mode |
|---|---|
| Resting bid exposure bounded by available balance. | Aggregate exposure can exceed available balance. |
| Concurrent fills are rare but possible. | Concurrent fills become expected. |
| Sellbacks are occasional. | Sellbacks become routine in cash-constrained moments. |
| Cascade failures are lower probability. | Correlated fill cascades are a central risk. |

### 9.2 Normandy Go/No-Go Discipline

The Normandy docs required staged validation:

| Phase | Purpose |
|---|---|
| Phase 0a | Historical replay to bound best-case economics. |
| Phase 0b | Correctness gates before live probing. |
| Phase 0c | Live micro-probe to measure competitor reaction and actual PnL. |
| Phase 1 | Safety gates for halt and flatten behavior. |
| Phase 2 | Constrained live deployment. |
| Phase 3 | Scaling after positive evidence. |

This was the correct research approach because historical replay could not observe competitor response to the bot entering markets.

Confidence: 0.89.

## 10. Risk And Reward Analysis

### 10.1 Reward Case

The reward case was plausible:

| Reward argument | Supporting logic |
|---|---|
| Polymarket pays makers for qualifying liquidity. | Reward program creates non-directional revenue opportunity. |
| Score proxy can identify competitive markets. | Visible book depth approximates competitor score. |
| Hedging can bound directional risk. | Opposite outcome purchases and sellbacks neutralize exposure. |
| Merge can lock complete pairs to collateral. | YES plus NO returns to $1 per pair when merge succeeds. |

### 10.2 Risk Case

The risk case became stronger over time:

| Risk | Why it mattered |
|---|---|
| Reward uncertainty | Estimated share was approximate. |
| Competitor reaction | Replay cannot observe how competitors respond to our orders. |
| Exchange truth lag | Bot could make wrong decisions from stale positions. |
| Correlated fills | Multiple markets can fill during the same shock. |
| Sellback failure | The fallback path depends on live bid depth. |
| Merge dependency | Pair exit depends on relayer and chain-facing state. |
| Emergency flatten delay | Risk can persist during global halt windows. |
| Maintenance burden | Each fix added more state, tests, and operational surface. |

### 10.3 Final Risk/Reward Judgment

The project likely stopped because the next phase did not offer a clean continuation.

| Path | Expected benefit | Problem |
|---|---|---|
| Keep Standard mode | Preserve safety and strategy purity. | Reward capture may be too small. |
| Lower price filters | Access richer longshot markets. | Increases tail and adverse-selection risk. |
| Expand with Normandy | Increase market count and reward capture. | Creates aggregate exposure and correlated-fill risk. |
| Continue hardening | Reduce operational failures. | Does not by itself prove positive economics. |

The rational strategic conclusion was that the project had proven engineering feasibility, and the later fill analysis showed the economics fail at the level of a single fill rather than at the level of scale.

Confidence: 0.86.

## 11. Conclusion

SpreadEater was a serious strategy-engineering project that moved from abstract thesis to live execution and evidence-based hardening. Its most important contribution was not only the bot itself, but the research process around it: logs, replay, probes, incident reports, and explicit strategy alignment.

The project demonstrated that a fully hedged liquidity-rewards bot can be built, but also that "fully hedged" is a runtime property that must be continuously proven. The final stop point was rational because the remaining upside required either accepting more risk than the original thesis intended or conducting a new empirical validation program before scaling.

In research-paper terms:

| Research claim | Result |
|---|---|
| A delta-neutral rewards bot is technically feasible. | Supported. |
| The strategy can be made materially safer through event-driven hardening. | Supported. |
| The safe Standard strategy captures the highest reward zones. | Not supported. |
| Scaling through aggregate exposure is economically attractive. | Unproven. |
| Continued development was clearly justified by reward upside. | Not established. |
| The reward earned per fill exceeds the crossing that fill must pay. | Not supported. |

Final conclusion:

SpreadEater validated the engineering discipline needed for hedged market-making, but the strategy pays more to hedge each fill than the reward program pays it to quote, and that inequality is structural rather than a matter of tuning or scale.

Overall confidence: 0.87.

## 12. References Within The Repository

| File | Role in this paper |
|---|---|
| `STRATEGY.md` | Authoritative Standard strategy. |
| `agents/summary.md` | Current project summary and implementation state. |
| `agents/changelog.md` | Chronological development record. |
| `agents/archive/prd.md` | Original product requirements and staged rollout. |
| `agents/archive/handoff.md` | Early operations guide. |
| `agents/archive/hedge-incidents/hedge-incident-20260324/hedge-incident-report-2026-03-24.md` | Missed-fill and unsafe hedge incident. |
| `agents/archive/hedge-incidents/hedge-incident-20260327/hedge-incident-report-2026-03-27.md` | Shai post-resolution truth incident. |
| `agents/hedge-incidents/2026-05-01-cuba-reconciliation-orphan-halt-handoff.md` | Cuba stale orphan recovery incident. |
| `agents/competitor-analysis/01-top-rewards-leaderboard-decode.md` | Competitor strategy comparison. |
| `agents/Normandy/strategy.md` | Scaling strategy design. |
| `agents/Normandy/thesis-validation.md` | Normandy validation framework. |
| `agents/Normandy/prerequisites.md` | Correctness, safety, and scaling gates. |
