# SpreadEater Pitfalls And Recoveries

## Human Summary

The biggest project pitfall was assuming that a hedged strategy only needed correct hedge math.

In production, the real problem was proving that the bot actually became flat after every fill.

The live bot encountered missed fills, stale local order state, stale Polymarket position truth, strict sellback orders that failed, unsafe BUY hedge semantics, duplicate reconciliation work, book WebSocket protocol drift, and CTF merge transport failures.

The March 24 incident showed a missed fill, an unsafe full-size BUY hedge at an unaffordable limit, and repeated reconciliation retries.

The March 27 Shai incident showed the opposite problem: a hedge probably executed, but the bot believed exposure remained and unwound incorrectly.

The May 1 Cuba incident showed stale post-sellback position truth causing orphan recovery to re-discover a position that had already been flattened.

Each incident led to targeted hardening: residual-based hedge sizing, dedicated fill handling, stronger reconciliation, bounded sellback recompute, better post-sync verification, idempotent halts, and safer cancel tracking.

The merge path also became a major learning area: the project moved from RPC assumptions to a preflighted gasless SAFE relayer flow with live merge proof.

The monitoring app and event logs became essential because nearly every serious diagnosis depended on event-level evidence.

The project did not just add features; it progressively removed ways the bot could lie to itself about risk.

Confidence in this recovery map: 0.90.

---

## AI Detail

### Pitfall 1: Fill Handling Could Be Blocked Or Missed

Early live behavior exposed that fills could arrive while the main runtime was busy doing discovery, refresh, or REST-heavy work.

Operational consequence:

| Failure mode | Bot behavior |
|---|---|
| Fill event delayed behind cycle work | Hedge did not begin immediately. |
| Fill not anchored to a tracked order | Normal fill handler could skip or miss it. |
| Fill only visible through positions later | Reconciliation became the recovery path instead of the primary path. |

Recovery:

| Fix | Effect |
|---|---|
| Dedicated `FillHandler` task | Hedge handling no longer waited behind discovery or refresh cycles. |
| User-stream reconnect and order subscription hardening | Reduced startup and reconnect fill-detection gaps. |
| Tracked order fallback data | Fill handler could hedge even when a market left active management. |
| Reconciliation across known markets | Missed fills on markets outside current admission could still be detected. |

Confidence this was a major project pitfall: 0.95.

### Pitfall 2: BUY Hedge Semantics Were Dangerous

Polymarket BUY FOK behavior interprets size as notional collateral spend rather than shares.

Operational consequence:

| Old assumption | Actual risk |
|---|---|
| Submit BUY FOK for share-sized hedge | Order could represent the wrong economic request. |
| Use a fixed aggressive BUY limit like 0.99 | Full requested hedge could be rejected on balance even when a cheaper partial hedge was possible. |
| Treat failed hedge as simple adverse market condition | The failure could be self-inflicted by request shape. |

Recovery:

| Fix | Effect |
|---|---|
| BUY hedges became GTC limit orders with a short cancellation window | Preserved share-sized semantics while still crossing the book. |
| Hedge sizing moved to residual exposure | Avoided overhedging during rapid or duplicate fills. |
| Planner compared hedge versus sellback per share | Let the bot choose cheaper neutralization route. |
| Hedge affordability was enforced inside the planner | Reduced mismatch between plan and executable risk check. |

Confidence this materially improved correctness: 0.93.

### Pitfall 3: Reconciliation Was Weaker Than The Hot Path

The March 24 incident showed reconciliation discovering a missed fill and then behaving less safely than the normal fill handler.

Operational consequence:

| Reconciliation weakness | Result |
|---|---|
| Attempted full-size BUY hedge at 0.99 | Rejected for not enough balance or allowance. |
| Did not gracefully partial hedge plus sellback | Exposure stayed unresolved. |
| Retried the same failed fill for minutes | Repeated failed risk work instead of converging. |
| Halt/cleanup did not guarantee prompt flattening | Manual risk remained. |

Recovery:

| Fix | Effect |
|---|---|
| Shared resolution flow between FillHandler and reconciliation | Recovery path used the same safer logic. |
| First reconciliation failure kills and flattens | Removed long repeated-failure loops. |
| Per-market mutex shared with fill handler | Reduced double-hedging risk. |
| Known-market flatten fallback | Halted markets could still resolve metadata and flatten. |

Confidence in this diagnosis and recovery path: 0.96.

### Pitfall 4: Position Truth Lagged Execution Truth

Several incidents were not simply "the bot did not trade." They were "the bot traded, then could not prove what happened."

Examples:

| Incident | Truth problem |
|---|---|
| Shai, March 27 | Exchange screenshot showed hedge execution, while bot recorded unresolved exposure. |
| Sellback confirmation work | Positions endpoint could lag after a sellback, causing false failure. |
| Cuba, May 1 | Bot sold back to flat, then orphan recovery re-discovered stale pre-sellback YES inventory. |

Recovery:

| Fix | Effect |
|---|---|
| Execution-confirmed sellback completion | Sellback-only runs could complete from authenticated execution evidence when positions lagged. |
| Associated trade IDs on `LiveOrder` | Execution proof became attached to order data instead of global ambient state. |
| Delayed order status parsing | `DELAYED` no longer collapsed into invalid zero-fill interpretation. |
| Bounded post-sync retry | Gave delayed truth a short chance to materialize. |
| Stronger live probe confirmation | Green live result required independent confirmation or flat funded-wallet truth. |

Confidence this was one of the central reliability themes: 0.94.

### Pitfall 5: Sellback Pricing Was Too Strict In Early Paths

The strategy needed sellback as a fast way to get flat when hedging was not economical or affordable.

Operational consequence:

| Strict sellback behavior | Risk |
|---|---|
| FOK at a high computed bid | If full size was not available at that exact price, the whole sellback could fail. |
| Planner and executor price divergence | The bot thought one price was planned but sent another. |
| No bounded recompute | A moved book could produce fail-closed behavior without a second current-truth attempt. |

Recovery:

| Fix | Effect |
|---|---|
| BUY-resolution sellbacks use the planner-computed real limit | Removed planner/executor divergence. |
| One bounded recompute after sellback miss | The bot could sync truth, refresh books, and try one updated plan. |
| Failure after second miss remains fail-closed | Avoided unbounded retry loops. |

Confidence this improved the sellback path without adding hot-loop overhead: 0.92.

### Pitfall 6: Book State And WebSocket Protocol Drift

The bot depended on current opposite-side depth.

Operational consequence:

| Problem | Result |
|---|---|
| WebSocket protocol changed or was parsed incorrectly | Cached books could appear stale or incomplete. |
| Stale book kill happened too aggressively | Bot could halt or cancel bids that were actually safe after REST refresh. |
| Missing book cache during ask placement | Inventory asks could fail silently. |

Recovery:

| Fix | Effect |
|---|---|
| Updated market-channel subscription and delta parsing | Aligned with current Polymarket WebSocket payloads. |
| REST refresh-before-kill path | Reduced false stale-book halts. |
| Book WS health counters in events | Made book-feed health diagnosable from logs. |
| REST fallback for inventory asks | Prevented silent ask-placement failures when cache was empty. |

Confidence this reduced false market churn: 0.89.

### Pitfall 7: CTF Merge Was A Separate External System

The strategy assumed complete YES/NO pairs could be merged back into collateral.

Operational consequence:

| Merge issue | Result |
|---|---|
| Missing or unhealthy RPC transport | Live merge probe had to abort before buying inventory. |
| SAFE relayer signature mismatch | Merge submission returned HTTP 400. |
| Approval and nonce state needed preflight | Merge success depended on external Safe/relayer readiness. |
| Merge confirmation lag | Internal and on-chain state could briefly diverge. |

Recovery:

| Fix | Effect |
|---|---|
| `PairMerger` interface | Allowed deterministic harnesses and production implementation separation. |
| Merge preflight | Prevented unnecessary live-money acquisition when merge transport was dead. |
| Gasless SAFE relayer flow | Removed self-funded Polygon RPC dependency from the bot wallet perspective. |
| EIP-191 SAFE signature encoding fix | Live merge probe succeeded with transaction hash and collateral delta. |

Confidence the merge path became properly validated by April 11 work: 0.92.

### Pitfall 8: Monitoring Was Not Optional

Every serious investigation relied on event logs.

Operational consequence:

| Without event logs | With event logs |
|---|---|
| Hard to distinguish missed fill from stale position truth | Event sequence showed exact source component and timestamps. |
| Hard to know if hedge failed or proof lagged | Order/trade/status events allowed stronger diagnosis. |
| Hard to validate reward estimates | Decision archives captured ranking and score metadata. |
| Hard to compare strategy changes | Replay harnesses and live probes gave repeatable evidence. |

Recovery:

| Monitoring addition | Value |
|---|---|
| Event schema extensions | More precise fill, hedge, neutrality, and halt diagnosis. |
| Monitor API and dashboard | Operator-facing visibility into orders, inventory, errors, config, and history. |
| Replay harnesses | Deterministic reproduction of hedge and reconciliation paths. |
| Live probes | Controlled real-money validation of specific production paths. |

Confidence monitoring became a core project pillar: 0.94.

### Bottom Line

SpreadEater's pitfalls were mostly not abstract algorithm mistakes.

They were production-trading boundary failures:

| Boundary | Typical failure |
|---|---|
| WebSocket to local order state | Missed or unattributed fills. |
| Exchange order execution to position API | Delayed or stale truth. |
| Planner to executor | Price or affordability mismatch. |
| Risk state to cleanup action | Halt did not immediately remove all exposure. |
| Strategy estimate to competitor reality | Expected rewards could be diluted by competitors. |

The recoveries progressively moved the bot from "assume neutral" toward "prove neutral or halt."

Confidence in this overall pitfall framing: 0.90.
