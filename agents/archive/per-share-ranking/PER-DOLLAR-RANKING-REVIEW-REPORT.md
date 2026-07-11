# Review Report: Per-Dollar Ranking Commit

## Executive Summary

I reviewed the latest branch commit, `0c1a75f` (`Redenominate reward estimation from per-share to per-dollar-committed`).

My conclusion is that the commit appears to move the documentation, reporting, and some viability terminology toward a per-dollar model, but the live execution path is not fully aligned with that description yet.

The three main findings are:

1. Status `est_daily` now diverges again from the discounted selection math.
2. The live allocator still ranks by absolute estimated reward, not the new per-dollar metric.
3. The viability denominator is still share-count based, not committed-dollar based, despite the new docs/tests describing committed capital as `price * size`.

## What Was Reviewed

- Latest commit:
  - `0c1a75f` `Redenominate reward estimation from per-share to per-dollar-committed`
- Main files inspected:
  - [live_engine.rs](<repo-root>/src/runtime/live_engine.rs)
  - [viability.rs](<repo-root>/src/strategy/viability.rs)
  - [shadow.rs](<repo-root>/src/reporting/shadow.rs)
  - [reward_per_dollar_tests.rs](<repo-root>/tests/unit/strategy/reward_per_dollar_tests.rs)
  - [STRATEGY.md](<repo-root>/STRATEGY.md)

## Findings

### 1. Status `est_daily` diverges again from the discounted selection math

In the new viability logic, `estimated_reward` is discounted:

- [viability.rs](<repo-root>/src/strategy/viability.rs#L30)

But the status helper still computes:

- `daily_reward_total * score_share`
- without the discount factor

See:

- [live_engine.rs](<repo-root>/src/runtime/live_engine.rs#L2124)
- [live_engine.rs](<repo-root>/src/runtime/live_engine.rs#L1966)

That means:

- selection/admission math is now discounted
- status logging still prints and aggregates undiscounted `est_daily`

This recreates an operator-facing mismatch between:

- what the bot uses to decide whether to trade
- what the logs imply the market is worth

**Why this matters**

This can make markets look more attractive in the logs than they really are under the current gate/ranking math.

**Confidence**

High confidence.

### 2. The allocator still ranks by absolute reward, not the new per-dollar metric

The commit introduces `return_per_dollar` in viability:

- [viability.rs](<repo-root>/src/strategy/viability.rs#L69)

But the live ranking path still sorts only by `estimated_reward`:

- [live_engine.rs](<repo-root>/src/runtime/live_engine.rs#L830)

So the effective behavior is still:

- markets pass/fail the viability gate
- then viable markets are ranked by absolute estimated daily reward

It is **not** yet:

- rank by best per-dollar return
- allocate capital in per-dollar order

**Why this matters**

A market with higher raw daily dollars but worse per-dollar efficiency can still consume budget before a better-yield market, as long as both clear the gate.

**Confidence**

High confidence.

### 3. The viability denominator is still shares, not committed dollars

The new docs/tests describe committed capital in dollar terms, especially `price * size`:

- [reward_per_dollar_tests.rs](<repo-root>/tests/unit/strategy/reward_per_dollar_tests.rs#L126)

But in production viability code, the denominator is still:

- `effective_quote_size`

See:

- [viability.rs](<repo-root>/src/strategy/viability.rs#L50)

That means the current live formula still behaves like:

- capital committed is roughly `$1/share`

rather than:

- actual posted notional or other explicitly dollar-denominated commitment

So a cheap YES bid and an expensive NO bid with the same share size still end up sharing the same denominator in admission logic, even though the new tests/docs imply they should differ.

**Why this matters**

This is the most important implementation mismatch in the commit. It means the new “per-dollar committed” description is not yet actually true in the live viability gate.

**Confidence**

High confidence.

## Testing Gap

The new test file mainly validates standalone arithmetic helpers rather than the real live code paths:

- [reward_per_dollar_tests.rs](<repo-root>/tests/unit/strategy/reward_per_dollar_tests.rs)

Because of that, the tests do not catch the fact that:

- ranking still uses `estimated_reward`
- viability still divides by `effective_quote_size`

So the current test coverage is not sufficient to prove that the production allocator really moved to per-dollar behavior.

**Confidence**

High confidence.

## Simple Fix Direction

If the strategy engineer agrees with the findings, the clean direction appears to be:

1. Decide what “committed capital” should mean in live strategy terms.
2. Make `compute_viability(...)` use that exact denominator.
3. Make live ranking sort by the intended metric, if the goal is truly per-dollar-first allocation.
4. Make status estimation use the same discounted/undediscounted basis as selection, so logs and admission stay comparable.
5. Add regression tests against the real production functions, not just standalone arithmetic examples.

This is a strategy-authority decision more than a mechanical code cleanup, especially around:

- ranking objective
- denominator semantics
- whether the discount factor should affect ranking, status, or both

## Confidence By Point

- `High confidence`: status `est_daily` is currently inconsistent with discounted selection math.
- `High confidence`: live allocator still ranks by absolute reward, not by per-dollar return.
- `High confidence`: viability denominator is still share-based, not committed-dollar based.
- `High confidence`: current new tests do not validate the real live ranking/viability implementation.

## Open Questions

1. Should the allocator rank by:
   - discounted absolute reward,
   - undiscounted absolute reward,
   - discounted per-dollar return,
   - or another metric entirely?
2. What is the intended definition of “capital committed” for passive bid admission?
3. Should status logs mirror the exact selection basis, or intentionally show a different operator-facing number?

## Bottom Line

The commit moves the repo language and some calculations toward a per-dollar model, but the live execution path is not fully there yet.

The discrepancy looks real and material enough that it should be reviewed as a strategy decision before implementation proceeds further.
