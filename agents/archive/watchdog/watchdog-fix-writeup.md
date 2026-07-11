# Watchdog Fix Writeup (Issue #21)

**Date:** 2026-03-28
**Branch:** `watchdog-fix`
**PR:** #28

## Problem

The watchdog was killing the app after ~62 seconds of "book WS silence" even when the WebSocket connection was perfectly healthy. This happened during quiet market periods when no new book updates (Snapshot/Delta) arrived, but the WS was still receiving heartbeats and subscription acknowledgments.

## Root Cause Analysis

### Bug 1: Silence Timer Only Counted Parsed Events (FIXED)

**Location:** `src/watchdog/health.rs` — `assess()` method, line 160

The `WsHealthTracker.book_ws_last_message_at` timestamp was only updated when `report_book_message()` was called, which only happened for successfully parsed `BookEvent::Snapshot` and `BookEvent::Delta` events.

Meanwhile, the WebSocket was still actively receiving messages:
- Heartbeat/ping frames
- Subscription confirmations
- Status messages
- Other non-book event types

These all called `BookWsStats.record_raw_message()` (updating `last_raw_message_at`), but the watchdog never checked that timestamp. So during quiet markets with no book updates, the silence timer would fire even though the connection was alive.

**Fix:** Added `update_book_ws_raw_activity()` to `WsHealthTracker` that feeds `BookWsStats.last_raw_message_at` into the silence timer before each `assess()` call. Any raw WS message now resets the silence timer, while genuine disconnections (no messages at all) are still detected.

### Bug 2: Recovery Deadlock from KillPending (FIXED)

**Location:** `src/watchdog/mod.rs` — state machine at line 279

Once the watchdog escalated to `KillPending` (Critical verdict for 10+ seconds), it could only recover if the system returned to fully `Healthy`. If the system improved to `Degraded` (e.g., WS reconnected but disconnect history still in the rolling window), the watchdog stayed stuck in `KillPending`.

**Scenario:**
1. Book WS goes silent → Critical → enters KillPending
2. WS reconnects, starts receiving messages → verdict becomes Degraded (disconnect times still in window)
3. System is recovering but watchdog stays in KillPending
4. If `enforce_actions=true` and `kill_confirmation_delay_secs` expires → kills the app during recovery

**Fix:** When in `KillPending` and verdict improves to `Degraded`, de-escalate to `Warning` with a fresh warning timer. This allows the system to recover naturally through Warning → Normal instead of staying stuck.

## Other Issues Identified (Not Fixed — Documented for Future)

### Issue 3: Status Poller False Kills (LOW-MEDIUM)
If the Polymarket status page is unreachable for ~150 seconds (5 consecutive poll failures at 30s intervals), the status poller returns Critical. Combined with the WS verdict, this could trigger KillPending even though the trading API is fine. **Mitigation:** `enforce_actions=false` by default prevents actual kills.

### Issue 4: Mutex Lock Contention (LOW)
The watchdog acquires `health_tracker.lock()` every 5 seconds. If LiveEngine holds the lock during a long market evaluation, the watchdog poll stalls. This could cause a 5+ second assessment gap. **Mitigation:** Assessment intervals are much shorter than the 60s silence threshold, so a single missed cycle is harmless.

### Issue 5: Heartbeat File Error Handling (LOW)
If the heartbeat file path becomes invalid (disk full, permissions), the error is silently warned. The external sidecar won't detect the watchdog is alive. **Mitigation:** Sidecar is a secondary failsafe.

## Changes Made

### `src/watchdog/health.rs`
- **Added** `update_book_ws_raw_activity(&mut self, stats: &BookWsStatsSnapshot)` — converts the raw-message UTC timestamp from `BookWsStats` to an `Instant` and updates the silence timer if more recent than the last parsed event
- **Added** import for `chrono::Utc` and `BookWsStatsSnapshot`
- **Added** 2 new tests:
  - `raw_activity_resets_silence_timer` — recent raw activity prevents false Critical
  - `stale_raw_activity_does_not_prevent_critical` — old raw activity still triggers Critical correctly

### `src/watchdog/mod.rs`
- **Added** `book_stats_snapshot` fetch before health assessment (line 138)
- **Added** `tracker.update_book_ws_raw_activity(&book_stats_snapshot)` call before `assess()` (line 142)
- **Fixed** KillPending → Degraded de-escalation: when verdict improves to Degraded while in KillPending, system now de-escalates to Warning instead of staying stuck (line 279)

## Before vs After

### Before
```
WS receiving heartbeats but no book events for 62s
→ Watchdog: "Book WS silent for 62s" → Critical
→ KillPending → (even if WS reconnects to Degraded, stays KillPending)
→ Kill app after 10s confirmation delay
```

### After
```
WS receiving heartbeats but no book events for 62s
→ BookWsStats.last_raw_message_at = recent
→ Watchdog: update_book_ws_raw_activity resets silence timer
→ Healthy (raw messages count as activity)
→ No false kill

If WS truly dies (no messages at all for 60s):
→ last_raw_message_at is stale
→ Watchdog: "Book WS silent for 62s" → Critical
→ KillPending
→ If WS reconnects (Degraded): de-escalate to Warning
→ If WS recovers fully (Healthy): reset to Normal
→ Only kills if Critical persists through full confirmation delay
```

## Test Results

- 524 tests passing (160 inline + 364 integration)
- 0 failures
- 2 new watchdog tests added
