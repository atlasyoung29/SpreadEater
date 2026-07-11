# SpreadEater — Operations Guide

## 1. Overview

SpreadEater is a local-first Rust bot for Polymarket that earns liquidity rewards through fully hedged binary market making. The strategy is non-directional: post passive quotes on reward-eligible markets, collect maker rewards, and hedge immediately on fill using opposite-outcome depth.

**Status:** All 4 stages complete. Live trading working as of 2026-03-08.

- Stage 1: Shadow mode (real data, no orders)
- Stage 2: Live trading foundation (auth, positions, dry-run)
- Stage 3: Live bot (order placement, hedging, kill switches)
- Stage 4: Refinement (score proxy, calibration, replay, parameter tuning)

---

## 2. Trade Lifecycle

### Step 1 — Discovery
Every 60 seconds (`poll_interval_secs`), fetch markets from Polymarket's `GET /sampling-markets` API. Filter for:
- Binary markets only
- Active, not closed/archived, accepting orders
- Reward-eligible with `daily_reward_total >= $10`

### Step 2 — Quote Generation & Ranking
All admitted markets are evaluated first (no orders placed), then ranked by estimated daily reward (highest first). For each market, compute 4-leg quotes:
- **YES BID** — buy YES tokens
- **YES ASK** — sell YES tokens (requires inventory)
- **NO BID** — buy NO tokens
- **NO ASK** — sell NO tokens (requires inventory)

Bid price is set at `mid - (bid_depth_pct * max_spread)`. Asks are priced at mid rounded up to nearest cent within `max_spread`.

### Step 3 — Hedge Check
Before placing a bid, verify the opposite side has enough depth to hedge at acceptable slippage (`max_slippage_bps`). If not:
- Depth below `min_size` → cancel the bid entirely
- Depth below order size but above `min_size` → scale bid down to hedgeable amount

This check runs every 15 seconds on all resting bids.

### Step 4 — Bid Placement (in ranked order)
Markets are processed in ranked order (highest estimated reward first). Place passive GTC limit bids (post-only). Total USDC committed across all BUY orders is capped by `max_total_exposure`. Only approved quotes get placed. Bids are placed unconditionally (no inventory required). Budget enforcement in `place_quotes` naturally caps lower-ranked markets as budget is consumed.

### Step 5 — Fill Detection (dedicated task)
WebSocket `UserStream` monitors the authenticated user channel for trade events. When a fill event arrives, the main `select!` loop forwards the `TradeEvent` via an unbounded channel to a dedicated `FillHandler` task (`tokio::spawn`). This ensures fill processing is never blocked by periodic work (discovery cycles, depth checks, quote refreshes) which can take 5-30s of REST calls.

### Step 6 — Hedge Execution
The `FillHandler` task matches the fill to a tracked order, computes the hedge size from **projected residual exposure** (not raw fill size), then `HedgeExecutor` places an offsetting order on the **opposite outcome** to lock in the spread. BUY hedges use aggressive GTC limit at $0.99 + cancel remainder (Polymarket BUY FOK interprets size as notional spend, not shares). SELL hedges use FOK at $0.01. Example: if YES BID fills, hedge by buying NO tokens. Fee rates are cached (5-min TTL) to eliminate an extra REST round-trip from the hedge critical path.

If the hedge fails (insufficient depth, timeout), the market is kill-switched: all orders cancelled, market halted.

### Step 7 — Position Tracking
`PositionManager` syncs positions from Polymarket's Data API (`GET /positions?user=ADDRESS`). After a hedge, you hold both YES and NO tokens.

### Step 8 — Ask Placement (Exit)
`place_inventory_asks()` places sell orders on **both** YES and NO inventory simultaneously. These asks:
- Earn rewards while resting on the book
- Exit the position when filled
- Are placed immediately after a hedge
- Are also placed during regular cycles if bid-side economics don't justify new positions

### Step 9 — Quote Refresh
Every 30 seconds (`quote_refresh_secs`), re-read cached books (maintained by WebSocket) and cancel-replace any orders that have drifted beyond `quote_drift_bps` from the new target price. No REST calls — reads from in-memory book cache.

---

## 3. Trade Evaluation

### Score Proxy
Estimates reward competitiveness using Polymarket's scoring formula:

```
S(v, s) = ((v - s) / v)^2 * size
```

Where `v` = max_spread, `s` = distance from mid. Orders closer to mid score higher.

### Competition Estimation
Competitor score is estimated from book depth with a `competition_multiplier` (default 1.5x). The bot estimates its share of total market score and sizes accordingly.

### Gates (all must pass)
1. **Reward viability** — estimated reward must exceed hedge + execution cost (`min_edge_threshold`)
2. **Hedgeability** — full fill size must be immediately hedgeable on the opposite side
3. **Capital budget** — order size (at $1/share hedge-aware cost) must fit within `available_budget = gross_balance - committed_exposure`

### Bid Depth
`bid_depth_pct` (default 0.50) controls how far from mid the bid is placed, as a fraction of `max_spread`. At 0.50, bids sit at 50% of the allowed spread from mid — balancing reward score against fill probability.

---

## 4. Hedging Strategy

- **Passive-first:** Never pre-hedge. Only execute a hedge after a resting order fills.
- **Immediate response:** On bid fill, place aggressive limit on opposite outcome within milliseconds. BUY hedges use GTC+cancel (not FOK — Polymarket BUY FOK interprets size as notional). SELL hedges use FOK.
- **Slippage buffer:** Hedge price includes a buffer derived from walking the opposite book.
- **Kill switch:** If hedge fails (no depth, timeout after `hedge_timeout_secs`), the market is halted and all its orders are cancelled.
- **Continuous monitoring:** Every 15 seconds, check that resting bids still have sufficient opposite-side depth. Cancel or scale down bids that can no longer be hedged.

---

## 5. Exit Strategy

After a hedge completes, you hold both YES and NO tokens. Exit happens through ask orders:

1. **Immediate ask placement** — `place_inventory_asks()` places sell orders on both YES and NO inventory right after the hedge.
2. **Ask pricing** — Asks are priced at the orderbook mid rounded up to the nearest cent, staying within `max_spread` to qualify for reward scoring.
3. **Ongoing refresh** — The 30-second quote refresh loop cancel-replaces ask orders that drift from optimal price.
4. **Cycle re-evaluation** — During regular discovery cycles, if bid-side economics are no longer viable, the bot cancels bids but keeps asks resting to exit inventory and continue earning rewards.
5. **No forced liquidation** — The bot relies on ask fills to exit positions. There is no market-order liquidation mechanism.

---

## 6. Risk Controls

| Control | Config Key | Current Value | Description |
|---|---|---|---|
| Max total exposure | `risk.max_total_exposure` | $300 | Caps total USDC committed across all BUY orders |
| Max position size | `risk.max_position_size` | 300 | Caps per-market position size |
| Hedge timeout | `risk.hedge_timeout_secs` | 60s | Kill switch if unhedged exposure exceeds this |
| Max slippage | `strategy.max_slippage_bps` | 80 bps | Rejects hedges with excessive slippage |
| Max hedge cost | `strategy.max_hedge_cost_bps` | 80 bps | Rejects hedges with excessive cost |
| Book staleness | `books.max_book_age_secs` | 30s | Skips quoting on stale books |

### Order Reconciliation
Every discovery cycle, `sync_open_orders()` fetches all live orders from the exchange API and:
- **Imports** any orders not yet tracked (handles restarts)
- **Prunes** any tracked orders no longer on the exchange (handles UI cancels, external fills)
- **Cancels cheap bids** below `min_outcome_price` ($0.20) found during import

After sync, `cancel_cheap_bids()` sweeps ALL tracked orders and cancels any bid priced below the threshold (catches orders from prior cycles).

### Market Kill Switch
Triggered by hedge failure or risk breach. Actions: cancel all orders for the market, halt the market from further quoting.

---

## 7. CLI Commands

All commands use the binary name. From the project root:

```bash
# Build
export PATH="/c/msys64/mingw64/bin:$PATH" && cargo build

# Run the binary (from target dir or with cargo run --)
```

| Command | Description |
|---|---|
| `spreadeater once` | Single shadow discovery + evaluation cycle (no orders) |
| `spreadeater run` | Continuous shadow loop |
| `spreadeater show-config` | Print default config as JSON |
| `spreadeater auth-check` | Verify API credentials work |
| `spreadeater dry-run` | Single live cycle with auth, no real orders |
| `spreadeater dry-run-loop` | Continuous dry-run loop |
| `spreadeater live` | **LIVE MODE** — real orders placed |
| `spreadeater live --dry-run` | Live engine with simulated orders (no real trades) |
| `spreadeater export` | Export latest session to CSV |
| `spreadeater export --session FILE` | Export specific session file to CSV |
| `spreadeater replay` | Replay archived sessions with current params |
| `spreadeater replay --competition-multiplier 2.0` | Replay with overridden competition multiplier |

### Stopping the Bot
- **Graceful:** `Ctrl+C` in the terminal
- **Force (PowerShell):** `Stop-Process -Name spreadeater -Force`

### Config Override
```bash
spreadeater --config custom-config.json live
```

---

## 8. Configuration

Config is loaded from `config.json` (defaults used if missing). Supports `//` line comments for inline documentation. Current values:

```json
{
  "mode": "Live",
  "discovery": {
    "min_daily_reward": "10",
    "poll_interval_secs": 60,
    "clob_base_url": "https://clob.polymarket.com",
    "gamma_base_url": "https://gamma-api.polymarket.com",
    "data_api_base_url": "https://data-api.polymarket.com"
  },
  "books": {
    "ws_url": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    "max_book_age_secs": 30,
    "resync_interval_secs": 60
  },
  "strategy": {
    "max_hedge_cost_bps": "80",
    "max_slippage_bps": "80",
    "default_quote_size": "5",
    "min_edge_threshold": "0.01",
    "quote_drift_bps": "30",
    "bid_depth_pct": "0.50",
    "ask_depth_pct": "0.20",
    "quote_refresh_secs": 30,
    "min_est_daily": "0.25",
    "min_outcome_price": "0.20",
    "score_proxy": {
      "competition_multiplier": "1.5",
      "max_score_share": "0.25",
      "min_score_share": "0.0001",
      "target_score_share": "0.03",
      "calibration_sample_size": 10
    }
  },
  "persistence": {
    "database_url": null,
    "archive_dir": "./data/archive"
  },
  "risk": {
    "max_position_size": "300",
    "hedge_timeout_secs": 60,
    "max_total_exposure": "300"
  }
}
```

### Config Field Reference

| Field | Description |
|---|---|
| `discovery.min_daily_reward` | Minimum daily reward ($) to admit a market |
| `discovery.poll_interval_secs` | Discovery cycle interval (seconds) |
| `books.max_book_age_secs` | Books older than this are considered stale |
| `books.resync_interval_secs` | REST resync interval for book data |
| `strategy.max_hedge_cost_bps` | Max acceptable hedge cost (basis points) |
| `strategy.max_slippage_bps` | Max acceptable slippage (basis points) |
| `strategy.default_quote_size` | Default quote size (USDC units) |
| `strategy.min_edge_threshold` | Minimum expected edge to approve a trade ($) |
| `strategy.quote_drift_bps` | Price drift threshold for cancel-replace (basis points) |
| `strategy.bid_depth_pct` | Bid offset as fraction of max_spread (0.50 = 50%) |
| `strategy.ask_depth_pct` | Ask offset from mid as fraction of max_spread (0.20 = 20%, for trading PnL) |
| `strategy.quote_refresh_secs` | Lightweight quote refresh interval (seconds) |
| `strategy.min_est_daily` | Minimum estimated daily reward (our share) to enter a market ($) |
| `strategy.min_outcome_price` | Minimum mid-price to bid on an outcome (per-leg, default $0.20) |
| `strategy.score_proxy.competition_multiplier` | Multiplier on competitor score estimate (higher = more conservative) |
| `strategy.score_proxy.max_score_share` | Cap on estimated reward share |
| `strategy.score_proxy.min_score_share` | Floor on estimated reward share |
| `strategy.score_proxy.target_score_share` | Target score share for dynamic sizing (e.g. 0.03 = 3%) |
| `strategy.score_proxy.calibration_sample_size` | Calibration samples before adjusting multiplier in live mode |
| `persistence.database_url` | Postgres connection URL (null = disabled) |
| `persistence.archive_dir` | Directory for JSON session archive files |
| `risk.max_position_size` | Max position per market (USDC) |
| `risk.hedge_timeout_secs` | Kill switch timeout for unhedged exposure (seconds) |
| `risk.max_total_exposure` | Total USDC cap across all BUY orders |

---

## 9. Environment Setup

### Prerequisites
- **Rust 1.94.0** (stable-x86_64-pc-windows-gnu toolchain)
- **MSYS2 MinGW-w64 GCC** at `/c/msys64/mingw64/bin` (must be in PATH for builds)
- Target directory set to `C:\rust-build\spreadeater` in `.cargo/config.toml` (avoids spaces-in-path issues)

### Build
```bash
export PATH="/c/msys64/mingw64/bin:$PATH:$HOME/.cargo/bin" && cargo build
```

Note: The bot executable is locked while running. Stop the bot before rebuilding.

### Environment Variables (.env file)
```
POLY_API_KEY=your-api-key-uuid
POLY_SECRET=your-base64-secret
POLY_PASSPHRASE=your-passphrase
POLY_ADDRESS=0xYourEOAAddress
POLY_PRIVATE_KEY=0xYourPrivateKey
POLY_FUNDER=0xYourGnosisSafeAddress
```

| Variable | Description |
|---|---|
| `POLY_API_KEY` | API key UUID from Polymarket |
| `POLY_SECRET` | Base64-encoded API secret (URL-safe, no padding) |
| `POLY_PASSPHRASE` | API passphrase |
| `POLY_ADDRESS` | EOA wallet address (for L2 auth headers) |
| `POLY_PRIVATE_KEY` | EOA private key (for EIP-712 order signing). Required for live mode. |
| `POLY_FUNDER` | Gnosis Safe address (maker address for order signing, signatureType=2) |

Derive/refresh credentials: `python derive_keys.py` (uses py-clob-client, needs MetaMask private key)

---

## 10. Key Log Lines

### Status and Monitoring
| Log Message | Meaning |
|---|---|
| `--- STATUS ---` | Periodic summary: managed_markets count, committed_capital, available_budget, max_exposure |
| `position` | Per-market inventory (yes_pos, no_pos) and resting orders |
| `unmanaged orders` | Tracked orders on markets not in current managed set |

### Order Activity
| Log Message | Meaning |
|---|---|
| `Passive order placed` | New GTC limit order submitted to exchange |
| `Order placed` | Order confirmed by exchange (includes order_id, status) |
| `Order cancelled` | Order removed from exchange |
| `Order resized` | Order cancel-replaced at new size |

### Hedge and Risk
| Log Message | Meaning |
|---|---|
| `Cancelling bid: hedge depth below min_size` | Opposite-side depth insufficient — bid cancelled. Includes market_name. |
| `Scaling down bid to match hedge depth` | Bid resized to match available hedge depth. Includes market_name. |
| `Quote drifted, cancel-replacing` | Refresh loop detected price drift, replacing order |
| `Market orders cancelled` | Kill switch triggered on a market |
| `Bid orders cancelled (asks preserved)` | Bid-side cancelled but asks kept for inventory exit |

### Sync and Reconciliation
| Log Message | Meaning |
|---|---|
| `Open orders fetched from API` | Shows api_returned and live_count from exchange |
| `Synced existing order` | Imported an exchange order into tracking |
| `Open order sync complete` | Summary: imported, already_tracked, pruned counts |
| `Pruned stale tracked order` | Removed a tracked order that disappeared from exchange |

### Discovery
| Log Message | Meaning |
|---|---|
| `Discovery complete` | Shows fetched, admitted, below_threshold, filtered_out counts |
| `No markets passed filters` | Zero markets met criteria this cycle |
| `Skipping halted market` | Market halted by risk manager, orders being cancelled |

---

## 11. Architecture

```
src/
  main.rs              CLI entry point (clap)
  config.rs            Config structs with defaults
  models/              All data types (Market, OrderBook, Quote, Hedge, etc.)
  discovery/           Market fetch (sampling-markets API) + filter/reconcile
  books/               REST bootstrap, WebSocket deltas, BookManager (in-memory)
  strategy/            Quote engine, hedgeability depth-walk, reward viability, score proxy
  trading/
    client.rs          TradingClient — authenticated CLOB API (place/cancel orders)
    order_manager.rs   OrderManager — tracks resting orders, place/cancel-replace/reconcile
    positions.rs       PositionManager — syncs positions from Data API
    hedge_executor.rs  HedgeExecutor — BUY GTC+cancel / SELL FOK hedge after fill
    user_stream.rs     UserStream — WebSocket user channel (fill events)
    risk_manager.rs    RiskManager — market halts, kill switches
  runtime/
    live_engine.rs     LiveEngine — main tokio::select! loop (discovery, refresh, fills, hedge monitor)
    orchestrator.rs    Orchestrator — shadow/dry-run modes
    replay.rs          ReplayEngine — replay archived sessions with param overrides
  auth/
    credentials.rs     ApiCredentials from env vars
    signer.rs          RequestSigner (HMAC-SHA256 L2 auth headers)
    order_signer.rs    OrderSigner (EIP-712 for order placement)
  persistence/         FileArchive (JSON session files)
  reporting/           Shadow reports, CSV export
```

### Key Polymarket API Endpoints

| Purpose | Endpoint | Auth |
|---|---|---|
| Market discovery | `GET /sampling-markets` | No |
| Order book | `GET /book?token_id=...` | No |
| Place order | `POST /order` | Yes (L2 + EIP-712) |
| Cancel order | `DELETE /order` (body: `{"orderID": id}`) | Yes (L2) |
| Cancel market orders | `DELETE /cancel-market-orders` (body: `{"market": cid, "asset_id": aid}`) | Yes (L2) |
| Open orders | `GET /data/orders` | Yes (L2) |
| Order scoring | `GET /orders/{id}/scoring-status` | Yes (L2) |
| Positions | `GET /positions?user=ADDR` (data-api host) | No |
| Book WebSocket | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | No |
| User WebSocket | `wss://ws-subscriptions-clob.polymarket.com/ws/user` | Yes |
