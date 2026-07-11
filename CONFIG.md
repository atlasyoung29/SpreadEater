# SpreadEater Config Guide

This file documents every field in [config.json](./config.json). The JSON file is now strict JSON with no comments so it can be parsed directly by the bot and the monitor.

## mode

- `mode`: `Shadow` means no real orders. `Live` means real trading.

## discovery

Controls how the bot finds reward-eligible Polymarket markets.

- `discovery.min_daily_reward`: minimum daily reward pool in USD required to admit a market.
- `discovery.poll_interval_secs`: seconds between full discovery cycles.
- `discovery.clob_base_url`: base URL for Polymarket CLOB APIs.
- `discovery.gamma_base_url`: base URL for the Gamma market metadata API.
- `discovery.data_api_base_url`: base URL for the Polymarket data API.

## books

Controls order book ingestion and resync.

- `books.ws_url`: market websocket URL.
- `books.max_book_age_secs`: books older than this are treated as stale and skipped for quoting.
- `books.resync_interval_secs`: REST resync interval for book snapshots.

## strategy

Controls quoting, gating, sizing, and scoring.

### Quote pricing

- `strategy.bid_depth_pct`: bid offset as a fraction of `max_spread` from mid.
  - `0.50` means the bid is halfway between mid and the reward floor.
  - Higher values move bids closer to mid.
  - Lower values move bids further from mid.
- `strategy.ask_depth_pct`: ask offset from mid as a fraction of `max_spread`.
  - `0.00` means at mid.
  - `0.20` means 20% of the allowed spread above mid.
  - `1.00` means at the `max_spread` boundary.
- `strategy.quote_drift_bps`: basis point drift threshold that triggers cancel-replace on resting orders.
- `strategy.quote_refresh_secs`: seconds between quote refresh passes that reuse cached books.

### Entry gates

- `strategy.min_edge_threshold`: minimum expected edge in USD, after hedge cost, required to enter.
- `strategy.min_est_daily`: minimum estimated daily reward in USD required to enter.
- `strategy.min_outcome_price`: minimum mid-price for placing a resting bid on an outcome.

### Sizing and hedge limits

- `strategy.default_quote_size`: default per-order quote size in USDC.
- `strategy.max_hedge_cost_bps`: maximum acceptable hedge cost in basis points.
- `strategy.max_slippage_bps`: maximum acceptable hedge slippage in basis points.

### score_proxy

Controls expected reward-share estimation.

- `strategy.score_proxy.competition_multiplier`: multiplier applied to competitor score estimates. Higher is more conservative.
- `strategy.score_proxy.max_score_share`: cap on estimated reward share.
- `strategy.score_proxy.min_score_share`: floor on estimated reward share.
- `strategy.score_proxy.target_score_share`: target score share used by dynamic sizing.
- `strategy.score_proxy.calibration_sample_size`: number of samples required before calibration adjusts the multiplier.

## risk

Hard risk limits.

- `risk.hedge_timeout_secs`: seconds of unhedged exposure before the market is halted and orders are cancelled. Default `10`.
- `risk.hedge_exposure_tolerance`: residual exposure (in shares) below which positions are considered hedged. Default `0.5`. Used uniformly by balance correction, risk timeout tracking, hedge verification, and reconciliation gating.
- `risk.cash_reserve`: USDC amount to always keep in the account, never used for orders or hedges. Budget = API_balance − cash_reserve. Default `50`.

## persistence

- `persistence.database_url`: optional Postgres URL for bot persistence. `null` disables it.
- `persistence.archive_dir`: path for JSON session archives.

## observability

Controls the append-only JSONL event stream used by the monitor.

- `observability.enabled`: enables monitor event emission.
- `observability.event_log_dir`: path for append-only event log files consumed by `spreadeater-monitor`.

## watchdog

Controls websocket/status watchdog assessment and kill enforcement.

- `watchdog.enabled`: spawns the watchdog loop. If `false`, no watchdog polling, verdict emission, or heartbeat writing occurs.
- `watchdog.enforce_actions`: allows the watchdog to call global halt and kill+flatten. Default `false` for observe-only mode.
- `watchdog.max_book_ws_silence_secs`: seconds without parsed book events before the websocket verdict becomes critical. Default `60`.
- `watchdog.max_user_ws_silence_secs`: seconds without user-stream events before the websocket verdict becomes critical. Default `120`.
- `watchdog.max_reconnects_in_window`: reconnect threshold inside the rolling window before the websocket verdict becomes critical. Default `5`.
- `watchdog.reconnect_window_secs`: rolling reconnect window size. Default `300`.
- `watchdog.max_consecutive_disconnects`: consecutive short-lived disconnect threshold before the websocket verdict becomes critical. Default `3`.
- `watchdog.degraded_timeout_secs`: seconds a degraded verdict can persist before escalation reaches kill-pending. Default `120`.
- `watchdog.kill_confirmation_delay_secs`: seconds kill-pending must persist before kill+flatten executes when enforcement is enabled. Default `10`.
- `watchdog.status_poll_interval_secs`: seconds between status-page polls. Default `30`.
- `watchdog.status_page_url`: Polymarket Instatus summary endpoint.
- `watchdog.critical_components`: status-page component names that count as critical.
- `watchdog.heartbeat_file`: heartbeat file written by the watchdog loop.
- `watchdog.kill_flatten_script`: script invoked by enforced kill+flatten.
