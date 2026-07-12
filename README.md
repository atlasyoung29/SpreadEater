# SpreadEater

> A local-first Rust market-making bot for Polymarket that earns liquidity rewards while working to stay delta-neutral through post-fill hedging.

SpreadEater posts passive two-sided limit orders on binary prediction markets to earn Polymarket's liquidity rewards; when a fill is detected, it attempts to resolve the resulting exposure via an opposite-token hedge or sellback to minimize directional exposure. It is designed to be non-directional — profit comes from rewards, net of hedge costs and fees — though residual exposure can remain when a hedge misses or an on-chain merge fails (see [Safety](#safety)). The project also ships a companion monitor dashboard (web + terminal UI) for observing live runs.

**Profit = liquidity rewards − hedge costs − fees**

---

## Overview

Polymarket pays liquidity rewards to makers who post resting orders close to the mid-price on eligible markets. SpreadEater automates capturing those rewards while minimizing event-outcome risk:

- **Post passively** — resting limit orders on both sides (bids and reward asks) of eligible binary markets, priced to score well against Polymarket's reward function.
- **Hedge on fill** — when a fill is detected, SpreadEater attempts to resolve the resulting exposure through an opposite-token hedge or sellback, neutralizing the position as far as market conditions allow.
- **Merge to cash** — when both YES and NO tokens are held for the same market, *attempt* to merge the pair on-chain into USDC at $1.00 per pair via the Conditional Token Framework (CTF) contract or the Neg Risk Adapter. Merging requires a configured private key and relayer; if those are unavailable or a merge fails, the position is exited via inventory asks instead.

The bot never pre-hedges: capital is only put at risk after a confirmed fill. It runs entirely on the operator's own machine against Polymarket's public CLOB, Gamma, and data APIs.

| | |
| --- | --- |
| **Type** | Automated market-making bot + monitor dashboard |
| **Purpose** | Earn Polymarket maker rewards without directional exposure |
| **Strategy** | Two-sided passive quotes with immediate hedge-on-fill |
| **Language** | Rust (bot + monitor API/TUI); TypeScript + React (monitor web UI) |

> **Note:** This is an active, experimental trading system that places real orders and risks real funds when run in live mode. See [Safety](#safety) before running anything that touches real money.

---

## How it works

A concise summary of the strategy. See [`STRATEGY.md`](STRATEGY.md) for the complete, authoritative breakdown.

1. **Discovery** — On a recurring interval, poll Polymarket's discovery/Gamma/data APIs for active binary markets and filter to reward-eligible candidates (minimum daily reward, sufficient time to expiry, actively accepting orders, distinct YES/NO token IDs).
2. **Evaluation** — For each candidate market, skip cheap tail outcomes, compute a 4-leg candidate quote set (YES bid, YES ask, NO bid, NO ask), verify the opposite book has enough depth to hedge within slippage limits, and estimate the expected reward share using Polymarket's scoring formula.
3. **Ranking & budget** — Sort viable markets by estimated reward per share and allocate available budget top-down. A bid-rotation "frontier" pass on the discovery cycle can displace at most one weaker market per cycle, and only when the improvement clears a configurable threshold (prevents churn on tiny deltas).
4. **Quoting** — Place passive bids priced a configurable fraction of the way from mid toward the reward floor, sized via score-share targeting and clamped by hedgeable depth and available budget. Resting orders are refreshed on an interval and cancel-replaced when they drift beyond a basis-point threshold.
5. **Hedge on fill** — A dedicated async fill-handler task (never blocked by discovery/refresh work) detects fills over the user WebSocket stream and neutralizes exposure. A greedy per-share cost comparison routes each share of exposure to whichever exit is cheaper: **hedge** (buy the opposite token, then CTF-merge the pair to USDC) or **sellback** (sell the filled token back into its bid book).
6. **Risk controls** — Per-market hedge timeout kill switch, opposite-book depth checks, slippage/hedge-cost caps, a cash reserve held back from the trading budget, and a two-layer watchdog (in-process Rust + external Python sidecar) that can halt and flatten on WebSocket/API failure.

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

### Monitor

The `spreadeater-monitor` crate provides an Axum API server (with WebSocket streaming), a projector that reads the bot's JSONL event stream into PostgreSQL, an optional terminal UI, and a React web dashboard (Overview, Open Orders, Inventory, History, Errors, Watchlist, Config tabs). The bot writes an append-only JSONL event log; the monitor consumes it — by design this keeps file and database work off the trading hot path (event emission is non-blocking through bounded channels).

---

## Prerequisites

- **Rust toolchain** (stable; 2021 edition). Install via [rustup](https://rustup.rs).
- **Polymarket account and API credentials** — an API key/secret/passphrase (L2 auth) plus, for live trading, a wallet private key. Supply these via environment variables / a `.env` file (see [Setup](#setup--installation)). Never commit real credentials.
- **PostgreSQL** — required only for the monitor dashboard (a `docker-compose.monitor.yml` is provided to run it in Docker). The core bot does **not** use Postgres — it archives to files, and bot-side database persistence is reserved / not yet implemented. Postgres is consumed by the monitor only.
- **Python 3** — required only for the operator/watchdog helper scripts (e.g. the emergency kill-flatten script and the external watchdog sidecar), which use the Polymarket Python CLOB client for authenticated exchange actions (e.g. order cancellation / flatten). Not needed to build or run the core bot in shadow/dry-run mode.
- **Node.js** — required only if you want to build the React monitor web frontend from source.

---

## Setup / installation

```bash
# 1. Clone
git clone <repo-url>
cd <repo-root>

# 2. Build the bot (release)
cargo build --release

# 3. Provide credentials via a .env file in the repo root (see below)

# 4. Verify credentials before doing anything else
cargo run -- auth-check
```

### Credentials

Credentials are read from environment variables (a `.env` file in the repo root is auto-loaded on startup). Use placeholders — **never commit real values**. The variables the bot reads:

| Variable | Required for | Purpose |
| --- | --- | --- |
| `POLY_API_KEY` | all authenticated modes | Polymarket L2 API key |
| `POLY_SECRET` | all authenticated modes | L2 API secret (HMAC signing) |
| `POLY_PASSPHRASE` | all authenticated modes | L2 API passphrase |
| `POLY_ADDRESS` | all authenticated modes | Signer / account address |
| `POLY_PRIVATE_KEY` | **live trading & hedging** | Wallet private key for EIP-712 order signing (required for `live` without `--dry-run`) |
| `POLY_FUNDER` | optional | Funder / SAFE wallet address; falls back to the signer address if unset |
| `RELAYER_API_KEY` | on-chain CTF merge | Relayer credential for gasless merge via the SAFE relayer |
| `RELAYER_API_KEY_ADDRESS` | on-chain CTF merge | Address paired with the relayer key |

If the relayer credentials are missing, the on-chain merger is disabled and post-hedge exits fall back to inventory asks.

> `POLY_BUILDER_CODE` is a **reserved** CLOB V2 builder-code field — present for future use, but not currently read or wired into order signing by the bot.

Example `.env` (placeholder values only):

```bash
POLY_API_KEY=<YOUR_API_KEY>
POLY_SECRET=<YOUR_API_SECRET>
POLY_PASSPHRASE=<YOUR_PASSPHRASE>
POLY_ADDRESS=<YOUR_ADDRESS>
# Required only for live trading:
POLY_PRIVATE_KEY=<YOUR_PRIVATE_KEY>
# Optional / merge-related:
POLY_FUNDER=<YOUR_FUNDER_ADDRESS>
RELAYER_API_KEY=<YOUR_RELAYER_KEY>
RELAYER_API_KEY_ADDRESS=<YOUR_RELAYER_ADDRESS>
```

---

## Configuration

Runtime behavior is driven by a JSON config file (`config.json` by default; override with `--config <path>`). Every field is documented in [`CONFIG.md`](CONFIG.md). Print the built-in defaults at any time:

```bash
cargo run -- show-config
```

The config surface is grouped into sections:

- **`mode`** — `Shadow` (no real orders) or `Live` (real trading).
- **`discovery`** — reward-eligibility floor, poll interval, and the CLOB / Gamma / data API base URLs.
- **`books`** — market WebSocket URL, book staleness threshold, and REST resync interval.
- **`strategy`** — quote pricing (bid/ask depth, drift threshold, refresh interval), entry gates (minimum edge, minimum estimated daily reward, minimum outcome price), sizing/hedge limits (default quote size, max hedge cost, max slippage), frontier-rotation thresholds, and the reward `score_proxy` estimator parameters.
- **`risk`** — hedge timeout kill switch, residual-exposure tolerance, and the cash reserve held back from the trading budget.
- **`persistence`** — the file archive directory, plus a reserved `database_url` field (`null` / disabled by default; bot-side Postgres persistence is not yet implemented).
- **`observability`** — toggle and directory for the append-only JSONL event stream consumed by the monitor.
- **`watchdog`** — enable flag, an `enforce_actions` flag (defaults to observe-only), WebSocket-silence and reconnect thresholds, escalation timers, status-page polling settings, the heartbeat file path, and the kill-flatten script path.

> All values in the repo's config files and docs are examples. Review and set them for your own account, risk tolerance, and market conditions before trading.

---

## Usage

The bot is a single binary with subcommands. A global `--config <path>` flag (default `config.json`) applies to all of them. Build first, or use `cargo run -- <command>` during development.

| Command | Places real orders? | What it does |
| --- | --- | --- |
| `once` | No | Run a single shadow discovery + evaluation cycle and print a summary (markets evaluated, would-trade counts). No orders. |
| `run` | No | Run shadow mode continuously (discovery + evaluation loop, no orders). |
| `show-config` | No | Print the default configuration as JSON. |
| `auth-check` | No | Verify API credentials work (authenticated dry-run check). |
| `dry-run` | No | Run one full live-pipeline cycle with real auth but simulated orders. |
| `dry-run-loop` | No | Run the live pipeline continuously with real auth but simulated orders. |
| `live` | **Yes** (unless `--dry-run`) | **LIVE MODE** — real order placement, fill-triggered hedging, and kill switches. Requires `POLY_PRIVATE_KEY`. |
| `live --dry-run` | No | Run the live engine end-to-end with simulated orders (no real trades). |
| `export` | No | Export an archived session to CSV for spreadsheet analysis. `--session <path>` (default: latest in archive), `--output <path>`. |
| `replay` | No | Replay archived sessions through the current parameters for sensitivity analysis. `--path <file-or-dir>` (default `./data/archive`), `--competition-multiplier <f64>`. |

Examples:

```bash
# Verify credentials
cargo run -- auth-check

# Full-pipeline dry run (real auth, simulated orders) — recommended before going live
cargo run -- live --dry-run

# Live trading (real orders — see the Safety section)
cargo run -- live

# Export the latest archived session to CSV
cargo run -- export

# Replay archived sessions with an overridden competition multiplier
cargo run -- replay --path ./data/archive --competition-multiplier 2.0
```

### Monitor dashboard

The monitor is a separate binary/crate. It requires PostgreSQL (a `docker-compose.monitor.yml` is provided). Operator convenience scripts under `scripts/` (`start-monitor`, `open-monitor`, `restart-monitor`, `stop-monitor`, in `.cmd`/`.ps1`/`.sh` variants) bring up Postgres and the monitor server and print the local dashboard URL. The web dashboard exposes Overview, Open Orders, Inventory, History, Errors, Watchlist, and Config tabs.

---

## Safety

**Running `live` (without `--dry-run`) places real orders on Polymarket and puts real funds at risk.** It requires a wallet private key (`POLY_PRIVATE_KEY`) and will buy and sell outcome tokens, execute hedges, and — when a relayer is configured — attempt to merge positions on-chain automatically.

- **Always test in shadow / dry-run first.** Use `once`, `run`, `auth-check`, `dry-run`, `dry-run-loop`, and `live --dry-run` to validate configuration and behavior before committing real capital.
- **Guard your credentials.** Never commit `.env`, private keys, API secrets, or wallet addresses. Treat the private key as you would any wallet key.
- **Understand the risk controls.** The hedge-timeout kill switch, opposite-book depth checks, slippage/hedge-cost caps, cash reserve, and watchdog reduce but do not eliminate risk. Hedges can miss, books can move, and on-chain merges can fail — leaving residual exposure that the bot attempts to flatten.
- **Watchdog enforcement is off by default.** In the shipped config the watchdog runs in observe-only mode (`enforce_actions: false`); it will not automatically halt/flatten unless you enable enforcement. The external Python sidecar is a separate crash-safety net.
- **The live hedge probe is dangerous.** The Layer 3 hedge test (see below) places real orders and costs real money. Do not run it against an account that has a live `spreadeater live` session in progress.

You are responsible for your own funds, credentials, and compliance. This software is provided as-is, with no warranty.

---

## Development & testing

The hedge pipeline has a dedicated, layered test suite documented in full in [`HEDGE_TESTING_SUITE.md`](HEDGE_TESTING_SUITE.md). Use the cheapest, safest layer that answers your question — do not jump to Layer 3 unless you need live exchange behavior.

| Layer | Money risk | What it proves |
| --- | --- | --- |
| **Layer 1** — deterministic harness | None | Post-attribution hedge resolution: side selection, sizing, hedge-vs-sellback split, post-sync truth, halt behavior. Runs against a mock exchange. |
| **Layer 2** — event replay | None | Pre-attribution event path: raw-trade attribution, order-update fallback, exchange-sync missed-fill detection, orphan recovery, reconciliation routing. |
| **Layer 3** — live hedge probe | **Real** | The real downstream hedge path against live books, balances, and order submission. Places real orders — operator-only, use tiny share caps on a quiet market. |

These layers run through the standard Rust test harness (`cargo test`), not as production CLI subcommands. Typical commands:

```bash
# Fast compile / type check
cargo check --quiet

# Layer 1 (deterministic, no money risk)
cargo test --bin spreadeater layer1_ -- --nocapture

# Layer 2 (replay, no money risk)
cargo test --bin spreadeater layer2_ -- --nocapture

# Full workspace test suite
cargo test --workspace --all-targets
```

Fixtures live under `fixtures/` (e.g. `fixtures/hedge_scenarios/`, `fixtures/hedge_replay_scenarios/`, `fixtures/hedge_live_probe_scenarios/`). Operator/benchmarking helper scripts (offline run summarizers, benchmark comparison, the watchdog sidecar, and the emergency kill-flatten script) live under `scripts/`.

---

## Documentation index

| Document | Contents |
| --- | --- |
| [`STRATEGY.md`](STRATEGY.md) | Complete strategy breakdown — core thesis, market selection, quote pricing, hedge execution, CTF merge, and risk controls. |
| [`CONFIG.md`](CONFIG.md) | Reference for every field in the JSON config file. |
| [`HEDGE_TESTING_SUITE.md`](HEDGE_TESTING_SUITE.md) | The three-layer hedge test suite: what each layer proves, how to run it, and safety rules. |

---

## Status & caveats

- **Active / experimental.** SpreadEater is a working but evolving system, developed and operated against Polymarket's live CLOB. Behavior, config keys, and internal APIs change as the strategy is tuned.
- **Polymarket-specific.** It targets Polymarket's CLOB V2 (EIP-712 order signing, match-time fees) and reward program; it is not a general-purpose exchange bot.
- **No guarantees.** Rewards, hedge availability, and merge execution all depend on live market and venue conditions outside the bot's control. Past behavior is not indicative of future results.
