# Changelog

## 2026-04-25 - Full validation and live probe refresh

- Ran a full validation pass after the BUY-side bid-reclaim and websocket hedge-depth guard changes. `cargo check --workspace --all-targets`, `cargo test --workspace --all-targets -- --nocapture`, `cargo run --quiet -- auth-check`, and the monitor web `npm run build` all passed.
- Revalidated live probe fixtures against current CLOB sampling markets and books:
  - refreshed `fixtures/hedge_live_probe_scenarios/scotus_mail_ballots_buy_probe_5.json` because the live YES ask moved enough that the old `0.69` trigger cap and `$3.45` trigger notional cap were stale
  - refreshed `fixtures/hedge_live_probe_scenarios/scotus_mail_ballots_yes_bid_probe_under_10.json` because the current NO hedge limit needed a `$1.70` hedge-notional cap
  - replaced the stale/non-sampling SCOTUS merge probe with `fixtures/merge_live_probe_scenarios/anthropic_ipo_before_2027_pair_merge_probe.json`, an active accepting sampling market with live YES/NO caps that matched current books
- Live validation completed:
  - user-stream smoke passed for `no_fed_rate_cuts_2026_yes_bid_probe_under_10`, `scotus_mail_ballots_buy_probe_5`, and `scotus_mail_ballots_yes_bid_probe_under_10`
  - armed hedge live probes passed with `meta_pass=true`, `standard_pass=true`, and `cleanup_pass=true` for the same three hedge scenarios
  - armed merge live probes passed with `meta_pass=true`, `standard_pass=true`, and `cleanup_pass=true` for `no_fed_rate_cuts_2026_pair_merge_probe`, `playboi_carti_before_gta6_pair_merge_probe`, and `anthropic_ipo_before_2027_pair_merge_probe`
- The old SCOTUS merge probe failed before replacement with `standard_pass=false` after acquiring `5.007` paired shares on a non-sampling market, but cleanup still passed and the account ended clean. Post-run read-only account audit reported `0` open orders and `0` active positions.
- Updated `HEDGE_TESTING_SUITE.md` because the old standalone `cargo run -- hedge-test` / `cargo run -- hedge-replay` examples are no longer exposed by the current binary; the doc now points at the current inline harness filters and live-probe scripts.
- The only validation surface not executed was the ignored monitor Postgres integration suite because Docker Desktop / the local Postgres service was not running on this machine. No `STRATEGY.md` update was required because this was validation fixture/documentation maintenance, not a strategy change.

## 2026-04-22 — BUY-side bid reclaim and websocket hedge-depth fast path

- Added two narrow hedge-safety improvements without changing the planner's hedge-vs-sellback economics or redefining unsafe market conditions:
  - `src/runtime/live_engine.rs` now lets BUY-side resolution prep reclaim external resting bid capital when current free hedge budget is still below residual exposure size. After the existing filled-market cancel/reconcile/balance refresh, resolution prep cancels bid legs on other markets, waits through the same bounded cancel window, refreshes balance again, and then hands the reclaimed `max_hedge_usdc` to the existing planner. SELL-side resolution remains unchanged.
  - `src/runtime/live_engine.rs` also now runs the existing hedge-depth guard immediately for the affected managed market on fresh book `Snapshot`/`Delta` events, reusing the same `min_outcome_price`, `min_size`, and hedgeable-depth predicates/actions as the 2-second hedge-depth pass. The 2-second hedge-depth loop and 5-second quote-refresh loop remain in place.
- Added `OrderManager` support in `src/trading/order_manager.rs` for canceling bid orders globally while excluding the filled market and for counting external active/pending bid state during reclaim waits.
- Added runtime regressions for BUY-side reclaim, SELL-side no-reclaim, sufficient-budget no-op reclaim, websocket-triggered immediate resize/cancel, unrelated-token no-op, stale-book no-op, and pending-cancel suppression.
- Updated `STRATEGY.md` to document BUY-side resolution-time bid reclaim and the websocket-triggered immediate hedge-depth guard as timing/availability improvements rather than strategy changes.
- Validation on 2026-04-22:
  - `cargo test --bin spreadeater --no-run`
  - `cargo test --bin spreadeater prepare_market_for_resolution_ -- --nocapture`
  - `cargo test --bin spreadeater ws_hedge_depth_guard_ -- --nocapture`
  - `cargo test --bin spreadeater hedge_depth_resize_emits_diagnostics -- --nocapture`

## 2026-04-22 — Benchmark order-event weighting correction

- Fixed `scripts/summarize_benchmark.py` so reward-bid exposure and live-order integrals now change at the actual order/fill event timestamp instead of waiting for the next `status_snapshot`.
  - order transitions (`order_submitted`, `fill_detected`, `order_cancelled`, `order_resized`) now first advance the active snapshot clock to the event timestamp, then mutate order state, then recompute reward-bid exposure utilization and live-order counts in-place for subsequent accrual
  - this corrects advisory benchmark fields that depend on event-time weighting: `mean_bid_exposure_utilization_pct`, `reward_bid_exposure_share_hours`, `reward_bid_order_hours`, and `bid_churn_per_live_bid_order_hour`
- Updated `tests/test_benchmarking_scripts.py` expectations for partial fills, cancel/replace churn, and the end-to-end fixture so they now assert the event-accurate integrals instead of the older snapshot-lagged values.
- `STRATEGY.md` required no update because this pass corrects offline benchmark math only.
- Validation on 2026-04-22:
  - `python -m unittest discover -s tests -p "test_benchmarking_scripts.py"`

## 2026-04-21 — Benchmark summarization stale-pointer hardening and artifact policy correction

- Hardened `scripts/kill_flatten.py` so `--summarize-run` only auto-resolves `data/current_run.json` when that file includes a valid PID for a live `spreadeater` process:
  - stale or malformed `current_run.json` metadata now emits a warning and skips auto-summarization unless `--run-id` is passed explicitly
  - this closes the stale-run misattribution path where a manual reward delta could be attached to an old run after a crash or prior exit
- Added Python regressions in `tests/test_benchmarking_scripts.py` covering both the live-PID happy path and the stale-PID skip path.
- Corrected the benchmark artifact ignore policy:
  - `data/events/**/benchmark_summary.json` remains trackable as durable team-facing benchmark output
  - `data/events/**/run_metadata.json` is now ignored again because it contains host-local paths and PIDs
  - raw `data/events` churn that was not meant to ship with the PR was removed from version control scope
- `STRATEGY.md` required no update because this pass changes benchmark operational safety and repository hygiene only, not trading behavior.
- Validation on 2026-04-21:
  - `python -m unittest discover -s tests -p "test_benchmarking_scripts.py"`
  - `git check-ignore -v data\\events\\run_20260422_001400\\run_metadata.json data\\events\\run_20260422_001400\\benchmark_summary.json`

## 2026-04-21 — Benchmark artifact gitignore narrowing

- Updated `.gitignore` so benchmark artifacts can be committed without admitting raw event dumps:
- `/data/*` stays ignored by default
  - `data/events/` traversal is reopened
  - `data/events/**/events.jsonl` and `data/events/**/events.jsonl.gz` remain ignored
  - per-run `run_metadata.json` and `benchmark_summary.json` are now trackable
  - `data/current_run.json` remains ignored so the active-run pointer does not create routine workspace noise
- Validation on 2026-04-21:
  - `git check-ignore -v data\\events\\run_20260422_001400\\events.jsonl data\\events\\run_20260422_001400\\run_metadata.json data\\events\\run_20260422_001400\\benchmark_summary.json data\\current_run.json`

## 2026-04-21 — Benchmark summarizer nanosecond timestamp fix

- Fixed `scripts/summarize_benchmark.py` so it now accepts Rust/Chrono nanosecond-precision ISO timestamps in `run_metadata.json` / `current_run.json`.
- The parser now normalizes fractional seconds to microsecond precision before calling Python 3.10 `datetime.fromisoformat(...)`, which avoids failures on values like `2026-04-22T00:06:27.102274200Z`.
- Added a regression in `tests/test_benchmarking_scripts.py` that drives the real summarizer with a nanosecond `started_at`.
- Validation on 2026-04-21:
  - `python -m unittest discover -s tests -p "test_benchmarking_scripts.py"`
  - `python scripts/summarize_benchmark.py --run-id run_20260422_000627 --reward-delta-usd 0.01`

## 2026-04-19 — Reward Benchmarking v1

- Added startup-only run metadata persistence for live-engine sessions:
  - new `src/runtime/run_metadata.rs` canonicalizes and SHA-256 hashes the loaded `Config`, resolves absolute paths, and writes identical metadata to `data/current_run.json` and `data/events/<run_id>/run_metadata.json`
  - `src/runtime/live_engine.rs` now records that metadata once during `LiveEngine::new(...)` using the generated `run_id`, selected mode, startup timestamp, resolved event paths, supplied config path, and `risk.cash_reserve`
  - `src/main.rs` and the hedge-support harness call sites now pass the config path into `LiveEngine::new(...)`
- Added offline benchmark analysis CLIs:
  - new `scripts/summarize_benchmark.py` streams `events.jsonl` line-by-line, rebuilds active reward-bid order state from raw order/fill events, computes the agreed summary metrics, and writes `data/events/<run_id>/benchmark_summary.json`
  - new `scripts/compare_benchmarks.py` compares candidate vs baseline summaries and emits `pass` / `warn` / `fail` / `incomplete` using `actual_reward_usd_per_hour` as the sole v1 verdict driver, with utilization / churn / estimated-reward posture remaining advisory
- Refactored `scripts/kill_flatten.py` into a structured CLI:
  - added `--summarize-run`, `--run-id`, `--reward-delta-usd`, `--note`, and `--ended-at`
  - when `--summarize-run` is used without `--run-id`, the script now reads `data/current_run.json` before kill/cleanup and summarizes that exact run afterward
  - the script stays non-interactive and `watchdog_sidecar.py` was intentionally left unchanged
- Added benchmark coverage:
  - runtime metadata emission regression in `src/runtime/live_engine.rs`
  - config-hash stability regression in `src/runtime/run_metadata.rs`
  - Python script coverage plus a realistic mini JSONL fixture in `tests/test_benchmarking_scripts.py` and `tests/fixtures/benchmarking/mini_events.jsonl`
- `STRATEGY.md` required no update because this change adds observability/benchmarking plumbing only and does not alter market admission, sizing, hedge execution, or runtime monitor behavior.
- Validation on 2026-04-19:
  - `python -m unittest discover -s tests -p "test_benchmarking_scripts.py"`
  - `cargo test --bin spreadeater startup_writes_run_metadata_files_with_expected_schema -- --nocapture`
  - `cargo test --bin spreadeater status_snapshot_emits_last_book_ws_stats -- --nocapture`
  - `cargo test --lib config_hash_is_stable_for_equivalent_configs -- --nocapture`
  - `cargo test --bin spreadeater --no-run`

## 2026-04-17 — SAFE relayer retry hardening and standard merge probe refresh

- Hardened `src/trading/ctf_merge.rs` so the gasless SAFE relayer path now survives the transient failures seen in live validation without weakening merge truth:
  - SAFE deployment / nonce / submit / lookup calls now do bounded retries on transport timeouts and transient relayer `408/429/5xx` responses
  - relayer transaction lookup timeouts no longer abort the poll loop immediately; they consume the existing merge poll budget and keep polling
  - terminal on-chain SAFE `STATE_FAILED` executions now retry with a fresh nonce up to 2 additional attempts before the runtime gives up and falls back to inventory asks
  - standard markets still skip ERC-1155 approval entirely; neg-risk approval remains on CTF for the adapter and now benefits from the same SAFE retry handling
- Added regression coverage in `tests/unit/trading/ctf_merge_tests.rs` for transient deployment failures, nonce `500`/timeout failures, exact-payload submit retry, transaction-lookup retry, terminal SAFE retry, and neg-risk approval retry.
- Refreshed the checked-in standard merge live-probe scenario by replacing the stale `china_invade_taiwan_2026_pair_merge_probe` fixture with `fixtures/merge_live_probe_scenarios/playboi_carti_before_gta6_pair_merge_probe.json`, because the China scenario was no longer a reliable live standard-merge probe candidate.
- Updated `tests/support/hedge/layer2.rs` split-resolution expectations so the Layer 2 harness now explicitly expects the documented fallback inventory asks on the paired residual when merge is unconfigured/failed.
- Updated `STRATEGY.md` Section 5 to document the bounded relayer/Safe retry behavior before fallback asks.
- Validation on 2026-04-17:
  - `cargo test --test core_types ctf_merge_ -- --nocapture`
  - `cargo test --bin spreadeater layer2_ -- --nocapture`
  - `cargo test --bin spreadeater -- --nocapture`
  - `cargo test --workspace --no-run`
  - `powershell -ExecutionPolicy Bypass -File <repo-root>/scripts/run_user_stream_smoke.ps1 -Scenario no_fed_rate_cuts_2026_yes_bid_probe_under_10`
  - `powershell -ExecutionPolicy Bypass -File <repo-root>/scripts/run_hedge_live_probe.ps1 -Scenario no_fed_rate_cuts_2026_yes_bid_probe_under_10`
  - `powershell -ExecutionPolicy Bypass -File <repo-root>/scripts/run_merge_live_probe.ps1 -Scenario no_fed_rate_cuts_2026_pair_merge_probe`
  - `powershell -ExecutionPolicy Bypass -File <repo-root>/scripts/run_merge_live_probe.ps1 -Scenario playboi_carti_before_gta6_pair_merge_probe`
  - `python scripts/kill_flatten.py`

## 2026-04-17 — Resolution truth cleanup, fill anchoring, and calibration hardening

- Fixed four stabilization issues in `src/runtime/live_engine.rs` and `src/trading/client.rs` without changing public config or strategy policy:
  - calibration no longer treats a market-level "would trade" result as proof that a sampled tracked order should still be scoring; prediction now recomputes order-level score compatibility under the current competition multiplier and reuses recent actual non-scoring observations on unchanged orders to stop repeated false-positive ratcheting
  - scoring endpoint non-2xx responses now surface as errors in `src/trading/client.rs` so failed scoring probes are skipped instead of silently being recorded as `actual_scoring=false`
  - fill anchoring now has a bounded exact-signature fallback for unattributed user-stream trades (same market, token, side, price, and sufficient size) against active or recently cancelled tracked orders, and late self-executed sellback trades that were already verified during resolution are suppressed instead of being reprocessed as new fills
  - hedge resolution prep and halted-market cleanup now reconcile exchange order truth and retry pending cancels before computing hedge affordability or deferring cleanup, which prevents stale tracked bids from pinning `available_hedge_budget` at zero or wedging halted markets in repeated pending-drain cleanup
- Added inline `live_engine` regressions for the new order-level calibration behavior, repeated false-positive suppression, exact-signature fallback anchoring, late sellback suppression, resolution-prep budget truth reconciliation, and halted-cleanup reconciliation before defer.
- Updated `STRATEGY.md` to reflect the order-level calibration rule, explicit fill anchoring fallback, pre-budget order-truth reconciliation, halted cleanup reconciliation, and suppression of late already-accounted sellback trades.
- Validation on 2026-04-17:
  - `cargo test --bin spreadeater sample_order_scoring -- --nocapture`
  - `cargo test --bin spreadeater predict_order_scoring_uses_recent_actual_false_for_unchanged_order -- --nocapture`
  - `cargo test --bin spreadeater exact_signature_fallback -- --nocapture`
  - `cargo test --bin spreadeater recent_resolution_trade_skip -- --nocapture`
  - `cargo test --bin spreadeater finalize_halted_cleanup_reconciles -- --nocapture`
  - `cargo test --bin spreadeater prepare_market_for_resolution_prunes_stale_orders_before_budget -- --nocapture`
  - `cargo test --bin spreadeater --no-run`
  - `cargo test --bin spreadeater -- --nocapture`
  - `cargo test --workspace --no-run`


- Validation:
  - `git status --short --branch`

## 2026-04-16 — Paired neg-risk merge routing and venue-safe approvals

- Added paired `neg_risk=true` merge support in `src/trading/ctf_merge.rs` by extending `PairMerger::merge_positions(...)` with explicit market-class routing and introducing internal standard vs neg-risk venue selection. Standard markets still merge through `CTF_ADDRESS`; neg-risk markets now submit the same `mergePositions(...)` calldata to `NEG_RISK_ADAPTER_ADDRESS`.
- Reworked SAFE approval handling in `src/trading/ctf_merge.rs` so `setApprovalForAll` is still sent to the CTF ERC-1155 contract, but the approved operator now matches the selected venue. Approval caches are now split by venue (`standard` vs `neg_risk`) so a prior standard approval does not suppress the first neg-risk approval.
- Updated `src/runtime/live_engine.rs`, `src/runtime/hedge_test.rs`, and `tests/support/hedge/support.rs` so pair exits resolve merge venue from `CanonicalMarket.neg_risk`, production never guesses a standard merge when market metadata is missing, and the harness fails clearly when it cannot resolve market metadata for venue selection.
- Added regression coverage in `tests/unit/trading/ctf_merge_tests.rs` and inline `live_engine` tests for standard routing, neg-risk routing, independent approval caches, neg-risk relayer failure surfacing, explicit missing-metadata failure, and runtime refusal to guess a merge venue.
- Refreshed `fixtures/hedge_live_probe_scenarios/scotus_mail_ballots_yes_bid_probe_under_10.json` after the standard hedge probe started failing on 2026-04-16 due to a stale trigger cap (`0.83` vs live-required `0.95`). The checked-in fixture now uses the live-safe cap that passed the probe and describes the current buy-side hedge/sellback path truthfully.
- Validation on 2026-04-16:
  - `cargo test --test core_types ctf_merge_ -- --nocapture`
  - `cargo test --bin spreadeater harness_merge_pairs_ -- --nocapture`
  - `cargo test --bin spreadeater merge_ -- --nocapture`
  - `powershell -ExecutionPolicy Bypass -File <repo-root>/scripts/run_user_stream_smoke.ps1 -Scenario scotus_mail_ballots_yes_bid_probe_under_10`
  - `powershell -ExecutionPolicy Bypass -File <repo-root>/scripts/run_hedge_live_probe.ps1 -Scenario scotus_mail_ballots_yes_bid_probe_under_10`
  - `powershell -ExecutionPolicy Bypass -File <repo-root>/scripts/run_merge_live_probe.ps1 -Scenario china_invade_taiwan_2026_pair_merge_probe`
  - `powershell -ExecutionPolicy Bypass -File <repo-root>/scripts/run_hedge_live_probe.ps1 -Scenario no_fed_rate_cuts_2026_yes_bid_probe_under_10`
  - `powershell -ExecutionPolicy Bypass -File <repo-root>/scripts/run_merge_live_probe.ps1 -Scenario no_fed_rate_cuts_2026_pair_merge_probe`
- Updated `STRATEGY.md` Section 5 to distinguish standard CTF merge vs neg-risk adapter merge and to state explicitly that event-level neg-risk `convertPositions(...)` remains out of scope on this branch.

## 2026-04-16 — User-WS liveness truth and calibration convergence

- Fixed the authenticated user websocket liveness path in `src/models/events.rs`, `src/trading/user_stream.rs`, `src/watchdog/health.rs`, and `src/runtime/live_engine.rs` by adding an internal `UserEvent::RawActivity` signal. The user stream now emits raw-activity events for authenticated `PING`, `PONG`, and non-business text frames that prove the socket is alive, and the live engine routes those events only into the watchdog liveness tracker without triggering fill or order side effects.
- Updated the relevant harness/test surfaces in `tests/support/hedge/layer2.rs`, `tests/support/hedge/live_probe.rs`, and `tests/hedge_live_probe.rs` so raw user-stream heartbeat traffic is accepted as socket liveness instead of being mistaken for a business-event gap.
- Fixed calibration convergence in `src/runtime/live_engine.rs` by replacing the old scoring predictor with a fresh-book, current-multiplier evaluation path. Sampled tracked orders are now predicted from the same competition-adjusted market evaluation the bot uses for admission, and a sampled order only predicts `true` when the market is still viable and that exact tracked leg remains compatible with a currently approved quote. Stale or missing books still skip sampling.
- Added regressions for raw user-WS liveness and multiplier-sensitive prediction in `tests/unit/models/events_tests.rs`, `tests/unit/trading/user_stream_tests.rs`, `tests/unit/watchdog/health_tests.rs`, and inline `live_engine` runtime tests, including the false-positive calibration window that now stops repeating once the multiplier increases enough to deadmit the sampled order.
- Validation on 2026-04-16:
  - `cargo test raw_user_activity_resets_silence_timer -- --nocapture`
  - `cargo test repeated_raw_user_activity_prevents_critical_silence -- --nocapture`
  - `cargo test predict_order_scoring_flips_when_competition_adjusted_evaluation_deadmits_market -- --nocapture`
  - `cargo test sample_order_scoring_stops_repeating_false_positives_once_prediction_flips -- --nocapture`
  - `cargo test sample_order_scoring_ -- --nocapture`
  - `cargo test raw_user_activity -- --nocapture`
  - `cargo test --bin spreadeater -- --nocapture` (`308` passed, `0` failed, `3` ignored)
- No `STRATEGY.md` update was required for this increment because the current strategy doc already describes truthful fresh-book calibration sampling and this change does not alter trading policy, config, or public interfaces.

## 2026-04-15 — Live probe handshake, marketable pricing, and null-order lookup hardening

- Fixed the user-stream live probe and production user-stream handshake in `src/trading/user_stream.rs` by switching the auth message to the documented `{"auth": ..., "type": "user"}` shape, sending an immediate heartbeat, and treating the first heartbeat/data frame as a real connection confirmation instead of waiting for a nonexistent subscription ACK. Added unit coverage for the auth payload shape and heartbeat detection.
- Hardened the active live probe harness in `tests/support/hedge/live_probe.rs` so both the hedge probe and merge probe now derive marketable BUY limits from current live asks plus one tick before placing any acquisition order, then compare those derived limits against the scenario safety caps. This removed the false “missing decision / missing pair” failures that were really stale scenario-price failures.
- Fixed the live probe cleanup observer in `tests/support/hedge/live_probe.rs` to keep a small dedicated cleanup window after the main probe budget expires and to honor a stable-baseline confirmation that lands exactly at the timeout boundary. Added regressions for the cleanup window floor and the marketable-limit derivation helpers.
- Fixed `TradingClient::get_order()` in `src/trading/client.rs` so a venue `null` order payload is treated as `None` instead of failing the whole merge live probe with an order-deserialization error. Added parser coverage for both `null` and normal order payloads.
- Refreshed the manual SCOTUS live-probe fixtures in `fixtures/hedge_live_probe_scenarios/scotus_mail_ballots_yes_bid_probe_under_10.json` and `fixtures/merge_live_probe_scenarios/scotus_mail_ballots_pair_merge_probe.json` so the scenario caps match the current live market while staying within the intended safety envelope.
- Validation on 2026-04-15:
  - `cargo test --bin spreadeater -- --nocapture` (304 passed, 0 failed, 3 ignored)
  - `cargo test --bin spreadeater hedge_harness -- --nocapture`
  - `cargo test --bin spreadeater harness_merge_pairs_ -- --nocapture`
  - `powershell -ExecutionPolicy Bypass -File <repo-root>/scripts/run_user_stream_smoke.ps1 -Scenario scotus_mail_ballots_yes_bid_probe_under_10`
  - `powershell -ExecutionPolicy Bypass -File <repo-root>/scripts/run_hedge_live_probe.ps1 -Scenario scotus_mail_ballots_yes_bid_probe_under_10`
  - `powershell -ExecutionPolicy Bypass -File <repo-root>/scripts/run_merge_live_probe.ps1 -Scenario scotus_mail_ballots_pair_merge_probe`
  - Final live results were green: user-stream smoke `ack_received=true`, hedge probe `meta_pass=true standard_pass=true cleanup_pass=true`, merge probe `meta_pass=true standard_pass=true cleanup_pass=true`.
- No `STRATEGY.md` update was required for this increment because the bot’s trading strategy and runtime decision policy did not change; this was live-validation and connector hardening only.

## 2026-04-14 — Stale-book auto-recovery, truthful scoring calibration, and hedge-depth admission clamp

- Fixed stale-book halt churn in `src/runtime/live_engine.rs` by deduplicating repeated stale-book kill transitions, tracking halted-market cleanup state, and auto-resuming stale-book halts only after cleanup verifies flat/no-orders plus two consecutive fresh-book confirmations. The cleanup degradation path now emits deduplicated monitor warnings instead of spamming repeated halt transitions.
- Fixed score-proxy calibration drift in `src/runtime/live_engine.rs` by replacing the old "all tracked orders should be scoring" assumption with fresh-book prediction via `per_order_score(...)`. Calibration samples now use the order's real market metadata, current tracked price/size, and fresh book truth; stale or missing book truth is skipped instead of manufacturing false positives.
- Fixed admission sizing overshoot in `src/runtime/live_engine.rs` by clamping approved bid candidates to currently hedgeable opposite-book depth before placement and flooring that clamp to whole shares. The same flooring is now used in the periodic hedge-depth resize path so neither admission nor resize can round up above actually hedgeable depth.
- Added regressions for duplicate stale-book halt suppression, two-confirmation stale-book auto-resume, streak reset after failed freshness confirmation, pre-admission hedge-depth clamping on fractional depth, truthful calibration sampling, and stale-book sample skipping. Also updated the existing hedge-depth no-op resize case to prove floor-based behavior.
- Validation: `cargo test --bin spreadeater stale_book_ -- --nocapture`, `cargo test --bin spreadeater sample_order_scoring_ -- --nocapture`, `cargo test --bin spreadeater pre_admission_clamps_bid_size_to_current_hedge_depth -- --nocapture`, `cargo test --bin spreadeater hedge_depth_ -- --nocapture`, `cargo test --bin spreadeater finalize_halted_cleanup_ -- --nocapture`, and `cargo test --bin spreadeater -- --nocapture` (298 passed, 0 failed, 3 ignored).
- Updated `STRATEGY.md` Section 2.2, Section 3.1, and Section 6.5 to describe truthful calibration sampling, pre-admission hedge-depth clamping, and stale-book auto-recovery behavior.

## 2026-04-13 — Merge truth convergence for legitimate `standard_pass`

- Added a shared post-merge truth observer in `src/runtime/live_engine.rs` so confirmed pair merges now reconcile against the same direct `/positions` truth source that the live probe checks.
- Production merge success still returns immediately after relayer confirmation, balance refresh, and initial position sync, but now spawns a detached bounded observer (1s polls, 30s max, 2 consecutive matching snapshots). Comparisons now normalize share dust to the bot's 2-decimal venue precision, and sync errors reset the match streak so stale cached truth cannot manufacture convergence. On timeout it emits a structured `MonitorDegraded` warning instead of forcing fallback liquidation or re-failing the merge.
- The harness path now waits on that same observer before returning `merge_succeeded`, so `standard_pass` must be backed by real post-merge truth convergence rather than a timing race between relayer success and `/positions` lag.
- Added regressions for missing-row-as-flat handling, consecutive-match requirements, non-blocking timeout warning emission, and lagged harness convergence.
- Added regressions for sub-share residual normalization and sync-error streak reset after a review caught the stale-cache false-convergence hole.
- Re-ran the armed merge live probe after the fix and got `meta_pass=true`, `standard_pass=true`, `cleanup_pass=true`, `pair_exit_status=merge_succeeded`, and `collateral_delta_usdc=3.000000`.
- Updated `STRATEGY.md` Section 5 to replace stale receipt-polling / `POLY_FUNDER` language with the current relayer flow and bounded truth-convergence behavior.

## 2026-04-13 — Mixed hedge+sellback reconciliation fallback fix

- Fixed the shared hedge-resolution executor so a buy-side resolution that uses both a hedge leg and a sellback leg no longer false-fails when the first retry confirms only the hedge leg and sellback truth is still lagging.
- The sellback evidence retry is no longer suppressed after a hedge-evidence retry, and execution-confirmed sellbacks can now project from the current post-sync position once hedge truth is already confirmed.
- Added regression coverage for execution-confirmed mixed-path sellback projection and removed the unused-assignment warning from the new post-sync fallback logic.
- Updated STRATEGY.md and agent docs to describe this as shared fill-handler/reconciliation behavior rather than reconciliation-only behavior.

## 2026-04-12 — Reconciliation sellback evidence retry to avoid false halts

- Added a bounded post-resolution sync retry when an execution-confirmed sellback is verified but the first post-sync exposure still exceeds tolerance, preventing false reconciliation halts on stale truth.
- Allowed execution-confirmed sellback evidence to override stale post-sync exposure (even when a post position exists) once tolerance would otherwise fail.
- Added unit regressions for sellback-evidence retry gating.
- Updated STRATEGY.md reconciliation section to reflect exposure verification and execution-confirmed sellback fallback.

## 2026-04-11 — Same-cycle frontier handoff to eliminate idle-cash window

- Added `frontier_handoff_window_secs` config parameter (default 5s) to `StrategyConfig` in [config.rs](src/config.rs)
- Implemented `run_same_cycle_frontier_handoff()` in [live_engine.rs](src/runtime/live_engine.rs): after cancelling a frontier loser, polls for cancel verification every 250ms within the handoff window. Calls `retry_pending_cancels()` during polling to push verification forward.
- Implemented `select_best_post_cancel_market()` in [live_engine.rs](src/runtime/live_engine.rs): once cancel is confirmed, fresh-evaluates the current cycle's admitted/evaluated non-held candidate markets with current books and budget, ranks by `reward_per_share`, and places the best viable market. Does not reuse the historical `known_markets` cache for same-cycle selection.
- Narrowed the Phase 3 `managed_markets` write-lock scope so the handoff polling and candidate selection no longer hold the guard across awaits, removing the lock contention that could delay fill-handler market metadata reads.
- Added regression coverage for same-cycle handoff disable, timeout, successful same-cycle placement, and the current-cycle-only post-cancel selector.
- If the handoff window times out, falls back to the existing next-cycle reservation activation (no behavior change from baseline).
- Live run evidence: 12 rotations in 1.5 hours, every one had ~60s idle gap. This change reduces the gap to <5s.
- Updated STRATEGY.md Section 2.3 and Section 9 config table.

## 2026-04-11 — Rotate decision archives into daily JSONL and prune old files on startup

- Updated [archive.rs](<repo-root>/src/persistence/archive.rs) so new decision archives now append compact JSONL records into a daily `decisions/YYYYMMDD.jsonl` file instead of writing one pretty JSON file per decision
- Added startup-only, best-effort retention pruning for `decisions/`:
  - prunes legacy `.json` decision files older than 7 days
  - prunes daily `.jsonl` decision files older than 7 days
  - leaves `sessions/` unchanged so replay/export behavior stays intact
  - emits quantitative prune logs for scanned files, deleted files, reclaimed bytes, and retention days
- Expanded [archive_tests.rs](<repo-root>/tests/unit/persistence/archive_tests.rs) to cover:
  - JSONL append behavior
  - pruning old decision `.json`
  - pruning old decision `.jsonl`
  - preserving recent decision files
  - preserving session files during startup pruning

## 2026-04-11 — Fix SAFE signature encoding and confirm live relayer merge

- Corrected SAFE relayer signature encoding in [ctf_merge.rs](<repo-root>/src/trading/ctf_merge.rs):
  - SafeTx digest signing now uses the EIP-191 prefix
  - signature v-byte now uses `31/32` (v_raw + 31) to match SAFE relayer expectations
- Updated the relayer submit mock tests to assert approval/merge intent via calldata selectors instead of removed request metadata
- Live merge probe validation now succeeds end-to-end with a relayer-submitted merge, real `merge_tx_hash`, and positive collateral delta
- Validation:
  - `cargo test ctf_merge_ -- --nocapture`
  - `python <repo-root>/scripts/kill_flatten.py`
  - `powershell -ExecutionPolicy Bypass -File <repo-root>/scripts/run_merge_live_probe.ps1 -Scenario scotus_mail_ballots_pair_merge_probe`

## 2026-04-11 — Replace self-funded merge RPC with gasless SAFE relayer flow

- Replaced the old Polygon-RPC/self-funded merge transport in [ctf_merge.rs](<repo-root>/src/trading/ctf_merge.rs) with a relayer-backed SAFE implementation:
  - merge no longer reads or depends on `POLYGON_RPC_URL`
  - relayer auth now uses `RELAYER_API_KEY` + `RELAYER_API_KEY_ADDRESS`
  - preflight now validates relayer auth, SAFE deployment, and SAFE nonce readiness
  - merge approval and `mergePositions` both submit through Polymarket’s relayer and poll relayer state to terminal success/failure
- Updated [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) so runtime merger initialization uses signer/funder credentials plus relayer env only
- Replaced the legacy RPC-constructor coverage in [ctf_merge_tests.rs](<repo-root>/tests/unit/trading/ctf_merge_tests.rs) with mocked relayer tests covering:
  - relayer-ready preflight success
  - auth / deployment / nonce failures
  - approval-then-merge happy path
  - approval skipping after first successful merge in-process
  - terminal relayer failure propagation
- Updated merge docs and operator terminology:
  - [STRATEGY.md](<repo-root>/STRATEGY.md)
  - [TERMINOLOGY.md](<repo-root>/tests/support/hedge/TERMINOLOGY.md)
  - [agents/summary.md](<repo-root>/agents/summary.md)
- Validation:
  - targeted merger unit tests
  - targeted merge harness tests
  - `cargo test --workspace`
  - `powershell -ExecutionPolicy Bypass -File <repo-root>/scripts/run_merge_live_probe.ps1`

## 2026-04-09 — Fail fast when merge RPC is missing or unhealthy

- Added merge probe guardrails in [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs):
  - merge live-probe scenario validation now rejects acquisition legs whose marketable BUY notional is below the venue minimum
  - merge probe failures now preserve the full error chain instead of collapsing to the top-level context
- Extended the merge interface in [ctf_merge.rs](<repo-root>/src/trading/ctf_merge.rs) with `preflight_check()` and implemented a harmless `eth_chainId` transport health check for the production merger
- Reworked Polygon JSON-RPC response handling in [ctf_merge.rs](<repo-root>/src/trading/ctf_merge.rs) so HTTP/non-JSON failures now include status/body snippets instead of surfacing as opaque `error decoding response body`
- Added [harness_ctf_merge_preflight()](<repo-root>/src/runtime/live_engine.rs) and targeted tests in [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) so the manual merge probe fails before placing orders when merge transport is unavailable
- Updated the sample live merge probe scenario in [scotus_mail_ballots_pair_merge_probe.json](<repo-root>/fixtures/merge_live_probe_scenarios/scotus_mail_ballots_pair_merge_probe.json) from `2` to `3` shares so both acquisition legs clear the venue’s minimum marketable BUY size
- Clarified the paired merge-failure/unconfigured fixture descriptions in:
  - [merge_failure_places_fallback_asks.json](<repo-root>/fixtures/hedge_scenarios/merge_failure_places_fallback_asks.json)
  - [merge_unconfigured_places_fallback_asks.json](<repo-root>/fixtures/hedge_scenarios/merge_unconfigured_places_fallback_asks.json)
- Validation:
  - `cargo test harness_ctf_merge_preflight -- --nocapture`
  - `cargo test validate_merge_live_probe_scenario -- --nocapture`
  - `cargo test live_probe_ -- --nocapture`
  - `cargo test layer1_merge_ -- --nocapture`
  - `cargo test --workspace`
  - armed merge live probe rerun now fails safely before acquisition with `cleanup_pass=true` and explicit error `ctf merger preflight failed: eth_chainId preflight request failed ... https://polygon-rpc.com ... unexpected EOF during handshake`

## 2026-04-09 — Added deterministic merge proof and a dedicated live merge-only probe

- Added a small merger interface in [ctf_merge.rs](<repo-root>/src/trading/ctf_merge.rs) so the deterministic hedge harness can inject a mock merge implementation without changing production behavior
- Refactored the existing post-fill pair-exit branch in [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) into shared helpers:
  - `try_merge_pairs`
  - `place_pair_fallback_asks`
  - `execute_pair_exit`
- Added two harness-only seams on [LiveEngine](<repo-root>/src/runtime/live_engine.rs):
  - `harness_ctf_merge_enabled()`
  - `harness_merge_pairs(...)`
- Extended the deterministic hedge harness schema in:
  - [hedge_test.rs](<repo-root>/src/runtime/hedge_test.rs)
  - [support.rs](<repo-root>/tests/support/hedge/support.rs)
  so scenarios can configure a mock merge outcome and assert merge/fallback telemetry
- Added deterministic Layer 1 merge fixtures:
  - [merge_success_after_full_buy_hedge.json](<repo-root>/fixtures/hedge_scenarios/merge_success_after_full_buy_hedge.json)
  - [merge_failure_places_fallback_asks.json](<repo-root>/fixtures/hedge_scenarios/merge_failure_places_fallback_asks.json)
  - [merge_unconfigured_places_fallback_asks.json](<repo-root>/fixtures/hedge_scenarios/merge_unconfigured_places_fallback_asks.json)
- Added deterministic merge tests in [layer1.rs](<repo-root>/tests/support/hedge/layer1.rs) and runtime harness tests in [live_engine.rs](<repo-root>/src/runtime/live_engine.rs)
- Corrected the paired merge-failure/unconfigured expectations to the current documented runtime behavior:
  - fully hedged `YES==NO` inventory cannot produce fallback asks through the existing `inventory_ask` path because asks only sell excess above a hedge pair
  - these fixtures now assert `fallback_asks_failed` with `fallback_ask_count=0`
  - this matches the existing known limitation already documented in [STRATEGY.md](<repo-root>/STRATEGY.md#L348)
- Added a dedicated ignored/manual live merge-only probe in [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs) with:
  - separate `SPREADEATER_MERGE_LIVE_PROBE_SCENARIO` env var
  - separate scenario schema for self-acquiring a YES/NO pair
  - pre-cleanup merge validation using `harness_merge_pairs(...)`
  - explicit balance-delta and pair-disappearance checks before cleanup
- Added operator runner and sample scenario:
  - [run_merge_live_probe.ps1](<repo-root>/scripts/run_merge_live_probe.ps1)
  - [scotus_mail_ballots_pair_merge_probe.json](<repo-root>/fixtures/merge_live_probe_scenarios/scotus_mail_ballots_pair_merge_probe.json)
- Updated [TERMINOLOGY.md](<repo-root>/tests/support/hedge/TERMINOLOGY.md) to distinguish the hedge live probe from the new merge live probe
- Validation:
  - `cargo test live_probe_ -- --nocapture`
  - `cargo test harness_merge_pairs_ -- --nocapture`
  - `cargo test layer1_merge_ -- --nocapture`
- No automatic production bot behavior changed in this increment

## 2026-04-09 — Added Layer 2 replay coverage for the sellback-miss recompute branch

- Added [raw_trade_sellback_miss_recompute_switches_to_hedge.json](<repo-root>/fixtures/hedge_replay_scenarios/raw_trade_sellback_miss_recompute_switches_to_hedge.json) to prove, end-to-end in the deterministic replay harness, that a BUY-resolution raw-trade hedge path can:
  - place the first sellback at the real computed `sellback_limit_price` (`SELL FOK @ 0.73`)
  - miss that first sellback
  - sync authoritative residual truth
  - recompute exactly once
  - switch to a `BUY GTC` hedge (`0.27`) on the residual
- Added [raw_trade_sellback_miss_recompute_fails_closed.json](<repo-root>/fixtures/hedge_replay_scenarios/raw_trade_sellback_miss_recompute_fails_closed.json) to prove the complementary bounded failure path:
  - two real-price sellback attempts at `0.73`
  - explicit fail-closed result after the one allowed recompute
  - existing downstream kill/flatten safety sell at `0.01` still occurs afterward, confirming the recompute is bounded even though the safety path emits its own flatten order
- Extended [layer2.rs](<repo-root>/tests/support/hedge/layer2.rs) with:
  - `layer2_sellback_miss_recompute_switches_to_hedge`
  - `layer2_sellback_miss_recompute_fails_closed_after_one_retry`
- The new Layer 2 assertions verify:
  - first sellback request uses the real planned limit, not `0.01`
  - success path switches to a recomputed `BUY GTC` hedge order
  - failure path is bounded to one recompute before the existing safety flatten behavior takes over
- Validation:
  - `cargo test layer2_sellback_miss_recompute_ -- --nocapture`
  - `cargo test layer2_ -- --nocapture`
  - `cargo test sellback_ -- --nocapture`
  - `cargo test --workspace`
- No production/runtime behavior changed in this increment
- Reviewed against [STRATEGY.md](<repo-root>/STRATEGY.md): no update required because this was replay coverage only

## 2026-04-09 — BUY-resolution sellbacks now execute at the real planned limit with one bounded recompute

- Changed [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) so BUY-resolution sellbacks no longer hardcode `FOK @ $0.01`; `execute_sellback_order(...)` now takes the planner-computed `sellback_limit_price`
- Kept the low-level single-attempt resolution primitive intact and added a narrow `LiveEngine` wrapper that:
  - runs the first resolution attempt exactly as before
  - only for BUY-side resolutions with a missed/unverified sellback, does one extra authoritative position sync
  - exits successfully from current truth if residual exposure is already within `hedge_exposure_tolerance`
  - otherwise runs one fresh `prepare_market_for_resolution(...)` + `plan_fill_resolution(...)` recompute on the residual position and executes that second plan once
  - fails closed after that second attempt if exposure/truth is still unresolved
- Added modular helpers for:
  - building the sellback `FOK` request from an explicit limit
  - planning BUY-side resolutions from fresh books
  - deriving planned hedge size / cost for repeated risk checks
  - detecting the narrow BUY-resolution sellback-miss recompute trigger
- Left the legacy SELL-hedge `FOK @ $0.01` path unchanged in [hedge_executor.rs](<repo-root>/src/trading/hedge_executor.rs)
- Updated [STRATEGY.md](<repo-root>/STRATEGY.md) so the documented strategy now matches the code:
  - BUY-resolution sellbacks use the real computed `sellback_limit_price` with one bounded recompute on miss
  - legacy SELL hedges still use `FOK @ $0.01`
- Aligned Layer 3 in [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs) with the new production proof path:
  - when production emits `post_sync_source="execution_confirmed_sellback"` together with `exit_path_status="sellback_complete"` and neutral internal post-sync inventory before cleanup, Layer 3 now treats that narrow source as `confirmed_before_cleanup`
  - the existing positions-lag warning path then applies normally if the post-decision funded-wallet snapshot is still briefly one-sided
- Added runtime regressions for:
  - explicit sellback request pricing
  - the BUY-resolution sellback recompute gate
  - success from authoritative current truth without a second order
  - one bounded recompute into a hedge-success path
  - fail-closed behavior after a second miss with no third retry
- Validation:
  - `cargo fmt`
  - `cargo test sellback_ -- --nocapture`
  - `cargo test layer2_ -- --nocapture`
  - `cargo test live_probe_ -- --nocapture`
  - `cargo test reconcile_production_truth_ -- --nocapture`
  - `cargo test --workspace`
  - `powershell -ExecutionPolicy Bypass -File <repo-root>/scripts/run_hedge_live_probe.ps1 -Scenario scotus_mail_ballots_yes_bid_probe_under_10`
- Final armed live result on April 9, 2026:
  - `meta_pass=true`
  - `standard_pass=true`
  - `cleanup_pass=true`
  - `flow_status=sellback_completed`
  - `production_exit_path_status=sellback_complete`
  - `truth_reconciliation_status=confirmed`
  - `truth_reconciliation_warning_status=positions_lag_after_confirmed_execution`
  - `production_sellback_confirmation_status=confirmed_before_cleanup`
  - cleanup returned the wallet flat; no `kill_flatten.py` run was needed

## 2026-04-09 — Review hardening closed runtime/harness proof gaps before merge

- Fixed a production-path parsing bug in [client.rs](<repo-root>/src/trading/client.rs) so authenticated order lookups now preserve `ORDER_STATUS_DELAYED` / `delayed` instead of collapsing them into `Invalid`
- Replaced the temporary global associated-trade-ID registry with a real additive `associated_trade_ids` field on [LiveOrder](<repo-root>/src/models/order.rs), eliminating unbounded ambient state on the production path
- Tightened Layer 3 sellback confirmation in [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs):
  - no longer upgraded `confirmed_before_cleanup` from `post_sync_source=execution_confirmed_sellback` alone during that hardening pass
  - no longer treats any positive matched shares as full confirmation
  - now requires independent full-fill proof or already-flat post-decision funded-wallet truth
- Strengthened the ignored armed live-money assertion so `live_probe_armed_runs_current_production_hedge_path` now requires `cleanup_pass=true` in addition to `meta_pass` and `standard_pass`
- Added regressions for:
  - delayed order parsing in `TradingClient`
  - delayed sellback lookup remaining `unverified`
  - execution-confirmed source alone not self-confirming Layer 3
  - partial lookup matched shares remaining unconfirmed in Layer 3
- Validation:
  - `cargo test live_probe_ -- --nocapture`
  - `cargo test sellback_ -- --nocapture`
  - `cargo test --workspace`
  - `powershell -ExecutionPolicy Bypass -File scripts/run_hedge_live_probe.ps1 -Scenario scotus_mail_ballots_yes_bid_probe_under_10`
- Latest armed SCOTUS rerun finished green with stronger evidence than the earlier warning-path pass:
  - `meta_pass=true`
  - `standard_pass=true`
  - `cleanup_pass=true`
  - `truth_reconciliation_status=confirmed`
  - `truth_reconciliation_warning_status=null`
  - post-decision direct funded-wallet truth was already flat for `9s` before cleanup
  - `production_sellback_confirmation_status=confirmed_before_cleanup` now came from independent flat-wallet confirmation, not production self-report alone

## 2026-04-09 — Execution-confirmed sellback completion can now finish sellback-only runs without positions sync

- Added a narrow production completion path in [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) for sellback-only resolutions whose positions sync is still missing after the current sync/retry budget
- Sellback verification now carries `confirmed_shares`, and full completion for this path requires:
  - terminal placement `Matched`, or placement `trade_ids`, or authenticated order lookup showing full fill
  - derived residual exposure from `pre_resolution_position - confirmed_sellback_shares` within `hedge_exposure_tolerance`
- Added the new authoritative completion source `post_sync_source="execution_confirmed_sellback"`
- Kept BUY-hedge missing-truth behavior unchanged and did not add any new endpoints or user-stream wiring
- Extended the existing order client/model path in [order.rs](<repo-root>/src/models/order.rs) and [client.rs](<repo-root>/src/trading/client.rs) so authenticated order lookups preserve `associate_trades` / `associated_trade_ids` without changing existing API call patterns
- Updated Layer 3 in [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs) and [TERMINOLOGY.md](<repo-root>/tests/support/hedge/TERMINOLOGY.md) so `execution_confirmed_sellback` is treated as a production-authoritative completion source for the narrow sellback-only case, upgrades the pre-cleanup sellback indicator to `confirmed_before_cleanup`, and downgrades short one-sided positions lag to the existing warning path instead of failing the run
- Updated the sellback-cheaper Layer 2 replay fixtures:
  - [raw_trade_sellback_cheaper.json](<repo-root>/fixtures/hedge_replay_scenarios/raw_trade_sellback_cheaper.json)
  - [exchange_sync_missing_fill_sellback_cheaper.json](<repo-root>/fixtures/hedge_replay_scenarios/exchange_sync_missing_fill_sellback_cheaper.json)
- Validation:
  - `cargo fmt`
  - `cargo test sellback_ -- --nocapture`
  - `cargo test reconcile_production_truth_ -- --nocapture`
  - `cargo test fill_handler_fails_closed_when_post_sync_truth_remains_missing -- --nocapture`
  - `cargo test should_retry_resolution_sync -- --nocapture`
  - `cargo test layer2_ -- --nocapture`
  - `cargo test --workspace`
  - `powershell -ExecutionPolicy Bypass -File scripts/run_hedge_live_probe.ps1 -Scenario scotus_mail_ballots_yes_bid_probe_under_10`
- Latest armed SCOTUS rerun finished green with:
  - `meta_pass=true`
  - `standard_pass=true`
  - `cleanup_pass=true`
  - `flow_status=sellback_completed`
  - `production_exit_path_status=sellback_complete`
  - `truth_reconciliation_status=confirmed`
  - `truth_reconciliation_warning_status=positions_lag_after_confirmed_execution`
  - `production_sellback_confirmation_status=confirmed_before_cleanup`
  - direct post-decision wallet truth still briefly showed `YES=5.007`, `NO=0` for 8 seconds before cleanup, which is now classified as lagging positions truth rather than a production sellback failure

## 2026-04-08 — Restored fail-closed runtime semantics for missing post-sync truth

- Narrowed the earlier rollback in [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) so production no longer synthesizes flatness from `post_position=None`
- Restored the explicit fail-closed runtime contract:
  - `success=false`
  - `post_sync_net_exposure=Decimal::MAX`
  - failure reason `final post-sync position truth missing after current sync/retry flow`
- Removed the broad synthetic-zero helper path again and stopped downstream neutrality/baseline updates from running off missing truth
- Restored BUY-side retry-on-missing-truth in `should_retry_resolution_sync(...)`
- Closed the remaining reconciliation-side leak by preventing `emit_reconciliation_hedge_exit(...)` from re-reading cached position state when `result.post_position` is missing
- Replaced the stale-cache reconciliation test with a regression that proves no `hedge_exit_path_recorded` is emitted from cached state after a missing-truth failure
- Tightened the sellback-cheaper Layer 2 replay fixtures so they now assert the restored explicit missing-truth failure reason, not just generic failure status
- Validation:
  - `cargo fmt`
  - `cargo test sellback_ -- --nocapture`
  - `cargo test reconcile_production_truth_ -- --nocapture`
  - `cargo test layer2_ -- --nocapture`
  - `cargo test --workspace`
  - `powershell -ExecutionPolicy Bypass -File scripts/run_hedge_live_probe.ps1 -Scenario scotus_mail_ballots_yes_bid_probe_under_10`
- Latest armed SCOTUS rerun from [events.jsonl](<repo-root>/data/events/run_20260409_035510/events.jsonl) confirmed the restored fail-closed production behavior:
  - `hedge_result_recorded.result_status="failed"`
  - `post_sync_source="first_sync"`
  - `post_sync_yes_size=null`
  - `post_sync_no_size=null`
  - no `hedge_exit_path_recorded`
  - Layer 3 therefore reported `truth_reconciliation_status=event_missing` and `standard_pass=false`
  - cleanup still returned flat, so no `kill_flatten.py` run was needed

## 2026-04-08 — Layer 3 now treats confirmed sellback execution as pass-with-warning when positions truth lags

- Updated [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs) so Layer 3 computes a dedicated pre-cleanup sellback confirmation verdict:
  - `production_sellback_confirmation_status`
  - `production_sellback_confirmation_reason`
- Added a harness-only sellback confirmation helper that prefers:
  - production sellback `trade_ids`
  - production sellback lookup status / matched shares
  - bounded authenticated `get_order(sellback_order_id)` plus open-order fallback
- Added a final pre-cleanup direct-truth upgrade so an already-flat post-decision funded-wallet snapshot upgrades the sellback indicator to `confirmed_before_cleanup`
- Explicitly does **not** treat `sellback_response_status` alone as sufficient proof that production sold before cleanup
- Reworked truth reconciliation so the narrow case
  - `sellback_complete`
  - neutral internal post-sync exposure
  - one-sided post-decision direct truth
  - no paired inventory
  - confirmed pre-cleanup sellback execution
  is now `truth_reconciliation_status=confirmed` with:
  - `truth_reconciliation_warning_status=positions_lag_after_confirmed_execution`
  - `truth_reconciliation_warning_reason=...`
  instead of a hard failure
- Normalized `flow_status` to `sellback_completed` for that same narrow confirmed-execution positions-lag case so `standard_pass` can remain true
- Kept hard failures unchanged for:
  - unverified sellbacks
  - zero-fill sellbacks
  - missing required `hedge_exit_path_recorded`
  - paired inventory contradictions
  - merge/fallback contradictions
- Updated [TERMINOLOGY.md](<repo-root>/tests/support/hedge/TERMINOLOGY.md) to document the new warning semantics and the explicit “did production sell before cleanup?” indicator
- Validation:
  - `cargo fmt`
  - `cargo test reconcile_production_truth_ -- --nocapture`
  - `cargo test resolve_sellback_confirmation_ -- --nocapture`
  - `cargo test classify_flow_status_ -- --nocapture`
  - `cargo test evaluate_standard_pass_ -- --nocapture`
  - `cargo test live_probe_ -- --nocapture`
  - `cargo test --workspace`
  - `powershell -ExecutionPolicy Bypass -File scripts/run_hedge_live_probe.ps1 -Scenario scotus_mail_ballots_yes_bid_probe_under_10`
- Final armed rerun result:
  - `meta_pass=true`
  - `standard_pass=true`
  - `cleanup_pass=true`
  - `flow_status=sellback_completed`
  - `truth_reconciliation_status=confirmed`
  - `production_sellback_confirmation_status=confirmed_before_cleanup`
  because the final rerun's post-decision direct funded-wallet snapshot was already flat before cleanup started, which confirmed production had resolved the inventory without needing cleanup to do it
- Confidence the root issue is now correctly isolated as Layer 3 truth-weighting rather than production sellback behavior: **high**
- Confidence this harness-only change fixes the live contradiction while adding zero performance overhead to the production trading bot: **high**

## 2026-04-08 — Surgical rollback of the mistaken production truth-path changes

- Backed out the suspect production truth-path changes in [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) after authenticated exchange-order verification showed the latest armed live run’s trigger BUY and sellback SELL were both fully matched
- Removed the temporary production `FlatNoRow` / `retry_sync_no_row_flat` path and restored the earlier success-path zero materialization for authoritative sync sources
- Kept the changes that still look correct and useful:
  - `tie_prefers_sellback` planner alignment in [hedge_executor.rs](<repo-root>/src/trading/hedge_executor.rs)
  - additive sellback verification evidence in [hedge.rs](<repo-root>/crates/spreadeater-core/src/payloads/hedge.rs), [emitters.rs](<repo-root>/src/monitor/emitters.rs), and their tests
  - Layer 3 repo event-log retention fanout in [support.rs](<repo-root>/tests/support/hedge/support.rs)
- Reverted the sellback-cheaper replay fixtures in:
  - [raw_trade_sellback_cheaper.json](<repo-root>/fixtures/hedge_replay_scenarios/raw_trade_sellback_cheaper.json)
  - [exchange_sync_missing_fill_sellback_cheaper.json](<repo-root>/fixtures/hedge_replay_scenarios/exchange_sync_missing_fill_sellback_cheaper.json)
  so Layer 2 expectations match the restored production semantics
- Backed [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs) away from the temporary “pre-exit missing-truth” failure wording that only existed to support the discarded production path
- Replaced one invalid full-flow fill-handler test in [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) with helper-level coverage for `materialize_authoritative_zero_post_position(...)`
  because the kept sellback-verification logic still correctly makes the dry-run sellback/no-lookup path fail closed as `unverified`
- Validation:
  - `cargo fmt`
  - `cargo test reconcile_production_truth_ -- --nocapture`
  - `cargo test layer2_ -- --nocapture`
  - `cargo test fill_handler_ -- --nocapture`
  - `cargo test materialize_authoritative_zero_post_position -- --nocapture`
  - `cargo test --workspace`
- Confidence the rollback target is correct now that authenticated order evidence shows prod sellback was successful: **high**
- Confidence the remaining issue focus should move to Layer 3 truth weighting rather than more production hedge-behavior changes: **high**

## 2026-04-08 — Added bounded sellback-only no-row retry/flat classification and validated the live contradiction

- Updated [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) so post-resolution sync now classifies truth explicitly as either:
  - `Position(Position)`
  - `FlatNoRow`
- Changed sellback-only post-resolution confirmation:
  - preserved the existing BUY-hedge retry path
  - added one bounded retry when the first successful post-sync result after a sellback-only resolution is `FlatNoRow`
  - only materialized an ephemeral zero position for telemetry/exit classification when:
    - no hedge BUY leg executed
    - a sellback leg executed
    - sellback verification is `VerifiedFilled`
    - first sync returned `FlatNoRow`
    - retry sync also returned `FlatNoRow`
- Kept the broad guardrails:
  - BUY-hedge paths still do not treat `FlatNoRow` as authoritative flat
  - unverified / zero-fill sellbacks still fail closed
  - `PositionManager` stored-state semantics were not changed globally
- Tightened failure wording in [live_engine.rs](<repo-root>/src/runtime/live_engine.rs):
  - successful sync + no market row outside the narrow verified sellback path now reports `positions API returned no market row after post-resolution sync/retry flow`
- Updated [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs) so Layer 3 recognizes the new explicit no-row failure wording while continuing to fail production/direct-truth contradictions
- Added/updated runtime coverage in [live_engine.rs](<repo-root>/src/runtime/live_engine.rs):
  - verified sellback `FlatNoRow -> FlatNoRow` retry success emits `retry_sync_no_row_flat`
  - verified sellback `FlatNoRow -> Position` uses the returned position
  - verified sellback `FlatNoRow -> Err` fails closed
  - BUY-hedge `FlatNoRow` still fails closed
  - truth-classification helper coverage
- Updated Layer 2 replay fixture expectations in:
  - [raw_trade_sellback_cheaper.json](<repo-root>/fixtures/hedge_replay_scenarios/raw_trade_sellback_cheaper.json)
  - [exchange_sync_missing_fill_sellback_cheaper.json](<repo-root>/fixtures/hedge_replay_scenarios/exchange_sync_missing_fill_sellback_cheaper.json)
  because the narrow verified-sellback no-row path now resolves those scenarios successfully
- Validation:
  - `cargo fmt`
  - `cargo test resolution_truth_ -- --nocapture`
  - `cargo test finalize_resolution_post_sync -- --nocapture`
  - `cargo test sync_position_for_resolution_ -- --nocapture`
  - `cargo test reconcile_production_truth_ -- --nocapture`
  - `cargo test reconciliation_exit_ -- --nocapture`
  - `cargo test fill_handler_fails_closed_when_post_sync_truth_remains_missing -- --nocapture`
  - `cargo test fill_handler_dry_run_sellback_only_missing_truth_halts_without_exit_event -- --nocapture`
  - `cargo test layer2_ -- --nocapture`
  - `cargo test --workspace`
  - `powershell -ExecutionPolicy Bypass -File scripts/run_hedge_live_probe.ps1 -Scenario scotus_mail_ballots_yes_bid_probe_under_10`
- Armed live result on April 8, 2026:
  - repo log retained at [events.jsonl](<repo-root>/data/events/run_20260409_001734/events.jsonl)
  - production emitted `hedge_result_recorded` with `post_sync_source=retry_sync_no_row_flat`, `post_sync_yes_size=0`, `post_sync_no_size=0`
  - production emitted `hedge_exit_path_recorded` with `exit_path_status=sellback_complete`
  - Layer 3 still failed the run because direct funded-wallet truth observed `YES=5.007`, `NO=0` for `8s` before cleanup
  - cleanup returned flat successfully, so no `kill_flatten.py` step was required
- Confidence the implemented code matches the requested narrow runtime plan: **high**
- Confidence the live rerun shows `retry_sync_no_row_flat` is not yet reliable enough to represent authoritative flat production truth: **high**

## 2026-04-07 — Fixed Layer 3 repo event log retention for armed live probes

- Root cause: the armed Layer 3 harness in [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs) created a normal repo-backed event writer through [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) and then immediately overwrote `engine.event_producer` with an in-memory collector, leaving the repo run directory created but its `events.jsonl` empty for the exact live run under investigation
- Added test-only [FanoutEventProducer](<repo-root>/tests/support/hedge/support.rs) so the armed live probe now fans runtime events to both:
  - the existing repo file producer
  - the existing in-memory collector used by Layer 3 assertions
- Updated [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs) to preserve the original repo producer instead of discarding it
- Added regression coverage in [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs):
  - `live_probe_event_fanout_preserves_repo_event_logs`
- Validation:
  - `cargo fmt`
  - `cargo test --bin spreadeater live_probe_event_fanout_preserves_repo_event_logs -- --nocapture`
  - `powershell -ExecutionPolicy Bypass -File scripts/run_hedge_live_probe.ps1 -Scenario scotus_mail_ballots_yes_bid_probe_under_10`
- Live verification result:
  - previous empty repo log `data/events/run_20260408_025926/events.jsonl` remained `0` bytes
  - new armed rerun retained [events.jsonl](<repo-root>/data/events/run_20260408_034030/events.jsonl) at `6613` bytes
  - rerun still failed functionally with `standard_pass=false`, but cleanup returned the funded wallet flat (`cleanup_pass=true`), so no extra `kill_flatten.py` step was required
- New retained-log findings:
  - `hedge_result_recorded` confirms `post_sync_source=first_sync`
  - `latency_ms=351`
  - `sellback_response_status=matched`
  - `sellback_order_id` present
  - no `hedge_exit_path_recorded` event was emitted for the run
- Confidence the empty repo log root cause was harness-side producer replacement, not writer failure: **high**
- Confidence the retained log strengthens the remaining production diagnosis toward sellback-only first-sync truth failure with no retry: **high**

## 2026-04-07 — Tightened sellback completion verification and surfaced sellback evidence

- Updated [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) so sellback execution now uses explicit verification states:
  - `VerifiedFilled`
  - `VerifiedZeroFill`
  - `Unknown`
- Changed sellback success semantics:
  - placement `Matched` or non-empty `trade_ids` => verified success
  - placement `Invalid` => verified zero fill / failed
  - placement `Live` or `Delayed` => provisional only
  - provisional responses now trigger one immediate `get_order(order_id)` lookup when `order_id` exists
  - lookup `matched` or positive `size_matched` => verified success
  - lookup `cancelled` / `invalid` with zero matched => verified zero fill / failed
  - lookup `live`, `delayed`, `missing`, lookup error, or provisional response without `order_id` => fail closed as `unverified`
- Aggregate resolution success in [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) now requires verified sellback completion in addition to neutral post-sync exposure; ambiguous sellback completion no longer rides through as success
- Extended [hedge.rs](<repo-root>/crates/spreadeater-core/src/payloads/hedge.rs) and [emitters.rs](<repo-root>/src/monitor/emitters.rs) with additive sellback verification evidence fields on `HedgeResultRecorded`:
  - `sellback_response_status`
  - `sellback_lookup_status`
  - `sellback_lookup_matched_shares`
  - `sellback_lookup_error`
  - `sellback_trade_ids`
- Updated [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs) so Layer 3 surfaces sellback verification evidence directly and reports sellback `unverified` / verified-zero-fill as the primary production failure reason
- Refreshed touched regressions in:
  - [live_engine.rs](<repo-root>/src/runtime/live_engine.rs)
  - [payload_tests.rs](<repo-root>/tests/unit/core/payload_tests.rs)
  - [postgres_integration.rs](<repo-root>/crates/spreadeater-monitor/tests/postgres_integration.rs)
- Validation:
  - `cargo fmt`
  - `cargo test sellback_ -- --nocapture`
  - `cargo test reconcile_production_truth_ -- --nocapture`
  - `cargo test --workspace`
- Confidence the current main bug was sellback success being inferred from placement-only evidence: **high**
- Confidence this bounded verify-and-fail-closed increment is the right first production fix: **high**

## 2026-04-07 — Fixed false-neutral hedge truth path and aligned BUY ties to sellback

- Updated [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) so successful hedge resolution no longer fabricates a zero `post_position` when final synced position truth is missing
- Reused the existing single retry sync budget for BUY-side hedges with `VerifiedFilled` / `Unknown` evidence and missing first post-sync truth, then fail closed if truth is still missing:
  - `success=false`
  - `post_sync_net_exposure=Decimal::MAX`
  - explicit failure reason `final post-sync position truth missing after current sync/retry flow`
  - existing market halt path preserved
- Removed operational synthetic-zero influence from:
  - reconciliation baseline updates
  - neutrality emission
  - hedge-signal insertion
  - exit classification
- Updated [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs) so failed traces with this reason report explicit truth-reconciliation failure instead of the old generic missing-exit fallback
- Changed BUY-side tie policy in [hedge_executor.rs](<repo-root>/src/trading/hedge_executor.rs) from `hedge_cost <= sellback_cost` to `hedge_cost < sellback_cost`, so exact ties now prefer sellback per [STRATEGY.md](<repo-root>/STRATEGY.md)
- Updated [emitters.rs](<repo-root>/src/monitor/emitters.rs) to emit `tie_prefers_sellback`, and updated [TERMINOLOGY.md](<repo-root>/tests/support/hedge/TERMINOLOGY.md) to match the canonical decision vocabulary
- Refreshed affected runtime and replay/live-probe regressions, including the Layer 2 sellback fixtures under:
  - [exchange_sync_missing_fill_sellback_cheaper.json](<repo-root>/fixtures/hedge_replay_scenarios/exchange_sync_missing_fill_sellback_cheaper.json)
  - [raw_trade_sellback_cheaper.json](<repo-root>/fixtures/hedge_replay_scenarios/raw_trade_sellback_cheaper.json)
- Replaced an invalid dry-run late-sync expectation in [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) with a deterministic regression asserting the current dry-run sellback-only missing-truth halt path
- Validation:
  - targeted runtime/planner/live-probe tests for missing-truth fail-closed behavior and tie-to-sellback planner semantics
  - `cargo fmt --all`
  - `cargo test --workspace`
  - `powershell -ExecutionPolicy Bypass -File scripts/run_hedge_live_probe.ps1 -Scenario scotus_mail_ballots_yes_bid_probe_under_10`
- Live result after this pass:
  - `meta_pass=true`
  - `decision_audit_status=confirmed`
  - `production_decision_reason_code=tie_prefers_sellback`
  - `planned_hedge_shares=0`
  - `planned_sellback_shares=5`
  - `cleanup_pass=true`
  - remaining failure: `truth_reconciliation_status=failed` because production failed closed on `final post-sync position truth missing after current sync/retry flow` before exit classification
  - `production_exit_path_status=null`
  - cleanup returned the wallet to flat on the scenario condition
  - no Layer 3 observability gap was implicated in the remaining failure
  - Confidence the decision contradiction is resolved: **high**
  - Confidence the remaining blocker is now explicit post-sellback truth resolution in production code: **high**

## 2026-04-07 — Documented deferred STRATEGY.md section 4 misalignments

- Added [STRATEGY-MD-MISALIGNMENTS-2026-04-07.md](<repo-root>/retired strategy-alignment note) to record two known section 4 follow-ups that are intentionally being deferred rather than fixed immediately
- Documented finding `#2`: the planner computes `sellback_limit_price`, but live sellback execution still routes through a hardcoded `0.01` FOK path instead of honoring the planned depth-aware sellback price
- Documented finding `#3`: the current 10-second hedge timeout starts too late and does not wrap the full fill-to-finish lifecycle described in section 4
- Recorded the current assessment that finding `#2` is the larger future fix because the likely correct implementation is a staged execution-policy change: bounded sellback attempt at the planned price, bounded re-evaluation of hedge-vs-sell under refreshed books, and only then an explicit terminal `0.01` fallback if desired
- Validation: documentation-only change; no code changes or runtime tests were run
- Confidence the note accurately captures the current section 4 gaps: **high**

## 2026-04-06 — Fixed PR #31 Linux CI harness module path

- Replaced the fragile inline-test `#[path = "<repo-root>/../tests/support/hedge/mod.rs"]` declaration in [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) with `include!(concat!(env!("CARGO_MANIFEST_DIR"), ...))`-based loading for the hedge harness support files
- Kept the fix strictly under `#[cfg(test)]`; no production runtime modules, strategy code, network behavior, or monitor emission paths changed
- Root cause: GitHub Actions on Linux resolved the old `#[path]` relative to the synthetic inline-module base `src/runtime/live_engine/tests/`, which does not exist in the repo tree, causing `couldn't read ... tests/support/hedge/mod.rs`
- Validation:
  - `cargo test --lib --no-run`
  - `cargo test processed_trade_cache_prunes_expired_entries -- --nocapture`
  - `cargo test --workspace`

## 2026-04-06 — Surfaced production hedge verification evidence into Layer 3

- Updated [hedge_executor.rs](<repo-root>/src/trading/hedge_executor.rs) so the existing BUY-side hedge verification path retains the monitor-only facts it already observes:
  - cancel outcome (`confirmed`, `rejected`, `unknown`)
  - cancel reason text
  - post-cancel `get_order` status (`matched`, `live`, `cancelled`, `invalid`, `missing`, `error`)
  - post-cancel matched shares
  - post-cancel lookup error text
  - placement `trade_ids`
- Extended [hedge.rs](<repo-root>/crates/spreadeater-core/src/payloads/hedge.rs) `HedgeResultPayload` with additive optional verification fields:
  - `hedge_cancel_status`
  - `hedge_cancel_reason`
  - `hedge_lookup_status`
  - `hedge_lookup_matched_shares`
  - `hedge_lookup_error`
  - `hedge_trade_ids`
- Updated [emitters.rs](<repo-root>/src/monitor/emitters.rs) to serialize those fields onto the existing `hedge_result_recorded` event without adding a new event type or schema bump
- Updated [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs) so Layer 3:
  - exposes `production_hedge_cancel_status`
  - exposes `production_hedge_cancel_reason`
  - exposes `production_hedge_lookup_status`
  - exposes `production_hedge_lookup_matched_shares`
  - exposes `production_hedge_lookup_error`
  - exposes `production_hedge_trade_ids`
  - prefers production hedge verification evidence before bounded external fallback
  - replaces the old generic `lookup_unavailable` path with more specific production-led verdicts where possible
- Updated [TERMINOLOGY.md](<repo-root>/tests/support/hedge/TERMINOLOGY.md) and [layer3-meta-pass-observability-report-2026-04-01.md](<repo-root>/agents/archive/hedge-test/layer3-meta-pass-observability-report-2026-04-01.md) to document the new production-vs-harness hedge verification distinction
- Validation:
  - `cargo test hedge_result_payload_serde_roundtrip -- --nocapture`
  - `cargo test hedge_verification_metadata_ -- --nocapture`
  - `cargo test hedge_result_event_carries_verification_metadata -- --nocapture`
  - `cargo test resolve_hedge_verification_ -- --nocapture`
  - `cargo test --workspace`
  - `powershell -ExecutionPolicy Bypass -File scripts/run_hedge_live_probe.ps1 -Scenario scotus_mail_ballots_yes_bid_probe_under_10`
- Live result after this pass:
  - `meta_pass=true`
  - `hedge_verification_state=production_lookup_missing_after_cancel_unknown`
  - `production_hedge_cancel_status=rejected`
  - `production_hedge_cancel_reason="matched orders can't be canceled"`
  - `production_hedge_lookup_status=missing`
  - `standard_pass=false`
  - `cleanup_pass=false`

## 2026-04-06 — Fixed uptime-sensitive `Instant` underflow blocking Layer 3

- Updated [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) so processed-trade TTL pruning and recent-synthetic-fill pruning no longer compute cutoff instants via raw `Instant - Duration`
- Added internal safe age-check helpers in [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) and kept the existing runtime semantics unchanged:
  - processed trade ids still expire after `24h`
  - recent synthetic fills still expire after `15s`
  - zero-sized synthetic entries are still dropped
  - over-capacity processed-trade pruning still works
- Updated [order_manager.rs](<repo-root>/src/trading/order_manager.rs) so:
  - `cleanup_stale_cancels` no longer uses raw `Instant - 30s`
  - pending-cancel retry readiness now uses the same safe age-check pattern
- Reworked directly affected tests to remove touched raw `Instant::now() - Duration` fixtures:
  - processed-trade cache pruning now uses a tiny injected TTL
  - recent synthetic fill pruning has dedicated non-underflow coverage
  - recently-cancelled pruning has dedicated non-underflow coverage
  - pending-cancel retry test now uses a bounded real wait instead of subtracting `3s` from a fresh `Instant`
- Validation:
  - `cargo test processed_trade_cache_prunes_expired_entries -- --nocapture`
  - `cargo test recent_synthetic_fill_pruning_ -- --nocapture`
  - `cargo test prune_recently_cancelled_entries_ -- --nocapture`
  - `cargo test retry_pending_cancels_confirms_and_clears_dry_run_orders -- --nocapture`
  - `cargo test layer0_build_fill_work_item_ -- --nocapture`
  - `cargo test layer2_duplicate_trade_id_is_deduped_before_second_hedge -- --nocapture`
  - `cargo test layer2_raw_trade_ -- --nocapture`
  - `cargo test layer2_recently_cancelled_order_is_not_misattributed -- --nocapture`
  - `cargo test --workspace`
- Armed live rerun after the fix:
  - no `Instant` overflow crash
  - probe reached a real Layer 3 result on `scotus_mail_ballots_yes_bid_probe_under_10`
  - result was `meta_pass=true`, `standard_pass=false`, `cleanup_pass=false`
  - production still recorded `decision_mode=buy_side_resolution`, `reason_code=tie_prefers_hedge`, and `production_exit_path_status=no_exit_needed`
  - direct truth still contradicted production and cleanup ended with residual inventory
  - `hedge_verification_state=lookup_unavailable`, `hedge_lookup_status=missing`

## 2026-04-06 — Harness-only Layer 3 truth staging and hedge verification cleanup

- Updated [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs) so Layer 3 direct funded-wallet truth is now stage-specific and canonical:
  - added `post_decision_direct_yes_size`
  - added `post_decision_direct_no_size`
  - added `post_decision_direct_observed_for_secs`
  - added `cleanup_direct_yes_size`
  - added `cleanup_direct_no_size`
  - added `cleanup_direct_observed_for_secs`
- Reworked Layer 3 direct-truth messaging:
  - `truth_reconciliation_reason` now explicitly describes the `post_decision` snapshot
  - `cleanup_status` now explicitly describes the `cleanup` snapshot
  - reconciliation and cleanup now share one canonical YES/NO summarization path
- Replaced the old placeholder `hedge_verification_state` behavior in [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs):
  - raw `success` -> `verified_filled`
  - raw `failed` -> `verified_zero_fill`
  - raw `skipped` -> `skipped`
  - raw `unverified` now uses bounded external evidence to classify `external_fill_confirmed`, `resting_open`, `external_zero_fill`, `missing_order_id`, or `lookup_unavailable`
- Added additive Layer 3 lookup evidence fields:
  - `hedge_lookup_status`
  - `hedge_lookup_matched_shares`
- Updated [TERMINOLOGY.md](<repo-root>/tests/support/hedge/TERMINOLOGY.md) so the new Layer 3 result semantics and hedge verification vocabulary are part of the canonical hedge-harness documentation
- Replaced [layer3-meta-pass-observability-report-2026-04-01.md](<repo-root>/agents/archive/hedge-test/layer3-meta-pass-observability-report-2026-04-01.md) with the April 6, 2026 status:
  - harness-only reporting gaps closed/narrowed
  - fresh live confirmation currently blocked by a pre-existing `Instant` underflow path in [live_engine.rs](<repo-root>/src/runtime/live_engine.rs)
- Targeted validation completed:
  - `cargo test resolve_hedge_verification_ -- --nocapture`
  - `cargo test reconcile_production_truth_ -- --nocapture`
  - `cargo test build_cleanup_status_ -- --nocapture`
  - `cargo test post_decision_and_cleanup_snapshots_ -- --nocapture`
- Validation blockers:
  - `cargo test --workspace` currently fails in existing `live_engine.rs` tests with `overflow when subtracting duration from instant`
  - the armed SCOTUS live rerun is blocked by that same pre-existing underflow before reaching a new Layer 3 verdict

## 2026-04-01 — Closed successful-trace `hedge_exit_path_recorded` gap

- Updated [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) so successful hedge traces no longer depend only on the initial `result.post_position` for exit telemetry.
- Fill-handler path now:
  - rebuilds baseline exit telemetry after the existing late sync if the initial post-sync position was missing
  - refreshes that baseline again after sell-back resync before merge/fallback follow-through
  - materializes a successful sync with no position entry as authoritative zero inventory for exit reporting
  - emits a linked observability defect only when successful exit truth is truly unrecoverable, instead of silently skipping `hedge_exit_path_recorded`
- Reconciliation path now routes exit emission through a shared helper that:
  - prefers `result.post_position`
  - falls back to the current cached position when the result position is missing
  - keeps fallback-attempt semantics honest, so cached fallback does not pretend asks were placed when production never attempted them
  - emits the same explicit observability defect on successful traces that still lack final post-sync truth
- Tightened Layer 3 in [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs):
  - successful `HedgeResultRecorded` traces missing `HedgeExitPathRecorded` now return `truth_reconciliation_status="failed"`
  - failed or legacy traces still keep `event_missing`
- Updated [TERMINOLOGY.md](<repo-root>/tests/support/hedge/TERMINOLOGY.md) so the successful-trace requirement for `hedge_exit_path_recorded` is part of the canonical vocabulary
- Added deterministic coverage for:
  - fill-handler late-sync backfill emitting exactly one exit event
  - successful zero-inventory exit emission after a sync with no position entry
  - reconciliation cached-position fallback emission
  - explicit observability failure when successful position truth is intentionally unrecoverable
  - Layer 3 missing-success-exit classification vs failed-trace legacy fallback
- Targeted validation completed:
  - `cargo test fill_handler_`
  - `cargo test reconciliation_exit_`
  - `cargo test reconcile_production_truth_`
- Full validation completed:
  - `cargo test --workspace`
- Live validation attempt on April 1, 2026:
  - armed `scotus_mail_ballots_yes_bid_probe_under_10` rerun is currently blocked before trigger placement because the direct baseline is not flat on funded wallet `<redacted-wallet>` for condition `<redacted-id>` (`YES=5.007056`, `NO=5`)

## 2026-04-01 — Added explicit hedge decision and exit-path observability for Layer 3

- Bumped the shared monitor schema to `V1_5` in [envelope.rs](<repo-root>/crates/spreadeater-core/src/envelope.rs) and added the new wire-level hedge lifecycle event types:
  - `hedge_decision_evaluated`
  - `hedge_exit_path_recorded`
- Added canonical payloads in [hedge.rs](<repo-root>/crates/spreadeater-core/src/payloads/hedge.rs) for:
  - production hedge decision inputs/split/reasoning
  - production post-sync exit-path follow-through, including merge/fallback status
- Extended [emitters.rs](<repo-root>/src/monitor/emitters.rs) and [live_engine.rs](<repo-root>/src/runtime/live_engine.rs) so both the fill-handler hedge path and reconciliation hedge path now emit:
  - `HedgeDecisionEvaluated` before hedge/sellback submission
  - `HedgeExitPathRecorded` after post-sync truth is known and the merge/fallback branch resolves
- Preserved the no-prod-logic rule for the trading bot:
  - no new strategy branches
  - no extra REST calls on the hot path
  - no extra websocket subscriptions
  - no extra blocking syncs
  - only two additional monitor event envelopes on hedge paths
- Updated monitor compatibility in:
  - [projector/mod.rs](<repo-root>/crates/spreadeater-monitor/src/projector/mod.rs)
  - [store.rs](<repo-root>/crates/spreadeater-monitor/src/store.rs)
  - [postgres_integration.rs](<repo-root>/crates/spreadeater-monitor/tests/postgres_integration.rs)
- The monitor stack now accepts, stores, normalizes, and serializes the new event types, and trace timelines now label them with readable statuses:
  - `hedge_decision`
  - `hedge_exit`
- Reworked [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs) so Layer 3 now prefers the new explicit production events over reverse inference and reports the additional observability fields:
  - `production_decision_mode`
  - `production_decision_reason_code`
  - `production_exit_path_status`
  - `merge_status`
  - `merge_failure_reason`
  - `fallback_status`
  - `fallback_failure_reason`
  - `truth_reconciliation_status`
  - `truth_reconciliation_reason`
- Layer 3 now keeps direct funded-wallet truth as the final external authority and explicitly reports internal-vs-direct mismatches instead of collapsing them into generic cleanup failures
- Updated the canonical hedge vocabulary note in [TERMINOLOGY.md](<repo-root>/tests/support/hedge/TERMINOLOGY.md) so the new event names and Layer 3 result fields are documented
- Validation:
  - `cargo test --workspace`
  - Result: full workspace passing; monitor Postgres integration cases compiled and remained ignored because they require a local Postgres instance

## 2026-04-01 — Tightened Layer 2/3 hedge decision auditing and split cleanup from strategy verdicts

- Extended [layer2.rs](<repo-root>/tests/support/hedge/layer2.rs) so the replay harness now proves `sellback vs hedge + split` behavior through all four current pre-attribution ingress families that can lead to hedging:
  - raw user-trade attribution with `sellback cheaper`
  - order-update fallback with `split resolution`
  - exchange-sync missing-fill recovery with `sellback cheaper`
  - orphan/reconciliation recovery with `split resolution`
- Added four new replay fixtures under `fixtures/hedge_replay_scenarios/` for those scenarios
- Upgraded Layer 2 assertions to inspect the mock exchange request log itself instead of trusting only emitted payloads:
  - sellback-only cases now prove no hedge BUY placement happened and exactly one sellback SELL placement happened
  - split cases now prove both the hedge BUY request and the sellback SELL request with the expected sizes and prices
- Extended [support.rs](<repo-root>/tests/support/hedge/support.rs) so mock request bodies are captured, enabling request-level assertions on signed order placements
- Reworked [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs) so Layer 3 now reports three separate judgments:
  - `meta_pass`: did the intended real production hedge path run
  - `standard_pass`: did the bot make the right decision and follow the intended production flow
  - `cleanup_pass`: did the probe return the market to its exact baseline
- Added explicit Layer 3 strategy-audit and follow-through fields:
  - `decision_audit_status`
  - `decision_audit_reason`
  - `planned_hedge_shares`
  - `planned_sellback_shares`
  - `hedge_leg_status`
  - `sellback_leg_status`
  - `flow_status`
  - `merge_observed`
  - `fallback_asks_observed`
  - `cleanup_status`
- Layer 3 now computes a best-effort live decision audit from pre-trigger and immediate post-trade book snapshots using the same planner math, marks ambiguity as `inconclusive` instead of inventing certainty, and only fails strategy on clear contradictions or missing required follow-through
- Added live-probe helper regressions for:
  - merge success with cleanup left separate
  - fallback-ask success with cleanup failure
  - idle paired inventory as a standard failure
  - cleanup independence from strategy success
  - inconclusive decision audits
  - clear planner contradictions
- Updated [TERMINOLOGY.md](<repo-root>/tests/support/hedge/TERMINOLOGY.md) so the documented hedge-harness vocabulary now matches the actual Layer 3 result schema and verdict semantics
- Validation:
  - `cargo test layer2_ -- --nocapture`
  - `cargo test evaluate_standard_pass_ -- --nocapture`
  - `cargo test evaluate_decision_audit_ -- --nocapture`
  - `cargo test classify_flow_status_ -- --nocapture`
  - `cargo test`

## 2026-04-01 — Added hedge harness terminology note

- Added [TERMINOLOGY.md](<repo-root>/tests/support/hedge/TERMINOLOGY.md) under the hedge harness directory to capture the current shared vocabulary for:
  - `meta-pass` / `meta-fail`
  - `standard-pass` / `standard-fail`
  - `cleanup-pass` / `cleanup-fail`
  - `precondition failure`
  - `harness failure`
- The note makes the currently important distinction explicit: `standard-pass` means the bot did the right thing for the production scenario, while `cleanup-pass` is a separate probe-hygiene question

## 2026-04-01 — Layer 3 cleanup verdict now requires stable direct funded-wallet truth

- Updated [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs) so Layer 3 no longer trusts a single post-cleanup position sample, even from the direct public positions API
- Added direct funded-wallet position aggregation helpers for the probe, including exact YES/NO per-condition truth summarization and exact baseline matching
- Changed the cleanup verdict contract so `standard_pass` now requires both:
  - neutral post-sync exposure, and
  - a stable direct funded-wallet return to the baseline over a bounded post-cleanup observation window
- Added fast harness tests for direct truth aggregation and exact baseline matching:
  - `cargo test --bin spreadeater summarize_direct_market_positions_aggregates_yes_and_no -- --nocapture`
  - `cargo test --bin spreadeater direct_market_position_truth_matches_requires_exact_baseline -- --nocapture`
- Important finding from live validation: the previous direct-one-sample cleanup check was still too weak. A live SCOTUS rerun printed `cleanup_status="clean"` but the funded wallet still held a mergeable pair immediately afterward (`YES 5.007056`, `NO 5`), proving that a momentary flat read can precede later positions truth. The stable-window check was added specifically to eliminate that false pass class going forward.

## 2026-04-01 — Tightened Layer 3 to 60s, added connect-only smoke, and isolated the remaining live blocker

- Reduced the manual hedge-harness timeout budget from `180s` to `60s` by changing the default in [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs) and the prepared SCOTUS scenario in [scotus_mail_ballots_yes_bid_probe_under_10.json](<repo-root>/fixtures/hedge_live_probe_scenarios/scotus_mail_ballots_yes_bid_probe_under_10.json)
- Added a connect-only ignored user-stream smoke test in [live_probe.rs](<repo-root>/tests/support/hedge/live_probe.rs) plus the operator wrapper [run_user_stream_smoke.ps1](<repo-root>/scripts/run_user_stream_smoke.ps1) so the user websocket can be checked without placing any orders
- Confirmed the previous Layer 3 deadlock was a harness issue, not a production-path issue: the raw user websocket transport connects and accepts the subscription send, but Polymarket can keep the socket silent for 60s on an idle authenticated stream, so the probe no longer waits for a pre-order `Connected` ACK before placing the trigger
- Replaced the harness-only `sync_market_open_orders(...ObserveOnly)` trigger seeding step with a `#[cfg(test)]` helper on [order_manager.rs](<repo-root>/src/trading/order_manager.rs), allowing Layer 3 to seed the freshly placed trigger order into `OrderManager` without relying on the market-filtered `/data/orders?market=...` path that returned `401`
- Validation and findings:
  - `cargo run --quiet -- auth-check` passes and reports `open_orders=0`
  - the connect-only smoke wrapper reaches the websocket and subscription send but times out with no first frame inside `60s`
  - the repaired live probe now gets past the websocket/auth gating and stops on the clean-baseline safety check with a real residual position on the proxy wallet: `yes_size=5.007056`, `no_size=0` for condition `<redacted-id>`
  - direct public position lookup confirms that inventory exists on `POLY_FUNDER` (`<redacted-wallet>`), so Layer 3 is currently blocked by live state, not by the harness

## 2026-04-01 — Prepared SCOTUS Layer 3 live probe under $10 and taught wrapper to load `.env`

- Added [scotus_mail_ballots_yes_bid_probe_under_10.json](<repo-root>/fixtures/hedge_live_probe_scenarios/scotus_mail_ballots_yes_bid_probe_under_10.json) for the live market `SCOTUS bars counting mail ballots after election day?` using the current market metadata and book state resolved on April 1, 2026
- Chose a 5-share `YesBid` trigger because the live book currently advertises `min_order_size=5`; set bounded safety caps so trigger + planned hedge + cleanup order notional stays under `$10`
- Updated [run_hedge_live_probe.ps1](<repo-root>/scripts/run_hedge_live_probe.ps1) so the operator wrapper automatically imports the repo `.env` before arming and invoking the ignored live-money probe test
- No live-money probe was executed in this pass; this change only prepares the scenario and operator entrypoint

## 2026-04-01 — Rebuilt hedge validation as a test-only harness on restored production code

- Added a new test-only hedge harness under `tests/support/hedge/` and loaded it from `src/runtime/live_engine.rs` only inside the existing `#[cfg(test)]` tree so offline hedge validation can hit current private `LiveEngine` methods without changing compiled production behavior
- Added shared harness support for scenario loading, mock exchange HTTP scripting, event capture, expected-vs-observed comparison, and scenario/work-item conversion in `tests/support/hedge/support.rs`
- Added Layer 0 private-path unit coverage for residual hedge sizing, duplicate trade-id suppression, and pending-fill fallback attribution in `tests/support/hedge/layer0.rs`
- Added Layer 1 post-attribution functional harness coverage that drives the real `FillHandler::handle_fill(...)` path against a mutable mock exchange in `tests/support/hedge/layer1.rs`
- Added Layer 2 pre-attribution/event-sequence replay coverage that exercises the current private `LiveEngine` fill-attribution, fallback, exchange-sync, orphan-recovery, and reconciliation paths in `tests/support/hedge/layer2.rs`
- Added an ignored manual Layer 3 live-money probe in `tests/support/hedge/live_probe.rs` plus the operator wrapper `scripts/run_hedge_live_probe.ps1`; this is the only true meta-pass path because the reverted production surface no longer exposes an offline full-engine replay seam
- Validation: `cargo test layer0_ -- --nocapture`, `cargo test layer1_ -- --nocapture`, `cargo test layer2_ -- --nocapture`, and `cargo test`

## 2026-04-01 — Reverted PR #27 production wiring while preserving harness artifacts for later triage

- Restored the pre-PR `#27` versions of the shared production/runtime files: `src/runtime/live_engine.rs`, `src/trading/client.rs`, `src/trading/user_stream.rs`, `src/config.rs`, `src/models/order.rs`, `src/trading/order_manager.rs`, `src/main.rs`, `src/runtime/mod.rs`, `src/trading/mod.rs`, `src/auth/order_signer.rs`, `src/trading/hedge_executor.rs`, and `src/watchdog/mod.rs`
- Left the harness source/tests/fixtures on disk as reference material, but removed the harness runtime/module/CLI wiring from the active build so the reverted production bot no longer carries the PR `#27` harness-facing `LiveEngine` surface area
- Reverted the PR `#27` unit-test edits that only existed to support the reverted model/API surface, and froze `tests/hedge_test_harness.rs`, `tests/hedge_replay_harness.rs`, and `tests/hedge_live_probe.rs` with `#![cfg(any())]` so the archived harness crates stay in-tree without blocking default CI on the restored production path
- Validation: `cargo test --bin spreadeater -- --nocapture` (`161/161` passing), then `cargo test` (full suite passing; archived harness crates compile as zero-test placeholders by design)

## 2026-03-29 — Shared trade parsing, Decimal-safe probe truth, and additive fee coverage

- Added `src/trading/trade_parser.rs` and moved REST trade lookup parsing plus user-stream trade parsing onto one shared normalizer for side parsing, case-insensitive status normalization, RFC3339 timestamp parsing, and final `TradeEvent` construction
- Cleaned up `src/runtime/hedge_live_probe.rs` by replacing the probe-truth `f64 -> Decimal` conversion path with Decimal-aware JSON string/number deserialization and by replacing the positional trigger-summary constructor with a named `TriggerSummaryParts` input
- Added additive non-zero-fee harness coverage in `tests/hedge_test_harness.rs`, `tests/hedge_replay_harness.rs`, and `tests/hedge_live_probe.rs` so the mock suites now exercise the real `/fee-rate` fetch/signing path with non-zero defaults and per-token overrides while keeping the existing zero-fee fixture defaults intact
- Clarified the harness fidelity contract in `HEDGE_TESTING_SUITE.md`: Layer 1 is production-faithful after attribution, Layer 2 is production-faithful before and through attribution, and this pass does not blur that boundary
- Validation: `cargo test --test hedge_test_harness -- --nocapture`, `cargo test --test hedge_replay_harness -- --nocapture`, `cargo test --test hedge_live_probe -- --nocapture`, `cargo test --bin spreadeater -- --nocapture`, and `cargo test`

## 2026-03-29 — Restored BUY FOK guard and moved Layer 3 trigger acquisition to exact GTC + cancel

- Restored the shared `TradingClient` validation that rejects share-sized BUY `FOK/FAK` requests; the live 5.00 → 5.07 overshoot showed the removed safety rail was guarding a real semantic mismatch, so the shared production validator now blocks that path again
- Reworked `src/runtime/hedge_live_probe.rs` so Layer 3 trigger acquisition no longer depends on BUY `FOK`: it now places an aggressive share-sized BUY `GTC`, lets matching user-stream trigger trades reach the engine immediately through the normal production path, and explicitly cancels any remainder after the bounded observation window
- Tightened final Layer 3 trigger semantics so trigger success requires the normalized cumulative matching user-stream trigger shares to equal the requested trigger size exactly; partial fills now report `trigger_partial_fill`, overshoots now report `trigger_overshoot`, and both remain standard failures rather than meta failures because the production-faithful path was still exercised
- Updated `tests/hedge_live_probe.rs` to use the new `GTC + cancel` trigger sequence and added regressions for exact multi-trade trigger accumulation and overshoot classification; restored the client regression that share-sized BUY `FOK` is rejected while share-sized BUY `GTC` remains allowed
- Validation: `cargo test --test hedge_live_probe -- --nocapture`, `cargo test --test hedge_test_harness -- --nocapture`, `cargo test --test hedge_replay_harness -- --nocapture`, `cargo test --bin spreadeater -- --nocapture`, and `cargo test`

## 2026-03-29 — Layer 3 production-mirror verdict semantics aligned and green

- Fixed the interrupted Layer 3 regression suite so it now asserts the production-mirror contract instead of the older probe-specific contract: no-trigger/no-trade paths are standard failures but not meta-failures, and partial-trigger paths may still observe a production `result_status=success` while failing the scenario on a stricter expected hedge bound
- `tests/hedge_live_probe.rs` no longer treats `trigger_no_fill` as a Layer 3 meta-failure and no longer forces `observed.result_status="failed"` when the production-faithful runtime still reports a successful hedge outcome with a separate expected-bound mismatch
- Revalidated the full hedge stack after the interrupted edit: `cargo test --test hedge_live_probe -- --nocapture` (`21/21`), `cargo test --test hedge_test_harness -- --nocapture` (`8/8`), `cargo test --test hedge_replay_harness -- --nocapture` (`9/9`), `cargo test --bin spreadeater -- --nocapture` (`165/165`), and `cargo test`
- Layer 3 now has a regression-green production-mirror mock contract before the next live SCOTUS rerun: the remaining uncertainty is live Polymarket delivery/timing, not an internally failing Layer 3 harness baseline

## 2026-03-29 — Layer 3 now starts hedging from the real user stream

- Reworked `src/runtime/hedge_live_probe.rs` so the live probe subscribes to the authenticated user websocket before placing the probe trigger, waits for a real `UserEvent::Trade(...)` on the trigger order, and only then drives the existing replay helpers; the Layer 3 hedge-start path is now `UserEvent::Trade(...) -> build_fill_work_item(...) -> handle_fill(...)`, matching the live bot far more closely
- Added configurable `discovery.user_ws_url` in `src/config.rs`, made `UserStream` URL-injectable in `src/trading/user_stream.rs`, and wired `LiveEngine` to use the configured URL while preserving the production default user-stream endpoint
- Extended the in-process mock exchange in `src/runtime/hedge_test.rs` with a mock user websocket that sends subscription ACKs, trade events, delayed events, duplicate trade events, and order-only events so Layer 3 can validate the actual user-stream parser/trigger path without spending money
- Demoted REST order/trade/position endpoints in Layer 3 to diagnostics, cleanup, and final-flatness verification only; if REST suggests a fill but the user stream never confirms it, the probe now fails conservatively and flattens instead of hedging from REST-derived truth
- Expanded `tests/hedge_live_probe.rs` with isolated no-money websocket trigger tests plus delayed-trade, duplicate-trade, order-only, no-fill, and partial-fill regressions; current Layer 3 mock-backed baseline is `20/20`
- Validation: `cargo check --quiet`, `cargo test --test hedge_live_probe -- --nocapture`, `cargo test --test hedge_test_harness -- --nocapture`, `cargo test --test hedge_replay_harness -- --nocapture`, `cargo test --bin spreadeater -- --nocapture`, and `cargo test`

## 2026-03-29 — Layer 3 now reuses the bot’s real trigger path

- Reworked `src/runtime/hedge_live_probe.rs` so the live probe no longer waits for `/positions` truth before hedging; it now acquires the trigger leg with a bounded share-sized `FOK` BUY, resolves actual execution evidence from live order/trade data, seeds the tracked trigger order, and replays a real `UserEvent::Trade(...)` through the existing replay/user-event helpers
- Stopped manually constructing `FillWorkItem` in Layer 3; `build_fill_work_item(...)` and the normal downstream hedge path now remain the entry boundary for the live probe, matching the real bot more closely
- Extended the live trading models/client so `OrderResult` retains `status`, `trade_ids`, `transaction_hashes`, `taking_amount`, and `making_amount`, and added `TradingClient::get_trades(...)` for resolving trigger execution into replayable trade events
- Added mock `/data/trades` support plus associated trade IDs in the harness exchange so Layer 3 and Layer 2 tests can exercise late trade-resolution behavior deterministically
- Replaced the old Layer 3 tests with a rewritten `tests/hedge_live_probe.rs` suite covering FOK trigger execution, late trade lookup, unresolved trigger fills, conservative flatten-on-failure behavior, merge cleanup, cleanup failure, and CLI non-zero behavior
- Validation: `cargo check --quiet`, `cargo test --test hedge_live_probe -- --nocapture`, `cargo test --test hedge_test_harness -- --nocapture`, `cargo test --test hedge_replay_harness -- --nocapture`, `cargo test --bin spreadeater -- --nocapture`, and `cargo test`

## 2026-03-29 — Layer 3 truth-gated paired probe

- Tightened `src/runtime/hedge_live_probe.rs` so probe-only direct position truth is used only to confirm whether the trigger really filled and whether cleanup truly ended flat; hedge sizing and hedge execution still require the engine-confirmed trigger path
- Trigger acquisition now tracks engine-confirmed shares and direct-truth shares separately, reports explicit failure categories like `trigger_filled_but_engine_unconfirmed` and `trigger_partial_fill`, and only builds a `FillWorkItem` when the engine view confirms the full requested trigger size
- Cleanup now verifies both engine-synced and direct-truth positions against the baseline before reporting `clean_end_state=true`; if direct truth shows leftover inventory, cleanup flattens from that direct inventory view instead of trusting the engine cache alone
- Ambiguous trigger fills now flatten and fail conservatively instead of hedging from probe-only truth or falsely reporting a clean end state
- Added Layer 3 regressions for engine/direct disagreement, trigger-filled-but-engine-unconfirmed flatten failure, direct-truth partial fills, stale clean snapshots, and dual-truth cleanup verification
- Validation: `cargo test --test hedge_live_probe -- --nocapture`, `cargo test --test hedge_test_harness -- --nocapture`, `cargo test --test hedge_replay_harness -- --nocapture`, `cargo test --bin spreadeater -- --nocapture`, and `cargo test`

## 2026-03-28 — Phase 3 Layer 3 paired live hedge probe

- Replaced the original `hedge-live-probe` synthetic-trigger semantics with a true paired live probe that first acquires a small real trigger-side position, derives a real `FillWorkItem` from the actual trigger order plus position delta, then runs the real downstream hedge path
- Changed the Layer 3 scenario contract from `trigger.work_item` to paired-probe `trigger { leg, shares, max_trigger_limit_price }`, retained bounded safety caps, and added explicit `max_trigger_notional_usdc`, `max_cleanup_notional_usdc`, `cleanup_status`, and `clean_end_state` expectations
- Added harness-only post-hedge controls in `src/runtime/live_engine.rs` so the paired probe can reuse the real hedge path while suppressing probe-unsafe fallback asks and handing cleanup ownership to the probe without changing `LiveEngine::run()`
- Reworked `src/runtime/hedge_live_probe.rs` around the paired lifecycle: live trigger preflight, bounded marketable BUY acquisition, live trigger verification, real downstream hedge execution, and required `merge_or_flatten` cleanup back to baseline
- Expanded `tests/hedge_live_probe.rs` to cover unsupported trigger legs, clean-market rejection, trigger price caps, partial trigger acquisition failure, successful merge cleanup, successful flatten cleanup, cleanup failure, bounded expectation mismatch, and CLI non-zero behavior
- Replaced the live-probe templates with paired-probe templates under `fixtures/hedge_live_probe_scenarios/` (`template_small_yes_buy_probe.json`, `template_small_no_buy_probe.json`) and updated the SCOTUS operator scenario to the paired schema
- Updated `HEDGE_TESTING_SUITE.md`, `agents/archive/hedge-incidents/hedge-incident-20260327/hedge-test-report-2026-03-28.md`, and `agents/summary.md` so Layer 3 is documented as a paired live probe instead of a synthetic post-attribution trigger
- Validation: `cargo test --test hedge_live_probe -- --nocapture`, `cargo test --test hedge_replay_harness -- --nocapture`, `cargo test --test hedge_test_harness -- --nocapture`, `cargo test --bin spreadeater -- --nocapture`, and `cargo test`

## 2026-03-28 — Phase 2 Layer 2 hedge replay harness

- Added `src/runtime/hedge_replay.rs` and the new `hedge-replay --scenario <path>` CLI for deterministic pre-attribution hedge replay
- Refactored `LiveEngine` user-event handling into a shared helper and added replay-only hooks for seeding market/order/position state, dispatching raw user events, forcing pending-fill fallbacks due, draining fill work through the real `FillHandler`, and triggering `refresh_quotes`, orphan recovery, and reconciliation without changing normal `run()` behavior
- Added replay seeding helpers on `OrderManager` so the harness can preload active and recently cancelled tracked orders without touching production runtime behavior
- Added a shared harness-support facade for Layer 1 and Layer 2 access to the mutable mock exchange, in-memory event collector, observed-outcome extraction, and common scenario types
- Added six deterministic Layer 2 fixtures covering raw trade attribution, order-update fallback residual sizing, exchange-sync missed-fill routing, orphan recovery/reconciliation, cancelled-order non-misattribution, and duplicate-trade dedupe
- Added `tests/hedge_replay_harness.rs` covering scenario parsing, each replay branch, dedupe/regression behavior, and CLI zero/non-zero exit codes
- Updated the hedge test report and project summary to reflect Layer 2 as implemented and Layer 3 as deferred
- Validation: `cargo test --test hedge_replay_harness -- --nocapture`, `cargo test --test hedge_test_harness -- --nocapture`, `cargo test hedge_size_for_accounted_fill_ -- --nocapture`, `cargo run -- hedge-replay --scenario fixtures/hedge_replay_scenarios/raw_trade_immediate_attribution.json`, `cargo run -- hedge-replay --scenario fixtures/hedge_replay_scenarios/order_update_fallback_partial_accounted.json`, `cargo run -- hedge-replay --scenario fixtures/hedge_replay_scenarios/exchange_sync_missing_fill.json`, and `cargo test`

## 2026-03-28 — Hedge test harness corrected as a true Layer 1 post-attribution runner

- Replaced the original `src/runtime/hedge_test.rs` dry-run harness with a post-attribution Layer 1 runner driven by an explicit `trigger.work_item` scenario schema
- Swapped the dry-run trading client for a real `TradingClient` pointed at a mutable in-process mock exchange, with scripted support for `/fee-rate`, `/balance-allowance`, `/data/orders`, `/data/order/:id`, `/positions`, `/book`, `POST /order`, and `DELETE /order`
- Added deterministic result assertions from emitted `HedgeIntentCreated`, `HedgeResultRecorded`, and `NeutralityEvaluated` payloads plus final risk state; `hedge-test` now fails on expectation mismatches instead of only runtime errors
- Replaced the old fixtures with four deterministic Layer 1 scenarios: full buy hedge, thin-book split, delayed truth confirmation, and unresolved exposure halt
- Moved harness coverage into `tests/hedge_test_harness.rs`, including parsing, sequencing, deliberate expectation mismatch, and CLI exit-code regressions
- Added focused live-engine sizing regressions for partial accounted fills, pre-existing opposite inventory, and residuals smaller than raw fills
- Updated the hedge-test report and project summary to reflect the corrected Layer 1 design and fixture format
- Validation: `cargo test --test hedge_test_harness -- --nocapture`, `cargo test --bin spreadeater`, `cargo test`, `cargo run -- hedge-test --scenario fixtures/hedge_scenarios/clean_full_buy_hedge.json`, `cargo run -- hedge-test --scenario fixtures/hedge_scenarios/thin_book_split.json`, and `cargo run -- hedge-test --scenario <temporary-fixture>` (expected non-zero)

## 2026-03-28 — On-Demand Hedge Test Harness (Issue #19, Layer 1)

- Created `src/runtime/hedge_test.rs` — deterministic hedge test harness (~600 lines)
- JSON scenario format for defining market state, fill events, book depths, and expected outcomes
- Mock HTTP server exercises the real `FillHandler::handle_fill()` path end-to-end
- New CLI command: `hedge-test --scenario <path.json>`
- Promoted FillHandler/FillWorkItem/ResolutionExecutionResult to pub(crate) for harness access
- Created 2 fixture scenarios: standard buy hedge and thin-book split
- 11 new inline tests for the harness module
- 533 tests passing (169 inline + 364 integration)

## 2026-03-29 — Watchdog PR Review Follow-Ups

- Removed the stray `// Test comment for notification check` line from `src/main.rs`
- Added `enforcing_watchdog_recovers_from_kill_pending_before_re_escalating` to verify the watchdog clears `KillPending` on degraded recovery and only re-escalates after sustained degradation
- Validated with `cargo test enforcing_watchdog_recovers_from_kill_pending_before_re_escalating -- --nocapture`
- Validated with `cargo test watchdog -- --nocapture`

## 2026-03-28 — Fix Hedge Timeout Gap (Issue #23)

- Added 15s reqwest timeout to TradingClient HTTP client to prevent indefinite API hangs
- Added `execute_resolution_plan_with_timeout` wrapper with `tokio::time::timeout` using `hedge_timeout_secs` (10s)
- Updated both hedge call sites (fill handler + reconciliation) to use timeout wrapper
- On timeout: mutex releases immediately, hedge marked failed, market killed by existing failure path
- Added inline test for timeout behavior
- 522 tests passing (158 inline + 364 integration)

## 2026-03-28 — Extensive Unit Test Expansion

- Added 237 new integration tests across 45 new test files in tests/unit/
- Phase 1: Models, Config & Helpers — position, order, orderbook, quote, market, decision, events, hedge, enriched, config tests
- Phase 2: Risk & Strategy — RiskManager, calibration, hedgeability, viability, score proxy, quote engine tests
- Phase 3: Books & Discovery — BookManager, websocket stats, discovery filter tests
- Phase 4: Trading Core — order manager, hedge executor, trading client, positions, CTF merge, user stream tests
- Phase 5: Auth, Persistence, Reporting, Monitor — credentials, signer, order signer, archive, shadow reporting, CSV export, emitter tests
- Added shared test helpers in tests/unit/helpers.rs
- Added tempfile = "3" dev-dependency for filesystem test isolation
- Total: 364 integration tests + 157 inline tests = 521 passing tests
- All tests run in ~4 seconds

## 2026-03-28 — Fix Rust workflow failures on `feature/hedging-fix`

### Changes
1. **Added `src/lib.rs` for the package**: the repository previously only exposed `src/main.rs`, so integration tests under `tests/unit/watchdog/*` that imported `spreadeater::...` failed with unresolved-crate errors during `cargo test`.
2. **Published the existing top-level modules through the library target**: `src/lib.rs` now exports `auth`, `books`, `config`, `discovery`, `models`, `monitor`, `persistence`, `reporting`, `runtime`, `strategy`, `trading`, and `watchdog`, which gives the watchdog tests a stable crate surface without changing CLI/runtime behavior.
3. **Restored config serde-default behavior expected by watchdog tests**: `WatchdogConfig` now opts into `#[serde(default)]`, so partial JSON config fragments deserialize with struct defaults as the tests and checked-in config expect.
4. **Fixed the stale reconnect watchdog test to match current semantics**: the “below threshold is degraded” integration test now uses mixed-stream reconnects so it stays below the global reconnect threshold without accidentally tripping the same-stream consecutive-disconnect critical threshold.
5. **Stopped the watchdog manager test from killing the test runner**: `WatchdogManager` now depends on the existing `KillAction` trait internally and the enforcing watchdog test injects a mock kill action instead of the production `KillTrigger`, so the test still verifies global halt plus `WatchdogKillTriggered` emission without executing the production `std::process::exit(1)` fallback path.
6. **Unblocked the Rust workflow’s full build/test path**: `cargo build --verbose` and `cargo test --verbose` now both pass locally under the same commands used by `.github/workflows/rust.yml`.

### Validation
- `cargo build --verbose`
- `cargo test --verbose`

### Files modified
- `src/lib.rs` — added the package library target and re-exported the existing top-level modules for integration-test imports
- `src/config.rs` — enabled serde defaulting for `WatchdogConfig`
- `src/watchdog/mod.rs` — injected `KillAction` into `WatchdogManager` so tests can use a mock kill path without terminating the harness
- `tests/unit/watchdog/health_tests.rs` — aligned the degraded reconnect test with the current threshold model
- `agents/summary.md` — recorded the validated CI fix increment

## 2026-03-27 — Phase 3 Shai incident hedge-correctness hardening

### Changes
1. **Unsafe raw trade fallback matching was removed**: `build_fill_work_item()` now hedges immediately only when a raw user trade is anchored by `maker_order_id`, `taker_order_id`, or the existing order-update pending-fill fallback. The old broad `active_fallback` / `recently_cancelled_fallback` matching on `condition_id + asset_id + side` is gone, and unanchored raw trades are emitted as deferred-to-reconciliation instead of synthesizing hedgeable tracked orders.
2. **Hard trade-id idempotency was added on the runtime fill path**: the live engine now keeps a bounded `trade_id` cache (`24h`, `50_000` entries, oldest-first eviction) and rejects repeated exchange `trade_id`s before attribution or hedge work creation so duplicate raw WS trades cannot re-enter hedge handling.
3. **Market halts are now idempotent and preserve the first reason**: `RiskManager::halt_market()` now keeps the original halt reason as canonical, reports later halt attempts as suppressed duplicates, and both FillHandler and reconciliation continue through the shared halted-market cleanup/finalization path without recursively nesting `Market halted: ...` reasons.
4. **Resolution truth is now based on bounded position confirmation instead of one post-sync snapshot**: `execute_resolution_plan()` captures the pre-resolution position, executes the planner-selected hedge/sell-back legs, syncs positions once, and retries one more sync after `250ms` only when a BUY hedge has verified-or-unknown execution evidence but the first post-sync snapshot does not show the expected opposite-side inventory increase. Result payloads now carry `post_sync_source`, `post_sync_yes_size`, `post_sync_no_size`, and whether a duplicate halt signal was suppressed.
5. **Sell-back execution now matches `STRATEGY.md`**: the planner in `hedge_executor.rs` still compares hedge-vs-sell-back economics share-by-share and preserves the same split, but planner-selected sell-back shares are now executed as aggressive `0.01` FOK exits while the original book-derived sell-back bid is preserved only as audit/reference data.
6. **Targeted orphaned-position recovery was added on the existing quote-refresh cadence**: refresh now detects markets with net position exposure above tolerance, zero tracked buy commitment, and no hedge already in progress, then routes them immediately through the existing reconciliation hedge path with origin `reconciliation_position_orphan` instead of waiting for the slower aggregate reconciliation pass.
7. **Shared hedge event schema was extended additively**: `FillDetected` now includes `anchored_order_id` and `deferred_to_reconciliation`; `HedgeIntentCreated` now includes `planned_sellback_reference_bid`; `HedgeResultRecorded` now includes `sellback_execution_limit_price`, post-sync YES/NO sizes, post-sync source, and `halt_signal_suppressed`. Schema version advanced from `1.3` to `1.4`.

### Validation
- `cargo test --bin spreadeater -- --skip watchdog::tests::enforcing_watchdog_emits_kill_triggered_after_confirmation` — 156 passed
- `cargo test --test core_types` — still fails for the pre-existing unresolved `spreadeater` crate imports in `tests/unit/watchdog/*`

### Files modified
- `src/runtime/live_engine.rs` — removed unsafe raw-fill fallback matching, added trade-id dedupe and orphaned-position refresh recovery, made halts idempotent, aligned sell-back execution with strategy, strengthened post-resolution truth, and added incident-focused runtime tests
- `src/trading/risk.rs` — preserved canonical halt reasons and surfaced duplicate-halt suppression state
- `src/monitor/emitters.rs` — emitted additive fill/hedge audit fields for anchored-vs-deferred attribution, sell-back reference/execution prices, post-sync truth, and suppressed halt signals
- `crates/spreadeater-core/src/payloads/order.rs`, `crates/spreadeater-core/src/payloads/hedge.rs`, `crates/spreadeater-core/src/envelope.rs` — additive hedge/fill payload fields and schema version `1.4`
- `tests/unit/core/payload_tests.rs`, `tests/unit/core/envelope_tests.rs` — updated shared payload/envelope serde coverage
- `agents/summary.md` — recorded the validated Phase 3 increment

## 2026-03-27 — Observe-only watchdog defaults and runtime cancel diagnostics

### Changes
1. **Quote-refresh cancel events now explain themselves**: `OrderCancelledPayload` gained optional diagnostics, and `refresh_quotes()` now attaches `would_trade`, `reasons`, `effective_quote_size`, and `available_budget_usd` whenever bids are deadmitted through `origin=quote_refresh_non_viable`.
2. **Hedge-depth actions now expose book-side context and skip no-op churn**: hedge-depth cancels/resizes now emit structured diagnostics with `hedgeable_size`, `min_order_size`, `opposite_best_price`, and `opposite_best_size`. `check_hedge_depth()` now compares the rounded replacement size to the current order size first and logs/returns on `same -> same` cases instead of emitting pointless resize churn.
3. **Watchdog is now observe-only by default**: `WatchdogConfig` gained `enforce_actions`, checked-in `config.json` now sets `enabled=true` and `enforce_actions=false`, and the watchdog manager still runs verdict logic and heartbeat/status polling but does not call `global_halt()` or kill/flatten unless enforcement is explicitly re-enabled.
4. **Watchdog verdicts now land in `events.jsonl` with raw-vs-parsed WS telemetry**: `WatchdogVerdict` payloads now include enforcement state plus `last_raw_book_ws_message_at`, `last_parsed_book_event_at`, `last_book_parse_error_at`, and accepted/ignored/parse-error/snapshot/delta counters sourced from the existing book WS stats path. `WatchdogKillTriggered` is emitted only on enforced kill execution.
5. **Shared schema and monitor compatibility were updated additively**: schema version bumped to `1.3`, shared order/watchdog payloads were extended additively, the monitor projector now accepts watchdog events without new DB rollups, and the monitor Postgres integration fixtures were updated for the new optional diagnostics fields.

### Validation
- `cargo test --bin spreadeater` — 152 passed
- `cargo test -p spreadeater-monitor --test postgres_integration --no-run`

### Files modified
- `src/runtime/live_engine.rs` — added quote-refresh and hedge-depth diagnostics, skipped no-op hedge-depth resizes, wired watchdog event emission context, and expanded runtime tests
- `src/trading/order_manager.rs` — threaded optional diagnostics through cancel/resize event emission
- `src/watchdog/mod.rs` — added observe-only enforcement gating, watchdog verdict emission, and enforcement-path tests
- `src/watchdog/health.rs` — exposed live WS connection state for verdict payloads
- `src/books/websocket.rs` — tracked raw/parsing timestamps and lifetime counters for non-destructive watchdog telemetry
- `src/monitor/emitters.rs` — emitted additive diagnostics on order events and richer watchdog verdict payloads
- `src/config.rs`, `config.json`, `CONFIG.md` — added and documented `watchdog.enforce_actions` with observe-only defaults
- `crates/spreadeater-core/src/payloads/order.rs`, `crates/spreadeater-core/src/payloads/monitor.rs`, `crates/spreadeater-core/src/envelope.rs` — added additive payload fields and schema version `1.3`
- `crates/spreadeater-monitor/src/projector/mod.rs`, `crates/spreadeater-monitor/tests/postgres_integration.rs` — accepted watchdog events and updated fixtures for additive payload fields
- `agents/summary.md` — recorded this validated increment

## 2026-03-27 — Safety-first hotfix for over-allocation and duplicate same-leg bids

### Changes
1. **Global open-order sync is now non-destructive**: `OrderManager::sync_open_orders()` still imports and refreshes exchange-truth orders, but it no longer prunes tracked orders or rebuilds the exchange-leg guard from a partial global `/data/orders` snapshot. Missing tracked bids therefore stay capital-reserved until they are confirmed missing by a stricter path.
2. **Missing-order confirmation is now conservative**: `detect_missed_fills_from_exchange()` no longer hard-prunes a disappeared tracked bid on first observation. It now performs an observe-only market-scoped sync, keeps the order tracked on the first confirmed miss when no position delta exists, clears suspicion on live reappearance, and only prunes on the second confirmed miss without corroborating fill evidence.
3. **Fresh placement now uses immediate exchange-truth leg dedupe**: `place_candidate()` now refuses new entry when either local tracking or the exchange-leg guard already contains that `(condition_id, leg)`. Successful fresh placements and successful resize replacements now insert that leg into the exchange guard immediately instead of waiting for a later sync.
4. **Duplicate live same-leg bids are now a safety fault**: exchange-truth sync detects multiple live BUY orders for the same `(condition_id, leg)`, logs the duplicate order IDs, and routes the market through the shared halt/cancel/finalize/flatten kill path so the bot cannot continue re-entering a duplicated live leg.
5. **Regression coverage was expanded around the incident shape**: added tests for non-destructive global sync under partial omission, observe-only market sync preserving budget, immediate exchange-leg reservation on placement/resize, first-miss retention, second-miss prune, reappearance clearing missing-order suspicion, and duplicate-live-leg kill behavior.

### Validation
- `cargo test --bin spreadeater` — 144 passed

### Files modified
- `src/trading/order_manager.rs` — made global sync additive-only, added observe-only market sync mode, tightened exchange-leg lifecycle handling, and expanded order-manager regressions
- `src/runtime/live_engine.rs` — added conservative missing-order confirmation, duplicate-live-leg kill routing, and runtime regressions for first-miss retention / second-miss prune / reappearance / duplicate kill
- `agents/summary.md` — recorded the validated hotfix increment

## 2026-03-26 — Complete Phase 2 missed-fill detection and immediate flatten hardening

### Changes
1. **Fast exchange-truth missed-fill detection added**: `refresh_quotes()` now calls `detect_missed_fills_from_exchange()` on the existing quote-refresh cadence. It pulls one global `/data/orders` snapshot, detects matched-size increases on tracked bid orders, and synthesizes `FillWorkItem`s directly into `FillHandler` with `match_source=exchange_order_sync` and `fallback_match=true`.
2. **Disappeared-order fallback now uses position truth**: when a tracked bid disappears from exchange truth, the runtime performs one corroborating `sync_positions()` pass. If position size increased on the filled side, it queues a synthetic fill into `FillHandler`; otherwise it prunes the stale tracked order without hedging.
3. **Resolution prep now corrects market-scoped capital truth first**: `prepare_market_for_resolution()` starts with `OrderManager::sync_market_open_orders()`, which imports still-live exchange orders, updates tracked remaining/matched size, and prunes missing market orders before cancel-state counts and `available_hedge_resolution_usdc()` are computed.
4. **Reconciliation now kills and flattens on first aggregate failure**: the old repeated-failure escalation path is removed from runtime behavior. Aggregate reconciliation failure now routes immediately through the same shared halt/cancel/finalize/flatten helper already used by `FillHandler`.
5. **Flatten cleanup now works after active management stops**: halted-market flattening resolves metadata from `managed_markets` first and `known_markets` second, verifies post-flatten exposure from synced positions, and keeps the market halted/managed if flatten placement or verification fails so later finalization passes can retry.
6. **Targeted runtime coverage added**: new tests cover exchange-sync matched deltas, disappeared-order corroboration, stale-order prune-without-hedge, stale tracked-order capital cleanup before resolution budget, first-failure reconciliation halts, and known-market flatten fallback.

### Validation
- `cargo test --bin spreadeater` — 137 passed

### Files modified
- `src/runtime/live_engine.rs` — completed exchange-truth fill fallback, market-scoped prep sync wiring, first-failure reconciliation kill path, known-market flatten fallback, and routed runtime tests
- `src/trading/order_manager.rs` — completed market-scoped open-order sync and stale tracked-order cleanup support for resolution capital truth
- `agents/summary.md` — recorded the Phase 2 increment and validation status

## 2026-03-26 — Complete Phase 1 hedge-resolution hardening

### Changes
1. **Planner is now internally affordability-complete**: `plan_fill_resolution()` replaced the old hedge-plan-plus-gate split as the execution truth and now enforces hedge affordability against the actual submitted BUY limit price, so a budget-constrained plan cannot later fail the risk check just because the worst-book level widened the single GTC limit.
2. **Shared pre-resolution preparation added**: FillHandler and reconciliation now both use `prepare_market_for_resolution()` to cancel all tracked market orders concurrently, wait up to `2000ms` with `100ms` polls plus `retry_pending_cancels()`, refresh gross balance/risk state, and fetch fresh YES/NO books with bounded cache fallback.
3. **Shared aggregate execution semantics added**: both call paths now run through `execute_resolution_plan()`, which executes the optional hedge BUY leg and optional sell-back FOK leg, syncs positions afterward, and treats success as `post_sync_net_exposure <= hedge_exposure_tolerance` instead of using hedge-leg success alone.
4. **Reconciliation now matches the redesigned strategy flow**: it uses fresh prep/books, emits the enriched hedge intent/result payload context, updates baselines and replacement asks only from the actual synced post-resolution position, and no longer fabricates a patched position that assumes the full fill size was hedged.
5. **Emergency hedge capital is now explicit**: `OrderManager::available_hedge_resolution_usdc()` exposes free capital for incident resolution without subtracting the normal quoting reserve, and the hedge pre-trade check now uses the planned hedge leg's actual required USDC plus that override instead of the old total-balance / `$0.99` fallback behavior.
6. **Observability fixtures aligned**: hedge payloads/tests and monitor fixtures now include the new planned split, cancel-window, and post-sync exposure fields without requiring a monitor schema migration.

### Files modified
- `src/trading/hedge_executor.rs` — completed `plan_fill_resolution()` affordability logic and replaced the old affordability-gate tests with budget-aware planner coverage
- `src/runtime/live_engine.rs` — finished reconciliation migration onto the shared preparation/execution flow and removed the synthetic post-reconciliation position patch
- `src/trading/order_manager.rs` — added `available_hedge_resolution_usdc()` coverage and concurrent cancel dispatch helper usage
- `src/monitor/emitters.rs` — enriched hedge intent/result payload emission context
- `crates/spreadeater-core/src/payloads/hedge.rs` — additive hedge payload fields for planned legs, cancel-window counts, and post-sync exposure
- `tests/unit/core/payload_tests.rs` — updated hedge payload serde fixtures
- `crates/spreadeater-monitor/tests/postgres_integration.rs` — updated monitor hedge payload fixtures
- `agents/summary.md` — recorded the validated increment and current unrelated workspace blockers

## 2026-03-24 — Phase 1: Book-aware hedge resolution (replaces hardcoded $0.99 limit)

### Changes
1. **New cost-benefit book walk for BUY hedges**: `compute_hedge_resolution()` walks both the opposite token ask book (for hedging) and the filled token bid book (for selling back), comparing cost per share. Each share routes to whichever option is cheaper: hedge (buy opposite) or sell-back (sell filled). Ties prefer hedge for CTF merge eligibility.
2. **Affordability gate**: `apply_affordability_gate()` caps hedge shares to what the balance can cover, moving excess to the sell-back bucket. Secondary safety net — book-aware pricing makes this rare.
3. **`execute_hedge()` now accepts `Option<&HedgeResolution>`**: when provided, uses the computed limit price. Falls back to legacy $0.99 when `None` (for SELL hedges which use the existing FOK path).
4. **`execute_buy_gtc_cancel()` accepts limit price as parameter**: no longer calls `buy_hedge_limit_price()` internally.
5. **Both call sites wired**: FillHandler and reconciliation both compute resolution from live book state before hedging. Sell-back orders placed as FOK for shares cheaper to sell than hedge.
6. **Pre-trade risk check uses book-aware cost**: FillHandler risk check now estimates hedge cost from the resolution's limit price instead of worst-case $0.99.
7. **Reconciliation now refreshes balance** before computing resolution (mirrors FillHandler behavior).

### Files modified
- `src/trading/hedge_executor.rs` — added `HedgeResolution`, `compute_hedge_resolution()`, `apply_affordability_gate()`; modified `execute_hedge()` and `execute_buy_gtc_cancel()` signatures
- `src/runtime/live_engine.rs` — wired resolution into FillHandler (handle_fill) and reconciliation (execute_reconciliation_hedge); added sell-back order placement; updated imports
- `agents/archive/hedge-incidents/hedge-incident-20260324/phase1-hedge-resolution-redesign.md` — design document for this change
- `agents/archive/hedge-incidents/hedge-incident-20260324/hedge-incident-report-2026-03-24.md` — incident report that motivated this fix

### Tests added (18 new, 127 total)
- 13 unit tests in `hedge_executor::tests` for `compute_hedge_resolution` and `apply_affordability_gate`
- 5 wiring tests in `live_engine::tests` for book/token mapping, affordability integration, empty books, and SELL hedge bypass

## 2026-03-24 — Emit frontier counterfactual swap rationale in decision events and logs

### Changes
1. **Decision payload now carries the true frontier swap pair rationale**: `DecisionEvaluated` gained optional `frontier_counterfactual_*` fields for the selected entrant/loser pair, including the one-loser counterfactual budget, reclaimable bid capital, both condition IDs, both `reward_per_share` ranking metrics, and both expected daily rewards.
2. **Runtime frontier log line now labels counterfactual numbers explicitly**: the existing `Frontier rotation candidate selected` log now uses `frontier_counterfactual_*` field names so the entrant/loser values are clearly identified as the selector's counterfactual metrics rather than the immediate residual-budget ranking view.
3. **Selector now preserves the actual counterfactual budget used for the swap decision**: `FrontierRotationPlan` carries the computed `actual free budget + reclaimable loser bid capital` so the emitted rationale exactly matches the frontier evaluation path.
4. **Regression coverage extended across serializer, runtime, and monitor compatibility paths**: added core serde coverage for the new optional fields, an emitter test proving the selected pair fields are written into `DecisionEvaluated`, a frontier selector test proving the counterfactual entrant metrics exceed the immediate fallback view, and a monitor Postgres fixture/API assertion that raw trace decision payloads retain the new fields without schema changes.

### Files modified
- `crates/spreadeater-core/src/payloads/decision.rs` — added optional `frontier_counterfactual_*` fields to `DecisionEventPayload`
- `src/monitor/emitters.rs` — extended `DecisionRankingContext` and `build_decision_evaluated()` to populate the new fields
- `src/runtime/live_engine.rs` — carried the counterfactual budget through frontier selection, relabeled frontier logs, and attached selected-pair fields to the entrant/loser decision events
- `tests/unit/core/payload_tests.rs` — added serde coverage for new fields and backward compatibility without them
- `crates/spreadeater-monitor/tests/postgres_integration.rs` — extended the fixture decision payload and asserted the raw trace decision payload exposes the new frontier fields
- `agents/summary.md` — recorded the new frontier observability increment and validation

## 2026-03-23 — Reserve frontier capital and freeze unrelated same-cycle bid entries

### Changes
1. **Persistent frontier reservation added**: `LiveEngine` now stores a small reservation record once a `FrontierRebalance` loser is actually canceled, capturing the reserved entrant, loser, reclaimable bid capital, and the cycle that armed the handoff.
2. **Same-cycle capital leak closed**: after a frontier loser cancel is issued, unrelated new bid entries are skipped for the rest of that discovery cycle so the freed budget cannot fragment into small opportunistic bids before the reserved entrant gets its turn.
3. **Loser-market maintenance blocked while reservation is pending**: if the canceled loser still has active bid orders or pending cancel verification, discovery cycles freeze new bids and skip loser-market bid maintenance instead of drifting or refreshing the market we are trying to evict.
4. **Reserved entrant gets first claim next cycle**: once the loser is clear, the reserved entrant is attempted before the general ranked action loop and the reservation is then cleared, while the rest of that cycle remains frozen for unrelated new bids.
5. **Targeted regression coverage added**: new runtime tests lock in the bid-only freeze gate, reservation wait behavior while cancel verification is pending, and reserved-entrant activation/cleanup.

### Files modified
- `src/runtime/live_engine.rs` — added reservation state, activation/clear helpers, same-cycle freeze enforcement, loser-maintenance skip, and new frontier reservation tests
- `src/trading/order_manager.rs` — added bid-specific active/pending cancel state helpers used by the reservation path
- `STRATEGY.md` — clarified that frontier replacement now reserves capital immediately and freezes unrelated new bid entries until the next-cycle entrant handoff
- `agents/summary.md` — recorded the reservation/freeze allocator fix and validation

## 2026-03-23 — Start a running live investigation note for frontier rotation behavior

### Changes
1. **Created a dedicated investigation document**: added `retired frontier-rotation investigation` as the running note for the current uncommitted allocator stack.
2. **Captured live evidence from `run_20260324_023113`**: documented the held China market's drop from roughly `$0.549/day` to `$0.370/day`, confirmed that the run emitted no `FRONTIER_REBALANCE` cancels and no frontier metadata, and recorded that the visible `0.021/day` / `0.014/day` ranking rows are not reliable switch signals once budget is fully allocated.
3. **Recorded the current working hypothesis**: the strongest live issue is still observability, not proven allocator failure, because several alternative markets were materially above `$0.02/day` in the first full-budget cycle even though later residual-budget rankings made them look negligible.

### Files modified
- `retired frontier-rotation investigation` — running investigation document for allocator impact, performance, and behavior
- `agents/summary.md` — linked the new investigation note from Recent Work

## 2026-03-23 — Add discovery-cycle strict bid rotation

### Changes
1. **Tracked bid age added**: `TrackedOrder` now carries `created_at`. Exchange-synced orders preserve API timestamps, fresh local bid placements use `Utc::now()`, and same-market replace/resize paths preserve the original bid age so maintenance churn does not reset the hold window.
2. **New rank-based cancel reason**: added `CancelReasonCode::FrontierRebalance` for bid-rotation evictions.
3. **Counterfactual frontier evaluation**: `LiveEngine` now retains the books fetched during discovery evaluation, reuses them for a separate frontier pass, and compares non-held markets against one evictable held bid market at a time using `actual free budget + that loser market's reclaimable bid capital`.
4. **One-swap discovery-cycle rebalance**: when a better entrant exists and the loser market's continuous bid age exceeds `poll_interval_secs + 1`, the bot cancels only that loser market's bids during the 60s discovery cycle and defers replacement placement until the next discovery cycle after cancel verification. The 5s quote refresh, asks, and inventory handling were left unchanged.
5. **Observability updates**: `DecisionEvaluated` payloads can now carry optional frontier metadata for the selected entrant/loser pair, and the runtime emits a dedicated `Frontier rotation candidate selected` summary log when a swap is identified.
6. **Regression coverage**: added tests for exchange timestamp retention, resize age preservation, frontier hold-window enforcement, better-market selection with reclaimed capital, worse-market rejection, pending-cancel suppression, and the new cancel reason payload.

### Files modified
- `src/runtime/live_engine.rs` — added frontier selection/rebalance pass, shared ranking helpers, book reuse, and new rotation tests
- `src/trading/order_manager.rs` — tracked-order timestamps, age preservation helpers, pending-cancel test seeding, and timestamp regression tests
- `crates/spreadeater-core/src/reason_codes.rs` — added `FrontierRebalance`
- `crates/spreadeater-core/src/payloads/decision.rs` — added optional frontier metadata
- `src/monitor/emitters.rs` — passed frontier metadata through `DecisionEvaluated` payloads and updated tests
- `tests/unit/core/payload_tests.rs` — updated decision payload roundtrip coverage
- `crates/spreadeater-monitor/tests/postgres_integration.rs` — updated shared decision payload fixtures
- `STRATEGY.md` — documented the new discovery-cycle bid rotation layer
- `agents/summary.md` — recorded the new allocator behavior and validations

## 2026-03-23 — Watchdog: Auto Kill+Flatten on WebSocket/API Issues

### Added
- **Watchdog module** (`src/watchdog/`): In-process health monitoring for WebSocket connections and Polymarket status page
  - `WsHealthTracker`: Tracks book/user WS connection health (silence detection, reconnect counting, consecutive disconnect tracking)
  - `StatusPoller`: Polls Polymarket's Instatus status page every 30s for component outages
  - `KillTrigger`: Executes emergency kill+flatten via `kill_flatten.py` with process exit fallback
  - `WatchdogManager`: Orchestrates health + status assessment with 4-state escalation machine (Normal → Warning → KillPending → Killed)
- **WatchdogConfig** in `src/config.rs`: 13 configurable parameters (silence thresholds, reconnect limits, confirmation delay, status page URL, critical components)
- **Watchdog event types** in spreadeater-core: `WatchdogVerdict` and `WatchdogKillTriggered` with payloads
- **Watchdog emitters** in `src/monitor/emitters.rs`: `build_watchdog_verdict()` and `build_watchdog_kill_triggered()`
- **External sidecar** (`scripts/watchdog_sidecar.py`): Monitors heartbeat file for bot process crashes, triggers `kill_flatten.py` if stale >60s with open positions
- **Heartbeat file** (`./data/watchdog_heartbeat`): Written every 5s by in-process watchdog for sidecar monitoring
- **LiveEngine integration**: WS health events reported to shared `WsHealthTracker` from the `select!` loop; watchdog spawned as independent tokio task

### Architecture
- Hybrid two-layer design: in-process Rust watchdog (direct WS health access) + external Python sidecar (bot-crash safety net)
- Escalation: Critical verdict → immediate global_halt() → 10s confirmation → kill_flatten.py
- Degraded sustained >120s → escalate to KillPending
- Status page polling over webhook receiver (no public endpoint needed)

## 2026-03-23 — Unify `min_outcome_price` enforcement on outcome-mid semantics

### Changes
1. **Removed price-based cheap-bid sweeps**: `OrderManager::sync_open_orders()` no longer takes `min_outcome_price` and no longer flags imported bids for cancellation based on resting `order.price`. The tracked-order `cancel_cheap_bids()` sweep was deleted.
2. **Runtime startup path aligned**: `LiveEngine` no longer calls the removed cheap-bid sweep during cycle setup. Existing book-backed `min_outcome_price` checks remain unchanged in quote evaluation, refresh rejection handling, and hedge-depth monitoring.
3. **Documented entry semantics in tests**: added a quote-engine regression proving a bid may rest below the configured floor when the outcome mid still meets the threshold.
4. **Locked in retention semantics**: added live-engine regressions proving a below-floor resting bid survives while own-book mid stays healthy, and is still canceled when the own-book mid drops below `min_outcome_price`.

### Files modified
- `src/trading/order_manager.rs` — removed price-based `min_outcome_price` enforcement from sync/import and deleted `cancel_cheap_bids()`
- `src/runtime/live_engine.rs` — removed the cheap-bid sweep call and added mid-based hedge-depth regression tests
- `src/strategy/quote_engine.rs` — added regression coverage for sub-floor bid pricing when outcome mid remains above threshold
- `agents/summary.md` — recorded the new unified outcome-mid semantics

## 2026-03-23 — Roll back the broad reward-yield refactor and keep only a ranking hotfix

### Changes
1. **Restored the pre-refactor behavior**: reverted the uncommitted reward-yield worktree back to `HEAD`, keeping the existing viability/admission gate, hedgeability checks, monitor payload/schema, and runtime safety behavior exactly as they were before the failed live run.
2. **Ranking-only metric change**: Phase 2 market ordering now uses `reward_per_share = estimated_reward / shares_committed` instead of `return_per_share = estimated_edge / shares_committed`. This removes favorable hedge economics from the ranking numerator without changing quote approval or viability.
3. **Stable tie-breaks**: viable markets now sort by higher `reward_per_share`, then higher `estimated_reward`, then stable `condition_id` order.
4. **Ranking metadata rename only**: archived `DecisionEvaluated` ranking metadata and terminal `Market ranking` logs now use `ranking_metric_name=reward_per_share`. Legacy viability payload fields and monitor edge fields remain unchanged.
5. **Focused regression coverage**: added ranking-specific tests proving hedge economics no longer affect ordering when reward/share is equal, and updated emitter/core/monitor integration fixtures to match the new ranking metadata name/value.

### Files modified
- `src/strategy/viability.rs` — added a ranking-only `reward_per_share` helper while preserving legacy viability semantics
- `src/runtime/live_engine.rs` — switched Phase 2 sorting/logging/event ranking metadata to `reward_per_share`
- `src/monitor/emitters.rs` — updated ranking metadata test expectations
- `tests/unit/core/payload_tests.rs` — updated archived decision metric roundtrip expectations
- `crates/spreadeater-monitor/tests/postgres_integration.rs` — updated ranking metadata fixtures
- `STRATEGY.md` — documented that viability remains on the legacy path while ranking is now reward/share
- `agents/summary.md` — recorded the rollback plus the minimal hotfix

## 2026-03-23 — Archive cycle ranking metrics on DecisionEvaluated events

### Changes
1. **Post-sort rank archived**: live discovery now emits `DecisionEvaluated` after Phase 2 sorting so each archived decision carries its final `rank_in_cycle` and `ranked_market_count` for that discovery cycle.
2. **Exact ranking metric archived**: decision payloads now include `ranking_metric_name=return_per_share` and `ranking_metric_value`, which is the actual value used by the live allocator when ordering markets.
3. **Terminal ranking logs improved**: `Market ranking` logs now include the exact `return_per_share` value alongside `estimated_daily` and `would_trade`.
4. **Monitor DTO/store compatibility**: the monitor-side decision snapshot types now preserve the new ranking fields without requiring schema or database changes because they live inside the existing JSON payload.
5. **Regression coverage updated**: core payload roundtrip tests, emitter tests, and monitor Postgres integration fixtures now include the new additive decision-payload fields.

### Files modified
- `crates/spreadeater-core/src/payloads/decision.rs` — added optional rank/metric fields to `DecisionEventPayload`
- `src/monitor/emitters.rs` — added `DecisionRankingContext`, archived post-sort rank/metric fields, updated tests
- `src/runtime/live_engine.rs` — moved `DecisionEvaluated` emission to post-sort and included exact ranking metric in logs/events
- `src/runtime/orchestrator.rs` — passed default ranking context for non-ranked orchestrator emissions
- `crates/spreadeater-monitor/src/dto.rs` — preserved ranking fields in `DecisionSnapshot`
- `crates/spreadeater-monitor/src/store.rs` — parsed ranking fields out of stored decision payload JSON
- `crates/spreadeater-monitor/web/src/types.ts` — typed the new decision snapshot fields for web consumers
- `tests/unit/core/payload_tests.rs` — updated decision payload roundtrip assertions
- `crates/spreadeater-monitor/tests/postgres_integration.rs` — updated decision payload fixtures

## 2026-03-22 — Revert viability/ranking denominator from per-dollar to per-share

### Changes
1. **Per-share denominator**: `compute_viability()` now uses `sum(size)` for approved bid legs instead of `sum(price × size)`. In binary markets, bid + hedge ≈ $1/share, so shares = true account capacity consumed regardless of bid price. This matches `committed_exposure()` in `order_manager.rs` which already sums `order.size`.
2. **Renamed `return_per_dollar` → `return_per_share`** on `RewardViability` with `serde(alias)` for backward-compatible JSON deserialization.
3. **Status log format**: `r_per_dollar` / `¢/$` → `r_per_share` / `¢/sh` in per-market and summary log lines. Capital accumulator uses bid shares only.
4. **Ranking**: Phase 2 sort key updated from `return_per_dollar` to `return_per_share`.
5. **Tests rewritten**: Test A now asserts equal per-share return for same-size different-price bids. Test B uses different sizes to demonstrate meaningful ranking differences. Removed `capital_committed_accounts_for_price` test (no longer applicable).

### Files modified
- `src/models/decision.rs` — field rename + serde alias
- `src/strategy/viability.rs` — denominator, field assignment, inline tests
- `src/runtime/live_engine.rs` — ranking sort key, MarketStatus struct, computation, log formatting
- `src/monitor/emitters.rs` — test fixture field name
- `src/config.rs` — comment update
- `tests/unit/strategy/reward_per_dollar_tests.rs` — rewritten for per-share model

## 2026-03-22 — Fix per-dollar estimation mismatches

### Changes
1. **Discount factor in status logs**: `estimate_market_daily_reward()` now applies `reward_discount_factor` so status log `est_daily` values match the discounted viability math.
2. **Ranking by return_per_dollar**: Phase 2 market ranking now sorts by `return_per_dollar` (per-dollar return) instead of `estimated_reward` (absolute $). Capital-efficient markets get budget priority.
3. **Actual capital in viability denominator**: `compute_viability()` now computes capital as `sum(price × size)` for approved bid legs instead of using raw share count. This gives correct per-dollar returns for bids at different price levels.
4. **Regression tests**: Added `#[cfg(test)]` tests in `viability.rs` calling `compute_viability()` directly — covers price-dependent returns (Test A), absolute-vs-per-dollar ranking (Test B), and discount factor inclusion (Test C).

### Files modified
- `src/runtime/live_engine.rs` — discount in `estimate_market_daily_reward()`, ranking sort key
- `src/strategy/viability.rs` — bid_capital computation, regression test module

## 2026-03-22 — Redenominate reward estimation from per-share to per-dollar-committed

### Changes
1. **Viability gate now applies `reward_discount_factor`**: `compute_viability()` in `viability.rs` now multiplies estimated reward by `config.reward_discount_factor` (default 0.70). Previously the discount was only applied in status logging, creating an inconsistency where the viability gate used undiscounted rewards.
2. **New `return_per_dollar` field on `RewardViability`**: the per-dollar return is now computed and stored alongside other viability metrics, visible in decision reports and event payloads.
3. **Status logging switched to per-dollar**: `log_status()` now computes `R_dollar = est_daily / capital_committed` (where capital = price × size) instead of `R = est_daily / total_shares`. Log labels changed from `r_per_share` → `r_per_dollar` with `¢/$` units.
4. **Decision rule updated**: the old `P_Y + P_N - R < 100` framework is retired. The new decision rule is `R_dollar_effective > hedge_cost_per_dollar`, implemented via `return_per_dollar >= min_return_pct`.
5. **Tests rewritten**: `reward_per_share_tests.rs` → `reward_per_dollar_tests.rs` with per-dollar helpers, decision rule tests, and a capital-efficiency test showing price-dependent returns.
6. **Framework doc rewritten**: `retired per-share estimation document` → `agents/archive/per-share-ranking/Reward Per Dollar Estimation.md`.
7. **STRATEGY.md updated**: viability gate description now documents the per-dollar formula and discount factor; config reference table includes `reward_discount_factor`.

### Behavioral impact
- Markets that were borderline viable may now be rejected (estimated reward drops ~30% due to discount factor application in viability gate).
- Status log R values will differ from previous runs (denominated per dollar, not per share).

### Files modified
- `src/strategy/viability.rs` — apply discount factor, add return_per_dollar, update doc comment
- `src/models/decision.rs` — new `return_per_dollar` field on `RewardViability`
- `src/config.rs` — doc comment update (per-share → per-dollar)
- `src/runtime/live_engine.rs` — MarketStatus struct, capital-based R computation, log labels
- `src/monitor/emitters.rs` — test fixture updated with new field
- `tests/unit/strategy/reward_per_dollar_tests.rs` — rewritten from reward_per_share_tests.rs
- `tests/unit/strategy/mod.rs` — module rename
- `agents/archive/per-share-ranking/Reward Per Dollar Estimation.md` — rewritten from per-share framework
- `STRATEGY.md` — viability gate and config reference updates

## 2026-03-22 — Add reward-per-share (R) estimation to status logs

### Changes
1. **New config field `reward_discount_factor`**: added to `StrategyConfig` with serde default `0.70`. Controls the uncertainty discount applied to raw reward-per-share estimates (range 0.5–0.8 per strategy doc).
2. **Per-market R in status log**: `log_status()` now computes `R = est_daily / total_deployed_shares` and `R_effective = R × discount_factor` for each market with resting orders, displayed as cents per share (`r_per_share`, `r_effective`) alongside existing `est_daily`.
3. **Weighted average R in summary line**: the summary log now shows `avg_r_per_share` and `avg_r_effective` (weighted by deployed shares across all active markets).
4. **Tests**: config default deserialization test, unit tests for reward-per-share math (zero orders, single order, multi-order, discount application, weighted average, edge cases).

### Files modified
- `src/config.rs` — new field + default function
- `src/runtime/live_engine.rs` — `MarketStatus` struct, computation, per-market and summary log lines
- `tests/unit/strategy/reward_per_share_tests.rs` — new test file
- `tests/unit/strategy/mod.rs` — new module
- `tests/unit/mod.rs` — added strategy module

## 2026-03-22 — Refresh stale books before risk-halt and archive book-WS stats

### Changes
1. **Refresh-before-kill stale handling**: `check_hedge_depth()` now treats stale cached YES/NO books as a verification trigger instead of immediate proof of feed failure. When either side is stale for an active bid market, the engine performs a concurrent REST refresh of both books with a fixed 2s timeout and only falls back to `kill_market()` if that refresh fails.
2. **Same-pass hedge-depth evaluation**: successful stale-book refreshes are inserted back into `BookManager` and used immediately for the current hedge-depth pass, so quiet markets can survive beyond `max_book_age_secs` without skipping the actual mid-price / depth checks for that cycle.
3. **Archived book-WS observability**: `StatusSnapshotPayload` now carries optional `book_ws_*` counters (accepted, ignored, parse-error, snapshot, delta), fed from the once-per-cycle drained `BookWsStats` snapshot retained on `LiveEngine`. These counters now land in `events.jsonl`, not just terminal logs.
4. **Schema bump**: archived events now emit `SchemaVersion::V1_2` to reflect the additive `StatusSnapshotPayload` fields while remaining backward-compatible for existing projector SQL.
5. **Regression coverage**: added tests for successful stale-book REST rescue, refresh-timeout halt fallback, archived status-snapshot book-WS counters, and updated core payload/schema roundtrip coverage.

### Validation
- `cargo fmt`
- 103 tests passed (`cargo test`): 74 unit tests in `src/main.rs` + 29 core schema tests in `tests/core_types.rs`.

## 2026-03-22 — Update market websocket protocol and add book-WS health counters

### Changes
1. **Current Polymarket market-channel protocol**: `src/books/websocket.rs` now subscribes with the documented `{"type":"market","assets_ids":[...]}` payload, parses inbound `event_type`, and stops using the top-level `market` field as a book key.
2. **Current `price_change` support**: nested `price_changes` entries are now grouped by `asset_id` and mapped into the existing internal `BookEvent::Delta` format, with `BUY` updates routed to bids and `SELL` updates routed to asks.
3. **Parser result classification**: the market-book parser now distinguishes accepted messages, ignored-but-valid messages, and malformed payloads instead of silently returning `None`, making protocol drift observable.
4. **Cycle-level WS health logging**: added lightweight atomic counters for accepted messages, ignored messages, parse errors, snapshot events, and delta events, drained and logged once per live cycle without adding locks on the trading path.
5. **Regression coverage**: added parser fixtures for current `book` and `price_change` messages, subscribe-payload serialization coverage, counter reset coverage, and a live-engine test for cycle stats draining.

### Validation
- `cargo fmt`
- 100 tests passed (`cargo test`): 71 unit tests in `src/main.rs` + 29 core schema tests in `tests/core_types.rs`.

## 2026-03-22 — Preserve funded resting bids during refresh re-evaluation

### Changes
1. **Credit-aware re-evaluation**: the shared market evaluator now treats tracked resting BUY exposure on the same market as already funded when recomputing dynamic quote size, so refresh and later cycles do not zero out quote size merely because the existing order consumed free budget.
2. **Incremental bid placeability**: actionable quote filtering now applies same-leg funded bid credit before spending any remaining free budget. Existing funded bids are preserved, upsizes spend budget only on the incremental delta, and unrelated new bid legs still require real free budget.
3. **Self-cancel loop fixed**: `refresh_quotes()` no longer deadmits a market just because its own resting bid drove `available_budget` near zero. True non-budget deadmission reasons remain unchanged.
4. **Regression coverage**: added tests for preserving funded bids with no free budget, preventing one bid leg from borrowing another leg’s credit, keeping real rejections intact, and ensuring a funded resting bid survives refresh instead of disappearing.

### Validation
- 93 tests passed (`cargo test`): 64 unit tests in `src/main.rs` + 29 core schema tests in `tests/core_types.rs`.

## 2026-03-21 — Fix stale-book halt deadlock and require verified cancels

### Changes
1. **Deadlock-free stale-book kill**: `check_hedge_depth()` now snapshots managed markets before scanning and only calls `kill_market()` after the read lock is released, eliminating the guaranteed read→write self-deadlock on stale-book halts.
2. **Verified cancel outcomes**: `TradingClient::cancel_order()` now parses Polymarket cancel responses via `canceled` / `not_canceled` and falls back to `get_order()` when the response is ambiguous, returning explicit `Confirmed`, `Rejected`, or `Unknown` outcomes instead of treating any HTTP `2xx` as success.
3. **Conservative order tracking**: `OrderManager` now keeps orders tracked until cancel confirmation, records unverified/rejected cancels for retry, aborts resize/cancel-replace replacements when the original cancel is not verified, and only emits cancel events on confirmed cancels.
4. **Pending cancel retries**: added a lightweight pending-cancel retry registry with a 2s backoff, retried from the main cycle and quote-refresh path without adding a new background worker.
5. **Halted-market finalization**: `kill_market()` now halts immediately and attempts cancels immediately, but only flattens unhedged excess inventory and removes the market from `managed_markets` after there are no active or pending cancel-verification entries left.
6. **FillHandler parity**: the fill-handler kill path now uses the same delayed-finalization semantics, so hedge-failure halts no longer assume cancels succeeded.
7. **Regression coverage**: added tests for stale-book kill completion, unknown-cancel tracking retention, confirmed-cancel cleanup, and retry-driven pending-cancel resolution.

### Validation
- 89 tests passed (`cargo test`): 60 unit tests in `src/main.rs` + 29 core schema tests in `tests/core_types.rs`.

## 2026-03-21 — Align live reward gating, refresh viability, and status estimates

### Changes
1. **Placeable-leg reward gating**: live evaluation now derives an actionable quote set after hedgeability, suppressing asks without sellable inventory and capping/suppressing bids against the same hedge-aware budget logic used by the order manager.
2. **Approved-only score math**: score-proxy and viability calculations now ignore rejected/suppressed legs, so non-placeable quotes no longer inflate estimated reward or hedge-cost estimates.
3. **Shared cached-book evaluation path**: quote refresh now reuses the same evaluation flow as full-cycle admission (dynamic size, hedgeability, placeability, score proxy, viability) while still operating only on cached books.
4. **Refresh respects viability**: when a managed market is no longer viable on refresh, the bot cancels bid legs and does not reintroduce them until the next full-cycle admission path says it should.
5. **Status estimate consistency**: `log_status()` now computes per-market `est_daily` from current resting tracked orders using the same `compute_score_proxy()` path and live calibrated competition multiplier as selection, replacing the older custom estimator.
6. **Decision/report clarity**: suppressed legs now surface in decision reasons, and `effective_quote_size` is derived from approved actionable legs rather than the first raw candidate.
7. **LiveEngine kill helper restored**: reintroduced `LiveEngine::kill_market()` / `flatten_unhedged()` so the existing stale-book kill path compiles cleanly alongside the newer fill-handler copy.

### Validation
- 81 tests passed (`cargo test`): 52 unit tests in `src/main.rs` + 29 core schema tests in `tests/core_types.rs`.

## 2026-03-21 — Implement STRATEGY.md deltas (all 9 behavior changes)

### Changes
1. **Quote refresh 30s → 5s**: config.json and default updated.
2. **Depth check 5s → 2s**: hardcoded interval in `live_engine.rs` changed, position sync counter adjusted (every 15th invocation to maintain ~30s).
3. **Hedge timeout 60s → 10s**: config.json and default updated.
4. **Budget = API balance − cash_reserve**: new `cash_reserve` field on `RiskConfig` (default $50), `OrderManager::available_budget` subtracts it. `max_total_exposure` removed from budget model.
5. **Book staleness = kill trigger**: depth check now kills markets with stale books (> max_book_age_secs) instead of just cancelling individual bids.
6. **Per-market hedge mutex**: `hedge_locks: HashMap<String, Arc<Mutex<()>>>` shared between FillHandler and reconciliation. Replaces timestamp-based `recon_cooldowns` + `hedge_signals` duplicate prevention. Guarantees exactly one hedge operation per market at any time.
7. **Partial hedge → sell remainder**: if post-hedge residual exposure exceeds tolerance, the unhedged remainder is sold back via FOK at $0.01 before merging.
8. **CTF merge = primary exit**: merge runs after all hedge resolution (hedge + sell remainder). Asks placed only as fallback if merge fails or CTF merger not configured.
9. **Merge timing**: merge only fires after hedge is fully resolved (all options exhausted), not on partial fills.

### Files changed
- `src/config.rs`: added `cash_reserve` field + defaults + tests
- `src/trading/order_manager.rs`: `cash_reserve` in constructor, `available_budget` updated + tests
- `src/runtime/live_engine.rs`: depth interval, staleness kill, hedge_locks, restructured fill handler post-hedge flow, removed recon_cooldowns + tests
- `config.json`: quote_refresh_secs=5, hedge_timeout_secs=10, cash_reserve=50
- `CONFIG.md`: documented new fields

## 2026-03-21 — Created STRATEGY.md (full strategy breakdown for technical partner)

### What
- Created `STRATEGY.md` at project root: a comprehensive strategy document covering core thesis, market selection pipeline, quote pricing, hedge execution, CTF merge, risk controls, startup behavior, and operational lifecycle.
- Document captures both current behavior and 9 intended changes (marked `DELTA`) confirmed with the user via step-by-step Q&A.

### Key deltas documented
1. CTF merge as primary exit (asks become fallback only)
2. Quote refresh: 30s → 5s
3. Depth check: 5s → 2s
4. Hedge timeout: 60s → 10s
5. Budget from API balance − configurable reserve (not static cap)
6. Partial hedge: sell unhedged remainder, then merge
7. Stale book → kill that market (critical error)
8. Reconciliation with per-market mutex (prevents double-hedging)
9. CTF merge only after hedge fully resolved

## 2026-03-20 — Fill/fallback correctness fixes without strategy changes

### Issues addressed
- **Order update vs trade dedup**: the fill handler now keeps `effective_fill_size` separate from `size_to_apply`, so an `order update -> trade` sequence can still hedge the real fill even when the tracked order was already pre-accounted by `apply_order_update`.
- **Pending fallback residual sizing**: flushed pending-fill fallback work items now size hedges through `hedge_size_for_accounted_fill(...)` using live position state, instead of hedging the raw matched delta.
- **Explicit hedge verification state**: BUY `GTC + cancel` hedges now distinguish `VerifiedFilled`, `VerifiedZeroFill`, and `Unknown` instead of treating missing order lookups as confirmed fills.
- **Position-truth resolution for unknown hedges**: when a hedge cannot be verified through `get_order`, the bot syncs positions and resolves success/failure from real post-hedge exposure, avoiding both false success and an immediate erroneous sell-back.
- **Timestamp-aware duplicate suppression**: recent-hedge cooldown tracking now stores both monotonic and wall-clock timestamps, and the fill handler only suppresses a late duplicate if the trade timestamp is not newer than the last verified hedge while the market is still balanced within tolerance.

### Validation
- 70 tests passed (`cargo test`).

## 2026-03-20 — PR review fixes: hedge verification, cooldown scoping, dedup

### Issues addressed (from PR #4 review)
- **GTC+cancel fill verification**: `execute_buy_gtc_cancel` now queries the order after the 500ms cancel to check `size_matched`. Returns `success: false` if zero fills occurred, preventing silent hedge failures from being treated as successes.
- **Position-aware recon cooldown**: The blanket 3-minute market cooldown in `build_fill_work_item` now checks whether the position is already balanced before suppressing a fill. Genuine new fills on the same market are no longer dropped.
- **Deferred Connected event**: `UserEvent::Connected` is now emitted after the first server message is received, not immediately after sending the subscription request. Prevents position syncs before the stream is confirmed live.
- **Deduplicated `normalize_share_size`**: Single canonical implementation in `hedge_executor.rs` (handles negatives correctly), imported by `live_engine.rs`.
- **Deduplicated `whole_share_budget_limit`**: Single `pub(crate)` implementation in `order_manager.rs`, imported by `live_engine.rs`.
- **Removed `_stale_fill_size` parameter**: `execute_reconciliation_hedge` no longer accepts the unused size parameter; callers updated.
- **Inlined `reconciliation_hedge_size`**: Trivial wrapper removed; callers use `required_hedge_size` directly.
- **Added `TradingClient::get_order`**: New method to fetch a single order by ID for post-hedge fill verification.

### Validation
- 61 tests passed (`cargo test`).

## 2026-03-20 — Merge strategist hedge-flow fix with local safety rails

### Root cause addressed
- BUY hedges were being submitted through the same share-sized `OrderRequest` path as passive orders while using `FOK`, which is unsafe for Polymarket market-style BUY semantics and can turn an intended share hedge into a much larger dollar-spend hedge.
- Balance correction sold `floor(yes-no)` / `floor(no-yes)`, which could strand sub-share residuals and later trip the unhedged-timeout kill switch.
- Reconciliation hedges trusted the intended hedge size more than the live residual exposure and had weaker post-hedge verification than the hot-path fill handler.
- Live bid sizing used `available_budget` as a hedge-aware share ceiling during evaluation, but the final passive bid gate still compared `price * size` against that budget, allowing the last-step interpretation to drift back toward visible order cost.

### Fix (order/model, hedge executor, live engine, risk)
- Added explicit `OrderAmountKind` to `OrderRequest` and hard-rejected share-sized BUY `FOK/FAK` requests in `TradingClient`, so the dangerous order shape now fails fast instead of silently overhedging.
- Kept the strategist-approved BUY hedge path: aggressive share-sized `GTC` at `0.99`, short `500ms` grace period, then cancel any unfilled remainder; SELL hedges stay aggressive `FOK` sells.
- Updated order signing precision to preserve fractional share sizes (2dp) and tick-aware notional precision, allowing residual hedges and balance corrections to use exact 2dp share sizing instead of integer flooring.
- Hot-path fill hedging now sizes from the actually-accounted fill plus projected residual exposure instead of raw WS size, so pending-fill fallback accounting cannot double-count the same fill.
- Reconciliation now re-reads live position before hedging, skips hedges already within tolerance, and patches local position state immediately after a successful reconciliation hedge so stale API reads do not re-hedge the same inventory.
- User-stream connect/reconnect still emits explicit status events and now syncs positions when the connection comes up.
- Balance correction, neutrality verification, and risk timeout tracking now share one configured tolerance (`risk.hedge_exposure_tolerance`, default `0.5` shares) instead of mixing `> 1 share`, `10% of fill`, and exact-zero logic.
- Added explicit operational observability for user-stream reconnects and pending fill fallback queue/flush events so future missed-fill incidents are easier to attribute.
- Live quote evaluation now floors `available_budget` to a whole-share ceiling before dynamic sizing, and the final passive bid gate now caps directly against that same whole-share exposure budget instead of `price * size` / `budget / price`.

### Validation
- Added unit coverage for order-request validation, fractional order-signing precision, hedge-sizing helper logic that reproduces the Lyon-style residual math, and the new bid-budget capping helpers.
- Updated the core payload test fixture for the current `StatusSnapshotPayload` shape.
- Ran `PATH=/opt/homebrew/opt/rustup/bin:$PATH cargo test` successfully: 62 tests passed.

## 2026-03-12 — Fix unhedged positions: position cap blocking hedges + kill switch flattening

### Root cause: pre_trade_check blocked hedges with position cap (risk.rs)
- `pre_trade_check()` naively added hedge size to net exposure (`exposure + hedge_size > cap`), blocking hedges even though they REDUCE net exposure to zero.
- Added `is_hedge: bool` parameter; when true, skips the position cap check. Global halt, market halt, and balance checks still apply.
- This was the primary cause of stranded unhedged positions.

### Sell-back on hedge failure (live_engine.rs, handle_fill)
- When hedge FOK fails, bot now attempts to SELL back the original fill at market (FOK at $0.01) before killing the market.
- If sell-back succeeds: position flattened, market stays alive.
- If sell-back also fails: market killed (last resort).
- Previously: hedge failure → immediate kill → position stranded forever.

### Kill switch now flattens unhedged inventory (live_engine.rs, kill_market)
- New `flatten_unhedged()` method: syncs positions, computes directional excess (`abs(yes - no)`), sells excess side at market.
- Only sells the unhedged portion — balanced hedged inventory is left for normal ask exit.
- Called by `kill_market()` before removing the market from the managed set.
- Previously: kill only cancelled orders and removed market from tracking, leaving inventory on-chain permanently.

## 2026-03-11 — Main-first monitor reintegration onto current main

### Monitor stack ported onto `origin/main`
- Re-based the monitoring work onto fresh mainline application code instead of merging the older logging branch runtime wholesale.
- Restored the monitor workspace members (`crates/spreadeater-core`, `crates/spreadeater-monitor`), Docker bootstrap, shared event schema, JSONL writer/producer path, Postgres projector/API/TUI/dashboard, and monitor tests on top of the current bot.

### Schema and app-side observability updates
- Advanced the event schema to v1.1 with additive fields for decision budget/balance context, order origin/role, fill match source, hedge origin, and new monitor event families for risk state, user-stream status, status snapshots, and calibration adjustments.
- Wired the current `LiveEngine`, `OrderManager`, `FillHandler`, and legacy orchestrator dry-run paths back into the monitoring pipeline while keeping the trading hot path queue-based and monitor-isolated.

### Validation
- Re-ran workspace build/test coverage after the reintegration and updated fixtures for the new schema fields across the root core-type tests and monitor integration suite.
- Simplified the operator monitor startup commands to direct batch entrypoints so `start/open/restart` use the same `docker compose up -d` + `cargo run -p spreadeater-monitor -- serve ...` flow that already works manually, instead of relying on the earlier PowerShell wrapper/background process logic.

## 2026-03-11 — Book WebSocket: reconnection, real-time updates, REST reduction

### Problem
Book WebSocket (`BookWebSocket`) existed as dead code — defined but never instantiated. All book data came exclusively from REST `fetch_both_books()` in `evaluate_market()`, making 2N REST calls per cycle (20 calls for 10 markets every 60s = ~1,200 unnecessary calls/hour). The WS had no reconnection logic, no keepalive ping, and would exit permanently on first error.

### Fix (websocket.rs, live_engine.rs)
- **Reconnection with exponential backoff**: Book WS now mirrors UserStream pattern — retries on disconnect (1s → 2s → 4s → ... → 30s cap), resets backoff when connection is stable >60s.
- **Outbound ping keepalive**: Sends ping every 30s to detect stale/dead connections (matching UserStream). Uses `tokio::select!` for concurrent message read + ping.
- **Error-as-bail pattern**: WS errors now return `Err()` (triggering reconnect loop) instead of `Ok(())` (which caused permanent exit). Clean shutdown only when receiver is dropped.
- **LiveEngine integration**: Book WS started after initial cycle with all managed token IDs. Events processed in `select!` loop via `book_manager.apply_event()` — real-time snapshots + deltas maintain cache.
- **Automatic resubscription**: When managed market set changes (new markets discovered/dropped), book WS resubscribes with updated token IDs (same pattern as UserStream resubscription).
- **Cache-first evaluate_market()**: Now checks BookManager cache before REST fetch. Uses `max_book_age_secs` (30s) staleness threshold. Falls back to REST only when cache is empty or stale. Saves ~20 REST calls/cycle with 10 markets.
- **`get_book_token_ids()`**: New helper collects YES+NO token IDs from all managed markets for WS subscription.
- **`recv_book_event()`**: New free function (mirrors `recv_event`) for optional book channel in select! loop.

## 2026-03-11 — API verification & drift detection

### Problem
After FOK hedge, bot trusted HTTP response and placed asks based on synthetic expected positions — never verified via API that hedge actually filled or inventory existed. Between cycles, FillHandler used stale balance. Position drift from WS gaps or external trades went undetected.

### Fix (live_engine.rs, order_manager.rs)
- **Balance refresh after hedges**: FillHandler fetches fresh balance from API after each hedge execution, updates both `cached_balance` and `RiskManager`. Prevents stale risk checks when multiple fills arrive rapidly.
- **Post-hedge position verification**: After `sync_positions()`, verifies `net_exposure < 10% of fill_size`. If position is NOT hedged, kills market immediately (same escalation as hedge failure).
- **API-verified ask placement**: Replaces synthetic `expected_pos` with API-fetched position for ask sizing. Only places asks if both sides have inventory (`min_side > 0`). Prevents asks on phantom inventory.
- **Position drift detection**: New `detect_position_drift()` runs each cycle after `sync_positions()`. Detects phantom asks (asks resting but no API inventory → auto-cancels) and untracked positions (API shows inventory but no tracked orders → warns of external activity).
- **`cancel_asks_only()`**: New OrderManager method (mirrors `cancel_bids_only()`) for phantom ask cleanup.

## 2026-03-11 — Hedge-aware budget system

### Problem
Budget only checked bid cost (`price * size`), not total commitment including hedge. Example: YES bid at $0.40 for 25 shares costs $10 to place, but hedge costs $15 → total $25. Bot allowed this with only $10 budget, risking unhedgeable positions.

### Fix (order_manager.rs, live_engine.rs, risk.rs)
- **Hedge-aware budget**: New `committed_exposure()` tracks full notional per BUY order (`size` not `price * size`). `available_budget()` now uses exposure, preventing bids we can't hedge.
- **Cached balance**: `get_balance()` called once per cycle, cached in `Arc<RwLock<Decimal>>`, shared with FillHandler. Eliminates redundant API calls.
- **Risk gate**: `pre_trade_check()` now accepts optional `hedge_cost` — blocks BUY hedges when USDC balance is insufficient.
- **Low-balance warning**: FillHandler warns when cached balance < 2x hedge cost before attempting FOK.
- **Status log**: Now shows `order_exposure` alongside `order_committed`.

## 2026-03-11 — Fix balance endpoint (404 on /data/balance)

### Problem
`get_balance()` hit `GET /data/balance` which returned 404 ("page not found"). The bot silently fell back to config-budget-only mode, so API balance was never used as a hard exposure constraint.

### Fix (client.rs)
- Changed endpoint from `/data/balance` to `/balance-allowance?asset_type=COLLATERAL&signature_type=2`
- Verified from py-clob-client SDK (`GET_BALANCE_ALLOWANCE = "/balance-allowance"`) and Polymarket docs
- `signature_type=2` = Gnosis Safe (SpreadEater's wallet type)
- HMAC signs path only (`/balance-allowance`); query params stay in the URL only

## 2026-03-10 — Fix order amount precision for hedge/reconciliation orders

### Problem
Reconciliation hedges failed with 400: `"invalid amounts, the market buy orders maker amount supports a max accuracy of 2 decimals, taker amount a max of 4 decimals"`. Position API returns sizes with arbitrary precision (e.g. `73.59769`); the order signer computed raw micro-unit amounts without rounding to Polymarket's required precision.

### Fix (order_signer.rs, client.rs)
- Added `tick_size` parameter to `sign_order()` — passed through from `OrderRequest`
- Matches py-clob-client SDK rounding algorithm exactly:
  - Size rounded to 2 dp, price rounded to tick-size dp before computing amounts
  - Cross product (`price * size`) rounded to `tick_dp + 2` dp on the micro-unit result
- Added `round_decimal_down()`, `round_to_precision()`, and `tick_size_decimals()` helpers

## 2026-03-10 — Fix intermittent missed hedges (4 race conditions)

### Problem
Overnight, fills on resting bids intermittently failed to trigger hedges. Root cause: multiple race conditions between the FillHandler task and the periodic cycle.

### Fix 1: Cancellation→Fill race (live_engine.rs, order_manager.rs)
- WS Cancellation events now use `move_to_recently_cancelled()` (30s grace buffer) instead of hard-deleting via `remove_order()`
- Previously, when an order filled, the exchange sent both Trade + Cancellation events; if Cancellation was processed first, the order was deleted before FillHandler could match it
- Made `move_to_recently_cancelled` public on OrderManager

### Fix 2: Unmanaged market fallback (live_engine.rs, order_manager.rs)
- Added `opposite_token_id` field to `TrackedOrder` — stores the hedge target token at order creation time
- FillHandler now falls back to TrackedOrder data when market is not in `managed_markets` (removed from reward list)
- Previously returned early with "Fill on unmanaged market" warning and no hedge

### Fix 3: WebSocket reconnection backoff (user_stream.rs)
- Changed from fixed 5s reconnection delay to exponential backoff: 1s → 2s → 4s → ... → 30s max
- Backoff resets to 1s if prior connection lasted >60s (was healthy)
- Reduces typical reconnection gap from 5s to 1s

### Fix 4: Reconciliation scope (live_engine.rs)
- Added `known_markets: RwLock<HashMap<String, CanonicalMarket>>` to LiveEngine — populated during discovery, never pruned
- `reconcile_unhedged_positions` now checks all known markets, not just currently reward-eligible ones
- Previously, positions on markets that dropped out of the reward list were never reconciled

## 2026-03-09 — Instant hedge execution via dedicated fill handler task

### Problem
- `tokio::select!` loop processed one branch at a time — when `run_cycle()` was executing (5-30s of REST calls), fill events queued up and hedges couldn't fire until the cycle finished
- Every `place_order` call made a REST round-trip to `/fee-rate` (always returns 0), adding ~100-300ms latency to hedge execution

### Fix 1: FillHandler task (live_engine.rs)
- Created `FillHandler` struct that runs on a dedicated `tokio::spawn` task
- Trade fill events are forwarded via `mpsc::unbounded_channel` — non-blocking send
- FillHandler processes fills immediately, independently of periodic cycle/depth/refresh work
- Made `OrderManager`, `HedgeExecutor`, `BookRestClient` cloneable (`#[derive(Clone)]`) — all use Arc'd internal state, so clones share the same data

### Fix 2: Fee rate caching (client.rs)
- Added `fee_cache: Arc<RwLock<HashMap<String, (u64, Instant)>>>` to `TradingClient`
- `get_fee_rate_bps` checks cache first (5-minute TTL), skips REST call on cache hit
- Eliminates an HTTP round-trip from the hedge critical path

## 2026-03-09 — Configurable ask depth for trading PnL

### Ask pricing (live_engine.rs, config.rs)
- New `ask_depth_pct` config field controls how far from mid inventory asks are placed
- 0.0 = at mid (max reward score, no trading PnL) — previous hardcoded behavior
- 0.20 = 20% of max_spread above mid (default — balanced score + PnL)
- 1.0 = at max_spread boundary (max PnL, min score)
- `compute_ask_price()` now takes `ask_depth_pct` parameter: `target = mid + (ask_depth_pct * max_spread)`
- Score impact: `S = ((V - offset) / V)^2` — quadratic penalty for distance from mid

## 2026-03-09 — Rank markets by estimated reward before budget allocation

### Problem
- `run_cycle` iterated admitted markets in arbitrary discovery API order
- First market evaluated could consume the entire $300 budget via dynamic sizing
- Higher-reward markets evaluated later got nothing — a $0.15/day market could block a $5/day market

### Fix: Three-phase market loop (live_engine.rs)
- **Phase 1 (Evaluate):** Evaluate all admitted markets before placing any orders — level playing field for dynamic sizing
- **Phase 2 (Rank):** Sort by `estimated_reward` descending (daily_reward_total × score_share)
- **Phase 3 (Act):** Iterate in ranked order — high-reward markets consume budget first, low-reward markets get what's left
- `place_quotes` budget enforcement naturally caps lower-ranked markets
- Added `MarketEvaluation` struct to hold evaluation results between phases
- Ranked order logged each cycle for operational visibility

## 2026-03-09 — Fix cheap bid bypass in order sync

### Bug
- `sync_open_orders()` imported ALL resting orders from the exchange without checking `min_outcome_price`
- Orders from before the filter existed (or on markets dropped from discovery) persisted and could be filled
- User got filled at $0.005 despite $0.15 threshold

### Fix: Cheap bid cancellation at sync (order_manager.rs)
- `sync_open_orders()` now takes `min_outcome_price: Decimal` parameter
- Bids with `order.price < min_outcome_price` are cancelled on the exchange and skipped during import
- Cancellation happens after dropping the write lock to avoid holding it during API calls

### Fix: Tracked order sweep (order_manager.rs)
- New `cancel_cheap_bids(min_price)` method sweeps ALL tracked orders across all markets
- Cancels any bid priced below threshold and removes from tracking
- Called in `run_cycle()` immediately after `sync_open_orders()` — catches orders from prior cycles

### Threshold raised (config.json, config.rs)
- `min_outcome_price` default: $0.15 → $0.20

## 2026-03-09 — Commented config.json with lifecycle ordering

### Config comment support (main.rs)
- `load_config` now strips `//` line comments before JSON parsing
- String-aware: `//` inside quoted values is preserved
- Allows inline documentation directly in config.json

### Reorganized config.json
- Grouped by trade lifecycle: Discovery → Books → Strategy → Risk → Persistence
- Strategy sub-grouped: Quote Pricing, Entry Gates, Sizing, Hedge Cost Limits, Score Proxy
- Every field has an inline comment explaining what it does and how to tune it

## 2026-03-09 — Minimum outcome price filter

### Per-leg mid-price floor (quote_engine.rs, config.rs)
- New `min_outcome_price` config field (default $0.15) rejects bid legs where mid-price is below threshold
- Per-leg filter: only the cheap side is skipped, expensive side proceeds normally
- Fallback arms (bid-only or ask-only books) use best available price as mid proxy
- Hedge executor unaffected — FOK hedges into cheap outcomes still fire after fills

### Per-leg cancel method (order_manager.rs)
- New `cancel_leg(condition_id, leg)` method cancels orders for a specific QuoteLeg only
- Used instead of `cancel_bids_only` to avoid cancelling the profitable side's bid

### Reactive bid cancellation (live_engine.rs)
- `run_cycle`: after cancel-replace, scans for rejected bid legs and cancels resting orders on those legs
- `check_hedge_depth` (15s): checks own-book mid-price, cancels bid if mid drops below threshold between cycles
- `refresh_quotes` (30s): same rejected-leg scan as run_cycle for mid-cycle reaction

## 2026-03-09 — Fix ask placement reliability + colored log banners

### Ask REST fallback (live_engine.rs)
- `place_inventory_asks` now falls back to REST book fetch when cached books are `None` (WS disconnect overnight)
- Added warn/error logging for missing books and failed ask price computation
- Previously silently returned with no asks when books were missing from cache

### Preserve asks on market threshold drop (live_engine.rs)
- Markets dropping below `min_daily_reward` now only cancel bids when holding inventory — asks kept resting for rewards + position exit
- Previously `cancel_all` destroyed asks on hedged positions when reward threshold fluctuated

### Viable market ask catch-up (live_engine.rs)
- After `cancel_replace_if_drifted` in viable market path, explicitly call `place_inventory_asks` if inventory exists but no asks are tracked
- Catches asks that were never placed or silently lost

### Colored ANSI banners (live_engine.rs)
- Fill executed: bold blue banner
- Hedge OK: bold green banner
- Hedge failed: bold red banner
- Ask orders placed: bold cyan banner

## 2026-03-09 — Fix doc/config discrepancies

### Config defaults aligned (config.rs, config.json)
- `max_position_size` config.json: 10000 → 300; code default: 500 → 300
- `min_daily_reward` code default: 20 → 10 (matches config.json)
- `poll_interval_secs` code default: 300 → 60 (matches config.json)
- `default_quote_size` code default: 100 → 5 (matches config.json)

### handoff.md corrected
- Config JSON example updated to match actual config.json (min_daily_reward, poll_interval_secs, max_position_size)
- Added missing `min_est_daily`, `persistence` section, and `target_score_share` to config reference table
- Fixed cancel endpoint docs: `DELETE /order` (with body), `DELETE /cancel-market-orders` (with body)
- Discovery description updated: 60s interval, $10 reward threshold

### MEMORY.md corrected
- Fixed cancel endpoints (was `DELETE /order/{id}` and `POST /orders/cancel-market`)
- Fixed `min_score_share` default (was 0.01, actual is 0.0001)
- Added `export` CLI command, CSV export in reporting description
- Added `target_score_share` and `calibration_sample_size` to ScoreProxyConfig

## 2026-03-09 — Budget includes position cost + API balance

### Position-aware budget (live_engine.rs, positions.rs)
- `available_budget` now subtracts **position cost** (yes_size * avg_yes_price + no_size * avg_no_price) in addition to resting order cost.
- Previously only counted resting BUY orders — filled hedged pairs were invisible, causing over-allocation.
- Position sync moved before budget calculation in `run_cycle` so the numbers are accurate.

### API balance as hard constraint (client.rs, live_engine.rs)
- New `get_balance()` on TradingClient fetches actual USDC balance from `GET /data/balance`.
- Budget is now `min(config_budget, api_balance)` — actual cash is the hard floor.
- Status log and cycle log show `api_balance` alongside committed/budget for full visibility.
- Graceful fallback: if the balance endpoint fails, uses config budget only (warns).

## 2026-03-09 — Inventory asks: earn rewards on hedged positions

### Post-hedge ask placement from fill data (live_engine.rs)
- After a successful hedge, asks are now placed using **expected inventory computed from the fill event** (trigger size + hedge size) instead of querying the position API.
- Previously relied on `position_manager.get_position()` which may return stale data — Polymarket fills go through MATCHED→MINED→CONFIRMED, so the hedge leg may not be visible yet.
- Asks on both YES and NO inventory start earning scoring rewards immediately.

### Reconciliation ask placement (live_engine.rs)
- `execute_reconciliation_hedge` now calls `place_inventory_asks` after a successful reconciliation hedge.
- Previously, reconciliation only hedged but never placed asks — leaving hedged inventory idle with no resting orders earning rewards.

## 2026-03-09 — Reduce bid churn from staleness checks

### Separate staleness thresholds (live_engine.rs)
- `check_hedge_depth` now uses a **lenient** staleness threshold (4x `max_book_age_secs` = 120s) for preemptive bid cancellation.
- Previously used the same tight 30s as HedgeExecutor, causing bids to be cancelled on brief WS latency blips and re-placed next cycle — resetting the scoring clock and losing reward accrual time.

### HedgeExecutor: always attempt FOK (hedge_executor.rs)
- Removed staleness and depth pre-checks that blocked hedge attempts. FOK at 0.99/0.01 is the real safety — the exchange decides if depth exists.
- Previously, stale book or insufficient cached depth would reject the hedge *before trying*, leaving an unhedged position + triggering kill_market.
- Now logs warnings for stale/thin books but always sends the FOK. Either it fills (hedged) or it doesn't (kill_market still fires, but at least we tried).
- Removed unused `StrategyConfig` from `HedgeExecutor` struct.

### Discovery filter relaxed (config.json)
- `min_daily_reward` lowered from $20 to $10, roughly doubling the eligible market pool.

## 2026-03-09 — Hedge reliability: no silent failures

### check_hedge_depth hardened (live_engine.rs)
- Bids are now cancelled when the opposite book is **missing** (no cached data) — previously silently skipped, leaving an unhedgeable bid resting.
- Bids are now cancelled when the opposite book is **stale** (exceeds `max_book_age_secs`) — previously used stale data to validate depth, but `HedgeExecutor` would reject the stale book at fill time, causing a hedge failure.
- Both cases now log a WARN with reason before cancelling.

### Early WS subscription (live_engine.rs)
- UserStream WS is now subscribed to existing open orders **before** the initial `run_cycle()`.
- Previously, the first `run_cycle()` blocked for 10-30s (discovery + evaluation), during which fills on prior-session orders went undetected until `reconcile_unhedged_positions` ran on the next cycle (60s+ later).
- After the initial cycle completes, WS is re-subscribed only if the managed market set changed (preserves existing conditional re-subscribe logic).

## 2026-03-09 — Prominent fill/hedge logging

### Fill Visibility (live_engine.rs)
- Fill detection, hedge success, and hedge failure now log at `error!` level with `>>>>>>>>>>` banners for easy visual scanning in dense logs.
- Three distinct markers: `FILL EXECUTED`, `HEDGE OK`, `HEDGE FAILED — KILLING MARKET`.

## 2026-03-08 — Budget-aware dynamic sizing

### Dynamic Sizing (score_proxy.rs, live_engine.rs, orchestrator.rs)
- `compute_dynamic_size` now respects `max_size` on all early-return paths (previously returned bare `min_size` from Polymarket API, bypassing the clamp — root cause of 200-share orders on thin books).
- `evaluate_market` passes `available_budget()` (remaining capital) as `max_size` instead of `max_position_size`. Order sizes are now constrained by total exposure budget ($300), not a per-market cap.
- Shadow mode (`orchestrator.rs`) uses `max_total_exposure` as size ceiling since no real budget tracking exists.
- `max_position_size` raised to 10000 in config.json — now only serves as emergency halt threshold in risk.rs, no longer used for sizing.

## 2026-03-08 — Fix score proxy overestimation + size-drift resize

### Score Proxy Alignment (score_proxy.rs)
- Removed `two_sided_q_min` from `compute_score_proxy` and `compute_dynamic_size`.
- Both now use simple per-order score sums, matching the status log's `estimate_market_daily_reward`.
- Old formula compressed competitor scores via `two_sided_q_min`, inflating our estimated share and letting low-reward markets pass the `min_est_daily` gate.
- Removed dead `two_sided_q_min` function.

### Size-Drift Detection (order_manager.rs)
- `cancel_replace_if_drifted` now checks size drift (>50% difference) in addition to price drift.
- Orders from prior runs with oversized positions (e.g. 200 vs current max 10) are now resized on the next cycle.

## 2026-03-08 — Add min_est_daily reward gate

### Viability Filter (viability.rs, config.rs)
- New `min_est_daily` config field gates market entry on estimated daily reward alone (default: $0.25).
- Prevents one-time hedge profit from inflating viability edge and admitting low-reward markets.
## 2026-03-11 — Monitor ops expansion

### Monitor API and storage
- Added dedicated paginated monitor endpoints for `open-orders`, `inventory`, `watchlist`, `history`, `errors`, and `config`.
- Added Postgres-backed bot log ingestion tables and a monitor-side tailer that persists bot error lines and broadcasts them over websocket channel `errors`.

### Web operator console
- Reworked the monitor UI into a tabbed operator console: `Overview`, `Open Orders`, `Inventory`, `History`, `Errors`, `Watchlist`, and `Config`.
- Replaced overview-heavy scrolling with compact preview sections plus dense searchable/filterable tables built for hundreds of rows.
- Added read-only config inspection showing both the real JSON tree and a flattened key/value view.

### Config and docs
- Removed comments from the tracked root `config.json` so it is valid JSON.
- Added `CONFIG.md` to preserve human-readable explanations for every root config field.
- Updated the README with the redirected `RUST_LOG=error` launch pattern required for the Errors tab.

- Markets earning $0.02-$0.06/day will now be rejected, freeing capital for higher-reward markets.
- Existing `min_edge_threshold` still applies alongside the new check.

## 2026-03-08 — Hedge reconciliation + cancel-replace race fix

### Unhedged Position Reconciliation (live_engine.rs)
- New `reconcile_unhedged_positions` runs every cycle after position/order sync.
- Detects one-sided inventory (e.g. YES but no NO) with no resting bid — sign of a missed fill.
- Bootstraps fresh books and executes FOK hedge via HedgeExecutor.
- Catches fills that occurred during bot downtime (restart/deploy) or WebSocket gaps.

### Cancel-Replace Race Condition (order_manager.rs)
- Orders cancelled during cancel-replace/resize now move to a `recently_cancelled` grace buffer instead of being immediately deleted.
- `get_tracked_order` and `find_tracked_order` both check the grace buffer, so in-flight fill events arriving after cancel can still trigger hedges.
- Buffer auto-cleaned after 30 seconds each cycle.
- `find_tracked_order` fallback now also scans recently-cancelled orders.

## 2026-03-08 — Fix est_daily estimation + post-only crossing guard

### est_daily Estimation (live_engine.rs)
- `estimate_market_daily_reward` no longer uses `two_sided_q_min` for scoring. Each order earns independently — simple sum of per-order scores replaces Q1/Q2 split.
- Single-leg markets now show non-zero `est_daily` values (previously always $0.00).

### Post-Only Crossing Guard (order_manager.rs)
- "crosses book" 400 errors downgraded from `ERROR` to `WARN` since they're expected on fast-moving books.
- Bot already handled this gracefully (continues to next candidate); just reduced log noise.

## 2026-03-08 — WS parse logging + status log cleanup

### WebSocket Parse Logging (user_stream.rs)
- `parse_user_message` now logs every parse failure at `warn!` level (JSON errors, missing fields, invalid trade/order data) instead of silently returning None.
- Successfully parsed trade/order events logged at `info!` level with full context (`>>> WS TRADE EVENT received`, `>>> WS ORDER EVENT received`).
- Raw incoming messages logged at `debug!` level for troubleshooting.

### Status Log Cleanup (live_engine.rs)
- Status log now only shows markets with resting orders or inventory (skips idle markets).
- Summary line shows `active` and `idle` counts for visibility into total monitored set.

## 2026-03-08 — Fix fill detection + aggressive hedge execution

### Fill Detection (live_engine.rs)
- **Conditional UserStream re-subscription**: Only tear down/recreate WebSocket when managed market set actually changes. Prevents losing fills during reconnect gap that occurred every 5-min cycle.
- **Fallback fill matching**: `find_tracked_order` now falls back to matching by (condition_id, asset_id, side) when maker/taker order IDs are missing from trade events.
- **Diagnostic logging**: Unmatched trade events now emit a `warn!` with full context instead of being silently dropped.

### Aggressive Hedge Pricing (hedge_executor.rs)
- FOK hedge orders now use aggressive limit prices (0.99 for buys, 0.01 for sells) instead of tight slippage-buffer pricing.
- Depth walk kept as sanity check (rejects if zero liquidity), but no longer constrains the limit price.
- Guarantees fill on binary markets whenever any depth exists.

## 2026-03-08 — Fix duplicate orders on re-scan

- Added pagination support to `get_open_orders` (cursor-based)
- Fixed 401 from `LTE=` cursor encoding
- Conservative reconciliation: exchange-aware dedup guard

## 2026-03-08 — Score proxy and status logging

- Estimated daily reward per market in status logs
- Score proxy functions made public for use in LiveEngine
