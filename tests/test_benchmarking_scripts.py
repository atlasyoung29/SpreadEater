from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "benchmarking"


def load_script_module(name: str):
    path = SCRIPTS_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def make_event(
    event_type: str,
    occurred_at: str,
    payload: dict | None = None,
    order_id: str | None = None,
    run_id: str = "run_test",
    mode: str = "live",
) -> dict:
    return {
        "event_id": f"{event_type}-{occurred_at}",
        "schema_version": {"major": 1, "minor": 5},
        "event_type": event_type,
        "priority": "HIGH",
        "occurred_at": occurred_at,
        "recorded_at": occurred_at,
        "run_id": run_id,
        "cycle_id": None,
        "trace_id": None,
        "source_component": "test",
        "mode": mode,
        "condition_id": None,
        "market_slug": None,
        "question": None,
        "order_id": order_id,
        "asset_id": None,
        "hedge_id": None,
        "payload": payload or {},
    }


def write_events(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event))
            handle.write("\n")


def write_metadata(
    path: Path,
    run_id: str,
    started_at: str,
    events_path: Path,
    cash_reserve_usd: str = "20",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    metadata = {
        "run_id": run_id,
        "pid": 1234,
        "mode": "live",
        "started_at": started_at,
        "events_path": str(events_path),
        "event_log_dir": str(events_path.parent.parent),
        "config_path": str(path.parent.parent / "config.json"),
        "config_hash": "abc123",
        "cash_reserve_usd": cash_reserve_usd,
    }
    with path.open("w", encoding="utf-8") as handle:
        json.dump(metadata, handle)


class BenchmarkingScriptsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.summarizer = load_script_module("summarize_benchmark")
        cls.comparator = load_script_module("compare_benchmarks")
        cls.kill_flatten = load_script_module("kill_flatten")

    def create_run(
        self,
        events: list[dict],
        *,
        started_at: str = "2026-04-19T12:00:00Z",
        run_id: str = "run_test",
        cash_reserve_usd: str = "20",
    ) -> tuple[Path, Path, tempfile.TemporaryDirectory]:
        temp_dir = tempfile.TemporaryDirectory()
        root = Path(temp_dir.name)
        events_path = root / "data" / "events" / run_id / "events.jsonl"
        metadata_path = root / "data" / "events" / run_id / "run_metadata.json"
        write_events(events_path, events)
        write_metadata(
            metadata_path,
            run_id=run_id,
            started_at=started_at,
            events_path=events_path,
            cash_reserve_usd=cash_reserve_usd,
        )
        return metadata_path, events_path, temp_dir

    def test_summarizer_steady_reward_bid_no_fills(self) -> None:
        events = [
            make_event(
                "order_submitted",
                "2026-04-19T12:00:01Z",
                {
                    "side": "BUY",
                    "size": "10",
                    "matched_size": "0",
                    "role": "bid_entry",
                },
                order_id="order-1",
            ),
            make_event(
                "status_snapshot",
                "2026-04-19T12:00:02Z",
                {
                    "total_est_daily_usd": "2.0",
                    "total_committed_usd": "10",
                    "available_budget_usd": "90",
                    "competition_multiplier": "1.5",
                    "api_balance_usd": "100",
                },
            ),
            make_event(
                "status_snapshot",
                "2026-04-19T12:00:12Z",
                {
                    "total_est_daily_usd": "2.0",
                    "total_committed_usd": "10",
                    "available_budget_usd": "90",
                    "competition_multiplier": "1.5",
                    "api_balance_usd": "100",
                },
            ),
        ]
        metadata_path, _, temp_dir = self.create_run(events)
        self.addCleanup(temp_dir.cleanup)

        summary, summary_path = self.summarizer.summarize_benchmark(
            metadata_path=metadata_path,
            ended_at=datetime(2026, 4, 19, 12, 0, 20, tzinfo=timezone.utc),
        )

        self.assertTrue(summary_path.exists())
        self.assertAlmostEqual(summary["mean_bid_exposure_utilization_pct"], 12.5)
        self.assertEqual(summary["bid_churn_count"], 0)
        self.assertAlmostEqual(summary["reward_bid_exposure_share_hours"], 100 / 3600, places=8)

    def test_summarizer_partial_fill_reduces_live_bid_exposure(self) -> None:
        events = [
            make_event(
                "order_submitted",
                "2026-04-19T12:00:01Z",
                {
                    "side": "BUY",
                    "size": "10",
                    "matched_size": "0",
                    "role": "bid_entry",
                },
                order_id="order-1",
            ),
            make_event(
                "status_snapshot",
                "2026-04-19T12:00:02Z",
                {
                    "total_est_daily_usd": "2.0",
                    "total_committed_usd": "10",
                    "available_budget_usd": "90",
                    "competition_multiplier": "1.5",
                    "api_balance_usd": "100",
                },
            ),
            make_event(
                "fill_detected",
                "2026-04-19T12:00:08Z",
                {
                    "fill_size": "4",
                    "anchored_order_id": "order-1",
                },
                order_id="order-1",
            ),
            make_event(
                "status_snapshot",
                "2026-04-19T12:00:12Z",
                {
                    "total_est_daily_usd": "1.5",
                    "total_committed_usd": "6",
                    "available_budget_usd": "94",
                    "competition_multiplier": "1.5",
                    "api_balance_usd": "100",
                },
            ),
            make_event(
                "status_snapshot",
                "2026-04-19T12:00:20Z",
                {
                    "total_est_daily_usd": "1.5",
                    "total_committed_usd": "6",
                    "available_budget_usd": "94",
                    "competition_multiplier": "1.5",
                    "api_balance_usd": "100",
                },
            ),
        ]
        metadata_path, _, temp_dir = self.create_run(events)
        self.addCleanup(temp_dir.cleanup)

        summary, _ = self.summarizer.summarize_benchmark(
            metadata_path=metadata_path,
            ended_at=datetime(2026, 4, 19, 12, 0, 20, tzinfo=timezone.utc),
        )

        self.assertAlmostEqual(summary["mean_bid_exposure_utilization_pct"], 9.166666666666666)
        self.assertAlmostEqual(summary["reward_bid_exposure_share_hours"], 132 / 3600, places=8)

    def test_summarizer_cancel_replace_counts_bid_churn(self) -> None:
        events = [
            make_event(
                "order_submitted",
                "2026-04-19T12:00:01Z",
                {
                    "side": "BUY",
                    "size": "10",
                    "matched_size": "0",
                    "role": "bid_entry",
                },
                order_id="order-1",
            ),
            make_event(
                "status_snapshot",
                "2026-04-19T12:00:02Z",
                {
                    "total_est_daily_usd": "2.0",
                    "total_committed_usd": "10",
                    "available_budget_usd": "90",
                    "competition_multiplier": "1.5",
                    "api_balance_usd": "100",
                },
            ),
            make_event(
                "order_submitted",
                "2026-04-19T12:00:10Z",
                {
                    "side": "BUY",
                    "size": "8",
                    "matched_size": "0",
                    "role": "bid_entry",
                },
                order_id="order-2",
            ),
            make_event(
                "order_resized",
                "2026-04-19T12:00:11Z",
                {"old_order_id": "order-1", "new_order_id": "order-2"},
            ),
            make_event(
                "order_cancelled",
                "2026-04-19T12:00:16Z",
                {},
                order_id="order-2",
            ),
            make_event(
                "status_snapshot",
                "2026-04-19T12:00:20Z",
                {
                    "total_est_daily_usd": "1.0",
                    "total_committed_usd": "0",
                    "available_budget_usd": "100",
                    "competition_multiplier": "1.6",
                    "api_balance_usd": "100",
                },
            ),
        ]
        metadata_path, _, temp_dir = self.create_run(events)
        self.addCleanup(temp_dir.cleanup)

        summary, _ = self.summarizer.summarize_benchmark(
            metadata_path=metadata_path,
            ended_at=datetime(2026, 4, 19, 12, 1, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(summary["bid_resize_count"], 1)
        self.assertEqual(summary["bid_cancel_count"], 1)
        self.assertEqual(summary["bid_churn_count"], 2)
        self.assertGreater(summary["bid_churn_per_hour"], 0)
        self.assertAlmostEqual(summary["reward_bid_order_hours"], 15 / 3600, places=8)
        self.assertAlmostEqual(
            summary["bid_churn_per_live_bid_order_hour"],
            2 / (15 / 3600),
        )

    def test_summarizer_excludes_non_bid_orders_from_reward_metrics(self) -> None:
        events = [
            make_event(
                "order_submitted",
                "2026-04-19T12:00:01Z",
                {
                    "side": "SELL",
                    "size": "50",
                    "matched_size": "0",
                    "role": "ask_inventory",
                },
                order_id="order-ask",
            ),
            make_event(
                "status_snapshot",
                "2026-04-19T12:00:02Z",
                {
                    "total_est_daily_usd": "1.0",
                    "total_committed_usd": "50",
                    "available_budget_usd": "100",
                    "competition_multiplier": "1.5",
                    "api_balance_usd": "100",
                },
            ),
            make_event(
                "status_snapshot",
                "2026-04-19T12:00:12Z",
                {
                    "total_est_daily_usd": "1.0",
                    "total_committed_usd": "50",
                    "available_budget_usd": "100",
                    "competition_multiplier": "1.5",
                    "api_balance_usd": "100",
                },
            ),
        ]
        metadata_path, _, temp_dir = self.create_run(events)
        self.addCleanup(temp_dir.cleanup)

        summary, _ = self.summarizer.summarize_benchmark(
            metadata_path=metadata_path,
            ended_at=datetime(2026, 4, 19, 12, 0, 20, tzinfo=timezone.utc),
        )

        self.assertEqual(summary["reward_bid_exposure_share_hours"], 0.0)
        self.assertEqual(summary["reward_bid_order_hours"], 0.0)
        self.assertEqual(summary["mean_bid_exposure_utilization_pct"], 0.0)

    def test_summarizer_handles_zero_deployable_cash(self) -> None:
        events = [
            make_event(
                "order_submitted",
                "2026-04-19T12:00:01Z",
                {
                    "side": "BUY",
                    "size": "10",
                    "matched_size": "0",
                    "role": "bid_entry",
                },
                order_id="order-1",
            ),
            make_event(
                "status_snapshot",
                "2026-04-19T12:00:02Z",
                {
                    "total_est_daily_usd": "1.0",
                    "total_committed_usd": "10",
                    "available_budget_usd": "0",
                    "competition_multiplier": "1.5",
                    "api_balance_usd": "20",
                },
            ),
            make_event(
                "status_snapshot",
                "2026-04-19T12:00:12Z",
                {
                    "total_est_daily_usd": "1.0",
                    "total_committed_usd": "10",
                    "available_budget_usd": "0",
                    "competition_multiplier": "1.5",
                    "api_balance_usd": "20",
                },
            ),
        ]
        metadata_path, _, temp_dir = self.create_run(events, cash_reserve_usd="20")
        self.addCleanup(temp_dir.cleanup)

        summary, _ = self.summarizer.summarize_benchmark(
            metadata_path=metadata_path,
            ended_at=datetime(2026, 4, 19, 12, 0, 20, tzinfo=timezone.utc),
        )

        self.assertEqual(summary["mean_bid_exposure_utilization_pct"], 0.0)

    def test_summarizer_missing_manual_reward_delta_leaves_primary_metrics_nullable(self) -> None:
        events = [
            make_event(
                "status_snapshot",
                "2026-04-19T12:00:02Z",
                {
                    "total_est_daily_usd": "1.0",
                    "total_committed_usd": "0",
                    "available_budget_usd": "100",
                    "competition_multiplier": "1.5",
                    "api_balance_usd": "100",
                },
            ),
        ]
        metadata_path, _, temp_dir = self.create_run(events)
        self.addCleanup(temp_dir.cleanup)

        summary, _ = self.summarizer.summarize_benchmark(
            metadata_path=metadata_path,
            ended_at=datetime(2026, 4, 19, 12, 0, 20, tzinfo=timezone.utc),
        )

        self.assertIsNone(summary["actual_reward_delta_usd"])
        self.assertIsNone(summary["actual_reward_usd_per_hour"])

    def test_summarizer_accepts_rust_nanosecond_started_at_metadata(self) -> None:
        events = [
            make_event(
                "status_snapshot",
                "2026-04-19T12:00:28Z",
                {
                    "total_est_daily_usd": "1.0",
                    "total_committed_usd": "0",
                    "available_budget_usd": "100",
                    "competition_multiplier": "1.5",
                    "api_balance_usd": "100",
                },
            ),
        ]
        metadata_path, _, temp_dir = self.create_run(
            events,
            started_at="2026-04-19T12:00:27.102274200Z",
        )
        self.addCleanup(temp_dir.cleanup)

        summary, _ = self.summarizer.summarize_benchmark(
            metadata_path=metadata_path,
            ended_at=datetime(2026, 4, 19, 12, 0, 30, tzinfo=timezone.utc),
            reward_delta_usd=Decimal("0.01"),
        )

        self.assertEqual(summary["started_at"], "2026-04-19T12:00:27.102274Z")
        self.assertIsNotNone(summary["actual_reward_usd_per_hour"])

    def test_summarizer_fixture_e2e_produces_expected_summary(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        root = Path(temp_dir.name)
        run_id = "run_fixture"
        events_path = root / "data" / "events" / run_id / "events.jsonl"
        events_path.parent.mkdir(parents=True, exist_ok=True)
        events_path.write_text(
            (FIXTURES_DIR / "mini_events.jsonl").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        metadata_path = root / "data" / "events" / run_id / "run_metadata.json"
        write_metadata(
            metadata_path,
            run_id=run_id,
            started_at="2026-04-19T12:00:00Z",
            events_path=events_path,
            cash_reserve_usd="20",
        )

        summary, summary_path = self.summarizer.summarize_benchmark(
            metadata_path=metadata_path,
            ended_at=datetime(2026, 4, 19, 12, 1, 0, tzinfo=timezone.utc),
            reward_delta_usd=Decimal("1.5"),
            note="fixture",
        )

        self.assertTrue(summary_path.exists())
        self.assertEqual(summary["note"], "fixture")
        self.assertAlmostEqual(summary["mean_bid_exposure_utilization_pct"], 7.96875)
        self.assertAlmostEqual(summary["reward_bid_exposure_share_hours"], 204 / 3600, places=8)
        self.assertAlmostEqual(summary["reward_bid_order_hours"], 29 / 3600, places=8)
        self.assertEqual(summary["bid_cancel_count"], 1)
        self.assertEqual(summary["bid_resize_count"], 1)
        self.assertEqual(summary["bid_churn_count"], 2)
        self.assertAlmostEqual(summary["actual_reward_usd_per_hour"], 90.0)

    def test_comparator_verdict_matrix(self) -> None:
        baseline = {
            "actual_reward_usd_per_hour": 10.0,
            "mean_bid_exposure_utilization_pct": 50.0,
            "bid_churn_per_hour": 1.0,
            "mean_total_est_daily_usd": 2.0,
        }

        passed = self.comparator.compare_summaries(
            {**baseline, "actual_reward_usd_per_hour": 9.6},
            baseline,
        )
        warned = self.comparator.compare_summaries(
            {**baseline, "actual_reward_usd_per_hour": 8.6},
            baseline,
        )
        failed = self.comparator.compare_summaries(
            {**baseline, "actual_reward_usd_per_hour": 8.4},
            baseline,
        )
        incomplete = self.comparator.compare_summaries(
            {**baseline, "actual_reward_usd_per_hour": None},
            baseline,
        )

        self.assertEqual(passed["verdict"], "pass")
        self.assertEqual(warned["verdict"], "warn")
        self.assertEqual(failed["verdict"], "fail")
        self.assertEqual(incomplete["verdict"], "incomplete")

    def test_kill_flatten_summarize_run_uses_current_run_file(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        root = Path(temp_dir.name)
        current_run_path = root / "data" / "current_run.json"
        current_run_path.parent.mkdir(parents=True, exist_ok=True)
        current_run_path.write_text(json.dumps({"run_id": "run_exact", "pid": 4242}), encoding="utf-8")

        with mock.patch.object(self.kill_flatten, "CURRENT_RUN_PATH", current_run_path), mock.patch.object(
            self.kill_flatten, "is_spreadeater_pid_running", return_value=True
        ), mock.patch.object(
            self.kill_flatten, "load_env", return_value={"POLY_FUNDER": "0x0"}
        ), mock.patch.object(self.kill_flatten, "kill_spreadeater"), mock.patch.object(
            self.kill_flatten, "cancel_open_orders"
        ), mock.patch.object(self.kill_flatten, "flatten_positions"), mock.patch.object(
            self.kill_flatten, "run_summarizer", return_value=mock.Mock(returncode=0)
        ) as run_summarizer:
            result = self.kill_flatten.main(
                ["--summarize-run", "--reward-delta-usd", "1.25", "--note", "exact"]
            )

        self.assertEqual(result, 0)
        run_summarizer.assert_called_once_with(
            run_id="run_exact",
            reward_delta_usd="1.25",
            note="exact",
            ended_at=None,
        )

    def test_kill_flatten_summarize_run_skips_stale_current_run_pid(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        root = Path(temp_dir.name)
        current_run_path = root / "data" / "current_run.json"
        current_run_path.parent.mkdir(parents=True, exist_ok=True)
        current_run_path.write_text(json.dumps({"run_id": "run_exact", "pid": 4242}), encoding="utf-8")

        with mock.patch.object(self.kill_flatten, "CURRENT_RUN_PATH", current_run_path), mock.patch.object(
            self.kill_flatten, "is_spreadeater_pid_running", return_value=False
        ), mock.patch.object(
            self.kill_flatten, "load_env", return_value={"POLY_FUNDER": "0x0"}
        ), mock.patch.object(self.kill_flatten, "kill_spreadeater"), mock.patch.object(
            self.kill_flatten, "cancel_open_orders"
        ), mock.patch.object(self.kill_flatten, "flatten_positions"), mock.patch.object(
            self.kill_flatten, "run_summarizer", return_value=mock.Mock(returncode=0)
        ) as run_summarizer:
            result = self.kill_flatten.main(["--summarize-run", "--reward-delta-usd", "1.25"])

        self.assertEqual(result, 0)
        run_summarizer.assert_not_called()


if __name__ == "__main__":
    unittest.main()
