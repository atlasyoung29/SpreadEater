#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_EVENTS_ROOT = REPO_ROOT / "data" / "events"


@dataclass
class OrderState:
    order_id: str
    role: str | None
    side: str | None
    total_size: Decimal
    matched_size: Decimal
    active: bool

    @property
    def remaining_size(self) -> Decimal:
        remaining = self.total_size - self.matched_size
        return remaining if remaining > Decimal("0") else Decimal("0")

    @property
    def is_live_reward_bid(self) -> bool:
        return self.active and self.role == "bid_entry" and self.side == "BUY"


@dataclass
class SnapshotState:
    timestamp: datetime
    total_est_daily_usd: Decimal | None
    total_committed_usd: Decimal
    available_budget_usd: Decimal
    competition_multiplier: Decimal
    api_balance_usd: Decimal
    bid_exposure_utilization_pct: Decimal
    bid_exposure_shares: Decimal
    live_bid_order_count: Decimal


class WeightedMean:
    def __init__(self) -> None:
        self.weighted_sum = Decimal("0")
        self.duration_seconds = Decimal("0")

    def add(self, value: Decimal | None, duration_seconds: Decimal) -> None:
        if value is None or duration_seconds <= 0:
            return
        self.weighted_sum += value * duration_seconds
        self.duration_seconds += duration_seconds

    def mean(self) -> Decimal | None:
        if self.duration_seconds <= 0:
            return None
        return self.weighted_sum / self.duration_seconds


class BenchmarkSummarizer:
    def __init__(
        self,
        metadata: dict[str, Any],
        events_path: Path,
        ended_at: datetime,
        reward_delta_usd: Decimal | None,
        note: str | None,
    ) -> None:
        self.metadata = metadata
        self.events_path = events_path
        self.ended_at = ended_at
        self.reward_delta_usd = reward_delta_usd
        self.note = note
        self.cash_reserve_usd = parse_decimal(metadata["cash_reserve_usd"])
        self.orders: dict[str, OrderState] = {}
        self.last_event_at = parse_iso_datetime(metadata["started_at"])
        self.last_snapshot: SnapshotState | None = None
        self.total_est_daily_mean = WeightedMean()
        self.total_committed_mean = WeightedMean()
        self.available_budget_mean = WeightedMean()
        self.competition_multiplier_mean = WeightedMean()
        self.api_balance_mean = WeightedMean()
        self.bid_utilization_mean = WeightedMean()
        self.reward_bid_exposure_share_seconds = Decimal("0")
        self.reward_bid_order_seconds = Decimal("0")
        self.bid_cancel_count = 0
        self.bid_resize_count = 0

    def summarize(self) -> dict[str, Any]:
        with self.events_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                event = json.loads(line)
                event_type = event.get("event_type")
                if event_type not in {
                    "status_snapshot",
                    "order_submitted",
                    "order_cancelled",
                    "order_resized",
                    "fill_detected",
                }:
                    continue
                event_at = parse_iso_datetime(event["occurred_at"])
                if event_at > self.last_event_at:
                    self.last_event_at = event_at
                self._process_event(event_type, event, event_at)

        self._extend_last_snapshot(self.last_event_at)
        return self._build_summary()

    def _process_event(
        self,
        event_type: str,
        event: dict[str, Any],
        event_at: datetime,
    ) -> None:
        payload = event.get("payload") or {}
        if event_type == "status_snapshot":
            self._handle_status_snapshot(payload, event_at)
            return

        self._advance_snapshot_to_event(event_at)
        if event_type == "order_submitted":
            self._handle_order_submitted(event, payload)
        elif event_type == "fill_detected":
            self._handle_fill_detected(event, payload)
        elif event_type == "order_cancelled":
            self._handle_order_cancelled(event)
        elif event_type == "order_resized":
            self._handle_order_resized(payload)
        self._refresh_last_snapshot_order_metrics()

    def _handle_status_snapshot(self, payload: dict[str, Any], event_at: datetime) -> None:
        api_balance_usd = parse_decimal(payload["api_balance_usd"])
        self._advance_snapshot_to_event(event_at)

        self.last_snapshot = SnapshotState(
            timestamp=event_at,
            total_est_daily_usd=parse_optional_decimal(payload.get("total_est_daily_usd")),
            total_committed_usd=parse_decimal(payload["total_committed_usd"]),
            available_budget_usd=parse_decimal(payload["available_budget_usd"]),
            competition_multiplier=parse_decimal(payload["competition_multiplier"]),
            api_balance_usd=api_balance_usd,
            bid_exposure_utilization_pct=Decimal("0"),
            bid_exposure_shares=Decimal("0"),
            live_bid_order_count=Decimal("0"),
        )
        self._refresh_last_snapshot_order_metrics()

    def _handle_order_submitted(
        self,
        event: dict[str, Any],
        payload: dict[str, Any],
    ) -> None:
        order_id = event.get("order_id")
        if not order_id:
            return
        remaining_size = parse_decimal(payload["size"])
        matched_size = parse_decimal(payload.get("matched_size", "0"))
        self.orders[order_id] = OrderState(
            order_id=order_id,
            role=payload.get("role"),
            side=payload.get("side"),
            total_size=remaining_size + matched_size,
            matched_size=matched_size,
            active=True,
        )

    def _handle_fill_detected(
        self,
        event: dict[str, Any],
        payload: dict[str, Any],
    ) -> None:
        order_id = event.get("order_id") or payload.get("anchored_order_id")
        if not order_id:
            return
        order = self.orders.get(order_id)
        if order is None:
            return
        order.matched_size += parse_decimal(payload["fill_size"])
        if order.remaining_size <= Decimal("0"):
            order.active = False

    def _handle_order_cancelled(self, event: dict[str, Any]) -> None:
        order_id = event.get("order_id")
        if not order_id:
            return
        order = self.orders.get(order_id)
        if order is None:
            return
        if order.is_live_reward_bid:
            self.bid_cancel_count += 1
        order.active = False

    def _handle_order_resized(self, payload: dict[str, Any]) -> None:
        old_order = self.orders.get(payload["old_order_id"])
        if old_order is None:
            return
        if old_order.is_live_reward_bid:
            self.bid_resize_count += 1
        old_order.active = False

    def _extend_last_snapshot(self, next_timestamp: datetime) -> None:
        if self.last_snapshot is None or next_timestamp <= self.last_snapshot.timestamp:
            return

        duration_seconds = decimal_seconds(next_timestamp - self.last_snapshot.timestamp)
        snapshot = self.last_snapshot
        self.total_est_daily_mean.add(snapshot.total_est_daily_usd, duration_seconds)
        self.total_committed_mean.add(snapshot.total_committed_usd, duration_seconds)
        self.available_budget_mean.add(snapshot.available_budget_usd, duration_seconds)
        self.competition_multiplier_mean.add(snapshot.competition_multiplier, duration_seconds)
        self.api_balance_mean.add(snapshot.api_balance_usd, duration_seconds)
        self.bid_utilization_mean.add(snapshot.bid_exposure_utilization_pct, duration_seconds)
        self.reward_bid_exposure_share_seconds += (
            snapshot.bid_exposure_shares * duration_seconds
        )
        self.reward_bid_order_seconds += snapshot.live_bid_order_count * duration_seconds
        self.last_snapshot.timestamp = next_timestamp

    def _advance_snapshot_to_event(self, event_at: datetime) -> None:
        if self.last_snapshot is None:
            return
        self._extend_last_snapshot(event_at)

    def _refresh_last_snapshot_order_metrics(self) -> None:
        if self.last_snapshot is None:
            return

        bid_exposure_shares = sum(
            order.remaining_size for order in self.orders.values() if order.is_live_reward_bid
        )
        live_bid_order_count = Decimal(
            sum(1 for order in self.orders.values() if order.is_live_reward_bid)
        )
        deployable_cash = self.last_snapshot.api_balance_usd - self.cash_reserve_usd
        if deployable_cash < Decimal("0"):
            deployable_cash = Decimal("0")
        if deployable_cash == Decimal("0"):
            bid_exposure_utilization_pct = Decimal("0")
        else:
            bid_exposure_utilization_pct = (bid_exposure_shares / deployable_cash) * Decimal(
                "100"
            )

        self.last_snapshot.bid_exposure_shares = bid_exposure_shares
        self.last_snapshot.live_bid_order_count = live_bid_order_count
        self.last_snapshot.bid_exposure_utilization_pct = bid_exposure_utilization_pct

    def _build_summary(self) -> dict[str, Any]:
        benchmark_window_duration_secs = decimal_seconds(
            self.ended_at - parse_iso_datetime(self.metadata["started_at"])
        )
        benchmark_window_hours = hours_from_seconds(benchmark_window_duration_secs)
        actual_reward_usd_per_hour = None
        if self.reward_delta_usd is not None and benchmark_window_hours not in (
            None,
            Decimal("0"),
        ):
            actual_reward_usd_per_hour = self.reward_delta_usd / benchmark_window_hours

        reward_bid_order_hours = hours_from_seconds(self.reward_bid_order_seconds)
        bid_churn_count = self.bid_cancel_count + self.bid_resize_count
        if benchmark_window_hours in (None, Decimal("0")):
            bid_churn_per_hour = None
        else:
            bid_churn_per_hour = Decimal(bid_churn_count) / benchmark_window_hours

        if reward_bid_order_hours in (None, Decimal("0")):
            bid_churn_per_live_bid_order_hour = None
        else:
            bid_churn_per_live_bid_order_hour = Decimal(
                bid_churn_count
            ) / reward_bid_order_hours

        summary = {
            "run_id": self.metadata["run_id"],
            "mode": self.metadata["mode"],
            "started_at": format_iso_datetime(parse_iso_datetime(self.metadata["started_at"])),
            "ended_at": format_iso_datetime(self.ended_at),
            "last_event_at": format_iso_datetime(self.last_event_at),
            "config_hash": self.metadata["config_hash"],
            "actual_reward_delta_usd": decimal_output(self.reward_delta_usd),
            "actual_reward_usd_per_hour": decimal_output(actual_reward_usd_per_hour),
            "mean_total_est_daily_usd": decimal_output(self.total_est_daily_mean.mean()),
            "mean_total_committed_usd": decimal_output(self.total_committed_mean.mean()),
            "mean_available_budget_usd": decimal_output(self.available_budget_mean.mean()),
            "mean_competition_multiplier": decimal_output(
                self.competition_multiplier_mean.mean()
            ),
            "mean_api_balance_usd": decimal_output(self.api_balance_mean.mean()),
            "mean_bid_exposure_utilization_pct": decimal_output(
                self.bid_utilization_mean.mean()
            ),
            "bid_cancel_count": self.bid_cancel_count,
            "bid_resize_count": self.bid_resize_count,
            "bid_churn_count": bid_churn_count,
            "bid_churn_per_hour": decimal_output(bid_churn_per_hour),
            "bid_churn_per_live_bid_order_hour": decimal_output(
                bid_churn_per_live_bid_order_hour
            ),
            "reward_bid_exposure_share_hours": decimal_output(
                hours_from_seconds(self.reward_bid_exposure_share_seconds)
            ),
            "reward_bid_order_hours": decimal_output(reward_bid_order_hours),
            "note": self.note,
            "event_stream_duration_secs": decimal_output(
                decimal_seconds(
                    self.last_event_at - parse_iso_datetime(self.metadata["started_at"])
                )
            ),
            "benchmark_window_duration_secs": decimal_output(benchmark_window_duration_secs),
        }
        return summary


def summarize_benchmark(
    run_id: str | None = None,
    metadata_path: Path | None = None,
    events_path: Path | None = None,
    ended_at: datetime | None = None,
    reward_delta_usd: Decimal | None = None,
    note: str | None = None,
) -> tuple[dict[str, Any], Path]:
    metadata_file = resolve_metadata_path(run_id, metadata_path)
    metadata = read_json(metadata_file)
    resolved_events_path = events_path or Path(metadata["events_path"])
    if not resolved_events_path.is_absolute():
        resolved_events_path = (REPO_ROOT / resolved_events_path).resolve()
    if not resolved_events_path.exists():
        raise FileNotFoundError(f"events file not found: {resolved_events_path}")

    ended_at = ended_at or datetime.now(timezone.utc)
    summarizer = BenchmarkSummarizer(
        metadata=metadata,
        events_path=resolved_events_path,
        ended_at=ended_at,
        reward_delta_usd=reward_delta_usd,
        note=note,
    )
    summary = summarizer.summarize()
    summary_path = resolved_events_path.parent / "benchmark_summary.json"
    write_json(summary_path, summary)
    return summary, summary_path


def resolve_metadata_path(run_id: str | None, metadata_path: Path | None) -> Path:
    if metadata_path is not None:
        return metadata_path
    if run_id is None:
        raise ValueError("expected either run_id or metadata_path")
    return DEFAULT_EVENTS_ROOT / run_id / "run_metadata.json"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize a SpreadEater benchmark run.")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--run-id")
    target.add_argument("--metadata-path", type=Path)
    parser.add_argument("--events-path", type=Path)
    parser.add_argument("--ended-at")
    parser.add_argument("--reward-delta-usd")
    parser.add_argument("--note")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        ended_at = parse_iso_datetime(args.ended_at) if args.ended_at else None
        reward_delta_usd = (
            Decimal(args.reward_delta_usd) if args.reward_delta_usd is not None else None
        )
        summary, summary_path = summarize_benchmark(
            run_id=args.run_id,
            metadata_path=args.metadata_path,
            events_path=args.events_path,
            ended_at=ended_at,
            reward_delta_usd=reward_delta_usd,
            note=args.note,
        )
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(summary_path)
    print(
        "actual_reward_usd_per_hour="
        f"{format_synopsis_value(summary.get('actual_reward_usd_per_hour'))} "
        "mean_bid_exposure_utilization_pct="
        f"{format_synopsis_value(summary.get('mean_bid_exposure_utilization_pct'))} "
        "bid_churn_per_hour="
        f"{format_synopsis_value(summary.get('bid_churn_per_hour'))}"
    )
    return 0


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")


def parse_iso_datetime(value: str) -> datetime:
    normalized = value.strip().replace("Z", "+00:00")
    normalized = normalize_fractional_seconds_precision(normalized)
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_fractional_seconds_precision(value: str) -> str:
    match = re.match(
        r"^(?P<prefix>.+?\.\d+)(?P<offset>Z|[+-]\d{2}:\d{2})?$",
        value,
    )
    if match is None:
        return value

    prefix = match.group("prefix")
    offset = match.group("offset") or ""
    head, fractional = prefix.split(".", 1)
    if len(fractional) <= 6:
        return value
    return f"{head}.{fractional[:6]}{offset}"


def format_iso_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def parse_optional_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return parse_decimal(value)


def decimal_seconds(delta) -> Decimal:
    return Decimal(str(delta.total_seconds()))


def hours_from_seconds(seconds: Decimal) -> Decimal | None:
    if seconds is None:
        return None
    return seconds / Decimal("3600")


def decimal_output(value: Decimal | None) -> float | None:
    if value is None:
        return None
    return float(value)


def format_synopsis_value(value: Any) -> str:
    return "null" if value is None else str(value)


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
