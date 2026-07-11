#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any


ADVISORY_DOWN_RATIO = Decimal("0.85")
ADVISORY_UP_RATIO = Decimal("1.20")


def compare_benchmarks(
    candidate_summary_path: Path,
    baseline_summary_path: Path,
) -> dict[str, Any]:
    candidate = read_json(candidate_summary_path)
    baseline = read_json(baseline_summary_path)
    return compare_summaries(candidate, baseline)


def compare_summaries(candidate: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    candidate_reward = parse_optional_decimal(candidate.get("actual_reward_usd_per_hour"))
    baseline_reward = parse_optional_decimal(baseline.get("actual_reward_usd_per_hour"))

    diagnostics: list[str] = []
    if candidate_reward is None or baseline_reward is None:
        diagnostics.append(
            "Primary reward comparison is incomplete because one or both summaries lack actual_reward_usd_per_hour."
        )
        return build_result(
            verdict="incomplete",
            candidate=candidate,
            baseline=baseline,
            diagnostics=diagnostics,
            primary_ratio=None,
        )

    if baseline_reward == Decimal("0"):
        primary_ratio = None
        verdict = "pass" if candidate_reward >= Decimal("0") else "incomplete"
        diagnostics.append(
            "Baseline actual_reward_usd_per_hour is zero, so the primary ratio is undefined."
        )
    else:
        primary_ratio = candidate_reward / baseline_reward
        if primary_ratio >= Decimal("0.95"):
            verdict = "pass"
        elif primary_ratio >= Decimal("0.85"):
            verdict = "warn"
        else:
            verdict = "fail"
        diagnostics.append(
            f"Candidate actual reward rate is {float(primary_ratio * Decimal('100')):.2f}% of baseline."
        )

    diagnostics.extend(build_advisories(candidate, baseline))
    return build_result(
        verdict=verdict,
        candidate=candidate,
        baseline=baseline,
        diagnostics=diagnostics,
        primary_ratio=primary_ratio,
    )


def build_advisories(candidate: dict[str, Any], baseline: dict[str, Any]) -> list[str]:
    advisories: list[str] = []

    utilization_ratio = ratio_or_none(
        candidate.get("mean_bid_exposure_utilization_pct"),
        baseline.get("mean_bid_exposure_utilization_pct"),
    )
    if utilization_ratio is not None and utilization_ratio < ADVISORY_DOWN_RATIO:
        advisories.append(
            "Advisory: mean_bid_exposure_utilization_pct is materially below baseline."
        )

    est_daily_ratio = ratio_or_none(
        candidate.get("mean_total_est_daily_usd"),
        baseline.get("mean_total_est_daily_usd"),
    )
    if est_daily_ratio is not None and est_daily_ratio < ADVISORY_DOWN_RATIO:
        advisories.append("Advisory: mean_total_est_daily_usd is materially below baseline.")

    churn_ratio = ratio_or_none(
        candidate.get("bid_churn_per_hour"),
        baseline.get("bid_churn_per_hour"),
    )
    if churn_ratio is None:
        baseline_churn = parse_optional_decimal(baseline.get("bid_churn_per_hour"))
        candidate_churn = parse_optional_decimal(candidate.get("bid_churn_per_hour"))
        if baseline_churn == Decimal("0") and candidate_churn not in (None, Decimal("0")):
            advisories.append("Advisory: bid_churn_per_hour increased from zero.")
    elif churn_ratio > ADVISORY_UP_RATIO:
        advisories.append("Advisory: bid_churn_per_hour is materially above baseline.")

    return advisories


def build_result(
    verdict: str,
    candidate: dict[str, Any],
    baseline: dict[str, Any],
    diagnostics: list[str],
    primary_ratio: Decimal | None,
) -> dict[str, Any]:
    metrics = [
        "actual_reward_usd_per_hour",
        "mean_bid_exposure_utilization_pct",
        "bid_churn_per_hour",
        "mean_total_est_daily_usd",
    ]
    deltas = {
        metric: delta_or_none(candidate.get(metric), baseline.get(metric)) for metric in metrics
    }
    return {
        "verdict": verdict,
        "primary_ratio": decimal_output(primary_ratio),
        "candidate_actual_reward_usd_per_hour": candidate.get("actual_reward_usd_per_hour"),
        "baseline_actual_reward_usd_per_hour": baseline.get("actual_reward_usd_per_hour"),
        "metric_deltas": deltas,
        "diagnostics": diagnostics,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare two benchmark summaries.")
    parser.add_argument("--candidate-summary", required=True, type=Path)
    parser.add_argument("--baseline-summary", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        result = compare_benchmarks(args.candidate_summary, args.baseline_summary)
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result, indent=2))
    return 0


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def parse_optional_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def ratio_or_none(candidate_value: Any, baseline_value: Any) -> Decimal | None:
    candidate_decimal = parse_optional_decimal(candidate_value)
    baseline_decimal = parse_optional_decimal(baseline_value)
    if candidate_decimal is None or baseline_decimal is None or baseline_decimal == Decimal("0"):
        return None
    return candidate_decimal / baseline_decimal


def delta_or_none(candidate_value: Any, baseline_value: Any) -> float | None:
    candidate_decimal = parse_optional_decimal(candidate_value)
    baseline_decimal = parse_optional_decimal(baseline_value)
    if candidate_decimal is None or baseline_decimal is None:
        return None
    return decimal_output(candidate_decimal - baseline_decimal)


def decimal_output(value: Decimal | None) -> float | None:
    if value is None:
        return None
    return float(value)


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
