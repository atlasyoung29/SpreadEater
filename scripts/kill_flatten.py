"""Kill SpreadEater, cancel all resting orders, flatten positions, and optionally summarize a run."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import hmac
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parent.parent
CURRENT_RUN_PATH = REPO_ROOT / "data" / "current_run.json"
SUMMARIZER_PATH = Path(__file__).resolve().with_name("summarize_benchmark.py")
BASE = "https://clob.polymarket.com"
DATA_BASE = "https://data-api.polymarket.com"
EXPECTED_PROCESS_NAMES = {"spreadeater", "spreadeater.exe"}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summarize-run", action="store_true")
    parser.add_argument("--run-id")
    parser.add_argument("--reward-delta-usd")
    parser.add_argument("--note")
    parser.add_argument("--ended-at")
    return parser.parse_args(argv)


def load_env() -> dict[str, str]:
    load_dotenv(dotenv_path=REPO_ROOT / ".env")
    required = [
        "POLY_API_KEY",
        "POLY_SECRET",
        "POLY_PASSPHRASE",
        "POLY_ADDRESS",
        "POLY_PRIVATE_KEY",
        "POLY_FUNDER",
    ]
    return {name: os.environ[name] for name in required}


def sign(env: dict[str, str], method: str, path: str, body: str = "") -> dict[str, str]:
    timestamp = str(int(time.time()))
    message = f"{timestamp}{method}{path}{body}"
    secret_bytes = base64.urlsafe_b64decode(env["POLY_SECRET"].rstrip("=") + "==")
    signature = hmac.new(secret_bytes, message.encode(), hashlib.sha256).digest()
    return {
        "POLY_ADDRESS": env["POLY_ADDRESS"],
        "POLY_API_KEY": env["POLY_API_KEY"],
        "POLY_PASSPHRASE": env["POLY_PASSPHRASE"],
        "POLY_TIMESTAMP": timestamp,
        "POLY_NONCE": timestamp,
        "POLY_SIGNATURE": base64.urlsafe_b64encode(signature).decode(),
        "Content-Type": "application/json",
    }


def kill_spreadeater() -> None:
    print("Killing SpreadEater...")
    if os.name == "nt":
        result = subprocess.run(
            ["taskkill", "/F", "/IM", "spreadeater.exe"],
            capture_output=True,
            text=True,
            check=False,
        )
        print(result.stdout.strip() or result.stderr.strip() or "SpreadEater is not running.")
    elif sys.platform == "darwin":
        result = subprocess.run(
            ["pkill", "-x", "spreadeater"], capture_output=True, text=True, check=False
        )
        print("SpreadEater was killed." if result.returncode == 0 else "SpreadEater is not running.")
    else:
        result = subprocess.run(
            ["pkill", "-x", "spreadeater"], capture_output=True, text=True, check=False
        )
        print("SpreadEater was killed." if result.returncode == 0 else "SpreadEater is not running.")


def cancel_open_orders(env: dict[str, str]) -> int:
    print("\nFetching open orders...")
    path = "/data/orders"
    response = requests.get(f"{BASE}{path}", headers=sign(env, "GET", path))
    response.raise_for_status()
    orders = response.json().get("data", []) or []

    if not orders:
        print("No open orders found.")
        return 0

    print(f"Found {len(orders)} open order(s). Cancelling...")
    cancelled = 0
    for order in orders:
        order_id = order["id"]
        body = json.dumps({"orderID": order_id})
        try:
            cancel = requests.delete(
                f"{BASE}/order",
                headers=sign(env, "DELETE", "/order", body),
                data=body,
            )
            cancel.raise_for_status()
            print(f"  Cancelled {order_id[:12]}...")
            cancelled += 1
        except Exception as exc:  # pragma: no cover - network path
            print(f"  Failed to cancel {order_id[:12]}...: {exc}")
    return cancelled


def fetch_active_positions(env: dict[str, str]) -> list[dict[str, object]]:
    print("\nFetching positions...")
    response = requests.get(f"{DATA_BASE}/positions?user={env['POLY_FUNDER']}")
    response.raise_for_status()
    positions = response.json()
    return [position for position in positions if float(position.get("size", 0)) > 0]


def flatten_positions(env: dict[str, str]) -> tuple[int, int]:
    active = fetch_active_positions(env)
    if not active:
        print("No positions to flatten.")
        return (0, 0)

    print(f"Found {len(active)} position(s) to flatten:")
    for position in active:
        print(
            f"  {position.get('outcome', '?'):>3} "
            f"{float(position['size']):>10.2f}  "
            f"{position.get('title', position.get('conditionId', '?')[:40])}"
        )

    print("\nSelling all positions at market...")
    client, sell_side, asset_type, allowance_params, market_order_args = build_clob_client(env)

    print("\nSetting token allowances...")
    for position in active:
        token_id = position["asset"]
        try:
            client.update_balance_allowance(
                allowance_params(
                    asset_type=asset_type.CONDITIONAL,
                    token_id=token_id,
                    signature_type=2,
                )
            )
            print(f"  Allowance set for {token_id[:16]}...")
        except Exception as exc:  # pragma: no cover - network path
            print(f"  Warning: allowance failed for {token_id[:16]}...: {exc}")

    sold = 0
    failed = 0
    for position in active:
        token_id = position["asset"]
        size = float(position["size"])
        outcome = position.get("outcome", "?")
        condition = position.get("conditionId", "?")[:16]
        print(f"\n  Selling {size:.2f} {outcome} tokens ({condition}...)")
        try:
            signed_order = client.create_market_order(
                market_order_args(
                    token_id=token_id,
                    amount=size,
                    side=sell_side,
                )
            )
            response = client.post_order(signed_order)
            print(f"    OK: {response}")
            sold += 1
        except Exception as exc:  # pragma: no cover - network path
            print(f"    FAILED: {exc}")
            failed += 1
        time.sleep(0.5)

    print(f"\nFlattening complete: {sold} sold, {failed} failed.")
    return (sold, failed)


def build_clob_client(env: dict[str, str]):
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import (
            ApiCreds,
            AssetType,
            BalanceAllowanceParams,
            MarketOrderArgs,
        )
        from py_clob_client.order_builder.constants import SELL
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise RuntimeError(
            "py-clob-client not installed. Run: pip install py-clob-client"
        ) from exc

    client = ClobClient(
        host=BASE,
        key=env["POLY_PRIVATE_KEY"],
        chain_id=137,
        signature_type=2,
        funder=env["POLY_FUNDER"],
        creds=ApiCreds(
            api_key=env["POLY_API_KEY"],
            api_secret=env["POLY_SECRET"],
            api_passphrase=env["POLY_PASSPHRASE"],
        ),
    )
    return client, SELL, AssetType, BalanceAllowanceParams, MarketOrderArgs


def parse_current_run_pid(value: object) -> int | None:
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str) and value.isdigit():
        pid = int(value)
        if pid > 0:
            return pid
    return None


def normalize_process_name(name: str) -> str:
    return Path(name.strip().strip('"')).name.lower()


def is_expected_process_name(name: str) -> bool:
    return normalize_process_name(name) in EXPECTED_PROCESS_NAMES


def is_spreadeater_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        return is_windows_spreadeater_pid_running(pid)
    return is_posix_spreadeater_pid_running(pid)


def is_windows_spreadeater_pid_running(pid: int) -> bool:
    result = subprocess.run(
        ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return False

    for row in csv.reader(line for line in result.stdout.splitlines() if line.strip()):
        if len(row) < 2 or row[0].startswith("INFO:"):
            continue
        if row[1].strip('"') != str(pid):
            continue
        return is_expected_process_name(row[0])
    return False


def is_posix_spreadeater_pid_running(pid: int) -> bool:
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "comm="],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return False
    return is_expected_process_name(result.stdout)


def resolve_summary_run_id(args: argparse.Namespace) -> str | None:
    if not args.summarize_run:
        return args.run_id
    if args.run_id:
        return args.run_id
    metadata = read_current_run_metadata()
    if metadata is None:
        print(
            f"WARNING: {CURRENT_RUN_PATH} not found; skipping benchmark summarization.",
            file=sys.stderr,
        )
        return None
    run_id = metadata.get("run_id")
    if not run_id:
        print(
            f"WARNING: {CURRENT_RUN_PATH} does not contain run_id; skipping benchmark summarization.",
            file=sys.stderr,
        )
        return None
    pid = parse_current_run_pid(metadata.get("pid"))
    if pid is None:
        print(
            f"WARNING: {CURRENT_RUN_PATH} does not contain a valid pid; pass --run-id explicitly to summarize this run.",
            file=sys.stderr,
        )
        return None
    if not is_spreadeater_pid_running(pid):
        print(
            f"WARNING: {CURRENT_RUN_PATH} points to pid {pid}, but no live SpreadEater process matches it; pass --run-id explicitly to summarize a prior run.",
            file=sys.stderr,
        )
        return None
    return str(run_id)


def read_current_run_metadata() -> dict[str, object] | None:
    if not CURRENT_RUN_PATH.exists():
        return None
    with CURRENT_RUN_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def run_summarizer(
    run_id: str,
    reward_delta_usd: str | None = None,
    note: str | None = None,
    ended_at: str | None = None,
) -> subprocess.CompletedProcess[str]:
    command = [sys.executable, str(SUMMARIZER_PATH), "--run-id", run_id]
    if reward_delta_usd is not None:
        command.extend(["--reward-delta-usd", reward_delta_usd])
    if note is not None:
        command.extend(["--note", note])
    if ended_at is not None:
        command.extend(["--ended-at", ended_at])
    return subprocess.run(command, check=False, text=True)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_id = resolve_summary_run_id(args)
    env = load_env()

    kill_spreadeater()
    cancel_open_orders(env)
    flatten_positions(env)
    print("Done.")

    if not args.summarize_run:
        return 0
    if run_id is None:
        return 0

    print(f"\nSummarizing benchmark run {run_id}...")
    result = run_summarizer(
        run_id=run_id,
        reward_delta_usd=args.reward_delta_usd,
        note=args.note,
        ended_at=args.ended_at,
    )
    if result.returncode != 0:
        print("WARNING: benchmark summarizer failed.", file=sys.stderr)
        return result.returncode
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
