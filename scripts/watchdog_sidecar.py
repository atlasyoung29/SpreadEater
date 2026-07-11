"""External watchdog sidecar for SpreadEater.

Monitors the bot's heartbeat file. If the bot stops writing heartbeats
(process crashed or hung) and open positions exist, runs kill_flatten.py
to cancel orders and flatten all positions.

Usage:
    python scripts/watchdog_sidecar.py [--heartbeat ./data/watchdog_heartbeat]
                                        [--stale-threshold 60]
                                        [--check-interval 10]
                                        [--startup-grace 120]
"""

import os, sys, time, argparse, subprocess, hmac, hashlib, base64, json
from pathlib import Path
from dotenv import load_dotenv
import requests

load_dotenv()

FUNDER = os.environ.get("POLY_FUNDER", "")
API_KEY = os.environ.get("POLY_API_KEY", "")
SECRET = os.environ.get("POLY_SECRET", "")
PASSPHRASE = os.environ.get("POLY_PASSPHRASE", "")
ADDRESS = os.environ.get("POLY_ADDRESS", "")
DATA_BASE = "https://data-api.polymarket.com"
BASE = "https://clob.polymarket.com"


def sign(method, path, body=""):
    ts = str(int(time.time()))
    msg = f"{ts}{method}{path}{body}"
    secret_bytes = base64.urlsafe_b64decode(SECRET.rstrip("=") + "==")
    sig = hmac.new(secret_bytes, msg.encode(), hashlib.sha256).digest()
    return {
        "POLY_ADDRESS": ADDRESS,
        "POLY_API_KEY": API_KEY,
        "POLY_PASSPHRASE": PASSPHRASE,
        "POLY_TIMESTAMP": ts,
        "POLY_NONCE": ts,
        "POLY_SIGNATURE": base64.urlsafe_b64encode(sig).decode(),
        "Content-Type": "application/json",
    }


def has_open_positions():
    """Check if there are any open positions on Polymarket."""
    try:
        resp = requests.get(f"{DATA_BASE}/positions?user={FUNDER}", timeout=10)
        resp.raise_for_status()
        positions = resp.json()
        return any(float(p.get("size", 0)) > 0 for p in positions)
    except Exception as e:
        print(f"  Warning: Failed to check positions: {e}")
        # If we can't check, assume positions exist (safer)
        return True


def has_open_orders():
    """Check if there are any resting orders on Polymarket."""
    try:
        path = "/data/orders"
        resp = requests.get(f"{BASE}{path}", headers=sign("GET", path), timeout=10)
        resp.raise_for_status()
        orders = resp.json().get("data", []) or []
        return len(orders) > 0
    except Exception as e:
        print(f"  Warning: Failed to check orders: {e}")
        return True


def is_bot_running():
    """Check if the SpreadEater process is running."""
    if os.name == "nt":
        result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq spreadeater.exe"],
            capture_output=True, text=True
        )
        return "spreadeater.exe" in result.stdout
    else:
        result = subprocess.run(
            ["pgrep", "-x", "spreadeater"],
            capture_output=True, text=True
        )
        return result.returncode == 0


def read_heartbeat(heartbeat_path):
    """Read the heartbeat timestamp from the file. Returns None if unreadable."""
    try:
        content = Path(heartbeat_path).read_text().strip()
        return int(content)
    except (FileNotFoundError, ValueError, OSError):
        return None


def run_kill_flatten():
    """Run the kill_flatten.py script."""
    script_dir = Path(__file__).parent
    script = script_dir / "kill_flatten.py"
    print(f"\n{'='*60}")
    print(f"WATCHDOG SIDECAR: Triggering kill_flatten.py")
    print(f"{'='*60}\n")
    try:
        result = subprocess.run(
            [sys.executable, str(script)],
            timeout=120,
            capture_output=False,
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print("ERROR: kill_flatten.py timed out after 120s")
        return False
    except Exception as e:
        print(f"ERROR: Failed to run kill_flatten.py: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="SpreadEater watchdog sidecar")
    parser.add_argument(
        "--heartbeat",
        default="./data/watchdog_heartbeat",
        help="Path to heartbeat file (default: ./data/watchdog_heartbeat)",
    )
    parser.add_argument(
        "--stale-threshold",
        type=int,
        default=60,
        help="Seconds of stale heartbeat before triggering (default: 60)",
    )
    parser.add_argument(
        "--check-interval",
        type=int,
        default=10,
        help="Seconds between checks (default: 10)",
    )
    parser.add_argument(
        "--startup-grace",
        type=int,
        default=120,
        help="Seconds to wait for heartbeat file on startup (default: 120)",
    )
    args = parser.parse_args()

    print(f"SpreadEater Watchdog Sidecar")
    print(f"  Heartbeat file: {args.heartbeat}")
    print(f"  Stale threshold: {args.stale_threshold}s")
    print(f"  Check interval: {args.check_interval}s")
    print(f"  Startup grace: {args.startup_grace}s")
    print()

    # Wait for heartbeat file to appear (bot might be starting up)
    start = time.time()
    while not Path(args.heartbeat).exists():
        elapsed = time.time() - start
        if elapsed > args.startup_grace:
            print(f"Heartbeat file not found after {args.startup_grace}s grace period.")
            if has_open_positions() or has_open_orders():
                print("Open positions/orders detected — triggering kill_flatten.")
                run_kill_flatten()
            else:
                print("No open positions or orders. Bot may not be running.")
            return
        remaining = int(args.startup_grace - elapsed)
        print(f"  Waiting for heartbeat file... ({remaining}s remaining)")
        time.sleep(args.check_interval)

    print("Heartbeat file found. Monitoring...\n")
    triggered = False

    while not triggered:
        time.sleep(args.check_interval)

        ts = read_heartbeat(args.heartbeat)
        now = int(time.time())

        if ts is None:
            print(f"[{time.strftime('%H:%M:%S')}] Cannot read heartbeat file")
            continue

        age = now - ts
        if age <= args.stale_threshold:
            # Healthy
            continue

        # Heartbeat is stale
        print(f"\n[{time.strftime('%H:%M:%S')}] STALE heartbeat detected!")
        print(f"  Last heartbeat: {age}s ago (threshold: {args.stale_threshold}s)")

        # Check if bot is still running
        if is_bot_running():
            print("  Bot process is running but heartbeat stale — possible hang")
        else:
            print("  Bot process NOT running")

        # Check if there are positions/orders to protect
        if has_open_positions() or has_open_orders():
            print("  Open positions/orders detected — triggering emergency flatten")
            success = run_kill_flatten()
            if success:
                print("\nkill_flatten.py completed. Sidecar exiting.")
            else:
                print("\nkill_flatten.py failed. Please check manually!")
            triggered = True
        else:
            print("  No open positions or orders. Continuing to monitor...")


if __name__ == "__main__":
    main()
