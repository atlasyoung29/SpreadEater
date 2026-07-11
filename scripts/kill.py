"""Kill SpreadEater and cancel all resting orders on Polymarket."""

import os, time, hmac, hashlib, base64, json, subprocess, sys
from dotenv import load_dotenv
import requests

load_dotenv()

API_KEY = os.environ["POLY_API_KEY"]
SECRET = os.environ["POLY_SECRET"]
PASSPHRASE = os.environ["POLY_PASSPHRASE"]
ADDRESS = os.environ["POLY_ADDRESS"]
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


# 1. Fetch all open orders
print("Fetching open orders...")
path = "/data/orders"
resp = requests.get(f"{BASE}{path}", headers=sign("GET", path))
resp.raise_for_status()
orders = resp.json().get("data", []) or []

if not orders:
    print("No open orders found.")
else:
    print(f"Found {len(orders)} open order(s). Cancelling...")
    for order in orders:
        oid = order["id"]
        path = "/order"
        body = json.dumps({"orderID": oid})
        try:
            r = requests.delete(f"{BASE}{path}", headers=sign("DELETE", path, body), data=body)
            r.raise_for_status()
            print(f"  Cancelled {oid[:12]}...")
        except Exception as e:
            print(f"  Failed to cancel {oid[:12]}...: {e}")

# 2. Kill the process
if os.name == "nt":
    result = subprocess.run(["taskkill", "/F", "/IM", "spreadeater.exe"], capture_output=True, text=True)
    print(result.stdout.strip() or result.stderr.strip() or "SpreadEater is not running.")
elif sys.platform == "darwin":
    result = subprocess.run(["pkill", "-x", "spreadeater"], capture_output=True, text=True)
    print("SpreadEater was killed." if result.returncode == 0 else "SpreadEater is not running.")

print("Done.")
