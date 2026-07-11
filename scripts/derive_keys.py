"""
Derive fresh Polymarket CLOB API credentials from your MetaMask private key.

Usage:
  1. Export your private key from MetaMask:
     MetaMask > Account Details > Show Private Key
  2. Run: python derive_keys.py
  3. Paste the private key when prompted
  4. Copy the output into your .env file
"""

import getpass
import requests
from Crypto.Hash import keccak as keccak_mod
from py_clob_client.client import ClobClient

print("=== Polymarket API Key Derivation ===\n")
print("You need your MetaMask private key (the wallet connected to Polymarket).")
print("MetaMask > click ⋮ > Account Details > Show Private Key\n")

pk = getpass.getpass("Paste your MetaMask private key (hidden): ").strip()

# Ensure 0x prefix
if not pk.startswith("0x"):
    pk = "0x" + pk

client = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137,  # Polygon mainnet
    key=pk,
)

print("\nDeriving API credentials...")
creds = client.create_or_derive_api_creds()

print("\n=== Copy these into your .env file ===\n")
print(f"POLY_API_KEY={creds.api_key}")
print(f"POLY_SECRET={creds.api_secret}")
print(f"POLY_PASSPHRASE={creds.api_passphrase}")

# Look up the Gnosis Safe address (where Polymarket holds your funds)
eoa = client.get_address()
print(f"# EOA (MetaMask): {eoa}")

safe_addr = None
try:
    exchange = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
    k = keccak_mod.new(digest_bits=256, data=b"getSafeAddress(address)")
    selector = k.hexdigest()[:8]
    eoa_clean = eoa.lower().replace("0x", "").zfill(64)
    data = "0x" + selector + eoa_clean
    resp = requests.post(
        "https://1rpc.io/matic",
        json={"jsonrpc": "2.0", "method": "eth_call",
              "params": [{"to": exchange, "data": data}, "latest"], "id": 1},
        timeout=10,
    )
    result = resp.json().get("result", "0x")
    if len(result) >= 66:
        raw = result[-40:]
        if raw != "0" * 40:
            safe_addr = "0x" + raw
            print(f"# Gnosis Safe (on-chain): {safe_addr}")
except Exception as e:
    print(f"# Could not look up Gnosis Safe: {e}")

# Use Gnosis Safe if found, otherwise fall back to EOA
poly_addr = safe_addr if safe_addr else eoa
print(f"POLY_ADDRESS={poly_addr}")

print("\n=== Done! Run 'cargo run -- auth-check' to verify ===")
