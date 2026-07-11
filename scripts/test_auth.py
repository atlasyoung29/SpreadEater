"""Test if Polymarket API credentials work using the official Python client."""
import getpass
from py_clob_client.client import ClobClient

pk = getpass.getpass("Paste your MetaMask private key (hidden): ").strip()
if not pk.startswith("0x"):
    pk = "0x" + pk

client = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137,
    key=pk,
    signature_type=2,  # POLY_GNOSIS_SAFE
)

# Derive creds
creds = client.create_or_derive_api_creds()
print(f"API Key: {creds.api_key}")
print(f"Secret: {creds.api_secret}")
print(f"Passphrase: {creds.api_passphrase}")

# Set creds on client
client.set_api_creds(creds)

print("\nTesting GET /data/orders...")
try:
    orders = client.get_orders()
    print(f"SUCCESS! Got {len(orders)} open orders")
except Exception as e:
    print(f"FAILED: {e}")

print("\nTesting get_address (proxy wallet)...")
try:
    addr = client.get_address()
    print(f"Proxy address: {addr}")
except Exception as e:
    print(f"FAILED: {e}")
