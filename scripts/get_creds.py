from py_clob_client.client import ClobClient

private_key = input("Paste your Polygon wallet private key: ")

client = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137,
    key=private_key,
)

creds = client.create_or_derive_api_creds()
print(f"\nPaste this into SpreadEater/.env:\n")
print(f"POLY_API_KEY={creds.api_key}")
print(f"POLY_SECRET={creds.api_secret}")
print(f"POLY_PASSPHRASE={creds.api_passphrase}")
print(f"POLY_ADDRESS=<your wallet's public 0x address>")
