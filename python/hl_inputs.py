"""Hyperliquid inputs besides the fills (hl_pull.py):
  hourly asset contexts (funding rate and oracle price per coin) for 7 August to 19 September 2026 from Hyperliquid's public
  archive (bucket hyperliquid-archive, requester pays, so transfer is billed to the caller's AWS account), written to
  DATA/hl/asset_ctxs/<YYYYMMDD>.csv;
  the list of vault addresses from Hyperliquid's public stats endpoint, written to DATA/hl/vault_addresses.json. The list
  is the current one, so a later download can include vaults created after the study.
AWS credentials come from the standard boto3 chain.
Run: python3 hl_inputs.py"""
import datetime as dt
import json
import os

import boto3
import lz4.frame
import requests

from config import DATA

D = f"{DATA}/hl"
os.makedirs(f"{D}/asset_ctxs", exist_ok=True)
s3 = boto3.Session(region_name="us-east-1").client("s3")
d = dt.date(2026, 8, 7)
while d <= dt.date(2026, 9, 19):
    out = f"{D}/asset_ctxs/{d:%Y%m%d}.csv"
    if not os.path.exists(out):
        b = s3.get_object(Bucket="hyperliquid-archive", Key=f"asset_ctxs/{d:%Y%m%d}.csv.lz4", RequestPayer="requester")["Body"].read()
        open(out, "wb").write(lz4.frame.decompress(b))
    d += dt.timedelta(days=1)

v = requests.get("https://stats-data.hyperliquid.xyz/Mainnet/vaults", timeout=120).json()
addrs = sorted({(x.get("summary") or x).get("vaultAddress", "").lower() for x in v} - {""})
json.dump(addrs, open(f"{D}/vault_addresses.json", "w"))
print(f"{len(addrs)} vault addresses")
