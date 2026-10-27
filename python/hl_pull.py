"""Download Hyperliquid fills for 8 August to 18 September 2026 and store one compact parquet file per hour.

Source: Hyperliquid's public node archive on S3 (bucket hl-mainnet-node-data, ap-northeast-1, prefix
node_fills_by_block/hourly/). The bucket is requester-pays, so the data transfer is billed to the AWS account whose
credentials you use. Credentials come from the standard boto3 chain (environment variables, shared config or profile).
Each hourly lz4 file is decompressed in memory and only the fill fields used later are kept; the raw file is not stored.
The script stops once 52 GB have been downloaded (CAP). It is resumable: an hour whose parquet already exists is skipped.
Writes DATA/hl/fills/<YYYYMMDD>_<HH>.parquet and tallies downloaded bytes per key in DATA/hl/bytes.log.
Run: python3 hl_pull.py
"""
import datetime as dt
import fcntl
import json
import os
import sys
from multiprocessing import Pool

import boto3
import lz4.frame
import pyarrow as pa
import pyarrow.parquet as pq

from config import DATA

OUT = f"{DATA}/hl/fills"
LOG = f"{DATA}/hl/bytes.log"
CAP = 52e9
B = "hl-mainnet-node-data"
try:
    import orjson as _j
    loads = _j.loads
except ImportError:
    loads = json.loads


def spent():
    if not os.path.exists(LOG):
        return 0
    return sum(int(l.split()[1]) for l in open(LOG) if l.strip())


def record(key, n):
    with open(LOG, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.write(f"{key} {n}\n")


def job(key):
    d, h = key.split("/")[-2], key.split("/")[-1].split(".")[0]
    out = f"{OUT}/{d}_{int(h):02d}.parquet"
    if os.path.exists(out):
        return key, 0, "skip"
    if spent() >= CAP:
        return key, 0, "CAP"
    s3 = boto3.Session(region_name="ap-northeast-1").client("s3")
    body = s3.get_object(Bucket=B, Key=key, RequestPayer="requester")["Body"].read()
    record(key, len(body))
    cols = {k: [] for k in ("user", "coin", "time", "px", "sz", "side", "start", "dir", "closed_pnl", "fee",
                            "builder_fee", "crossed", "liq", "block_time")}
    for line in lz4.frame.decompress(body).splitlines():
        blk = loads(line)
        for u, f in blk["events"]:
            cols["user"].append(u); cols["coin"].append(f["coin"]); cols["time"].append(f["time"])
            cols["px"].append(float(f["px"])); cols["sz"].append(float(f["sz"])); cols["side"].append(f["side"])
            cols["start"].append(float(f["startPosition"])); cols["dir"].append(f["dir"])
            cols["closed_pnl"].append(float(f["closedPnl"])); cols["fee"].append(float(f["fee"]))
            cols["builder_fee"].append(float(f.get("builderFee") or 0)); cols["crossed"].append(bool(f["crossed"]))
            cols["liq"].append(f.get("liquidation") is not None); cols["block_time"].append(blk["block_time"])
    pq.write_table(pa.table(cols), out + ".tmp", compression="zstd")
    os.replace(out + ".tmp", out)
    return key, len(body), "ok"


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    keys, d = [], dt.date(2026, 8, 8)
    while d <= dt.date(2026, 9, 18):
        keys += [f"node_fills_by_block/hourly/{d:%Y%m%d}/{h}.lz4" for h in range(24)]
        d += dt.timedelta(days=1)
    with Pool(4) as p:
        for i, (k, n, st) in enumerate(p.imap_unordered(job, keys), 1):
            if st == "CAP":
                print("CAP REACHED - stopping", flush=True); p.terminate(); sys.exit(2)
            if i % 24 == 0:
                print(f"{i}/{len(keys)} hours | downloaded {spent() / 1e9:.2f} GB (~${spent() / 1e9 * 0.114:.2f})", flush=True)
    print(f"DONE | downloaded {spent() / 1e9:.2f} GB (~${spent() / 1e9 * 0.114:.2f})", flush=True)
