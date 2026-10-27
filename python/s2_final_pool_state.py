"""PumpSwap pool state of every graduated census token at the end of the data (2026-09-18 15:17 UTC), pulled from Dune.

Study 2 uses it to value holdings still open at the end of the data. The state and fee rules are those of Study 1
(dune_pool_state_full.SQL). Reads data/bets_v2.parquet. Writes data/dune_state/final_pool_state.csv (p, b, f, pool).
Requires DUNE_API_KEY.
Run: python3 s2_final_pool_state.py
"""
import os
import numpy as np, pandas as pd
from config import DATA
from dune import run
from dune_pool_state import EV_DATES
from dune_pool_state_full import SQL, drop, upload_retry
D = DATA
END = int(pd.Timestamp("2026-09-18 15:17:00", tz="UTC").timestamp())
b = pd.read_parquet(f"{D}/bets_v2.parquet", columns=["mint", "pool"]).dropna().drop_duplicates("mint")
pools = pd.DataFrame({"pool": sorted(b.pool.unique())}); pools["pid"] = range(len(pools))
lk = pools.assign(lid=pools.pid, t_s=END)
out = f"{D}/dune_state/final_pool_state.csv"
pinfo = upload_retry(pools[["pid", "pool"]], "ms_pools_final")
try:
    info = upload_retry(lk[["lid", "pid", "t_s"]], "ms_lk_final")
    try:
        st, cost, nb, s = run(SQL.format(tbl=info["full_name"], ptbl=pinfo["full_name"], dates=EV_DATES), "medium", out + ".tmp", max_wait=7200)
    finally:
        drop(info["full_name"])
finally:
    drop(pinfo["full_name"])
print("final pool state:", len(lk), st, cost, nb, s.get("error"))
if st == "QUERY_STATE_COMPLETED":
    r = pd.read_csv(out + ".tmp"); assert len(r) == len(lk)
    r["pool"] = pools.pool.values
    r.to_csv(out, index=False); os.remove(out + ".tmp")
