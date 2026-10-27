"""Dune pool-phase pulls for tokens that graduated without a recorded graduation event (Study 1 registration, Erratum 3).

The tokens and lookups come from final_inputs.py. The SQL and rules are those of the main pulls (dune_pool_state_full.SQL,
dune_entry_fee_and_peak.SQL_FEE and SQL_PEAK):
  A. pool state at every extra Study 1 lookup and at the end of the data for each of these pools, written to
     dune_state/extra_state.csv (lid, p, b, f) and dune_state/final_pool_state_extra.csv (pool, p, b, f; the format of
     final_pool_state.csv);
  B. buy fee at the extra pool-phase entries, written to dune_state/extra_buyfee.csv (lid, fb);
  C. pool peak per extra bet, written to dune_state/extra_peak.csv (bet_id, p, b).
All paths are under data/. An output that already exists is not pulled again. Requires DUNE_API_KEY.
Run: python3 dune_final_inputs.py
"""
import os

import pandas as pd

from config import DATA
from dune import run
from dune_entry_fee_and_peak import SQL_FEE, SQL_PEAK
from dune_pool_state import EV_DATES
from dune_pool_state_full import SQL, drop, upload_retry

D = DATA
OUT = f"{D}/dune_state"

lk = pd.read_parquet(f"{D}/pool_lookups_extra.parquet")
fin = pd.read_parquet(f"{D}/pool_lookups_final_extra.parquet")
lkb = pd.read_parquet(f"{D}/pool_lookups_by_bet_extra.parquet")
pkw = pd.read_parquet(f"{D}/pool_peak_windows_extra.parquet")
pools = pd.DataFrame({"pool": sorted(set(lk.pool) | set(fin.pool))})
pools["pid"] = range(len(pools))
total = 0.0
pinfo = upload_retry(pools[["pid", "pool"]], "ms_pools_e3")
try:
    out = f"{OUT}/extra_state.csv"
    if not os.path.exists(out):
        allk = pd.concat([lk, fin], ignore_index=True).merge(pools, on="pool").sort_values("lid")
        info = upload_retry(allk[["lid", "pid", "t_s"]], "ms_lk_e3")
        try:
            st, cost, nb, s = run(SQL.format(tbl=info["full_name"], ptbl=pinfo["full_name"], dates=EV_DATES), "medium", out + ".tmp", max_wait=7200)
        finally:
            drop(info["full_name"])
        print("state:", len(allk), st, cost, nb, s.get("error"), flush=True)
        total += float(cost or 0)
        if st == "QUERY_STATE_COMPLETED":
            r = pd.read_csv(out + ".tmp")
            assert len(r) == len(allk)
            r.insert(0, "lid", allk.lid.values)
            r.insert(1, "pool", allk.pool.values)
            r[r.lid >= 20_000_000].drop(columns="lid").to_csv(f"{OUT}/final_pool_state_extra.csv", index=False)
            r[r.lid < 20_000_000].drop(columns="pool").to_csv(out, index=False)
            os.remove(out + ".tmp")
    out = f"{OUT}/extra_buyfee.csv"
    if not os.path.exists(out):
        e = lkb[lkb.role == "t_entry"][["lid", "pool", "t_s"]].drop_duplicates("lid").merge(pools, on="pool")
        info = upload_retry(e[["lid", "pid", "t_s"]], "ms_entry_e3")
        try:
            st, cost, nb, s = run(SQL_FEE.format(tbl=info["full_name"], ptbl=pinfo["full_name"], dates=EV_DATES), "medium", out + ".tmp", max_wait=7200)
        finally:
            drop(info["full_name"])
        print("entry fee:", len(e), st, cost, nb, s.get("error"), flush=True)
        total += float(cost or 0)
        if st == "QUERY_STATE_COMPLETED":
            os.replace(out + ".tmp", out)
    out = f"{OUT}/extra_peak.csv"
    if not os.path.exists(out):
        b = pkw.merge(pools, on="pool")
        info = upload_retry(b[["bet_id", "pid", "t0", "t1"]], "ms_peakwin_e3")
        try:
            st, cost, nb, s = run(SQL_PEAK.format(tbl=info["full_name"], ptbl=pinfo["full_name"], dates=EV_DATES), "medium", out + ".tmp", max_wait=7200)
        finally:
            drop(info["full_name"])
        print("pool peak:", len(b), st, cost, nb, s.get("error"), flush=True)
        total += float(cost or 0)
        if st == "QUERY_STATE_COMPLETED":
            os.replace(out + ".tmp", out)
finally:
    drop(pinfo["full_name"])
print(f"credits this run: {total:.1f}", flush=True)
