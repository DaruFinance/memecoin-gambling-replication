"""Pools for dune_s2_full.py: census tokens that completed their bonding curve by the end of the data but are missing
from the local pool list, because their graduation fell in a local collection outage.

Step A finds each such token's PumpSwap SOL pools created by the end of the data and counts each pool's swaps; the pool
with the most swaps is the token's pool, the rule of Erratum S2-E3 for graduations without a recorded event. Output:
DATA/dune_state/s2full_pools_missing.csv (mint, pool, n).
Step B queries each of these pools' final state at the end of the data with the same SQL as final_pool_state.csv
(dune_pool_state_full.SQL). Output: DATA/dune_state/s2full_pools_extra.csv (mint, pool, p, b, f).
Finished steps are skipped on rerun.

Run: DUNE_API_KEY=... python dune_s2_full_pools.py
"""
import os
import sys

import pandas as pd

from dune import run
from dune_pool_state import EV_DATES
from dune_pool_state_full import SQL as SQL_STATE, drop, upload_retry
from dune_s2_full import DATES, END, SOL, inputs
from config import DATA

OUT = f"{DATA}/dune_state"
WSOL = "So11111111111111111111111111111111111111112"
END_S = 1789744620  # 2026-09-18 15:17:00 UTC

SQL_MISS = f"""
WITH cr AS (
  SELECT mint, min_by(quote_mint, (evt_block_slot, evt_tx_index, evt_inner_instruction_index)) AS qm, min(evt_block_time) AS t_create
  FROM pumpdotfun_solana.pump_evt_createevent
  WHERE evt_block_date BETWEEN DATE '2026-08-08' AND DATE '2026-09-10' GROUP BY 1),
tok AS (SELECT mint FROM cr WHERE t_create >= TIMESTAMP '2026-08-08 21:16:00' AND t_create < TIMESTAMP '2026-09-10 15:17:00'
          AND coalesce(qm, '{SOL}') = '{SOL}'),
cp AS (SELECT DISTINCT mint FROM pumpdotfun_solana.pump_evt_completeevent WHERE {DATES} AND evt_block_time <= {END}),
miss AS (SELECT tok.mint FROM tok JOIN cp ON cp.mint = tok.mint LEFT JOIN {{pools}} pl ON pl.mint = tok.mint WHERE pl.mint IS NULL),
pc AS (SELECT p.base_mint AS mint, p.pool FROM pumpdotfun_solana.pump_amm_evt_createpoolevent p JOIN miss ON miss.mint = p.base_mint
       WHERE p.{DATES} AND p.evt_block_time <= {END} AND p.quote_mint = '{WSOL}'),
sw AS (SELECT pool, count(*) AS n FROM (
         SELECT pool FROM pumpdotfun_solana.pump_amm_evt_buyevent WHERE {DATES} AND evt_block_time <= {END} AND pool IN (SELECT pool FROM pc)
         UNION ALL
         SELECT pool FROM pumpdotfun_solana.pump_amm_evt_sellevent WHERE {DATES} AND evt_block_time <= {END} AND pool IN (SELECT pool FROM pc))
       GROUP BY 1)
SELECT miss.mint, pc.pool, coalesce(sw.n, 0) AS n FROM miss LEFT JOIN pc ON pc.mint = miss.mint LEFT JOIN sw ON sw.pool = pc.pool
"""

if __name__ == "__main__":
    assert pd.Timestamp(END_S, unit="s", tz="UTC") == pd.Timestamp("2026-09-18 15:17:00", tz="UTC")
    fa = f"{OUT}/s2full_pools_missing.csv"
    if not os.path.exists(fa):
        pools = inputs()[0]
        info = upload_retry(pools[["mint"]], "ms_s2full_lpools")
        try:
            st, cost, nb, x = run(SQL_MISS.format(pools=info["full_name"]), "medium", fa + ".tmp", max_wait=3600)
        finally:
            drop(info["full_name"])
        print("missing pools:", st, cost, nb, x.get("error"), flush=True)
        if st != "QUERY_STATE_COMPLETED":
            sys.exit(1)
        os.replace(fa + ".tmp", fa)
    m = pd.read_csv(fa)
    print(f"completed tokens without a local pool: {m.mint.nunique()}, with a PumpSwap SOL pool: {m[m.pool.notna()].mint.nunique()}", flush=True)
    best = m[m.pool.notna()].sort_values(["mint", "n", "pool"], ascending=[True, False, True]).drop_duplicates("mint")
    fb = f"{OUT}/s2full_pools_extra.csv"
    if not os.path.exists(fb):
        pp = pd.DataFrame({"pool": sorted(best.pool.unique())}); pp["pid"] = range(len(pp))
        lk = pp.assign(lid=pp.pid, t_s=END_S)[["lid", "pid", "t_s"]]
        pinfo = upload_retry(pp[["pid", "pool"]], "ms_s2full_xpools")
        linfo = upload_retry(lk, "ms_s2full_xlk")
        try:
            st, cost, nb, x = run(SQL_STATE.format(tbl=linfo["full_name"], ptbl=pinfo["full_name"], dates=EV_DATES), "medium",
                                  fb + ".tmp", max_wait=3600)
        finally:
            drop(pinfo["full_name"]); drop(linfo["full_name"])
        print("final state:", st, cost, nb, x.get("error"), flush=True)
        if st != "QUERY_STATE_COMPLETED":
            sys.exit(1)
        r = pd.read_csv(fb + ".tmp")
        assert len(r) == len(pp)
        r.insert(0, "pool", pp.pool.values)
        best[["mint", "pool"]].merge(r, on="pool").to_csv(fb, index=False)
        os.remove(fb + ".tmp")
    e = pd.read_csv(fb)
    print(f"extra pools: {len(e)}, with state: {int(e.b.notna().sum())}", flush=True)
