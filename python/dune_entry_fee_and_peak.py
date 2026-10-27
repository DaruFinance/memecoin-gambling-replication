"""Two pool-phase pulls from Dune for Study 1: the buy fee at each entry and the pool peak of each bet.

Rules: Study 1 registration, Amendment 1 and its erratum and the peak readout.
A. Buy fee at each pool-phase entry lookup: realized fee of the nearest buy (at or after t, else before),
   f = user_quote_amount_in / quote_amount_in - 1. The sign is normalised later, in assemble_x.py.
B. Pool peak per bet: over pool events with time in (max(t_entry, t_grad), t_exit], the pre-trade state with the highest
   price (rq + V_pool) / rb. On a constant-product pool the sell value of a fixed quantity rises with price, up to the
   second-order change in depth, so this is the peak state.
Reads data/pool_lookups_by_bet.parquet and data/bets_v2.parquet. Writes data/dune_state/entry_buyfee.csv (lid, fb) and
data/dune_state/pool_peak.csv (bet_id, p, b). An output that already exists is not pulled again. Requires DUNE_API_KEY.
Run: python3 dune_entry_fee_and_peak.py
"""
import os

import pandas as pd

from config import DATA
from dune import run
from dune_pool_state import EV_DATES
from dune_pool_state_full import drop, upload_retry

D = DATA
OUT = f"{D}/dune_state"

EV = """
ev0 AS (
  SELECT pool, evt_block_time bt, evt_block_slot slot, evt_tx_index txi, evt_outer_instruction_index oix,
         evt_inner_instruction_index iix, 1 AS is_buy,
         CAST(pool_base_token_reserves AS double) rb, CAST(pool_quote_token_reserves AS double) rq,
         CAST(user_quote_amount_in AS double) / NULLIF(CAST(quote_amount_in AS double), 0) - 1 AS f,
         CAST(NULL AS double) AS v
  FROM pumpdotfun_solana.pump_amm_evt_buyevent WHERE {dates} AND pool IN (SELECT pool FROM pools)
  UNION ALL
  SELECT pool, evt_block_time, evt_block_slot, evt_tx_index, evt_outer_instruction_index, evt_inner_instruction_index, 0,
         CAST(pool_base_token_reserves AS double), CAST(pool_quote_token_reserves AS double), NULL,
         CAST(quote_amount_out AS double) * (CAST(pool_base_token_reserves AS double) + CAST(base_amount_in AS double))
           / NULLIF(CAST(base_amount_in AS double), 0) - CAST(pool_quote_token_reserves AS double)
  FROM pumpdotfun_solana.pump_amm_evt_sellevent WHERE {dates} AND pool IN (SELECT pool FROM pools)
),
ev AS (SELECT *, date_add('millisecond', CAST(row_number() OVER (PARTITION BY pool, bt ORDER BY slot, txi, oix, iix) - 1 AS bigint), bt) AS tk FROM ev0)
"""

SQL_FEE = """
WITH lk AS (SELECT l.lid, p.pool, from_unixtime(l.t_s) AS t FROM {tbl} l JOIN {ptbl} p ON p.pid = l.pid),
pools AS (SELECT DISTINCT pool FROM lk),
""" + EV + """,
evb AS (SELECT pool, tk, f FROM ev WHERE is_buy = 1 AND f IS NOT NULL),
n AS (SELECT lk.lid, e.f FROM lk ASOF LEFT JOIN evb e ON e.pool = lk.pool AND e.tk >= lk.t),
p AS (SELECT lk.lid, e.f FROM lk ASOF LEFT JOIN evb e ON e.pool = lk.pool AND e.tk < lk.t)
SELECT lk.lid, CAST(round(coalesce(n.f, p.f) * 1e5) AS integer) AS fb
FROM lk LEFT JOIN n ON n.lid = lk.lid LEFT JOIN p ON p.lid = lk.lid ORDER BY lk.lid
"""

SQL_PEAK = """
WITH w AS (SELECT l.bet_id, p.pool, from_unixtime(l.t0) AS t0, from_unixtime(l.t1) AS t1 FROM {tbl} l JOIN {ptbl} p ON p.pid = l.pid),
pools AS (SELECT DISTINCT pool FROM w),
""" + EV + """,
vp AS (SELECT pool, approx_percentile(v, 0.5) AS V FROM ev WHERE is_buy = 0 AND v IS NOT NULL GROUP BY pool),
s AS (SELECT e.pool, e.tk, e.rb, e.rq + coalesce(vp.V, 0) AS rqe FROM ev e LEFT JOIN vp ON vp.pool = e.pool WHERE e.rb > 0)
SELECT w.bet_id, format('%.6e', max_by(s.rqe / s.rb, s.rqe / s.rb)) AS p, format('%.3e', max_by(s.rb, s.rqe / s.rb)) AS b
FROM w JOIN s ON s.pool = w.pool AND s.tk > w.t0 AND s.tk <= w.t1
GROUP BY w.bet_id
"""


def main():
    os.makedirs(OUT, exist_ok=True)
    lk = pd.read_parquet(f"{D}/pool_lookups_by_bet.parquet")
    bets = pd.read_parquet(f"{D}/bets_v2.parquet", columns=["bet_id", "pool", "t_entry", "t_exit", "t_grad"])
    pools = pd.DataFrame({"pool": sorted(set(lk.pool) | set(bets.pool.dropna()))})
    pools["pid"] = range(len(pools))
    pinfo = upload_retry(pools[["pid", "pool"]], "ms_pools2")
    try:
        out = f"{OUT}/entry_buyfee.csv"
        if not os.path.exists(out):
            e = lk[lk.role == "t_entry"][["lid", "pool", "t_s"]].drop_duplicates("lid").merge(pools, on="pool")
            info = upload_retry(e[["lid", "pid", "t_s"]], "ms_entry")
            try:
                st, cost, nb, s = run(SQL_FEE.format(tbl=info["full_name"], ptbl=pinfo["full_name"], dates=EV_DATES),
                                      "medium", out + ".tmp", max_wait=7200)
            finally:
                drop(info["full_name"])
            print("entry fee:", len(e), st, cost, nb, s.get("error"), flush=True)
            if st == "QUERY_STATE_COMPLETED":
                os.replace(out + ".tmp", out)
        out = f"{OUT}/pool_peak.csv"
        if not os.path.exists(out):
            b = bets[bets.t_grad.notna() & (bets.t_exit > bets.t_grad)].copy()
            b["t0"] = b[["t_entry", "t_grad"]].max(axis=1).astype("int64") // 10**9
            b["t1"] = b.t_exit.astype("int64") // 10**9
            b = b.merge(pools, on="pool")
            info = upload_retry(b[["bet_id", "pid", "t0", "t1"]], "ms_peakwin")
            try:
                st, cost, nb, s = run(SQL_PEAK.format(tbl=info["full_name"], ptbl=pinfo["full_name"], dates=EV_DATES),
                                      "medium", out + ".tmp", max_wait=7200)
            finally:
                drop(info["full_name"])
            print("pool peak:", len(b), st, cost, nb, s.get("error"), flush=True)
            if st == "QUERY_STATE_COMPLETED":
                os.replace(out + ".tmp", out)
    finally:
        drop(pinfo["full_name"])


if __name__ == "__main__":
    main()
