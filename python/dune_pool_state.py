"""Pool-phase state at each lookup time, computed on Dune (Study 1 registration, Amendment 1 and its erratum).

For each lookup (pool, t):
  reserves = pre-trade reserves of the first pool event at or after t; if there is none, the post-trade reserves of the
             last event before t (buy: rb - base_out, rq + quote_in_with_lp_fee; sell: rb + base_in,
             rq - quote_out_without_lp_fee);
  rq_eff   = rq + V_pool, where V_pool is the median over the pool's sells of quote_out * (rb + base_in) / base_in - rq;
  f_buy    = realized fee of the nearest buy (at or after t, else before): user_quote_in / quote_in - 1;
  f_sell   = realized fee of the nearest sell: 1 - user_quote_out / quote_out.
The lookups are uploaded to Dune as a table that is deleted after the query. The event date range and the upload helper
defined here are reused by the other Dune scripts. Requires DUNE_API_KEY.
Run: python3 dune_pool_state.py <lookups.parquet> <table_name> <out.csv> [n_sample]
"""
import sys

import pandas as pd
import requests

from dune import API, H, run

EV_DATES = "evt_block_date BETWEEN DATE '2026-08-08' AND DATE '2026-09-19'"

SQL = """
WITH lk AS (SELECT lid, pool, from_unixtime(t_s) AS t FROM {tbl}),
pools AS (SELECT DISTINCT pool FROM lk),
ev0 AS (
  SELECT pool, evt_block_time bt, evt_block_slot slot, evt_tx_index txi, evt_outer_instruction_index oix,
         evt_inner_instruction_index iix, 1 AS is_buy,
         CAST(pool_base_token_reserves AS double) rb, CAST(pool_quote_token_reserves AS double) rq,
         CAST(base_amount_out AS double) dbase, CAST(quote_amount_in_with_lp_fee AS double) dq,
         CAST(user_quote_amount_in AS double) / NULLIF(CAST(quote_amount_in AS double), 0) - 1 AS f,
         CAST(NULL AS double) AS v
  FROM pumpdotfun_solana.pump_amm_evt_buyevent
  WHERE {dates} AND pool IN (SELECT pool FROM pools)
  UNION ALL
  SELECT pool, evt_block_time, evt_block_slot, evt_tx_index, evt_outer_instruction_index,
         evt_inner_instruction_index, 0,
         CAST(pool_base_token_reserves AS double), CAST(pool_quote_token_reserves AS double),
         CAST(base_amount_in AS double), CAST(quote_amount_out_without_lp_fee AS double),
         1 - CAST(user_quote_amount_out AS double) / NULLIF(CAST(quote_amount_out AS double), 0),
         CAST(quote_amount_out AS double) * (CAST(pool_base_token_reserves AS double) + CAST(base_amount_in AS double))
           / NULLIF(CAST(base_amount_in AS double), 0) - CAST(pool_quote_token_reserves AS double)
  FROM pumpdotfun_solana.pump_amm_evt_sellevent
  WHERE {dates} AND pool IN (SELECT pool FROM pools)
),
ev AS (
  SELECT *, date_add('millisecond', CAST(row_number() OVER (PARTITION BY pool, bt ORDER BY slot, txi, oix, iix) - 1 AS bigint), bt) AS tk
  FROM ev0
),
vp AS (SELECT pool, approx_percentile(v, 0.5) AS V FROM ev WHERE is_buy = 0 AND dbase > 0 AND v IS NOT NULL GROUP BY pool),
evb AS (SELECT pool, tk, f FROM ev WHERE is_buy = 1 AND f IS NOT NULL),
evs AS (SELECT pool, tk, f FROM ev WHERE is_buy = 0 AND f IS NOT NULL),
nx AS (SELECT lk.lid, e.rb nrb, e.rq nrq FROM lk ASOF LEFT JOIN ev e ON e.pool = lk.pool AND e.tk >= lk.t),
pv AS (SELECT lk.lid,
         e.rb + CASE WHEN e.is_buy = 1 THEN -e.dbase ELSE e.dbase END prb,
         e.rq + CASE WHEN e.is_buy = 1 THEN e.dq ELSE -e.dq END prq
       FROM lk ASOF LEFT JOIN ev e ON e.pool = lk.pool AND e.tk < lk.t),
fbn AS (SELECT lk.lid, e.f FROM lk ASOF LEFT JOIN evb e ON e.pool = lk.pool AND e.tk >= lk.t),
fbp AS (SELECT lk.lid, e.f FROM lk ASOF LEFT JOIN evb e ON e.pool = lk.pool AND e.tk < lk.t),
fsn AS (SELECT lk.lid, e.f FROM lk ASOF LEFT JOIN evs e ON e.pool = lk.pool AND e.tk >= lk.t),
fsp AS (SELECT lk.lid, e.f FROM lk ASOF LEFT JOIN evs e ON e.pool = lk.pool AND e.tk < lk.t)
SELECT lk.lid,
       CAST(round(coalesce(nx.nrb, pv.prb)) AS bigint) AS rb,
       CAST(round(coalesce(nx.nrq, pv.prq) + coalesce(vp.V, 0)) AS bigint) AS rqe,
       round(coalesce(fbn.f, fbp.f) * 1e4, 2) AS fb,
       round(coalesce(fsn.f, fsp.f) * 1e4, 2) AS fs,
       CASE WHEN nx.nrb IS NULL THEN 1 ELSE 0 END AS pv
FROM lk
LEFT JOIN nx ON nx.lid = lk.lid LEFT JOIN pv ON pv.lid = lk.lid
LEFT JOIN vp ON vp.pool = lk.pool
LEFT JOIN fbn ON fbn.lid = lk.lid LEFT JOIN fbp ON fbp.lid = lk.lid
LEFT JOIN fsn ON fsn.lid = lk.lid LEFT JOIN fsp ON fsp.lid = lk.lid
"""


def upload(df, table_name):
    csv = df.to_csv(index=False)
    r = requests.post(f"{API}/uploads/csv", headers=H, timeout=(60, 900),
                      json={"data": csv, "table_name": table_name, "is_private": False,
                            "description": "memecoin-slots pool lookups (pool, unix second)"})
    print("upload:", r.status_code, r.text[:500])
    r.raise_for_status()
    return r.json()


if __name__ == "__main__":
    src, table_name, out = sys.argv[1], sys.argv[2], sys.argv[3]
    lk = pd.read_parquet(src)[["lid", "pool", "t_s"]]
    if len(sys.argv) > 4:
        lk = lk.sample(int(sys.argv[4]), random_state=11)
    info = upload(lk, table_name)
    full = info.get("full_name") or info.get("table_name") or info.get("name")
    print("table:", full)
    state, cost, nbytes, s = run(SQL.format(tbl=full, dates=EV_DATES), "medium", out, max_wait=3600)
    print(state, "credits", cost, "bytes", nbytes, "rows", len(lk), s.get("error"))
    ns, tn = full.split(".")[1], full.split(".")[2]
    d = requests.delete(f"{API}/uploads/{ns}/{tn}", headers=H, timeout=120)
    print("deleted upload:", d.status_code, d.text[:200])
