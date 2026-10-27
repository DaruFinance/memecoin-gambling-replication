"""Pool-phase state at every Study 1 lookup, pulled from Dune in four chunks with a compact export.

State and fee rules are those of dune_pool_state.py (Study 1 registration, Amendment 1 and its erratum). Output rows are
ordered by lid and carry no lid column, so row i of a chunk is lid chunk_start + i. Columns:
  p = (rq + V_pool) / rb at t, as %.6e text;
  b = rb at t, as %.3e text (it only enters the price-impact term);
  f = realized fee of the nearest sell, in tenths of a basis point (integer).
Buy fees at entry lookups come from dune_entry_fee_and_peak.py.
Reads data/pool_lookups.parquet. Writes data/dune_state/state_chunk<k>.csv and appends one line per finished chunk to
data/dune_state/chunks.tsv (chunk, first lid, end lid, credits, bytes). Finished chunks are skipped on restart.
Requires DUNE_API_KEY.
Run: python3 dune_pool_state_full.py <chunk index 0-3 | all>
"""
import os
import sys
import time

import numpy as np
import pandas as pd
import requests

from config import DATA
from dune import API, H, run
from dune_pool_state import EV_DATES, upload

OUT = f"{DATA}/dune_state"
N_CHUNKS = 4

SQL = """
WITH lk AS (SELECT l.lid, p.pool, from_unixtime(l.t_s) AS t FROM {tbl} l JOIN {ptbl} p ON p.pid = l.pid),
pools AS (SELECT DISTINCT pool FROM lk),
ev0 AS (
  SELECT pool, evt_block_time bt, evt_block_slot slot, evt_tx_index txi, evt_outer_instruction_index oix,
         evt_inner_instruction_index iix, 1 AS is_buy,
         CAST(pool_base_token_reserves AS double) rb, CAST(pool_quote_token_reserves AS double) rq,
         CAST(base_amount_out AS double) dbase, CAST(quote_amount_in_with_lp_fee AS double) dq,
         CAST(NULL AS double) AS f, CAST(NULL AS double) AS v
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
evs AS (SELECT pool, tk, f FROM ev WHERE is_buy = 0 AND f IS NOT NULL),
nx AS (SELECT lk.lid, e.rb nrb, e.rq nrq FROM lk ASOF LEFT JOIN ev e ON e.pool = lk.pool AND e.tk >= lk.t),
pv AS (SELECT lk.lid,
         e.rb + CASE WHEN e.is_buy = 1 THEN -e.dbase ELSE e.dbase END prb,
         e.rq + CASE WHEN e.is_buy = 1 THEN e.dq ELSE -e.dq END prq
       FROM lk ASOF LEFT JOIN ev e ON e.pool = lk.pool AND e.tk < lk.t),
fsn AS (SELECT lk.lid, e.f FROM lk ASOF LEFT JOIN evs e ON e.pool = lk.pool AND e.tk >= lk.t),
fsp AS (SELECT lk.lid, e.f FROM lk ASOF LEFT JOIN evs e ON e.pool = lk.pool AND e.tk < lk.t),
st AS (
  SELECT lk.lid,
         coalesce(nx.nrb, pv.prb) AS rb,
         coalesce(nx.nrq, pv.prq) + coalesce(vp.V, 0) AS rqe,
         coalesce(fsn.f, fsp.f) AS fs
  FROM lk
  LEFT JOIN nx ON nx.lid = lk.lid LEFT JOIN pv ON pv.lid = lk.lid LEFT JOIN vp ON vp.pool = lk.pool
  LEFT JOIN fsn ON fsn.lid = lk.lid LEFT JOIN fsp ON fsp.lid = lk.lid
)
SELECT CASE WHEN rb > 0 THEN format('%.6e', rqe / rb) END AS p,
       CASE WHEN rb > 0 THEN format('%.3e', rb) END AS b,
       CAST(round(fs * 1e5) AS integer) AS f
FROM st ORDER BY lid
"""


def chunks():
    lk = pd.read_parquet(f"{DATA}/pool_lookups.parquet")[["lid", "pool", "t_s"]].sort_values("lid")
    assert (lk.lid.values == np.arange(len(lk))).all(), "lids must be contiguous"
    # split on pool boundaries so each chunk's pools are disjoint
    cuts = [0]
    for k in range(1, N_CHUNKS):
        i = k * len(lk) // N_CHUNKS
        while i < len(lk) and lk.pool.iat[i] == lk.pool.iat[i - 1]:
            i += 1
        cuts.append(i)
    cuts.append(len(lk))
    return lk, cuts


def upload_retry(df, name, tries=6):
    for i in range(tries):
        try:
            return upload(df, name)
        except requests.RequestException as e:
            print(f"upload {name} attempt {i + 1} failed: {e.__class__.__name__}", flush=True)
            time.sleep(30 * (i + 1))
    raise RuntimeError(f"upload {name} failed")


def drop(full):
    ns, tn = full.split(".")[1], full.split(".")[2]
    for i in range(10):
        try:
            d = requests.delete(f"{API}/uploads/{ns}/{tn}", headers=H, timeout=(60, 120))
            print("deleted upload:", full, d.status_code, d.text[:120], flush=True)
            return
        except requests.RequestException as e:
            print(f"delete retry {i + 1}: {e.__class__.__name__}", flush=True)
            time.sleep(30)
    print("WARNING: could not delete", full, "- delete manually", flush=True)


def do_chunk(lk, cuts, k, ptbl):
    out = f"{OUT}/state_chunk{k}.csv"
    if os.path.exists(out):
        print(f"chunk {k}: exists, skip"); return
    part = lk.iloc[cuts[k]:cuts[k + 1]][["lid", "pid", "t_s"]]
    info = upload_retry(part, f"ms_lk_c{k}")
    full = info["full_name"]
    t0 = time.time()
    try:
        state, cost, nbytes, s = run(SQL.format(tbl=full, ptbl=ptbl, dates=EV_DATES), "medium", out + ".tmp", max_wait=7200)
    finally:
        drop(full)
    print(f"chunk {k}: lids {cuts[k]}..{cuts[k + 1] - 1} ({len(part):,}), {state}, credits {cost}, "
          f"bytes {nbytes:,}, {time.time() - t0:.0f}s, err {s.get('error')}")
    if state == "QUERY_STATE_COMPLETED":
        n = sum(1 for _ in open(out + ".tmp")) - 1
        assert n == len(part), f"row count {n} != {len(part)}"
        os.replace(out + ".tmp", out)
        with open(f"{OUT}/chunks.tsv", "a") as f:
            f.write(f"{k}\t{cuts[k]}\t{cuts[k + 1]}\t{cost}\t{nbytes}\n")


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    lk, cuts = chunks()
    pools = pd.DataFrame({"pool": sorted(lk.pool.unique())})
    pools["pid"] = range(len(pools))
    lk = lk.merge(pools, on="pool").sort_values("lid").reset_index(drop=True)
    pinfo = upload_retry(pools[["pid", "pool"]], "ms_pools")
    try:
        ks = range(N_CHUNKS) if sys.argv[1] == "all" else [int(sys.argv[1])]
        for k in ks:
            do_chunk(lk, cuts, k, pinfo["full_name"])
    finally:
        drop(pinfo["full_name"])
