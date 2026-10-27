"""Study 2 data check (not registered): the Study 2 retail computation rebuilt on Dune from its decoded events.

The local trade streams lose data during collection outages. Dune's decoded pump.fun and PumpSwap events do not have these
gaps. The query applies the registered rules to every census token:
  census: first create event per token from 2026-08-08 21:16 to 2026-09-10 15:17 UTC, with the curve quoted in SOL;
  positions: per (token, wallet), curve trades (paid = sol_amount + fee + creator_fee on buys, received = sol_amount - fee -
    creator_fee on sells) and trades in the token's PumpSwap SOL pool, up to the end of the data;
  transaction cost: each leg pays its hour's median cost from s2_txcost_hourly.parquet, since Dune's events carry no fees;
  classes: creator and signer of the first create event, launch block (first curve buy within 2 slots of the create),
    creator-distributed, protocol, oversold; the wallet-level bot flags (atomic, more than 200 trades in an hour) come from
    the local data;
  liquidation: residual tokens are sold into the final pool state (graduated by the end of the data) or the final curve
    state on Dune, paying the cost of the last hour.
Only aggregates come back: sums per (launch day, class, multiple bin), wallet counts, win and tail counts.

Reads the local pool list, bot wallets and creator-distributed pairs from the DuckDB file that s2_analyze.py leaves in TMP,
DATA/dune_state/final_pool_state.csv and final_pool_state_extra.csv, and DATA/s2_txcost_hourly.parquet. Writes
DATA/dune_state/s2full_census.csv and s2full_agg.csv. With POOLS_EXTRA=1 it adds the pools of tokens that graduated during
local outages (DATA/dune_state/s2full_pools_extra.csv from dune_s2_full_pools.py) and writes s2full_agg_allpools.csv. With
OTHER_VENUES=1 (exploratory, implies POOLS_EXTRA) it also counts each census token's trades against wrapped SOL on every
other DEX in dex_solana.trades, which the registered rules leave out, and writes s2full_agg_allvenues.csv.
s2_dune_full_compare.py and s2_dune_allvenues_compare.py turn these files into results. Finished outputs are skipped on rerun.

Run: DUNE_API_KEY=... python dune_s2_full.py
"""
import os
import sys

import duckdb
import pandas as pd

from dune import run
from dune_pool_state_full import drop, upload_retry
from config import DATA, TMP

D = DATA
OUT = f"{D}/dune_state"
DBF = f"{TMP}/s2_analyze.duckdb"
END = "TIMESTAMP '2026-09-18 15:17:00'"
DATES = "evt_block_date BETWEEN DATE '2026-08-08' AND DATE '2026-09-18'"
PROTOCOL = "BwWK17cbHxwWBKZkUYvzxLcNQ1YVyaFezduWbtm2de6s"
SOL = "11111111111111111111111111111111"
GRID = (2, 3, 5, 10, 20, 50, 100, 200, 500, 1000)
POOLS_EXTRA = os.environ.get("POOLS_EXTRA") == "1"
OTHER_VENUES = os.environ.get("OTHER_VENUES") == "1"
POOLS_EXTRA = POOLS_EXTRA or OTHER_VENUES
WSOL = "So11111111111111111111111111111111111111112"

SQL = f"""
WITH cr AS (
  SELECT mint, min_by(creator, (evt_block_slot, evt_tx_index, evt_inner_instruction_index)) AS creator,
         min_by(evt_tx_signer, (evt_block_slot, evt_tx_index, evt_inner_instruction_index)) AS signer,
         min_by(quote_mint, (evt_block_slot, evt_tx_index, evt_inner_instruction_index)) AS qm,
         min(evt_block_slot) AS cslot, min(evt_block_time) AS t_create
  FROM pumpdotfun_solana.pump_evt_createevent
  WHERE evt_block_date BETWEEN DATE '2026-08-08' AND DATE '2026-09-10' GROUP BY 1),
tok AS (SELECT * FROM cr WHERE t_create >= TIMESTAMP '2026-08-08 21:16:00' AND t_create < TIMESTAMP '2026-09-10 15:17:00'
          AND coalesce(qm, '{SOL}') = '{SOL}'),
h AS (SELECT CAST(hr AS bigint) AS hr, lam FROM {{txc}}),
c AS (
  SELECT t.mint, t."user" AS w,
    sum(CASE WHEN t.is_buy THEN CAST(t.sol_amount + t.fee + t.creator_fee AS double) ELSE 0 END) AS paid,
    sum(CASE WHEN t.is_buy THEN CAST(t.token_amount AS double) ELSE 0 END) AS tin,
    sum(CASE WHEN NOT t.is_buy THEN CAST(t.sol_amount AS double) - CAST(t.fee AS double) - CAST(t.creator_fee AS double) ELSE 0 END) AS recv,
    sum(CASE WHEN NOT t.is_buy THEN CAST(t.token_amount AS double) ELSE 0 END) AS tout,
    sum(CASE WHEN t.is_buy THEN coalesce(h.lam, {{fall}}) ELSE 0 END) AS cbuy,
    sum(CASE WHEN NOT t.is_buy THEN coalesce(h.lam, {{fall}}) ELSE 0 END) AS csell,
    min(CASE WHEN t.is_buy THEN t.evt_block_slot END) AS fbs
  FROM pumpdotfun_solana.pump_evt_tradeevent t JOIN tok ON tok.mint = t.mint
  LEFT JOIN h ON h.hr = CAST(to_unixtime(date_trunc('hour', t.evt_block_time)) AS bigint)
  WHERE t.{DATES} AND t.evt_block_time <= {END} GROUP BY 1, 2),
fc AS (
  SELECT t.mint, CAST(max_by(t.virtual_sol_reserves, (t.evt_block_slot, t.evt_tx_index, t.evt_inner_instruction_index)) AS double) AS vs,
         CAST(max_by(t.virtual_token_reserves, (t.evt_block_slot, t.evt_tx_index, t.evt_inner_instruction_index)) AS double) AS vt,
         CAST(max_by(t.creator_fee_basis_points, (t.evt_block_slot, t.evt_tx_index, t.evt_inner_instruction_index)) AS double) AS cb
  FROM pumpdotfun_solana.pump_evt_tradeevent t JOIN tok ON tok.mint = t.mint
  WHERE t.{DATES} AND t.evt_block_time <= {END} GROUP BY 1),
pl AS (SELECT * FROM {{pools}}),
pb AS (
  SELECT pl.mint, b."user" AS w, sum(CAST(b.user_quote_amount_in AS double)) AS paid, sum(CAST(b.base_amount_out AS double)) AS tin,
         sum(coalesce(h.lam, {{fall}})) AS cbuy
  FROM pumpdotfun_solana.pump_amm_evt_buyevent b JOIN pl ON pl.pool = b.pool LEFT JOIN h ON h.hr = CAST(to_unixtime(date_trunc('hour', b.evt_block_time)) AS bigint)
  WHERE b.{DATES} AND b.evt_block_time <= {END} GROUP BY 1, 2),
ps AS (
  SELECT pl.mint, s."user" AS w, sum(CAST(s.user_quote_amount_out AS double)) AS recv, sum(CAST(s.base_amount_in AS double)) AS tout,
         sum(coalesce(h.lam, {{fall}})) AS csell
  FROM pumpdotfun_solana.pump_amm_evt_sellevent s JOIN pl ON pl.pool = s.pool LEFT JOIN h ON h.hr = CAST(to_unixtime(date_trunc('hour', s.evt_block_time)) AS bigint)
  WHERE s.{DATES} AND s.evt_block_time <= {END} GROUP BY 1, 2),
{{ov}}a AS (
  SELECT mint, w, paid, tin, recv, tout, cbuy, csell, fbs FROM c
  UNION ALL SELECT mint, w, paid, tin, 0, 0, cbuy, 0, CAST(NULL AS bigint) FROM pb
  UNION ALL SELECT mint, w, 0, 0, recv, tout, 0, csell, CAST(NULL AS bigint) FROM ps{{ov_union}}),
pos AS (SELECT mint, w, sum(paid) AS paid, sum(tin) AS tin, sum(recv) AS recv, sum(tout) AS tout, sum(cbuy) AS cbuy,
               sum(csell) AS csell, min(fbs) AS fbs FROM a GROUP BY 1, 2),
k AS (
  SELECT pos.*, date_diff('day', DATE '2026-08-01', CAST(tok.t_create AS date)) AS dayi,
    (pos.w = tok.creator OR pos.w = tok.signer) AS is_creator, coalesce(pos.fbs <= tok.cslot + 2, false) AS is_lb,
    (d.w IS NOT NULL) AS is_dist, (pos.w = '{PROTOCOL}') AS is_prot, (bt.w IS NOT NULL) AS is_bot,
    greatest(pos.tin - pos.tout, 0) AS resid, (pos.tout > pos.tin) AS oversold,
    (pl.mint IS NOT NULL) AS grad, pl.rb, pl.rqe, pl.fs, fc.vs, fc.vt, fc.cb
  FROM pos JOIN tok ON tok.mint = pos.mint
  LEFT JOIN {{dist}} d ON d.mint = pos.mint AND d.w = pos.w
  LEFT JOIN {{bots}} bt ON bt.w = pos.w
  LEFT JOIN pl ON pl.mint = pos.mint
  LEFT JOIN fc ON fc.mint = pos.mint),
x AS (
  SELECT *, CASE WHEN resid = 0 THEN 0 WHEN grad THEN rqe * resid / (rb + resid) * (1 - fs)
                 ELSE (vs - vs * vt / (vt + resid)) * (1 - (95 + coalesce(cb, 0)) / 1e4) END AS liq FROM k),
y AS (
  SELECT dayi, w,
    CASE WHEN is_prot THEN 'protocol' WHEN is_creator THEN 'creator' WHEN is_dist THEN 'creator-distributed'
         WHEN is_lb THEN 'launch-block' WHEN is_bot THEN 'bot' WHEN oversold THEN 'oversold' ELSE 'retail' END AS cls,
    paid + cbuy AS ci,
    recv - csell + CASE WHEN resid > 0 AND coalesce(liq, 0) > {{txc_end}} THEN liq - {{txc_end}} ELSE 0 END AS co
  FROM x WHERE paid + cbuy > 0),
z AS (
  SELECT *, co / ci AS xm, row_number() OVER (PARTITION BY w, cls) AS rn,
         sum(co) OVER (PARTITION BY w, cls) > sum(ci) OVER (PARTITION BY w, cls) AS ahead FROM y)
SELECT dayi, cls, CAST(floor(log10(greatest(xm, 1e-8)) * 20) AS integer) AS xb,
  count(*) AS n, sum(ci) AS ci, sum(co) AS co, sum(xm) AS sx, count_if(rn = 1) AS wallets, count_if(rn = 1 AND ahead) AS wallets_ahead,
  count_if(xm > 1) AS win, sum(CASE WHEN xm >= 100 THEN xm ELSE 0 END) AS jx,
  count_if(xm >= 10) AS hill_n, sum(CASE WHEN xm >= 10 THEN ln(xm / 10) ELSE 0 END) AS hill_s,
  {", ".join(f"count_if(xm >= {m}) AS ge{m}" for m in GRID)}
FROM z GROUP BY 1, 2, 3
"""

SQL_CENSUS = f"""
WITH cr AS (
  SELECT mint, min_by(quote_mint, (evt_block_slot, evt_tx_index, evt_inner_instruction_index)) AS qm, min(evt_block_time) AS t_create
  FROM pumpdotfun_solana.pump_evt_createevent
  WHERE evt_block_date BETWEEN DATE '2026-08-08' AND DATE '2026-09-10' GROUP BY 1),
tok AS (SELECT * FROM cr WHERE t_create >= TIMESTAMP '2026-08-08 21:16:00' AND t_create < TIMESTAMP '2026-09-10 15:17:00'),
cp AS (SELECT DISTINCT mint FROM pumpdotfun_solana.pump_evt_completeevent WHERE {DATES} AND evt_block_time <= {END})
SELECT date_diff('day', DATE '2026-08-01', CAST(t_create AS date)) AS dayi, coalesce(qm, '{SOL}') = '{SOL}' AS sol,
       count(*) AS n, count_if(cp.mint IS NOT NULL) AS completed, count_if(cp.mint IS NOT NULL AND pl.mint IS NULL) AS completed_no_pool
FROM tok LEFT JOIN cp ON cp.mint = tok.mint LEFT JOIN {{pools}} pl ON pl.mint = tok.mint GROUP BY 1, 2
"""


OV = f"""ov AS (
  SELECT tok.mint, d.trader_id AS w, CAST(d.token_sold_amount_raw AS double) AS paid, CAST(d.token_bought_amount_raw AS double) AS tin,
         0e0 AS recv, 0e0 AS tout, coalesce(h.lam, {{fall}}) AS cbuy, 0e0 AS csell
  FROM dex_solana.trades d JOIN tok ON tok.mint = d.token_bought_mint_address
  LEFT JOIN h ON h.hr = CAST(to_unixtime(date_trunc('hour', d.block_time)) AS bigint)
  WHERE d.block_month BETWEEN DATE '2026-08-01' AND DATE '2026-09-01' AND d.block_time <= {END}
    AND d.token_sold_mint_address = '{WSOL}' AND d.project NOT IN ('pumpdotfun', 'pumpswap')
  UNION ALL
  SELECT tok.mint, d.trader_id, 0e0, 0e0, CAST(d.token_bought_amount_raw AS double), CAST(d.token_sold_amount_raw AS double),
         0e0, coalesce(h.lam, {{fall}})
  FROM dex_solana.trades d JOIN tok ON tok.mint = d.token_sold_mint_address
  LEFT JOIN h ON h.hr = CAST(to_unixtime(date_trunc('hour', d.block_time)) AS bigint)
  WHERE d.block_month BETWEEN DATE '2026-08-01' AND DATE '2026-09-01' AND d.block_time <= {END}
    AND d.token_bought_mint_address = '{WSOL}' AND d.project NOT IN ('pumpdotfun', 'pumpswap')),
"""
OV_UNION = "\n  UNION ALL SELECT mint, w, paid, tin, recv, tout, cbuy, csell, CAST(NULL AS bigint) FROM ov"


def inputs():
    c = duckdb.connect()
    c.sql(f"set threads=1; set memory_limit='1500MB'; set TimeZone='UTC'; set temp_directory='{TMP}/s2full'")
    c.sql(f"attach '{DBF}' as a (read_only)")
    fps = pd.concat([pd.read_csv(f"{OUT}/final_pool_state.csv"), pd.read_csv(f"{OUT}/final_pool_state_extra.csv")], ignore_index=True)
    fps["rb"] = pd.to_numeric(fps.b, errors="coerce"); fps["rqe"] = pd.to_numeric(fps.p, errors="coerce") * fps.rb; fps["fs"] = fps.f / 1e5
    c.register("fps", fps[["pool", "rb", "rqe", "fs"]])
    pools = c.sql("""select bp.mint, bp.pool, fps.rb, fps.rqe, fps.fs from a.bpool bp join fps using (pool)
                     where bp.t_grad <= TIMESTAMPTZ '2026-09-18 15:17:00+00'""").df()
    if POOLS_EXTRA and os.path.exists(f"{OUT}/s2full_pools_extra.csv"):
        # Tokens that completed on Dune by the end of the data without a local pool.
        e = pd.read_csv(f"{OUT}/s2full_pools_extra.csv")
        e["rb"] = pd.to_numeric(e.b, errors="coerce"); e["rqe"] = pd.to_numeric(e.p, errors="coerce") * e.rb
        e["fs"] = pd.to_numeric(e.f, errors="coerce") / 1e5
        print(f"extra pools without a state: {int(e.rb.isna().sum())}, without a sell fee (median used): {int(e.fs.isna().sum())}", flush=True)
        e["fs"] = e.fs.fillna(e.fs.median())
        pools = pd.concat([pools, e[["mint", "pool", "rb", "rqe", "fs"]]], ignore_index=True)
    bots = c.sql("select distinct w from a.fin where is_atomic_bot or max_hour > 200").df()
    dist = c.sql("select mint, w from a.dist").df()
    txc = c.sql(f"select epoch(hr)::bigint hr, lam from read_parquet('{D}/s2_txcost_hourly.parquet')").df()
    fall = c.sql(f"select median(lam) from read_parquet('{D}/s2_txcost_hourly.parquet')").fetchone()[0]
    txc_end = c.sql(f"""select lam from read_parquet('{D}/s2_txcost_hourly.parquet')
                       where hr = date_trunc('hour', TIMESTAMPTZ '2026-09-18 15:17:00+00')""").fetchone()
    c.close()
    return pools, bots, dist, txc, float(fall), float(txc_end[0] if txc_end else fall)


if __name__ == "__main__":
    pools, bots, dist, txc, fall, txc_end = inputs()
    print(f"pools {len(pools)} (no state {int(pools.rb.isna().sum())}), bot wallets {len(bots)}, dist {len(dist)}, hours {len(txc)}, "
          f"fallback {fall}, end-hour cost {txc_end}", flush=True)
    up = {}
    try:
        for k, df in (("pools", pools), ("bots", bots), ("dist", dist), ("txc", txc)):
            up[k] = upload_retry(df, f"ms_s2full_{k}")["full_name"]
        for name, sql in (("census", SQL_CENSUS), ("agg_allvenues" if OTHER_VENUES else "agg_allpools" if POOLS_EXTRA else "agg", SQL)):
            out = f"{OUT}/s2full_{name}.csv"
            if os.path.exists(out):
                continue
            q = sql.format(fall=fall, txc_end=txc_end, ov=OV.format(fall=fall) if OTHER_VENUES else "",
                           ov_union=OV_UNION if OTHER_VENUES else "", **up)
            st, cost, nb, x = run(q, "medium", out + ".tmp", max_wait=3 * 3600)
            print(name, st, cost, nb, x.get("error"), flush=True)
            if st == "QUERY_STATE_COMPLETED":
                os.replace(out + ".tmp", out)
            else:
                break
    finally:
        for f in up.values():
            drop(f)
