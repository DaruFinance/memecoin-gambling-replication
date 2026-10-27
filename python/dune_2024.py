"""Exploratory check (not registered): the memecoin retail computation of Studies 2 and 3 rebuilt on Dune for two 2024 windows.

The windows keep the calendar clock of the registered 2026 window, shifted back two years ("aug24"), or moved to the late
2024 peak ("nov24"). Census: first create event per token from START to CUTOFF. Positions per (token, wallet) run to END.
Differences from the 2026 computation, forced by the 2024 protocol and by what Dune holds:
  curve: 2024 trade events carry no fee columns; the fee was 1% of sol_amount on both sides (paid = 1.01 x sol_amount,
    received = 0.99 x sol_amount).
  after graduation: tokens migrated to Raydium, not PumpSwap. Pool legs are dex_solana.trades rows between the token and
    wrapped SOL on any venue except the pump.fun curve. Amounts are what the wallet sent and received, so venue fees are
    included.
  transaction cost: each leg pays its hour's median solana.transactions fee (base plus priority) over pump.fun trade
    transactions in the first two minutes of that hour. Jito tips are not counted, which favours traders.
  liquidation at END: curve tokens are sold into the final curve state with the 1% fee. Graduated tokens are valued at the
    last DEX price, sold into a constant-product pool holding sqrt(k p) SOL and sqrt(k / p) tokens, with k = 79 SOL x
    206.9M tokens (the 2024 migration deposit, LP burned), less the 0.25% Raydium fee. The value without price impact
    (last price x tokens) is reported too.
  dust: positions paying under 0.001 SOL form their own class. In 2024 bots bought about 0.00001 SOL of graduated tokens
    from very many single-use wallets; they put in almost no SOL but would dominate the wallet counts.
  classes: creator (create event user or signer), launch block (first buy within 2 slots of the create), bot (a buy and a
    sell of one token in one transaction, on the curve or any DEX, like the atomic flag of the 2026 swap data; or more than
    200 trades in an hour), oversold. There is no creator-distributed or protocol class.
Bot wallets are computed in a first query and uploaded for the main query. Only aggregates come back, in five blocks:
'pos' (per launch day, class and 0.05-decade multiple bin, as in dune_s2_full.py), 'hold' (retail, by holding time and
full exit), 'wal' (retail wallets, by wallet return bucket and number of positions), 'cls' (cash-flow decomposition by
class, graduation and full exit) and 'size' (retail by stake).

Writes DATA/dune_2024/<w>_txcost.csv, <w>_bots.csv and <w>_agg_v4.csv. Finished steps are skipped on rerun.
dune_2024_summary.py turns the aggregates into results.

Run: DUNE_API_KEY=... python dune_2024.py aug24   (or nov24)
"""
import os
import sys

import pandas as pd

from dune import run
from dune_pool_state_full import drop, upload_retry
from config import DATA

OUT = f"{DATA}/dune_2024"
WINDOWS = {
    "aug24": dict(d0="2024-08-08", d1="2024-09-18", start="2024-08-08 21:16:00", cutoff="2024-09-10 15:17:00",
                  end="2024-09-18 15:17:00", base="2024-08-01"),
    "nov24": dict(d0="2024-11-08", d1="2024-12-19", start="2024-11-08 21:16:00", cutoff="2024-12-11 15:17:00",
                  end="2024-12-19 15:17:00", base="2024-11-01"),
}
WSOL = "So11111111111111111111111111111111111111112"
GRID = (2, 3, 5, 10, 20, 50, 100, 200, 500, 1000)
K_POOL = 79e9 * 206.9e12  # lamports x raw token units (6 decimals) deposited in the Raydium pool at migration
ORD = "(evt_block_slot, evt_tx_index, evt_inner_instruction_index)"

SQL_TXC = """
WITH t AS (SELECT DISTINCT evt_tx_id id FROM pumpdotfun_solana.pump_evt_tradeevent
           WHERE evt_block_date BETWEEN DATE '{d0}' AND DATE '{d1}' AND minute(evt_block_time) < 2)
SELECT CAST(to_unixtime(date_trunc('hour', x.block_time)) AS bigint) AS hr, count(*) AS n, approx_percentile(x.fee, 0.5) AS lam
FROM t JOIN solana.transactions x ON x.id = t.id
WHERE x.block_date BETWEEN DATE '{d0}' AND DATE '{d1}' AND minute(x.block_time) < 2 GROUP BY 1 ORDER BY 1
"""

SQL = """
WITH h AS (SELECT * FROM (VALUES {hours}) AS v(hr, lam)),
cr AS (
  SELECT mint, min_by("user", {ord}) AS creator, min_by(evt_tx_signer, {ord}) AS signer,
         min(evt_block_slot) AS cslot, min(evt_block_time) AS t_create
  FROM pumpdotfun_solana.pump_evt_createevent WHERE evt_block_date BETWEEN DATE '{d0}' AND DATE '{d1}' GROUP BY 1),
tok AS (SELECT * FROM cr WHERE t_create >= TIMESTAMP '{start}' AND t_create < TIMESTAMP '{cutoff}'),
tr AS (
  SELECT mint, "user" AS w, evt_tx_id, evt_block_time AS bt, evt_block_slot, evt_tx_index, evt_inner_instruction_index,
         coalesce(is_buy, isBuy) AS b, CAST(coalesce(sol_amount, solAmount) AS double) AS sa,
         CAST(coalesce(token_amount, tokenAmount) AS double) AS ta,
         CAST(coalesce(fee, 0.01 * coalesce(sol_amount, solAmount)) AS double) + CAST(coalesce(creator_fee, 0) AS double) AS fe,
         CAST(coalesce(virtual_sol_reserves, virtualSolReserves) AS double) AS vs,
         CAST(coalesce(virtual_token_reserves, virtualTokenReserves) AS double) AS vt
  FROM pumpdotfun_solana.pump_evt_tradeevent
  WHERE evt_block_date BETWEEN DATE '{d0}' AND DATE '{d1}' AND evt_block_time <= TIMESTAMP '{end}'),
grad AS (SELECT DISTINCT c.mint FROM pumpdotfun_solana.pump_evt_completeevent c JOIN tok ON tok.mint = c.mint
         WHERE c.evt_block_date BETWEEN DATE '{d0}' AND DATE '{d1}' AND c.evt_block_time <= TIMESTAMP '{end}'),
dx AS (
  SELECT g.mint, d.trader_id AS w, d.tx_id, d.block_time AS bt, d.block_slot, d.tx_index, d.outer_instruction_index oi,
         coalesce(d.inner_instruction_index, 0) ii, true AS b,
         CAST(d.token_sold_amount_raw AS double) AS sol, CAST(d.token_bought_amount_raw AS double) AS ta
  FROM dex_solana.trades d JOIN grad g ON d.token_bought_mint_address = g.mint
  WHERE d.block_month BETWEEN date_trunc('month', DATE '{d0}') AND DATE '{d1}' AND d.block_time >= TIMESTAMP '{start}'
    AND d.block_time <= TIMESTAMP '{end}' AND d.token_sold_mint_address = '{wsol}' AND d.project NOT IN ('pumpdotfun', 'pumpswap')
  UNION ALL
  SELECT g.mint, d.trader_id, d.tx_id, d.block_time, d.block_slot, d.tx_index, d.outer_instruction_index,
         coalesce(d.inner_instruction_index, 0), false,
         CAST(d.token_bought_amount_raw AS double), CAST(d.token_sold_amount_raw AS double)
  FROM dex_solana.trades d JOIN grad g ON d.token_sold_mint_address = g.mint
  WHERE d.block_month BETWEEN date_trunc('month', DATE '{d0}') AND DATE '{d1}' AND d.block_time >= TIMESTAMP '{start}'
    AND d.block_time <= TIMESTAMP '{end}' AND d.token_bought_mint_address = '{wsol}' AND d.project NOT IN ('pumpdotfun', 'pumpswap')),
bots AS (SELECT w FROM {bots}),
legs AS (
  SELECT tr.mint, tr.w, tr.b, tr.bt, CASE WHEN tr.b THEN tr.sa + tr.fe ELSE tr.sa - tr.fe END AS sol, tr.ta, tr.evt_block_slot AS slot
  FROM tr JOIN tok ON tok.mint = tr.mint
  UNION ALL SELECT dx.mint, dx.w, dx.b, dx.bt, dx.sol, dx.ta, CAST(NULL AS bigint) FROM dx),
pos AS (
  SELECT l.mint, l.w,
    sum(CASE WHEN l.b THEN l.sol ELSE 0 END) AS paid, sum(CASE WHEN l.b THEN l.ta ELSE 0 END) AS tin,
    sum(CASE WHEN NOT l.b THEN l.sol ELSE 0 END) AS recv, sum(CASE WHEN NOT l.b THEN l.ta ELSE 0 END) AS tout,
    sum(CASE WHEN l.b AND l.slot IS NULL THEN l.sol ELSE 0 END) AS paid_dx, sum(CASE WHEN NOT l.b AND l.slot IS NULL THEN l.sol ELSE 0 END) AS recv_dx,
    sum(CASE WHEN l.b THEN coalesce(h.lam, {fall}) ELSE 0 END) AS cbuy,
    sum(CASE WHEN NOT l.b THEN coalesce(h.lam, {fall}) ELSE 0 END) AS csell,
    min(CASE WHEN l.b THEN l.slot END) AS fbs, min(CASE WHEN l.b THEN l.bt END) AS t_first, max(l.bt) AS t_last
  FROM legs l LEFT JOIN h ON h.hr = CAST(to_unixtime(date_trunc('hour', l.bt)) AS bigint) GROUP BY 1, 2),
fc AS (SELECT mint, max_by(vs, (evt_block_slot, evt_tx_index, evt_inner_instruction_index)) AS vs,
              max_by(vt, (evt_block_slot, evt_tx_index, evt_inner_instruction_index)) AS vt
       FROM tr WHERE vs IS NOT NULL GROUP BY 1),
fp AS (SELECT mint, max_by(sol / ta, (block_slot, tx_index, oi, ii)) AS px FROM dx WHERE ta > 0 AND sol > 0 GROUP BY 1),
k AS (
  SELECT pos.*, date_diff('day', DATE '{base}', CAST(tok.t_create AS date)) AS dayi,
    (pos.w = tok.creator OR pos.w = tok.signer) AS is_creator, coalesce(pos.fbs <= tok.cslot + 2, false) AS is_lb,
    (bots.w IS NOT NULL) AS is_bot, greatest(pos.tin - pos.tout, 0) AS resid, (pos.tout > pos.tin) AS oversold,
    (grad.mint IS NOT NULL) AS is_grad, fp.px, fc.vs, fc.vt
  FROM pos JOIN tok ON tok.mint = pos.mint LEFT JOIN bots ON bots.w = pos.w LEFT JOIN grad ON grad.mint = pos.mint
  LEFT JOIN fp ON fp.mint = pos.mint LEFT JOIN fc ON fc.mint = pos.mint),
x AS (SELECT *, CASE WHEN resid = 0 THEN 0 WHEN is_grad THEN coalesce(sqrt({k} * px) * resid / (sqrt({k} / px) + resid), 0) * (1 - 0.0025)
                     ELSE (vs - vs * vt / (vt + resid)) * (1 - 0.01) END AS liq,
                CASE WHEN is_grad AND resid > 0 THEN coalesce(px, 0) * resid * (1 - 0.0025) ELSE 0 END AS liq_noimp FROM k),
y AS (
  SELECT dayi, w,
    CASE WHEN is_creator THEN 'creator' WHEN is_lb THEN 'launch-block' WHEN is_bot THEN 'bot' WHEN oversold THEN 'oversold'
         WHEN paid < 1e6 THEN 'dust' ELSE 'retail' END AS cls,
    paid + cbuy AS ci, recv - csell + CASE WHEN resid > 0 AND coalesce(liq, 0) > {txc_end} THEN liq - {txc_end} ELSE 0 END AS co,
    to_unixtime(t_last) - to_unixtime(t_first) AS hs, tout >= 0.99 * tin AS ex, is_grad, paid, cbuy, recv, csell, paid_dx, recv_dx,
    CASE WHEN resid > 0 AND coalesce(liq, 0) > {txc_end} THEN liq - {txc_end} ELSE 0 END AS liq_used,
    CASE WHEN liq_noimp > {txc_end} THEN liq_noimp - {txc_end} ELSE 0 END AS liq_noimp_used, resid > 0 AS has_resid
  FROM x WHERE paid + cbuy > 0),
z AS (
  SELECT *, co / ci AS xm, row_number() OVER (PARTITION BY w, cls) AS rn,
         sum(co) OVER (PARTITION BY w, cls) > sum(ci) OVER (PARTITION BY w, cls) AS ahead FROM y),
wl AS (SELECT w, count(*) AS k, sum(ci) AS ci, sum(co) AS co FROM y WHERE cls = 'retail' GROUP BY 1)
SELECT 'pos' AS blk, dayi, cls, CAST(floor(log10(greatest(xm, 1e-8)) * 20) AS integer) AS xb, CAST(NULL AS varchar) AS hb,
  CAST(NULL AS boolean) AS ex, CAST(NULL AS varchar) AS kb, CAST(NULL AS varchar) AS rb,
  count(*) AS n, sum(ci) AS ci, sum(co) AS co, sum(xm) AS sx, count_if(rn = 1) AS wallets, count_if(rn = 1 AND ahead) AS wallets_ahead,
  count_if(xm > 1) AS win, sum(CASE WHEN xm >= 100 THEN xm ELSE 0 END) AS jx,
  count_if(xm >= 10) AS hill_n, sum(CASE WHEN xm >= 10 THEN ln(xm / 10) ELSE 0 END) AS hill_s,
  {ge}
FROM z GROUP BY 1, 2, 3, 4
UNION ALL
SELECT 'hold', NULL, 'retail', CAST(floor(log10(greatest(xm, 1e-8)) * 4) AS integer),
  CASE WHEN hs < 60 THEN '1 <1m' WHEN hs < 300 THEN '2 1-5m' WHEN hs < 1800 THEN '3 5-30m' WHEN hs < 7200 THEN '4 30m-2h'
       WHEN hs < 86400 THEN '5 2-24h' WHEN hs < 604800 THEN '6 1-7d' ELSE '7 7d+' END, ex, NULL, NULL,
  count(*), sum(ci), sum(co), sum(xm), NULL, NULL, count_if(xm > 1), NULL, NULL, NULL, {ge}
FROM z WHERE cls = 'retail' GROUP BY 1, 2, 3, 4, 5, 6
UNION ALL
SELECT 'wal', NULL, 'retail', NULL, NULL, NULL,
  CASE WHEN k = 1 THEN '1' WHEN k < 10 THEN '2-9' WHEN k < 100 THEN '10-99' ELSE '100+' END,
  CASE WHEN co < 0.1 * ci THEN '1 lost 90%+' WHEN co < 0.5 * ci THEN '2 lost 50-90%' WHEN co <= ci THEN '3 lost 0-50%'
       WHEN co < 2 * ci THEN '4 up 0-100%' ELSE '5 doubled+' END,
  count(*), sum(ci), sum(co), NULL, count(*), count_if(co > ci), NULL, NULL, NULL, NULL, {ge0}
FROM wl GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
UNION ALL
SELECT 'cls', NULL, cls, NULL, CASE WHEN is_grad THEN 'grad' ELSE 'curve' END, ex, NULL, NULL,
  count(*), sum(ci), sum(co), sum(paid), sum(cbuy), sum(recv), count_if(has_resid), sum(csell), sum(paid_dx), sum(recv_dx),
  sum(liq_used), sum(liq_noimp_used), {ge0x}
FROM y GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
UNION ALL
SELECT 'size', NULL, 'retail', NULL,
  CASE WHEN ci < 1e7 THEN '1 <0.01' WHEN ci < 1e8 THEN '2 0.01-0.1' WHEN ci < 1e9 THEN '3 0.1-1' WHEN ci < 1e10 THEN '4 1-10'
       ELSE '5 10+' END, NULL, NULL, NULL,
  count(*), sum(ci), sum(co), sum(xm), NULL, NULL, count_if(xm > 1), NULL, NULL, NULL, {ge}
FROM z WHERE cls = 'retail' GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
"""

SQL_BOTS = "WITH " + SQL[SQL.index("cr AS (\n"):SQL.index("bots AS (SELECT w FROM {bots}),")] + """hrc AS (
  SELECT w, max(n) AS mh FROM (
    SELECT w, hh, count(*) AS n FROM (SELECT w, date_trunc('hour', bt) AS hh FROM tr UNION ALL SELECT w, date_trunc('hour', bt) FROM dx)
    GROUP BY 1, 2) GROUP BY 1)
SELECT w FROM hrc WHERE mh > 200
UNION SELECT w FROM tr GROUP BY evt_tx_id, w, mint HAVING bool_or(b) AND bool_or(NOT b)
UNION SELECT w FROM dx GROUP BY tx_id, w, mint HAVING bool_or(b) AND bool_or(NOT b)
"""


def main(wn):
    W = WINDOWS[wn]
    os.makedirs(OUT, exist_ok=True)
    f_txc = f"{OUT}/{wn}_txcost.csv"
    if not os.path.exists(f_txc):
        st, cost, nb, s = run(SQL_TXC.format(**W), "medium", f_txc + ".tmp", max_wait=3 * 3600)
        print("txcost", st, cost, nb, s.get("error"), flush=True)
        if st != "QUERY_STATE_COMPLETED":
            return
        os.replace(f_txc + ".tmp", f_txc)
    txc = pd.read_csv(f_txc)
    fall = float(txc.lam.median())
    end_hr = int(pd.Timestamp(W["end"], tz="UTC").floor("h").timestamp())
    txc_end = float(txc.set_index("hr").lam.get(end_hr, fall))
    hours = ", ".join(f"({int(r.hr)}, {float(r.lam)})" for r in txc.itertuples())
    print(f"{wn}: {len(txc)} hours, fallback {fall}, end-hour cost {txc_end}", flush=True)
    f_bots = f"{OUT}/{wn}_bots.csv"
    if not os.path.exists(f_bots):
        st, cost, nb, s = run(SQL_BOTS.format(ord=ORD, wsol=WSOL, **W), "medium", f_bots + ".tmp", max_wait=4 * 3600)
        print("bots", st, cost, nb, s.get("error"), flush=True)
        if st != "QUERY_STATE_COMPLETED":
            return
        os.replace(f_bots + ".tmp", f_bots)
    f_agg = f"{OUT}/{wn}_agg_v4.csv"
    if os.path.exists(f_agg):
        return
    bots = pd.read_csv(f_bots)
    print(f"{wn}: {len(bots)} bot wallets", flush=True)
    up = upload_retry(bots[["w"]], f"ms_y24_bots_{wn}")["full_name"]
    try:
        q = SQL.format(hours=hours, fall=fall, txc_end=txc_end, ord=ORD, wsol=WSOL, bots=up,
                       ge=", ".join(f"count_if(xm >= {m}) AS ge{m}" for m in GRID),
                       ge0=", ".join("NULL" for _ in GRID), ge0x=", ".join("NULL" for _ in GRID[2:]), k=K_POOL, **W)
        st, cost, nb, s = run(q, "medium", f_agg + ".tmp", max_wait=4 * 3600)
        print("agg", st, cost, nb, s.get("error"), flush=True)
        if st == "QUERY_STATE_COMPLETED":
            os.replace(f_agg + ".tmp", f_agg)
    finally:
        drop(up)


if __name__ == "__main__":
    main(sys.argv[1])
