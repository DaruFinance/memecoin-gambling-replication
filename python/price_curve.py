"""Curve-phase inputs for every bet time point (Study 1 registration, Pricing, curve phase). No X is computed here.

Writes to data/:
  curve_points.parquet  bet_id, role, t, state source, vS, vT, real tokens, creator fee bps, gap flags
  entry_inputs.parquet  bet_id, SOLUSD at entry, stake in lamports for each stake size, hourly tx cost at entry and exit
  curve_peak.parquet    bet_id, highest post-trade vS (and its vT) on the curve inside (t_entry, min(t_exit, t_grad)]
  tsec.parquet          curve state per (mint, second), merged from data/tsec_parts; built only if it does not exist
Study 1 registration, Erratum 3: tokens that completed their curve and moved to PumpSwap without a recorded graduation
event take t_grad = the completing curve trade (data/grad_inferred.parquet); the hourly tx cost counts each transaction
once (data/s2_txcost_hourly.parquet, the Study 2 table); curve points also carry an unregistered flag, gap_ext, for every
curve-stream silence over 5 minutes (data/trade_outages.parquet).
Also reads data/bets_v2.parquet, the create events in data/hf_pumpfun_v2 and the Binance SOLUSDT 1-minute klines in
data/binance. Uses one thread and 1.5 GB of memory.
Run: python3 price_curve.py
"""
import glob
import zipfile

import duckdb
import pandas as pd

from config import DATA, TMP

R = f"{DATA}/hf_pumpfun_v2"
D = DATA
ROLES = ["t_entry", "t_exit", "t_h5m", "t_h1h", "t_h24h", "t_h7d"]
# registered trade-stream gap hours, converted from UTC-3 to UTC, as [start, end)
GAPS = [("2026-09-05 04:00:00", "2026-09-05 16:00:00"), ("2026-09-06 05:00:00", "2026-09-06 06:00:00"),
        ("2026-09-06 12:00:00", "2026-09-06 14:00:00")]

c = duckdb.connect()
c.sql(f"set threads=1; set memory_limit='1500MB'; set temp_directory='{TMP}'; set TimeZone='UTC'; set preserve_insertion_order=false")
tcols = ["t_create", "t_grad", "t_entry", "t_exit", "t_h5m", "t_h1h", "t_h24h", "t_h7d"]
# stored bet times are naive UTC; cast to TIMESTAMPTZ (session TimeZone=UTC) so they compare with trade times
c.sql(f"create table bets as select * replace ({', '.join(f'{x}::timestamptz as {x}' for x in tcols)}) from '{D}/bets_v2.parquet'")
c.sql(f"""update bets set t_grad = g.t_grad, pool = g.pool from read_parquet('{D}/grad_inferred.parquet') g
          where bets.mint = g.mint and bets.t_grad is null""")
c.sql("create table mints as select distinct mint from bets")

# 1. per (mint, second): pre-state of the first trade, post-state of the last trade. The per-file parts merge exactly
#    because arg_min/arg_max over (slot, tx_index) compose across parts.
PART = f"{D}/tsec_parts"
import os
if os.path.exists(f"{D}/tsec.parquet"):
    c.sql(f"create table tsec as select * from '{D}/tsec.parquet'")
else:
    c.sql(f"""
    create table tsec as
    select mint, sec,
      arg_min(pre_vs, kn) pre_vs, arg_min(pre_vt, kn) pre_vt, arg_min(pre_rt, kn) pre_rt, arg_min(pre_cb, kn) pre_cb,
      arg_max(post_vs, kx) post_vs, arg_max(post_vt, kx) post_vt, arg_max(post_rt, kx) post_rt, arg_max(post_cb, kx) post_cb
    from read_parquet('{PART}/*.parquet') group by all
    """)
    c.sql(f"copy tsec to '{D}/tsec.parquet' (format parquet)")
c.sql("""create table mint_cb as select mint, max(coalesce(pre_cb, post_cb)) cb from tsec group by mint""")
print("tsec rows:", c.sql("select count(*) from tsec").fetchone()[0], flush=True)

c.sql(f"""create table cr as select mint, any_value(virtual_sol_reserves::double) vs, any_value(virtual_token_reserves::double) vt,
          any_value(real_token_reserves::double) rt from (select distinct on (payload_hash) * from read_parquet('{R}/*/*/create/*.parquet'))
          where mint in (select mint from mints) group by mint""")

# 2. curve-phase points
union = " union all ".join(f"select bet_id, mint, t_create, t_grad, '{r}' as role, {r} as t from bets" for r in ROLES)
c.sql(f"create table pts as select *, date_trunc('second', t) tsec_ from ({union}) where t_grad is null or t < t_grad")
print("curve points:", c.sql("select count(*) from pts").fetchone()[0], flush=True)
c.sql("""
create table nx as select p.bet_id, p.role, q.sec nsec, q.pre_vs, q.pre_vt, q.pre_rt, q.pre_cb
from pts p asof left join tsec q on p.mint = q.mint and q.sec >= p.tsec_""")
c.sql("""
create table pv as select p.bet_id, p.role, q.sec psec, q.post_vs, q.post_vt, q.post_rt, q.post_cb
from pts p asof left join tsec q on p.mint = q.mint and q.sec < p.tsec_""")
gap_expr = " or ".join(f"(least(p.t, u.src_t) < TIMESTAMP '{b}+00' and greatest(p.t, u.src_t) > TIMESTAMP '{a}+00')" for a, b in GAPS)
# unregistered flag: every silence of more than 5 minutes in the curve trade stream
OUTAGES = c.sql(f"select strftime(\"start\", '%Y-%m-%d %H:%M:%S'), strftime(\"end\", '%Y-%m-%d %H:%M:%S') from read_parquet('{D}/trade_outages.parquet') where secs > 300").fetchall()
ext_expr = " or ".join(f"(least(u.t, u.src_t) < TIMESTAMP '{b}+00' and greatest(u.t, u.src_t) > TIMESTAMP '{a}+00')" for a, b in OUTAGES)
c.sql(f"""
create table curve_points as
with u as (
  select p.bet_id, p.role, p.t, p.mint,
    case when nx.nsec is not null then 'next' when pv.psec is not null then 'prev' else 'create' end src,
    coalesce(nx.nsec, pv.psec, p.t_create) src_t,
    coalesce(nx.pre_vs, pv.post_vs, cr.vs) vs, coalesce(nx.pre_vt, pv.post_vt, cr.vt) vt,
    coalesce(nx.pre_rt, pv.post_rt, cr.rt) rt,
    coalesce(nx.pre_cb, pv.post_cb, mc.cb, 0) cb
  from pts p join nx using (bet_id, role) join pv using (bet_id, role)
  left join cr on cr.mint = p.mint left join mint_cb mc on mc.mint = p.mint
)
select u.bet_id, u.role, u.t, u.src, u.src_t, u.vs, u.vt, u.rt, u.cb, ({gap_expr.replace('p.t', 'u.t')}) as gap_excluded,
  ({ext_expr}) as gap_ext
from u
""")
c.sql(f"copy curve_points to '{D}/curve_points.parquet' (format parquet)")
print(c.sql("select role, src, count(*) n, sum(gap_excluded::int) gap, sum(gap_ext::int) gap_ext from curve_points group by all order by all").df().to_string(index=False), flush=True)

# 3. SOLUSD at entry (open of the minute containing t_entry) and hourly tx cost
rows = []
for z in sorted(glob.glob(f"{D}/binance/SOLUSDT-1m-*.zip")):
    with zipfile.ZipFile(z) as zz:
        df = pd.read_csv(zz.open(zz.namelist()[0]), header=None, usecols=[0, 1])
    rows.append(df)
sol = pd.concat(rows)
sol.columns = ["open_time", "open"]
sol["open_time"] = sol.open_time.where(sol.open_time < 1e14, sol.open_time // 1000)  # microseconds to milliseconds where needed
sol["minute"] = pd.to_datetime(sol.open_time, unit="ms", utc=True)
c.register("sol", sol[["minute", "open"]])
c.sql(f"create table txcost as select hr, lam lamports from read_parquet('{D}/s2_txcost_hourly.parquet')")
c.sql(f"""
create table entry_inputs as
select b.bet_id, s.open solusd,
  10.0 / s.open * 1e9 stake10_lam, 100.0 / s.open * 1e9 stake100_lam, 1000.0 / s.open * 1e9 stake1000_lam,
  te.lamports cost_entry, tx.lamports cost_exit
from bets b
left join sol s on s.minute = date_trunc('minute', b.t_entry)
left join txcost te on te.hr = date_trunc('hour', b.t_entry)
left join txcost tx on tx.hr = date_trunc('hour', b.t_exit)
""")
c.sql(f"copy entry_inputs to '{D}/entry_inputs.parquet' (format parquet)")
print(c.sql("select count(*) n, count(solusd) with_sol, count(cost_entry) with_ce, count(cost_exit) with_cx from entry_inputs").df().to_string(index=False), flush=True)

# 4. curve peak: max post-trade vS within (t_entry, min(t_exit, t_grad)]. On the curve vS * vT = k, so the sell value
#    of a fixed q rises with vS.
c.sql(f"""
create table curve_peak as
select b.bet_id, arg_max(q.post_vs, q.post_vs) peak_vs, arg_max(q.post_vt, q.post_vs) peak_vt, arg_max(q.sec, q.post_vs) peak_sec
from bets b join tsec q on q.mint = b.mint
  and q.sec > date_trunc('second', b.t_entry)
  and q.sec <= least(b.t_exit, coalesce(b.t_grad, b.t_exit))
group by b.bet_id
""")
c.sql(f"copy curve_peak to '{D}/curve_peak.parquet' (format parquet)")
print("curve_peak rows:", c.sql("select count(*) from curve_peak").fetchone()[0], flush=True)
