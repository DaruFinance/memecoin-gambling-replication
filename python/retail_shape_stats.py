"""Retail outcome shape statistics used in the paper (descriptive, not registered) for the memecoin and Polymarket arms,
under the exact Study 3 retail filters (s3_analyze.load_meme and s3_analyze.load_pm).

Per venue: position win rate, median multiple and survival P(X >= m); wallet outcome buckets by wallet RTP; share of
wallets ahead by number of positions. The position and wallet counts and the share of wallets ahead are checked against
the Study 3 primary results (REG) before anything is written.

Reads the inputs of s3_analyze.py. Writes AGG/retail_shape_stats.json.

Run: python retail_shape_stats.py
"""
import json
import os
import sys

import duckdb

from s3_analyze import load_meme, load_pm
from config import AGG, DATA, TMP

D = DATA
REG = {"meme": (44_114_785, 6_693_737, 0.14127997559509733), "pm": (21_428_139, 327_866, 0.3901227940683084)}

c = duckdb.connect()
c.sql(f"set threads=2; set memory_limit='4GB'; set preserve_insertion_order=false; "
      f"set temp_directory='{TMP}/retail_shape_stats_{os.getpid()}'; set TimeZone='UTC'")
out = {}
for v, loader in (("meme", load_meme), ("pm", load_pm)):
    sql = loader()[1]
    c.sql(f"create or replace temp table p as select wh, cash_in, cash_out, cash_out / cash_in x from ({sql}) "
          f"where isfinite(cash_out) and cash_in > 0")
    n, med, win = c.sql("select count(*), median(x), avg((x > 1)::int) from p").fetchone()
    surv = {m: c.sql(f"select avg((x >= {m})::int) from p").fetchone()[0] for m in (2, 5, 10, 100, 1000)}
    c.sql("create or replace temp table w as select wh, count(*) k, sum(cash_in) ci, sum(cash_out) co, sum(cash_out) / sum(cash_in) r "
          "from p group by wh")
    nw, ahead = c.sql("select count(*), avg((co > ci)::int) from w").fetchone()
    assert (n, nw) == REG[v][:2], (v, n, nw)
    assert abs(ahead - REG[v][2]) < 1e-9, (v, ahead)
    buckets = dict(c.sql("""select case when r < 0.1 then '1 lost 90%+' when r < 0.5 then '2 lost 50-90%' when r <= 1 then '3 lost 0-50%'
                             when r < 2 then '4 up 0-100%' else '5 doubled+' end b, count(*) / sum(count(*)) over () s
                             from w group by b order by b""").fetchall())
    by_k = {b: list(x) for b, *x in c.sql("""select case when k = 1 then '1' when k < 10 then '2-9' when k < 100 then '10-99' else '100+' end b,
                                            count(*), avg((co > ci)::int), sum(co) / sum(ci) from w group by b order by min(k)""").fetchall()}
    out[v] = {"positions": n, "wallets": nw, "share_ahead": ahead, "median_x": med, "win_rate": win, "surv": surv,
              "wallet_buckets": buckets, "by_positions": by_k}
    print(v, json.dumps(out[v], default=float), flush=True)
json.dump(out, open(f"{AGG}/retail_shape_stats.json", "w"), indent=1, default=float)
