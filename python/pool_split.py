"""Exploratory check (not registered): 2026 memecoin retail RTP for positions that traded in a PumpSwap pool against
positions that traded only on the bonding curve, under the Study 3 retail filter. Also reports the retail sample without
dust positions (paid under 0.001 SOL), the class that the 2024 Dune windows (dune_2024.py) report separately.

Reads DATA/s2_positions.parquet (from s2_analyze.py). Also reports wallets that staked at least 0.1 SOL in total. Writes AGG/pool_split.json.

Run: python pool_split.py
"""
import json
import os

import duckdb

from config import AGG, DATA, TMP

D = DATA
BASE = ("not is_creator and not is_launch_block and not is_distributed and not is_protocol and not is_atomic_bot "
        "and max_hour <= 200 and not oversold and cash_in > 0 and isfinite(cash_out)")
c = duckdb.connect()
c.sql(f"set threads=2; set memory_limit='3GB'; set preserve_insertion_order=false; "
      f"set temp_directory='{TMP}/pool_split_{os.getpid()}'")
rows = c.sql(f"""select (n_buy_pool + n_sell_pool > 0) pool, count(*) n, sum(cash_in) ci, sum(cash_out) co
                 from read_parquet('{D}/s2_positions.parquet') where {BASE} group by 1 order by 1""").fetchall()
tot_loss = sum(r[2] - r[3] for r in rows)
out = {("touched_pool" if p else "curve_only"): {"positions": n, "sol_in": ci, "rtp": co / ci, "share_of_retail_loss": (ci - co) / tot_loss}
       for p, n, ci, co in rows}
d = c.sql(f"""with p as (select w, cash_in, cash_out from read_parquet('{D}/s2_positions.parquet') where {BASE} and paid >= 1e6),
               g as (select sum(cash_in) ci, sum(cash_out) co from p group by w)
               select (select count(*) from p), (select sum(cash_out) / sum(cash_in) from p), avg((co > ci)::int), count(*) from g""").fetchone()
out["no_dust"] = {"positions": d[0], "rtp": d[1], "share_wallets_ahead": d[2], "wallets": d[3]}
b = c.sql(f"""with g as (select w, sum(cash_in) ci, sum(cash_out) co from read_parquet('{D}/s2_positions.parquet') where {BASE} group by w)
               select count(*), avg((co > ci)::int), sum(co) / sum(ci) from g where ci >= 1e8""").fetchone()
out["wallets_staking_0.1_sol_or_more"] = {"wallets": b[0], "share_ahead": b[1], "rtp": b[2]}
json.dump(out, open(f"{AGG}/pool_split.json", "w"), indent=1, default=float)
print(json.dumps(out, indent=1, default=float))
