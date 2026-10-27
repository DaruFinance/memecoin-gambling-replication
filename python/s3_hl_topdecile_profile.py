"""Exploratory, not registered: who is in the Hyperliquid H5 top decile?

Uses the same retail episodes and halves as s3_analyze.py (venue hl). The top decile is the wallets with at least 5 episodes
in each half whose half-1 ROI is in the top 10%. They are compared with the other eligible wallets on notional, hold time,
maker share, liquidations, coins, fees and funding.
Reads DATA/hl/episodes, DATA/hl/vault_addresses.json and the halves midpoint from AGG/s3_results_hl.json (run
s3_analyze.py hl first). Writes AGG/s3_hl_topdecile_profile.json.
Run: python3 s3_hl_topdecile_profile.py
"""
import json
import os

import duckdb

from config import AGG, DATA, TMP

D = DATA
import pandas as pd
MID_MS = int(pd.Timestamp(json.load(open(f"{AGG}/s3_results_hl.json"))["halves_mid_utc"]).timestamp() * 1000)
c = duckdb.connect()
c.sql(f"set threads=4; set memory_limit='4GB'; set temp_directory='{TMP}/s3_hl_topdecile_profile_{os.getpid()}'")
vaults = json.load(open(f"{D}/hl/vault_addresses.json"))
c.sql(f"create table vaults as select unnest(from_json('{json.dumps(vaults)}', '[\"VARCHAR\"]')) u")
c.sql(f"""create table e as
  with act as (select * from read_parquet('{D}/hl/episodes/act_*.parquet')),
  bad as (select user from act where max_hour > 200 or (fills >= 1000 and maker_fills >= 0.8 * fills) union select u from vaults)
  select user, coin, t0, t1, stake, pnl, fees, funding, liq, maker_share, n_legs, (t0 >= {MID_MS})::int half
  from read_parquet('{D}/hl/episodes/ep_*.parquet') where not pre_window and stake > 0 and user not in (select user from bad)""")
c.sql("""create table el as select user, sum(case when half = 0 then stake + pnl end) / sum(case when half = 0 then stake end) roi1
         from e group by user having sum((half = 0)::int) >= 5 and sum((half = 1)::int) >= 5""")
q90 = c.sql("select quantile_cont(roi1, 0.9) from el").fetchone()[0]
c.sql(f"create table grp as select user, roi1 >= {q90} top from el")
out = {"q90_roi1": q90, "profile": c.sql("""select g.top, count(*) episodes, count(distinct e.user) wallets,
    median(e.stake) med_peak_notional_usd, median((e.t1 - e.t0) / 1000.0) med_hold_s, avg(((e.t1 - e.t0) <= 60000)::int) share_hold_le60s,
    avg(e.maker_share) avg_maker_share, avg(e.liq::double) liquidation_rate, avg(e.n_legs) avg_fills,
    avg((e.coin in ('BTC', 'ETH'))::int) share_btc_eth, sum(e.fees) / sum(e.stake) fees_per_notional,
    sum(e.funding) / nullif(sum(abs(e.pnl)), 0) funding_vs_abs_pnl, sum(e.stake + e.pnl) / sum(e.stake) rtp, avg((e.pnl > 0)::int) win_rate
    from e join grp g using (user) group by 1 order by 1""").df().to_dict("records"),
    "per_wallet": c.sql("""select g.top, median(n) med_episodes, median(coins) med_coins from
      (select user, count(*) n, count(distinct coin) coins from e group by 1) x join grp g using (user) group by 1 order by 1""").df().to_dict("records")}
json.dump(out, open(f"{AGG}/s3_hl_topdecile_profile.json", "w"), indent=1, default=float)
print(json.dumps(out, indent=1, default=float))
