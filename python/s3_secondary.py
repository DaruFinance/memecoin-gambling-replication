"""Study 3 registered secondary reports.

1. The excluded classes on each venue, reported separately with their net flows, as the Study 3 registration requires.
   Classes are assigned in priority order, so each position or episode is counted once.
2. Hyperliquid levered view: liquidation rates for retail episodes.
3. Polymarket combo exchange: its share of window volume and its wallet count (Study 3 registration, S3-A5).
Reads DATA/s2_positions.parquet, DATA/hl/episodes, DATA/hl/vault_addresses.json, DATA/pm/positions_v2 and the Polymarket
fills under DATA/pm/fills and DATA/pm/combo_fills. Writes AGG/s3_secondary.json.
Run: python3 s3_secondary.py
"""
import json
import os

import duckdb

from config import AGG, DATA, TMP

D = DATA
c = duckdb.connect()
c.sql(f"set threads=1; set memory_limit='1500MB'; set temp_directory='{TMP}/s3_secondary_{os.getpid()}'; set TimeZone='UTC'")
out = {}

flows = """count(*) n, count(distinct {w}) wallets, sum(cin) cash_in, sum(cout) cash_out, sum(cout) - sum(cin) net,
           sum(cout) / nullif(sum(cin), 0) rtp"""

# memecoins, in SOL (s2_positions amounts are lamports)
out["meme_classes_sol"] = c.sql(f"""
  with p as (select w, cash_in / 1e9 cin, cash_out / 1e9 cout,
    case when is_creator then '1 creator' when is_launch_block then '2 launch block' when is_distributed then '3 distributed (insider)'
         when is_protocol then '4 protocol' when is_atomic_bot then '5 atomic bot' when max_hour > 200 then '6 > 200 fills/hour'
         when oversold then '7 oversold (no cost basis)' else '0 retail' end cls
    from read_parquet('{D}/s2_positions.parquet') where cash_in > 0 and isfinite(cash_out))
  select cls, {flows.format(w='w')} from p group by 1 order by 1""").df().to_dict("records")

c.sql(f"create table vaults as select unnest(from_json('{json.dumps(json.load(open(f'{D}/hl/vault_addresses.json')))}', '[\"VARCHAR\"]')) u")
out["hl_classes_usd"] = c.sql(f"""
  with act as (select * from read_parquet('{D}/hl/episodes/act_*.parquet')),
  e as (select e.user, e.stake cin, e.stake + e.pnl cout, e.liq,
    case when e.pre_window then '1 opened before window' when e.user in (select u from vaults) then '2 vault'
         when a.max_hour > 200 then '3 > 200 fills/hour' when a.fills >= 1000 and a.maker_fills >= 0.8 * a.fills then '4 market maker'
         else '0 retail' end cls
    from read_parquet('{D}/hl/episodes/ep_*.parquet') e left join act a using (user) where e.stake > 0)
  select cls, {flows.format(w='user')}, avg(liq::double) liquidation_rate from e group by 1 order by 1""").df().to_dict("records")
out["hl_levered_view_retail"] = c.sql(f"""
  with act as (select * from read_parquet('{D}/hl/episodes/act_*.parquet')),
  e as (select e.* from read_parquet('{D}/hl/episodes/ep_*.parquet') e left join act a using (user)
        where not e.pre_window and e.stake > 0 and e.user not in (select u from vaults)
          and not (a.max_hour > 200 or (a.fills >= 1000 and a.maker_fills >= 0.8 * a.fills)))
  select avg(liq::double) episode_liquidation_rate, sum(case when liq = 1 then stake end) / sum(stake) stake_share_liquidated,
         sum(case when liq = 1 then pnl end) liquidated_pnl_usd,
         (select avg(x::double) from (select user, max(liq) = 1 x from e group by 1)) wallet_share_ever_liquidated
  from e""").df().to_dict("records")

out["pm_classes_usd"] = c.sql(f"""
  with act as (select * from read_parquet('{D}/pm/positions_v2/act_*.parquet')),
  p as (select p.w, p.cash_in cin, p.cash_out cout,
    case when a.max_hour > 200 then '1 > 200 fills/hour' when a.fills >= 1000 and a.maker_fills >= 0.8 * a.fills then '2 market maker'
         when p.oversold then '3 oversold (no cost basis)' when p.cash_out is null then '4 no end value' else '0 retail' end cls
    from read_parquet('{D}/pm/positions_v2/pos_*.parquet') p left join act a using (w) where p.cash_in > 0)
  select cls, {flows.format(w='w')} from p group by 1 order by 1""").df().to_dict("records")
e0, e1 = c.sql("select epoch(timestamptz '2026-08-08 00:00:00+00'), epoch(timestamptz '2026-09-18 15:17:00+00')").fetchone()
cb = c.sql(f"""select count(*), sum(usd_amount), count(distinct address) from read_parquet('{D}/pm/combo_fills/*.parquet')
               where ts >= {e0} and ts <= {e1}""").fetchone()
al = c.sql(f"""select sum(usd_amount), count(distinct address) from read_parquet('{D}/pm/fills/*.parquet')
               where ts >= {e0} and ts <= {e1}""").fetchone()
out["pm_combo_share"] = {"combo_fills": cb[0], "combo_usd": cb[1], "combo_wallets": cb[2],
                         "share_of_usd": cb[1] / (cb[1] + al[0]), "single_market_wallets": al[1]}
json.dump(out, open(f"{AGG}/s3_secondary.json", "w"), indent=1, default=float)
print(json.dumps(out, indent=1, default=float))
