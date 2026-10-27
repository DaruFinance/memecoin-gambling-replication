"""Exploratory, not registered: descriptive cuts of the Study 3 retail samples.

1. Polymarket retail by coarse market category: pooled RTP, share of positions won, positions and wallets per category.
2. Every venue: pooled RTP and share of positions won by stake decile (deciles of cash in, computed within each venue).
Uses the same retail filters as s3_analyze.py. Reads DATA/s2_positions.parquet, DATA/hl/episodes, DATA/hl/vault_addresses.json,
DATA/pm/positions_v2 and the Polymarket market metadata in DATA/pm/markets. Writes AGG/s3_explore_cuts.json.
Run: python3 s3_explore_cuts.py
"""
import json
import os

import duckdb

from config import AGG, DATA, TMP

D = DATA
c = duckdb.connect()
c.sql(f"set threads=1; set memory_limit='4GB'; set temp_directory='{TMP}/s3_explore_cuts_{os.getpid()}'; set TimeZone='UTC'; set preserve_insertion_order=false")
out = {}
c.sql(f"""create table pm as
  with act as (select * from read_parquet('{D}/pm/positions_v2/act_*.parquet')),
  bad as (select w from act where max_hour > 200 or (fills >= 1000 and maker_fills >= 0.8 * fills))
  select p.w, p.token, p.cash_in, p.cash_out from read_parquet('{D}/pm/positions_v2/pos_*.parquet') p
  where not p.oversold and p.cash_in > 0 and p.cash_out is not null and p.w not in (select w from bad)""")
# the category and tag fields are empty in the market metadata, so classes come from keyword rules on series, event title and question
c.sql(f"""create table cat as select pm_token_id token,
          case when coalesce(series, '') ilike '%up or down%' or event_title ilike '%up or down%' then 'crypto up/down (short-interval)'
               when regexp_matches(coalesce(series, '') || ' ' || coalesce(event_title, ''),
                    '(?i)valorant|counter-strike|cs2|league of legends|lol:|dota|esports|overwatch|rocket league|call of duty') then 'esports'
               when regexp_matches(coalesce(series, '') || ' ' || coalesce(event_title, ''),
                    '(?i)\\bnba\\b|\\bnfl\\b|\\bmlb\\b|\\bnhl\\b|\\bwnba\\b|cbb|cwbb|ncaa|premier league|la liga|serie a|bundesliga|ligue 1|champions league|\\bufc\\b|tennis|atp|wta|f1|grand prix|golf|pga|cricket|boxing| vs\\.? ') then 'sports'
               when regexp_matches(coalesce(event_title, '') || ' ' || coalesce(question, ''),
                    '(?i)election|president|senate|congress|trump|biden|governor|parliament|prime minister|nominee|primary') then 'politics'
               when regexp_matches(coalesce(event_title, '') || ' ' || coalesce(question, ''), '(?i)bitcoin|ethereum|solana|crypto|btc|eth |xrp|price of') then 'crypto (other)'
               else 'other' end category
          from read_parquet('{D}/pm/markets/*.parquet', union_by_name=true) where is_current""")
out["pm_by_category"] = c.sql("""select coalesce(k.category, 'unmapped') cat,
      count(*) positions, count(distinct p.w) wallets, sum(p.cash_in) stake_usd, sum(p.cash_out) / sum(p.cash_in) rtp,
      avg((p.cash_out > p.cash_in)::int) position_win_rate
    from pm p left join cat k using (token) group by 1 having sum(p.cash_in) > 1e6 order by stake_usd desc""").df().to_dict("records")
srcs = {
  "meme": f"""select w, cash_in, cash_out from read_parquet('{D}/s2_positions.parquet')
             where not is_creator and not is_launch_block and not is_distributed and not is_protocol and not is_atomic_bot
               and max_hour <= 200 and not oversold and cash_in > 0 and isfinite(cash_out)""",
  "pm": "select w, cash_in, cash_out from pm",
}
vaults = json.load(open(f"{D}/hl/vault_addresses.json"))
c.sql(f"create table vaults as select unnest(from_json('{json.dumps(vaults)}', '[\"VARCHAR\"]')) u")
srcs["hl"] = f"""with act as (select * from read_parquet('{D}/hl/episodes/act_*.parquet')),
  bad as (select user from act where max_hour > 200 or (fills >= 1000 and maker_fills >= 0.8 * fills) union select u from vaults)
  select user w, stake cash_in, stake + pnl cash_out from read_parquet('{D}/hl/episodes/ep_*.parquet')
  where not pre_window and stake > 0 and user not in (select user from bad)"""
for v, q in srcs.items():
    out[f"{v}_by_stake_decile"] = c.sql(f"""with s as ({q}), d as (select *, ntile(10) over (order by cash_in) decile from s)
      select decile, min(cash_in) lo, max(cash_in) hi, count(*) n, sum(cash_out) / sum(cash_in) rtp, avg((cash_out > cash_in)::int) win_rate
      from d group by 1 order by 1""").df().to_dict("records")
    print(v, "done", flush=True)
json.dump(out, open(f"{AGG}/s3_explore_cuts.json", "w"), indent=1, default=float)
for k, v in out.items():
    print(k)
    for r in v:
        print("  ", {kk: (round(vv, 4) if isinstance(vv, float) else vv) for kk, vv in r.items()})
