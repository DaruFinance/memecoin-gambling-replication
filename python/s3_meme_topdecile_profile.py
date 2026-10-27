"""Exploratory, not registered: who is in the memecoin H5 top decile?

Uses the same retail positions and halves as s3_analyze.py (venue meme). The top decile is the wallets with at least 5
positions in each half whose half-1 ROI is in the top 10%. They are compared with the other eligible wallets on entry
speed, hold time, stake, activity and exits.
Reads DATA/s2_positions.parquet and the halves midpoint from AGG/s3_results_meme.json (run s3_analyze.py meme first).
Writes AGG/s3_meme_topdecile_profile.json.
Run: python3 s3_meme_topdecile_profile.py
"""
import json
import os

import duckdb

from config import AGG, DATA, TMP

D = DATA
MID = json.load(open(f"{AGG}/s3_results_meme.json"))["halves_mid_utc"]   # halves midpoint (Erratum S3-E1 item 2)
c = duckdb.connect()
c.sql(f"set threads=1; set memory_limit='4GB'; set temp_directory='{TMP}/s3_meme_topdecile_profile_{os.getpid()}'; set TimeZone='UTC'; set preserve_insertion_order=false")
base = ("not is_creator and not is_launch_block and not is_distributed and not is_protocol and not is_atomic_bot "
        "and max_hour <= 200 and not oversold and cash_in > 0 and isfinite(cash_out)")
c.sql(f"""create table p as select w, mint, cash_in, cash_out, first_t, last_t, t_create, max_hour, n_buy, n_sell, n_buy_pool, n_sell_pool,
          tok_in, tok_out, (first_t >= timestamptz '{MID}')::int half
          from read_parquet('{D}/s2_positions.parquet') where {base}""")
c.sql("""create table wh as select w, sum(case when half = 0 then 1 end) n1, sum(case when half = 1 then 1 end) n2,
            sum(case when half = 0 then cash_out end) / sum(case when half = 0 then cash_in end) roi1,
            sum(case when half = 1 then cash_out end) / sum(case when half = 1 then cash_in end) roi2
         from p group by w""")
c.sql("create table el as select * from wh where n1 >= 5 and n2 >= 5")
q90 = c.sql("select quantile_cont(roi1, 0.9) from el").fetchone()[0]
c.sql(f"create table grp as select w, (roi1 >= {q90}) top, roi2 from el")
out = {"q90_roi1": q90}
out["wallets"] = c.sql("select top, count(*) from grp group by 1 order by 1").fetchall()
out["profile"] = c.sql("""select g.top,
      count(*) positions, count(distinct p.w) wallets,
      median(epoch(p.first_t) - epoch(p.t_create)) med_entry_delay_s,
      avg((epoch(p.first_t) - epoch(p.t_create) <= 10)::int) share_entry_le10s,
      avg((epoch(p.first_t) - epoch(p.t_create) <= 60)::int) share_entry_le60s,
      median(epoch(p.last_t) - epoch(p.first_t)) med_hold_s,
      avg((epoch(p.last_t) - epoch(p.first_t) <= 60)::int) share_hold_le60s,
      median(p.cash_in) med_stake_sol, avg(p.cash_in) mean_stake_sol,
      avg((p.tok_out >= 0.999 * p.tok_in)::int) share_fully_exited,
      avg((p.n_buy_pool + p.n_sell_pool > 0)::int) share_touch_pumpswap,
      sum(p.cash_out) / sum(p.cash_in) rtp_all, avg((p.cash_out > p.cash_in)::int) win_rate,
      median(p.max_hour) med_max_fills_per_hour
    from p join grp g using (w) group by 1 order by 1""").df().to_dict("records")
out["per_wallet"] = c.sql("""select g.top, median(n) med_positions, median(ndays) med_active_days, median(mints) med_coins from
      (select w, count(*) n, count(distinct date_trunc('day', first_t)) ndays, count(distinct mint) mints from p group by w) x
      join grp g using (w) group by 1 order by 1""").df().to_dict("records")
# share of each wallet's half-2 gross profit that comes from its single best position
out["jackpot_share_half2"] = c.sql("""select g.top, median(best / nullif(tot, 0)) med_share_of_profit_from_best from
      (select w, max(cash_out - cash_in) best, sum(greatest(cash_out - cash_in, 0)) tot from p where half = 1 group by w) x
      join grp g using (w) group by 1 order by 1""").fetchall()
print(json.dumps(out, indent=1, default=str))
json.dump(out, open(f"{AGG}/s3_meme_topdecile_profile.json", "w"), indent=1, default=str)
