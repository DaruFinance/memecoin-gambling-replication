"""Study 3, Hyperliquid: build flat-to-flat position episodes per (wallet, coin) for core perpetuals, 8 August to 18 September 2026.

PnL follows the cash-flow identity: -sum(q_i * px_i) + q_end * mark_end - fees - builder fees - funding. A fill that flips
the position is split into a closing leg (old episode) and an opening leg (new episode), with fees prorated by size.
Stake is the peak |position| * px within the episode. Funding is the position held between fills times the hourly funding
payments (rate * oracle price) at hour boundaries. Episodes already open at the window start are flagged pre_window and
excluded from the primary analysis. Per Study 3 registration, Erratum S3-E2, an episode counts as liquidated when it has a
crossed (taker-side) liquidation fill that is not auto-deleveraging, and '#' outcome contracts are excluded from core perps.

Reads DATA/hl/fills/*.parquet (from hl_pull.py) and DATA/hl/asset_ctxs/*.csv (Hyperliquid's hourly asset contexts).
Writes DATA/hl/episodes/ep_NN.parquet (episodes) and act_NN.parquet (per-wallet activity counts) for 16 wallet hash
partitions. Completed partitions are skipped on rerun.
Run: python3 hl_episodes.py
"""
import glob
import os

import duckdb

from config import DATA, TMP

D = f"{DATA}/hl"
OUT = f"{D}/episodes"
NP = 16
EPS = 1e-9


def con():
    db = f"{TMP}/hl_ep.duckdb"
    for f_ in (db, db + ".wal"):
        if os.path.exists(f_):
            os.remove(f_)
    c = duckdb.connect(db)
    c.sql(f"set threads=3; set memory_limit='4GB'; set temp_directory='{TMP}/hl_episodes_{os.getpid()}'; set TimeZone='UTC'; set preserve_insertion_order=false")
    return c


def main():
    os.makedirs(OUT, exist_ok=True)
    c = con()
    # funding index per coin: F(t) = cumulative sum of rate * oracle_px over hour boundaries <= t
    c.sql(f"""create table fund as
      select coin, epoch_ms(time::timestamptz) t_ms,
             sum(funding * oracle_px) over (partition by coin order by time rows unbounded preceding) F
      from read_csv('{D}/asset_ctxs/*.csv', header=true, columns={{'time':'VARCHAR','coin':'VARCHAR','funding':'DOUBLE',
            'open_interest':'DOUBLE','prev_day_px':'DOUBLE','day_ntl_vlm':'DOUBLE','premium':'DOUBLE','oracle_px':'DOUBLE',
            'mark_px':'DOUBLE','mid_px':'DOUBLE','impact_bid_px':'DOUBLE','impact_ask_px':'DOUBLE'}})
      where minute(time::timestamptz) = 0""")
    F = f"read_parquet('{D}/fills/*.parquet')"
    end_ms = c.sql(f"select max(time) from {F}").fetchone()[0]
    c.sql(f"create table mark as select coin, arg_max(px, time) mark from {F} group by coin")
    for p in range(NP):
        out = f"{OUT}/ep_{p:02d}.parquet"
        if os.path.exists(out):
            continue
        # execution order = hour file order, then row order inside the file (blocks, then events, as written)
        c.sql(f"""create or replace table f as
          select user, coin, time, px, crossed, (liq and crossed and dir <> 'Auto-Deleveraging') liq, fee + builder_fee fees, closed_pnl,
                 case when side = 'B' then sz else -sz end q, start,
                 regexp_extract(filename, '([0-9]{{8}}_[0-9]{{2}})', 1) fh, file_row_number fr
          from read_parquet('{D}/fills/*.parquet', filename=true, file_row_number=true)
          where hash(user) % {NP} = {p} and coin not like '%:%' and coin not like '@%' and coin not like '%/%' and coin not like '#%'
            and dir not in ('Buy', 'Sell', 'Split Outcome', 'Merge Outcome')""")
        c.sql(f"""create or replace table legs as
          with g as (select *, start + q pos_after, (abs(start) > {EPS} and abs(start + q) > {EPS} and sign(start) <> sign(start + q)) flip from f)
          select user, coin, time, fh, fr, px, crossed, liq, closed_pnl, start, q, pos_after, fees, false opening_leg from g where not flip
          union all select user, coin, time, fh, fr, px, crossed, liq, closed_pnl, start, -start, 0.0, fees * abs(start) / abs(q), false from g where flip
          union all select user, coin, time, fh, fr, px, crossed, liq, 0.0, 0.0, pos_after, pos_after, fees * abs(pos_after) / abs(q), true from g where flip""")
        c.sql(f"""create or replace table ep as
          with o as (select *, row_number() over (partition by user, coin order by fh, fr, opening_leg) rn from legs),
          s as (select *, sum((abs(start) <= {EPS})::int) over (partition by user, coin order by rn rows unbounded preceding) ep_id from o),
          seg as (select s.*, lead(time) over (partition by user, coin order by rn) t_next,
                        abs(start - lag(pos_after) over (partition by user, coin order by rn)) > 1e-6 chain_break from s),
          fz as (select seg.*, f1.F F1, f2.F F2 from seg
                 asof left join fund f1 on f1.coin = seg.coin and f1.t_ms <= seg.time
                 asof left join fund f2 on f2.coin = seg.coin and f2.t_ms <= coalesce(seg.t_next, {end_ms}))
          select user, coin, ep_id, min(time) t0, max(time) t1, count(*) n_legs,
                 -sum(q * px) cashflow, arg_max(pos_after, rn) q_end, max(abs(pos_after) * px) stake,
                 sum(fees) fees, sum(pos_after * (coalesce(F2, 0) - coalesce(F1, 0))) funding,
                 sum(closed_pnl) closed_pnl, max(liq::int) liq, avg((not crossed)::int) maker_share, sum(coalesce(chain_break, false)::int) chain_breaks
          from fz group by user, coin, ep_id""")
        c.sql(f"""copy (select e.*, (ep_id = 0) pre_window, abs(q_end) > {EPS} open_at_end,
                  e.cashflow + case when abs(q_end) > {EPS} then q_end * m.mark else 0 end - fees - funding pnl
                  from ep e left join mark m using (coin)) to '{out}.tmp' (format parquet)""")
        os.replace(out + ".tmp", out)
        # wallet activity for the retail rules; each wallet lies wholly inside one partition
        c.sql(f"""copy (select user, max(n) max_hour, sum(n) fills, sum(mk) maker_fills from (
                    select user, time // 3600000 h, count(*) n, sum((not crossed)::int) mk from {F}
                    where hash(user) % {NP} = {p} group by all) group by user) to '{OUT}/act_{p:02d}.parquet' (format parquet)""")
        print(f"partition {p + 1}/{NP} done", flush=True)
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
