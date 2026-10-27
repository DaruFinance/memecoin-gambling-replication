"""Study 2 positions: per (token, wallet) cash flows on the pump.fun bonding curve and in PumpSwap pools.

Reads the pump.fun trade, swap and transaction-metadata parquet files under DATA/hf_pumpfun_v2, the census token list
(DATA/_mints.parquet, built from DATA/bets_v2.parquet if missing) and the slot clock DATA/s2_slot_time.parquet
(from s2_slot_time.py). Writes per-group curve parts to DATA/s2_parts_v4, per-window swap parts to DATA/s2_swap_v4 and
the hourly median transaction cost table DATA/s2_txcost_hourly.parquet. s2_analyze.py merges the parts.

Deduplication follows Study 2 registration, Errata S2-E1 and S2-E3. Curve files that overlap in block_time are grouped and
deduplicated together on payload_hash. Swap rows are deduplicated on (slot, tx_index, trader, input_mint, output_mint,
input_amount, output_amount) within windows of 100,000 slots, reading every file that holds slots in the window, so the
parts are disjoint and their sums compose. A swap row is timed by its slot's clock. Trades after the end of the data
(2026-09-18 15:17 UTC) are dropped. PumpSwap legs carry the median transaction cost of their own hour (Amendment S2-2,
Erratum S2-E2).

Run: python s2_positions.py  (set S2_WORKERS for more than one worker; each uses one thread and 2.5 GB).
"""
import glob
import os
from multiprocessing import Pool

import duckdb

from config import DATA, TMP

D = DATA
R = f"{D}/hf_pumpfun_v2"
OUT = f"{D}/s2_parts_v4"
OUT_SW = f"{D}/s2_swap_v4"
CLOCK = f"{D}/s2_slot_time.parquet"
END = "2026-09-18 15:17:00+00"
WIN = 100_000  # slots per swap window, about 11 hours
HOURLY = f"{D}/s2_txcost_hourly.parquet"  # hr (UTC), lam = 5000 + median(priority fee + tip) in lamports, n = transactions
WSOL = "So11111111111111111111111111111111111111112"


def con():
    tmp = f"{TMP}/s2pos_{os.getpid()}"
    os.makedirs(tmp, exist_ok=True)
    db = f"{tmp}/work.duckdb"
    for f in (db, db + ".wal"):
        if os.path.exists(f):
            os.remove(f)
    c = duckdb.connect(db)
    c.sql(f"set threads=1; set memory_limit='2500MB'; set temp_directory='{tmp}'; set TimeZone='UTC'; set preserve_insertion_order=false")
    return c


def groups(files):
    """Group files whose [min, max] block_time intervals overlap."""
    c = con()
    iv = []
    for f in files:
        lo, hi = c.sql(f"select min(block_time), max(block_time) from read_parquet('{f}')").fetchone()
        if lo is not None:
            iv.append((lo, hi, f))
    c.close()
    iv.sort()
    out, cur, cur_hi = [], [], None
    for lo, hi, f in iv:
        if cur and lo <= cur_hi:
            cur.append(f); cur_hi = max(cur_hi, hi)
        else:
            if cur:
                out.append(cur)
            cur, cur_hi = [f], hi
    if cur:
        out.append(cur)
    return out


def flist(fs):
    return "[" + ", ".join(f"'{f}'" for f in fs) + "]"


def curve_group(i, fs):
    out = f"{OUT}/curve_g{i:04d}.parquet"
    if os.path.exists(out):
        return out
    stamps = [os.path.basename(f).split("_", 1)[1] for f in fs]
    txf = [g for s in stamps for g in glob.glob(f"{R}/txmeta_lean/txmeta_{s}")]
    txs = f"read_parquet({flist(txf)})" if txf else "(select NULL::varchar signature, NULL::varchar first_signer, 0 priority_fee, 0 tip_amount where false)"
    c = con()
    c.sql(f"""create table allt as select distinct on (payload_hash) payload_hash, mint, trade_user w, signature, slot, block_time bt,
                 coalesce(is_buy, false) b, sol_amount::double s, token_amount::double k,
                 coalesce(fee, 0)::double fee, coalesce(creator_fee, 0)::double cfee
              from read_parquet({flist(fs)}) where block_time <= TIMESTAMPTZ '{END}'""")
    c.sql(f"""copy (
      with tr as (select * from allt where mint in (select mint from read_parquet('{D}/_mints.parquet'))),
      tx as (select signature, any_value(first_signer) fs, any_value(5000 + coalesce(priority_fee,0) + coalesce(tip_amount,0))::double as tcost
             from {txs} group by signature),
      j as (select tr.*, case when tx.fs = tr.w then tx.tcost / count(*) over (partition by tr.signature, tr.w) else 0 end txc
            from tr left join tx using (signature))
      select mint, w,
        sum(case when b then s + fee + cfee else 0 end) paid, sum(case when b then k else 0 end) tok_in, sum(b::int) n_buy,
        sum(case when not b then s - fee - cfee else 0 end) recv, sum(case when not b then k else 0 end) tok_out, sum((not b)::int) n_sell,
        sum(case when b then txc else 0 end) cost_buy, sum(case when not b then txc else 0 end) cost_sell,
        min(case when b then slot end) first_buy_slot, min(bt) first_t, max(bt) last_t
      from j group by mint, w) to '{out}.tmp' (format parquet)""")
    c.sql(f"""copy (select w, date_trunc('hour', bt) hr, count(*) n from allt group by all)
              to '{OUT}/act_curve_g{i:04d}.parquet' (format parquet)""")
    c.close()
    os.replace(out + ".tmp", out)
    return out


def swap_group(i, fs):
    out = f"{OUT_SW}/swap_g{i:05d}.parquet"
    if os.path.exists(out):
        return out
    a, b = i * WIN, (i + 1) * WIN
    c = con()
    fall = c.sql(f"select median(lam) from read_parquet('{HOURLY}')").fetchone()[0]  # fallback for hours without a record, as in Study 1
    c.sql(f"""create table s0 as select distinct on (slot, tx_index, trader, input_mint, output_mint, input_amount, output_amount)
                 slot, trader w, input_mint, output_mint, input_amount::double ia, output_amount::double oa, is_atomic
              from read_parquet({flist(fs)}) where dex = 'pumpswap' and slot >= {a} and slot < {b}""")
    c.sql(f"""create table s as select s0.* exclude (slot), k.clock bt from s0 join read_parquet('{CLOCK}') k using (slot)
              where k.clock <= TIMESTAMPTZ '{END}'""")
    c.sql(f"""copy (
      with m0 as (select * from s where (output_mint = '{WSOL}' and input_mint in (select mint from read_parquet('{D}/_mints.parquet')))
                              or (input_mint = '{WSOL}' and output_mint in (select mint from read_parquet('{D}/_mints.parquet')))),
      m as (select m0.*, coalesce(h.lam, {fall})::double txc from m0 left join read_parquet('{HOURLY}') h on h.hr = date_trunc('hour', m0.bt))
      select case when input_mint = '{WSOL}' then output_mint else input_mint end mint, w,
        sum(case when input_mint = '{WSOL}' then ia else 0 end) paid, sum(case when input_mint = '{WSOL}' then oa else 0 end) tok_in,
        sum((input_mint = '{WSOL}')::int) n_buy,
        sum(case when output_mint = '{WSOL}' then oa else 0 end) recv, sum(case when output_mint = '{WSOL}' then ia else 0 end) tok_out,
        sum((output_mint = '{WSOL}')::int) n_sell,
        sum(case when input_mint = '{WSOL}' then txc else 0 end) cost_buy, sum(case when output_mint = '{WSOL}' then txc else 0 end) cost_sell,
        min(bt) first_t, max(bt) last_t, max(coalesce(is_atomic, false)::int) any_atomic
      from m group by all) to '{out}.tmp' (format parquet)""")
    c.sql(f"""copy (select w, date_trunc('hour', bt) hr, count(*) n, max(coalesce(is_atomic,false)::int) atomic from s group by all)
              to '{OUT_SW}/act_swap_g{i:05d}.parquet' (format parquet)""")
    c.close()
    os.replace(out + ".tmp", out)
    return out


def job(x):
    kind, i, fs = x
    return curve_group(i, fs) if kind == "c" else swap_group(i, fs)


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    os.makedirs(OUT_SW, exist_ok=True)
    if not os.path.exists(HOURLY):
        c = con()
        c.sql(f"""copy (select date_trunc('hour', block_time) hr, 5000 + median(coalesce(priority_fee, 0) + coalesce(tip_amount, 0)) lam, count(*) n
                  from (select distinct on (signature) block_time, priority_fee, tip_amount from read_parquet('{R}/txmeta_lean/*.parquet'))
                  group by 1 order by 1) to '{HOURLY}.tmp' (format parquet)""")
        c.close()
        os.replace(HOURLY + ".tmp", HOURLY)
        print("hourly tx cost table written", flush=True)
    if not os.path.exists(f"{D}/_mints.parquet"):
        c = duckdb.connect()
        c.sql(f"copy (select distinct mint from read_parquet('{D}/bets_v2.parquet')) to '{D}/_mints.parquet' (format parquet)")
        c.close()
    cg = groups(sorted(glob.glob(f"{R}/*/*/trade/*.parquet")))
    c = con()
    rng = [(f, *c.sql(f"select min(slot), max(slot) from read_parquet('{f}')").fetchone()) for f in sorted(glob.glob(f"{R}/swap_lean/*.parquet"))]
    c.close()
    rng = [(f, lo, hi) for f, lo, hi in rng if lo is not None]
    sw = {}
    for f, lo, hi in rng:
        for i in range(lo // WIN, hi // WIN + 1):
            sw.setdefault(i, []).append(f)
    print(f"curve files -> {len(cg)} groups (multi-file: {[len(g) for g in cg if len(g) > 1]}); "
          f"swap files -> {len(sw)} slot windows of {WIN:,} (files per window: max {max(len(v) for v in sw.values())})", flush=True)
    jobs = [("c", i, g) for i, g in enumerate(cg)] + [("s", i, sw[i]) for i in sorted(sw)]
    with Pool(int(os.environ.get("S2_WORKERS", 1))) as p:
        for k, _ in enumerate(p.imap_unordered(job, jobs), 1):
            if k % 20 == 0:
                print(f"groups {k}/{len(jobs)}", flush=True)
    print("parts done", flush=True)
