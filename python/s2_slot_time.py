"""Slot clock used to time PumpSwap swaps (Study 2 registration, Erratum S2-E3; Study 3 registration, Erratum S3-E4).

Curve trade times are on-chain block times in whole seconds. PumpSwap swap rows carry an arrival stamp instead: a few seconds
after the block in most rows, and up to hours after it in rows the upstream feed delivered again. A slot has one block time,
so every swap row is timed by its slot:
  bt    = the earliest time recorded for the slot in any swap file or curve trade file;
  clock = the earliest bt over this slot and every later slot (block time cannot fall as the slot number rises).
Where a slot also holds a curve trade, clock equals the curve stream's block time to within the curve's whole-second
rounding.
Reads data/hf_pumpfun_v2 (swap_lean/ and the curve trade files). Writes data/s2_slot_time.parquet (slot, bt, clock,
in_curve) and prints the checks. Uses one thread and 1.5 GB of memory.
Run: python3 s2_slot_time.py
"""
import os

import duckdb

from config import DATA, TMP

D = DATA
R = f"{D}/hf_pumpfun_v2"
OUT = f"{D}/s2_slot_time.parquet"
c = duckdb.connect()
tmp = f"{TMP}/s2_slot_time_{os.getpid()}"
os.makedirs(tmp, exist_ok=True)
c.sql(f"set threads=1; set memory_limit='1500MB'; set temp_directory='{tmp}'; set TimeZone='UTC'; set preserve_insertion_order=false")
c.sql(f"""create table st as select slot, min(bt) bt, bool_or(src = 'c') in_curve from (
            select slot, block_time bt, 's' src from read_parquet('{R}/swap_lean/*.parquet')
            union all select slot, block_time bt, 'c' src from read_parquet('{R}/*/*/trade/*.parquet')) group by slot""")
c.sql("""create table ck as select slot, bt, in_curve,
           min(bt) over (order by slot desc rows between unbounded preceding and current row) clock from st""")
print("slots, with curve trades, slot range, time range:",
      c.sql("select count(*), sum(in_curve::int), min(slot), max(slot), min(clock), max(clock) from ck").fetchone(), flush=True)
print("slots whose earliest record runs back > 60 s against the previous slot:",
      c.sql("select count(*) from (select bt, lag(bt) over (order by slot) pb from st) where bt < pb - interval 60 second").fetchone()[0])
print("clock back-steps (must be 0):",
      c.sql("select count(*) from (select clock, lag(clock) over (order by slot) pc from ck) where clock < pc").fetchone()[0])
print("slots moved by the clock: > 1 s / > 60 s (swap-only, with curve trades):",
      c.sql("""select sum((bt - clock > interval 1 second and not in_curve)::int), sum((bt - clock > interval 60 second and not in_curve)::int),
                      sum((bt - clock > interval 1 second and in_curve)::int), sum((bt - clock > interval 60 second and in_curve)::int)
               from ck""").fetchone(), flush=True)
c.sql(f"copy (select slot, bt, clock, in_curve from ck order by slot) to '{OUT}.tmp' (format parquet)")
os.replace(OUT + ".tmp", OUT)
print("written", OUT, flush=True)
