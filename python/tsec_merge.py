"""Merge the per-file curve-state parts (tsec_parts.py) into DATA/tsec.parquet: for every (mint, second), the state before
the first trade and after the last trade. final_inputs.py needs this table, and price_curve.py reuses it when it exists
(the SQL is the same as the fallback in price_curve.py).
Run: python3 tsec_merge.py"""
import duckdb

from config import DATA, TMP

D = DATA
PART = f"{D}/tsec_parts"
c = duckdb.connect()
c.sql(f"set threads=1; set memory_limit='1500MB'; set temp_directory='{TMP}'; set TimeZone='UTC'; set preserve_insertion_order=false")
c.sql(f"""
create table tsec as
select mint, sec,
  arg_min(pre_vs, kn) pre_vs, arg_min(pre_vt, kn) pre_vt, arg_min(pre_rt, kn) pre_rt, arg_min(pre_cb, kn) pre_cb,
  arg_max(post_vs, kx) post_vs, arg_max(post_vt, kx) post_vt, arg_max(post_rt, kx) post_rt, arg_max(post_cb, kx) post_cb
from read_parquet('{PART}/*.parquet') group by all
""")
c.sql(f"copy tsec to '{D}/tsec.parquet' (format parquet)")
print("tsec rows:", c.sql("select count(*) from tsec").fetchone()[0])
