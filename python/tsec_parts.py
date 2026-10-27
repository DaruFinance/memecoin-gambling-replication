"""Per-second bonding-curve state for every sampled token, one partial file per curve trade file.

For each (mint, second) the script keeps the reserves before the first trade of that second and after the last one, plus the
creator fee. Pre-trade reserves are rebuilt from each trade's post-trade reserves and amounts. price_curve.py merges the
partial files into data/tsec.parquet.
Reads data/bets_v2.parquet (to get the sampled mints) and the curve trade files in data/hf_pumpfun_v2. Writes
data/_mints.parquet and data/tsec_parts/. Runs in a 4-process pool; finished parts are skipped on restart.
Run: python3 tsec_parts.py
"""
import glob
import os
from multiprocessing import Pool

import duckdb

from config import DATA, TMP

R = f"{DATA}/hf_pumpfun_v2"
D = DATA
PART = f"{D}/tsec_parts"


def part(f):
    out = f"{PART}/{os.path.basename(f)}"
    if os.path.exists(out):
        return out
    cc = duckdb.connect()
    cc.sql(f"set threads=2; set memory_limit='3GB'; set temp_directory='{TMP}'; set TimeZone='UTC'")
    cc.sql(f"""copy (
      with tr as (
        select distinct on (payload_hash) mint, block_time bt, slot, tx_index, coalesce(is_buy, false) b,
               sol_amount::double s, token_amount::double k, virtual_sol_reserves::double vs,
               virtual_token_reserves::double vt, real_token_reserves::double rt,
               fee_basis_points fb, creator_fee_basis_points cb
        from read_parquet('{f}')
        where mint in (select mint from read_parquet('{D}/_mints.parquet'))
          and virtual_sol_reserves > 0 and virtual_token_reserves > 0)
      select mint, bt as sec, min(slot * 100000 + tx_index) kn, max(slot * 100000 + tx_index) kx,
        arg_min(case when b then vs - s else vs + s end, (slot, tx_index)) pre_vs,
        arg_min(case when b then vt + k else vt - k end, (slot, tx_index)) pre_vt,
        arg_min(case when b then rt + k else rt - k end, (slot, tx_index)) pre_rt,
        arg_min(case when fb > 0 then cb end, (slot, tx_index)) pre_cb,
        arg_max(vs, (slot, tx_index)) post_vs, arg_max(vt, (slot, tx_index)) post_vt, arg_max(rt, (slot, tx_index)) post_rt,
        arg_max(case when fb > 0 then cb end, (slot, tx_index)) post_cb
      from tr group by all) to '{out}.tmp' (format parquet)""")
    os.replace(out + ".tmp", out)
    return out


if __name__ == "__main__":
    os.makedirs(PART, exist_ok=True)
    c = duckdb.connect()
    c.sql(f"copy (select distinct mint from read_parquet('{D}/bets_v2.parquet')) to '{D}/_mints.parquet' (format parquet)")
    c.close()
    files = sorted(glob.glob(f"{R}/*/*/trade/*.parquet"))
    with Pool(4) as pool:
        for i, _ in enumerate(pool.imap_unordered(part, files), 1):
            if i % 10 == 0:
                print(f"tsec parts {i}/{len(files)}", flush=True)
    print("tsec parts done", flush=True)
