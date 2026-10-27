"""Study 2 sensitivity to token transfers (registered sensitivity, Erratum S3-E2 item 5).

The primary analysis values residual tokens as if still held and liquidates them at the final state. This script reduces
each retail position's liquidation value by the share of its residual that left the wallet on net (transfers out minus
transfers in, same token, capped at the residual), using the pump.fun transfer events available (2026-08-14 to
2026-09-08). It also reports an upper bound that uses gross transfers out and ignores tokens that came back.

Reads DATA/hf_pumpfun_v2/*/*/other/transfer_*.parquet, DATA/_mints.parquet and DATA/s2_positions.parquet (from
s2_analyze.py). Writes AGG/s2_transfer_sensitivity.json with pooled RTP and the share of wallets ahead, primary and adjusted.

Run: python s2_transfer_sensitivity.py
"""
import glob
import json
import os

import duckdb

from config import AGG, DATA, TMP

D = DATA
R = f"{D}/hf_pumpfun_v2"
c = duckdb.connect()
c.sql(f"set threads=1; set memory_limit='4GB'; set temp_directory='{TMP}/s2_transfer_sensitivity_{os.getpid()}'; set TimeZone='UTC'; set preserve_insertion_order=false")
tf = sorted(glob.glob(f"{R}/*/*/other/transfer_*.parquet"))
fl = "[" + ", ".join(f"'{f}'" for f in tf) + "]"
c.sql(f"""create table tr as select distinct on (payload_hash) from_wallet, to_wallet, token_address mint, amount::double amt
          from read_parquet({fl}) where token_address in (select mint from read_parquet('{D}/_mints.parquet'))""")
c.sql("""create table gross as select from_wallet w, mint, sum(amt) gross_out from tr group by all""")
c.sql("""create table net as select w, mint, sum(o) - sum(i) net_out from (
           select from_wallet w, mint, amt o, 0 i from tr union all select to_wallet, mint, 0, amt from tr) group by all""")
base = ("not is_creator and not is_launch_block and not is_distributed and not is_protocol and not is_atomic_bot "
        "and max_hour <= 200 and not oversold and cash_in > 0 and isfinite(cash_out)")
c.sql(f"""create table p as select p.w, p.cash_in, p.cash_out, coalesce(p.liq_net, 0) liq, p.resid,
          least(greatest(coalesce(n.net_out, 0), 0) / nullif(p.resid, 0), 1) frac,
          least(coalesce(g.gross_out, 0) / nullif(p.resid, 0), 1) frac_gross
          from read_parquet('{D}/s2_positions.parquet') p left join net n using (w, mint) left join gross g using (w, mint) where {base}""")
out = {"transfer_files": len(tf), "transfer_rows": c.sql("select count(*) from tr").fetchone()[0]}
out["positions_affected"] = c.sql("select count(*) from p where frac > 0 and liq > 0").fetchone()[0]
out["liquidation_removed_sol"] = c.sql("select sum(liq * coalesce(frac, 0)) / 1e9 from p").fetchone()[0]
r = c.sql("""select sum(cash_out) / sum(cash_in), sum(cash_out - liq * coalesce(frac, 0)) / sum(cash_in) from p""").fetchone()
out["pooled_rtp_primary"], out["pooled_rtp_adjusted"] = r
r = c.sql("""select avg((co > ci)::int), avg((coa > ci)::int) from (
               select w, sum(cash_in) ci, sum(cash_out) co, sum(cash_out - liq * coalesce(frac, 0)) coa from p group by w)""").fetchone()
out["share_wallets_ahead_primary"], out["share_wallets_ahead_adjusted"] = r
out["upper_bound_gross"] = dict(zip(["positions_affected", "liquidation_removed_sol", "pooled_rtp", "share_wallets_ahead"],
    list(c.sql("select count(*) filter (where frac_gross > 0 and liq > 0), sum(liq * coalesce(frac_gross, 0)) / 1e9, "
               "sum(cash_out - liq * coalesce(frac_gross, 0)) / sum(cash_in) from p").fetchone()) +
    [c.sql("select avg((co > ci)::int) from (select w, sum(cash_in) ci, sum(cash_out - liq * coalesce(frac_gross, 0)) co from p group by w)").fetchone()[0]]))
json.dump(out, open(f"{AGG}/s2_transfer_sensitivity.json", "w"), indent=1, default=float)
print(json.dumps(out, indent=1, default=float))
