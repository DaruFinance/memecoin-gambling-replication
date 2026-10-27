"""Study 2 funding check (Amendment S2-1): SOL transfers from each token's creator to wallets that traded the token.

Draws a random sample of 20,000 census tokens (seed 20260924) from the pump.fun create events under DATA/hf_pumpfun_v2 and
DATA/_mints.parquet, uploads it to Dune and finds system-program SOL transfers from the creator to any wallet within
72 hours either side of the token's creation, kept only when the recipient also traded the token on the curve.
Writes DATA/s2_funding_sample_mints.csv (the sample), DATA/s2_funding_links_raw.csv (Dune output) and
DATA/s2_funding_links.csv (mint, w, lamports), which s2_analyze.py reads.

Run: DUNE_API_KEY=... python s2_funding_sample.py
"""
import os, sys
import duckdb, numpy as np, pandas as pd
from dune import run
from dune_pool_state_full import drop, upload_retry
from config import DATA
D = DATA; R = f"{D}/hf_pumpfun_v2"
c = duckdb.connect(); c.sql("set TimeZone='UTC'")
tok = c.sql(f"""with cr as (select distinct on (payload_hash) mint, creator, block_time from read_parquet('{R}/*/*/create/*.parquet'))
  select mint, any_value(creator) creator, min(block_time) t_create, epoch(min(block_time))::bigint t_s from cr
  where mint in (select mint from read_parquet('{D}/_mints.parquet')) group by mint order by mint""").df()
s = tok.sample(20000, random_state=20260924).reset_index(drop=True)
s["mid"] = np.arange(len(s))  # t_s is epoch seconds from DuckDB, so the upload does not depend on pandas timestamp units
s[["mid", "mint"]].to_csv(f"{D}/s2_funding_sample_mints.csv", index=False)
SQL = """
WITH u AS (SELECT mid, mint, creator, from_unixtime(t_s) AS tc FROM {tbl}),
f AS (
  SELECT u.mid, u.mint, t.account_to AS w, sum(t.lamports) AS lamports
  FROM system_program_solana.system_program_call_Transfer t
  JOIN u ON t.account_from = u.creator
   AND t.call_block_time BETWEEN u.tc - INTERVAL '72' HOUR AND u.tc + INTERVAL '72' HOUR
  WHERE t.call_block_time BETWEEN TIMESTAMP '2026-08-05' AND TIMESTAMP '2026-09-14' AND t.account_to <> u.creator
  GROUP BY 1, 2, 3),
tr AS (SELECT DISTINCT mint, "user" AS w FROM pumpdotfun_solana.pump_evt_tradeevent
       WHERE evt_block_date BETWEEN DATE '2026-08-08' AND DATE '2026-09-18' AND mint IN (SELECT mint FROM u))
SELECT f.mid, f.w, f.lamports FROM f JOIN tr ON tr.mint = f.mint AND tr.w = f.w
"""
out = f"{D}/s2_funding_links_raw.csv"
info = upload_retry(s[["mid", "mint", "creator", "t_s"]], "ms_fund_sample")
try:
    st, cost, nb, x = run(SQL.format(tbl=info["full_name"]), "medium", out, max_wait=7200)
finally:
    drop(info["full_name"])
print("funding sample:", st, cost, nb, x.get("error"))
if st == "QUERY_STATE_COMPLETED":
    l = pd.read_csv(out).merge(s[["mid", "mint"]], on="mid")
    l[["mint", "w", "lamports"]].to_csv(f"{D}/s2_funding_links.csv", index=False)
    print("links:", len(l), "tokens with any link:", l.mint.nunique())
