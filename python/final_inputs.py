"""Inputs for the corrections in Study 1 registration, Erratum 3; Study 2 registration, Erratum S2-E3; and Study 3
registration, Erratum S3-E4. No outcome is read.

Writes to data/:
1. nonsol_mints.parquet: census tokens whose bonding curve is not quoted in SOL. A SOL curve starts at 30 SOL of virtual
   reserves. These start elsewhere (mostly 4,292 units of a 6-decimal dollar token), and their trade records carry no SOL
   amounts or reserves, so the registered SOL pricing cannot be applied to them.
2. grad_inferred.parquet: census tokens with no recorded graduation event whose curve completed (real token reserves reached
   0) and whose token then traded on a PumpSwap SOL pool. t_grad is the block time of the completing curve trade; pool is the
   token's PumpSwap SOL pool with the most swaps. Both rules are checked against tokens that do have a graduation event.
3. trade_outages.parquet: silences of more than 60 s in the curve trade stream (start, end, seconds), from the slot clock.
4. Pool-phase lookups for the Study 1 bets on the tokens in (2), in the format of sample_bets.py with lids offset by
   10,000,000 (pool_lookups_by_bet_extra.parquet, pool_lookups_extra.parquet); their peak windows
   (pool_peak_windows_extra.parquet); and end-of-data lookups for their pools, lids offset by 20,000,000, used by Study 2's
   liquidation (pool_lookups_final_extra.parquet).
Reads data/bets_v2.parquet, data/_mints.parquet, data/tsec.parquet, data/s2_slot_time.parquet and the corpus in
data/hf_pumpfun_v2. Uses one thread and 1.5 GB of memory.
Run: python3 final_inputs.py
"""
import os

import duckdb
import numpy as np
import pandas as pd

from config import DATA, TMP

D = DATA
R = f"{D}/hf_pumpfun_v2"
WSOL = "So11111111111111111111111111111111111111112"
END = pd.Timestamp("2026-09-18 15:17:00", tz="UTC")
TCOLS = ["t_entry", "t_exit", "t_h5m", "t_h1h", "t_h24h", "t_h7d"]
c = duckdb.connect()
tmp = f"{TMP}/erratum_inputs_{os.getpid()}"
os.makedirs(tmp, exist_ok=True)
c.sql(f"set threads=1; set memory_limit='1500MB'; set temp_directory='{tmp}'; set TimeZone='UTC'; set preserve_insertion_order=false")

# 1. non-SOL curves
c.sql(f"""create table cr as select mint, virtual_sol_reserves cvs from (
            select distinct on (payload_hash) mint, slot, tx_index, virtual_sol_reserves from read_parquet('{R}/*/*/create/*.parquet'))
          where mint in (select mint from read_parquet('{D}/_mints.parquet'))
          qualify row_number() over (partition by mint order by slot, tx_index) = 1""")
c.sql(f"copy (select mint, cvs from cr where cvs <> 30000000000) to '{D}/nonsol_mints.parquet' (format parquet)")
print("census tokens, SOL curves, non-SOL curves (of which 4,292e6):",
      c.sql("select count(*), sum((cvs = 30000000000)::int), sum((cvs <> 30000000000)::int), sum((cvs = 4292000000)::int) from cr").fetchone(),
      flush=True)

# 2. graduation without a recorded event
c.sql(f"create table tok as select distinct mint, t_grad::timestamptz t_grad, pool from read_parquet('{D}/bets_v2.parquet')")
c.sql(f"create table comp as select mint, min(sec) t_complete from read_parquet('{D}/tsec.parquet') where post_rt = 0 group by mint")
c.sql(f"""create table cand as select t.mint, comp.t_complete from tok t join comp using (mint)
          where t.t_grad is null and t.mint not in (select mint from read_parquet('{D}/nonsol_mints.parquet'))""")
# rule check sample: 3,000 tokens that have a graduation event, chosen by hash of the mint
c.sql("create table chk as select mint from tok where t_grad is not null order by hash(mint) limit 3000")
c.sql(f"""create table sw as select case when input_mint = '{WSOL}' then output_mint else input_mint end mint, pool, count(*) n, min(slot) first_slot
          from read_parquet('{R}/swap_lean/*.parquet')
          where dex = 'pumpswap' and ((input_mint = '{WSOL}' and output_mint in (select mint from cand union all select mint from chk))
                                   or (output_mint = '{WSOL}' and input_mint in (select mint from cand union all select mint from chk)))
          group by all""")
c.sql(f"""create table top as select s.mint, arg_max(s.pool, s.n) pool, sum(s.n) n, min(k.clock) first_swap
          from sw s left join read_parquet('{D}/s2_slot_time.parquet') k on k.slot = s.first_slot group by s.mint""")
v = c.sql("""select count(*) n, avg((top.pool = tok.pool)::int) same_pool,
                    quantile_cont(epoch(tok.t_grad) - epoch(comp.t_complete), [0.01, 0.5, 0.99]) grad_minus_complete_s,
                    quantile_cont(epoch(top.first_swap) - epoch(tok.t_grad), [0.01, 0.5, 0.99]) first_swap_minus_grad_s
             from chk join tok using (mint) join comp using (mint) join top using (mint)""").df().iloc[0].to_dict()
print("rule check on tokens with a graduation event:", v, flush=True)
c.sql(f"""copy (select cand.mint, cand.t_complete t_grad, top.pool, top.n n_pool_swaps
          from cand join top using (mint) where top.pool is not null) to '{D}/grad_inferred.parquet' (format parquet)""")
print("completed curve, no graduation event:", c.sql("select count(*) from cand").fetchone()[0],
      "| with a PumpSwap SOL pool:", c.sql(f"select count(*), sum(n_pool_swaps) from read_parquet('{D}/grad_inferred.parquet')").fetchone(),
      flush=True)

# 3. curve trade stream outages
c.sql(f"""copy (select prev_t "start", clock "end", epoch(clock) - epoch(prev_t) secs from (
            select clock, lag(clock) over (order by slot) prev_t from read_parquet('{D}/s2_slot_time.parquet') where in_curve)
          where clock - prev_t > interval 60 second order by 1) to '{D}/trade_outages.parquet' (format parquet)""")
print("curve-stream silences > 60 s, total h; > 5 min, total h:",
      c.sql(f"""select count(*), sum(secs)/3600, sum((secs > 300)::int), sum(case when secs > 300 then secs end)/3600
                from read_parquet('{D}/trade_outages.parquet')""").fetchone(), flush=True)
c.close()

# 4. extra pool lookups (Study 1) and end-of-data lookups (Study 2)
gi = pd.read_parquet(f"{D}/grad_inferred.parquet")
bets = pd.read_parquet(f"{D}/bets_v2.parquet", columns=["bet_id", "mint"] + TCOLS)
g = bets.merge(gi[["mint", "t_grad", "pool"]], on="mint")
g["t_grad"] = pd.to_datetime(g.t_grad, utc=True).dt.tz_localize(None)
parts = []
for col in TCOLS:
    m = g[col] >= g.t_grad
    parts.append(pd.DataFrame({"bet_id": g.bet_id[m].values, "role": col, "pool": g["pool"][m].values, "t": g[col][m].values}))
look = pd.concat(parts, ignore_index=True)
look["t_s"] = (pd.to_datetime(look.t, utc=True).astype("int64") // 10**9).astype("int64")
look = look.sort_values(["pool", "t_s"]).reset_index(drop=True)
uniq = look[["pool", "t_s"]].drop_duplicates().reset_index(drop=True)
uniq["lid"] = 10_000_000 + np.arange(len(uniq), dtype=np.int64)
look = look.merge(uniq, on=["pool", "t_s"])
look.to_parquet(f"{D}/pool_lookups_by_bet_extra.parquet", index=False)
uniq.to_parquet(f"{D}/pool_lookups_extra.parquet", index=False)
pk = g[g.t_exit > g.t_grad].copy()
pk["t0"] = pk[["t_entry", "t_grad"]].max(axis=1).astype("int64") // 10**9
pk["t1"] = pk.t_exit.astype("int64") // 10**9
pk[["bet_id", "pool", "t0", "t1"]].to_parquet(f"{D}/pool_peak_windows_extra.parquet", index=False)
fin = pd.DataFrame({"pool": sorted(gi.pool.unique())})
fin["t_s"] = int(END.timestamp())
fin["lid"] = 20_000_000 + np.arange(len(fin), dtype=np.int64)
fin.to_parquet(f"{D}/pool_lookups_final_extra.parquet", index=False)
print(f"bets on these tokens: {g.bet_id.nunique():,}; pool-phase lookups by role: {look.role.value_counts().to_dict()}; "
      f"unique: {len(uniq):,}; peak windows: {len(pk):,}; end-of-data pools: {len(fin):,}", flush=True)
