"""Draw the Study 1 launch-tier bet sample as registered (Study 1 registration, Launch-tier sample). Reads no prices.

The census is every token created between 2026-08-08 21:16 and 2026-09-10 15:17 UTC. Each token gets K = 5 bets with seed
20260923: entry uniform in [60 s, 24 h) after creation, hold log-uniform between 60 s and 7 days, plus fixed horizons of
5 min, 1 h, 24 h and 7 days after entry.
Reads the create and graduate events in data/hf_pumpfun_v2. Writes data/bets_v2.parquet (one row per bet), and the
pool-phase time points that need Dune reserves: data/pool_lookups_by_bet.parquet (bet, role, lid) and
data/pool_lookups.parquet (one row per unique pool and second).
Run: python3 sample_bets.py
"""
import duckdb
import numpy as np
import pandas as pd

from config import DATA, TMP

R = f"{DATA}/hf_pumpfun_v2"
OUT = DATA
SEED, K = 20260923, 5
T_START = pd.Timestamp("2026-08-08 21:16", tz="UTC")
T_LAST_CREATE = pd.Timestamp("2026-09-10 15:17", tz="UTC")
HORIZONS = {"h5m": 300, "h1h": 3600, "h24h": 86400, "h7d": 7 * 86400}

c = duckdb.connect()
c.sql(f"set threads=4; set memory_limit='8GB'; set temp_directory='{TMP}'; set TimeZone='UTC'")
tok = c.sql(f"""
  with cr as (select distinct on (payload_hash) mint, block_time, slot, tx_index, is_mayhem_mode
              from read_parquet('{R}/*/*/create/*.parquet')),
  first_cr as (select * from cr qualify row_number() over (partition by mint order by slot, tx_index) = 1),
  gr as (select distinct on (payload_hash) mint, block_time, slot, tx_index, pool from read_parquet('{R}/*/*/graduate/*.parquet')),
  first_gr as (select * from gr qualify row_number() over (partition by mint order by slot, tx_index) = 1)
  select c.mint, c.block_time t_create, c.is_mayhem_mode is_mayhem, g.block_time t_grad, g.pool
  from first_cr c left join first_gr g using (mint)
  where c.block_time >= TIMESTAMP '{T_START:%Y-%m-%d %H:%M:%S}+00' and c.block_time <= TIMESTAMP '{T_LAST_CREATE:%Y-%m-%d %H:%M:%S}+00'
  order by c.slot, c.tx_index
""").df()
print(f"census: {len(tok):,} tokens, {tok.t_grad.notna().sum():,} graduated, {tok.is_mayhem.mean():.1%} mayhem")

rng = np.random.default_rng(SEED)
n = len(tok) * K
idx = np.repeat(np.arange(len(tok)), K)
entry_off = rng.uniform(60, 86400, n)
hold = np.exp(rng.uniform(np.log(60), np.log(7 * 86400), n))
bets = pd.DataFrame({
    "bet_id": np.arange(n, dtype=np.int64),
    "mint": tok.mint.values[idx],
    "is_mayhem": tok.is_mayhem.values[idx],
    "t_create": tok.t_create.values[idx],
    "t_grad": tok.t_grad.values[idx],
    "pool": tok["pool"].values[idx],
})
bets["t_entry"] = bets.t_create + pd.to_timedelta(entry_off, unit="s")
bets["t_exit"] = bets.t_entry + pd.to_timedelta(hold, unit="s")
bets["hold_s"] = hold
for k, s in HORIZONS.items():
    bets[f"t_{k}"] = bets.t_entry + pd.to_timedelta(s, unit="s")
bets.to_parquet(f"{OUT}/bets_v2.parquet", index=False)

tcols = ["t_entry", "t_exit"] + [f"t_{k}" for k in HORIZONS]
g = bets[bets.t_grad.notna()]
parts = []
for col in tcols:
    m = g[col] >= g.t_grad
    parts.append(pd.DataFrame({"bet_id": g.bet_id[m].values, "role": col, "pool": g["pool"][m].values, "t": g[col][m].values}))
look = pd.concat(parts, ignore_index=True)
look["t_s"] = (pd.to_datetime(look.t, utc=True).astype("int64") // 10**9).astype("int64")
look = look.sort_values(["pool", "t_s"]).reset_index(drop=True)
uniq = look[["pool", "t_s"]].drop_duplicates().reset_index(drop=True)
uniq["lid"] = np.arange(len(uniq), dtype=np.int64)
look = look.merge(uniq, on=["pool", "t_s"])
look.to_parquet(f"{OUT}/pool_lookups_by_bet.parquet", index=False)
uniq.to_parquet(f"{OUT}/pool_lookups.parquet", index=False)
print(f"bets: {n:,}; bets on graduated tokens: {len(g):,}")
print("pool-phase lookups by role:", look.role.value_counts().to_dict())
print(f"unique (pool, second) lookups: {len(uniq):,} across {uniq.pool.nunique():,} pools")
