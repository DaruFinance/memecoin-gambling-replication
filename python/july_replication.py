"""Study 1 replication slice (Study 1 registration, Arms, Replication).

pump.fun launches from 2026-07-04 to 2026-07-14, from the Hugging Face corpus Slinky21/Pumpfun_Memecoin_Corpus, with the
launch-tier design and the hold capped at 24 h (the corpus tracks each token for 48 h).
Bets: K = 5 per token, entry = t_create + U[60 s, 24 h), hold = min(exp(U[ln 60 s, ln 7 d]), 24 h). Stake $100 at the
Binance SOLUSDT 1-minute open.
Curve phase: the state at t is the post-trade reserves of the last trade at or before t with non-null reserves (corpus
known issue 3.2), else the creation reserves. Fee f = 125 bp (95 protocol + 30 creator; the corpus has no per-trade
creator fee).
  buy: sol_in = S / (1 + f); q = vT - vS * vT / (vS + sol_in), capped at the real tokens left on the curve.
  exit on the curve includes the bettor's own buy (Study 1 registration, Erratum 2): vS' = vS + sol_in, vT' = vT - q;
  net = (vS' - vS' * vT' / (vT' + q)) * (1 - f).
Graduation phase (lower fidelity, as registered): price = market_cap_usd / 1e9 from the last DEX snapshot at or before t
(postgard_snapshots), converted at SOLUSDT; PumpSwap fee 125 bp per side. A snapshot more than 1 h old excludes the bet.
Bets whose exit falls after the token's tracking end are excluded. X = cash out / stake.
Statistics and ruling use the functions of stats_and_ruling.py; the table reference for the BTC control is read from
Study 1's results.json.
Reads data/hf_pumpfun_v1 (tokens, trades, postgard_snapshots) and the July SOLUSDT kline zips in data/binance. Writes
s1_replication_july.json to results/aggregates.
Run: python3 july_replication.py (after stats_and_ruling.py)
"""
import glob
import json
import os
import zipfile

import duckdb
import numpy as np
import pandas as pd

from casino_games import SWEET_BONANZA, plinko_16_high, roulette
from config import AGG, DATA, TMP
from stats_and_ruling import day_bootstrap, distance, stats, surv_exact

H = f"{DATA}/hf_pumpfun_v1"
D = DATA
RNG = np.random.default_rng(20260923)
F = 0.0125
REAL_OFFSET = 279_900_000 * 1e6        # virtual-minus-real tokens on the pump.fun curve (raw units)
c = duckdb.connect()
c.sql(f"set threads=4; set memory_limit='5GB'; set temp_directory='{TMP}/july_replication_{os.getpid()}'; set TimeZone='UTC'")

tok = c.sql("""select mint, epoch(detected_at) t0, epoch(tracking_expires_at) t_end, epoch(graduated_at) t_grad,
                      v_sol_bonding_curve vs0, v_tokens_bonding_curve vt0, is_mayhem_mode mayhem
               from read_parquet('%s/tokens.parquet')
               where detected_at >= timestamptz '2026-07-04 00:00:00+00' and detected_at < timestamptz '2026-07-15 00:00:00+00'""" % H).df()
n = len(tok)
K = 5
b = pd.DataFrame({"mint": np.repeat(tok.mint.values, K), "t0": np.repeat(tok.t0.values, K)})
b = b.merge(tok, on=["mint", "t0"])
b["bet_id"] = np.arange(len(b))
b["te"] = b.t0 + RNG.uniform(60, 86400, len(b))
b["hold"] = np.minimum(np.exp(RNG.uniform(np.log(60), np.log(7 * 86400), len(b))), 86400)
b["tx"] = b.te + b.hold
b["launch_day"] = pd.to_datetime(b.t0, unit="s", utc=True).dt.strftime("%Y-%m-%d")
excl_track = b.tx > b.t_end
print(f"tokens {n}, bets {len(b)}, exit after tracking end {int(excl_track.sum())}", flush=True)

# SOLUSDT minute opens (timestamps in microseconds)
rows = []
for z in sorted(glob.glob(f"{D}/binance/SOLUSDT-1m-2026-07*.zip")):
    with zipfile.ZipFile(z) as zf:
        rows.append(pd.read_csv(zf.open(zf.namelist()[0]), header=None, usecols=[0, 1], names=["ot", "open"]))
sol = pd.concat(rows)
sol["m"] = (sol.ot // 1_000_000 // 60).astype(np.int64)
sol = sol.set_index("m").open
solpx = lambda t: sol.reindex((np.asarray(t) // 60).astype(np.int64)).values

c.register("b", b[["bet_id", "mint", "te", "tx"]])
c.sql("create table pts as select bet_id, mint, 'e' as rl, te t from b union all select bet_id, mint, 'x', tx from b")
c.sql(f"""create table tr as select mint, epoch(event_time) t, id, v_sol_bonding_curve vs, v_tokens_bonding_curve vt
          from read_parquet('{H}/trades/*.parquet') where v_sol_bonding_curve is not null and v_tokens_bonding_curve is not null
            and mint in (select distinct mint from b)""")
st = c.sql("""select p.bet_id, p.rl, t.vs, t.vt from pts p asof left join tr t on t.mint = p.mint and t.t <= p.t""").df()
c.sql(f"""create table ps as select mint, epoch(snapshot_time) t, market_cap_usd from read_parquet('{H}/postgard_snapshots.parquet')
          where market_cap_usd is not null and mint in (select distinct mint from b)""")
pp = c.sql("""select p.bet_id, p.rl, s.market_cap_usd mc, s.t ts from pts p asof left join ps s on s.mint = p.mint and s.t <= p.t""").df()

def piv(df, cols):
    w = df.pivot(index="bet_id", columns="rl", values=cols)
    w.columns = [f"{a}_{r}" for a, r in w.columns]
    return w

b = b.join(piv(st, ["vs", "vt"]), on="bet_id").join(piv(pp, ["mc", "ts"]), on="bet_id")
for r in ("e", "x"):   # no trade yet: creation reserves
    b[f"vs_{r}"] = b[f"vs_{r}"].fillna(b.vs0)
    b[f"vt_{r}"] = b[f"vt_{r}"].fillna(b.vt0)
b["sol_e"], b["sol_x"] = solpx(b.te), solpx(b.tx)
S = 100.0 / b.sol_e * 1e9                                   # stake in lamports
pool_e = b.t_grad.notna() & (b.te >= b.t_grad)
pool_x = b.t_grad.notna() & (b.tx >= b.t_grad)
sol_in = S / (1 + F)
q_curve = np.minimum(b.vt_e - b.vs_e * b.vt_e / (b.vs_e + sol_in), np.maximum(b.vt_e - REAL_OFFSET, 0))
px_e_pool = b.mc_e / 1e9 / b.sol_e * 1e9                     # lamports per whole token
q_pool = S * (1 - F) / px_e_pool * 1e6                       # raw units
q = np.where(pool_e, q_pool, q_curve)
vs2, vt2 = b.vs_x + sol_in, b.vt_x - q
net_curve = (vs2 - vs2 * vt2 / (vt2 + q)) * (1 - F)
px_x_pool = b.mc_x / 1e9 / b.sol_x * 1e9
net_pool = q / 1e6 * px_x_pool * (1 - F)
b["X"] = np.where(pool_x, net_pool, net_curve) / S
stale = (pool_e & ((b.te - b.ts_e) > 3600)) | (pool_x & ((b.tx - b.ts_x) > 3600)) | (pool_x & b.mc_x.isna())
b["excluded"] = excl_track | stale | b.sol_e.isna() | b.sol_x.isna() | ~np.isfinite(b.X)
prim = b[~b.excluded]
res = {"counts": {"tokens": n, "bets": len(b), "included": len(prim), "excluded_tracking": int(excl_track.sum()),
                  "excluded_pool_price_stale_or_missing": int(stale.sum()), "pool_exit_bets": int((pool_x & ~b.excluded).sum())},
       "primary": stats(prim.X.values), "primary_ci_day": day_bootstrap(prim.X.values, prim.launch_day.values, B=2000)}
for flag, name in ((True, "mayhem"), (False, "normal")):
    res[f"split::{name}"] = stats(prim[prim.mayhem == flag].X.values)
refs_slot = {"Plinko 16 high": surv_exact(plinko_16_high()), "Sweet Bonanza": {int(k): v for k, v in SWEET_BONANZA["survival"].items()}}
s1 = json.load(open(f"{AGG}/results.json"))
refs_table = {"BTC control (Study 1)": {int(k): v for k, v in s1["control::BTC"]["surv"].items()},
              "European roulette even money": surv_exact(roulette(37)["even-money"])}
p = res["primary"]
d_slot = {k: distance(p["surv"], v) for k, v in refs_slot.items()}
d_table = {k: distance(p["surv"], v) for k, v in refs_table.items()}
cond = {"1 median < 1": p["median"] < 1, "2 win rate < 48.65%": p["win_rate"] < 0.4865, "3 jackpot share >= 9%": p["jackpot_share"] >= 0.09,
        "4 slot distance < table distance": min(d_slot.values()) < min(d_table.values())}
res["ruling"] = {"distances_slot": d_slot, "distances_table": d_table, "conditions": cond, "passes": all(cond.values())}
json.dump(res, open(f"{AGG}/s1_replication_july.json", "w"), indent=1, default=float)
print(json.dumps({"counts": res["counts"], "primary": {k: v for k, v in p.items() if k != "surv"}, "ruling": res["ruling"]}, indent=1, default=float))
