"""Pool-phase pricing gate as first registered (Study 1 registration, Pricing). It failed; Amendment 1 replaced it.

Dune pre-trade reserves plus the constant-product pool formula must reproduce the net outputs of the corpus swap stream
within 1 bp, on at least 1,000 swaps across at least 50 pools. The check uses swaps only, no bets: 60 pools drawn from the
lookup pools, up to 25 swaps each.
Reads data/pool_lookups.parquet and data/hf_pumpfun_v2/swap_lean. Writes data/gate_pool_events.csv (the matching Dune
events) and data/gate_pool_pricing_matches.parquet, and prints the error quantiles and the gate count. Requires DUNE_API_KEY.
Run: python3 gate_pool_pricing.py
"""
import duckdb
import numpy as np
import pandas as pd

from config import DATA
from dune import run

R = f"{DATA}/hf_pumpfun_v2"
c = duckdb.connect()
c.sql("set threads=4; set memory_limit='6GB'; set TimeZone='UTC'")
bets_pools = c.sql(f"select distinct pool from read_parquet('{DATA}/pool_lookups.parquet')").df()
rng = np.random.default_rng(7)
pools = list(rng.choice(bets_pools.pool.values, 60, replace=False))
c.register("pp", pd.DataFrame({"pool": pools}))
sw = c.sql(f"""select pool, slot, tx_index, block_time, input_mint, output_mint, input_amount, output_amount
  from read_parquet('{R}/swap_lean/*.parquet') where dex='pumpswap' and pool in (select pool from pp) and tx_index is not null and slot is not null""").df()
sw = sw.groupby("pool", group_keys=False).apply(lambda d: d.sample(min(len(d), 25), random_state=1))
print(f"HF swaps sampled: {len(sw)} across {sw.pool.nunique()} pools")
pairs = ",".join(f"({int(s)},{int(t)})" for s, t in sw[["slot", "tx_index"]].drop_duplicates().values)
days = ",".join(f"date '{d}'" for d in sorted(pd.to_datetime(sw.block_time, utc=True).dt.date.astype(str).unique()))
plist = ",".join(f"'{p}'" for p in pools)
cols = ("evt_block_slot slot, evt_tx_index txi, pool, pool_base_token_reserves rb, pool_quote_token_reserves rq, "
        "lp_fee_basis_points lpb, protocol_fee_basis_points pb, coin_creator_fee_basis_points cb, lp_fee, protocol_fee, coin_creator_fee")
sql = f"""
select 'buy' side, {cols}, base_amount_out base_amt, quote_amount_in quote_amt, user_quote_amount_in user_quote
from pumpdotfun_solana.pump_amm_evt_buyevent
where evt_block_date in ({days}) and pool in ({plist}) and (evt_block_slot, evt_tx_index) in (values {pairs})
union all
select 'sell', {cols}, base_amount_in, quote_amount_out, user_quote_amount_out
from pumpdotfun_solana.pump_amm_evt_sellevent
where evt_block_date in ({days}) and pool in ({plist}) and (evt_block_slot, evt_tx_index) in (values {pairs})
"""
out = f"{DATA}/gate_pool_events.csv"
state, cost, nbytes, s = run(sql, "medium", out)
print(state, "credits", cost, "bytes", nbytes, s.get("error"))
ev = pd.read_csv(out)
sw["side"] = np.where(sw.output_mint.str.endswith("pump") | ~sw.input_mint.str.endswith("pump"), "buy", "sell")
m = sw.merge(ev, left_on=["pool", "slot", "tx_index", "side"], right_on=["pool", "slot", "txi", "side"])
# keep one-to-one matches on amount: for a sell base_in == input_amount, for a buy base_out == output_amount
m = m[np.where(m.side == "sell", m.base_amt == m.input_amount, m.base_amt == m.output_amount)]
f = (m.lpb + m.pb + m.cb) / 1e4
rb, rq = m.rb.astype(float), m.rq.astype(float)
sell = m.side == "sell"
q = m.input_amount.astype(float)
gross = rq - rb * rq / (rb + q)
pred_sell_net = gross * (1 - f)
pred_buy = rb - rb * rq / (rq + m.quote_amt.astype(float))
err = np.where(sell, pred_sell_net / m.output_amount - 1, pred_buy / m.output_amount - 1)
m["err_bp"] = err * 1e4
for side in ("sell", "buy"):
    e = m.loc[m.side == side, "err_bp"].abs()
    print(f"{side}: n={len(e)}, pools={m.loc[m.side == side, 'pool'].nunique()}, |err| bp median={e.median():.4f} "
          f"p95={e.quantile(.95):.4f} p99={e.quantile(.99):.4f} max={e.max():.4f}, within 1bp={np.mean(e <= 1):.2%}")
b = m[~sell]
print("buy: user_quote / quote_amt - 1 (bp) median", float(((b.user_quote / b.quote_amt - 1) * 1e4).median()),
      "| total fee bps median", float(((b.lpb + b.pb + b.cb)).median()))
n_ok = int((m.err_bp.abs() <= 1).sum())
print(f"GATE: matched swaps {len(m)} across {m.pool.nunique()} pools; within 1 bp: {n_ok}")
m.to_parquet(f"{DATA}/gate_pool_pricing_matches.parquet", index=False)
