"""Daily coverage of the pump.fun trade corpus against Dune's decoded trade events (8 August to 18 September 2026): trades and
distinct mints per day in each source and their ratios. s2_analyze.py uses it for the coverage robustness row.
Reads the curve trade files in DATA/hf_pumpfun_v2. Writes DATA/dune_trade_daily.csv and DATA/trade_coverage_daily.csv.
Requires DUNE_API_KEY.
Run: python3 trade_coverage.py"""
import duckdb
import pandas as pd

from config import DATA, TMP
from dune import run

run("""select date_trunc('day', evt_block_time) d, count(*) n_trades, count(distinct mint) n_mints
       from pumpdotfun_solana.pump_evt_tradeevent
       where evt_block_date between date '2026-08-08' and date '2026-09-18'
       group by 1 order by 1""", out=f"{DATA}/dune_trade_daily.csv")
c = duckdb.connect()
c.sql(f"set TimeZone='UTC'; set threads=6; set memory_limit='10GB'; set temp_directory='{TMP}'")
hf = c.sql(f"""select strftime(date_trunc('day', block_time), '%Y-%m-%d') d, count(distinct payload_hash) hf_trades,
               count(distinct mint) hf_mints
               from read_parquet('{DATA}/hf_pumpfun_v2/*/*/trade/*.parquet') group by 1 order by 1""").df()
du = pd.read_csv(f"{DATA}/dune_trade_daily.csv")
du["d"] = du.d.str[:10]
m = du.merge(hf, on="d", how="left")
m["trade_cov"] = m.hf_trades / m.n_trades
m["mint_cov"] = m.hf_mints / m.n_mints
m.to_csv(f"{DATA}/trade_coverage_daily.csv", index=False)
print(m[["d", "n_trades", "hf_trades", "trade_cov", "mint_cov"]].round(3).to_string(index=False))
