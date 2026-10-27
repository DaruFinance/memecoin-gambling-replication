"""Daily SOL/USD prices from Dune's public price table (prices.day, wrapped SOL on Solana), used to quote SOL amounts in dollars.

sol_usd_mean(start, end) returns the mean daily price over the calendar days from start to end, both included.
Run as a script to write AGG/sol_usd_window.json for the 2026 study window (8 August to 18 September 2026).
Needs DUNE_API_KEY."""
import json
import os

import pandas as pd

from config import AGG, TMP
from dune import run

WSOL = "So11111111111111111111111111111111111111112"


def daily(start, end):
    d0, d1 = pd.Timestamp(start).date(), pd.Timestamp(end).date()
    out = f"{TMP}/sol_usd_{d0}_{d1}.csv"
    if not os.path.exists(out):
        run(f"""select cast(date_trunc('day', timestamp) as date) d, avg(price) p from prices.day
                where blockchain = 'solana' and contract_address = from_base58('{WSOL}')
                  and timestamp >= timestamp '{d0}' and timestamp < timestamp '{d1}' + interval '1' day
                group by 1 order by 1""", out=out)
    return pd.read_csv(out).p.values


def sol_usd_mean(start, end):
    return float(daily(start, end).mean())


if __name__ == "__main__":
    p = daily("2026-08-08", "2026-09-18")
    res = {"n": len(p), "mean": float(p.mean()), "first": float(p[0]), "last": float(p[-1]), "min": float(p.min()), "max": float(p.max())}
    json.dump(res, open(f"{AGG}/sol_usd_window.json", "w"), indent=1)
    print(res)
