"""Study 3 engine checks. These test the position and episode construction, not the outcome statistics.

Hyperliquid: (1) for closed episodes, compare the cash-flow P&L before fees and funding with the sum of Hyperliquid's own
closedPnl field; (2) for 5 random wallets, compare the closed-episode gross P&L from hl_episodes.py with the closedPnl sum from the public info
API (userFillsByTime).
Polymarket: for 5 random wallets, compare the buy and sell USD totals from pm_positions.py with the public data API trade history in the window.
Reads DATA/hl/episodes/ep_*.parquet and DATA/pm/positions_v2/pos_*.parquet and queries api.hyperliquid.xyz and
data-api.polymarket.com. Prints the comparisons; writes nothing.
Run: python3 s3_validate.py
"""
import json
import random

import duckdb
import requests

from config import DATA

D = DATA
W0_MS, W1_MS = 1786147200000, 1789776000000          # 2026-08-08 and 2026-09-19 00:00 UTC
c = duckdb.connect(); c.sql("set threads=4; set memory_limit='4GB'")
E = f"read_parquet('{D}/hl/episodes/ep_*.parquet')"
print("HL closed-episode identity (cashflow vs closedPnl):")
print(c.sql(f"""select count(*) n, quantile_cont(abs(cashflow - closed_pnl) / greatest(stake, 1e-9), [.5, .99, .999]) rel_err_q,
               avg((abs(cashflow - closed_pnl) <= 0.01 + 1e-6 * stake)::int) share_within_1c
               from {E} where not pre_window and not open_at_end""").df().to_string(index=False))
random.seed(7)
users = [u for (u,) in c.sql(f"""select user from (select user, count(*) n from {E} where not pre_window group by 1)
                                 where n between 3 and 40 using sample 5 rows (reservoir, 7)""").fetchall()]
for u in users:
    ours = c.sql(f"select sum(closed_pnl), count(*) from {E} where user = '{u}' and not pre_window and not open_at_end").fetchone()
    fills, t = [], W0_MS
    while t < W1_MS:
        r = requests.post("https://api.hyperliquid.xyz/info", json={"type": "userFillsByTime", "user": u, "startTime": t, "endTime": W1_MS}, timeout=60).json()
        if not r:
            break
        fills += r
        t = max(f["time"] for f in r) + 1
        if len(r) < 2000:
            break
    core = [f for f in fills if ":" not in f["coin"] and not f["coin"].startswith("@") and "/" not in f["coin"]]
    api = sum(float(f["closedPnl"]) for f in core)
    print(f"HL {u[:10]}.. ours closedPnl(closed eps)={ours[0]:.4f} over {ours[1]} eps | API closedPnl all core fills={api:.4f} ({len(core)} fills)")
P = f"read_parquet('{D}/pm/positions_v2/pos_*.parquet')"
ws = [w for (w,) in c.sql(f"""select w from (select w, sum(n_fills) n from {P} group by 1) where n between 3 and 30
                              using sample 5 rows (reservoir, 7)""").fetchall()]
for w in ws:
    ours = c.sql(f"select sum(buy_usd), sum(sell_usd) from {P} where w = '{w}'").fetchone()
    r = requests.get("https://data-api.polymarket.com/trades", params={"user": w, "limit": 500, "takerOnly": "false"}, timeout=60).json()
    tr = [x for x in r if W0_MS / 1000 <= x["timestamp"] <= W1_MS / 1000]
    buy = sum(float(x["size"]) * float(x["price"]) for x in tr if x["side"] == "BUY")
    sell = sum(float(x["size"]) * float(x["price"]) for x in tr if x["side"] == "SELL")
    print(f"PM {w[:10]}.. ours buy={ours[0]:.2f} sell={ours[1]:.2f} | API buy={buy:.2f} sell={sell:.2f} ({len(tr)} trades in window)")
