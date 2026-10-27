"""Control arm of Study 1 (Study 1 registration, Arms): BTC and ETH spot.

One control bet per launch-tier bet, with that bet's t_entry and t_exit and a 10 bp taker fee per side:
X = P_exit / P_entry * (1 - 0.001)^2, where P is the open of the Binance 1-minute kline containing the time.
Reads data/bets_v2.parquet and the BTCUSDT and ETHUSDT 1-minute kline zips in data/binance (data.binance.vision).
Writes data/control_x.parquet (bet_id, X_BTC, X_ETH, launch_day).
Run: python3 control_btc_eth.py
"""
import glob
import zipfile

import numpy as np
import pandas as pd

from config import DATA

D = DATA


def minute_open(sym):
    rows = []
    for z in sorted(glob.glob(f"{D}/binance/{sym}-1m-*.zip")):
        with zipfile.ZipFile(z) as zz:
            df = pd.read_csv(zz.open(zz.namelist()[0]), header=None, usecols=[0, 1])
        rows.append(df)
    k = pd.concat(rows)
    k.columns = ["ot", "open"]
    k["ot"] = np.where(k.ot > 1e14, k.ot // 1000, k.ot)
    return pd.Series(k.open.values, index=pd.to_datetime(k.ot, unit="ms", utc=True)).sort_index()


bets = pd.read_parquet(f"{D}/bets_v2.parquet", columns=["bet_id", "t_create", "t_entry", "t_exit"])
out = bets[["bet_id", "t_create"]].copy()
for sym in ("BTCUSDT", "ETHUSDT"):
    px = minute_open(sym)
    pe = px.reindex(bets.t_entry.dt.tz_localize("UTC").dt.floor("min")).values  # stored times are naive UTC
    pxe = px.reindex(bets.t_exit.dt.tz_localize("UTC").dt.floor("min")).values
    out[f"X_{sym[:3]}"] = pxe / pe * (1 - 0.001) ** 2
    print(sym, "missing entry", int(np.isnan(pe).sum()), "missing exit", int(np.isnan(pxe).sum()))
out["launch_day"] = pd.to_datetime(out.t_create, utc=True).dt.floor("D")
out.drop(columns="t_create").to_parquet(f"{D}/control_x.parquet", index=False)
