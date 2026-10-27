"""Exploratory check (not registered): results of the 2024 Dune rebuild (dune_2024.py) next to the registered 2026 numbers.

Per window: net flow by class, pooled retail RTP with the launch-day bootstrap (as in s2_dune_full_compare.py), share of
retail wallets ahead, the shape of the retail multiple, RTP by holding time and full exit, wallet outcome buckets, share
ahead by number of positions, a cash-flow decomposition and SOL amounts in dollars at the window's mean daily SOL/USD
price (sol_price.py).

Reads DATA/dune_2024/<w>_agg_v4.csv. Writes AGG/dune_2024_summary.json.

Run: DUNE_API_KEY=... python dune_2024_summary.py
"""
import json
import os
import sys

import numpy as np
import pandas as pd

from config import AGG
from dune_2024 import GRID, OUT, WINDOWS
from sol_price import sol_usd_mean


def sol_usd(W):
    return sol_usd_mean(W["start"], W["end"])


def summarise(wn):
    a = pd.read_csv(f"{OUT}/{wn}_agg_v4.csv", keep_default_na=True)
    for c in a.columns:
        if c not in ("blk", "cls", "hb", "kb", "rb", "ex"):
            a[c] = pd.to_numeric(a[c], errors="coerce")
    px = sol_usd(WINDOWS[wn])
    out = {"sol_usd_mean": px}
    p = a[a.blk == "pos"]
    cls = p.groupby("cls").agg(positions=("n", "sum"), wallets=("wallets", "sum"), ci=("ci", "sum"), co=("co", "sum"))
    out["by_class"] = {k: {"positions": int(r.positions), "wallets": int(r.wallets), "sol_in": r.ci / 1e9, "sol_out": r.co / 1e9,
                           "net_sol": (r.co - r.ci) / 1e9, "net_usd": (r.co - r.ci) / 1e9 * px, "rtp": r.co / r.ci}
                       for k, r in cls.iterrows()}
    r = p[p.cls == "retail"]
    g = r.groupby("dayi")[["ci", "co"]].sum()
    rng = np.random.default_rng(20260924)
    bs = []
    for _ in range(10_000):
        w = np.bincount(rng.integers(0, len(g), len(g)), minlength=len(g))
        bs.append((g.co.values * w).sum() / (g.ci.values * w).sum())
    n = r.n.sum()
    hb = r.groupby("xb").n.sum().sort_index()
    mb = hb.index[np.searchsorted(hb.cumsum().values, n / 2)]
    out["retail"] = {"rtp": float(g.co.sum() / g.ci.sum()), "ci_day": list(np.percentile(bs, [2.5, 97.5])), "positions": int(n),
                     "wallets": int(r.wallets.sum()), "share_wallets_ahead": float(r.wallets_ahead.sum() / r.wallets.sum()),
                     "win_rate": float(r.win.sum() / n), "median_bin": [10 ** (mb / 20), 10 ** ((mb + 1) / 20)],
                     "jackpot_share": float(r.jx.sum() / r.sx.sum()), "surv": {m: float(r[f"ge{m}"].sum() / n) for m in GRID},
                     "loss_sol": float((g.ci.sum() - g.co.sum()) / 1e9), "loss_usd": float((g.ci.sum() - g.co.sum()) / 1e9 * px)}
    h = a[a.blk == "hold"].copy()
    h["ex"] = h.ex.astype(str).str.lower() == "true"
    hold = {}
    for (b, e), s in h.groupby(["hb", "ex"]):
        hold[f"{b} | {'sold all' if e else 'kept bag'}"] = {"n": int(s.n.sum()), "rtp": float(s.co.sum() / s.ci.sum()),
                                                           "win": float(s.win.sum() / s.n.sum()), "sol_in": float(s.ci.sum() / 1e9)}
    for e, s in h.groupby("ex"):
        hold[f"all | {'sold all' if e else 'kept bag'}"] = {"n": int(s.n.sum()), "rtp": float(s.co.sum() / s.ci.sum()),
                                                            "sol_in": float(s.ci.sum() / 1e9), "loss_sol": float((s.ci.sum() - s.co.sum()) / 1e9)}
    out["hold"] = hold
    wl = a[a.blk == "wal"]
    out["wallet_buckets"] = {b: float(s.n.sum() / wl.n.sum()) for b, s in wl.groupby("rb")}
    out["ahead_by_positions"] = {k: {"wallets": int(s.n.sum()), "ahead": float(s.wallets_ahead.sum() / s.n.sum()),
                                     "rtp": float(s.co.sum() / s.ci.sum())} for k, s in wl.groupby("kb")}
    d = a[a.blk == "cls"].rename(columns={"sx": "paid", "wallets": "cbuy", "wallets_ahead": "recv", "win": "n_resid", "jx": "csell",
                                          "hill_n": "paid_dx", "hill_s": "recv_dx", "ge2": "liq", "ge3": "liq_noimp"})
    d["ex"] = d.ex.astype(str).str.lower() == "true"
    cols = ["n", "ci", "co", "paid", "cbuy", "recv", "csell", "paid_dx", "recv_dx", "liq", "liq_noimp", "n_resid"]
    out["decomposition_sol"] = {f"{c} | {v} | {'sold all' if e else 'kept bag'}": {k: float(s[k].sum()) / (1 if k in ("n", "n_resid") else 1e9) for k in cols}
                                for (c, v, e), s in d.groupby(["cls", "hb", "ex"])}
    t = d[cols].sum()
    out["all_classes_sol"] = {k: float(t[k]) / (1 if k in ("n", "n_resid") else 1e9) for k in cols}
    rr = d[d.cls == "retail"]
    out["retail_rtp_grad_bags_no_impact"] = float((rr.co.sum() + (rr.liq_noimp - rr.liq)[rr.hb == "grad"].sum()) / rr.ci.sum())
    out["retail_rtp_grad_bags_zero"] = float((rr.co.sum() - rr.liq[rr.hb == "grad"].sum()) / rr.ci.sum())
    sz = a[a.blk == "size"]
    out["by_stake_sol"] = {b: {"n": int(s.n.sum()), "rtp": float(s.co.sum() / s.ci.sum()), "win": float(s.win.sum() / s.n.sum())}
                           for b, s in sz.groupby("hb")}
    return out


res = {wn: summarise(wn) for wn in WINDOWS if os.path.exists(f"{OUT}/{wn}_agg_v4.csv")}
res["registered_2026_reference"] = "results/aggregates/s3_results_meme.json (retail RTP 0.754, 14.1% of wallets ahead)"
json.dump(res, open(f"{AGG}/dune_2024_summary.json", "w"), indent=1, default=float)
print(json.dumps(res, indent=1, default=float))
