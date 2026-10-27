"""Study 2 data check (not registered): results of the Dune rebuild (dune_s2_full.py) next to the registered local results.

Reads DATA/dune_state/s2full_agg_allpools.csv and, if present, s2full_agg.csv (the same rebuild with the local pool list
only), DATA/dune_state/s2full_census.csv and AGG/s2_results.json. Computes the statistics of s2_analyze.py that the
aggregates allow: pooled retail RTP with the launch-day bootstrap and the H1 decisions (Holm, 97.5% bound), net flow by
class, the share of retail wallets ahead (the retail side of H2) and the shape of the retail multiple (H3). The median
comes from a histogram in 0.05-decade bins. Writes AGG/s2_dune_full.json.

Run: python s2_dune_full_compare.py
"""
import json
import os
import sys

import numpy as np
import pandas as pd

from casino_games import baccarat, craps_pass, plinko_16_high, roulette
from config import AGG, DATA

D = DATA
DS = f"{D}/dune_state"
GRID = (2, 3, 5, 10, 20, 50, 100, 200, 500, 1000)
GAMES = {"European roulette even money": roulette(37)["even-money"], "American roulette even money": roulette(38)["even-money"],
         "Craps pass line": craps_pass(), "Baccarat banker": baccarat()["banker"], "Plinko 16 high": plinko_16_high()}
GAME_RTP = {n: float(sum(float(v) * float(p) for v, p in d.items())) for n, d in GAMES.items()}
GAME_RTP["Sweet Bonanza (published)"] = 0.965


def h1(g, rng):
    k = len(g)
    bs = np.empty(10_000)
    for i in range(10_000):
        w = np.bincount(rng.integers(0, k, k), minlength=k)
        bs[i] = (g.co.values * w).sum() / (g.ci.values * w).sum()
    pv = {n: float((bs >= r).mean()) for n, r in GAME_RTP.items()}
    order = sorted(pv, key=pv.get)
    holm = {}
    for i, n in enumerate(order):
        holm[n] = min(1.0, max(pv[n] * (len(order) - i), max([holm[o] for o in order[:i]], default=0)))
    return list(np.percentile(bs, [2.5, 97.5])), {n: holm[n] < 0.025 for n in GAME_RTP}


def summarise(f):
    a = pd.read_csv(f)
    num = [c for c in a.columns if c not in ("cls",)]
    a[num] = a[num].apply(pd.to_numeric, errors="coerce")
    out = {}
    cls = a.groupby("cls").agg(positions=("n", "sum"), wallets=("wallets", "sum"), ci=("ci", "sum"), co=("co", "sum"))
    cls["rtp"] = cls.co / cls.ci
    out["net_flow_by_class_SOL"] = {k: {"positions": int(r.positions), "wallets": int(r.wallets), "sol_in": r.ci / 1e9,
                                        "sol_out": r.co / 1e9, "rtp": r.rtp} for k, r in cls.iterrows()}
    r = a[a.cls == "retail"]
    g = r.groupby("dayi")[["ci", "co"]].sum()
    out["H1_pooled_retail_rtp"] = float(g.co.sum() / g.ci.sum())
    out["H1_ci_day"], out["H1_holds"] = h1(g, np.random.default_rng(20260924))
    o = r[r.dayi.between(33, 38)]
    out["retail_rtp_launched_09_03_to_09_08"] = float(o.co.sum() / o.ci.sum())
    out["retail_rtp_launched_other_days"] = float(r[~r.dayi.between(33, 38)].co.sum() / r[~r.dayi.between(33, 38)].ci.sum())
    out["retail_wallets"] = int(r.wallets.sum())
    out["share_wallets_ahead"] = float(r.wallets_ahead.sum() / r.wallets.sum())
    n = r.n.sum()
    hb = r.groupby("xb").n.sum().sort_index()
    med_bin = hb.index[np.searchsorted(hb.cumsum().values, n / 2)]
    out["H3"] = {"n": int(n), "mean_x": float(r.sx.sum() / n), "median_bin": [10 ** (med_bin / 20), 10 ** ((med_bin + 1) / 20)],
                 "win_rate": float(r.win.sum() / n), "jackpot_share": float(r.jx.sum() / r.sx.sum()),
                 "hill_above_10": float(r.hill_n.sum() / r.hill_s.sum()),
                 "surv": {m: float(r[f"ge{m}"].sum() / n) for m in GRID}}
    return out


res = {}
for tag, fn in (("dune_rebuild", "s2full_agg_allpools.csv"), ("dune_rebuild_local_pool_list", "s2full_agg.csv")):
    if os.path.exists(f"{DS}/{fn}"):
        res[tag] = summarise(f"{DS}/{fn}")
cen = pd.read_csv(f"{DS}/s2full_census.csv")
res["dune_census"] = {"sol_curve_tokens": int(cen[cen.sol].n.sum()), "other_quote_tokens": int(cen[~cen.sol].n.sum()),
                      "completed_by_end": int(cen[cen.sol].completed.sum()),
                      "completed_without_local_pool": int(cen[cen.sol].completed_no_pool.sum())}
L = json.load(open(f"{AGG}/s2_results.json"))
res["local_registered"] = {"H1_pooled_retail_rtp": L["H1_pooled_retail_rtp"], "H1_ci_day": L["H1_ci_day"],
                           "retail_positions": L["counts"]["retail"], "retail_wallets": L["retail_wallets"],
                           "share_wallets_ahead": L["share_wallets_ahead"],
                           "H3": {k: L["H3_retail_position_shape"][k] for k in ("n", "median", "win_rate", "jackpot_share", "hill_above_10", "surv")}}
json.dump(res, open(f"{AGG}/s2_dune_full.json", "w"), indent=1, default=float)
print(json.dumps(res, indent=1, default=float))
