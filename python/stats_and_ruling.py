"""Study 1 statistics, bootstrap intervals and the four-condition ruling (Study 1 registration: Unit and statistics,
Inference, Ruling).

The launch-day cluster bootstrap (10,000 resamples) runs on per-day sufficient statistics and per-day log10 histograms, so
the bootstrap median is read from a histogram with bins 0.005 decades wide. The secondary token-cluster bootstrap uses
Poisson(1) token weights, 1,000 draws, and mean-type statistics only.
Reads data/bets_x.parquet and data/control_x.parquet; the casino references come from casino_games.py. Writes
results.json to results/aggregates and prints the headline numbers.
Run: python3 stats_and_ruling.py
"""
import json
from math import comb

import numpy as np
import pandas as pd

from casino_games import SWEET_BONANZA, plinko_16_high, roulette
from config import AGG, DATA

D = DATA
GRID = [2, 3, 5, 10, 20, 50, 100, 200, 500, 1000]
EDGES = np.arange(-8, 6.0001, 0.005)          # log10 X histogram edges; X <= 1e-8 lands in bin 0
RNG = np.random.default_rng(20260923)


def stats(x):
    x = x[np.isfinite(x)]
    n = len(x)
    s = {"n": n, "rtp": x.mean(), "median": float(np.median(x)), "win_rate": (x > 1).mean()}
    for m in (10, 100, 1000):
        s[f"p_ge_{m}"] = (x >= m).mean()
    s["jackpot_share"] = x[x >= 100].sum() / x.sum()
    S = lambda m: (x >= m).mean()
    for a, b in ((10, 100), (100, 500), (500, 1000)):
        s[f"slope_{a}_{b}"] = (-(np.log(S(b)) - np.log(S(a))) / (np.log(b) - np.log(a))) if S(b) > 0 else np.nan
    t = x[x >= 10]
    s["hill_above_10"] = len(t) / np.log(t / 10).sum() if len(t) > 1 else np.nan
    s["surv"] = {m: S(m) for m in GRID}
    return s


def day_aggregates(x, day):
    df = pd.DataFrame({"x": x, "d": day}).dropna()
    df = df[np.isfinite(df.x)]
    g = df.groupby("d")
    agg = pd.DataFrame({
        "n": g.size(), "sx": g.x.sum(), "win": g.x.apply(lambda v: (v > 1).sum()),
        "jx": g.x.apply(lambda v: v[v >= 100].sum()),
        "hill_n": g.x.apply(lambda v: (v >= 10).sum()), "hill_s": g.x.apply(lambda v: np.log(v[v >= 10] / 10).sum()),
    })
    for m in GRID + [10, 100, 1000]:
        agg[f"ge{m}"] = g.x.apply(lambda v, m=m: (v >= m).sum())
    lx = np.log10(np.clip(df.x.values, 1e-8, None))
    idx = np.clip(np.searchsorted(EDGES, lx, side="right") - 1, 0, len(EDGES) - 2)
    days = agg.index
    H = np.zeros((len(days), len(EDGES) - 1))
    di = pd.Index(days).get_indexer(df.d.values)
    np.add.at(H, (di, idx), 1)
    return agg, H


def from_agg(a, H):
    n = a.n.sum()
    S = lambda m: a[f"ge{m}"].sum() / n
    c = np.cumsum(H.sum(0))
    med = 10 ** EDGES[np.searchsorted(c, n / 2)]
    r = {"rtp": a.sx.sum() / n, "median": med, "win_rate": a.win.sum() / n,
         "p_ge_10": S(10), "p_ge_100": S(100), "p_ge_1000": S(1000), "jackpot_share": a.jx.sum() / a.sx.sum(),
         "hill_above_10": a.hill_n.sum() / a.hill_s.sum() if a.hill_s.sum() > 0 else np.nan}
    for lo, hi in ((10, 100), (100, 500), (500, 1000)):
        r[f"slope_{lo}_{hi}"] = -(np.log(S(hi)) - np.log(S(lo))) / (np.log(hi) - np.log(lo)) if S(hi) > 0 else np.nan
    return r


def day_bootstrap(x, day, B=10_000):
    a, H = day_aggregates(x, day)
    k = len(a)
    draws = []
    A = a.values
    cols = list(a.columns)
    for _ in range(B):
        w = np.bincount(RNG.integers(0, k, k), minlength=k)
        aw = pd.DataFrame(A * w[:, None], columns=cols)
        draws.append(from_agg(aw, H * w[:, None]))
    d = pd.DataFrame(draws)
    return {c: (float(np.nanpercentile(d[c], 2.5)), float(np.nanpercentile(d[c], 97.5))) for c in d.columns}


def token_bootstrap(x, token, B=1000):
    df = pd.DataFrame({"x": x, "t": token}).dropna()
    df = df[np.isfinite(df.x)]
    g = df.groupby("t").x
    agg = pd.DataFrame({"n": g.size(), "sx": g.sum(), "win": g.apply(lambda v: (v > 1).sum()),
                        "jx": g.apply(lambda v: v[v >= 100].sum()), "g100": g.apply(lambda v: (v >= 100).sum())})
    out = []
    for _ in range(B):
        w = RNG.poisson(1.0, len(agg))
        n = (agg.n * w).sum()
        out.append({"rtp": (agg.sx * w).sum() / n, "win_rate": (agg.win * w).sum() / n,
                    "p_ge_100": (agg.g100 * w).sum() / n, "jackpot_share": (agg.jx * w).sum() / (agg.sx * w).sum()})
    d = pd.DataFrame(out)
    return {c: (float(d[c].quantile(.025)), float(d[c].quantile(.975))) for c in d.columns}


def surv_exact(dist):
    return {m: float(sum(p for v, p in dist.items() if v >= m)) for m in GRID}


def distance(arm_surv, ref_surv):
    pts = [m for m in GRID if m in ref_surv]
    f = lambda p: np.log10(max(p, 1e-7))
    return float(np.mean([abs(f(arm_surv[m]) - f(ref_surv[m])) for m in pts]))


def main():
    bx = pd.read_parquet(f"{D}/bets_x.parquet")
    cx = pd.read_parquet(f"{D}/control_x.parquet")
    prim = bx[~bx.excluded]
    res = {"counts": {"bets": len(bx), "included": len(prim), "excluded": int(bx.excluded.sum()),
                      "excluded_nonsol_curve": int(bx.nonsol.sum()), "tokens_nonsol_curve": int(bx[bx.nonsol].mint.nunique()),
                      "tokens": int(prim.mint.nunique()), "launch_days": int(prim.launch_day.nunique())}}
    x = prim.X100_exit.values
    res["primary"] = stats(x)
    res["primary_ci_day"] = day_bootstrap(x, prim.launch_day.values)
    res["primary_ci_token"] = token_bootstrap(x, prim.mint.values)
    top = np.quantile(x[np.isfinite(x)], 0.9999)
    res["rtp_without_top_0.01pct"] = float(x[np.isfinite(x) & (x < top)].mean())
    # unregistered robustness check: the registered gap rule drops dead-token bets disproportionately
    res["robustness_unregistered::gap_rule_off"] = stats(bx[~bx.excluded_nogap].X100_exit.values)
    # unregistered (Erratum 3): the gap rule applied to every curve-stream silence over 5 minutes, not only the listed hours
    res["robustness_unregistered::gap_rule_all_outages_over_5min"] = stats(bx[~bx.excluded_extgap].X100_exit.values)
    res["counts"]["excluded_extgap"] = int(bx.excluded_extgap.sum())
    for col in [c for c in bx.columns if c.startswith("X") and c != "X100_exit"]:
        res[f"secondary::{col}"] = stats(prim[col].values)
    for flag, name in ((True, "mayhem"), (False, "normal")):
        res[f"split::{name}"] = stats(prim[prim.is_mayhem == flag].X100_exit.values)
    wk = pd.to_datetime(prim.launch_day).dt.to_period("W").astype(str)
    for w in sorted(wk.unique()):
        res[f"split::week {w}"] = stats(prim[wk == w].X100_exit.values)
    for sym in ("BTC", "ETH"):
        cc = cx.merge(prim[["bet_id"]], on="bet_id")
        res[f"control::{sym}"] = stats(cc[f"X_{sym}"].values)
    refs_slot = {"Plinko 16 high": surv_exact(plinko_16_high()),
                 "Sweet Bonanza": {int(k): v for k, v in SWEET_BONANZA["survival"].items()}}
    refs_table = {"BTC control": res["control::BTC"]["surv"],
                  "European roulette even money": surv_exact(roulette(37)["even-money"])}
    arm = res["primary"]["surv"]
    d_slot = {k: distance(arm, v) for k, v in refs_slot.items()}
    d_table = {k: distance(arm, v) for k, v in refs_table.items()}
    p = res["primary"]
    cond = {
        "1 median < 1": p["median"] < 1,
        "2 win rate < 48.65%": p["win_rate"] < 0.4865,
        "3 jackpot share >= 9%": p["jackpot_share"] >= 0.09,
        "4 slot distance < table distance": min(d_slot.values()) < min(d_table.values()),
    }
    res["ruling"] = {"distances_slot": d_slot, "distances_table": d_table, "conditions": cond,
                     "passes": all(cond.values())}
    json.dump(res, open(f"{AGG}/results.json", "w"), indent=1, default=float)
    print(json.dumps({"counts": res["counts"], "primary": {k: v for k, v in p.items() if k != "surv"},
                      "ci_day": res["primary_ci_day"], "ruling": res["ruling"]}, indent=1, default=float))


if __name__ == "__main__":
    main()
