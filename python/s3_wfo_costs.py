"""Study 3 sensitivity checks, not registered: rolling walk-forward persistence and cost stress.

Walk-forward: the window is cut into calendar weeks from its start. For fold k = 1..5 the in-sample period is weeks 0 to k-1
(expanding) and the out-of-sample period is week k. Wallets with at least 5 positions in both periods are ranked by
in-sample ROI, and the statistic is the out-of-sample RTP of the in-sample top decile, against all other eligible wallets.
The null permutes multiples within (day x stake decile) strata as in H4 and H5 (100 permutations) and recomputes the same
statistic. Folds are also pooled.
Cost stress: pooled retail RTP and day-cluster bootstrap CI with transaction costs (memecoins) or exchange fees
(Hyperliquid) set to zero and doubled.
Uses the retail loaders of s3_analyze.py. Writes AGG/s3_wfo_costs.json, adding one key per run.
Run: python3 s3_wfo_costs.py <meme|hl|pm|costs>
"""
import json
import os
import sys

import numpy as np
import pandas as pd

from config import AGG
from s3_analyze import D, W0, con, load_hl, load_meme, load_pm

PERMS, MIN_POS = 100, 5
RNG = np.random.default_rng(20261021)
OUT = f"{AGG}/s3_wfo_costs.json"


def wfo(venue):
    reg, sql = {"meme": load_meme, "hl": load_hl, "pm": load_pm}[venue]()
    c = con()
    if reg is not None:
        c.register("vaults", reg)
    w0 = W0.timestamp()
    a = c.sql(f"""with b as (select * from ({sql}) where isfinite(cash_out) and cash_in > 0)
                  select dense_rank() over (order by wh) - 1 wid, floor((t - {w0}) / 86400)::int "day", cash_in, cash_out
                  from b order by wid, t, cash_in, cash_out""").fetchnumpy()
    c.close()
    wid = np.asarray(a["wid"]); day = np.asarray(a["day"]); cin = np.asarray(a["cash_in"], dtype=np.float64)
    X = np.asarray(a["cash_out"], dtype=np.float64) / cin
    del a
    week = (day // 7).clip(0, 5)
    nw = int(wid[-1]) + 1
    dec = pd.qcut(pd.Series(cin).rank(method="first"), 10, labels=False).values
    strat = (day.astype(np.int64) * 10 + dec)
    by_strat = np.argsort(strat, kind="stable")

    folds = []
    for k in range(1, 6):
        is_m, oos_m = week < k, week == k
        n_is = np.bincount(wid, weights=is_m.astype(float), minlength=nw)
        n_oos = np.bincount(wid, weights=oos_m.astype(float), minlength=nw)
        elig = (n_is >= MIN_POS) & (n_oos >= MIN_POS)
        den_is = np.bincount(wid, weights=cin * is_m, minlength=nw)
        den_oos = np.bincount(wid, weights=cin * oos_m, minlength=nw)
        folds.append((is_m, oos_m, elig, den_is, den_oos))

    def stats(xv):
        out = []
        for is_m, oos_m, elig, den_is, den_oos in folds:
            roi = np.bincount(wid, weights=cin * xv * is_m, minlength=nw)[elig] / den_is[elig]
            top = roi >= np.quantile(roi, 0.9)
            num = np.bincount(wid, weights=cin * xv * oos_m, minlength=nw)[elig]
            d = den_oos[elig]
            out.append((num[top].sum(), d[top].sum(), num[~top].sum(), d[~top].sum()))
        return np.array(out)

    obs = stats(X)
    null = np.empty((PERMS, 5, 4))
    for i in range(PERMS):
        order = np.lexsort((RNG.random(len(X)), strat))
        Xp = np.empty_like(X)
        Xp[by_strat] = X[order]
        null[i] = stats(Xp)
        if (i + 1) % 20 == 0:
            print(venue, f"perm {i + 1}/{PERMS}", flush=True)
    res = {"folds": []}
    for k in range(5):
        res["folds"].append({"oos_week": k + 1, "wallets": int(folds[k][2].sum()), "top_oos_rtp": obs[k, 0] / obs[k, 1],
                             "others_oos_rtp": obs[k, 2] / obs[k, 3],
                             "top_null_97.5": float(np.percentile(null[:, k, 0] / null[:, k, 1], 97.5))})
    pt, pn = obs.sum(0), null.sum(1)
    res["pooled"] = {"top_oos_rtp": pt[0] / pt[1], "others_oos_rtp": pt[2] / pt[3],
                     "top_null_97.5": float(np.percentile(pn[:, 0] / pn[:, 1], 97.5)),
                     "top_null_median": float(np.median(pn[:, 0] / pn[:, 1]))}
    res["pooled"]["persistence"] = bool(res["pooled"]["top_oos_rtp"] > res["pooled"]["top_null_97.5"])
    res["folds_above_null"] = sum(f["top_oos_rtp"] > f["top_null_97.5"] for f in res["folds"])
    return res


def boot(g):
    k = len(g)
    bs = np.empty(10_000)
    for i in range(10_000):
        w = np.bincount(RNG.integers(0, k, k), minlength=k)
        bs[i] = (g.co.values * w).sum() / (g.ci.values * w).sum()
    return [float(x) for x in np.percentile(bs, [2.5, 97.5])]


def costs():
    c = con()
    base = load_meme()[1].split("where", 1)[1]
    res = {}
    for m in (0.0, 1.0, 2.0):
        g = c.sql(f"""select floor((epoch(first_t) - {W0.timestamp()}) / 86400)::int d,
                      sum(cash_in + {m - 1} * (coalesce(cost_buy_curve, 0) + coalesce(cost_buy_pool, 0))) ci,
                      sum(cash_out - {m - 1} * (coalesce(cost_sell_curve, 0) + coalesce(cost_sell_pool, 0))) co
                      from read_parquet('{D}/s2_positions.parquet') where {base} and isfinite(cash_out) group by 1""").df()
        res[f"meme_tx_cost_x{m:g}"] = {"rtp": float(g.co.sum() / g.ci.sum()), "ci": boot(g)}
    reg, sql = load_hl()
    c.register("vaults", reg)
    hl_sql = sql.replace("e.stake + e.pnl cash_out", "e.stake + e.pnl cash_out, e.fees fees")
    for m in (0.0, 1.0, 2.0):
        g = c.sql(f"""select floor((t - {W0.timestamp()}) / 86400)::int d, sum(cash_in) ci, sum(cash_out + {1 - m} * fees) co
                      from ({hl_sql}) where isfinite(cash_out) group by 1""").df()
        res[f"hl_fees_x{m:g}"] = {"rtp": float(g.co.sum() / g.ci.sum()), "ci": boot(g)}
    c.close()
    return res


if __name__ == "__main__":
    what = sys.argv[1]
    out = json.load(open(OUT)) if os.path.exists(OUT) else {}
    out["costs" if what == "costs" else f"wfo_{what}"] = costs() if what == "costs" else wfo(what)
    json.dump(out, open(OUT, "w"), indent=1, default=float)
    print(json.dumps(out["costs" if what == "costs" else f"wfo_{what}"], indent=1, default=float))
