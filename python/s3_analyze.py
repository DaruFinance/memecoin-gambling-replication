"""Study 3 analysis: the same tests H1, H2, H4 and H5 on every venue (Study 3 registration, with Amendments S3-A1 and S3-A2).

Each venue is reduced to retail positions: (wallet id, calendar day, half, stake = cash_in, cash_out, X = cash_out / cash_in).
H1: pooled RTP against each casino game; day-cluster bootstrap with 10,000 draws; Holm correction; a comparison holds at
    the 97.5% bound (Erratum S3-E4).
H2: share of wallets ahead against stake-matched gamblers (200 draws per wallet from the exact game laws); wallet bootstrap
    of each wallet's paired difference (Erratum S3-E4); Holm; holds at the 97.5% bound.
H4: no-skill null. X is permuted within (stake decile x day) strata, 200 permutations. The statistic is the share of wallets
    with at least 10 positions whose ROI exceeds the 95th percentile of their own null; skill is present if the lower end of
    the wallet-bootstrap CI exceeds 5%.
H5: the window is split into halves. Spearman correlation of ROI in half 1 and half 2 for wallets with at least 5 positions
    per half, plus the half-2 RTP of the half-1 top decile. The null is the same statistics on each permutation; persistence
    is present if the observed value exceeds the null 97.5th percentile.
Venues: meme (DATA/s2_positions.parquet), hl (DATA/hl/episodes), pm (DATA/pm/positions_v2), the registered sensitivity
pm_noconv, the exploratory stricter bot cut meme_bot50 and pm_feb / pm_feb_nopairs, which read an earlier position table
for 23 March to 4 May 2026 (not distributed).
synthetic_<name> reads DATA/synthetic/<name>.parquet (smoke test only).
Writes AGG/s3_results_<venue>.json.
Run: python3 s3_analyze.py <meme|hl|pm|pm_noconv|meme_bot50|pm_feb|pm_feb_nopairs|synthetic_memecoin>
"""
import json
import os
import sys

import duckdb
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from casino_games import baccarat, craps_pass, plinko_16_high, roulette
from config import AGG, DATA, TMP
from sweet_bonanza_law import sb_law

D = DATA
RNG = np.random.default_rng(20261010)
PERMS, DRAWS = 200, 200
W0, W1 = pd.Timestamp("2026-08-08", tz="UTC"), pd.Timestamp("2026-09-19", tz="UTC")


def con():
    c = duckdb.connect()
    c.sql(f"set threads=1; set memory_limit='1500MB'; set temp_directory='{TMP}/s3_analyze_{os.getpid()}'; set TimeZone='UTC'")
    return c


def load_meme(max_hour=200):
    base = ("not is_creator and not is_launch_block and not is_distributed and not is_protocol and not is_atomic_bot "
            f"and max_hour <= {max_hour} and not oversold")
    return None, f"""select hash(w) wh, epoch(first_t) t, cash_in, cash_out from read_parquet('{D}/s2_positions.parquet')
                     where {base} and cash_in > 0"""


def load_hl():
    vaults = json.load(open(f"{D}/hl/vault_addresses.json"))
    return pd.DataFrame({"user": vaults}), f"""
      with act as (select * from read_parquet('{D}/hl/episodes/act_*.parquet')),
      bad as (select user from act where max_hour > 200 or (fills >= 1000 and maker_fills >= 0.8 * fills)
              union select user from vaults)
      select hash(e.user) wh, e.t0 / 1000.0 t, e.stake cash_in, e.stake + e.pnl cash_out
      from read_parquet('{D}/hl/episodes/ep_*.parquet') e
      where not e.pre_window and e.stake > 0 and e.user not in (select user from bad)"""


def load_synthetic(name):
    return None, f"""select hash(w) wh, t, cash_in, cash_out from read_parquet('{D}/synthetic/{name}.parquet') where cash_in > 0"""


def load_pm_feb():
    # Amendment S3-A1 replication: an earlier position table for 23 March to 4 May 2026 (not distributed)
    return None, f"""select hash(w) wh, t, cash_in, cash_out from read_parquet('{D}/pm/feb_positions.parquet')
                     where not oversold and not bot and cash_in > 0 and cash_out is not null"""


def load_pm_feb_nopairs():
    # Amendment S3-A1 sensitivity: drop positions where the wallet holds both outcomes of a market (likely minted pairs)
    return None, f"""select hash(w) wh, t, cash_in, cash_out from read_parquet('{D}/pm/feb_positions_pairs.parquet')
                     where not oversold and not bot and not pair and cash_in > 0 and cash_out is not null"""


def load_pm(extra=""):
    return None, f"""
      with act as (select * from read_parquet('{D}/pm/positions_v2/act_*.parquet')),
      bad as (select w from act where max_hour > 200 or (fills >= 1000 and maker_fills >= 0.8 * fills))
      select hash(p.w) wh, p.first_ts::double t, p.cash_in, p.cash_out
      from read_parquet('{D}/pm/positions_v2/pos_*.parquet') p
      where not p.oversold and p.cash_in > 0 and p.cash_out is not null and p.w not in (select w from bad) {extra}"""


def holm(p):
    order = sorted(p, key=p.get)
    out, m = {}, len(order)
    for i, n in enumerate(order):
        out[n] = min(1.0, max(p[n] * (m - i), max([out[o] for o in order[:i]], default=0)))
    return out


def main(venue):
    reg, sql = {"meme": load_meme, "hl": load_hl, "pm": load_pm,
                "pm_noconv": lambda: load_pm("and not p.conv"), "meme_bot50": lambda: load_meme(50),
                "pm_feb": load_pm_feb, "pm_feb_nopairs": load_pm_feb_nopairs}.get(venue, lambda: load_synthetic(venue.split("_", 1)[1]))()
    c = con()
    if reg is not None:
        c.register("vaults", reg)
    w0 = (pd.Timestamp("2026-03-23", tz="UTC") if venue.startswith("pm_feb") else W0).timestamp()
    # day = floor((t - w0) / 86400) in UTC; halves split the venue's own window [w0, last event] at its midpoint
    # (Erratum S3-E1 items 1-3)
    a = c.sql(f"""with b as (select * from ({sql}) where isfinite(cash_out) and cash_in > 0),
                  m as (select ({w0} + max(t)) / 2 mid from b)
                  select dense_rank() over (order by wh) - 1 wid, floor((t - {w0}) / 86400)::int "day",
                         (t >= m.mid)::tinyint half, cash_in, cash_out from b, m
                  order by wid, t, cash_in, cash_out""").fetchnumpy()
    mid_ts = c.sql(f"select ({w0} + max(t)) / 2 from ({sql}) where isfinite(cash_out) and cash_in > 0").fetchone()[0]
    c.close()
    df = pd.DataFrame({k: np.asarray(v) for k, v in a.items()}, copy=False)
    del a
    df["x"] = df.cash_out.values / df.cash_in.values
    res = {"venue": venue, "positions": int(len(df)), "wallets": int(df.wid.values[-1] + 1), "halves_mid_utc": str(pd.Timestamp(mid_ts, unit="s", tz="UTC"))}

    pooled = df.cash_out.sum() / df.cash_in.sum()
    g = df.groupby("day")[["cash_in", "cash_out"]].sum()
    k = len(g); bs = np.empty(10_000)
    for i in range(10_000):
        w = np.bincount(RNG.integers(0, k, k), minlength=k)
        bs[i] = (g.cash_out.values * w).sum() / (g.cash_in.values * w).sum()
    games = {"European roulette": roulette(37)["even-money"], "American roulette": roulette(38)["even-money"],
             "Craps pass line": craps_pass(), "Baccarat banker": baccarat()["banker"], "Plinko 16 high": plinko_16_high()}
    rtp = {n: float(sum(float(v) * float(p) for v, p in d.items())) for n, d in games.items()}
    rtp["Sweet Bonanza"] = 0.965
    p1 = {n: float((bs >= r).mean()) for n, r in rtp.items()}
    h = holm(p1)
    res["H1"] = {"pooled_rtp": pooled, "ci": list(np.percentile(bs, [2.5, 97.5])),
                 "vs_games": {n: {"rtp": rtp[n], "p_holm": h[n], "holds": h[n] < 0.025} for n in rtp}}
    print("H1", venue, pooled, res["H1"]["ci"], flush=True)

    starts = np.r_[0, np.flatnonzero(np.diff(df.wid.values)) + 1]
    stakes = df.cash_in.values
    tot = np.add.reduceat(stakes, starts)
    ahead = np.add.reduceat(df.cash_out.values, starts) > tot
    res["H2_retail_share_ahead"] = float(ahead.mean())
    laws = dict(games); laws["Sweet Bonanza (approx. law)"] = sb_law()
    h2, pv = {}, {}
    for n, law in laws.items():
        vals = np.array([float(v) for v in law]); pr = np.array([float(p) for p in law.values()]); cdf = np.cumsum(pr / pr.sum())
        a = np.zeros(len(starts))
        for _ in range(DRAWS):
            o = vals[np.searchsorted(cdf, RNG.random(len(stakes)), side="right").clip(0, len(vals) - 1)]
            a += np.add.reduceat(stakes * o, starts) > tot
        share = float((a / DRAWS).mean())
        d_w = ahead - a / DRAWS
        diffs = [d_w[RNG.integers(0, len(ahead), len(ahead))].mean() for _ in range(1000)]
        h2[n] = {"gamblers_ahead": share, "diff_ci": list(np.percentile(diffs, [2.5, 97.5]))}
        pv[n] = float((np.array(diffs) >= 0).mean())
    hh = holm(pv)
    for n in h2:
        h2[n].update({"p_holm": hh[n], "holds": hh[n] < 0.025})
    res["H2"] = h2
    print("H2", venue, res["H2_retail_share_ahead"], {n: round(v["gamblers_ahead"], 3) for n, v in h2.items()}, flush=True)

    dec = pd.qcut(df.cash_in.rank(method="first"), 10, labels=False).values
    strat = (df.day.values * 10 + dec).astype(np.int64)
    by_strat = np.argsort(strat, kind="stable")
    X = df.x.values; cin = df.cash_in.values; wid = df.wid.values; half = df.half.values
    npos = np.bincount(wid)
    elig = npos >= 10
    roi_obs = np.add.reduceat(cin * X, starts) / tot
    def roi_half(xv, hv):
        num = np.bincount(wid, weights=cin * xv * (half == hv), minlength=len(npos))
        den = np.bincount(wid, weights=cin * (half == hv), minlength=len(npos))
        cnt = np.bincount(wid, weights=(half == hv).astype(float), minlength=len(npos))
        return num / np.where(den > 0, den, np.nan), cnt
    r1, c1 = roi_half(X, 0); r2, c2 = roi_half(X, 1)
    both = (c1 >= 5) & (c2 >= 5)

    idx_both = np.flatnonzero(both)
    den2 = np.bincount(wid, weights=cin * (half == 1), minlength=len(npos))[idx_both]

    def h5_stats(xv, r1_, r2_):
        rho = spearmanr(r1_[both], r2_[both]).statistic if both.sum() > 10 else np.nan
        top = r1_[both] >= np.nanquantile(r1_[both], 0.9)
        num2 = np.bincount(wid, weights=cin * xv * (half == 1), minlength=len(npos))[idx_both]
        return rho, num2[top].sum() / den2[top].sum(), num2[~top].sum() / den2[~top].sum()
    rho_obs, top_obs, rest_obs = h5_stats(X, r1, r2)
    null_roi = np.empty((PERMS, int(elig.sum())), dtype=np.float32)
    rho_null = np.empty(PERMS); top_null = np.empty(PERMS)
    for i in range(PERMS):
        order = np.lexsort((RNG.random(len(X)), strat))
        Xp = np.empty_like(X); Xp[by_strat] = X[order]
        null_roi[i] = (np.add.reduceat(cin * Xp, starts) / tot)[elig]
        a1, _ = roi_half(Xp, 0); a2, _ = roi_half(Xp, 1)
        rho_null[i], top_null[i], _ = h5_stats(Xp, a1, a2)
        if (i + 1) % 20 == 0:
            print(f"perm {i + 1}/{PERMS}", flush=True)
    p95 = np.percentile(null_roi, 95, axis=0)
    above = roi_obs[elig] > p95
    share = float(above.mean())
    bs4 = [above[RNG.integers(0, len(above), len(above))].mean() for _ in range(1000)]
    res["H4"] = {"wallets_ge10": int(elig.sum()), "share_above_null95": share, "ci": list(np.percentile(bs4, [2.5, 97.5])),
                 "skill_present": bool(np.percentile(bs4, 2.5) > 0.05)}
    res["H5"] = {"wallets_both_halves": int(both.sum()), "spearman": float(rho_obs),
                 "null_97.5": float(np.nanpercentile(rho_null, 97.5)), "top_decile_half2_rtp": float(top_obs),
                 "others_half2_rtp": float(rest_obs), "top_decile_null_97.5": float(np.nanpercentile(top_null, 97.5)),
                 "persistence_present": bool(rho_obs > np.nanpercentile(rho_null, 97.5)),
                 "top_decile_persistence": bool(top_obs > np.nanpercentile(top_null, 97.5))}
    print("H4", venue, res["H4"], "\nH5", venue, res["H5"], flush=True)
    json.dump(res, open(f"{AGG}/s3_results_{venue}.json", "w"), indent=1, default=float)


if __name__ == "__main__":
    main(sys.argv[1])
