"""Unregistered check: launch-day cluster bootstrap intervals (10,000 draws) for the memecoin wallet classes, and the retail
H1 interval clustered on launch day as the Study 3 registration states (s3_analyze.py clusters memecoin positions on the
day of their first buy). Classes as in s3_secondary.py. Paired differences against retail use the same day draws.
Reads DATA/s2_positions.parquet. Writes AGG/s3_class_ci.json.
Run: python3 s3_class_ci.py"""
import json
import os

import duckdb
import numpy as np
import pandas as pd

from config import AGG, DATA, TMP

D = DATA
W0 = pd.Timestamp("2026-08-08", tz="UTC").timestamp()
RNG = np.random.default_rng(20261025)
c = duckdb.connect()
c.sql(f"set threads=2; set memory_limit='3GB'; set temp_directory='{TMP}/s3_class_ci_{os.getpid()}'; set TimeZone='UTC'")
g = c.sql(f"""
  with p as (select cash_in, cash_out, epoch(t_create) tc, epoch(first_t) tf,
    case when is_creator then 'creator' when is_launch_block then 'launch block' when is_distributed then 'distributed'
         when is_protocol then 'protocol' when is_atomic_bot then 'atomic bot' when max_hour > 200 then 'over 200 an hour'
         when oversold then 'oversold' else 'retail' end cls
    from read_parquet('{D}/s2_positions.parquet') where cash_in > 0 and isfinite(cash_out))
  select cls, floor((tc - {W0}) / 86400)::int lday, floor((tf - {W0}) / 86400)::int fday, sum(cash_in) ci, sum(cash_out) co
  from p group by all""").df()


def boot(day_col):
    t = g.groupby(["cls", day_col])[["ci", "co"]].sum().reset_index()
    days = np.sort(t[day_col].unique())
    piv_ci = t.pivot(index=day_col, columns="cls", values="ci").reindex(days).fillna(0)
    piv_co = t.pivot(index=day_col, columns="cls", values="co").reindex(days).fillna(0)
    k = len(days)
    W = np.stack([np.bincount(RNG.integers(0, k, k), minlength=k) for _ in range(10_000)])
    rtp = (W @ piv_co.values) / (W @ piv_ci.values)
    out = {}
    r_idx = list(piv_ci.columns).index("retail")
    for j, cl in enumerate(piv_ci.columns):
        est = piv_co[cl].sum() / piv_ci[cl].sum()
        d = rtp[:, j] - rtp[:, r_idx]
        out[cl] = {"rtp": float(est), "ci": [float(x) for x in np.percentile(rtp[:, j], [2.5, 97.5])],
                   "diff_vs_retail_ci": [float(x) for x in np.percentile(d, [2.5, 97.5])], "days": int(k)}
    return out


res = {"launch_day_clusters": boot("lday"), "first_buy_day_clusters": boot("fday")}
json.dump(res, open(f"{AGG}/s3_class_ci.json", "w"), indent=1)
for k, v in res.items():
    print(k)
    for cl, x in v.items():
        print(f"  {cl:18s} {x['rtp']:.3f} {np.round(x['ci'], 3)} diff {np.round(x['diff_vs_retail_ci'], 3)} days {x['days']}")
