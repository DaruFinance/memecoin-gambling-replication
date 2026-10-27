"""Study 3 secondary analysis: the listed-memecoin arm (design registered in Study 1, "Listed memecoins" and carried into the
Study 3 registration).

Each record in DATA/cg/arm_bets_v2.jsonl holds K = 5 bets for one listed coin: entry time uniform over the coin's history,
holding period log-uniform between 1 minute and 7 days, with entry and exit prices. The prices were pulled from CoinGecko's
paid API, and that price pull is not included in this repository. The records are the re-priced set of Study 3 registration,
Erratum S3-E2 item 4 (no lookahead, coin-side token).
X = P_exit / P_entry * (1 - c)^2 with c = 30 bp per side (sensitivities at 10 and 100 bp).
Dead-pool sensitivity: X = 0 when the exit price is more than 7 days older than the exit time.
The statistics and the four-condition slot ruling use Study 1's code (stats_and_ruling.py) and Study 1's BTC control from
AGG/results.json. The CI is a coin-cluster bootstrap, since the listed arm has no launch day. The last block adds pricing
staleness diagnostics and an unregistered check on bets whose entry and exit prices are both at most 1 hour old.
Writes AGG/s3_results_listed.json.
Run: python3 listed_arm_analyze.py
"""
import json
import sys

import numpy as np
import pandas as pd

from casino_games import SWEET_BONANZA, plinko_16_high, roulette
from config import AGG, DATA
from stats_and_ruling import distance, stats, surv_exact, token_bootstrap

D = DATA
rows = []
for line in open(f"{D}/cg/arm_bets_v2.jsonl"):
    r = json.loads(line)
    for b in r.get("bets") or []:
        rows.append({"coin": r["id"], "active": r["active"], "mode": r["mode"], **b})
df = pd.DataFrame(rows)
ok = df.pe.notna() & (df.pe > 0) & df.px.notna() & (df.px >= 0)
unpriced = df[~ok]
causes = (unpriced.get("pe_err", pd.Series(dtype=object)).fillna("ok").astype(str) + " / " +
          unpriced.get("px_err", pd.Series(dtype=object)).fillna("ok").astype(str)).value_counts().to_dict()
res = {"counts": {"bets": int(len(df)), "priced": int(ok.sum()), "coins": int(df[ok].coin.nunique()),
                  "inactive_coins": int(df[ok & ~df.active].coin.nunique()), "agg_mode_bets": int((ok & (df["mode"] == "agg")).sum()),
                  "unpriced_by_cause_entry_exit": causes, "unpriced_inactive_coins_bets": int((~ok & ~df.active).sum())}}
df = df[ok].copy()
df["stale_exit"] = (df.tx - df.px_t) > 7 * 86400
res["counts"]["stale_exit_bets"] = int(df.stale_exit.sum())
raw = (df.px / df.pe).values
for bp in (30, 10, 100):
    x = raw * (1 - bp / 1e4) ** 2
    res[f"cost_{bp}bp"] = stats(x)
x = raw * (1 - 30 / 1e4) ** 2
res["primary_ci_coin"] = token_bootstrap(x, df.coin.values)
xd = np.where(df.stale_exit.values, 0.0, x)
res["dead_pool_sensitivity"] = stats(xd)
res["split::active"] = stats(x[df.active.values])
res["split::inactive"] = stats(x[~df.active.values])
s1 = json.load(open(f"{AGG}/results.json"))
refs_slot = {"Plinko 16 high": surv_exact(plinko_16_high()),
             "Sweet Bonanza": {int(k): v for k, v in SWEET_BONANZA["survival"].items()}}
refs_table = {"BTC control": {int(k): v for k, v in s1["control::BTC"]["surv"].items()},
              "European roulette even money": surv_exact(roulette(37)["even-money"])}
for name, s in (("primary", res["cost_30bp"]), ("dead_pool", res["dead_pool_sensitivity"])):
    arm = s["surv"]
    d_slot = {k: distance(arm, v) for k, v in refs_slot.items()}
    d_table = {k: distance(arm, v) for k, v in refs_table.items()}
    cond = {"1 median < 1": s["median"] < 1, "2 win rate < 48.65%": s["win_rate"] < 0.4865,
            "3 jackpot share >= 9%": s["jackpot_share"] >= 0.09,
            "4 slot distance < table distance": min(d_slot.values()) < min(d_table.values())}
    res[f"ruling::{name}"] = {"distances_slot": d_slot, "distances_table": d_table, "conditions": cond, "passes": all(cond.values())}
ent_st = (df.te - df.pe_t) / 3600
ex_st = (df.tx - df.px_t) / 3600
res["diagnostics"] = {"share_same_price_point": float((df.pe_t == df.px_t).mean()),
                      "entry_staleness_h_q50_q90_q99": [float(v) for v in ent_st.quantile([.5, .9, .99])],
                      "exit_staleness_h_q50_q90_q99": [float(v) for v in ex_st.quantile([.5, .9, .99])]}
fresh = ((ent_st <= 1) & (ex_st <= 1) & (df.pe_t != df.px_t)).values
res["robustness_unregistered::fresh_prices_le1h"] = stats(x[fresh])
res["robustness_unregistered::fresh_prices_le1h_ci_coin"] = token_bootstrap(x[fresh], df.coin.values[fresh])
json.dump(res, open(f"{AGG}/s3_results_listed.json", "w"), indent=1, default=float)
show = {k: ({kk: vv for kk, vv in v.items() if kk != "surv"} if isinstance(v, dict) and "surv" in v else v) for k, v in res.items()}
print(json.dumps(show, indent=1, default=float))
