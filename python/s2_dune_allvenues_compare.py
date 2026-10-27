"""Exploratory check (not registered): the Dune rebuild with trades on every Solana DEX venue (dune_s2_full.py with
OTHER_VENUES=1) next to the same rebuild with PumpSwap only, as the registered rules define it.

Reads DATA/dune_state/s2full_agg_allpools.csv and s2full_agg_allvenues.csv. Runs summarise() from s2_dune_full_compare.py
unchanged, by executing the part of that script before its results block, so that script's own output is not touched.
Writes AGG/s2_dune_allvenues.json.

Run: python s2_dune_allvenues_compare.py
"""
import json
import os

from config import AGG, DATA

D = DATA
src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "s2_dune_full_compare.py")).read()
ns = {}
exec(src[:src.index("\nres = {}")], ns)  # imports, constants, h1() and summarise()
res = {"pumpswap_only_registered_rules": ns["summarise"](f"{D}/dune_state/s2full_agg_allpools.csv"),
       "all_venues": ns["summarise"](f"{D}/dune_state/s2full_agg_allvenues.csv")}
json.dump(res, open(f"{AGG}/s2_dune_allvenues.json", "w"), indent=1, default=float)
for k, v in res.items():
    print(k, "RTP", round(v["H1_pooled_retail_rtp"], 4), "CI", [round(x, 4) for x in v["H1_ci_day"]], "ahead", round(v["share_wallets_ahead"], 4),
          "wallets", v["retail_wallets"], "win", round(v["H3"]["win_rate"], 4))
    print("  ", {c: (round(x["rtp"], 3), round(x["sol_in"])) for c, x in v["net_flow_by_class_SOL"].items()})
