"""Smoke test, about a minute on a laptop. Run from the repository root:  python python/smoke_test.py

1. Exact casino laws: recomputes the six games and compares them with the shipped results.
2. Synthetic venues: builds fake position tables with known properties (synthetic_data.py) and runs the paper's
   registered tests (s3_analyze.py: H1, H2, H4, H5) on them. The memecoin table must come out below every game with
   its planted skilled wallets detected; the perpetual and prediction tables must show no skill.
3. Paper numbers: recomputes every number quoted in the paper from results/aggregates and compares it with the shipped
   results/tables/paper_numbers.csv, then redraws the figures.
All output goes to a temporary folder; nothing in the repository is modified."""
import json
import os
import subprocess
import sys
import tempfile
import time

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def run(script, *args, env):
    r = subprocess.run([sys.executable, os.path.join(HERE, script), *args], env=env, cwd=HERE, capture_output=True, text=True)
    if r.returncode:
        sys.exit(f"{script} failed:\n{r.stdout[-2000:]}\n{r.stderr[-2000:]}")


def main():
    t0 = time.time()
    tmp = tempfile.mkdtemp(prefix="memecoin_smoke_")
    env = dict(os.environ, MEMECOIN_PAPER_DATA=os.path.join(tmp, "data"), MEMECOIN_PAPER_AGG=os.path.join(tmp, "agg"),
               MEMECOIN_PAPER_TABLES=os.path.join(tmp, "tables"), MEMECOIN_PAPER_FIGURES=os.path.join(tmp, "figures"))
    checks = []

    run("casino_games.py", env=env)
    new = json.load(open(os.path.join(tmp, "agg", "casino_games.json")))
    old = json.load(open(os.path.join(ROOT, "results", "aggregates", "casino_games.json")))
    checks.append(("casino laws match the shipped results",
                   new == old))

    run("synthetic_data.py", env=env)
    res = {}
    for v in ("memecoin", "perpetual", "prediction"):
        run("s3_analyze.py", f"synthetic_{v}", env=env)
        res[v] = json.load(open(os.path.join(tmp, "agg", f"s3_results_synthetic_{v}.json")))
    below = {v: sum(g["holds"] for g in r["H1"]["vs_games"].values()) for v, r in res.items()}
    checks += [("synthetic memecoin RTP is below all six games (H1)", below["memecoin"] == 6),
               ("synthetic perpetual RTP is below none of the games (H1)", below["perpetual"] == 0),
               ("planted memecoin skill is detected (H4)", res["memecoin"]["H4"]["skill_present"]),
               ("no skill found where none was planted (H4)",
                not res["perpetual"]["H4"]["skill_present"] and not res["prediction"]["H4"]["skill_present"])]

    shipped = {k: v for k, v in env.items() if k != "MEMECOIN_PAPER_AGG"}
    run("paper_numbers.py", env=shipped)
    new = pd.read_csv(os.path.join(tmp, "tables", "paper_numbers.csv"), dtype=str, keep_default_na=False)
    old = pd.read_csv(os.path.join(ROOT, "results", "tables", "paper_numbers.csv"), dtype=str, keep_default_na=False)
    checks.append((f"all {len(old)} paper numbers reproduce from the aggregates", new.equals(old)))
    run("paper_figures.py", env=shipped)
    checks.append(("figures 1 to 5 redraw", len([f for f in os.listdir(os.path.join(tmp, "figures")) if f.endswith(".pdf")]) == 5))

    for v, r in res.items():
        print(f"  synthetic {v:<10} RTP {r['H1']['pooled_rtp']:.3f}  wallets ahead {r['H2_retail_share_ahead']:.1%}  "
              f"above own null {r['H4']['share_above_null95']:.1%}")
    num = dict(zip(new.name, new.value))
    print(f"  paper: memecoin RTP {num['MemeRTP']} ({num['MemeLo']} to {num['MemeHi']}), Hyperliquid {num['HlRTP']}, "
          f"Polymarket {num['PmRTP']}, American roulette {num['RtpAR']}")
    for name, ok in checks:
        print(f"{'PASS' if ok else 'FAIL'}  {name}")
    print(f"{sum(ok for _, ok in checks)}/{len(checks)} checks passed in {time.time() - t0:.0f} s (output in {tmp})")
    sys.exit(0 if all(ok for _, ok in checks) else 1)


if __name__ == "__main__":
    main()
