"""Synthetic retail position tables for the smoke test, with known properties.

Each venue is a table of positions (w, t, cash_in, cash_out) over the 42-day window starting 8 August 2026:
  memecoin: most positions lose most of their stake, a few win and a small group of wallets (4%) is given a real edge;
  perpetual: multiples close to one with a small fee drag and no edge;
  prediction: binary bets bought slightly above fair probability and no edge.
Writes DATA/synthetic/<venue>.parquet. The tables are fake and carry no information about the real venues."""
import os

import numpy as np
import pandas as pd

from config import DATA

T0 = pd.Timestamp("2026-08-08", tz="UTC").timestamp()
DAYS = 42


def wallets(rng, n_wallets, mean_positions):
    k = rng.geometric(1 / mean_positions, n_wallets)
    w = np.repeat(np.arange(n_wallets), k)
    t = T0 + rng.random(len(w)) * DAYS * 86400
    return w, t


def memecoin(rng, n_wallets=25_000):
    w, t = wallets(rng, n_wallets, 4)
    stake = rng.lognormal(np.log(0.1), 1.0, len(w))
    win = rng.random(len(w)) < 0.24
    x = np.where(win, rng.lognormal(np.log(1.6), 0.6, len(w)), rng.beta(0.6, 1.4, len(w)))
    jackpot = rng.random(len(w)) < 2e-4
    x = np.where(jackpot, rng.uniform(100, 400, len(w)), x)
    skilled = rng.random(n_wallets) < 0.04
    x = np.where(skilled[w], x * 1.8, x)
    return w, t, stake, stake * x


def perpetual(rng, n_wallets=6_000):
    w, t = wallets(rng, n_wallets, 15)
    stake = rng.lognormal(np.log(2_000), 1.2, len(w))
    x = 1 + rng.normal(0, 0.03, len(w)) - 0.0004
    return w, t, stake, stake * x


def prediction(rng, n_wallets=8_000):
    w, t = wallets(rng, n_wallets, 12)
    stake = rng.lognormal(np.log(50), 1.0, len(w))
    p = rng.uniform(0.05, 0.95, len(w))
    x = (rng.random(len(w)) < p) / (p + 0.003)
    return w, t, stake, stake * x


def main(seed=20261021):
    rng = np.random.default_rng(seed)
    out = os.path.join(DATA, "synthetic")
    os.makedirs(out, exist_ok=True)
    for name, f in (("memecoin", memecoin), ("perpetual", perpetual), ("prediction", prediction)):
        w, t, cin, cout = f(rng)
        pd.DataFrame({"w": w.astype(np.int64), "t": t, "cash_in": cin, "cash_out": cout}).to_parquet(f"{out}/{name}.parquet")
        print(f"{name}: {len(w):,} positions, {w.max() + 1:,} wallets, pooled RTP {cout.sum() / cin.sum():.3f}")


if __name__ == "__main__":
    main()
