"""Return distributions of the casino reference games. One bet stakes 1 unit; X is the total returned.

Roulette, craps, baccarat and crash are computed exactly from the games' rules. Two inputs come from published tables:
the Plinko multipliers (Rainbet, 16 rows, high risk) and the Sweet Bonanza survival points (101rtp.net, 34,968,204 demo
spins). Games without a sourced table are listed in PENDING and are not used.

Reads nothing. Writes casino_games.json (summary statistics per game) to results/aggregates.
Run: python3 casino_games.py
"""
import itertools
import json
from fractions import Fraction as F
from math import comb

from config import AGG

def roulette(pockets):
    return {
        "even-money": {F(2): F(18, pockets), F(0): F(pockets - 18, pockets)},
        "dozen": {F(3): F(12, pockets), F(0): F(pockets - 12, pockets)},
        "straight-up": {F(36): F(1, pockets), F(0): F(pockets - 1, pockets)},
    }


def craps_pass():
    # come-out: 7/11 win (8/36), 2/3/12 lose (4/36); point p then p-before-7
    ways = {4: 3, 5: 4, 6: 5, 8: 5, 9: 4, 10: 3}
    win = F(8, 36) + sum(F(w, 36) * F(w, w + 6) for w in ways.values())
    return {F(2): win, F(0): 1 - win}


def baccarat(decks=8):
    # exact, drawing without replacement over rank classes: ranks 1-9 (4*decks each) and tens (16*decks)
    counts = [16 * decks] + [4 * decks] * 9  # index = point value
    total = sum(counts)
    res = {"banker": F(0), "player": F(0), "tie": F(0)}

    def draw_prob(seq):
        c = counts[:]
        p = F(1)
        n = total
        for v in seq:
            if c[v] == 0:
                return F(0)
            p *= F(c[v], n)
            c[v] -= 1
            n -= 1
        return p

    for p1, b1, p2, b2 in itertools.product(range(10), repeat=4):  # deal order P B P B
        base = (p1, b1, p2, b2)
        pt, bt = (p1 + p2) % 10, (b1 + b2) % 10
        if pt >= 8 or bt >= 8:
            outcomes = [((), pt, bt)]
        else:
            outcomes = []
            if pt <= 5:
                for p3 in range(10):
                    pt3 = (pt + p3) % 10
                    if bt <= 2 or (bt == 3 and p3 != 8) or (bt == 4 and p3 in range(2, 8)) \
                       or (bt == 5 and p3 in range(4, 8)) or (bt == 6 and p3 in (6, 7)):
                        for b3 in range(10):
                            outcomes.append(((p3, b3), pt3, (bt + b3) % 10))
                    else:
                        outcomes.append(((p3,), pt3, bt))
            elif bt <= 5:
                for b3 in range(10):
                    outcomes.append(((b3,), pt, (bt + b3) % 10))
            else:
                outcomes = [((), pt, bt)]
        for extra, fp, fb in outcomes:
            pr = draw_prob(base + extra)
            res["tie" if fp == fb else ("player" if fp > fb else "banker")] += pr
    b, p, t = res["banker"], res["player"], res["tie"]
    return {
        "banker": {F(195, 100): b, F(1): t, F(0): p},  # 5% commission
        "player": {F(2): p, F(1): t, F(0): b},
        "tie 8:1": {F(9): t, F(0): 1 - t},
    }


def plinko_16_high():
    # Rainbet Plinko multipliers, 16 rows, high risk, listed from the edge to the centre; slot probabilities are binomial(16, 1/2)
    edge = [1000, 130, 26, 9, 4, 2, 0.2, 0.2, 0.2]
    mult = edge + edge[-2::-1]
    dist = {}
    for k, m in enumerate(mult):
        dist[F(str(m))] = dist.get(F(str(m)), 0) + F(comb(16, k), 2 ** 16)
    return dist


def crash_target(rtp, target):
    # crash point C satisfies P(C >= m) = rtp/m for m >= 1; a fixed cash-out target is a 2-outcome bet
    p = F(rtp) / F(target)
    return {F(target): p, F(0): 1 - p}


SWEET_BONANZA = {
    "source": "101rtp.net/slots/sweet-bonanza, 34,968,204 demo spins, measured RTP 96.40% (published 96.48/96.49/95.45)",
    "rtp_published": 0.965,
    "p_zero": 1 - 1 / 2.33,
    "survival": {10: 1 / 102, 100: 1 / 1329, 500: 1 / 39290, 1000: 1 / 431706},
    "max_observed": 2417.1,
    "max_theoretical": 21175,
}

PENDING = ["video poker 9/6 Jacks or Better (Wizard of Odds return table)",
           "blackjack basic strategy outcome distribution",
           "keno (casino-specific paytable)", "lotteries: Mega-Sena, Powerball (official prize tiers)",
           "Plinko low/medium risk multipliers", "Fortune Tiger (PG Soft rules sheet)"]


def stats(dist):
    items = sorted(dist.items())
    rtp = sum(x * p for x, p in items)
    cum, median = F(0), None
    for x, p in items:
        cum += p
        if cum >= F(1, 2):
            median = x
            break
    surv = lambda m: sum(p for x, p in items if x >= m)
    jack = sum(x * p for x, p in items if x >= 100)
    return {
        "rtp": float(rtp), "median_x": float(median), "win_rate": float(sum(p for x, p in items if x > 1)),
        "p_ge_10": float(surv(10)), "p_ge_100": float(surv(100)), "p_ge_1000": float(surv(1000)),
        "jackpot_share_ge100": float(jack / rtp) if rtp else None, "max_x": float(items[-1][0]),
    }


def main():
    games = {}
    for name, d in roulette(37).items():
        games[f"European roulette, {name}"] = d
    for name, d in roulette(38).items():
        games[f"American roulette, {name}"] = d
    games["Craps, pass line"] = craps_pass()
    for name, d in baccarat().items():
        games[f"Baccarat (8 decks), {name}"] = d
    games["Plinko 16 rows, high risk (Rainbet)"] = plinko_16_high()
    for t in (2, 10, 100, 1000):
        games[f"Crash 99% RTP, cash out at {t}x"] = crash_target("0.99", t)
    out = {g: stats(d) for g, d in games.items()}
    for g, s in out.items():
        print(f"{g:45s} RTP {s['rtp']:.4%}  median {s['median_x']:g}x  win {s['win_rate']:.2%}  "
              f"P>=100 {s['p_ge_100']:.5%}  jackpot share {s['jackpot_share_ge100']:.1%}")
    out["_sweet_bonanza_survival"] = SWEET_BONANZA
    out["_pending"] = PENDING
    json.dump(out, open(f"{AGG}/casino_games.json", "w"), indent=1)


if __name__ == "__main__":
    main()
