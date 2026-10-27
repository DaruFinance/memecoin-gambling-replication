"""Figures 1 to 5 of the paper, drawn only from the aggregated results in results/aggregates. Writes results/figures/*.pdf."""
import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from casino_games import SWEET_BONANZA, plinko_16_high, roulette
from stats_and_ruling import GRID, surv_exact

from config import AGG, FIGURES

D = AGG
OUT = FIGURES
J = lambda f: json.load(open(f"{D}/{f}.json"))
plt.rcParams.update({"font.family": "serif", "font.size": 9, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.titlesize": 9, "legend.frameon": False, "pdf.fonttype": 42})
COL = {"meme": "#c2410c", "hl": "#1d4ed8", "pm": "#047857"}
NAME = {"meme": "pump.fun memecoins", "hl": "Hyperliquid perpetuals", "pm": "Polymarket"}
GREY = "#6b7280"
V = {k: J(f"s3_results_{k}") for k in ("meme", "hl", "pm")}
GAMES = [("European roulette", "European roulette"), ("American roulette", "American roulette"),
         ("Craps pass line", "Craps (pass line)"), ("Baccarat banker", "Baccarat (banker)"),
         ("Plinko 16 high", "Plinko (16 rows, high)"), ("Sweet Bonanza", "Sweet Bonanza")]


def save(fig, name):
    fig.savefig(f"{OUT}/{name}.pdf", bbox_inches="tight")
    plt.close(fig)


# Figure 1: pooled RTP of each venue against each game
rows = [(lbl, V["meme"]["H1"]["vs_games"][k]["rtp"], None, GREY) for k, lbl in GAMES]
for k in V:
    h = V[k]["H1"]
    rows.append((f"{NAME[k]} (retail)", h["pooled_rtp"], h["ci"], COL[k]))
rows.sort(key=lambda r: r[1])
fig, ax = plt.subplots(figsize=(6.3, 3.0))
for i, (lbl, v, ci, col) in enumerate(rows):
    if ci:
        ax.errorbar(v, i, xerr=[[v - ci[0]], [ci[1] - v]], fmt="o", color=col, ms=5, capsize=3, lw=1.2)
    else:
        ax.plot(v, i, "s", color=col, ms=4.5, mfc="white", mew=1.2)
    ax.text(1.031, i, f"{v:.4f}" if 0.9995 <= v < 1.0005 else f"{v:.3f}", va="center", fontsize=8, color=col)
ax.set_yticks(range(len(rows)), [r[0] for r in rows])
for t, r in zip(ax.get_yticklabels(), rows):
    t.set_color(r[3])
ax.axvline(1, color="black", lw=0.6, ls=":")
ax.set_xlim(0.70, 1.03)
ax.set_xlabel("Return to player (cash out / cash in), with 95% day-cluster bootstrap interval")
save(fig, "fig1_rtp")

# Figure 2: share of wallets ahead vs stake-matched gamblers
fig, axs = plt.subplots(1, 3, figsize=(6.3, 2.6), sharex=True)
for ax, k in zip(axs, V):
    h2 = V[k]["H2"]
    labs = ["Retail wallets"] + [lbl for _, lbl in GAMES]
    vals = [V[k]["H2_retail_share_ahead"]] + [v["gamblers_ahead"] for v in h2.values()]
    cols = [COL[k]] + [GREY] * 6
    y = np.arange(len(vals))[::-1]
    ax.barh(y, vals, color=cols, height=0.7)
    for yi, v in zip(y, vals):
        ax.text(v + 0.01, yi, f"{100 * v:.1f}%", va="center", fontsize=7)
    ax.set_yticks(y, labs if k == "meme" else [""] * len(labs))
    ax.set_title(NAME[k], color=COL[k])
    ax.set_xlim(0, 0.62)
    ax.xaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1, decimals=0))
axs[1].set_xlabel("Share of wallets ending the window ahead")
fig.tight_layout()
save(fig, "fig2_ahead")

# Figure 3: survival curves P(X >= x)
s1 = J("results")
s2 = J("s2_results")
floor = 1e-7
curves = [("Random memecoin bets (Study 1)", s1["primary"]["surv"], COL["meme"], "-", "o"),
          ("Retail memecoin positions", s2["H3_retail_position_shape"]["surv"], COL["meme"], "--", "^"),
          ("Plinko (16 rows, high)", surv_exact(plinko_16_high()), GREY, "-", "s"),
          ("Sweet Bonanza (measured points)", {m: SWEET_BONANZA["survival"][m] for m in SWEET_BONANZA["survival"]}, GREY, ":", "D"),
          ("European roulette (even money)", surv_exact(roulette(37)["even-money"]), "black", "-", "x")]
fig, ax = plt.subplots(figsize=(6.3, 3.2))
for lbl, sv, col, ls, mk in curves:
    xs = sorted(int(x) for x in sv)
    ys = [max(float(sv[str(x)] if str(x) in sv else sv[x]), floor) for x in xs]
    ax.plot(xs, ys, ls=ls, marker=mk, color=col, label=lbl, ms=4, lw=1.2, mfc="white" if col == GREY else col)
ax.set_xscale("log"); ax.set_yscale("log")
ax.set_ylim(floor * 0.6, 1)
ax.axhline(floor, color=GREY, lw=0.5)
ax.text(2.05, floor * 1.4, "zero", fontsize=7, color=GREY)
ax.set_xticks(GRID, [f"{g:,}" for g in GRID])
ax.set_xlabel("Multiple of stake, x")
ax.set_ylabel("P(payout multiple $\\geq$ x)")
ax.legend(fontsize=7.5, loc="upper right")
save(fig, "fig3_survival")

# Figure 4: skill (H4) and persistence (H5)
fig = plt.figure(figsize=(6.3, 4.9), layout="constrained")
top, bot = fig.subfigures(2, 1, height_ratios=[1, 1.15], hspace=0.06)
top.suptitle("Skill (H4): wallets with ROI above their own null's 95th percentile", fontsize=9)
ax = top.subplots(1, 3, gridspec_kw={"width_ratios": [1, 1.6, 1]})[1]
for a in top.axes:
    if a is not ax:
        a.set_axis_off()
for i, k in enumerate(V):
    h4 = V[k]["H4"]
    v = h4["share_above_null95"]
    ax.errorbar(i, v, yerr=[[v - h4["ci"][0]], [h4["ci"][1] - v]], fmt="o", color=COL[k], ms=5, capsize=3)
ax.axhline(0.05, color="black", ls="--", lw=0.8)
ax.text(2.45, 0.052, "5% by chance", fontsize=7, ha="right", va="bottom")
ax.set_xticks(range(3), ["Memecoins", "Hyperliquid", "Polymarket"])
for t, k in zip(ax.get_xticklabels(), V):
    t.set_color(COL[k])
ax.set_xlim(-0.5, 2.5)
ax.set_ylim(0, 0.12)
ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1, decimals=0))
bot.suptitle("Persistence (H5): second-half RTP, wallets ranked on the first half", fontsize=9)
axs = bot.subplots(1, 3)
for ax, k in zip(axs, V):
    h5 = V[k]["H5"]
    ax.plot([0], [h5["top_decile_half2_rtp"]], "o", color=COL[k], ms=6)
    ax.plot([1], [h5["others_half2_rtp"]], "o", color=GREY, ms=6)
    ax.plot([-0.25, 0.25], [h5["top_decile_null_97.5"]] * 2, color="black", lw=1.5)
    lo = min(h5["others_half2_rtp"], h5["top_decile_half2_rtp"], h5["top_decile_null_97.5"])
    hi = max(h5["others_half2_rtp"], h5["top_decile_half2_rtp"], h5["top_decile_null_97.5"])
    pad = (hi - lo) * 0.35
    ax.set_ylim(lo - pad, hi + pad)
    ax.set_xlim(-0.6, 1.6)
    ax.set_xticks([0, 1], ["Top decile", "Others"])
    ax.set_title({"meme": "Memecoins", "hl": "Hyperliquid", "pm": "Polymarket"}[k], color=COL[k])
axs[0].annotate("null 97.5%", (0.25, V["meme"]["H5"]["top_decile_null_97.5"]), xytext=(0.45, V["meme"]["H5"]["top_decile_null_97.5"] - 0.06),
                fontsize=6.5, arrowprops={"arrowstyle": "-", "lw": 0.5})
save(fig, "fig4_skill")

# Figure 5: RTP by stake decile (exploratory)
ex = J("s3_explore_cuts")
fig, ax = plt.subplots(figsize=(6.3, 2.6))
for k, key in (("meme", "meme_by_stake_decile"), ("hl", "hl_by_stake_decile"), ("pm", "pm_by_stake_decile")):
    d = ex[key]
    ax.plot([r["decile"] for r in d], [r["rtp"] for r in d], "o-", color=COL[k], label=NAME[k], ms=4)
ax.axhline(1, color="black", lw=0.6, ls=":")
ax.set_xticks(range(1, 11))
ax.set_xlabel("Position stake decile within venue (1 = smallest)")
ax.set_ylabel("Pooled RTP")
ax.legend(fontsize=7.5, loc="lower right")
save(fig, "fig5_stake_deciles")
print("figures written to", OUT)
