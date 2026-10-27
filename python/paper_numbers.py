"""Every number quoted in the paper, computed from the aggregated results in results/aggregates.
Writes results/tables/paper_numbers.csv (name, value); the names match the placeholders used in the paper's source."""
import json
import math

import pandas as pd

from config import AGG, TABLES

D = AGG
OUT = f"{TABLES}/paper_numbers.csv"
J = lambda f: json.load(open(f"{D}/{f}.json"))
lines = []


def m(name, val):
    lines.append((name, str(val).replace("\\%", "%").replace("~", " ").replace("\\$", "$")))


def n0(x):
    return f"{int(round(x)):,}"


def f3(x):
    return f"{x:.3f}"


def pct(x, d=1):
    return f"{100 * x:.{d}f}\\%"


def one_in(p):
    v = 1 / p
    if v >= 1e6:
        return f"{v / 1e6:.1f}~million"
    return f"{round(v, -3):,.0f}" if v >= 1e4 else f"{round(v):,.0f}"


GAMES = ["European roulette", "American roulette", "Craps pass line", "Baccarat banker", "Plinko 16 high", "Sweet Bonanza"]

# Study 3 venues
for key, pre in (("meme", "Meme"), ("hl", "Hl"), ("pm", "Pm")):
    r = J(f"s3_results_{key}")
    dec = 4 if key == "hl" else 3
    fx = lambda x: f"{x:.{dec}f}"
    m(pre + "Pos", n0(r["positions"])); m(pre + "Wal", n0(r["wallets"]))
    m(pre + "RTP", fx(r["H1"]["pooled_rtp"])); m(pre + "Lo", fx(r["H1"]["ci"][0])); m(pre + "Hi", fx(r["H1"]["ci"][1]))
    m(pre + "HoneN", sum(v["holds"] for v in r["H1"]["vs_games"].values()))
    m(pre + "Ahead", pct(r["H2_retail_share_ahead"]))
    ga = [v["gamblers_ahead"] for v in r["H2"].values()]
    m(pre + "GamMin", pct(min(ga))); m(pre + "GamMax", pct(max(ga)))
    m(pre + "HtwoN", sum(v["holds"] for v in r["H2"].values()))
    for g, (gk, v) in zip(["ER", "AR", "CR", "BA", "PL", "SB"], r["H2"].items()):
        m(pre + "Gam" + g, pct(v["gamblers_ahead"]))
    h4 = r["H4"]
    m(pre + "HfourWal", n0(h4["wallets_ge10"])); m(pre + "Hfour", pct(h4["share_above_null95"]))
    m(pre + "HfourLo", pct(h4["ci"][0])); m(pre + "HfourHi", pct(h4["ci"][1]))
    h5 = r["H5"]
    m(pre + "HfiveWal", n0(h5["wallets_both_halves"]))
    m(pre + "Rho", f"{h5['spearman']:.3f}"); m(pre + "RhoNull", f"{h5['null_97.5']:.3f}")
    m(pre + "Top", fx(h5["top_decile_half2_rtp"])); m(pre + "TopNull", fx(h5["top_decile_null_97.5"]))
    m(pre + "Others", fx(h5["others_half2_rtp"]))

c = J("casino_games")
m("RtpER", f3(c["European roulette, even-money"]["rtp"])); m("RtpAR", f3(c["American roulette, even-money"]["rtp"]))
m("RtpCR", f3(c["Craps, pass line"]["rtp"])); m("RtpBA", f3(c["Baccarat (8 decks), banker"]["rtp"]))
m("RtpPL", f3(c["Plinko 16 rows, high risk (Rainbet)"]["rtp"])); m("RtpSB", "0.965")
m("WinER", pct(c["European roulette, even-money"]["win_rate"], 2))
m("PlinkoPhundred", one_in(c["Plinko 16 rows, high risk (Rainbet)"]["p_ge_100"]))
m("PlinkoJack", pct(c["Plinko 16 rows, high risk (Rainbet)"]["jackpot_share_ge100"]))

# Study 1
s1 = J("results")
p = s1["primary"]
m("SoneBets", n0(s1["counts"]["included"])); m("SoneTokens", n0(s1["counts"]["tokens"])); m("SoneDays", s1["counts"]["launch_days"])
m("SoneRTP", f3(p["rtp"])); m("SoneMedian", f3(p["median"])); m("SoneWin", pct(p["win_rate"], 2))
m("SoneRTPLo", f3(s1["primary_ci_day"]["rtp"][0])); m("SoneRTPHi", f3(s1["primary_ci_day"]["rtp"][1]))
m("SonePhundred", one_in(p["p_ge_100"])); m("SoneJack", pct(p["jackpot_share"])); m("SoneHill", f"{p['hill_above_10']:.2f}")
m("SoneThousandN", n0(p["p_ge_1000"] * s1["counts"]["included"]))
rd = s1["ruling"]
m("SoneDistSB", f"{rd['distances_slot']['Sweet Bonanza']:.2f}"); m("SoneDistPL", f"{rd['distances_slot']['Plinko 16 high']:.2f}")
m("SoneDistBTC", f"{rd['distances_table']['BTC control']:.2f}"); m("SoneDistER", f"{rd['distances_table']['European roulette even money']:.2f}")
m("SoneBTC", f3(s1["control::BTC"]["rtp"])); m("SoneBTCWin", pct(s1["control::BTC"]["win_rate"]))
m("SoneMayhemN", n0(s1["split::mayhem"]["n"]))
# Erratum 3: tokens not quoted in SOL (share of the census, K = 5 bets per token) and graduations dated without an event
m("NonSolShare", pct(s1["counts"]["tokens_nonsol_curve"] / (s1["counts"]["bets"] / 5)))
m("GradInferredN", n0(J("census_counts")["graduations_inferred"]))
m("SoneGapOffRTP", f3(s1["robustness_unregistered::gap_rule_off"]["rtp"]))
m("SoneExtGapRTP", f3(s1["robustness_unregistered::gap_rule_all_outages_over_5min"]["rtp"]))
jl = J("s1_replication_july")
m("JulyBets", n0(jl["counts"]["included"])); m("JulyTokens", n0(jl["counts"]["tokens"]))
m("JulyRTP", f3(jl["primary"]["rtp"])); m("JulyMedian", f3(jl["primary"]["median"]))
m("JulyWin", pct(jl["primary"]["win_rate"], 2)); m("JulyJack", pct(jl["primary"]["jackpot_share"], 2))

# Study 2 (memecoin position shape and robustness)
s2 = J("s2_results")
cnt = s2["counts"]
m("STwoAll", n0(cnt["positions"]))
h3 = s2["H3_retail_position_shape"]
m("PosMedian", f3(h3["median"])); m("PosWin", pct(h3["win_rate"])); m("PosMean", f3(h3["rtp"]))
m("PosPhundred", one_in(h3["p_ge_100"]))
m("PosVsPlinko", f'{round(c["Plinko 16 rows, high risk (Rainbet)"]["p_ge_100"] / h3["p_ge_100"], -1):.0f}')
m("MemeHundred", f'{round(100 * s2["H1_pooled_retail_rtp"]):.0f}'); m("PosJack", pct(h3["jackpot_share"]))
rb = s2["robustness_pooled_rtp"]
m("RobAfterOneH", f3(rb["opened >1h after launch"])); m("RobCoverage", f3(rb["launch day coverage >= 97%"]))
m("RobBotFifty", f3(rb["bot rule > 50/hour"])); m("RobBotThousand", f3(rb["bot rule > 1000/hour"]))
m("RobStakeA", f3(rb["stake 0-0.1 SOL"])); m("RobStakeB", f3(rb["stake 0.1-1 SOL"]))
m("RobStakeC", f3(rb["stake 1-10 SOL"])); m("RobStakeD", f3(rb["stake 10-1000000000.0 SOL"]))
m("RobDistSeven", f3(rb["creator-distributed window +-7 days"]))
dr = J("s2_dune_full")      # unregistered: the Study 2 retail computation rebuilt from Dune's decoded events
dd = dr["dune_rebuild"]
m("DuneLaunches", n0(dr["dune_census"]["sol_curve_tokens"])); m("LocalLaunches", n0(s1["counts"]["bets"] / 5 - s1["counts"]["tokens_nonsol_curve"]))
m("DunePos", f"{dd['H3']['n'] / 1e6:.1f}~million"); m("DuneRTP", f3(dd["H1_pooled_retail_rtp"]))
m("DuneCI", f"{dd['H1_ci_day'][0]:.3f} to {dd['H1_ci_day'][1]:.3f}"); m("DuneAhead", pct(dd["share_wallets_ahead"]))
m("DuneOutageRTP", f3(dd["retail_rtp_launched_09_03_to_09_08"])); m("DuneGradMissing", n0(dr["dune_census"]["completed_without_local_pool"]))
assert all(dd["H1_holds"].values())
fs = s2["S2_1_funding_sample_check"]
m("FundShare", pct(fs["share_positions_reclassified"]))
b50 = J("s3_results_meme_bot50")
m("BotFiftyRTP", f3(b50["H1"]["pooled_rtp"])); m("BotFiftyHfour", pct(b50["H4"]["share_above_null95"]))
m("BotFiftyTop", f3(b50["H5"]["top_decile_half2_rtp"])); m("BotFiftyTopNull", f3(b50["H5"]["top_decile_null_97.5"]))
tr = J("s2_transfer_sensitivity")
m("TrPos", n0(tr["positions_affected"])); m("TrSol", n0(tr["liquidation_removed_sol"])); m("TrRTP", f"{tr['pooled_rtp_adjusted']:.4f}")
m("TrGrossPos", n0(tr["upper_bound_gross"]["positions_affected"])); m("TrGrossSol", n0(tr["upper_bound_gross"]["liquidation_removed_sol"]))
m("TrGrossRTP", f"{tr['upper_bound_gross']['pooled_rtp']:.4f}")

sec = J("s3_secondary")
mc = {r["cls"].split(" ", 1)[1]: r for r in sec["meme_classes_sol"]}
m("ClsCreator", f"{mc['creator']['rtp']:.2f}"); m("ClsCreatorWal", n0(mc["creator"]["wallets"]))
m("ClsCreatorNet", n0(mc["creator"]["net"]))
m("ClsLaunch", f"{mc['launch block']['rtp']:.2f}"); m("ClsLaunchWal", n0(mc["launch block"]["wallets"]))
m("ClsLaunchNet", n0(mc["launch block"]["net"]))
m("ClsInsider", f"{mc['distributed (insider)']['rtp']:.2f}"); m("ClsInsiderN", n0(mc["distributed (insider)"]["n"]))
m("ClsProtocol", f"{mc['protocol']['rtp']:.2f}")
m("ClsAtomic", f"{mc['atomic bot']['rtp']:.2f}"); m("ClsAtomicWal", n0(mc["atomic bot"]["wallets"]))
m("ClsHour", f"{mc['> 200 fills/hour']['rtp']:.2f}"); m("ClsHourWal", n0(mc["> 200 fills/hour"]["wallets"]))
m("ClsOversold", f"{mc['oversold (no cost basis)']['rtp']:.2f}"); m("ClsOversoldN", n0(mc["oversold (no cost basis)"]["n"]))
m("RetailNet", n0(-mc["retail"]["net"])); m("RetailIn", n0(mc["retail"]["cash_in"]))
hc = {r["cls"].split(" ", 1)[1]: r for r in sec["hl_classes_usd"]}
m("HlClsVault", f3(hc["vault"]["rtp"])); m("HlClsMM", f3(hc["market maker"]["rtp"])); m("HlClsHour", f3(hc["> 200 fills/hour"]["rtp"]))
m("HlClsPre", f3(hc["opened before window"]["rtp"]))
m("HlNet", f"{-hc['retail']['net'] / 1e6:.1f}"); m("HlIn", f"{hc['retail']['cash_in'] / 1e9:.1f}")
pc = {r["cls"].split(" ", 1)[1]: r for r in sec["pm_classes_usd"]}
m("PmClsMM", f3(pc["market maker"]["rtp"])); m("PmClsHour", f3(pc["> 200 fills/hour"]["rtp"]))
m("PmNet", f"{-pc['retail']['net'] / 1e6:.1f}"); m("PmIn", f"{pc['retail']['cash_in'] / 1e9:.2f}")
lv = sec["hl_levered_view_retail"][0]
m("HlLiqEp", pct(lv["episode_liquidation_rate"])); m("HlLiqStake", pct(lv["stake_share_liquidated"]))
m("HlLiqWal", pct(lv["wallet_share_ever_liquidated"]))
cb = sec["pm_combo_share"]
m("ComboShare", pct(cb["share_of_usd"])); m("ComboWal", n0(cb["combo_wallets"]))
nc = J("s3_results_pm_noconv")
m("NoconvRTP", f3(nc["H1"]["pooled_rtp"]))
fb = J("s3_results_pm_feb")
m("FebRTP", f3(fb["H1"]["pooled_rtp"])); m("FebAhead", pct(fb["H2_retail_share_ahead"])); m("FebHfour", pct(fb["H4"]["share_above_null95"]))
m("FebHtwoN", sum(v["holds"] for v in fb["H2"].values())); m("FebPos", n0(fb["positions"])); m("FebWal", n0(fb["wallets"]))
fnp = J("s3_results_pm_feb_nopairs")
m("FebNPRTP", f3(fnp["H1"]["pooled_rtp"])); m("FebNPHfour", pct(fnp["H4"]["share_above_null95"]))
ls = J("s3_results_listed")
m("ListedCoins", n0(ls["counts"]["coins"])); m("ListedBets", n0(ls["counts"]["priced"])); m("ListedInactive", n0(ls["counts"]["inactive_coins"]))
m("ListedRTP", f3(ls["cost_30bp"]["rtp"])); m("ListedLo", f3(ls["primary_ci_coin"]["rtp"][0])); m("ListedHi", f3(ls["primary_ci_coin"]["rtp"][1]))
m("ListedMedian", f3(ls["cost_30bp"]["median"])); m("ListedWin", pct(ls["cost_30bp"]["win_rate"])); m("ListedJack", pct(ls["cost_30bp"]["jackpot_share"]))
m("ListedDead", f3(ls["dead_pool_sensitivity"]["rtp"])); m("ListedSame", pct(ls["diagnostics"]["share_same_price_point"], 0))
m("ListedFresh", f3(ls["robustness_unregistered::fresh_prices_le1h"]["rtp"]))

ex = J("s3_explore_cuts")
md = ex["meme_by_stake_decile"]
m("MemeDecOne", f3(md[0]["rtp"])); m("MemeDecNine", f3(md[8]["rtp"])); m("MemeDecTen", f3(md[9]["rtp"]))
hd = [r["rtp"] for r in ex["hl_by_stake_decile"]]
m("HlDecMin", f3(min(hd))); m("HlDecMax", f3(max(hd)))
cat = {r["cat"]: r for r in ex["pm_by_category"]}
m("PmCatPolitics", f3(cat["politics"]["rtp"])); m("PmCatSports", f3(cat["sports"]["rtp"])); m("PmCatEsports", f3(cat["esports"]["rtp"]))
m("PmCatUpDown", f3(cat["crypto up/down (short-interval)"]["rtp"]))
m("PmCatUpDownShare", pct(cat["crypto up/down (short-interval)"]["positions"] / sum(r["positions"] for r in ex["pm_by_category"]), 0))
tp = J("s3_meme_topdecile_profile")
top = {r["top"]: r for r in tp["profile"]}
m("TopEntry", f"{top[True]['med_entry_delay_s'] / 60:.0f}"); m("OthEntry", f"{top[False]['med_entry_delay_s'] / 60:.0f}")
m("TopHold", f"{top[True]['med_hold_s']:.0f}"); m("OthHold", f"{top[False]['med_hold_s'] / 60:.0f}")
m("TopStake", f"{top[True]['med_stake_sol'] / 1e9:.2f}"); m("OthStake", f"{top[False]['med_stake_sol'] / 1e9:.2f}")
m("TopExit", pct(top[True]["share_fully_exited"], 0)); m("OthExit", pct(top[False]["share_fully_exited"], 0))
m("TopRTPAll", f3(top[True]["rtp_all"])); m("OthRTPAll", f3(top[False]["rtp_all"]))
m("TopWal", n0(top[True]["wallets"]))
hp = {r["top"]: r for r in J("s3_hl_topdecile_profile")["profile"]}
m("HlTopHold", f"{hp[True]['med_hold_s'] / 3600:.0f}"); m("HlOthHold", f"{hp[False]['med_hold_s'] / 60:.0f}")
m("HlTopMaker", pct(hp[True]["avg_maker_share"], 0)); m("HlOthMaker", pct(hp[False]["avg_maker_share"], 0))
m("HlTopBtc", pct(hp[True]["share_btc_eth"], 0)); m("HlOthBtc", pct(hp[False]["share_btc_eth"], 0))
m("HlTopLiq", pct(hp[True]["liquidation_rate"])); m("HlOthLiq", pct(hp[False]["liquidation_rate"]))
m("HlTopRTPAll", f3(hp[True]["rtp"])); m("HlOthRTPAll", f3(hp[False]["rtp"]))

# Unregistered checks: other DEX venues, dust, pool split, 2024 windows, walk-forward, costs, class intervals, callout rewards
av = J("s2_dune_allvenues")["all_venues"]
m("AllVenRTP", f3(av["H1_pooled_retail_rtp"])); m("AllVenCI", f"{av['H1_ci_day'][0]:.3f} to {av['H1_ci_day'][1]:.3f}")
m("AllVenAhead", pct(av["share_wallets_ahead"]))
ps = J("pool_split")
m("PoolRTP", f3(ps["touched_pool"]["rtp"])); m("CurveRTP", f3(ps["curve_only"]["rtp"]))
m("PoolLossShare", pct(ps["touched_pool"]["share_of_retail_loss"], 0))
m("NoDustRTP", f3(ps["no_dust"]["rtp"])); m("NoDustAhead", pct(ps["no_dust"]["share_wallets_ahead"]))
m("MemeAheadTenth", pct(ps["wallets_staking_0.1_sol_or_more"]["share_ahead"]))
wc = J("s3_wfo_costs")
m("MemeTxZero", f3(wc["costs"]["meme_tx_cost_x0"]["rtp"])); m("MemeTxTwo", f3(wc["costs"]["meme_tx_cost_x2"]["rtp"]))
m("HlFeeZero", f"{wc['costs']['hl_fees_x0']['rtp']:.4f}"); m("HlFeeTwo", f"{wc['costs']['hl_fees_x2']['rtp']:.4f}")
for key, pre in (("meme", "Meme"), ("hl", "Hl"), ("pm", "Pm")):
    wf = wc[f"wfo_{key}"]
    fx = (lambda x: f"{x:.4f}") if key == "hl" else f3
    m(pre + "WfoTop", fx(wf["pooled"]["top_oos_rtp"])); m(pre + "WfoNull", fx(wf["pooled"]["top_null_97.5"]))
    m(pre + "WfoOthers", fx(wf["pooled"]["others_oos_rtp"])); m(pre + "WfoFolds", int(wf["folds_above_null"]))
cc = J("s3_class_ci")
ld = cc["launch_day_clusters"]
m("ClsCreatorLo", f"{ld['creator']['ci'][0]:.2f}"); m("ClsCreatorHi", f"{ld['creator']['ci'][1]:.2f}")
m("ClsLaunchLo", f"{ld['launch block']['ci'][0]:.2f}"); m("ClsLaunchHi", f"{ld['launch block']['ci'][1]:.2f}")
m("MemeLoLaunch", f3(ld["retail"]["ci"][0])); m("MemeHiLaunch", f3(ld["retail"]["ci"][1]))
_h = J("s3_results_hl")["H4"]
m("HlHfourZ", f"{(_h['share_above_null95'] - 0.05) / math.sqrt(_h['share_above_null95'] * (1 - _h['share_above_null95']) / _h['wallets_ge10']):.0f}")
NAMES = {"meme": "memecoins", "hl": "Hyperliquid", "pm": "Polymarket"}
join = lambda xs: xs[0] if len(xs) == 1 else ", ".join(xs[:-1]) + " and " + xs[-1]
yes = [NAMES[k] for k in NAMES if wc[f"wfo_{k}"]["pooled"]["persistence"]]
no = [NAMES[k] for k in NAMES if not wc[f"wfo_{k}"]["pooled"]["persistence"]]
agree = all(wc[f"wfo_{k}"]["pooled"]["persistence"] == J(f"s3_results_{k}")["H5"]["top_decile_persistence"] for k in NAMES)
parts = ([f"beats its null on {join(yes)}"] if yes else []) + ([f"does not on {join(no)}"] if no else [])
m("WfoReading", ("As in H5, the" if agree else "Unlike H5 on some venues, the") + " out-of-sample top decile " + " and ".join(parts) + ".")
xs = J("retail_shape_stats")["meme"]
px26 = J("sol_usd_window")["mean"]
m("MemeWin", pct(xs["win_rate"])); m("MemeLostNinety", pct(xs["wallet_buckets"]["1 lost 90%+"]))
m("MemePhundredRet", one_in(xs["surv"]["100"])); m("SolUsdTwentySix", f"{px26:.0f}")
m("MemeSystem", f3(sum(r["cash_out"] for r in sec["meme_classes_sol"]) / sum(r["cash_in"] for r in sec["meme_classes_sol"])))
m("RetailNetUsd", f"{-mc['retail']['net'] * px26 / 1e9:.2f}")
y = J("dune_2024_summary")
for wn, pre in (("aug24", "Ya"), ("nov24", "Yn")):
    r, a = y[wn]["retail"], y[wn]["all_classes_sol"]
    m(pre + "Pos", n0(r["positions"])); m(pre + "Wal", n0(r["wallets"]))
    m(pre + "RTP", f3(r["rtp"])); m(pre + "Lo", f3(r["ci_day"][0])); m(pre + "Hi", f3(r["ci_day"][1]))
    m(pre + "Zero", f3(y[wn]["retail_rtp_grad_bags_zero"])); m(pre + "NoImp", f3(y[wn]["retail_rtp_grad_bags_no_impact"]))
    m(pre + "Ahead", pct(r["share_wallets_ahead"])); m(pre + "Win", pct(r["win_rate"]))
    m(pre + "Phundred", one_in(r["surv"]["100"])); m(pre + "LostNinety", pct(y[wn]["wallet_buckets"]["1 lost 90%+"]))
    m(pre + "LossSol", n0(r["loss_sol"])); m(pre + "LossUsd", f"{r['loss_usd'] / 1e6:,.0f}"); m(pre + "Sol", f"{y[wn]['sol_usd_mean']:.0f}")
    m(pre + "System", f3(a["co"] / a["ci"]))
    m(pre + "Creator", f3(y[wn]["by_class"]["creator"]["rtp"])); m(pre + "Launch", f3(y[wn]["by_class"]["launch-block"]["rtp"]))
    m(pre + "DustWal", f"{y[wn]['by_class']['dust']['wallets'] / 1e6:.0f}~million")
# Callout rewards are paid in USDC outside the trades and are not in any cash flow. Bound: the $300,000 a day that pump.fun
# called the baseline in October 2026, paid on every day from launch (13 August) to the end of the data (18 September).
import datetime as dt
cd = (dt.date(2026, 9, 18) - dt.date(2026, 8, 13)).days + 1
cap = 300_000 * cd
retail_usd = mc["retail"]["cash_in"] * px26
m("CalloutDays", cd); m("CalloutCap", f"{cap / 1e6:.1f}"); m("CalloutShare", pct(cap / retail_usd, 2))
m("CalloutRTP", f3(J("s3_results_meme")["H1"]["pooled_rtp"] + cap / retail_usd))

pd.DataFrame(lines, columns=["name", "value"]).to_csv(OUT, index=False)
print(f"{len(lines)} numbers -> {OUT}")
