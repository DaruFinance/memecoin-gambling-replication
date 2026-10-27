"""Study 2 analysis: retail position outcomes on pump.fun against casino games (Study 2 registration, Amendments S2-1 and
S2-2, Errata S2-E1 to S2-E3).

Steps: merge the position parts from s2_positions.py; classify positions (creator, launch block, creator-distributed,
protocol, atomic or high-frequency bot, oversold); liquidate residual tokens at the end of the data on the curve or in the
pool; H3 shape statistics of retail position multiples; H1 pooled retail RTP against each game with a launch-day bootstrap
and Holm adjustment; H2 share of wallets ending ahead against stake-matched gamblers drawn from exact game laws (200 draws
per wallet) with a wallet bootstrap of the paired difference and Holm adjustment; net SOL flow by class; the registered
robustness rows and the Amendment S2-1 funding-sample check. A hypothesis holds when the upper 97.5% bound lies below the
game, i.e. the Holm-adjusted one-sided p is below 0.025.

Reads from DATA: s2_parts_v4 and s2_swap_v4 (override with S2_PARTS and S2_SWAP_PARTS), s2_txcost_hourly.parquet,
hf_pumpfun_v2 create and transfer events, _mints.parquet, nonsol_mints.parquet (tokens whose curve is not quoted in SOL,
left out), dune_state/final_pool_state.csv and final_pool_state_extra.csv, bets_v2.parquet, grad_inferred.parquet
(graduations without a recorded event, from final_inputs.py and dune_final_inputs.py), tsec.parquet,
trade_coverage_daily.csv, s2_funding_links.csv and s2_funding_sample_mints.csv.
Writes DATA/s2_positions.parquet and AGG/s2_results.json.

Run: python s2_analyze.py  (one thread, 1.5 GB DuckDB memory limit).
"""
import json
import os
import sys

import duckdb
import numpy as np
import pandas as pd

from casino_games import SWEET_BONANZA, baccarat, craps_pass, plinko_16_high, roulette
from stats_and_ruling import stats
from config import AGG, DATA, TMP

D = DATA
R = f"{D}/hf_pumpfun_v2"
P = os.environ.get("S2_PARTS", f"{D}/s2_parts_v4")
PS = os.environ.get("S2_SWAP_PARTS", f"{D}/s2_swap_v4")
HOURLY = f"{D}/s2_txcost_hourly.parquet"
END = "2026-09-18 15:17:00+00"
PROTOCOL = "BwWK17cbHxwWBKZkUYvzxLcNQ1YVyaFezduWbtm2de6s"
RNG = np.random.default_rng(20260924)

import os
DBF = f"{TMP}/s2_analyze.duckdb"  # file-backed so the position table can spill to disk
for f_ in (DBF, DBF + ".wal"):
    if os.path.exists(f_):
        os.remove(f_)
c = duckdb.connect(DBF)
c.sql(f"set threads=1; set memory_limit='1500MB'; set temp_directory='{TMP}/s2_analyze_{os.getpid()}'; set TimeZone='UTC'; set preserve_insertion_order=false")

c.sql(f"""
create table pos as
with cu as (select mint, w, paid, tok_in, n_buy, recv, tok_out, n_sell, cost_buy, cost_sell, 0.0 cost_buy_pool, 0.0 cost_sell_pool,
                   first_buy_slot, first_t, last_t,
                   0 as any_atomic, n_buy as n_buy_curve, n_sell as n_sell_curve from read_parquet('{P}/curve_*.parquet')),
     sw as (select mint, w, paid, tok_in, n_buy, recv, tok_out, n_sell, 0.0 cost_buy, 0.0 cost_sell, cost_buy cost_buy_pool,
                   cost_sell cost_sell_pool, NULL::bigint first_buy_slot,
                   first_t, last_t, any_atomic, 0 n_buy_curve, 0 n_sell_curve from read_parquet('{PS}/swap_*.parquet'))
select mint, w, sum(paid) paid, sum(tok_in) tok_in, sum(n_buy) n_buy, sum(recv) recv, sum(tok_out) tok_out, sum(n_sell) n_sell,
       sum(cost_buy) cost_buy_curve, sum(cost_sell) cost_sell_curve, sum(cost_buy_pool) cost_buy_pool, sum(cost_sell_pool) cost_sell_pool,
       sum(n_buy) - sum(n_buy_curve) n_buy_pool, sum(n_sell) - sum(n_sell_curve) n_sell_pool,
       min(first_buy_slot) first_buy_slot, min(first_t) first_t, max(last_t) last_t, max(any_atomic) any_atomic
from (select * from cu union all select * from sw)
where mint not in (select mint from read_parquet('{D}/nonsol_mints.parquet'))
group by mint, w
""")
print("positions:", c.sql("select count(*) from pos").fetchone()[0], flush=True)

# Bot activity: only (wallet, hour) pairs that can exceed 50 trades per hour are summed. An hour can sit in up to about 8
# parts, so a total above 50 needs some part above 6 (Erratum S3-E2).
c.sql(f"""create table act as
  with parts as (select hash(w) hw, hr, n from read_parquet('{P}/act_curve_*.parquet')
                 union all select hash(w) hw, hr, n from read_parquet('{PS}/act_swap_*.parquet')),
  cand as (select distinct hw, hr from parts where n > 6)
  select hw, max(tot) max_hour from (select p.hw, p.hr, sum(p.n) tot from parts p semi join cand using (hw, hr) group by all) group by hw""")
c.sql(f"""create table atomic_w as select distinct hash(w) hw from read_parquet('{PS}/act_swap_*.parquet') where atomic = 1
          union select distinct hash(w) from pos where any_atomic = 1""")
c.sql(f"""create table tok as
  with cr as (select distinct on (payload_hash) mint, creator, tx_signer, slot, tx_index, block_time from read_parquet('{R}/*/*/create/*.parquet'))
  select mint, arg_min(creator, (slot, tx_index)) creator, arg_min(tx_signer, (slot, tx_index)) signer, min(slot) cslot, min(block_time) t_create from cr
  where mint in (select mint from read_parquet('{D}/_mints.parquet')) group by mint""")
c.sql(f"""create table dist as select distinct t.token_address mint, t.to_wallet w
  from read_parquet('{R}/*/*/other/*.parquet') t join tok on tok.mint = t.token_address and t.from_wallet = tok.creator
  where t.block_time between tok.t_create - interval 72 hour and tok.t_create + interval 24 hour""")
c.sql(f"""create table dist7 as select distinct t.token_address mint, t.to_wallet w
  from read_parquet('{R}/*/*/other/*.parquet') t join tok on tok.mint = t.token_address and t.from_wallet = tok.creator
  where t.block_time between tok.t_create - interval 7 day and tok.t_create + interval 7 day""")
c.sql(f"""create table cls as
  select p.*, tok.t_create,
    (p.w = tok.creator or p.w = tok.signer) is_creator,
    coalesce(p.first_buy_slot <= tok.cslot + 2, false) is_launch_block,
    (d.w is not null) is_distributed, (d7.w is not null) is_distributed7,
    (p.w = '{PROTOCOL}') is_protocol,
    (aw.hw is not null) is_atomic_bot,
    coalesce(a.max_hour, 0) max_hour
  from pos p join tok using (mint) left join dist d on d.mint = p.mint and d.w = p.w left join dist7 d7 on d7.mint = p.mint and d7.w = p.w
  left join atomic_w aw on aw.hw = hash(p.w) left join act a on a.hw = hash(p.w)""")

fps = pd.concat([pd.read_csv(f"{D}/dune_state/final_pool_state.csv"),
                 pd.read_csv(f"{D}/dune_state/final_pool_state_extra.csv")], ignore_index=True)
fps["rb"] = pd.to_numeric(fps.b, errors="coerce"); fps["rqe"] = pd.to_numeric(fps.p, errors="coerce") * fps.rb; fps["fs"] = fps.f / 1e5
c.register("fps", fps[["pool", "rb", "rqe", "fs"]])
c.sql(f"""create table bpool as select b.mint, coalesce(b.pool, g.pool) pool, coalesce(b.t_grad::timestamptz, g.t_grad) t_grad
          from (select distinct mint, pool, t_grad from read_parquet('{D}/bets_v2.parquet')) b
          left join read_parquet('{D}/grad_inferred.parquet') g using (mint)""")
c.sql(f"""create table fcurve as select mint, arg_max(post_vs, sec) vs, arg_max(post_vt, sec) vt from read_parquet('{D}/tsec.parquet')
          where sec <= TIMESTAMPTZ '{END}' group by mint""")
# Creator fee (basis points) of the last fee-paying trade at or before END, the trade that sets the final curve state (S2-E3).
c.sql(f"""create table mcb as select mint, arg_max(post_cb, sec) cb from read_parquet('{D}/tsec.parquet')
          where sec <= TIMESTAMPTZ '{END}' and post_cb is not null group by mint""")
# The end-of-data liquidation pays the median transaction cost of END's hour (Erratum S2-E2).
txc_fall = c.sql(f"select median(lam) from read_parquet('{HOURLY}')").fetchone()[0]
txc = c.sql(f"""select coalesce((select lam from read_parquet('{HOURLY}') where hr = date_trunc('hour', TIMESTAMPTZ '{END}')), {txc_fall})""").fetchone()[0]
c.sql(f"""create table xp as
  select k.*,
    greatest(k.tok_in - k.tok_out, 0) resid, (k.tok_out > k.tok_in) oversold,
    case when bp.t_grad is not null and bp.t_grad <= TIMESTAMPTZ '{END}' then
           fps.rqe * greatest(k.tok_in - k.tok_out, 0) / (fps.rb + greatest(k.tok_in - k.tok_out, 0)) * (1 - fps.fs)
         else (fc.vs - fc.vs * fc.vt / (fc.vt + greatest(k.tok_in - k.tok_out, 0))) * (1 - (95 + coalesce(mcb.cb, 0)) / 1e4) end liq_gross
  from cls k left join bpool bp using (mint) left join fps on fps.pool = bp.pool left join fcurve fc using (mint) left join mcb using (mint)""")
c.sql(f"""create table px as select *,
    case when resid > 0 and coalesce(liq_gross, 0) > {txc} then liq_gross - {txc} else 0 end liq_net,
    paid + cost_buy_curve + cost_buy_pool cash_in,
    recv - cost_sell_curve - cost_sell_pool cash_out_trades
  from xp""")
c.sql("create table fin as select *, cash_out_trades + liq_net cash_out, (cash_out_trades + liq_net) / cash_in x from px where cash_in > 0")
c.sql(f"copy fin to '{D}/s2_positions.parquet' (format parquet)")


def retail_filter(bot_thresh=200, dist="is_distributed"):
    return (f"not is_creator and not is_launch_block and not {dist} and not is_protocol and not is_atomic_bot "
            f"and max_hour <= {bot_thresh} and not oversold")


res = {"tx_cost_end_hour_lamports": txc, "tx_cost_hourly_fallback_lamports": txc_fall}
res["counts"] = c.sql(f"""select count(*) positions, sum(oversold::int) oversold, sum(is_creator::int) creator,
    sum(is_launch_block::int) launch_block, sum(is_distributed::int) distributed, sum(is_protocol::int) protocol,
    sum(is_atomic_bot::int) atomic_bot, sum((max_hour > 200)::int) hour_bot, sum(({retail_filter()})::int) retail,
    count(distinct w) filter (where {retail_filter()}) retail_wallets from fin""").df().iloc[0].to_dict()
print(res["counts"], flush=True)

res["net_flow_by_class_SOL"] = c.sql(f"""select
    case when is_protocol then 'protocol' when is_creator then 'creator' when is_distributed then 'creator-distributed'
         when is_launch_block then 'launch-block' when is_atomic_bot or max_hour > 200 then 'bot'
         when oversold then 'oversold (no cost basis)' else 'retail' end cls,
    count(*) positions, count(distinct w) wallets, sum(cash_in)/1e9 sol_in, sum(cash_out)/1e9 sol_out, (sum(cash_out) - sum(cash_in))/1e9 net_sol,
    sum(cash_out)/sum(cash_in) rtp
  from fin group by 1 order by net_sol""").df().to_dict(orient="records")
print(pd.DataFrame(res["net_flow_by_class_SOL"]).to_string(index=False), flush=True)

cov = pd.read_csv(f"{D}/trade_coverage_daily.csv")
c.register("goodday", pd.DataFrame({"d": cov.loc[cov.trade_cov >= 0.97, "d"].astype(str)}))
c.sql(f"""create table ret as select (dense_rank() over (order by w))::int wid, cash_in, cash_out,
    date_diff('day', DATE '2026-08-01', t_create::date)::smallint dayi,
    (first_t - t_create) > interval 1 hour after1h, (liq_net = 0) noliq,
    strftime(t_create, '%Y-%m-%d') in (select d from goodday) okday
  from fin where {retail_filter()}""")
ret = c.sql("select wid, cash_in, cash_out, dayi from ret order by wid").df()
_x = ret.cash_out.values / ret.cash_in.values
res["H3_retail_position_shape"] = {k: v for k, v in stats(_x).items()}
del _x
pooled = ret.cash_out.sum() / ret.cash_in.sum()
res["H1_pooled_retail_rtp"] = pooled
g = ret.groupby("dayi")[["cash_in", "cash_out"]].sum()
k = len(g)
bs = []
for _ in range(10_000):
    w = np.bincount(RNG.integers(0, k, k), minlength=k)
    bs.append((g.cash_out.values * w).sum() / (g.cash_in.values * w).sum())
lo, hi = np.percentile(bs, [2.5, 97.5])
res["H1_ci_day"] = [lo, hi]
GAMES = {"European roulette even money": roulette(37)["even-money"], "American roulette even money": roulette(38)["even-money"],
         "Craps pass line": craps_pass(), "Baccarat banker": baccarat()["banker"], "Plinko 16 high": plinko_16_high()}
game_rtp = {n: float(sum(float(v) * float(p) for v, p in d.items())) for n, d in GAMES.items()}
game_rtp["Sweet Bonanza (published)"] = 0.965
bsa = np.array(bs)
pvals = {n: float((bsa >= r).mean()) for n, r in game_rtp.items()}  # one-sided bootstrap p: P(RTP_retail >= RTP_game)
order = sorted(pvals, key=pvals.get)
holm, m = {}, len(order)
for i, n in enumerate(order):
    holm[n] = min(1.0, max(pvals[n] * (m - i), max([holm[o] for o in order[:i]], default=0)))
res["H1"] = {n: {"game_rtp": game_rtp[n], "p_boot": pvals[n], "p_holm": holm[n], "holds": holm[n] < 0.025} for n in game_rtp}

wl = ret.groupby("wid").agg(cin=("cash_in", "sum"), cout=("cash_out", "sum"), n=("cash_in", "size"))
wl["ahead"] = wl.cout > wl.cin
res["retail_wallets"] = int(len(wl))
res["share_wallets_ahead"] = float(wl.ahead.mean())
res["wallet_positions_quantiles"] = wl.n.quantile([.5, .9, .99]).tolist()


def sb_law():
    """Approximate Sweet Bonanza law: published survival points and P(0), with two mid-bin values set to the 0.965 RTP."""
    p0 = 0.57063
    tail = {14.0: 0.007445, 40.0: 0.00153, 200.0: 0.000727, 700.0: 0.0000231, 1500.0: 0.00000232}
    rest = 1 - p0 - sum(tail.values())
    lo_mass, hi_mass = 0.255 / (0.255 + 0.165) * rest, 0.165 / (0.255 + 0.165) * rest
    tail_rtp = sum(v * p for v, p in tail.items())
    # v_lo in (0, 1) is fixed at 0.4; v_hi in [1, 10) solves RTP = 0.965
    v_lo = 0.4
    v_hi = (0.965 - tail_rtp - v_lo * lo_mass) / hi_mass
    law = {0.0: p0, v_lo: lo_mass, v_hi: hi_mass, **tail}
    return law


GAMES_H2 = dict(GAMES)
GAMES_H2["Sweet Bonanza (approx. law)"] = sb_law()
res["sweet_bonanza_approx_law"] = {str(k): v for k, v in GAMES_H2["Sweet Bonanza (approx. law)"].items()}
ret_sorted = ret
wid = ret_sorted.wid.values
starts = np.r_[0, np.flatnonzero(wid[1:] != wid[:-1]) + 1]
stakes = ret_sorted.cash_in.values
tot = np.add.reduceat(stakes, starts)
DRAWS = 200
bw = np.unique(np.searchsorted(starts, np.arange(0, len(stakes), 4_000_000), side="left"))
bw = bw[bw < len(starts)]
blocks = [(starts[wa], starts[wb] if wb < len(starts) else len(stakes), wa, wb) for wa, wb in zip(bw, list(bw[1:]) + [len(starts)])]
h2, pg = {}, {}
for name, law in GAMES_H2.items():
    vals = np.array([float(v) for v in law.keys()]); probs = np.array([float(p) for p in law.values()]); probs /= probs.sum()
    cdf = np.cumsum(probs)
    ahead = np.zeros(len(starts), dtype=np.uint16)
    for d in range(DRAWS):
        for pa, pb, wa, wb in blocks:
            u = RNG.random(pb - pa)
            o = vals[np.searchsorted(cdf, u, side="right").clip(0, len(vals) - 1)]
            ahead[wa:wb] += np.add.reduceat(stakes[pa:pb] * o, starts[wa:wb] - pa) > tot[wa:wb]
    pg[name] = (ahead / DRAWS).astype(np.float32)
    h2[name] = {"share_gamblers_ahead": float(pg[name].mean(dtype=np.float64))}
    print("H2", name, h2[name], flush=True)
# Wallet bootstrap of the paired difference: wallet ahead minus its stake-matched gambler's P(ahead) (S2-E3).
ra = wl.sort_index().ahead.values.astype(np.float32)
res["H2"] = h2
res["H2_retail_share_ahead"] = float(ra.mean(dtype=np.float64))
nW = len(ra)
diffs = {n: [] for n in h2}
dd = {n: ra - pg[n] for n in h2}
for _ in range(1000):
    idx = RNG.integers(0, nW, nW)
    for n in h2:
        diffs[n].append(float(dd[n][idx].mean(dtype=np.float64)))
del dd
pv = {n: float((np.array(diffs[n]) >= 0).mean()) for n in h2}
order = sorted(pv, key=pv.get)
holm2 = {}
for i, n in enumerate(order):
    holm2[n] = min(1.0, max(pv[n] * (len(order) - i), max([holm2[o] for o in order[:i]], default=0)))
for n in h2:
    res["H2"][n].update({"diff_ci": list(np.percentile(diffs[n], [2.5, 97.5])), "p_holm": holm2[n], "holds": holm2[n] < 0.025})

rob = {}
q = lambda where: c.sql(f"select sum(cash_out)/sum(cash_in) from ret where {where}").fetchone()[0]
rob["opened >1h after launch"] = q("after1h")
for lo_, hi_ in ((0, .1), (.1, 1), (1, 10), (10, 1e9)):
    rob[f"stake {lo_}-{hi_} SOL"] = q(f"cash_in >= {lo_ * 1e9} and cash_in < {hi_ * 1e9}")
rob["launch day coverage >= 97%"] = q("okday")
rob["no liquidation value"] = q("noliq")
for th in (50, 1000):
    rob[f"bot rule > {th}/hour"] = c.sql(f"select sum(cash_out)/sum(cash_in) from fin where {retail_filter(th)}").fetchone()[0]
# The registered "creator-funded window +-7 days" row, applied to the creator-distributed rule of Amendment S2-1.
rob["creator-distributed window +-7 days"] = c.sql(
    f"select sum(cash_out)/sum(cash_in) from fin where {retail_filter(dist='is_distributed7')}").fetchone()[0]
fl = pd.read_csv(f"{D}/s2_funding_links.csv")[["mint", "w"]].drop_duplicates()
sm = pd.read_csv(f"{D}/s2_funding_sample_mints.csv")[["mint"]]
c.register("flinks", fl); c.register("fsample", sm)
s21 = c.sql(f"""select
    sum(cash_out) / sum(cash_in) rtp_as_registered,
    sum(case when l.w is null then cash_out end) / sum(case when l.w is null then cash_in end) rtp_funded_removed,
    avg((l.w is not null)::int) share_positions_reclassified,
    sum(case when l.w is not null then cash_in else 0 end) / sum(cash_in) share_sol_reclassified
  from fin f left join flinks l on l.mint = f.mint and l.w = f.w
  where f.mint in (select mint from fsample) and {retail_filter()}""").df().iloc[0].to_dict()
sd = c.sql(f"""select date_diff('day', DATE '2026-08-01', f.t_create::date) dayi, sum(cash_in) ci, sum(cash_out) co,
    sum(case when l.w is null then cash_in else 0 end) ci_r, sum(case when l.w is null then cash_out else 0 end) co_r
  from fin f left join flinks l on l.mint = f.mint and l.w = f.w
  where f.mint in (select mint from fsample) and {retail_filter()} group by 1 order by 1""").df()
kd = len(sd)
bsa2 = {"as_registered": np.empty(10_000), "funded_removed": np.empty(10_000)}
for i in range(10_000):
    w = np.bincount(RNG.integers(0, kd, kd), minlength=kd)
    bsa2["as_registered"][i] = (sd.co.values * w).sum() / (sd.ci.values * w).sum()
    bsa2["funded_removed"][i] = (sd.co_r.values * w).sum() / (sd.ci_r.values * w).sum()
for key, arr in bsa2.items():
    pv_ = {n: float((arr >= r).mean()) for n, r in game_rtp.items()}
    od = sorted(pv_, key=pv_.get); hm = {}
    for i, n in enumerate(od):
        hm[n] = min(1.0, max(pv_[n] * (len(od) - i), max([hm[o] for o in od[:i]], default=0)))
    s21[f"H1_{key}"] = {"ci": list(np.percentile(arr, [2.5, 97.5])), "holds": {n: hm[n] < 0.025 for n in game_rtp}}
res["S2_1_funding_sample_check"] = s21
res["robustness_pooled_rtp"] = rob
res["claim_tiers"] = {
    "a_worse_than_european_roulette": res["H1"]["European roulette even money"]["holds"] and res["H2"]["European roulette even money"]["holds"],
    "b_worse_than_slot": res["H1"]["Sweet Bonanza (published)"]["holds"] and res["H2"]["Sweet Bonanza (approx. law)"]["holds"],
    "c_worse_than_every_game": all(v["holds"] for v in res["H1"].values()) and all(v["holds"] for v in res["H2"].values()),
}
json.dump(res, open(f"{AGG}/s2_results.json", "w"), indent=1, default=float)
print(json.dumps({k: res[k] for k in ["counts", "H1_pooled_retail_rtp", "H1_ci_day", "H1", "retail_wallets",
                                      "H2_retail_share_ahead", "H2", "robustness_pooled_rtp", "claim_tiers"]}, indent=1, default=float))
