"""Compute X for every Study 1 launch-tier bet (Study 1 registration: Pricing, Amendment 1 and its erratum, Erratum 2,
Readouts).

Local inputs in data/: bets_v2.parquet, entry_inputs.parquet, curve_points.parquet, curve_peak.parquet,
grad_inferred.parquet, nonsol_mints.parquet, pool_lookups_by_bet.parquet and its _extra version, s2_txcost_hourly.parquet.
Dune inputs in data/dune_state: the state chunks, extra_state.csv, entry_buyfee.csv, extra_buyfee.csv, pool_peak.csv and
extra_peak.csv.
Writes data/bets_x.parquet with X at $10, $100 and $1,000, horizon X, peak X, sensitivity X and exclusion flags and
data/curve_points_<role>.parquet as intermediate files. Every rule is the registered one; nothing is tuned.
Study 1 registration, Erratum 3: tokens whose curve is not quoted in SOL are excluded (flag nonsol); tokens that graduated
without a recorded graduation event take the inferred t_grad and pool, priced from the supplementary Dune pulls
(dune_final_inputs.py); the hourly tx cost is the deduplicated Study 2 table; an unregistered flag marks prices taken across
any curve-stream silence over 5 minutes.
Run: python3 assemble_x.py
"""
import glob

import duckdb
import numpy as np
import pandas as pd

from config import DATA

D = DATA
R = f"{D}/hf_pumpfun_v2"
STAKES = {"10": "stake10_lam", "100": "stake100_lam", "1000": "stake1000_lam"}
ROLES = ["t_exit", "t_h5m", "t_h1h", "t_h24h", "t_h7d"]

bets = pd.read_parquet(f"{D}/bets_v2.parquet")
gi = pd.read_parquet(f"{D}/grad_inferred.parquet")
gi["t_grad"] = pd.to_datetime(gi.t_grad, utc=True).dt.tz_localize(None)
bets = bets.merge(gi[["mint", "t_grad", "pool"]].rename(columns={"t_grad": "tg_i", "pool": "pool_i"}), on="mint", how="left")
bets["t_grad"] = bets.t_grad.fillna(bets.tg_i); bets["pool"] = bets["pool"].fillna(bets.pool_i)
bets = bets.drop(columns=["tg_i", "pool_i"])
NONSOL = set(pd.read_parquet(f"{D}/nonsol_mints.parquet").mint)
ei = pd.read_parquet(f"{D}/entry_inputs.parquet")
CPCOLS = ["bet_id", "vs", "vt", "rt", "cb", "gap_excluded", "gap_ext"]
_cp = {}


def rss():
    return f"RSS {int(open('/proc/self/status').read().split('VmRSS:')[1].split()[0]) / 1e6:.1f} GB"


_c = duckdb.connect()
_c.sql("set threads=1; set memory_limit='1GB'")
for _r in ["t_entry", "t_exit", "t_h5m", "t_h1h", "t_h24h", "t_h7d"]:     # one file per role to keep memory low
    _c.sql(f"copy (select {', '.join(CPCOLS)} from read_parquet('{D}/curve_points.parquet') where role = '{_r}') to '{D}/curve_points_{_r}.parquet' (format parquet)")
_c.close()


def cp_role(role):
    if role not in _cp:
        _cp.clear()
        _cp[role] = pd.read_parquet(f"{D}/curve_points_{role}.parquet").set_index("bet_id")
        print(role, "curve points loaded,", rss(), flush=True)
    return _cp[role]


cpk = pd.read_parquet(f"{D}/curve_peak.parquet")
lkb = pd.concat([pd.read_parquet(f"{D}/pool_lookups_by_bet.parquet")[["bet_id", "role", "lid"]],
                 pd.read_parquet(f"{D}/pool_lookups_by_bet_extra.parquet")[["bet_id", "role", "lid"]]], ignore_index=True)

# pool states: chunk files are ordered by lid with no lid column
ch = pd.read_csv(f"{D}/dune_state/chunks.tsv", sep="\t", header=None, names=["k", "a", "b", "cost", "bytes"]).sort_values("a")
parts = []
for r in ch.itertuples():
    s = pd.read_csv(f"{D}/dune_state/state_chunk{r.k}.csv")
    assert len(s) == r.b - r.a
    s["lid"] = np.arange(r.a, r.b)
    parts.append(s)
parts.append(pd.read_csv(f"{D}/dune_state/extra_state.csv"))
ps = pd.concat(parts, ignore_index=True)
ps["rb"] = pd.to_numeric(ps.b, errors="coerce")
ps["rqe"] = pd.to_numeric(ps.p, errors="coerce") * ps.rb
ps["fs"] = ps.f / 1e5
ps = ps[["lid", "rb", "rqe", "fs"]]
fee = pd.concat([pd.read_csv(f"{D}/dune_state/entry_buyfee.csv"), pd.read_csv(f"{D}/dune_state/extra_buyfee.csv")], ignore_index=True)
f = fee.fb / 1e5
fee["fb"] = np.where(f < 0, -f / (1 + f), f)          # fee on top, max/min - 1 of the two recorded amounts (Erratum 3)
ppk = pd.concat([pd.read_csv(f"{D}/dune_state/pool_peak.csv"), pd.read_csv(f"{D}/dune_state/extra_peak.csv")], ignore_index=True)
ppk["prb"] = pd.to_numeric(ppk.b, errors="coerce")
ppk["prqe"] = pd.to_numeric(ppk.p, errors="coerce") * ppk.prb

c = duckdb.connect(); c.sql("set TimeZone='UTC'; set threads=1; set memory_limit='1GB'")
txc = c.sql(f"select hr, lam from read_parquet('{D}/s2_txcost_hourly.parquet')").df()
txc["hr"] = pd.to_datetime(txc.hr, utc=True)
txmap = txc.set_index("hr").lam
med_cost = float(txc.lam.median())


def txcost(t):
    return pd.to_datetime(t, utc=True).dt.floor("h").map(txmap).fillna(med_cost).values


print("inputs loaded,", rss(), flush=True)
B = bets[["bet_id", "mint", "is_mayhem", "t_create", "t_grad", "t_entry", "t_exit"] + [f"t_h{h}" for h in ("5m", "1h", "24h", "7d")]].merge(ei, on="bet_id")
B["entry_pool"] = B.t_grad.notna() & (B.t_entry >= B.t_grad)

ce = cp_role("t_entry").copy()
pe = lkb[lkb.role == "t_entry"].merge(ps, on="lid").merge(fee[["lid", "fb"]], on="lid", how="left").set_index("bet_id")
B = B.set_index("bet_id")
out = pd.DataFrame(index=B.index)
out["mint"], out["is_mayhem"], out["t_create"], out["entry_pool"] = B.mint, B.is_mayhem, B.t_create, B.entry_pool
cost_in = B.cost_entry.fillna(med_cost)

vs, vt, rt, cb = (ce.reindex(B.index)[k] for k in ("vs", "vt", "rt", "cb"))
gap_in = ce.reindex(B.index).gap_excluded.fillna(False).astype(bool) & ~B.entry_pool
fc = (95 + cb.fillna(0)) / 1e4
prb, prqe, pfb = (pe.reindex(B.index)[k] for k in ("rb", "rqe", "fb"))

Q, REF = {}, {}
Q_sens = {}
A_CURVE, A_POOL = {}, {}   # bettor's net SOL added to the curve or pool (Erratum 2: own price impact)
for s, col in STAKES.items():
    S = B[col]
    sol_in = S / (1 + fc)
    tok = vt - vs * vt / (vs + sol_in)
    capped = tok > rt
    need = vs * vt / (vt - rt) - vs
    tok_c = np.where(capped, rt, tok)
    refund_c = np.where(capped, S - need * (1 + fc), 0.0)
    qn = S / (1 + pfb)
    tok_p = prb * qn / (prqe + qn)
    tok_p_sens = prb * S / (prqe + S)                     # registered sensitivity: Q = S
    Q[s] = np.where(B.entry_pool, tok_p, tok_c)
    A_CURVE[s] = np.where(B.entry_pool, 0.0, np.where(capped, need, sol_in))
    A_POOL[s] = np.where(B.entry_pool, qn, 0.0)
    Q_sens[s] = np.where(B.entry_pool, tok_p_sens, tok_c)
    REF[s] = np.where(B.entry_pool, 0.0, refund_c)
out["unpriced_entry"] = np.where(B.entry_pool, prb.isna() | prqe.isna() | pfb.isna(),
                                 vs.isna() | (rt <= 0)).astype(bool)
out["gap_entry"] = gap_in.values
out["gap_ext_entry"] = (ce.reindex(B.index).gap_ext.fillna(False).astype(bool) & ~B.entry_pool).values
out["nonsol"] = B.mint.isin(NONSOL).values


def sell_value(role, q, a_curve, a_pool, fee_shift=0.0):
    """Net lamports from selling q tokens at time `role`, plus flags.

    Erratum 2: the state includes the bettor's own buy. Curve: vS' = vS_mkt + a_curve, vT' = k / vS'. Pool entry: rq' =
    rqe_mkt + a_pool, rb' = rb_mkt * rqe_mkt / rq'. Curve entry with pool exit: no adjustment, because migration amounts do
    not depend on who bought."""
    t = B[role]
    in_pool = B.t_grad.notna() & (t >= B.t_grad)
    cc = cp_role(role).reindex(B.index)
    fcx = (95 + cc.cb.fillna(0)) / 1e4
    k = cc.vs * cc.vt
    vs2 = cc.vs + a_curve
    vt2 = k / vs2
    v_curve = (vs2 - k / (vt2 + q)) * (1 - fcx)
    pp = lkb[lkb.role == role].merge(ps, on="lid").set_index("bet_id").reindex(B.index)
    kp = pp.rb * pp.rqe
    rq2 = pp.rqe + a_pool
    rb2 = kp / rq2
    v_pool = rq2 * q / (rb2 + q) * (1 - (pp.fs + fee_shift))
    val = np.where(in_pool, v_pool, v_curve)
    unpriced = np.where(in_pool, pp.rb.isna() | pp.rqe.isna() | pp.fs.isna(), cc.vs.isna())
    gap = np.where(in_pool, False, cc.gap_excluded.fillna(False).astype(bool))
    gap_ext = np.where(in_pool, False, cc.gap_ext.fillna(False).astype(bool))
    return val, unpriced.astype(bool), gap.astype(bool), in_pool.values, gap_ext.astype(bool)


def x_of(v, s, cost_out):
    """Erratum 2: a holder whose sale returns less than the exit tx fee does not sell (X = refund only)."""
    net = np.maximum(v - cost_out, 0.0)
    return (net + REF[s]) / (B[STAKES[s]] + cost_in)


print("entry done,", rss(), flush=True)
for role in ROLES:
    cost_out = txcost(B[role])
    tag = "exit" if role == "t_exit" else role[2:]
    for s, col in STAKES.items():
        v, un, gp, inpool, gx = sell_value(role, Q[s], A_CURVE[s], A_POOL[s])
        out[f"X{s}_{tag}"] = x_of(v, s, cost_out)
    out[f"unpriced_{tag}"], out[f"gap_{tag}"] = un, gp
    if role == "t_exit":
        out["gap_ext_exit"] = gx
    if role == "t_exit":
        out["exit_pool"] = inpool
        S100 = B.stake100_lam
        a_sens = np.where(B.entry_pool, S100, 0.0)
        for name, qq, ap, sh in (("sens_Qeq_S", Q_sens["100"], a_sens, 0.0), ("sens_fee_p5", Q["100"], A_POOL["100"], 5e-4),
                                 ("sens_fee_m5", Q["100"], A_POOL["100"], -5e-4)):
            v, *_ = sell_value(role, qq, A_CURVE["100"], ap, sh)
            out[f"X100_{name}"] = x_of(v, "100", cost_out)

# peak (perfect hindsight, $100 stake): the highest of the curve-peak, pool-peak and exit values
cost_out = txcost(B.t_exit)
q = Q["100"]
k = cpk.set_index("bet_id").reindex(B.index)
fcm = (95 + cp_role("t_exit").reindex(B.index).cb.fillna(0)) / 1e4
kk = k.peak_vs * k.peak_vt
pvs = k.peak_vs + A_CURVE["100"]
v_cpk = (pvs - kk / (kk / pvs + q)) * (1 - fcm)
pk = ppk.set_index("bet_id").reindex(B.index)
fs_exit = lkb[lkb.role == "t_exit"].merge(ps, on="lid").set_index("bet_id").reindex(B.index).fs.fillna(0.0125)
kq = pk.prb * pk.prqe
prq2 = pk.prqe + A_POOL["100"]
v_ppk = prq2 * q / (kq / prq2 + q) * (1 - fs_exit)
v_exit, *_ = sell_value("t_exit", q, A_CURVE["100"], A_POOL["100"])
v_peak = np.nanmax(np.vstack([np.asarray(v_cpk, float), np.asarray(v_ppk, float), np.asarray(v_exit, float)]), axis=0)
out["X100_peak"] = x_of(v_peak, "100", cost_out)

out["launch_day"] = pd.to_datetime(out.t_create, utc=True).dt.floor("D")
out["excluded"] = out.nonsol | out.unpriced_entry | out.unpriced_exit | out.gap_entry | out.gap_exit
out["excluded_nogap"] = out.nonsol | out.unpriced_entry | out.unpriced_exit   # unregistered robustness check: gap rule off
# unregistered robustness check: the gap rule applied to every curve-stream silence over 5 minutes
out["excluded_extgap"] = out.excluded | out.gap_ext_entry | out.gap_ext_exit
print("X done,", rss(), flush=True)
out = out.reset_index()
_c = duckdb.connect()
_c.sql("set threads=1; set memory_limit='1GB'")
_c.sql(f"copy (select * from out) to '{D}/bets_x.parquet' (format parquet)")
_c.close()
print("bets:", len(out), "| excluded:", int(out.excluded.sum()), "(non-SOL curve", int(out.nonsol.sum()), "| unpriced entry", int(out.unpriced_entry.sum()), "exit", int(out.unpriced_exit.sum()),
      "gap entry", int(out.gap_entry.sum()), "exit", int(out.gap_exit.sum()), ")")
