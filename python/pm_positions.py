"""Study 3, Polymarket: build positions per (wallet, outcome token) under the registered rule (Study 3, S3-A5).

Window: 8 August 2026 00:00 to 18 September 2026 15:17 UTC.
Inputs, all under DATA/pm:
  fills/*.parquet: fills decoded from the Polymarket exchange contracts' on-chain events on Polygon, one row per fill side,
    with columns address, pm_token_id, ts (unix seconds), role (maker/taker), direction (BUY/SELL), exchange, usd_amount,
    token_amount, fee, price.
  combo_fills/*.parquet: fills on the combo exchange (address, usd_amount, ts). They are excluded and only their share is reported.
  markets/*.parquet: market metadata and resolutions from Polymarket's public market data (pm_token_id, pm_market_id,
    question_id, outcome_index, neg_risk, xrefs with neg_risk_market_id, created_at, resolved_at, resolution_price, is_current).
  ctf/*/*.parquet and adapter/*/*.parquet: split, merge and convert events of the conditional token framework and the
    neg-risk adapter on Polygon (stakeholder, event_type, condition_id or market_id, index_set, amount, ts).
Fees: on the pUSD exchanges a buy costs usd + fee and a sell returns usd - fee; on the older exchanges a buy delivers
tokens - fee and a sell returns usd - fee.
Side legs count only for addresses that also appear in the fills:
  CTF split/merge: amount / n_outcomes per token (Study 3 registration, S3-A2).
  Neg-risk adapter split/merge: amount / 2 per token, dropping adapter rows whose stakeholder is the collateral adapter.
  Conversions (Study 3 registration, Erratum S3-E1 item 5) are treated as an exchange at market. The YES tokens of the other
  questions listed at conversion time enter with cash in = amount x last fill price at or before the conversion; the k NO
  positions receive (k - 1) * amount USDC plus that YES value. These positions are flagged conv.
End value, only if tokens remain: the resolution price if the market resolved by the window end, otherwise the last fill
price at or before the window end. Redemptions are not added because the payout valuation already counts them.
Writes DATA/pm/positions_v2/pos_N.parquet (positions) and act_N.parquet (per-wallet activity) for 8 wallet hash partitions.
Run: python3 pm_positions.py
"""
import os

import duckdb

from config import DATA, TMP

D = f"{DATA}/pm"
OUT = f"{D}/positions_v2"
END = "2026-09-18 15:17:00+00"
W0 = "2026-08-08 00:00:00+00"
NP = 8
COMBO = "0xe3333700ca9d93003f00f0f71f8515005f6c00aa"
COLL_ADAPTER = "0xada2005600dec949baf300f4c6120000bdb6eaab"
PUSD_EX = ("0xe111180000d2663c0091e4f400237545b87b996b", "0xe2222d279d744050d28e00520010520000310f59")


def main():
    os.makedirs(OUT, exist_ok=True)
    db = f"{TMP}/pm_pos_v2.duckdb"
    for f_ in (db, db + ".wal"):
        if os.path.exists(f_):
            os.remove(f_)
    c = duckdb.connect(db)
    c.sql(f"set threads=4; set memory_limit='6GB'; set temp_directory='{TMP}/pm_positions_v2_{os.getpid()}'; set TimeZone='UTC'; set preserve_insertion_order=false")
    e0, e1 = c.sql(f"select epoch(timestamptz '{W0}'), epoch(timestamptz '{END}')").fetchone()

    c.sql(f"""create table fills as
      select address w, pm_token_id token, ts, role, direction, lower(exchange) ex,
             case when lower(exchange) in {PUSD_EX} then
                    case when direction = 'BUY' then usd_amount + coalesce(fee, 0) else usd_amount - coalesce(fee, 0) end
                  else case when direction = 'SELL' then usd_amount - coalesce(fee, 0) else usd_amount end end usd,
             case when lower(exchange) not in {PUSD_EX} and direction = 'BUY' then token_amount - coalesce(fee, 0) else token_amount end tok,
             price
      from read_parquet('{D}/fills/*.parquet')
      where ts >= {e0} and ts <= {e1} and lower(exchange) <> '{COMBO}'""")
    print("fills:", c.sql("select ex, count(*), sum(usd) from fills group by 1 order by 1").fetchall(), flush=True)
    combo = c.sql(f"""select count(*), sum(usd_amount), count(distinct address) from read_parquet('{D}/combo_fills/*.parquet')
                      where ts >= {e0} and ts <= {e1}""").fetchone()
    allv = c.sql("select sum(usd), count(distinct w) from fills").fetchone()
    print(f"combo share: fills={combo[0]} usd={combo[1]:,.0f} ({combo[1] / (combo[1] + allv[0]):.4%} of USD) wallets={combo[2]}", flush=True)

    c.sql(f"""create table sm as
      select pm_token_id token, pm_market_id cond, question_id qid, outcome_index oi, neg_risk,
             json_extract_string(xrefs, '$.neg_risk_market_id') nrm, created_at,
             (resolved_at is not null and resolved_at::timestamptz <= timestamptz '{END}') resolved, resolution_price payout
      from read_parquet('{D}/markets/*.parquet', union_by_name=true) where is_current""")
    c.sql("create table nout as select cond, count(*) n_out from sm group by 1")
    print("market tokens:", c.sql("select count(*), sum(resolved::int), sum(neg_risk::int) from sm").fetchone(), flush=True)
    c.sql("create table lastpx as select token, arg_max(price, ts) px from fills group by 1")

    c.sql("create table users as select distinct w from fills")
    users = "(select w from users)"
    c.sql(f"""create table ctfu as
      select e.stakeholder w, e.event_type, e.condition_id cond, e.amount usd, e.ts from read_parquet('{D}/ctf/*/*.parquet') e
      where e.event_type in ('split', 'merge') and e.ts >= {e0} and e.ts <= {e1} and e.stakeholder in {users}""")
    c.sql(f"""create table ad as select * from read_parquet('{D}/adapter/*/*.parquet', union_by_name=true)
              where ts >= {e0} and ts <= {e1} and event_type in ('split', 'merge', 'convert')
                and not (lower(address) = '0xd91e80cf2e7be2e162c6513ced06f1dd0da35296' and lower(stakeholder) = '{COLL_ADAPTER}')
                and stakeholder in {users}""")
    print("adapter user rows:", c.sql("select address, event_type, count(*), sum(amount) from ad group by all order by 1, 2").fetchall(), flush=True)
    c.sql("""create table legs as
      select u.w, s.token, case when u.event_type = 'split' then u.usd / n.n_out else 0 end cin,
             case when u.event_type = 'merge' then u.usd / n.n_out else 0 end cout,
             case when u.event_type = 'split' then u.usd else 0 end tin, case when u.event_type = 'merge' then u.usd else 0 end tout,
             false conv, u.ts
      from ctfu u join sm s on s.cond = u.cond join nout n on n.cond = u.cond
      union all
      select a.stakeholder, s.token, case when a.event_type = 'split' then a.amount / 2 else 0 end,
             case when a.event_type = 'merge' then a.amount / 2 else 0 end,
             case when a.event_type = 'split' then a.amount else 0 end, case when a.event_type = 'merge' then a.amount else 0 end, false, a.ts
      from ad a join sm s on s.cond = a.condition_id where a.event_type in ('split', 'merge')""")
    # conversions: bit i of index_set marks question index i as converted
    c.sql("""create table q as select nrm, cond, token, oi, created_at,
             ('0x' || substr(qid, 65, 2))::int qidx from sm where neg_risk and nrm is not null and qid is not null""")
    c.sql("""create table cv as select stakeholder w, market_id nrm, try_cast(index_set as hugeint) iset, amount, ts,
                row_number() over () cid from ad where event_type = 'convert'""")
    c.sql("""create table cvq as
      select cv.cid, cv.w, cv.amount, cv.ts, q.token, q.oi,
             ((cv.iset >> q.qidx) & 1) = 1 is_conv
      from cv join q on q.nrm = cv.nrm and (q.created_at is null or q.created_at::timestamptz <= to_timestamp(cv.ts))""")
    c.sql("""create table cvy as select * from cvq where not is_conv and oi = 0""")
    c.sql("""create table fy as select token, ts, price from fills where token in (select distinct token from cvy)""")
    c.sql("""create table cvyp as select y.*, coalesce(f.price, 0) px from cvy y asof left join fy f on f.token = y.token and f.ts <= y.ts""")
    c.sql("""create table cvk as
      select k.cid, k.k, coalesce(v.yes_val, 0) yes_val
      from (select cid, sum((is_conv and oi = 1)::int) k from cvq group by 1) k
      left join (select cid, sum(amount * px) yes_val from cvyp group by 1) v using (cid)""")
    c.sql("""insert into legs
      select x.w, x.token, 0, (greatest(k.k - 1, 0) * x.amount + k.yes_val) / k.k, 0, x.amount, true, x.ts
      from cvq x join cvk k using (cid) where x.is_conv and x.oi = 1 and k.k > 0
      union all
      select y.w, y.token, y.amount * y.px, 0, y.amount, 0, true, y.ts from cvyp y""")
    print("conversions:", c.sql("select count(*), count(distinct cid) from cvq").fetchone(), "of", c.sql("select count(*) from cv").fetchone(), flush=True)
    print("legs:", c.sql("select conv, count(*), sum(cin), sum(cout) from legs group by 1").fetchall(), flush=True)

    for p in range(NP):
        out = f"{OUT}/pos_{p}.parquet"
        if os.path.exists(out):
            continue
        c.sql(f"""create or replace table pf as select w, token,
              sum(case when direction = 'BUY' then usd else 0 end) buy_usd, sum(case when direction = 'BUY' then tok else 0 end) buy_tok,
              sum(case when direction = 'SELL' then usd else 0 end) sell_usd, sum(case when direction = 'SELL' then tok else 0 end) sell_tok,
              count(*) n_fills, sum((role = 'maker')::int) maker_fills, min(ts) first_ts
              from fills where hash(w) % {NP} = {p} group by w, token""")
        c.sql(f"""create or replace table lg as select w, token, sum(cin) cin, sum(cout) cout, sum(tin) tin, sum(tout) tout,
                  bool_or(conv) conv, min(ts) first_leg_ts from legs where hash(w) % {NP} = {p} group by w, token""")
        c.sql(f"""copy (
          with a as (select coalesce(pf.w, lg.w) w, coalesce(pf.token, lg.token) token,
                       coalesce(buy_usd, 0) buy_usd, coalesce(buy_tok, 0) buy_tok, coalesce(sell_usd, 0) sell_usd, coalesce(sell_tok, 0) sell_tok,
                       coalesce(cin, 0) leg_in, coalesce(cout, 0) leg_out, coalesce(tin, 0) leg_tin, coalesce(tout, 0) leg_tout,
                       coalesce(conv, false) conv, coalesce(n_fills, 0) n_fills, coalesce(maker_fills, 0) maker_fills,
                       least(coalesce(first_ts, 9e18), coalesce(first_leg_ts, 9e18)) first_ts
                     from pf full outer join lg on pf.w = lg.w and pf.token = lg.token)
          select a.*, a.buy_tok + a.leg_tin - a.sell_tok - a.leg_tout net_tok,
                 (a.buy_tok + a.leg_tin - a.sell_tok - a.leg_tout) < -1e-6 oversold,
                 sm.resolved, sm.payout, lp.px last_px, (sm.token is null) unmapped,
                 a.buy_usd + a.leg_in cash_in,
                 a.sell_usd + a.leg_out
                   + case when a.buy_tok + a.leg_tin - a.sell_tok - a.leg_tout > 0
                          then (a.buy_tok + a.leg_tin - a.sell_tok - a.leg_tout)
                               * case when sm.resolved and sm.payout is not null then sm.payout else lp.px end
                          else 0 end cash_out
          from a left join sm on sm.token = a.token left join lastpx lp on lp.token = a.token
        ) to '{out}.tmp' (format parquet)""")
        os.replace(out + ".tmp", out)
        c.sql(f"""copy (select w, max(n) max_hour, sum(n) fills, sum(mk) maker_fills from (
                  select w, ts // 3600 h, count(*) n, sum((role = 'maker')::int) mk from fills where hash(w) % {NP} = {p} group by all)
                  group by w) to '{OUT}/act_{p}.parquet' (format parquet)""")
        print(f"partition {p + 1}/{NP} done", flush=True)
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
