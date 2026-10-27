"""Minimal client for the Dune SQL API, used by every script that queries Dune.

Each execution is logged to dune_ledger.tsv in the TMP folder set in config.py: time, execution id, engine size, final
state, credits charged, result bytes and the start of the SQL. The API key is read from the DUNE_API_KEY environment
variable.
Run: python3 dune.py "<sql>" [performance] [out.csv]
"""
import json
import os
import sys
import time

import requests

from config import TMP

KEY = os.environ["DUNE_API_KEY"]
API = "https://api.dune.com/api/v1"
H = {"X-DUNE-API-KEY": KEY, "Content-Type": "application/json"}
LEDGER = f"{TMP}/dune_ledger.tsv"


def get_retry(url, tries=12, **kw):
    for i in range(tries):
        try:
            r = requests.get(url, headers=H, timeout=(60, 600), **kw)
            if r.status_code < 500:
                return r
        except requests.RequestException as e:
            print(f"GET retry {i + 1}: {e.__class__.__name__}", flush=True)
        time.sleep(min(120, 10 * (i + 1)))
    raise RuntimeError(f"GET failed: {url}")


def run(sql, performance="medium", out=None, poll=3, max_wait=1800):
    r = requests.post(f"{API}/sql/execute", headers=H, json={"sql": sql, "performance": performance}, timeout=(60, 300))  # not retried: a timeout may mean the query already ran
    r.raise_for_status()
    eid = r.json()["execution_id"]
    t0 = time.time()
    while True:
        s = get_retry(f"{API}/execution/{eid}/status").json()
        state = s.get("state")
        if state in ("QUERY_STATE_COMPLETED", "QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED", "QUERY_STATE_EXPIRED"):
            break
        if time.time() - t0 > max_wait:
            requests.post(f"{API}/execution/{eid}/cancel", headers=H, timeout=60)
            state = "CANCELLED_BY_CLIENT"
            break
        time.sleep(poll)
    cost = s.get("execution_cost_credits")
    nbytes = 0
    if state == "QUERY_STATE_COMPLETED" and out:
        res = get_retry(f"{API}/execution/{eid}/results/csv")
        res.raise_for_status()
        open(out, "wb").write(res.content)
        nbytes = len(res.content)
    with open(LEDGER, "a") as f:
        f.write("\t".join(map(str, [time.strftime("%F %T"), eid, performance, state, cost, nbytes,
                                    " ".join(sql.split())[:300]])) + "\n")
    return state, cost, nbytes, s


if __name__ == "__main__":
    sql = sys.argv[1]
    perf = sys.argv[2] if len(sys.argv) > 2 else "medium"
    out = sys.argv[3] if len(sys.argv) > 3 else None
    state, cost, nbytes, s = run(sql, perf, out)
    print(json.dumps({"state": state, "execution_cost_credits": cost, "result_bytes": nbytes,
                      "error": s.get("error")}, indent=1))
    if out and nbytes:
        print(open(out).read()[:3000])
