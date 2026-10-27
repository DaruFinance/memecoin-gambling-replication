"""Download the pump.fun launch corpus (Hugging Face dataset Slinky21/Pumpfun_v2_dataset at revision 68276cd, the version used in the paper) for the launch tier.

The download runs in four stages, one file at a time:
  1. create and graduate events, kept as published;
  2. bonding-curve trades, kept as published (about 23 GB);
  3. swaps (185 GB raw), reduced to swaps that touch a mint from the create events, with fewer columns;
  4. transaction metadata (25 GB raw), reduced to the cost columns.
In stages 3 and 4 each raw file is deleted once its reduced copy is written. Writes to data/hf_pumpfun_v2 (stages 3 and 4
go to swap_lean/ and txmeta_lean/). The run can be restarted: files listed in _done.txt and complete files already on disk
are skipped.
Run: python3 pull_corpus.py
"""
import os
import shutil
import sys
import time

from config import DATA, TMP

os.environ.setdefault("HF_HOME", f"{DATA}/hf_cache")
os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "0"

import duckdb
import requests
from huggingface_hub import HfApi

REPO = "Slinky21/Pumpfun_v2_dataset"
REVISION = "68276cd008872d3ca623ace95865d487af5ca143"
ROOT = f"{DATA}/hf_pumpfun_v2"
RAW = f"{ROOT}/_raw_tmp"
DONE = f"{ROOT}/_done.txt"


def log(msg):
    free = shutil.disk_usage(DATA).free / 1e9
    print(f"[{time.strftime('%H:%M:%S')}] {msg} | disk free {free:.0f} GB", flush=True)


def done_set():
    return set(open(DONE).read().split()) if os.path.exists(DONE) else set()


def mark_done(name):
    with open(DONE, "a") as f:
        f.write(name + "\n")


os.makedirs(RAW, exist_ok=True)
SIZES = {i.path: i.size for i in HfApi().list_repo_tree(REPO, repo_type="dataset", recursive=True, revision=REVISION)
         if getattr(i, "size", None) is not None and i.path.endswith(".parquet")}
files = sorted(SIZES)
URL = f"https://huggingface.co/datasets/{REPO}/resolve/{REVISION}/{{}}"


def fetch(path, dest_root):
    """Resumable HTTP download checked against the listed size (the hf client's file locks fail on some mounted drives)."""
    dest = f"{dest_root}/{path}"
    if os.path.exists(dest) and os.path.getsize(dest) == SIZES[path]:
        return dest
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    part = dest + ".part"
    for attempt in range(8):
        have = os.path.getsize(part) if os.path.exists(part) else 0
        try:
            hdr = {"Range": f"bytes={have}-"} if have else {}
            with requests.get(URL.format(path), headers=hdr, stream=True, timeout=60) as r:
                if r.status_code == 416:
                    break
                r.raise_for_status()
                if have and r.status_code != 206:
                    have = 0
                with open(part, "ab" if have else "wb") as f:
                    for chunk in r.iter_content(8 << 20):
                        f.write(chunk)
            if os.path.getsize(part) == SIZES[path]:
                break
        except (requests.RequestException, OSError) as e:
            log(f"retry {attempt + 1} {os.path.basename(path)}: {e}")
            time.sleep(min(60, 5 * 2 ** attempt))
    if os.path.getsize(part) != SIZES[path]:
        raise RuntimeError(f"size mismatch {path}: {os.path.getsize(part)} vs {SIZES[path]}")
    os.replace(part, dest)
    return dest
by_kind = {}
for f in files:
    by_kind.setdefault(f.split("/")[-2], []).append(f)
log("listing: " + ", ".join(f"{k}={len(v)}" for k, v in sorted(by_kind.items())))

log("stage 1: create + graduate")
for f in by_kind["create"] + by_kind["graduate"]:
    fetch(f, ROOT)
log("stage 2: trade")
for i, f in enumerate(by_kind["trade"], 1):
    fetch(f, ROOT)
    if i % 10 == 0:
        log(f"trade {i}/{len(by_kind['trade'])}")
log("stages 1-2 done")

c = duckdb.connect()
c.sql(f"set threads=4; set memory_limit='6GB'; set temp_directory='{TMP}'")
c.sql(f"create table mints as select distinct mint from read_parquet('{ROOT}/*/*/create/*.parquet')")
log(f"mint set: {c.sql('select count(*) from mints').fetchone()[0]:,} mints")

SWAP_COLS = ("block_time, slot, tx_index, dex, trader, pool, input_mint, output_mint, "
             "input_amount, output_amount, input_decimals, output_decimals, direction, is_atomic, net_sol")
TX_COLS = ("block_time, slot, tx_index, signature, first_signer, compute_unit_limit, compute_unit_price, "
           "priority_fee, tip_provider, tip_wallet, tip_amount")


def stream(kind, select_sql):
    out_dir = f"{ROOT}/{kind}_lean"
    os.makedirs(out_dir, exist_ok=True)
    todo = [f for f in by_kind[kind] if f not in done_set()]
    log(f"stage {kind}: {len(todo)} of {len(by_kind[kind])} files to go")
    for i, f in enumerate(todo, 1):
        raw = fetch(f, RAW)
        out = f"{out_dir}/{os.path.basename(f)}"
        c.sql(f"copy ({select_sql.format(src=raw)}) to '{out}.tmp' (format parquet, compression zstd)")
        n_in = c.sql(f"select count(*) from '{raw}'").fetchone()[0]
        n_out = c.sql(f"select count(*) from read_parquet('{out}.tmp')").fetchone()[0]
        os.replace(f"{out}.tmp", out)
        os.remove(raw)
        mark_done(f)
        log(f"{kind} {i}/{len(todo)} {os.path.basename(f)} rows {n_in:,} -> {n_out:,}")


stream("swap", f"select {SWAP_COLS} from '{{src}}' "
               "where input_mint in (select mint from mints) or output_mint in (select mint from mints)")
stream("txmeta", f"select {TX_COLS} from '{{src}}'")
shutil.rmtree(RAW, ignore_errors=True)
log("all done")
