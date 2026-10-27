"""Download the 'other' folder of the Hugging Face dataset Slinky21/Pumpfun_v2_dataset.

The folder holds system-program SOL transfers (52 files, 1.74 GB). They are used to label insiders by who funded each
token creator. Writes to data/hf_pumpfun_v2; complete files already on disk are skipped.
Run: python3 pull_transfers.py
"""
import json, os, urllib.request

from config import DATA

os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "0"
import requests
ROOT = f"{DATA}/hf_pumpfun_v2"
URL = "https://huggingface.co/datasets/Slinky21/Pumpfun_v2_dataset/resolve/68276cd008872d3ca623ace95865d487af5ca143/{}"
j = json.load(urllib.request.urlopen("https://huggingface.co/api/datasets/Slinky21/Pumpfun_v2_dataset/tree/68276cd008872d3ca623ace95865d487af5ca143?recursive=true"))
files = [(f["path"], f["size"]) for f in j if f["type"] == "file" and "/other/" in f["path"]]
for i, (p, size) in enumerate(files, 1):
    dest = f"{ROOT}/{p}"
    if os.path.exists(dest) and os.path.getsize(dest) == size:
        continue
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    for attempt in range(8):
        try:
            with requests.get(URL.format(p), stream=True, timeout=(60, 300)) as r:
                r.raise_for_status()
                with open(dest + ".part", "wb") as f:
                    for ch in r.iter_content(8 << 20):
                        f.write(ch)
            if os.path.getsize(dest + ".part") == size:
                os.replace(dest + ".part", dest); break
        except requests.RequestException as e:
            print("retry", p, e.__class__.__name__, flush=True)
    print(f"{i}/{len(files)}", flush=True)
print("DONE")
