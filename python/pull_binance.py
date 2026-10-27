"""Download the Binance spot 1-minute klines used for SOL/USD prices (random-bet stakes and the July replication) and for
the BTC and ETH control bets, from Binance's public archive (data.binance.vision).
Writes DATA/binance/<SYMBOL>-1m-<YYYY-MM>.zip for July and August 2026 and the daily files for 1 to 22 September 2026.
Run: python3 pull_binance.py"""
import os
import urllib.request

from config import DATA

BASE = "https://data.binance.vision/data/spot"
OUT = f"{DATA}/binance"
FILES = [("SOLUSDT", "monthly", "2026-07")] + [(s, "monthly", "2026-08") for s in ("SOLUSDT", "BTCUSDT", "ETHUSDT")] + \
        [(s, "daily", f"2026-09-{d:02d}") for s in ("SOLUSDT", "BTCUSDT", "ETHUSDT") for d in range(1, 23)]

os.makedirs(OUT, exist_ok=True)
for sym, freq, period in FILES:
    name = f"{sym}-1m-{period}.zip"
    if not os.path.exists(f"{OUT}/{name}"):
        urllib.request.urlretrieve(f"{BASE}/{freq}/klines/{sym}/1m/{name}", f"{OUT}/{name}")
print(f"{len(FILES)} files in {OUT}")
