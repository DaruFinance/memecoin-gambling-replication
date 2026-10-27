# Data folder

Raw inputs are not distributed. The scripts in `python/` write their downloads and intermediate tables here, or under the
folder named by `MEMECOIN_PAPER_DATA`. The main layout after a full run:

```
hf_pumpfun_v2/      pump.fun corpus for the study window (pull_corpus.py, pull_transfers.py)
hf_pumpfun_v1/      pump.fun corpus for the July replication (huggingface-cli download)
binance/            spot 1-minute klines (pull_binance.py)
dune_state/         pool states and other Dune query results
dune_2024/          aggregates of the 2024 windows (dune_2024.py)
hl/                 Hyperliquid fills, asset contexts, vault list and episodes
pm/                 Polymarket fills, combo fills, markets, conditional-token and adapter events, positions
cg/                 listed-memecoin bet records (not distributed)
tmp/                DuckDB temporary files and the Dune credit ledger
```

`python/pm_positions.py` describes the columns of the Polymarket input tables.
