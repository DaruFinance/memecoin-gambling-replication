# Is Retail Crypto Trading Better Than Gambling? Replication code

Code and aggregated results for the paper *Is Retail Crypto Trading Better Than Gambling? Memecoins, Perpetual Futures
and Prediction Markets Against Casino Odds* by Daniel Gatto (October 2026).

The paper measures what retail traders get back per unit staked on pump.fun memecoins, Hyperliquid perpetual futures and
Polymarket from 8 August to 18 September 2026. It compares that return with six casino games under pre-registered tests.
Retail memecoin traders get back 0.754 per unit staked, below every game in the set. Only 14.1% of their wallets end the
window ahead. Hyperliquid (0.9999 per unit of peak notional exposure) and Polymarket (0.984) pay out at casino rates.

## Contents

```
README.md
LICENSE                 MIT
requirements.txt
python/                 every script: data pulls, position building, tests, paper numbers and figures
results/aggregates/     JSON results behind every number in the paper (no raw data)
results/tables/         paper_numbers.csv: each number quoted in the paper, by name
results/figures/        Figures 1 to 5 as PDF
registration/           registrations, hash record and OpenTimestamps proofs (see registration/README.md)
data/                   empty; raw inputs go here (see data/README.md)
```

The repository holds no raw data. `results/aggregates` contains only summary statistics: pooled returns, test outcomes,
counts and binned distributions.

## Smoke test (about a minute)

From the repository root, in PowerShell or any shell:

```
python -m pip install -r requirements.txt
python python/smoke_test.py
```

The smoke test needs no data download. It checks three things and prints PASS or FAIL for each:

1. The six casino payout laws are recomputed and match the shipped results.
2. Synthetic position tables with known properties (`python/synthetic_data.py`) go through the paper's registered tests
   (`python/s3_analyze.py`). A memecoin-like table with a planted group of skilled wallets must come out below every
   game with the skilled wallets detected, while the tables without planted skill must show none.
3. Every number in the paper is recomputed from `results/aggregates` and compared with `results/tables/paper_numbers.csv`.
   The five figures are redrawn.

Output goes to a temporary folder, so the repository is left unchanged.

## Reproducing the paper from the source data

Set `MEMECOIN_PAPER_DATA` to a folder with enough space (the pump.fun corpus alone is about 60 GB) or use `data/`.
Scripts that query Dune need `DUNE_API_KEY` (a free Dune account is enough). Run the scripts from `python/`.

### Sources

| Data | Source | Script |
|---|---|---|
| pump.fun trades, 2026 window | Hugging Face dataset `Slinky21/Pumpfun_v2_dataset`, revision `68276cd` | `pull_corpus.py`, `pull_transfers.py` |
| pump.fun trades, July 2026 replication | Hugging Face dataset `Slinky21/Pumpfun_Memecoin_Corpus`, revision `13bec2b` | `huggingface-cli download Slinky21/Pumpfun_Memecoin_Corpus --repo-type dataset --revision 13bec2bf3089d56d50c35e602616d1b993bb37f5 --local-dir <data>/hf_pumpfun_v1` |
| PumpSwap pool states, second-source rebuild, 2024 windows, SOL prices | Dune decoded Solana tables | `dune_*.py`, `s2_final_pool_state.py`, `sol_price.py` |
| Spot minute prices (SOL, BTC, ETH) | data.binance.vision | `pull_binance.py` |
| Hyperliquid fills and hourly funding | Hyperliquid public S3 archives (requester pays) and stats endpoint | `hl_pull.py`, `hl_inputs.py` |
| Polymarket fills, conversions and resolutions | Polymarket exchange and conditional-token contract events on Polygon, Polymarket public market data | none; the expected tables are described in `pm_positions.py` |

Not included: the CoinGecko price pulls for the listed-memecoin arm (a paid plan) and the earlier Polymarket position
table used for the February to May 2026 replication. The scripts that read them (`listed_arm_analyze.py` and
`s3_analyze.py pm_feb`) are included, with their results in `results/aggregates`.

### Order

Each step lists the scripts in the order they run.

1. **Memecoin inputs:** `pull_corpus.py`, `pull_transfers.py`, `pull_binance.py`, `sample_bets.py`, `tsec_parts.py`,
   `tsec_merge.py`, `s2_slot_time.py`, `final_inputs.py`, then the Dune pulls `dune_pool_state_full.py all`,
   `dune_entry_fee_and_peak.py`, `dune_final_inputs.py`, `s2_final_pool_state.py` and `trade_coverage.py`.
   `gate_pool_pricing.py` reproduces the registered pool-pricing gate that failed and was replaced (Amendment 1).
2. **Random bets (Study 1):** `price_curve.py`, `assemble_x.py`, `control_btc_eth.py`, `stats_and_ruling.py`,
   `july_replication.py`.
3. **Memecoin retail positions (Study 2):** `s2_positions.py`, `s2_funding_sample.py`, `s2_analyze.py`,
   `s2_transfer_sensitivity.py`.
4. **Three venues (Study 3):** `hl_pull.py`, `hl_inputs.py`, `hl_episodes.py`, `pm_positions.py`, then
   `s3_analyze.py` with `meme`, `hl`, `pm`, `pm_noconv` and `meme_bot50`, then `s3_secondary.py`, `s3_explore_cuts.py`,
   `s3_meme_topdecile_profile.py`, `s3_hl_topdecile_profile.py` and `listed_arm_analyze.py`.
5. **Unregistered checks:** `dune_s2_full_pools.py`; `dune_s2_full.py` three times (plain, with `POOLS_EXTRA=1`, with
   `OTHER_VENUES=1`); `s2_dune_full_compare.py`; `s2_dune_allvenues_compare.py`; `s3_wfo_costs.py` with `costs`, `meme`,
   `hl` and `pm`; `s3_class_ci.py`; `pool_split.py`; `retail_shape_stats.py`; `sol_price.py`; `dune_2024.py aug24`; `dune_2024.py nov24`;
   `dune_2024_summary.py`.
6. **Paper outputs:** `paper_numbers.py` and `paper_figures.py`.

`config.py` holds every path. `s2_analyze.py` leaves a DuckDB file in the temporary folder that `dune_s2_full.py` reads,
so keep it until step 5 is done.

### Where each result comes from

| Paper item | Aggregate | Producer |
|---|---|---|
| Table 2 (three venues), Figures 1, 2 and 4 | `s3_results_meme.json`, `s3_results_hl.json`, `s3_results_pm.json` | `s3_analyze.py` |
| Table 1 (casino games) | `casino_games.json` | `casino_games.py` |
| Table 3 (excluded classes) and their intervals | `s3_secondary.json`, `s3_class_ci.json` | `s3_secondary.py`, `s3_class_ci.py` |
| Table 4 (2024 windows) | `dune_2024_summary.json`, `retail_shape_stats.json`, `sol_usd_window.json` | `dune_2024_summary.py`, `retail_shape_stats.py`, `sol_price.py` |
| Random bets and Figure 3 | `results.json`, `s1_replication_july.json`, `s2_results.json` | `stats_and_ruling.py`, `july_replication.py`, `s2_analyze.py` |
| Sensitivity checks | `s2_results.json`, `s2_dune_full.json`, `s2_dune_allvenues.json`, `s2_transfer_sensitivity.json`, `s3_wfo_costs.json`, `s3_results_meme_bot50.json`, `s3_results_pm_noconv.json`, `s3_results_pm_feb*.json`, `s3_results_listed.json` | the scripts of the same name |
| Where the 2026 losses sit, dust, 0.1 SOL wallets | `pool_split.json` | `pool_split.py` |
| Appendix B and Figure 5 | `s3_explore_cuts.json`, `s3_meme_topdecile_profile.json`, `s3_hl_topdecile_profile.json` | the scripts of the same name |

`paper_numbers.py` turns these files into `results/tables/paper_numbers.csv`. `paper_figures.py` draws the figures.

## Seeds

20260923 (random bets), 20260924 (memecoin retail study and 2024 windows), 20261010 (three-venue study), 20261021
(unregistered robustness checks and the synthetic smoke-test data).

## License

MIT. See `LICENSE`.
