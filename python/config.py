"""Paths used by every script. Inputs and intermediate tables live under MEMECOIN_PAPER_DATA (default: data/ in the
repository root); the aggregated results behind the paper's tables and figures are written to results/aggregates
(MEMECOIN_PAPER_AGG, _FIGURES and _TABLES override the output folders; the smoke test uses them to keep its output apart)."""
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.environ.get("MEMECOIN_PAPER_DATA", os.path.join(ROOT, "data"))
TMP = os.environ.get("MEMECOIN_PAPER_TMP", os.path.join(DATA, "tmp"))
AGG = os.environ.get("MEMECOIN_PAPER_AGG", os.path.join(ROOT, "results", "aggregates"))
FIGURES = os.environ.get("MEMECOIN_PAPER_FIGURES", os.path.join(ROOT, "results", "figures"))
TABLES = os.environ.get("MEMECOIN_PAPER_TABLES", os.path.join(ROOT, "results", "tables"))
for _d in (TMP, AGG, FIGURES, TABLES):
    os.makedirs(_d, exist_ok=True)
