"""
Build the BKIQ UNIVERSE -> v23 "Prefix | Name" canonical mapping.

Rules (per the approved integration plan, Phase 1):
  EQ_US, EQ_SECT, EQ_DM, EQ_IDX, EQ_APAC, EQ_EM, DEFENCE -> "EQ | <name>"
  FI, FI_INTL                                            -> "FI | <name>"
  CMD                                                     -> "CMD | <name>"
  CRYPTO                                                  -> "CRYPTO | <name>"
  FX                                                       -> "FX | <name>"      (system-context only, not scored)
  RATES                                                    -> excluded entirely (yield tickers stay on Yahoo elsewhere, untouched)
  VOL                                                      -> "VOL | <name>"    (display-only, outside v23's score_universe() allowlist)
  ALT: IFRA/PSP/AMLP/REET/KBWY -> "EQ | <name>" ; BCI/PDBC -> "CMD | <name>"

Where a BKIQ ticker matches one of v23's own 54 native tickers, we reuse
v23's exact name string so the hardcoded lookups inside v23.py keep working
verbatim: "EQ | World (ACWI)" (pillar_corr anchor), "CMD | Gold",
"FI | US Treasuries (20+y)", "FI | HY Credit" (compute_fss), "CRYPTO | Bitcoin"
(compute_lsd_v2).

One data-integrity finding surfaced while building this mapping (reported to
BK, not silently resolved): BKIQ's single oil instrument is ticker BNO,
labelled "WTI Oil (BNO proxy)" in BKIQ's own UNIVERSE -- but BNO is actually
a Brent Oil ETN, and v23 already separately and correctly tracks
"CMD | Brent Oil" -> BNO. There is no ticker collision to resolve by fiat:
BKIQ's BNO instrument maps onto v23's existing "CMD | Brent Oil" slot, using
v23's correct name (not BKIQ's "WTI Oil (BNO proxy)" mislabel). v23's own
"CMD | WTI Oil" -> USO has NO BKIQ counterpart -- BKIQ has never tracked a
real WTI instrument (it deliberately dropped USO due to the reverse-split
corruption documented in BKIQ's own commit history). Rather than introduce a
brand-new instrument BKIQ has never displayed, carrying the exact ticker BKIQ
already identified as broken, this mapping DROPS "CMD | WTI Oil" (USO)
entirely from the merged panel. This resolves integration-plan decision 8
(WTI/USO disposition) for the *scoring* universe: not "sanity-clip and keep",
but "BKIQ never had it, don't add it." Flagged explicitly, not assumed.
"""
import re
import ast
import json
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
# NOTE (fixed 2026-09-08): this script lives at tools/fragility_migration/,
# two directories below the repo root -- not one. BUILD_DIR must therefore be
# .parent THREE times (same bug, same fix, as build_canonical_panel.py).
BUILD_DIR = SCRIPT_DIR.parent.parent

def load_bkiq_universe():
    src = (BUILD_DIR / "bk_market_dashboard.py").read_text(encoding="utf-8")
    m = re.search(r"UNIVERSE\s*=\s*\[(.*?)\n\]\n", src, re.S)
    return ast.literal_eval("[" + m.group(1) + "]")  # (category, ticker, name, asset_class)

def load_v23_yahoo_tickers():
    src = (BUILD_DIR / "fragility_v23.py").read_text(encoding="utf-8")
    m = re.search(r"YAHOO_TICKERS[^=]*=\s*\{(.*?)\n\}\n", src, re.S)
    return ast.literal_eval("{" + m.group(1) + "}")  # v23_name -> ticker

EQ_CATEGORIES = {"EQ_US", "EQ_SECT", "EQ_DM", "EQ_IDX", "EQ_APAC", "EQ_EM", "DEFENCE"}
FI_CATEGORIES = {"FI", "FI_INTL"}
ALT_TO_EQ = {"IFRA", "PSP", "AMLP", "REET", "KBWY"}
ALT_TO_CMD = {"BCI", "PDBC"}
# BKIQ never tracked a real WTI instrument; do not introduce v23's broken USO ticker.
DROP_TICKERS = {"USO"}

# BKIQ's own name for its USD Index instrument ("US Dollar Index") doesn't
# contain the substrings build_hmm_features() searches for ("USD Index" or
# "DXY") -- v23's own native name for the same concept ("USD Index (DXY)")
# does. Force this one name so the DXY feature in the HMM regime classifier
# actually resolves instead of silently falling back to a 0.0 placeholder.
NAME_OVERRIDES = {
    "DX-Y.NYB": "FX | USD Index (DXY)",
}

def build_mapping():
    universe = load_bkiq_universe()
    v23_names_by_ticker = {}
    for name, tk in load_v23_yahoo_tickers().items():
        v23_names_by_ticker[tk] = name

    rows = []          # scored + FX system-context + VOL display rows
    excluded = []       # RATES + explicitly dropped
    seen_names = {}

    for category, ticker, name, asset_class in universe:
        if ticker in DROP_TICKERS:
            excluded.append({"category": category, "ticker": ticker, "name": name, "reason": "dropped (broken/never tracked by BKIQ)"})
            continue
        if category == "RATES":
            excluded.append({"category": category, "ticker": ticker, "name": name, "reason": "yield index, excluded from fragility panel per decision 5"})
            continue

        if category in EQ_CATEGORIES:
            prefix, scored = "EQ", True
        elif category in FI_CATEGORIES:
            prefix, scored = "FI", True
        elif category == "CMD":
            prefix, scored = "CMD", True
        elif category == "CRYPTO":
            prefix, scored = "CRYPTO", True
        elif category == "FX":
            prefix, scored = "FX", False
        elif category == "VOL":
            prefix, scored = "VOL", False
        elif category == "ALT":
            if ticker in ALT_TO_EQ:
                prefix, scored = "EQ", True
            elif ticker in ALT_TO_CMD:
                prefix, scored = "CMD", True
            else:
                raise ValueError(f"Unclassified ALT ticker: {ticker} ({name})")
        else:
            raise ValueError(f"Unhandled BKIQ category: {category} ({ticker})")

        # Reuse v23's exact name string when this ticker is one of v23's native 54,
        # so v23's hardcoded column-name lookups keep resolving.
        if ticker in NAME_OVERRIDES:
            canonical_name = NAME_OVERRIDES[ticker]
        elif ticker in v23_names_by_ticker:
            canonical_name = v23_names_by_ticker[ticker]
        else:
            canonical_name = f"{prefix} | {name}"

        if canonical_name in seen_names and seen_names[canonical_name] != ticker:
            raise ValueError(
                f"Name collision: '{canonical_name}' claimed by both "
                f"{seen_names[canonical_name]} and {ticker}"
            )
        seen_names[canonical_name] = ticker

        rows.append({
            "category": category, "ticker": ticker, "bkiq_name": name,
            "asset_class": asset_class, "canonical_name": canonical_name,
            "prefix": prefix, "scored": scored,
            "reused_v23_slot": ticker in v23_names_by_ticker,
        })

    return rows, excluded

if __name__ == "__main__":
    rows, excluded = build_mapping()
    by_prefix = {}
    for r in rows:
        by_prefix.setdefault(r["prefix"], []).append(r)
    print(f"Total mapped: {len(rows)}  |  Excluded: {len(excluded)}")
    for prefix, items in sorted(by_prefix.items()):
        scored_n = sum(1 for i in items if i["scored"])
        print(f"  {prefix:8s} {len(items):3d} instruments ({scored_n} scored, {len(items)-scored_n} context/display)")
    print("\nExcluded:")
    for e in excluded:
        print(f"  {e['ticker']:10s} {e['name']:30s} -- {e['reason']}")
    reused = [r for r in rows if r["reused_v23_slot"]]
    print(f"\nReused v23's native 54 name/ticker slots: {len(reused)}")
    # Written beside this script (tools/fragility_migration/), matching where
    # it already lives on disk and where build_canonical_panel.py's
    # load_mapping() reads it from -- not BUILD_DIR/"data" (that's for the
    # panel CSVs and the existing prices_cache.csv/volumes_cache.csv).
    out_path = SCRIPT_DIR / "ticker_mapping.json"
    json.dump({"mapped": rows, "excluded": excluded}, open(out_path, "w"), indent=2)
    print(f"\nWrote {out_path}")
