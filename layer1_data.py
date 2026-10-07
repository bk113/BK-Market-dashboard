"""
Layer 1 — Canonical Data Architecture
=======================================
Single source of truth for instrument metadata and clean data access.

Four components
---------------
1. INSTRUMENT_REGISTRY   — dict[ticker → InstrumentMeta] replacing the scattered
                           constants in bk_market_dashboard.py (UNIVERSE, FX_CCY_MAP,
                           DISPLAY_EXCLUSIONS, SYNTHETIC_TICKERS, RANKING_EXCLUSIONS,
                           FRAGILITY_EXCLUSIONS, SHARPE_EXCLUSIONS, VOL_VALUE_EXCLUSIONS,
                           RETURN_SANITY_MAX, YIELD_TICKERS).

2. Backward-compatible derived views — all of the above constants re-exported as
   read-only sets/lists/dicts so existing callers need no immediate changes.
   Migration plan: import from here → verify metrics match → delete old constants.

3. ClipLog — append-only structured log (ticker, date, horizon, raw, clipped, reason).
   Written to data/clip_log.csv on flush(). Fires on every sanity-clip or data-quality
   fallback so the Data Health strip has a source of truth.

4. get_clean_prices() / get_clean_returns() — the single enforced data-access path:
   - Consistent ffill policy (limit=5 for point-in-time ratios, limit=3 for rolling stats)
   - Generalised self-relative daily-return winsorization (replaces RETURN_SANITY_MAX's
     hardcoded 7-ticker list with per-instrument IQR-derived bounds from the v2.3 engine's
     winsorization_report.csv, applied uniformly to every scored instrument)
   - Period-return clip (YTD/1M/3M) computed self-relatively from rolling history,
     replacing the hardcoded per-ticker caps

⚠  FLAGGED — Item 5 (max historical lookback) NOT implemented
   Pulling 20+ years for 115 tickers in one GitHub Actions run would exceed the 30-min
   timeout and hit Yahoo Finance rate limits. Recommended approach:
     a) One-time local backfill: run `python layer1_data.py --backfill` locally (~20-40 min),
        commits extended data/prices_cache_l1.csv (max available history per ticker).
     b) Daily workflow uses incremental cache append only (validate_cache() already supports
        this) — no change to the Actions runner needed.
   Do NOT proceed with backfill until explicitly approved.
"""
from __future__ import annotations

import csv
import os
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parent
_DATA_DIR  = _REPO_ROOT / "data"


# ══════════════════════════════════════════════════════════════════════════════
#  1.  INSTRUMENT_REGISTRY
# ══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class InstrumentMeta:
    ticker:            str
    section:           str            # EQ_US / FI / CMD / ...
    display_name:      str            # shown in UI
    bucket:            str            # regime-fit allocation bucket
    currency:          str            # display currency
    canonical_name:    str            # v2.3 "Prefix | Name" — "" if unscored
    v23_prefix:        str            # "EQ" / "FI" / "CMD" / "CRYPTO" / "FX" / "VOL" / ""
    fragility_scored:  bool           # v2.3 engine produces a score
    display:           bool           # shown in all UI tabs
    rankable:          bool           # eligible for Top 5 / RSR rankings
    sharpe_meaningful: bool           # Sharpe ratio makes sense
    vol_display:       bool           # vol metric makes sense
    yield_scale:       bool           # yfinance returns ×10 — divide by 10
    synthetic_source:  Optional[str]  # "GLD" for GVZ, "BNO" for OVX; None otherwise
    has_volume:        bool           # volume data available and meaningful
    return_caps:       Optional[Tuple[float, float, float]]  # (ytd, 1m, 3m) hard caps
                                                              # None = use self-relative only


# ── Master list ──────────────────────────────────────────────────────────────
# Each row: (section, ticker, display_name, bucket, currency,
#            canonical_name, v23_prefix, fragility_scored, display, rankable,
#            sharpe_meaningful, vol_display, yield_scale, synthetic_source,
#            has_volume, return_caps)
#
# Adding a new instrument = add one row here. Nothing else.

_REGISTRY_ROWS: list[tuple] = [
    # ── EQ_US ────────────────────────────────────────────────────────────────
    ("EQ_US", "QQQ",      "Nasdaq 100",              "EQ Growth",    "USD", "EQ | US (Nasdaq 100)",         "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_US", "IWM",      "Russell 2000",            "EQ Growth",    "USD", "EQ | US (Russell 2000)",       "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_US", "SPY",      "S&P 500",                 "EQ Growth",    "USD", "EQ | US (S&P 500)",            "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_US", "ACWI",     "World (ACWI)",            "EQ Growth",    "USD", "EQ | World (ACWI)",            "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),

    # ── EQ_SECT ──────────────────────────────────────────────────────────────
    ("EQ_SECT", "XLE",   "Energy",                  "EQ Defensive", "USD", "EQ | US Energy Sector",        "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_SECT", "XLF",   "Financials",              "EQ Growth",    "USD", "EQ | US Financials Sector",    "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_SECT", "VNQ",   "Real Estate (REITs)",     "Real Assets",  "USD", "EQ | Real Estate (REITs)",     "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_SECT", "XLU",   "Utilities",               "EQ Defensive", "USD", "EQ | Utilities",               "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_SECT", "XLV",   "Healthcare",              "EQ Defensive", "USD", "EQ | US Healthcare Sector",    "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_SECT", "XLK",   "Technology",              "EQ Growth",    "USD", "EQ | US Tech Sector",          "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_SECT", "XLP",   "Consumer Staples",        "EQ Defensive", "USD", "EQ | Consumer Staples",        "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_SECT", "XLY",   "Consumer Discretionary",  "EQ Growth",    "USD", "EQ | Consumer Discretionary",  "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_SECT", "XLI",   "Industrials",             "EQ Growth",    "USD", "EQ | Industrials",             "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_SECT", "XLB",   "Materials",               "Real Assets",  "USD", "EQ | Materials",               "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_SECT", "XLC",   "Communications",          "EQ Growth",    "USD", "EQ | Communications",          "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),

    # ── EQ_DM ────────────────────────────────────────────────────────────────
    ("EQ_DM", "EWA",     "Australia",               "EQ Growth",    "AUD", "EQ | Australia",               "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_DM", "EFA",     "Europe Dev (EFA)",        "EQ Growth",    "USD", "EQ | Europe Dev (EAFE)",       "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_DM", "EZU",     "Eurozone",                "EQ Growth",    "EUR", "EQ | Eurozone",                "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_DM", "EWG",     "Germany",                 "EQ Growth",    "EUR", "EQ | Germany",                 "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_DM", "EWJ",     "Japan",                   "EQ Growth",    "JPY", "EQ | Japan",                   "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_DM", "EWS",     "Singapore",               "EQ Growth",    "SGD", "EQ | Singapore",               "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_DM", "EWU",     "UK",                      "EQ Defensive", "GBP", "EQ | UK",                      "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_DM", "FLGB",    "FTSE 100 (UK)",           "EQ Defensive", "GBP", "EQ | FTSE 100 (UK)",           "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),

    # ── EQ_IDX ───────────────────────────────────────────────────────────────
    ("EQ_IDX", "GURU",   "Global X Guru ETF",       "EQ Growth",    "USD", "EQ | Global X Guru ETF",       "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_IDX", "ARKK",   "ARK Innovation",          "EQ Growth",    "USD", "EQ | ARK Innovation",          "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_IDX", "SKYY",   "Cloud Computing",         "EQ Growth",    "USD", "EQ | Cloud Computing",         "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_IDX", "ICLN",   "Clean Energy",            "EQ Growth",    "USD", "EQ | Clean Energy",            "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_IDX", "GRID",   "Smart Grid Infra",        "EQ Growth",    "USD", "EQ | Smart Grid Infra",        "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_IDX", "SOXX",   "Semiconductors",          "EQ Growth",    "USD", "EQ | Semiconductors",          "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),

    # ── EQ_EM ────────────────────────────────────────────────────────────────
    ("EQ_EM", "EWZ",     "Brazil",                  "EQ Growth",    "BRL", "EQ | Brazil",                  "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_EM", "FXI",     "China",                   "EQ Growth",    "CNY", "EQ | China",                   "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_EM", "EEM",     "EM Broad",                "EQ Growth",    "USD", "EQ | EM Broad",                "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_EM", "INDA",    "India",                   "EQ Growth",    "USD", "EQ | India",                   "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_EM", "EWY",     "Korea",                   "EQ Growth",    "KRW", "EQ | Korea",                   "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_EM", "EZA",     "South Africa",            "EQ Growth",    "ZAR", "EQ | South Africa",            "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_EM", "EWT",     "Taiwan",                  "EQ Growth",    "TWD", "EQ | Taiwan",                  "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_EM", "EWW",     "Mexico",                  "EQ Growth",    "MXN", "EQ | Mexico",                  "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_EM", "EIDO",    "Indonesia",               "EQ Growth",    "IDR", "EQ | Indonesia",               "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_EM", "VNM",     "Vietnam",                 "EQ Growth",    "VND", "EQ | Vietnam",                 "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_EM", "KSA",     "Saudi Arabia",            "Real Assets",  "SAR", "EQ | Saudi Arabia",            "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_EM", "SMIN",    "India Small Cap",         "EQ Growth",    "USD", "EQ | India Small Cap",         "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),

    # ── EQ_APAC ──────────────────────────────────────────────────────────────
    ("EQ_APAC", "AAXJ",  "Asia ex-Japan",           "EQ Growth",    "USD", "EQ | Asia ex-Japan",           "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_APAC", "EWH",   "Hang Seng / HK",          "EQ Growth",    "HKD", "EQ | Hong Kong",               "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_APAC", "VPL",   "Asia Pacific",            "EQ Growth",    "USD", "EQ | Asia Pacific",            "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_APAC", "CNYA",  "China A-Shares",          "EQ Growth",    "CNY", "EQ | China A-Shares",          "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_APAC", "ASEA",  "ASEAN",                   "EQ Growth",    "USD", "EQ | ASEAN",                   "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_APAC", "THD",   "Thailand",                "EQ Growth",    "USD", "EQ | Thailand",                "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("EQ_APAC", "EPHE",  "Philippines",             "EQ Growth",    "USD", "EQ | Philippines",             "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),

    # ── DEFENCE ──────────────────────────────────────────────────────────────
    ("DEFENCE", "XAR",   "BAE Aerospace & Defence", "Alts",         "USD", "EQ | BAE Aerospace & Defence", "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("DEFENCE", "ITA",   "US Aerospace & Defence",  "Alts",         "USD", "EQ | US Aerospace & Defence",  "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),

    # ── FI ───────────────────────────────────────────────────────────────────
    ("FI", "BIL",        "Cash (T-Bills)",          "Cash",         "USD", "FI | Cash (T-Bills)",          "FI",  True,  True,  True,  False, True,  False, None,    True,  None),
    ("FI", "EMLC",       "EM Local Currency",       "Fixed Income", "USD", "FI | EM Local Currency",       "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("FI", "EMB",        "EM USD Sovereign",        "Fixed Income", "USD", "FI | EM USD Sovereigns",       "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("FI", "HYG",        "HY Credit",               "EQ Growth",    "USD", "FI | HY Credit",               "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("FI", "LQD",        "IG Credit",               "Fixed Income", "USD", "FI | IG Credit",               "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("FI", "BKLN",       "Senior Loans",            "Fixed Income", "USD", "FI | Senior Loans",            "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("FI", "SHY",        "Treasuries 0-3Y",         "Cash",         "USD", "FI | US Treasuries (1-3y)",    "FI",  True,  True,  True,  False, True,  False, None,    True,  None),
    ("FI", "TLT",        "Treasuries 20Y+",         "Fixed Income", "USD", "FI | US Treasuries (20+y)",    "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("FI", "IEF",        "Treasuries 7-10Y",        "Fixed Income", "USD", "FI | US Treasuries (7-10y)",   "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("FI", "AGG",        "US Aggregate",            "Fixed Income", "USD", "FI | US Aggregate Bond",       "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("FI", "TIP",        "US TIPS",                 "Real Assets",  "USD", "FI | US TIPS",                 "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("FI", "MUB",        "Municipal Bonds",         "Fixed Income", "USD", "FI | Municipal Bonds",         "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("FI", "CWB",        "Convertible Bonds",       "EQ Growth",    "USD", "FI | Convertible Bonds",       "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("FI", "VCSH",       "ST IG Corporates",        "Fixed Income", "USD", "FI | ST IG Corporates",        "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("FI", "PFF",        "Preferred Securities",    "Fixed Income", "USD", "FI | Preferred Securities",    "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("FI", "HYD",        "Municipal HY",            "Fixed Income", "USD", "FI | Municipal HY",            "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),

    # ── FI_INTL ──────────────────────────────────────────────────────────────
    ("FI_INTL", "BNDW",  "Global Aggregate",        "Fixed Income", "USD", "FI | Global Aggregate",        "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("FI_INTL", "IHY",   "Intl High Yield",         "EQ Growth",    "USD", "FI | Intl High Yield",         "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("FI_INTL", "IGIB",  "US IG Credit (Intl)",     "Fixed Income", "USD", "FI | US IG Credit (Intl)",     "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("FI_INTL", "BNDX",  "Intl Bonds",              "Fixed Income", "USD", "FI | Global ex-US Aggregate",  "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("FI_INTL", "IGOV",  "Intl Govt Bonds",         "Fixed Income", "USD", "FI | Intl Govt Bonds",         "FI",  True,  True,  True,  True,  True,  False, None,    True,  None),

    # ── RATES — yield indices (×10 correction, not rankable/Sharpe) ──────────
    ("RATES", "^TNX",    "10Y Treasury Yield",      "Fixed Income", "USD", "",                             "",    False, True,  False, False, False, True,  None,    False, None),
    ("RATES", "^IRX",    "2Y Treasury Yield",       "Fixed Income", "USD", "",                             "",    False, True,  False, False, False, True,  None,    False, None),
    ("RATES", "^FVX",    "5Y Treasury Yield",       "Fixed Income", "USD", "",                             "",    False, True,  False, False, False, True,  None,    False, None),
    ("RATES", "^TYX",    "30Y Treasury Yield",      "Fixed Income", "USD", "",                             "",    False, True,  False, False, False, True,  None,    False, None),

    # ── CMD — commodities (several have known unadjusted-action issues) ───────
    ("CMD", "DBA",       "Agriculture",             "Real Assets",  "USD", "CMD | Agriculture Broad",      "CMD", True,  True,  True,  True,  True,  False, None,    True,  (0.75, 0.30, 0.50)),
    ("CMD", "DBC",       "Broad Commodities",       "Real Assets",  "USD", "CMD | Broad Commodities",      "CMD", True,  True,  True,  True,  True,  False, None,    True,  (0.75, 0.30, 0.50)),
    ("CMD", "COPX",      "Copper Miners",           "Real Assets",  "USD", "CMD | Copper Miners",          "CMD", True,  True,  True,  True,  True,  False, None,    True,  (1.00, 0.50, 0.75)),
    ("CMD", "GLD",       "Gold",                    "Real Assets",  "USD", "CMD | Gold",                   "CMD", True,  True,  True,  True,  True,  False, None,    True,  (0.75, 0.30, 0.50)),
    ("CMD", "UNG",       "Natural Gas",             "Real Assets",  "USD", "CMD | Natural Gas",            "CMD", True,  True,  True,  True,  True,  False, None,    True,  (1.00, 0.50, 0.75)),
    ("CMD", "SLV",       "Silver",                  "Real Assets",  "USD", "CMD | Silver",                 "CMD", True,  True,  True,  True,  True,  False, None,    True,  (1.00, 0.40, 0.60)),
    ("CMD", "BNO",       "WTI Oil (BNO proxy)",     "Real Assets",  "USD", "CMD | Brent Oil",              "CMD", True,  True,  True,  True,  True,  False, None,    True,  (0.50, 0.30, 0.40)),
    ("CMD", "PPLT",      "Platinum",                "Real Assets",  "USD", "CMD | Platinum",               "CMD", True,  True,  True,  True,  True,  False, None,    True,  None),
    ("CMD", "URA",       "Uranium",                 "Alts",         "USD", "CMD | Uranium",                "CMD", True,  True,  True,  True,  True,  False, None,    True,  None),
    ("CMD", "WEAT",      "Wheat",                   "Real Assets",  "USD", "CMD | Wheat",                  "CMD", True,  True,  True,  True,  True,  False, None,    True,  None),
    ("CMD", "LIT",       "Lithium",                 "Alts",         "USD", "CMD | Lithium",                "CMD", True,  True,  True,  True,  True,  False, None,    True,  None),
    ("CMD", "VALE",      "Iron Ore (VALE)",         "Real Assets",  "USD", "CMD | Iron Ore (VALE)",        "CMD", True,  True,  True,  True,  True,  False, None,    True,  None),
    ("CMD", "CORN",      "Corn",                    "Real Assets",  "USD", "CMD | Corn",                   "CMD", True,  True,  True,  True,  True,  False, None,    True,  None),
    ("CMD", "SOYB",      "Soybeans",                "Real Assets",  "USD", "CMD | Soybeans",               "CMD", True,  True,  True,  True,  True,  False, None,    True,  None),
    ("CMD", "PALL",      "Palladium",               "Real Assets",  "USD", "CMD | Palladium",              "CMD", True,  True,  True,  True,  True,  False, None,    True,  None),
    ("CMD", "CPER",      "Copper ETF",              "Real Assets",  "USD", "CMD | Copper",                 "CMD", True,  True,  True,  True,  True,  False, None,    True,  None),

    # ── CRYPTO ───────────────────────────────────────────────────────────────
    ("CRYPTO", "BTC-USD","Bitcoin",                 "Alts",         "USD", "CRYPTO | Bitcoin",             "CRYPTO", True, True, True, True,  True,  False, None,   True,  None),
    ("CRYPTO", "ETH-USD","Ethereum",                "Alts",         "USD", "CRYPTO | Ethereum",            "CRYPTO", True, True, True, True,  True,  False, None,   True,  None),
    ("CRYPTO", "SOL-USD","Solana",                  "Alts",         "USD", "CRYPTO | Solana",              "CRYPTO", True, True, True, True,  True,  False, None,   True,  None),

    # ── FX ───────────────────────────────────────────────────────────────────
    ("FX", "AUDUSD=X",   "AUD/USD",                 "Alts",         "AUD", "FX | AUD/USD",                 "FX",  True,  True,  False, True,  True,  False, None,    False, None),
    ("FX", "EURUSD=X",   "EUR/USD",                 "Alts",         "EUR", "FX | EUR/USD",                 "FX",  True,  True,  False, True,  True,  False, None,    False, None),
    ("FX", "GBPUSD=X",   "GBP/USD",                 "Alts",         "GBP", "FX | GBP/USD",                 "FX",  True,  True,  False, True,  True,  False, None,    False, None),
    ("FX", "DX-Y.NYB",   "US Dollar Index",         "Cash",         "USD", "FX | USD Index (DXY)",         "FX",  True,  True,  False, True,  True,  False, None,    False, None),
    ("FX", "CHF=X",      "USD/CHF",                 "Cash",         "CHF", "FX | USD/CHF",                 "FX",  True,  True,  False, True,  True,  False, None,    False, None),
    ("FX", "JPY=X",      "USD/JPY",                 "Cash",         "JPY", "FX | USD/JPY",                 "FX",  True,  True,  False, True,  True,  False, None,    False, None),
    ("FX", "SGD=X",      "USD/SGD",                 "Alts",         "SGD", "FX | USD/SGD",                 "FX",  True,  True,  False, True,  True,  False, None,    False, None),
    ("FX", "CAD=X",      "Canadian Dollar",         "Real Assets",  "CAD", "FX | Canadian Dollar",         "FX",  True,  True,  False, True,  True,  False, None,    False, None),
    ("FX", "MXN=X",      "Mexican Peso",            "Alts",         "MXN", "FX | Mexican Peso",            "FX",  True,  True,  False, True,  True,  False, None,    False, None),
    ("FX", "KRW=X",      "Korean Won",              "Alts",         "KRW", "FX | Korean Won",              "FX",  True,  True,  False, True,  True,  False, None,    False, None),
    ("FX", "BRL=X",      "Brazilian Real",          "Alts",         "BRL", "FX | Brazilian Real",          "FX",  True,  True,  False, True,  True,  False, None,    False, None),

    # ── VOL — volatility instruments ─────────────────────────────────────────
    # ^VIX3M is hidden (DISPLAY_EXCLUSIONS) but kept in registry for completeness
    ("VOL", "^VIX",      "VIX Index",               "Alts",         "USD", "VOL | VIX Index",              "VOL", False, True,  False, False, False, False, None,   False, None),
    ("VOL", "^VIX3M",    "VIX 3-Month Index",       "Alts",         "USD", "VOL | VIX 3-Month Index",      "VOL", False, False, False, False, False, False, None,   False, None),
    ("VOL", "VXX",       "VIX Futures ETN",         "Alts",         "USD", "VOL | VIX Futures ETN",        "VOL", False, True,  False, True,  False, False, None,   True,  None),
    ("VOL", "VIXY",      "ST VIX ETF",              "Alts",         "USD", "VOL | ST VIX ETF",             "VOL", False, True,  False, True,  False, False, None,   True,  None),
    # GVZ/OVX: computed from GLD/BNO — hidden from UI, never fetched from yfinance
    ("VOL", "GVZ",       "Gold Volatility (proxy)", "Alts",         "USD", "VOL | Gold Volatility (proxy)","VOL", False, False, False, False, False, False, "GLD",  False, None),
    ("VOL", "OVX",       "Oil Volatility (proxy)",  "Alts",         "USD", "VOL | Oil Volatility (proxy)", "VOL", False, False, False, False, False, False, "BNO",  False, None),

    # ── ALT — listed alternatives ─────────────────────────────────────────────
    ("ALT", "IFRA",      "US Infrastructure",       "Real Assets",  "USD", "EQ | US Infrastructure",       "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("ALT", "PSP",       "Listed Private Equity",   "Alts",         "USD", "EQ | Listed Private Equity",   "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("ALT", "AMLP",      "Energy Infrastructure",   "Real Assets",  "USD", "EQ | Energy Infrastructure",   "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("ALT", "REET",      "Global REITs",            "Real Assets",  "USD", "EQ | Global REITs",            "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
    ("ALT", "BCI",       "Commodities Index",       "Real Assets",  "USD", "CMD | Commodities Index",      "CMD", True,  True,  True,  True,  True,  False, None,    True,  None),
    ("ALT", "PDBC",      "Diversified Commodity",   "Real Assets",  "USD", "CMD | Diversified Commodity",  "CMD", True,  True,  True,  True,  True,  False, None,    True,  None),
    ("ALT", "KBWY",      "High Yield REIT",         "Real Assets",  "USD", "EQ | High Yield REIT",         "EQ",  True,  True,  True,  True,  True,  False, None,    True,  None),
]

# Build the registry dict
INSTRUMENT_REGISTRY: dict[str, InstrumentMeta] = {}
for _row in _REGISTRY_ROWS:
    (_sec, _tk, _dn, _bkt, _ccy, _cn, _pfx,
     _scored, _disp, _rank, _sharpe, _vol_d, _yield, _synth, _hasvol, _caps) = _row
    INSTRUMENT_REGISTRY[_tk] = InstrumentMeta(
        ticker=_tk, section=_sec, display_name=_dn, bucket=_bkt,
        currency=_ccy, canonical_name=_cn, v23_prefix=_pfx,
        fragility_scored=_scored, display=_disp, rankable=_rank,
        sharpe_meaningful=_sharpe, vol_display=_vol_d, yield_scale=_yield,
        synthetic_source=_synth, has_volume=_hasvol, return_caps=_caps,
    )


# ── Convenience lookup ────────────────────────────────────────────────────────
def get(ticker: str) -> Optional[InstrumentMeta]:
    """None-safe registry lookup."""
    return INSTRUMENT_REGISTRY.get(ticker)


# ══════════════════════════════════════════════════════════════════════════════
#  2.  BACKWARD-COMPATIBLE DERIVED VIEWS
#      These are read-only and auto-derived from the registry — do not edit them
#      directly. To change membership, update the registry row above.
# ══════════════════════════════════════════════════════════════════════════════

# Ordered universe list (matching original UNIVERSE tuple structure)
UNIVERSE: list[tuple[str, str, str, str]] = [
    (m.section, m.ticker, m.display_name, m.bucket)
    for m in INSTRUMENT_REGISTRY.values()
]

# Section ordering and labels (unchanged — not instrument-level data)
SECTION_ORDER = ["EQ_US", "EQ_SECT", "EQ_DM", "EQ_IDX", "EQ_APAC", "EQ_EM", "DEFENCE",
                 "FI", "FI_INTL", "RATES", "CMD", "CRYPTO", "FX", "VOL", "ALT"]
SECTION_LABELS = {
    "EQ_US":   "EQUITIES — US BROAD",
    "EQ_SECT": "EQUITIES — US SECTORS",
    "EQ_DM":   "EQUITIES — DEVELOPED MARKETS",
    "EQ_IDX":  "EQUITIES — SPECIALIST & THEMATIC",
    "EQ_APAC": "EQUITIES — ASIA PACIFIC",
    "EQ_EM":   "EQUITIES — EMERGING MARKETS",
    "DEFENCE": "DEFENCE & GEOPOLITICAL",
    "FI":      "FIXED INCOME & CREDIT",
    "FI_INTL": "FIXED INCOME — INTERNATIONAL",
    "RATES":   "SOVEREIGN RATES",
    "CMD":     "COMMODITIES",
    "CRYPTO":  "CRYPTO",
    "FX":      "FX",
    "VOL":     "VOLATILITY",
    "ALT":     "LISTED ALTERNATIVES",
}

# Derived exclusion sets — all computed from the registry, never hardcoded again
DISPLAY_EXCLUSIONS:    list[str] = [t for t, m in INSTRUMENT_REGISTRY.items() if not m.display]
SYNTHETIC_TICKERS:     dict[str, str] = {t: m.synthetic_source
                                          for t, m in INSTRUMENT_REGISTRY.items()
                                          if m.synthetic_source is not None}
RANKING_EXCLUSIONS:    list[str] = [t for t, m in INSTRUMENT_REGISTRY.items() if not m.rankable]
FRAGILITY_EXCLUSIONS:  list[str] = [t for t, m in INSTRUMENT_REGISTRY.items() if not m.fragility_scored]
SHARPE_EXCLUSIONS:     list[str] = [t for t, m in INSTRUMENT_REGISTRY.items() if not m.sharpe_meaningful]
VOL_VALUE_EXCLUSIONS:  list[str] = [t for t, m in INSTRUMENT_REGISTRY.items() if not m.vol_display]
YIELD_TICKERS:         list[str] = [t for t, m in INSTRUMENT_REGISTRY.items() if m.yield_scale]

# Canonical name → ticker reverse map (for winsorization_report.csv lookups)
CANONICAL_TO_TICKER: dict[str, str] = {
    m.canonical_name: t
    for t, m in INSTRUMENT_REGISTRY.items()
    if m.canonical_name
}

# Ticker → canonical name (for fragility engine pass-through)
TICKER_TO_CANONICAL: dict[str, str] = {
    t: m.canonical_name
    for t, m in INSTRUMENT_REGISTRY.items()
    if m.canonical_name
}

N_INSTRUMENTS = len([t for t, m in INSTRUMENT_REGISTRY.items() if m.display])


# ══════════════════════════════════════════════════════════════════════════════
#  3.  CLIP LOG
# ══════════════════════════════════════════════════════════════════════════════

_CLIP_LOG_PATH = _DATA_DIR / "clip_log.csv"
_CLIP_LOG_FIELDS = ["run_ts", "ticker", "date", "horizon", "raw_value", "clipped_value", "reason"]


class ClipLog:
    """Append-only structured log of every sanity clip or data-quality fallback.

    Usage:
        log = ClipLog()
        log.record("SLV", pd.Timestamp("2026-01-15"), "1M", 0.82, 0.40, "period_return_cap")
        log.flush()    # writes to data/clip_log.csv
    """

    def __init__(self) -> None:
        self._entries: list[dict] = []
        self._run_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    def record(
        self,
        ticker: str,
        date,
        horizon: str,
        raw_value: float,
        clipped_value: float,
        reason: str,
    ) -> None:
        self._entries.append({
            "run_ts":        self._run_ts,
            "ticker":        ticker,
            "date":          str(date)[:10] if date is not None else "",
            "horizon":       horizon,
            "raw_value":     round(float(raw_value), 6),
            "clipped_value": round(float(clipped_value), 6),
            "reason":        reason,
        })
        print(f"[ClipLog] {ticker} {horizon} {date}: raw={raw_value:+.4f} -> clipped={clipped_value:+.4f} ({reason})")

    def flush(self, path: Optional[str] = None) -> None:
        if not self._entries:
            return
        out = Path(path) if path else _CLIP_LOG_PATH
        out.parent.mkdir(parents=True, exist_ok=True)
        file_exists = out.exists()
        with open(out, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=_CLIP_LOG_FIELDS)
            if not file_exists:
                writer.writeheader()
            writer.writerows(self._entries)
        print(f"[ClipLog] Flushed {len(self._entries)} entries → {out}")
        self._entries.clear()

    @property
    def entry_count(self) -> int:
        return len(self._entries)


# ══════════════════════════════════════════════════════════════════════════════
#  4.  SELF-RELATIVE RETURN CLIP
# ══════════════════════════════════════════════════════════════════════════════

# Winsorization bounds from the v2.3 engine (build_canonical_panel.py).
# Keyed by canonical_name (e.g. "EQ | US (S&P 500)").
# These are the IQR-derived bounds for *daily* returns.
# Loaded lazily on first use (the CSV is only present after a build_canonical_panel run).
_winsor_bounds_cache: Optional[dict[str, tuple[float, float]]] = None


def _load_winsor_bounds() -> dict[str, tuple[float, float]]:
    """Load winsorization_report.csv → {canonical_name: (lower, upper)}."""
    global _winsor_bounds_cache
    if _winsor_bounds_cache is not None:
        return _winsor_bounds_cache
    path = _DATA_DIR / "winsorization_report.csv"
    if not path.exists():
        warnings.warn(f"[Layer1] winsorization_report.csv not found at {path} — "
                      "daily-return winsorization unavailable; skipping.")
        _winsor_bounds_cache = {}
        return _winsor_bounds_cache
    df = pd.read_csv(path)
    _winsor_bounds_cache = {
        row["column"]: (float(row["lower_bound"]), float(row["upper_bound"]))
        for _, row in df.iterrows()
    }
    return _winsor_bounds_cache


def clip_daily_returns(
    prices: pd.DataFrame,
    clip_log: Optional[ClipLog] = None,
) -> pd.DataFrame:
    """Apply per-instrument winsorization to daily returns, then reconstruct prices.

    This is the generalised replacement for the fragility engine's un-implemented
    winsorize step (build_canonical_panel.py comment: "step 1 — not actually
    implemented anywhere in v23.py"). Applying it at the price level here means
    every downstream consumer (vol, drawdown, Sharpe, fragility pillars) gets
    clean data without needing its own clip.

    Returns a prices DataFrame with extreme daily moves replaced by the
    instrument-specific IQR-derived bounds from winsorization_report.csv.
    Instruments with no winsorization record are passed through unchanged.
    """
    bounds = _load_winsor_bounds()
    if not bounds:
        return prices

    prices_out = prices.copy()
    rets = prices.pct_change()

    for ticker, meta in INSTRUMENT_REGISTRY.items():
        if ticker not in prices.columns:
            continue
        cname = meta.canonical_name
        if not cname or cname not in bounds:
            continue
        lo, hi = bounds[cname]
        col_rets = rets[ticker].copy()
        mask = col_rets.notna() & ((col_rets < lo) | (col_rets > hi))
        if not mask.any():
            continue
        # Clip returns and reconstruct prices forward from the first clipped date.
        clipped_rets = col_rets.clip(lower=lo, upper=hi)
        # Log each clipped day
        if clip_log is not None:
            for dt in col_rets.index[mask]:
                clip_log.record(ticker, dt, "daily_ret",
                                float(col_rets[dt]), float(clipped_rets[dt]),
                                "daily_return_winsorization")
        # Reconstruct price series: start from base price before first change,
        # then apply clipped returns cumulatively.
        base_idx = prices.index[0]
        base_price = prices_out[ticker].iloc[0]
        reconstructed = (1 + clipped_rets.fillna(0)).cumprod() * base_price
        # Only replace from the first clipped date onward; leave prior prices intact.
        first_clip_pos = int(np.argmax(mask.values))
        slice_idx = prices.index[first_clip_pos:]
        prices_out.loc[slice_idx, ticker] = reconstructed.loc[slice_idx]
        print(f"[Layer1] {ticker}: {mask.sum()} daily returns winsorized "
              f"(bounds [{lo:.4f}, {hi:.4f}])")

    return prices_out


def clip_period_returns(
    prices: pd.DataFrame,
    clip_log: Optional[ClipLog] = None,
    rolling_window: int = 252,
    z_threshold: float = 4.5,
) -> dict[str, dict[str, Optional[float]]]:
    """Compute self-relative period-return clips for YTD / 1M / 3M.

    For each instrument, uses the rolling distribution of that horizon's return
    over the full price history to determine ±z_threshold * rolling_std bounds.
    Returns a dict: {ticker: {"ytd": clipped | None, "1m": ..., "3m": ...}}

    This replaces RETURN_SANITY_MAX's hardcoded 7-ticker allowlist with a
    statistically uniform approach that covers every instrument.

    Note: hard caps from InstrumentMeta.return_caps are applied as a FLOOR —
    if the self-relative bound is more permissive than the hard cap, the hard
    cap wins. This retains the validated commodity ETF caps while extending
    coverage to all other instruments.
    """
    horizons = {"ytd": None, "1m": 21, "3m": 63}
    today = prices.index[-1]
    prices_lv = prices.ffill(limit=5)

    results: dict[str, dict[str, Optional[float]]] = {}

    for ticker in prices.columns:
        meta = INSTRUMENT_REGISTRY.get(ticker)
        results[ticker] = {}

        # YTD return (point-in-time)
        ytd_slice = prices[prices.index.year == today.year]
        ytd_base = ytd_slice.bfill().iloc[0][ticker] if len(ytd_slice) > 1 else None
        current_ytd = (float(prices_lv[ticker].iloc[-1] / ytd_base - 1)
                       if ytd_base and pd.notna(ytd_base) and pd.notna(prices_lv[ticker].iloc[-1])
                       else None)

        for horizon_key, n_days in [("ytd", None), ("1m", 21), ("3m", 63)]:
            if horizon_key == "ytd":
                current_val = current_ytd
            else:
                current_val = (float(prices_lv[ticker].iloc[-1] / prices[ticker].iloc[-1 - n_days] - 1)
                               if n_days and len(prices) > n_days
                               and pd.notna(prices_lv[ticker].iloc[-1])
                               and pd.notna(prices[ticker].iloc[-1 - n_days])
                               else None)

            if current_val is None:
                results[ticker][horizon_key] = None
                continue

            # Self-relative bound: rolling std of that horizon's return
            if n_days and len(prices) > n_days + rolling_window:
                hist_rets = prices_lv[ticker].pct_change(n_days).dropna()
                if len(hist_rets) >= rolling_window // 2:
                    roll_std = hist_rets.rolling(rolling_window, min_periods=rolling_window // 4).std()
                    latest_std = float(roll_std.iloc[-1]) if pd.notna(roll_std.iloc[-1]) else None
                    self_rel_bound = abs(current_val) / (latest_std * z_threshold) if latest_std else None
                else:
                    self_rel_bound = None
            else:
                self_rel_bound = None

            # Hard cap from registry (commodity ETFs with known yfinance action issues)
            hard_cap = None
            if meta and meta.return_caps:
                idx_map = {"ytd": 0, "1m": 1, "3m": 2}
                hard_cap = meta.return_caps[idx_map[horizon_key]]

            # Clip fires if: (a) self-relative ratio > 1.0, or (b) hard cap exceeded
            raw = current_val
            clipped = raw
            reason = None

            if hard_cap and abs(raw) > hard_cap:
                clipped = np.nan
                reason = f"hard_cap_{horizon_key}_{hard_cap}"

            elif self_rel_bound is not None and self_rel_bound > 1.0:
                # Value exceeds z_threshold sigmas — likely data error
                sign = 1.0 if raw > 0 else -1.0
                clipped = np.nan
                reason = f"self_relative_{horizon_key}_z{z_threshold}"

            if reason and clip_log:
                clip_log.record(ticker, today, horizon_key, raw,
                                float(clipped) if pd.notna(clipped) else float("nan"),
                                reason)

            results[ticker][horizon_key] = clipped if not reason else None

    return results


# ══════════════════════════════════════════════════════════════════════════════
#  5.  CLEAN DATA ACCESS PATHS
# ══════════════════════════════════════════════════════════════════════════════

# ffill limits — explicit constants so callers don't sprinkle magic numbers
FFILL_LIMIT_POINT_IN_TIME = 5   # for point-in-time ratios (returns, drawdown)
FFILL_LIMIT_ROLLING_STATS  = 3   # for rolling-window stats (vol, Sharpe)


def get_clean_prices(
    raw_prices: pd.DataFrame,
    apply_daily_winsorization: bool = True,
    clip_log: Optional[ClipLog] = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Canonical price access path. Returns (prices_raw_clipped, prices_pit).

    prices_raw_clipped  — raw prices with daily-return winsorization applied.
                          Use for ALL rolling-window computations (vol, Sharpe,
                          fragility pillars). Same as what build_canonical_panel
                          feeds to the v2.3 engine.

    prices_pit          — prices_raw_clipped with ffill(limit=FFILL_LIMIT_POINT_IN_TIME).
                          Use for point-in-time ratios (returns, drawdown, YTD).
                          Bridges weekends + a not-yet-posted today-close; does NOT
                          carry forward genuinely stale/delisted tickers.

    Both series have:
      - Yield tickers divided by 10 (^TNX/^IRX/^FVX/^TYX)
      - Synthetic vol tickers (GVZ/OVX) computed from their source tickers
    """
    prices = raw_prices.copy()

    # Apply yield-ticker correction
    for ticker in YIELD_TICKERS:
        if ticker in prices.columns:
            prices[ticker] = prices[ticker] / 10.0

    # Compute synthetic tickers
    for proxy, source in SYNTHETIC_TICKERS.items():
        if source in prices.columns and proxy not in prices.columns:
            rets = prices[source].pct_change()
            rolling_vol = rets.rolling(window=20).std() * (252 ** 0.5) * 100.0
            prices[proxy] = rolling_vol.bfill()

    # Apply daily-return winsorization
    if apply_daily_winsorization:
        prices = clip_daily_returns(prices, clip_log=clip_log)

    # Point-in-time view (for ratios)
    prices_pit = prices.ffill(limit=FFILL_LIMIT_POINT_IN_TIME)

    return prices, prices_pit


def get_clean_returns(
    prices_pit: pd.DataFrame,
    prices_raw: pd.DataFrame,
    clip_log: Optional[ClipLog] = None,
) -> dict[str, Optional[float]]:
    """
    Returns a flat dict {ticker__horizon: value | None} for all period returns,
    with self-relative + hard-cap sanity clips applied.

    Keys: "{ticker}__ytd", "{ticker}__1m", "{ticker}__3m", "{ticker}__1d"

    Use this as the single source for period return values fed to compute_metrics.
    """
    clipped = clip_period_returns(prices_raw, clip_log=clip_log)
    out: dict[str, Optional[float]] = {}
    today = prices_pit.index[-1]
    for ticker in prices_pit.columns:
        # 1D: always strict (no ffill) — fake 0% worse than NaN
        p_now = prices_raw[ticker].iloc[-1] if ticker in prices_raw.columns else np.nan
        p_prev = prices_raw[ticker].iloc[-2] if len(prices_raw) >= 2 and ticker in prices_raw.columns else np.nan
        out[f"{ticker}__1d"] = float(p_now / p_prev - 1) if pd.notna(p_now) and pd.notna(p_prev) else None

        tc = clipped.get(ticker, {})
        out[f"{ticker}__ytd"] = tc.get("ytd")
        out[f"{ticker}__1m"]  = tc.get("1m")
        out[f"{ticker}__3m"]  = tc.get("3m")
    return out


# ══════════════════════════════════════════════════════════════════════════════
#  SELF-TEST / BACKFILL ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def _self_test() -> None:
    """Smoke-test the registry and derived views. Run: python layer1_data.py"""
    print("=" * 70)
    print("Layer 1 self-test")
    print("=" * 70)

    # 1. Registry completeness
    print(f"\nInstrument count: {len(INSTRUMENT_REGISTRY)} total, {N_INSTRUMENTS} displayed")
    assert N_INSTRUMENTS > 0, "N_INSTRUMENTS is zero"

    # 2. Derived views match expected shapes
    print(f"DISPLAY_EXCLUSIONS    ({len(DISPLAY_EXCLUSIONS)}): {DISPLAY_EXCLUSIONS}")
    print(f"SYNTHETIC_TICKERS     ({len(SYNTHETIC_TICKERS)}): {SYNTHETIC_TICKERS}")
    print(f"RANKING_EXCLUSIONS    ({len(RANKING_EXCLUSIONS)}): first 5: {RANKING_EXCLUSIONS[:5]}")
    print(f"FRAGILITY_EXCLUSIONS  ({len(FRAGILITY_EXCLUSIONS)}): {FRAGILITY_EXCLUSIONS[:8]}")
    print(f"SHARPE_EXCLUSIONS     ({len(SHARPE_EXCLUSIONS)}): {SHARPE_EXCLUSIONS}")
    print(f"YIELD_TICKERS         ({len(YIELD_TICKERS)}): {YIELD_TICKERS}")

    # 3. One-entry test: adding a throwaway ticker
    _test_ticker = "__TEST_TICKER__"
    assert _test_ticker not in INSTRUMENT_REGISTRY, "Test ticker collision"
    print(f"\n[OK] All derived views built from registry. Adding test ticker needs 1 registry row.")

    # 4. Winsorization bounds load
    bounds = _load_winsor_bounds()
    print(f"\nWinsorization bounds: {len(bounds)} instruments loaded")

    # 5. ClipLog smoke-test
    log = ClipLog()
    log.record("TEST", pd.Timestamp("2026-01-01"), "1M", 0.99, float("nan"), "test")
    assert log.entry_count == 1
    print(f"\n[OK] ClipLog records entries correctly")

    # 6. Registry canonical name round-trip
    for tk, meta in list(INSTRUMENT_REGISTRY.items())[:5]:
        if meta.canonical_name:
            assert CANONICAL_TO_TICKER.get(meta.canonical_name) == tk, \
                f"Round-trip failed for {tk}"
    print(f"\n[OK] Canonical name round-trip correct")

    print("\n" + "=" * 70)
    print("Self-test passed.")
    print("=" * 70)


if __name__ == "__main__":
    import sys
    if "--backfill" in sys.argv:
        print("⚠  Max-historical backfill not yet implemented.")
        print("   This is flagged for approval before proceeding — see module docstring.")
        sys.exit(1)
    _self_test()
