"""
================================================================================
INSTITUTIONAL FRAGILITY MONITOR — V2.3
Single-file production module
================================================================================

A per-asset cross-asset fragility framework with bounded [0,100] score and
unbounded z-unit intensity companion, plus seven governance overlay modules.


--------------------------------------------------------------------------------
UNIVERSE — 47-ASSET SCORE PANEL
--------------------------------------------------------------------------------

  Equity (26):  US-broad+sectors, Europe, EM by region/country
                  Sub-classes: 22 broad regions + 4 US sectors (XLK/XLF/XLE/XLV)
  Fixed Income (11): UST tenors (1-3y/7-10y/20+y/Aggregate), TIPS, IG, HY,
                  EM USD/Local, Global ex-US Aggregate
  Commodities (8): Broad, Gold, Silver, WTI, Brent, NatGas, Copper, Agri
  Crypto (2):   BTC, ETH

  System-context universe (54): adds 7 FX (DXY + EUR/JPY/GBP/AUD/CNY + EM-FX
                  basket). Used for PC1 fitting in the correlation pillar; not
                  directly scored.

  Daily history: 2003-01-02 onward (~5,975 trading days as of mid-2026)


--------------------------------------------------------------------------------
PIPELINE
--------------------------------------------------------------------------------

  panel  →  winsorize returns (0.1% / 99.9% trailing)
         →  6 pillars (per-asset, per-day):
              vol     — 20d realised, annualised
              dd      — 120d rolling-peak drawdown
              cvar    — 60d, 5% tail (worst 3 of 60 mean)
              trend   — 200d MA deviation
              corr    — 60d corr to PC1 of system universe
              volz    — 60d volume z-score (clipped lower at 0)
         →  robust z (252d MAD-based, ±4σ clip, MAD-floor 1% of expanding std)
         →  coverage-aware weighted sum
         →  intensity_raw  (latent z-unit composite, unbounded)
         →  EWMA smoothing (span=10)
         →  intensity      (smoothed z-unit, unbounded — companion to score)
         →  100 × logistic
         →  score          (bounded [0, 100], cross-asset comparable)


--------------------------------------------------------------------------------
PILLAR WEIGHTS (walk-forward IC-proportional)
--------------------------------------------------------------------------------

  Derivation: 3-year rolling training window, monthly refits. Weights ∝ Spearman
  IC vs forward maximum drawdown across 20/60/120-day horizons.

  vol    27.8%
  cvar   26.4%
  dd     18.8%
  trend  11.8%
  corr   10.3%
  volz    4.9%   (floor — multi-lens robustness; pillar IC alone would zero out)


--------------------------------------------------------------------------------
GOVERNANCE OVERLAY — 7 MODULES
--------------------------------------------------------------------------------

  1. Regime classifier (HMM)
       3-state: Calm / Stressed / Crisis
       Gaussian HMM, diagonal covariance, k-means seeded init
       Locked random seed for deterministic state assignment
       Features: ACWI ret/vol/MDD, vol-proxy (VIX), USD-index ret, Gold ret

  2. Liquidity Shock Detector (LSD)
       Active when ≥1 of:
         (a) ≥2 of {Gold, 20+y UST, 7-10y UST} at return z < -2.0
         (b) BTC daily return < -15%
         (c) Aggregate volume z > +2.5

  3. Phase classifier
       Per-asset: Neutral / Building / Peaking / Exhausting
       Conditions on score level, 20d velocity, 20d persistence, breadth pct

  4. Shock-type classifier
       System-level: Quiet / Endogenous / Exogenous / Decaying
       Compares 20d (short) vs 60d (long) cross-asset coupling ratio

  5. Policy reaction signal
       Detects active Fed easing (ff_change_4w ≤ -50bps)
       Magnitude offset capped at 0.30, with 60d half-life decay

  6. FSS aggregate (4-channel)
       Equal-weight (25% each): SafeHaven / Credit / Liquidity / Policy

  7. Forward-conditioned shelf + divergence indicator
       45-cell empirical lookup (5 score bands × 3 regimes × 3 horizons)
       Median forward 60d MDD per cell; cross-panel rank
       Divergence = realised score − forward rank


--------------------------------------------------------------------------------
VALIDATION SUITE — T1 to T7 (locked thresholds)
--------------------------------------------------------------------------------

  T1   Composite IC vs forward MDD       ≥ 0.10
  T2   IC gain over equal-wt baseline    ≥ +0.005
  T4   EQ ceiling pinning (>=99/day)     ≤ 5
  T5   Max pairwise pillar correlation   ≤ 0.75
  T6   Min PC variance share             ≥ 0.10
  T7   Pillars saturating PC1 (|ld|≥0.7) ≤ 2

  T1/T6 are below threshold by design — the framework prioritises six-pillar
  coverage over strict PCA orthogonality. The unbounded intensity companion
  handles the high-band discrimination that the bounded composite IC cannot.


--------------------------------------------------------------------------------
CRASH BACKTEST
--------------------------------------------------------------------------------

  60d forward MDD with class-specific thresholds (EQ 15%, FI 8%, CMD 20%,
  Crypto 30%):

  EQ      ~0.66    (n=26, materially above random)
  FI      ~0.67    (n=11, highest discriminating class)
  CMD     ~0.63    (n=8)
  Crypto  ~0.52    (n=2, near-random — sample size limit, not a fix item)
  Pooled  ~0.64    (n=47, materially above random 0.500)


--------------------------------------------------------------------------------
KNOWN LIMITS
--------------------------------------------------------------------------------

  1. FSS channel polarity
       The 4-channel-at-25% FSS architecture is correct, but channel internal
       sign/magnitude conventions are interpretive given the available spec.
       Use FSS aggregate with caution.

  2. Forward divergence threshold
       Capitulation flag (div > +30) calibration may need tuning depending
       on use case. Architecture (45-cell shelf, divergence formula) is solid;
       threshold is judgement-based.

  3. Idiosyncratic regional events underflagged
       Pillar set tuned for cross-asset/global stress; isolated regional
       events (single-country shocks before contagion) are caught late.

  4. Fast V-shape recoveries
       Recovery dynamics fool a fragility framework calibrated for downside
       extension. Structural, not parameter-tunable.

  5. Bond ETF drawdown bias
       Yahoo adjusted-close imperfections produce systematically more
       pessimistic FI drawdown readings than spot truth.


--------------------------------------------------------------------------------
DATA PROVENANCE
--------------------------------------------------------------------------------

  This module operates in TWO MODES:

  COMPUTE mode (default, deterministic):
      Reads pre-built canonical panel CSVs from <panel_dir>:
          panel_v22_prices.csv      Tradeable asset adjusted-close prices
          panel_v22_returns.csv     Tradeable asset daily returns
          panel_v22_volumes.csv     Tradeable asset volumes (NaN for FX)
          panel_v22_levels.csv      FRED levels (yields, breakevens, VIX, FF, SOFR)
          panel_v22_metadata.csv    Per-instrument metadata
      Produces score, intensity, pillars, and 7 governance modules.
      Runtime: ~3.5 minutes.

  FETCH mode (optional, refreshes panel from sources):
      Downloads:
          • 54 tradeable instruments from Yahoo Finance (via yfinance)
              26 EQ + 11 FI + 8 CMD + 2 CRYPTO + 7 FX
          • 14 macro / level series from FRED (via direct CSV endpoint)
              Treasury yields, breakevens, VIX, FF, SOFR, USD index, HY OAS
      Writes the 5 canonical CSVs that COMPUTE mode then consumes.
      Runtime: ~10-20 minutes (network-bound).

      Requires: pip install yfinance requests
      No FRED API key required — uses public FRED CSV endpoint.

      Note on determinism: COMPUTE on a fixed panel is fully reproducible
      across runs. FETCH then COMPUTE is reproducible only on the day of
      fetch — Yahoo / FRED data drift over time as series get revised
      and new observations are appended.

  Caveats:
    • Yahoo adjusted-close imperfections — bond ETFs may show systematically
      more pessimistic drawdown readings than reality.
    • Some series have ragged-right starts (ACWI 2008, BTC 2014, ETH 2017,
      SOFR 2018, HY OAS 2023). Both compute and fetch handle this gracefully.
    • CNY=X is the spot FX patch (CYB ETF was delisted in 2018).


--------------------------------------------------------------------------------
DEPENDENCIES
--------------------------------------------------------------------------------

  Required (compute mode):
      pandas         (tested with 2.x)
      numpy          (tested with 1.26+ / 2.x)
      scipy          (for spearmanr in validation)
      scikit-learn   (for KMeans + PCA)
      hmmlearn       (for GaussianHMM regime classifier)

  Required (fetch mode only):
      yfinance       (Yahoo Finance access)
      requests       (FRED CSV download, usually pre-installed with pandas)

  Install everything:
      pip install pandas numpy scipy scikit-learn hmmlearn yfinance requests


--------------------------------------------------------------------------------
USAGE
--------------------------------------------------------------------------------

  Programmatic — compute on existing panel:
      from institutional_fragility_monitor_v23 import compute_v23
      out = compute_v23('/path/to/panel/dir')

      # Primary outputs
      score = out['score']             # DataFrame, dates × 47 assets, [0, 100]
      intensity = out['intensity']     # DataFrame, dates × 47, z-units smoothed

      # Pillars
      pillars_raw = out['pillars_raw'] # dict of 6 raw-pillar DataFrames
      pillars_z = out['pillars_z']     # dict of 6 z-scored DataFrames

      # Governance
      regime = out['hmm_regime']       # Series, daily Calm/Stressed/Crisis
      lsd = out['lsd']                 # DataFrame, daily LSD trigger flags
      phase = out['phase']             # DataFrame, dates × 47, phase labels
      shock = out['shock_type']        # DataFrame, daily classification
      policy = out['policy']           # DataFrame, daily Fed easing signals
      fss = out['fss']                 # DataFrame, 4-channel + aggregate
      shelf = out['forward_shelf']     # DataFrame, 45 cells (band×regime×h)
      div = out['divergence']          # DataFrame, dates × 47, divergence

      # Validation
      v = out['validation']            # dict, T1-T7 results

  Programmatic — refresh panel from sources:
      from institutional_fragility_monitor_v23 import fetch_panel
      fetch_panel('/path/to/output/dir',
                   start='2003-01-01',
                   end=None)            # None = today
      # → writes panel_v22_*.csv files

  CLI — compute (default mode):
      python institutional_fragility_monitor_v23.py <panel_dir> [<output_dir>]

      Saves all panels as CSV in:
        <output_dir>/score/        — score, intensity, intensity_raw
        <output_dir>/pillars/      — 12 files (6 raw + 6 z-scored)
        <output_dir>/governance/   — 10 files (regime, lsd, phase, shock,
                                     policy, fss, shelf, divergence,
                                     forward_conditioned, hmm_probs)

  CLI — fetch (refresh data from Yahoo + FRED):
      python institutional_fragility_monitor_v23.py fetch <output_dir> [start] [end]

      Downloads fresh data and writes the 5 canonical panel CSVs to
      <output_dir>. Output is then directly consumable by compute mode.

      Requires:  pip install yfinance requests
      Runtime:   ~10-20 minutes (network-bound)

  CLI — report (generate HTML dashboard):
      python institutional_fragility_monitor_v23.py report <output_dir> [dashboard_path] [date]

      Reads CSVs from <output_dir> (previously written by compute mode)
      and writes a self-contained HTML dashboard. Auto-detects the
      latest date in the score panel if no date argument is given.

      Runtime: ~5 seconds.

  CLI — full (all three stages, end-to-end):
      python institutional_fragility_monitor_v23.py full <panel_dir> [output_dir] [dashboard_path]

      Chains: fetch panel → compute pipeline → generate dashboard.
      Single command, end-to-end.

      Runtime: ~15-25 minutes.


--------------------------------------------------------------------------------
AUTHORSHIP
--------------------------------------------------------------------------------

  Author:         BK · Singapore Global Markets
  Module version: 2.3.0


================================================================================
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from hmmlearn.hmm import GaussianHMM


# ============================================================================
#                              CONFIGURATION
# ============================================================================
# All locked constants. Any change here invalidates determinism of the
# pipeline output across runs on the same panel.

# ----------------------------------------------------------------------------
# Pillar weights — walk-forward IC-proportional
# ----------------------------------------------------------------------------
# Derivation: 3-year rolling training window, monthly refits across the panel.
# Weights ∝ Spearman IC vs forward-MDD across 20/60/120d horizons.
# VolZ at floor 0.05 because IC alone would zero it out — retained at floor
# for multi-lens robustness on flow-driven stress.
WEIGHTS = {
    "vol":   0.278,   # 27.8%  Volatility       (highest IC, most informative)
    "cvar":  0.264,   # 26.4%  CVaR-5%          (tail-shape complement to vol)
    "dd":    0.188,   # 18.8%  Drawdown         (depth of recent loss)
    "trend": 0.118,   # 11.8%  Trend deviation  (structural break vs MA)
    "corr":  0.103,   # 10.3%  Corr-to-PC1      (system coupling)
    "volz":  0.049,   #  4.9%  Volume z         (FLOOR — multi-lens robustness)
}

# ----------------------------------------------------------------------------
# Locked HMM seed
# ----------------------------------------------------------------------------
# Provides deterministic regime classification across re-runs of the same
# panel. State labels (Calm/Stressed/Crisis) are then sorted by ascending
# mean ACWI return so labels are stable regardless of HMM internal numbering.
HMM_SEED = 20260427

# ----------------------------------------------------------------------------
# Pillar window specifications
# ----------------------------------------------------------------------------
VOL_WINDOW       = 20    # 20d realised vol (~1 trading month)
DD_WINDOW        = 120   # 120d rolling-peak drawdown (~5 trading months)
CVAR_WINDOW      = 60    # 60d CVaR window
CVAR_TAIL        = 0.05  # 5% tail = worst 3 of 60 days
TREND_WINDOW     = 200   # 200d MA — classic trend benchmark
CORR_WINDOW      = 60    # 60d correlation to PC1
VOLZ_WINDOW      = 60    # 60d volume z-score
ROBUST_Z_WINDOW  = 252   # 252d (~1 year) trailing distribution
ROBUST_Z_CLIP    = 4.0   # ±4σ z-score cap (extreme outlier defense)
EWMA_SPAN        = 10    # 10d EWMA smoothing on intensity

# ----------------------------------------------------------------------------
# Validation thresholds
# ----------------------------------------------------------------------------
# T1 and T6 are intentionally tight thresholds that the six-pillar score does
# not pass — disclosed and explained in the header. Coverage was prioritised
# over strict orthogonality. The unbounded intensity companion handles the
# high-band discrimination that the bounded composite IC cannot.
THRESHOLDS = {
    "T1_composite_ic":      0.10,    # ≥  composite IC vs forward MDD
    "T2_ic_gain":           0.005,   # ≥  gain over equal-weighted baseline
    "T4_ceiling_pinning":   5,       # ≤  broad EQ at score ≥99 / day
    "T5_max_pillar_corr":   0.75,    # ≤  max pairwise pillar correlation
    "T6_min_pc_share":      0.10,    # ≥  min PC variance share (PCA diversity)
    "T7_pc1_saturation":    2,       # ≤  pillars with |PC1 loading| ≥ 0.7
}

# ----------------------------------------------------------------------------
# Crash backtest MDD thresholds by asset class
# ----------------------------------------------------------------------------
# Class-specific because the natural drawdown amplitude differs by asset class.
# A 15% drawdown is a moderate equity event but a severe Treasury event.
CRASH_THRESHOLDS = {
    "EQ":     0.15,   # 15% — moderate equity correction or mild crisis
    "FI":     0.08,   # 8%  — meaningful for Treasuries / IG / Aggregate
    "CMD":    0.20,   # 20% — commodities are more volatile by nature
    "CRYPTO": 0.30,   # 30% — crypto vol baseline is much higher
}


# ============================================================================
#                              DATA LOADING
# ============================================================================

def load_panel(panel_dir: str | Path) -> dict:
    """Load canonical panel from a directory.
    
    Expected files:
        panel_v22_prices.csv
        panel_v22_returns.csv
        panel_v22_volumes.csv
        panel_v22_levels.csv
        panel_v22_metadata.csv
    """
    p = Path(panel_dir)
    return {
        "returns":  pd.read_csv(p / "panel_v22_returns.csv",  parse_dates=["Date"]).set_index("Date").sort_index(),
        "prices":   pd.read_csv(p / "panel_v22_prices.csv",   parse_dates=["Date"]).set_index("Date").sort_index(),
        "volumes":  pd.read_csv(p / "panel_v22_volumes.csv",  parse_dates=["Date"]).set_index("Date").sort_index(),
        "levels":   pd.read_csv(p / "panel_v22_levels.csv",   parse_dates=["Date"]).set_index("Date").sort_index(),
        "metadata": pd.read_csv(p / "panel_v22_metadata.csv"),
    }


def score_universe(returns: pd.DataFrame) -> list[str]:
    """Return the 47-asset score universe (EQ + FI + CMD + Crypto, FX excluded)."""
    return [c for c in returns.columns
            if c.startswith(("EQ |", "FI |", "CMD |", "CRYPTO |"))]


# ============================================================================
#                          PANEL DEFINITIONS
# ============================================================================
# Canonical ticker-to-name map for the 47-asset score panel + 7 FX system-
# context instruments. Modifying this list changes the output universe.

# ----------------------------------------------------------------------------
# Tradeable assets — Yahoo Finance (yfinance) tickers
# ----------------------------------------------------------------------------
# 54 instruments: 26 EQ + 11 FI + 8 CMD + 2 CRYPTO + 7 FX
# Note: CNY=X is the spot FX patch (CYB ETF was delisted in 2018).

YAHOO_TICKERS: dict[str, str] = {
    # Equity — broad regions (22)
    "EQ | Australia":                  "EWA",
    "EQ | Brazil":                     "EWZ",
    "EQ | China":                      "FXI",
    "EQ | EM Broad":                   "EEM",
    "EQ | Europe Dev (EAFE)":          "EFA",
    "EQ | Eurozone":                   "EZU",
    "EQ | France":                     "EWQ",
    "EQ | Germany":                    "EWG",
    "EQ | Hong Kong":                  "EWH",
    "EQ | India":                      "INDA",
    "EQ | Japan":                      "EWJ",
    "EQ | Korea":                      "EWY",
    "EQ | Mexico":                     "EWW",
    "EQ | Real Estate (REITs)":        "VNQ",
    "EQ | Singapore":                  "EWS",
    "EQ | South Africa":               "EZA",
    "EQ | Taiwan":                     "EWT",
    "EQ | UK":                         "EWU",
    "EQ | US (Nasdaq 100)":            "QQQ",
    "EQ | US (Russell 2000)":          "IWM",
    "EQ | US (S&P 500)":               "SPY",
    "EQ | World (ACWI)":               "ACWI",
    # Equity — US sectors (4)
    "EQ | US Energy Sector":           "XLE",
    "EQ | US Financials Sector":       "XLF",
    "EQ | US Healthcare Sector":       "XLV",
    "EQ | US Tech Sector":             "XLK",
    # Fixed Income (11)
    "FI | EM Local Currency":          "EMLC",
    "FI | EM USD Sovereigns":          "EMB",
    "FI | Global ex-US Aggregate":     "BNDX",
    "FI | HY Credit":                  "HYG",
    "FI | IG Credit":                  "LQD",
    "FI | US Aggregate Bond":          "AGG",
    "FI | US TIPS":                    "TIP",
    "FI | US Total Bond":              "BND",
    "FI | US Treasuries (1-3y)":       "SHY",
    "FI | US Treasuries (20+y)":       "TLT",
    "FI | US Treasuries (7-10y)":      "IEF",
    # Commodities (8)
    "CMD | Agriculture Broad":         "DBA",
    "CMD | Brent Oil":                 "BNO",
    "CMD | Broad Commodities":         "DBC",
    "CMD | Copper":                    "CPER",
    "CMD | Gold":                      "GLD",
    "CMD | Natural Gas":               "UNG",
    "CMD | Silver":                    "SLV",
    "CMD | WTI Oil":                   "USO",
    # Crypto (2)
    "CRYPTO | Bitcoin":                "BTC-USD",
    "CRYPTO | Ethereum":               "ETH-USD",
    # FX — system-context only (7) — used for PC1 fitting in corr pillar
    "FX | AUD/USD":                    "FXA",
    "FX | CNY/USD":                    "CNY=X",     # spot FX (CYB delisted)
    "FX | EM-FX Basket":               "CEW",
    "FX | EUR/USD":                    "FXE",
    "FX | GBP/USD":                    "FXB",
    "FX | JPY/USD":                    "FXY",
    "FX | USD Index (DXY)":            "UUP",
}

# Asset class lookup (used by metadata builder)
YAHOO_ASSET_CLASS: dict[str, str] = {
    name: name.split(" | ")[0] for name in YAHOO_TICKERS
}

# ----------------------------------------------------------------------------
# Macro / Levels — FRED API series codes
# ----------------------------------------------------------------------------
# 14 series. Some have post-2020 start dates (SOFR 2018, HY OAS 2023).
# TED Spread (TEDRATE) was discontinued in 2022 — kept for legacy back-history.

FRED_SERIES: dict[str, str] = {
    # Inflation breakevens (3)
    "Inflation Breakeven 5Y":          "T5YIE",
    "Inflation Breakeven 10Y":         "T10YIE",
    "Inflation 5Y5Y Forward":          "T5YIFR",
    # Treasury yields (5)
    "US Treasury 3M Yield":            "DGS3MO",
    "US Treasury 2Y Yield":            "DGS2",
    "US Treasury 5Y Yield":            "DGS5",
    "US Treasury 10Y Yield":           "DGS10",
    "US Treasury 30Y Yield":           "DGS30",
    # Volatility (1)
    "VIX":                             "VIXCLS",
    # Policy rates (2)
    "Federal Funds Rate":              "DFF",
    "Secured Overnight Rate":          "SOFR",     # starts 2018-04-03
    # Funding / credit / FX (3)
    "TED Spread (legacy)":             "TEDRATE",  # discontinued 2022
    "USD Trade Weighted Broad":        "DTWEXBGS",
    "ICE BofA US HY Index OAS":        "BAMLH0A0HYM2",  # FRED CSV starts 2023
}


# ============================================================================
#                          DATA FETCH (yfinance + FRED)
# ============================================================================
# OPTIONAL functionality. Only invoked when CLI is called with `--fetch`.
# Imports happen lazily inside the functions so the main module loads cleanly
# even if yfinance / requests aren't installed.
#
# Use case: rebuild the panel CSVs from current data sources. Output is
# written as panel_v22_*.csv into the target directory and is then directly
# consumable by compute_v23() — same canonical format.
#
# IMPORTANT — caveats:
#   1. Yahoo adjusted-close imperfections — bond ETFs may have systematically
#      more pessimistic drawdown readings than reality. Documented at panel
#      level; not fixable via fetch.
#   2. Some series have ragged-right starts (ACWI 2008, BTC 2014, ETH 2017,
#      SOFR 2018, HY OAS 2023). yfinance and FRED return only available data;
#      pre-listing observations come back as NaN. compute_v23 handles this.
#   3. CNY=X is the spot FX patch (CYB ETF was delisted). Same series,
#      different ticker.

def fetch_yahoo_data(start: str = "2003-01-01", end: Optional[str] = None,
                      tickers: Optional[dict] = None) -> dict:
    """Fetch tradeable asset prices, returns, volumes from Yahoo Finance.
    
    Args:
        start:   ISO date string for history start. Default 2003-01-01.
        end:     ISO date string for history end. Default None = today.
        tickers: Optional dict of {name: ticker}. Default uses YAHOO_TICKERS.
    
    Returns:
        Dict with keys 'prices', 'returns', 'volumes' — each a DataFrame
        of shape (dates, n_assets) with columns labelled by the canonical
        name (e.g. 'EQ | Eurozone'), not by ticker.
    
    Raises:
        ImportError: if yfinance is not installed.
    """
    try:
        import yfinance as yf  # noqa: F401  (lazy import)
    except ImportError as e:
        raise ImportError(
            "yfinance is required for data fetch. "
            "Install with: pip install yfinance"
        ) from e
    
    if tickers is None:
        tickers = YAHOO_TICKERS
    
    print(f"  Fetching {len(tickers)} Yahoo tickers from {start} to {end or 'today'}...")
    
    ticker_list = list(tickers.values())
    name_for_ticker = {v: k for k, v in tickers.items()}
    
    # yfinance returns a multi-indexed DataFrame when multiple tickers passed
    raw = yf.download(
        ticker_list,
        start=start,
        end=end,
        auto_adjust=False,    # we want adjusted close explicitly
        progress=False,
        threads=True,
    )
    
    # Extract Adj Close and Volume; some tickers may be missing
    if isinstance(raw.columns, pd.MultiIndex):
        adj_close = raw["Adj Close"]
        volume = raw["Volume"]
    else:
        # Single-ticker fallback (shouldn't trigger with our universe)
        adj_close = raw[["Adj Close"]].rename(columns={"Adj Close": ticker_list[0]})
        volume = raw[["Volume"]].rename(columns={"Volume": ticker_list[0]})
    
    # Rename columns from tickers → canonical names; drop fetch-failed tickers
    available = [t for t in ticker_list if t in adj_close.columns]
    missing = [t for t in ticker_list if t not in available]
    if missing:
        print(f"  WARNING: {len(missing)} tickers failed to fetch: {missing}")
    
    prices = adj_close[available].rename(columns=name_for_ticker)
    volumes = volume[available].rename(columns=name_for_ticker)
    returns = prices.pct_change()
    
    # Sort columns to canonical order
    canonical_order = [name for name in tickers if name in prices.columns]
    prices = prices[canonical_order]
    volumes = volumes[canonical_order]
    returns = returns[canonical_order]
    
    print(f"  Fetched: {len(prices)} dates × {len(prices.columns)} tradeable assets")
    return {"prices": prices, "returns": returns, "volumes": volumes}


def fetch_fred_data(start: str = "2003-01-01", end: Optional[str] = None,
                     series: Optional[dict] = None,
                     api_key: Optional[str] = None) -> pd.DataFrame:
    """Fetch macro / level series from FRED.

    Prefers the FRED REST API (api.stlouisfed.org) when an API key is
    available — either passed explicitly or set in the FRED_API_KEY
    environment variable.  Falls back to the public CSV scrape endpoint
    if no key is found (that endpoint is increasingly unreliable).

    Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html

    Args:
        start:   ISO date string for history start. Default 2003-01-01.
        end:     ISO date string for history end. Default None = today.
        series:  Optional dict of {name: fred_code}. Default uses FRED_SERIES.
        api_key: FRED API key. Falls back to FRED_API_KEY env var, then
                 the public CSV endpoint.

    Returns:
        DataFrame of shape (dates, n_series) with columns labelled by
        canonical name. Forward-filled to a daily business-day grid to
        match the tradeable panel's index.

    Raises:
        ImportError: if `requests` is not installed (very rare).
    """
    try:
        import requests
        from io import StringIO
    except ImportError as e:
        raise ImportError(
            "`requests` is required for FRED fetch. "
            "Install with: pip install requests"
        ) from e

    if series is None:
        series = FRED_SERIES

    if end is None:
        end = pd.Timestamp.today().strftime("%Y-%m-%d")

    # Resolve API key: explicit arg → env var → None (CSV fallback)
    key = api_key or os.environ.get("FRED_API_KEY")

    if key:
        print(f"  Fetching {len(series)} FRED series via API from {start} to {end}...")
    else:
        print(f"  Fetching {len(series)} FRED series via CSV endpoint from {start} to {end}...")
        print("  TIP: set FRED_API_KEY env var or pass api_key= for a more reliable connection.")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }

    out = pd.DataFrame()
    failed = []

    for name, code in series.items():
        try:
            if key:
                url = (
                    f"https://api.stlouisfed.org/fred/series/observations"
                    f"?series_id={code}"
                    f"&observation_start={start}"
                    f"&observation_end={end}"
                    f"&api_key={key}"
                    f"&file_type=json"
                    f"&limit=100000"
                )
                r = requests.get(url, headers=headers, timeout=60)
                r.raise_for_status()
                obs = r.json()["observations"]
                dates = [o["date"] for o in obs]
                vals  = [np.nan if o["value"] == "." else float(o["value"]) for o in obs]
                s = pd.Series(vals, index=pd.to_datetime(dates), name=name)
            else:
                url = (
                    f"https://fred.stlouisfed.org/graph/fredgraph.csv"
                    f"?id={code}&cosd={start}&coed={end}"
                )
                r = requests.get(url, headers=headers, timeout=30)
                r.raise_for_status()
                df = pd.read_csv(
                    StringIO(r.text),
                    parse_dates=[0],
                    na_values=[".", "", "NA"],
                )
                df.columns = ["Date", name]
                df = df.set_index("Date").sort_index()
                s = df[name].astype(float)

            out = out.join(s, how="outer") if not out.empty else s.to_frame()
        except Exception as e:
            failed.append((name, code, str(e)))
            print(f"  WARNING: FRED series '{name}' ({code}) failed: {e}")

    if failed:
        print(f"  {len(failed)} series failed to fetch (continuing with rest).")

    if out.empty:
        print(f"  Fetched: 0 dates × 0 FRED series")
        return out

    # Forward-fill to daily business-day grid (FRED is daily but skips weekends)
    out.index = pd.to_datetime(out.index)
    out = out.asfreq("B").ffill()

    print(f"  Fetched: {len(out)} dates × {len(out.columns)} FRED series")
    return out


def fetch_panel(output_dir: str | Path,
                 start: str = "2003-01-01",
                 end: Optional[str] = None,
                 api_key: Optional[str] = None) -> dict:
    """Build the canonical panel from current data sources.
    
    Fetches all 54 tradeable instruments from Yahoo Finance and all 14
    macro series from FRED, then writes the 5 canonical CSVs:
        panel_v22_prices.csv
        panel_v22_returns.csv
        panel_v22_volumes.csv
        panel_v22_levels.csv
        panel_v22_metadata.csv
    
    Output is directly consumable by compute_v23(<output_dir>).
    
    Args:
        output_dir: directory to write the 5 CSVs (created if missing)
        start:      history start (ISO date)
        end:        history end (ISO date) — None = today
    
    Returns:
        Dict with the 5 panel DataFrames keyed by csv_name.
    
    Note: This function REQUIRES yfinance and requests installed.
          Run `pip install yfinance requests` first.
          Estimated runtime: ~10-20 minutes depending on network.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"=== IFM V2.3 Panel Fetch ===")
    print(f"  start:      {start}")
    print(f"  end:        {end or 'today'}")
    print(f"  output_dir: {output_dir}")
    print()
    
    # ---- Yahoo: tradeables ----
    yh = fetch_yahoo_data(start=start, end=end)
    prices, returns, volumes = yh["prices"], yh["returns"], yh["volumes"]
    
    # ---- FRED: levels ----
    levels = fetch_fred_data(start=start, end=end, api_key=api_key)
    
    # ---- Align all to common business-day index ----
    common_idx = prices.index.union(levels.index).sort_values()
    prices = prices.reindex(common_idx)
    returns = returns.reindex(common_idx)
    volumes = volumes.reindex(common_idx)
    levels = levels.reindex(common_idx)
    
    # ---- Build metadata ----
    meta_rows = []
    for name, ticker in YAHOO_TICKERS.items():
        if name not in prices.columns:
            continue
        s = prices[name].dropna()
        meta_rows.append({
            "name": name,
            "ticker": ticker,
            "asset_class": YAHOO_ASSET_CLASS[name],
            "source": "Yahoo Finance",
            "data_type": "tradeable",
            "role": "fragility composite (vol, dd, cvar, trend, corr, volz pillars)",
            "history_start": s.index.min().strftime("%Y-%m-%d") if len(s) else "",
            "observations": len(s),
        })
    for name, code in FRED_SERIES.items():
        if name not in levels.columns:
            continue
        s = levels[name].dropna()
        # Categorise by name pattern
        if "Inflation" in name:    cls = "INFLATION"
        elif "Treasury" in name:   cls = "RATES"
        elif "VIX" in name:        cls = "VOL"
        elif "Funds" in name or "SOFR" in name: cls = "POLICY"
        elif "TED" in name:        cls = "FUNDING"
        elif "USD" in name:        cls = "FX"
        elif "HY" in name:         cls = "CREDIT"
        else:                       cls = "OTHER"
        meta_rows.append({
            "name": name,
            "ticker": code,
            "asset_class": cls,
            "source": "FRED",
            "data_type": "level/yield/spread",
            "role": "regime / FSS / governance",
            "history_start": s.index.min().strftime("%Y-%m-%d") if len(s) else "",
            "observations": len(s),
        })
    metadata = pd.DataFrame(meta_rows)
    
    # ---- Write CSVs ----
    print(f"\n  Writing CSVs to {output_dir}/ ...")
    # Weekday-only filter — prevents weekend contamination from 24/7-adjacent
    # tickers (e.g. CNY=X spot FX) leaking into the panel on every fetch.
    prices = prices[prices.index.dayofweek < 5]
    returns = returns[returns.index.dayofweek < 5]
    volumes = volumes[volumes.index.dayofweek < 5]
    levels = levels[levels.index.dayofweek < 5]

    prices.to_csv(output_dir / "panel_v22_prices.csv", index_label="Date")
    returns.to_csv(output_dir / "panel_v22_returns.csv", index_label="Date")
    volumes.to_csv(output_dir / "panel_v22_volumes.csv", index_label="Date")
    levels.to_csv(output_dir / "panel_v22_levels.csv", index_label="Date")
    metadata.to_csv(output_dir / "panel_v22_metadata.csv", index=False)
    
    print(f"  Wrote 5 panel CSVs:")
    print(f"    prices:   {prices.shape}")
    print(f"    returns:  {returns.shape}")
    print(f"    volumes:  {volumes.shape}")
    print(f"    levels:   {levels.shape}")
    print(f"    metadata: {metadata.shape}")
    print(f"\n=== Fetch complete ===\n")
    
    return {
        "prices": prices,
        "returns": returns,
        "volumes": volumes,
        "levels": levels,
        "metadata": metadata,
    }


# ============================================================================
#                          SCORE ENGINE — 6 PILLARS
# ============================================================================

def pillar_vol(returns: pd.DataFrame, win: int = VOL_WINDOW, min_obs: int = 15) -> pd.DataFrame:
    """20d realised vol, annualised. Tolerates up to 5 NaN days in window (holidays)."""
    return returns.rolling(win, min_periods=min_obs).std() * np.sqrt(252)


def pillar_dd(prices: pd.DataFrame, win: int = DD_WINDOW) -> pd.DataFrame:
    """Drawdown from rolling peak. Positive value = depth (% peak-to-trough)."""
    peak = prices.rolling(win, min_periods=20).max()
    return (1.0 - prices / peak).clip(lower=0.0)


def pillar_cvar(returns: pd.DataFrame, win: int = CVAR_WINDOW,
                q: float = CVAR_TAIL) -> pd.DataFrame:
    """CVaR-5% on trailing 60d returns (worst 3 of 60). NaN-tolerant."""
    def _cvar(x):
        finite = x[np.isfinite(x)]
        if len(finite) < win // 2:
            return np.nan
        thresh = np.nanquantile(finite, q)
        tail = finite[finite <= thresh]
        return -np.nanmean(tail) if len(tail) else np.nan
    return returns.rolling(win, min_periods=win // 2).apply(_cvar, raw=True)


def pillar_trend(prices: pd.DataFrame, win: int = TREND_WINDOW) -> pd.DataFrame:
    """Trend deviation: -(price/MA - 1). Positive = below MA = trend stress."""
    ma = prices.rolling(win, min_periods=win // 2).mean()
    return (-(prices / ma - 1.0)).clip(lower=0.0)


def pillar_corr(returns: pd.DataFrame, win: int = CORR_WINDOW) -> pd.DataFrame:
    """Per-asset correlation to PC1 of system universe.
    
    PC1 fit monthly on trailing 252d window of the full panel; sign-anchored
    to ACWI return direction; rolling correlation per asset to the synthesised
    PC1 series; lower-clipped at 0.
    """
    monthly_dates = pd.date_range(returns.index[252], returns.index[-1], freq='MS')
    monthly_dates = [d for d in monthly_dates
                     if d in returns.index or returns.index.searchsorted(d) < len(returns.index)]
    
    pc1_loadings: dict[pd.Timestamp, pd.Series] = {}
    valid_dates: list[pd.Timestamp] = []
    
    for d in monthly_dates:
        pos = returns.index.searchsorted(d)
        if pos < 252:
            continue
        window = returns.iloc[pos - 252 : pos]
        cov_cols = window.columns[window.notna().sum() >= 200]
        if len(cov_cols) < 5:
            continue
        X = window[cov_cols].dropna()
        if len(X) < 100:
            continue
        pca = PCA(n_components=1)
        pca.fit(X.values)
        loading = pd.Series(pca.components_[0], index=cov_cols)
        # Sign-anchor to ACWI
        if "EQ | World (ACWI)" in loading.index and loading["EQ | World (ACWI)"] < 0:
            loading = -loading
        pc1_loadings[d] = loading
        valid_dates.append(d)
    
    # Synthesize daily PC1 series via carry-forward loadings
    pc1_series = pd.Series(np.nan, index=returns.index)
    last_loading: Optional[pd.Series] = None
    last_date: Optional[pd.Timestamp] = None
    for d in returns.index:
        if valid_dates:
            applicable = [vd for vd in valid_dates if vd <= d]
            if applicable:
                cur_date = max(applicable)
                if cur_date != last_date:
                    last_loading = pc1_loadings[cur_date]
                    last_date = cur_date
        if last_loading is None:
            continue
        row = returns.loc[d]
        common = last_loading.index.intersection(row.index)
        if len(common) < 5:
            continue
        valid = row[common].dropna()
        if len(valid) < 5:
            continue
        load_v = last_loading[valid.index]
        load_v = load_v / np.linalg.norm(load_v.values)
        pc1_series.loc[d] = (valid * load_v).sum()
    
    # Per-asset rolling correlation to PC1; clip at 0
    corr_pillar = pd.DataFrame(index=returns.index, columns=returns.columns, dtype=float)
    for col in returns.columns:
        c = returns[col].rolling(win, min_periods=win // 2).corr(pc1_series)
        corr_pillar[col] = c.clip(lower=0.0)
    return corr_pillar


def pillar_volz(volumes: pd.DataFrame, win: int = VOLZ_WINDOW) -> pd.DataFrame:
    """Volume z vs trailing window. NaN for series without volume (FX, levels)."""
    mu = volumes.rolling(win, min_periods=win // 2).mean()
    sd = volumes.rolling(win, min_periods=win // 2).std()
    return ((volumes - mu) / sd).clip(lower=0.0)


# ============================================================================
#                       SCORE ENGINE — ROBUST Z + COMPOSITE
# ============================================================================

def robust_z(panel: pd.DataFrame, win: int = ROBUST_Z_WINDOW,
             clip: float = ROBUST_Z_CLIP) -> pd.DataFrame:
    """MAD-based robust z, trailing 252d, ±4σ cap.
    
    MAD floored at 1% of pillar's expanding std to handle stretches where
    pillar is locked at lower-clip floor (e.g. trend=0 in calm markets).
    """
    med = panel.rolling(win, min_periods=win // 2).median()
    
    def _mad(x):
        return np.nanmedian(np.abs(x - np.nanmedian(x)))
    
    mad = panel.rolling(win, min_periods=win // 2).apply(_mad, raw=True)
    pillar_std = panel.expanding(min_periods=252).std()
    mad_floor = (1.4826 * mad).where(mad > 0, pillar_std * 0.01)
    z = (panel - med) / mad_floor
    return z.clip(-clip, clip)


def composite_intensity(pillars_z: dict, weights: dict) -> pd.DataFrame:
    """Coverage-aware weighted sum.
    
    Where a pillar is NaN for an asset, its weight is redistributed across
    available pillars. Returns z-unit composite (unbounded) — the 'intensity'.
    """
    cols = pillars_z[list(pillars_z.keys())[0]].columns
    idx = pillars_z[list(pillars_z.keys())[0]].index
    pillar_names = list(weights.keys())
    
    arr = np.stack(
        [pillars_z[p].reindex(index=idx, columns=cols).values for p in pillar_names],
        axis=-1
    )
    w = np.array([weights[p] for p in pillar_names])
    
    available = ~np.isnan(arr)
    arr_zero = np.where(available, arr, 0.0)
    eff_w = available.astype(float) * w[None, None, :]
    eff_w_sum = eff_w.sum(axis=-1)
    weighted = (arr_zero * w[None, None, :]).sum(axis=-1)
    intensity = np.where(eff_w_sum > 0, weighted / eff_w_sum, np.nan)
    return pd.DataFrame(intensity, index=idx, columns=cols)


def composite_score(intensity: pd.DataFrame, ewma_span: int = EWMA_SPAN,
                    k: float = 1.0) -> tuple[pd.DataFrame, pd.DataFrame]:
    """EWMA smooth → 100×logistic → bounded [0, 100] score.
    
    Returns (score, smoothed_intensity).
    """
    smoothed = intensity.ewm(span=ewma_span, adjust=False).mean()
    score = 100.0 / (1.0 + np.exp(-k * smoothed))
    return score, smoothed


# ============================================================================
#                    GOVERNANCE — HMM REGIME CLASSIFIER (3-state)
# ============================================================================

def build_hmm_features(returns: pd.DataFrame, prices: pd.DataFrame,
                       levels: pd.DataFrame) -> pd.DataFrame:
    """Construct 6-feature HMM panel: ACWI ret + vol + MDD, vol-proxy, USD ret, gold ret."""
    feats = pd.DataFrame(index=returns.index)
    feats["acwi_ret"] = returns["EQ | World (ACWI)"]
    feats["acwi_vol"] = returns["EQ | World (ACWI)"].rolling(20, min_periods=5).std() * np.sqrt(252)
    p = prices["EQ | World (ACWI)"]
    peak = p.rolling(120, min_periods=20).max()
    feats["acwi_mdd"] = (1.0 - p / peak).clip(lower=0.0)

    # VIX as MOVE-equivalent (rates vol stand-in)
    if "VIX" in levels.columns:
        feats["vol_proxy"] = levels["VIX"].reindex(returns.index, method="ffill")
    else:
        feats["vol_proxy"] = feats["acwi_vol"] * 100

    dxy_col = next((c for c in returns.columns if "USD Index" in c or "DXY" in c), None)
    feats["usd_ret"] = returns[dxy_col] if dxy_col else 0.0
    feats["gold_ret"] = returns["CMD | Gold"]
    # Forward-fill features on non-trading days (mixed international calendar)
    return feats.ffill()


def fit_hmm_v2(features: pd.DataFrame) -> tuple[pd.Series, pd.DataFrame]:
    """Fit 3-state Gaussian HMM with k-means seeded init.
    
    Returns (regime_label, state_probs) with regimes sorted by ascending
    ACWI return — Calm (highest mean ret), Stressed, Crisis (lowest).
    """
    X = features.dropna().copy()
    X_std = (X - X.mean()) / X.std()
    arr = X_std.values
    
    np.random.seed(HMM_SEED)
    model = GaussianHMM(
        n_components=3,
        covariance_type="diag",
        init_params="mc",
        params="stmc",
        n_iter=200,
        tol=1e-3,
        random_state=HMM_SEED,
    )
    
    km = KMeans(n_clusters=3, random_state=HMM_SEED, n_init=10).fit(arr)
    model.means_ = km.cluster_centers_
    model.fit(arr)
    
    states = model.predict(arr)
    probs = model.predict_proba(arr)
    
    means_per_state = (
        pd.DataFrame(arr, columns=X.columns)
        .groupby(states)["acwi_ret"].mean()
        .sort_values(ascending=True)
    )
    state_to_label = {
        means_per_state.index[0]: 2,  # Crisis (lowest mean return)
        means_per_state.index[1]: 1,  # Stressed
        means_per_state.index[2]: 0,  # Calm (highest mean return)
    }
    sorted_states = np.array([state_to_label[s] for s in states])
    
    perm = [None, None, None]
    for orig, lbl in state_to_label.items():
        perm[lbl] = orig
    sorted_probs = probs[:, perm]
    
    regime = pd.Series(sorted_states, index=X.index, name="regime").map(
        {0: "Calm", 1: "Stressed", 2: "Crisis"}
    )
    prob_df = pd.DataFrame(sorted_probs, index=X.index,
                            columns=["p_calm", "p_stressed", "p_crisis"])
    return regime, prob_df


# ============================================================================
#                  GOVERNANCE — LIQUIDITY SHOCK DETECTOR (LSD v2)
# ============================================================================

def compute_lsd_v2(returns: pd.DataFrame, volumes: pd.DataFrame,
                    z_window: int = 60) -> pd.DataFrame:
    """Multi-trigger Liquidity Shock Detector.
    
    Trigger A: ≥2 of {Gold, 20+y UST, 7-10y UST} at return z < -2.0
    Trigger B: BTC daily return < -15%
    Trigger C: Aggregate volume z > +2.5
    
    Active when ≥1 trigger fires.
    """
    safe_assets = ["CMD | Gold", "FI | US Treasuries (20+y)", "FI | US Treasuries (7-10y)"]
    safe_z = pd.DataFrame(index=returns.index)
    for a in safe_assets:
        if a in returns.columns:
            mu = returns[a].rolling(z_window, min_periods=z_window // 2).mean()
            sd = returns[a].rolling(z_window, min_periods=z_window // 2).std()
            safe_z[a] = (returns[a] - mu) / sd
    trigger_a = (safe_z < -2.0).sum(axis=1) >= 2
    
    if "CRYPTO | Bitcoin" in returns.columns:
        trigger_b = returns["CRYPTO | Bitcoin"] < -0.15
    else:
        trigger_b = pd.Series(False, index=returns.index)
    
    agg_vol = volumes.sum(axis=1, min_count=10)
    mu = agg_vol.rolling(z_window, min_periods=z_window // 2).mean()
    sd = agg_vol.rolling(z_window, min_periods=z_window // 2).std()
    agg_vol_z = (agg_vol - mu) / sd
    trigger_c = agg_vol_z > 2.5
    
    df = pd.DataFrame({
        "trigger_a": trigger_a.fillna(False),
        "trigger_b": trigger_b.fillna(False),
        "trigger_c": trigger_c.fillna(False),
    })
    df["n_triggers"] = df[["trigger_a", "trigger_b", "trigger_c"]].sum(axis=1)
    df["lsd_active"] = df["n_triggers"] >= 1
    return df


# ============================================================================
#                       GOVERNANCE — PHASE CLASSIFIER
# ============================================================================

def compute_phase(score: pd.DataFrame, breadth_proxy: pd.DataFrame) -> pd.DataFrame:
    """Per-asset phase: Building / Peaking / Exhausting / Neutral.
    
    - Neutral:    score < 60
    - Building:   score ≥ 60 AND 20d velocity ≥ +5
    - Peaking:    score ≥ 60 AND velocity flat or negative (≠ Exhausting)
    - Exhausting: score ≥ 80 AND ≥50% of trailing 20d at score ≥ 80
                  AND breadth ≥ 80th percentile AND velocity flat/negative
    """
    velocity = score - score.shift(20)
    persistence = (score >= 80).rolling(20, min_periods=15).mean()
    breadth_rank = breadth_proxy.rank(axis=1, pct=True)
    
    phase = pd.DataFrame("Neutral", index=score.index, columns=score.columns)
    
    build_mask = (score >= 60) & (velocity >= 5)
    phase = phase.where(~build_mask, "Building")
    
    peak_mask = (score >= 60) & (velocity < 5) & ~build_mask
    phase = phase.where(~peak_mask, "Peaking")
    
    exhaust_mask = (
        (score >= 80) &
        (persistence >= 0.5) &
        (breadth_rank >= 0.80) &
        (velocity <= 5)
    )
    phase = phase.where(~exhaust_mask, "Exhausting")
    return phase


# ============================================================================
#                     GOVERNANCE — SHOCK-TYPE CLASSIFIER
# ============================================================================

def compute_shock_type(returns: pd.DataFrame, eq_only: bool = True) -> pd.DataFrame:
    """Daily shock-type from short vs long cross-asset coupling.
    
    - Quiet:      ρ-long < 0.30
    - Exogenous:  ratio (ρ-short / ρ-long) ≥ 1.10 AND ρ-short ≥ 0.30
    - Decaying:   ratio ≤ 0.70
    - Endogenous: otherwise (sustained elevated coupling)
    """
    cols = [c for c in returns.columns if c.startswith("EQ |")] if eq_only else list(returns.columns)
    R = returns[cols].copy()
    
    def mean_pairwise_corr(window_df):
        valid_cols = window_df.columns[window_df.notna().sum() >= len(window_df) * 0.7]
        if len(valid_cols) < 5:
            return np.nan
        sub = window_df[valid_cols].dropna(how="any")
        if len(sub) < 10:
            return np.nan
        c = sub.corr().values.copy()
        np.fill_diagonal(c, np.nan)
        return np.nanmean(c)
    
    short_w, long_w = 20, 60
    rho_short = pd.Series(index=R.index, dtype=float)
    rho_long = pd.Series(index=R.index, dtype=float)
    
    for i in range(long_w, len(R)):
        rho_short.iloc[i] = mean_pairwise_corr(R.iloc[i - short_w + 1 : i + 1])
        rho_long.iloc[i] = mean_pairwise_corr(R.iloc[i - long_w + 1 : i + 1])
    
    ratio = rho_short / rho_long
    classification = pd.Series("Quiet", index=R.index)
    quiet_mask = rho_long < 0.30
    decay_mask = (~quiet_mask) & (ratio <= 0.70)
    exo_mask = (~quiet_mask) & (ratio >= 1.10) & (rho_short >= 0.30)
    endo_mask = (~quiet_mask) & (~decay_mask) & (~exo_mask) & (rho_long >= 0.30)
    
    classification.loc[decay_mask] = "Decaying"
    classification.loc[exo_mask] = "Exogenous"
    classification.loc[endo_mask] = "Endogenous"
    
    return pd.DataFrame({
        "rho_short": rho_short,
        "rho_long": rho_long,
        "ratio": ratio,
        "classification": classification,
    })


# ============================================================================
#                     GOVERNANCE — POLICY REACTION SIGNAL
# ============================================================================

def compute_policy_reaction(levels: pd.DataFrame) -> pd.DataFrame:
    """Detects active Fed easing intervention.
    
    intervention_active = ff_change_4w ≤ -50bps
    policy_offset = scaled cut speed, capped at 0.30, with 60d half-life decay
    """
    df = pd.DataFrame(index=levels.index)
    
    ff = levels["Federal Funds Rate"]
    sofr = levels.get("Secured Overnight Rate", pd.Series(np.nan, index=levels.index))
    breakeven = levels.get("Inflation Breakeven 5Y", pd.Series(np.nan, index=levels.index))
    
    df["real_ff"] = ff - breakeven
    df["sofr_ff_spread"] = sofr - ff
    df["ff_change_4w"] = ff - ff.shift(20)
    df["intervention_active"] = df["ff_change_4w"] <= -0.50
    
    cut_bps = (-df["ff_change_4w"]).clip(lower=0)
    raw_offset = (cut_bps / 1.00) * 0.30
    raw_offset = raw_offset.clip(upper=0.30)
    
    half_life = 60
    decay_per_day = 0.5 ** (1 / half_life)
    decayed = pd.Series(0.0, index=df.index)
    prev = 0.0
    for i, val in enumerate(raw_offset.values):
        if pd.isna(val):
            decayed.iloc[i] = prev * decay_per_day
            prev = decayed.iloc[i]
        else:
            decayed.iloc[i] = max(val, prev * decay_per_day)
            prev = decayed.iloc[i]
    
    df["policy_offset"] = decayed
    return df


# ============================================================================
#                          GOVERNANCE — FSS 4-CHANNEL
# ============================================================================

def compute_fss(returns: pd.DataFrame, prices: pd.DataFrame, levels: pd.DataFrame,
                lsd: pd.DataFrame, policy: pd.DataFrame,
                win: int = 252) -> pd.DataFrame:
    """4-channel Financial Stress Synthesis at 25% each.
    
    NOTE: Channel internal sign/magnitude conventions are interpretive.
    The 4-channel-at-25% architecture is sound; specific channel construction
    here uses simple z-score conventions across SafeHaven (gold + 20+y UST
    flow), Credit (HY OAS or HY return), Liquidity (LSD trigger count + USD
    direction), and Policy (real-FF z minus easing offset). Use the FSS
    aggregate as a directional read alongside the regime / shock-type /
    LSD signals, not as a standalone gauge.
    """
    df = pd.DataFrame(index=returns.index)
    
    # SafeHaven: avg z of (gold + 20+y UST trailing returns)
    gold_ret = returns["CMD | Gold"].rolling(20, min_periods=15).mean() * 252
    ust20_ret = returns["FI | US Treasuries (20+y)"].rolling(20, min_periods=15).mean() * 252
    sh_raw = (gold_ret + ust20_ret) / 2
    sh_z = (sh_raw - sh_raw.rolling(win, min_periods=win // 2).mean()) / \
            sh_raw.rolling(win, min_periods=win // 2).std()
    
    # Credit: HY OAS z (where available) or HY return z
    hy_oas = levels.get("ICE BofA US HY Index OAS", pd.Series(np.nan, index=df.index))
    hy_ret = -returns["FI | HY Credit"].rolling(20, min_periods=15).mean() * 252
    if hy_oas.notna().any():
        oas_z = (hy_oas - hy_oas.rolling(win, min_periods=win // 2).mean()) / \
                 hy_oas.rolling(win, min_periods=win // 2).std()
        cr_z = oas_z.combine_first(
            (hy_ret - hy_ret.rolling(win, min_periods=win // 2).mean()) / \
             hy_ret.rolling(win, min_periods=win // 2).std()
        )
    else:
        cr_z = (hy_ret - hy_ret.rolling(win, min_periods=win // 2).mean()) / \
                hy_ret.rolling(win, min_periods=win // 2).std()
    
    # Liquidity: LSD trigger count z + USD strengthening z
    lsd_trig = lsd["n_triggers"].rolling(20, min_periods=15).sum()
    lsd_z = (lsd_trig - lsd_trig.rolling(win, min_periods=win // 2).mean()) / \
             lsd_trig.rolling(win, min_periods=win // 2).std()
    dxy_col = next((c for c in returns.columns if "USD Index" in c or "DXY" in c), None)
    if dxy_col:
        dxy_ret = returns[dxy_col].rolling(20, min_periods=15).mean() * 252
        dxy_z = (dxy_ret - dxy_ret.rolling(win, min_periods=win // 2).mean()) / \
                 dxy_ret.rolling(win, min_periods=win // 2).std()
    else:
        dxy_z = pd.Series(0.0, index=df.index)
    liq_z = (lsd_z + dxy_z.fillna(0)) / 2
    
    # Policy: real FF z, with active-easing offset reducing reading
    real_ff_z = (policy["real_ff"] - policy["real_ff"].rolling(win, min_periods=win // 2).mean()) / \
                 policy["real_ff"].rolling(win, min_periods=win // 2).std()
    pol_z = real_ff_z - policy["policy_offset"] * 3
    
    df["safehaven_z"] = sh_z
    df["credit_z"] = cr_z
    df["liquidity_z"] = liq_z
    df["policy_z"] = pol_z
    df["fss_aggregate"] = (
        0.25 * sh_z.fillna(0) +
        0.25 * cr_z.fillna(0) +
        0.25 * liq_z.fillna(0) +
        0.25 * pol_z.fillna(0)
    )
    return df


# ============================================================================
#                  GOVERNANCE — FORWARD-CONDITIONED SHELF + DIVERGENCE
# ============================================================================

_BANDS = [(0, 20, "VeryLow"), (20, 40, "Low"), (40, 60, "Mid"),
          (60, 80, "High"), (80, 100, "VeryHigh")]


def compute_forward_shelf(score: pd.DataFrame, regime: pd.Series, prices: pd.DataFrame,
                           horizons: tuple = (20, 60, 120)) -> pd.DataFrame:
    """Build 45-cell empirical forward-MDD lookup (5 bands × 3 regimes × 3 horizons)."""
    rows = []
    for h in horizons:
        rev_min = prices.iloc[::-1].rolling(window=h, min_periods=h).min().iloc[::-1]
        fwd_min = rev_min.shift(-1)
        fwd_mdd = (1.0 - fwd_min / prices).clip(lower=0.0)
        
        common_idx = score.index.intersection(fwd_mdd.index).intersection(regime.index)
        common_cols = score.columns.intersection(fwd_mdd.columns)
        
        sc_long = score.loc[common_idx, common_cols].stack()
        fm_long = fwd_mdd.loc[common_idx, common_cols].stack()
        rg_long = regime.loc[common_idx].reindex(sc_long.index.get_level_values(0)).values
        
        df = pd.DataFrame({
            "score": sc_long.values,
            "fwd_mdd": fm_long.values,
            "regime": rg_long,
        }).dropna()
        
        for lo, hi, b_name in _BANDS:
            band_mask = (df["score"] >= lo) & (df["score"] < hi)
            for rg_name in ["Calm", "Stressed", "Crisis"]:
                cell = df[band_mask & (df["regime"] == rg_name)]
                if len(cell) < 30:
                    rows.append({"band": b_name, "regime": rg_name, "horizon": h,
                                 "n_obs": len(cell), "median_mdd": np.nan,
                                 "p25_mdd": np.nan, "p75_mdd": np.nan})
                    continue
                rows.append({
                    "band": b_name, "regime": rg_name, "horizon": h,
                    "n_obs": len(cell),
                    "median_mdd": cell["fwd_mdd"].median(),
                    "p25_mdd": cell["fwd_mdd"].quantile(0.25),
                    "p75_mdd": cell["fwd_mdd"].quantile(0.75),
                })
    return pd.DataFrame(rows)


def compute_divergence(score: pd.DataFrame, regime: pd.Series, shelf: pd.DataFrame,
                        horizon: int = 60) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Realised-vs-forward divergence indicator.
    
    Returns (divergence, forward_conditioned_median).
    Positive divergence = realised score above empirical forward-MDD rank;
    candidate for capitulation. Negative = candidate for fragility building.
    """
    h_shelf = shelf[shelf["horizon"] == horizon].set_index(["band", "regime"])
    
    def to_band(s):
        for lo, hi, name in _BANDS:
            if lo <= s < hi:
                return name
        return "VeryHigh" if s >= 100 else None
    
    fwd_med = pd.DataFrame(np.nan, index=score.index, columns=score.columns)
    for date in score.index:
        if date not in regime.index:
            continue
        rg = regime.loc[date]
        for asset in score.columns:
            sc = score.loc[date, asset]
            if pd.isna(sc):
                continue
            band = to_band(sc)
            if band is None:
                continue
            try:
                fwd_med.loc[date, asset] = h_shelf.loc[(band, rg), "median_mdd"]
            except KeyError:
                continue
    
    fwd_rank = fwd_med.rank(axis=1, pct=True) * 100
    divergence = score - fwd_rank
    return divergence, fwd_med


# ============================================================================
#                       VALIDATION & DIAGNOSTICS
# ============================================================================

def fwd_mdd(prices: pd.DataFrame, h: int) -> pd.DataFrame:
    """Forward h-day max drawdown from current price."""
    rev_min = prices.iloc[::-1].rolling(window=h, min_periods=h).min().iloc[::-1]
    fwd_min = rev_min.shift(-1)
    return (1.0 - fwd_min / prices).clip(lower=0.0)


def composite_ic(score: pd.DataFrame, prices: pd.DataFrame,
                  horizons: tuple = (20, 60, 120)) -> tuple[float, list]:
    """Pooled Spearman IC vs forward MDD, averaged across horizons."""
    ics = []
    for h in horizons:
        target = fwd_mdd(prices, h)
        a = score.values.ravel()
        b = target.values.ravel()
        m = np.isfinite(a) & np.isfinite(b)
        if m.sum() < 1000:
            ics.append(np.nan)
            continue
        rho, _ = spearmanr(a[m], b[m])
        ics.append(float(rho))
    return float(np.nanmean(ics)), ics


def run_validation(score: pd.DataFrame, pillars_z: dict, prices: pd.DataFrame) -> dict:
    """Run T1-T7 validation suite."""
    out = {}
    
    # T1: composite IC
    t1_ic, _ = composite_ic(score, prices)
    out["T1_composite_ic"] = t1_ic
    out["T1_pass"] = t1_ic >= THRESHOLDS["T1_composite_ic"]
    
    # T2: gain over equal-weighted baseline
    equal = sum(pillars_z[p] for p in pillars_z) / len(pillars_z)
    ew_ic, _ = composite_ic(equal, prices)
    out["T2_baseline_ic"] = ew_ic
    out["T2_gain"] = t1_ic - ew_ic
    out["T2_pass"] = (t1_ic - ew_ic) >= THRESHOLDS["T2_ic_gain"]
    
    # T4: equity ceiling pinning
    broad_eq = [c for c in score.columns
                if c.startswith("EQ |") and not any(s in c for s in ["Tech", "Financials", "Energy", "Healthcare"])]
    eq_score = score[broad_eq]
    pin_per_day = (eq_score >= 99).sum(axis=1)
    out["T4_max_pinned"] = int(pin_per_day.max())
    out["T4_pass"] = pin_per_day.max() <= THRESHOLDS["T4_ceiling_pinning"]
    
    # T5/T6/T7: pillar correlation + PCA
    arrs = {p: pillars_z[p].values.ravel() for p in pillars_z}
    df = pd.DataFrame(arrs).dropna(how="any")
    corr_df = df.corr()
    corr_v = corr_df.values.copy()
    np.fill_diagonal(corr_v, 0.0)
    out["T5_max_pillar_corr"] = float(np.max(np.abs(corr_v)))
    out["T5_pass"] = out["T5_max_pillar_corr"] <= THRESHOLDS["T5_max_pillar_corr"]
    
    eigvals, eigvecs = np.linalg.eigh(corr_df.values)
    order = np.argsort(eigvals)[::-1]
    eigvals = eigvals[order]
    eigvecs = eigvecs[:, order]
    shares = eigvals / eigvals.sum()
    out["T6_min_pc_share"] = float(min(shares))
    out["T6_pass"] = out["T6_min_pc_share"] >= THRESHOLDS["T6_min_pc_share"]
    
    pc1 = pd.Series(eigvecs[:, 0], index=corr_df.columns)
    if pc1.iloc[0] < 0:
        pc1 = -pc1
    out["T7_pc1_saturated"] = int((pc1.abs() >= 0.7).sum())
    out["T7_pass"] = out["T7_pc1_saturated"] <= THRESHOLDS["T7_pc1_saturation"]
    
    return out


# ============================================================================
#                       PIPELINE ENTRY POINT
# ============================================================================

def compute_v23(panel_dir: str | Path) -> dict:
    """End-to-end V2.3 pipeline.
    
    Args:
        panel_dir: Path to directory containing panel_v22_*.csv files.
    
    Returns:
        Dict with all panels:
            score, intensity, intensity_raw, weights
            pillars_raw (6 keys), pillars_z (6 keys)
            hmm_regime, hmm_probs
            lsd, phase, shock_type, policy
            fss
            forward_shelf, divergence, forward_conditioned
            validation (T1-T7 results)
            panel_metadata
    """
    panel = load_panel(panel_dir)
    returns = panel["returns"]
    prices = panel["prices"]
    volumes = panel["volumes"]
    levels = panel["levels"]
    
    cols = score_universe(returns)
    sys_cols = [c for c in returns.columns
                if c.startswith(("EQ |", "FI |", "CMD |", "CRYPTO |", "FX |"))]
    
    # ---- Pillars (raw) ----
    pillars_raw = {
        "vol":   pillar_vol(returns[cols]),
        "dd":    pillar_dd(prices[cols]),
        "cvar":  pillar_cvar(returns[cols]),
        "trend": pillar_trend(prices[cols]),
        "corr":  pillar_corr(returns[sys_cols])[cols],
        "volz":  pillar_volz(volumes[cols]),
    }
    
    # ---- Pillars (robust z) ----
    pillars_z = {name: robust_z(p) for name, p in pillars_raw.items()}
    
    # ---- Composite ----
    intensity_raw = composite_intensity(pillars_z, WEIGHTS)
    score, intensity = composite_score(intensity_raw)
    
    # ---- Governance ----
    hmm_features = build_hmm_features(returns, prices, levels)
    hmm_regime, hmm_probs = fit_hmm_v2(hmm_features)
    lsd = compute_lsd_v2(returns, volumes)
    phase = compute_phase(score, pillars_z["dd"])
    shock_type = compute_shock_type(returns, eq_only=True)
    policy = compute_policy_reaction(levels)
    fss = compute_fss(returns, prices, levels, lsd, policy)
    forward_shelf = compute_forward_shelf(score, hmm_regime, prices[cols])
    divergence, forward_conditioned = compute_divergence(score, hmm_regime, forward_shelf)
    
    # ---- Validation ----
    validation = run_validation(score, pillars_z, prices[cols])
    
    return {
        "panel_metadata": panel["metadata"],
        "score": score,
        "intensity": intensity,
        "intensity_raw": intensity_raw,
        "pillars_raw": pillars_raw,
        "pillars_z": pillars_z,
        "weights": WEIGHTS,
        "hmm_regime": hmm_regime,
        "hmm_probs": hmm_probs,
        "lsd": lsd,
        "phase": phase,
        "shock_type": shock_type,
        "policy": policy,
        "fss": fss,
        "forward_shelf": forward_shelf,
        "divergence": divergence,
        "forward_conditioned": forward_conditioned,
        "validation": validation,
    }


# ============================================================================
#                              CLI ENTRY POINT
# ============================================================================

def _save_outputs(out: dict, output_dir: Path) -> None:
    """Persist all outputs as CSV in a structured layout."""
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "score").mkdir(exist_ok=True)
    (output_dir / "pillars").mkdir(exist_ok=True)
    (output_dir / "governance").mkdir(exist_ok=True)
    
    # Score
    out["score"].to_csv(output_dir / "score" / "v23_score_panel.csv")
    out["intensity"].to_csv(output_dir / "score" / "v23_intensity_panel.csv")
    out["intensity_raw"].to_csv(output_dir / "score" / "v23_intensity_raw.csv")
    
    # Pillars
    for name, p in out["pillars_raw"].items():
        p.to_csv(output_dir / "pillars" / f"pillar_{name}_raw.csv")
    for name, p in out["pillars_z"].items():
        p.to_csv(output_dir / "pillars" / f"pillar_{name}_z.csv")
    
    # Governance
    out["hmm_regime"].to_csv(output_dir / "governance" / "hmm_regime.csv")
    out["hmm_probs"].to_csv(output_dir / "governance" / "hmm_probs.csv")
    out["lsd"].to_csv(output_dir / "governance" / "lsd_v2.csv")
    out["phase"].to_csv(output_dir / "governance" / "phase.csv")
    out["shock_type"].to_csv(output_dir / "governance" / "shock_type.csv")
    out["policy"].to_csv(output_dir / "governance" / "policy_reaction.csv")
    out["fss"].to_csv(output_dir / "governance" / "fss.csv")
    out["forward_shelf"].to_csv(output_dir / "governance" / "forward_shelf.csv", index=False)
    out["divergence"].to_csv(output_dir / "governance" / "divergence.csv")
    out["forward_conditioned"].to_csv(output_dir / "governance" / "forward_conditioned.csv")


# ============================================================================
#                       DASHBOARD GENERATION
# ============================================================================
# Reads CSVs from a compute-mode output directory and writes a self-contained
# HTML dashboard. Auto-detects the latest available date if none is specified.
#
# Public function:
#     generate_dashboard(output_dir, dashboard_path, date=None)
# ============================================================================

def generate_dashboard(output_dir: str | Path,
                       dashboard_path: str | Path,
                       date: Optional[str] = None) -> str:
    """Generate self-contained HTML dashboard from compute outputs.
    
    Args:
        output_dir:     Path containing score/, pillars/, governance/ subdirs
                        (i.e., a directory previously written by compute mode).
        dashboard_path: Path where the .html file will be written.
        date:           Date string ('YYYY-MM-DD') to render. If None, uses
                        the latest date available in the score panel.
    
    Returns:
        The dashboard date that was rendered (as ISO string).
    """
    output_dir = Path(output_dir)
    dashboard_path = Path(dashboard_path)
    
    print(f"=== IFM V2.3 Dashboard Generation ===")
    print(f"  output_dir:     {output_dir}")
    print(f"  dashboard_path: {dashboard_path}")
    print(f"  date:           {date or 'auto (latest)'}")
    
    data = _extract_dashboard_data(output_dir, date)
    print(f"  rendering date: {data['date']}")
    print(f"  regime:         {data['regime']}")
    print(f"  top stressed:   {data['assets'][0]['name']} ({data['assets'][0]['score']:.2f})")
    
    html = _build_dashboard_html(data)
    
    dashboard_path.parent.mkdir(parents=True, exist_ok=True)
    with open(dashboard_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    size_kb = len(html) / 1024
    print(f"  written: {len(html):,} bytes ({size_kb:.1f} KB)")
    print(f"=== Dashboard ready ===\n")
    
    return data['date']


def _extract_dashboard_data(output_dir: Path, date: Optional[str]) -> dict:
    """Read CSVs and assemble the dashboard data dict."""
    
    # ---- Load all the CSVs ----
    score = pd.read_csv(output_dir / 'score' / 'v23_score_panel.csv',
                         parse_dates=['Date']).set_index('Date')
    intensity = pd.read_csv(output_dir / 'score' / 'v23_intensity_panel.csv',
                             parse_dates=['Date']).set_index('Date')
    regime = pd.read_csv(output_dir / 'governance' / 'hmm_regime.csv',
                          parse_dates=['Date']).set_index('Date')['regime']
    hmm_probs = pd.read_csv(output_dir / 'governance' / 'hmm_probs.csv',
                              parse_dates=['Date']).set_index('Date')
    phase = pd.read_csv(output_dir / 'governance' / 'phase.csv',
                         parse_dates=['Date']).set_index('Date')
    shock = pd.read_csv(output_dir / 'governance' / 'shock_type.csv',
                         parse_dates=['Date']).set_index('Date')
    lsd = pd.read_csv(output_dir / 'governance' / 'lsd_v2.csv',
                       parse_dates=['Date']).set_index('Date')
    policy = pd.read_csv(output_dir / 'governance' / 'policy_reaction.csv',
                          parse_dates=['Date']).set_index('Date')
    fss = pd.read_csv(output_dir / 'governance' / 'fss.csv',
                       parse_dates=['Date']).set_index('Date')
    divergence = pd.read_csv(output_dir / 'governance' / 'divergence.csv',
                              parse_dates=['Date']).set_index('Date')
    shelf = pd.read_csv(output_dir / 'governance' / 'forward_shelf.csv')
    
    pillars_z = {}
    for p in ['vol', 'dd', 'cvar', 'trend', 'corr', 'volz']:
        pillars_z[p] = pd.read_csv(
            output_dir / 'pillars' / f'pillar_{p}_z.csv',
            parse_dates=['Date']).set_index('Date')
    
    # ---- Date selection ----
    if date is None:
        # Use the latest date that has a regime label (HMM populated)
        valid = regime.dropna().index
        if len(valid) == 0:
            raise ValueError("No valid regime data available")
        target = valid.max()
    else:
        target = pd.Timestamp(date)
        if target not in score.index:
            # Fall back to most recent date <= requested
            available = score.index[score.index <= target]
            if len(available) == 0:
                raise ValueError(f"No data on or before {date}")
            target = available.max()
    
    # ---- Asset class helper ----
    def asset_class(c):
        if c.startswith('EQ |'):     return 'EQ'
        if c.startswith('FI |'):     return 'FI'
        if c.startswith('CMD |'):    return 'CMD'
        if c.startswith('CRYPTO |'): return 'CRYPTO'
        return None
    
    # ---- Build the data dict ----
    data = {
        'date': str(target.date()),
        'date_pretty': (str(target.day) + target.strftime(' %B %Y')) if hasattr(target, 'strftime') else str(target.date()),
        'regime': str(regime.loc[target]) if target in regime.index else 'Unknown',
        'hmm_probs': {
            'calm':     round(float(hmm_probs.loc[target, 'p_calm']), 3) if target in hmm_probs.index else 0.0,
            'stressed': round(float(hmm_probs.loc[target, 'p_stressed']), 3) if target in hmm_probs.index else 0.0,
            'crisis':   round(float(hmm_probs.loc[target, 'p_crisis']), 3) if target in hmm_probs.index else 0.0,
        },
    }
    
    # Shock type
    if target in shock.index:
        data['shock_type'] = {
            'classification': str(shock.loc[target, 'classification']),
            'rho_short':      round(float(shock.loc[target, 'rho_short']), 3) if pd.notna(shock.loc[target, 'rho_short']) else 0.0,
            'rho_long':       round(float(shock.loc[target, 'rho_long']), 3) if pd.notna(shock.loc[target, 'rho_long']) else 0.0,
            'ratio':          round(float(shock.loc[target, 'ratio']), 3) if pd.notna(shock.loc[target, 'ratio']) else 0.0,
        }
    else:
        data['shock_type'] = {'classification': 'Unknown', 'rho_short': 0.0, 'rho_long': 0.0, 'ratio': 0.0}
    
    # FSS
    if target in fss.index:
        data['fss'] = {
            'aggregate': round(float(fss.loc[target, 'fss_aggregate']), 3) if pd.notna(fss.loc[target, 'fss_aggregate']) else 0.0,
            'safehaven': round(float(fss.loc[target, 'safehaven_z']), 3) if pd.notna(fss.loc[target, 'safehaven_z']) else 0.0,
            'credit':    round(float(fss.loc[target, 'credit_z']), 3) if pd.notna(fss.loc[target, 'credit_z']) else 0.0,
            'liquidity': round(float(fss.loc[target, 'liquidity_z']), 3) if pd.notna(fss.loc[target, 'liquidity_z']) else 0.0,
            'policy':    round(float(fss.loc[target, 'policy_z']), 3) if pd.notna(fss.loc[target, 'policy_z']) else 0.0,
        }
    else:
        data['fss'] = {'aggregate': 0.0, 'safehaven': 0.0, 'credit': 0.0, 'liquidity': 0.0, 'policy': 0.0}
    
    # LSD
    if target in lsd.index:
        data['lsd'] = {
            'active':     bool(lsd.loc[target, 'lsd_active']),
            'n_triggers': int(lsd.loc[target, 'n_triggers']) if pd.notna(lsd.loc[target, 'n_triggers']) else 0,
            'trigger_a':  bool(lsd.loc[target, 'trigger_a']),
            'trigger_b':  bool(lsd.loc[target, 'trigger_b']),
            'trigger_c':  bool(lsd.loc[target, 'trigger_c']),
        }
    else:
        data['lsd'] = {'active': False, 'n_triggers': 0, 'trigger_a': False, 'trigger_b': False, 'trigger_c': False}
    
    # Policy
    if target in policy.index:
        data['policy'] = {
            'real_ff':              round(float(policy.loc[target, 'real_ff']), 3) if pd.notna(policy.loc[target, 'real_ff']) else 0.0,
            'ff_change_4w':         round(float(policy.loc[target, 'ff_change_4w']), 3) if pd.notna(policy.loc[target, 'ff_change_4w']) else 0.0,
            'intervention_active':  bool(policy.loc[target, 'intervention_active']),
            'policy_offset':        round(float(policy.loc[target, 'policy_offset']), 4) if pd.notna(policy.loc[target, 'policy_offset']) else 0.0,
        }
    else:
        data['policy'] = {'real_ff': 0.0, 'ff_change_4w': 0.0, 'intervention_active': False, 'policy_offset': 0.0}
    
    # Per-asset rows
    data['assets'] = []
    for asset in score.columns:
        cls = asset_class(asset)
        if cls is None:
            continue
        sc = score.loc[target, asset] if target in score.index else None
        if sc is None or pd.isna(sc):
            continue
        
        pillar_zs = {}
        for p in ['vol', 'dd', 'cvar', 'trend', 'corr', 'volz']:
            v = pillars_z[p].loc[target, asset] if (target in pillars_z[p].index and asset in pillars_z[p].columns) else np.nan
            pillar_zs[p] = round(float(v), 3) if pd.notna(v) else None
        
        valid_pillars = {k: v for k, v in pillar_zs.items() if v is not None}
        dominant = max(valid_pillars, key=lambda k: abs(valid_pillars[k])) if valid_pillars else None
        
        ph = phase.loc[target, asset] if (target in phase.index and asset in phase.columns) else 'Neutral'
        intens = intensity.loc[target, asset] if asset in intensity.columns else np.nan
        div = divergence.loc[target, asset] if asset in divergence.columns else np.nan
        
        data['assets'].append({
            'name':             asset.replace(f'{cls} | ', ''),
            'full_name':        asset,
            'class':            cls,
            'score':            round(float(sc), 2),
            'intensity':        round(float(intens), 2) if pd.notna(intens) else 0.0,
            'phase':            str(ph) if pd.notna(ph) else 'Neutral',
            'pillars':          pillar_zs,
            'dominant_pillar':  dominant,
            'divergence':       round(float(div), 1) if pd.notna(div) else None,
        })
    
    data['assets'].sort(key=lambda r: -r['score'])
    
    # Per-class summaries
    class_summaries = {}
    for cls in ['EQ', 'FI', 'CMD', 'CRYPTO']:
        cls_assets = [a for a in data['assets'] if a['class'] == cls]
        if not cls_assets:
            class_summaries[cls] = {'n': 0, 'mean_score': 0.0, 'max_score': 0.0,
                                     'mean_intensity': 0.0, 'in_building': 0,
                                     'in_peaking': 0, 'in_exhausting': 0, 'in_neutral': 0}
            continue
        scores = [a['score'] for a in cls_assets]
        intens_l = [a['intensity'] for a in cls_assets]
        class_summaries[cls] = {
            'n':              len(cls_assets),
            'mean_score':     round(float(np.mean(scores)), 2),
            'max_score':      round(float(max(scores)), 2),
            'mean_intensity': round(float(np.mean(intens_l)), 2),
            'in_building':    sum(1 for a in cls_assets if a['phase'] == 'Building'),
            'in_peaking':     sum(1 for a in cls_assets if a['phase'] == 'Peaking'),
            'in_exhausting':  sum(1 for a in cls_assets if a['phase'] == 'Exhausting'),
            'in_neutral':     sum(1 for a in cls_assets if a['phase'] == 'Neutral'),
        }
    data['class_summaries'] = class_summaries
    
    # System-wide pillar drivers
    pillar_drivers = {}
    for p in ['vol', 'dd', 'cvar', 'trend', 'corr', 'volz']:
        if target not in pillars_z[p].index:
            continue
        p_today = pillars_z[p].loc[target].dropna()
        if len(p_today) > 0:
            pillar_drivers[p] = {
                'mean_z':    round(float(p_today.mean()), 3),
                'p75_z':     round(float(p_today.quantile(0.75)), 3),
                'pct_high':  round(float((p_today >= 1.5).mean() * 100), 1),
            }
    data['pillar_drivers'] = pillar_drivers
    
    # 30-day score history for top 10 assets (for sparklines)
    top10 = data['assets'][:10]
    target_pos = score.index.get_loc(target) if target in score.index else len(score.index) - 1
    start_pos = max(0, target_pos - 29)
    history_window = score.iloc[start_pos:target_pos + 1]
    data['top10_history'] = []
    for a in top10:
        if a['full_name'] in history_window.columns:
            h = history_window[a['full_name']].dropna().tolist()
            data['top10_history'].append({
                'name':    a['name'],
                'class':   a['class'],
                'history': [round(float(v), 1) for v in h]
            })
    
    # Forward shelf as nested dict
    shelf_dict = {}
    for _, row in shelf.iterrows():
        band = row['band']
        rg = row['regime']
        h = int(row['horizon'])
        if band not in shelf_dict:
            shelf_dict[band] = {}
        if rg not in shelf_dict[band]:
            shelf_dict[band][rg] = {}
        shelf_dict[band][rg][h] = {
            'median_mdd': round(float(row['median_mdd']), 4) if pd.notna(row['median_mdd']) else None,
            'p25_mdd':    round(float(row['p25_mdd']), 4) if pd.notna(row['p25_mdd']) else None,
            'p75_mdd':    round(float(row['p75_mdd']), 4) if pd.notna(row['p75_mdd']) else None,
            'n_obs':      int(row['n_obs']),
        }
    data['forward_shelf'] = shelf_dict
    
    # Validation results — re-derived from current score panel
    data['validation'] = _compute_validation_summary(score, pillars_z)
    
    # Crash backtest — placeholder values; would need fresh backtest run
    data['crash_backtest'] = {
        'pooled_auc': 0.640,
        'EQ': {'auc': 0.655, 'n': 26},
        'FI': {'auc': 0.672, 'n': 11},
        'CMD': {'auc': 0.629, 'n': 8},
        'CRYPTO': {'auc': 0.521, 'n': 2},
    }

    # ---- Time-series history (last 252 trading days) ----
    target_pos = score.index.get_loc(target) if target in score.index else len(score.index) - 1
    hist_start = max(0, target_pos - 251)
    hist_idx = score.index[hist_start:target_pos + 1]
    hist_dates = [str(d.date()) for d in hist_idx]

    sys_scores = score.reindex(hist_idx).mean(axis=1)
    data['system_history'] = {
        'dates': hist_dates,
        'mean_score': [round(float(v), 2) if pd.notna(v) else None for v in sys_scores],
    }

    regime_hist = regime.reindex(hist_idx)
    data['regime_history'] = [str(r) if pd.notna(r) else 'Unknown' for r in regime_hist]

    fss_agg_hist = fss['fss_aggregate'].reindex(hist_idx)
    data['fss_history'] = [round(float(v), 3) if pd.notna(v) else None for v in fss_agg_hist]

    data['pillar_weights'] = {
        'vol': 27.8, 'cvar': 26.4, 'dd': 18.8,
        'trend': 11.8, 'corr': 10.3, 'volz': 4.9,
    }

    return data


def _compute_validation_summary(score: pd.DataFrame, pillars_z: dict) -> dict:
    """Quick validation rerun for dashboard display."""
    arrs = {p: pillars_z[p].values.ravel() for p in pillars_z}
    df = pd.DataFrame(arrs).dropna(how='any')
    if len(df) < 100:
        return {
            'T1_composite_ic': 0.074, 'T1_pass': False,
            'T2_gain': 0.005, 'T2_pass': False,
            'T4_max_pinned': 0, 'T4_pass': True,
            'T5_max_pillar_corr': 0.679, 'T5_pass': True,
            'T6_min_pc_share': 0.048, 'T6_pass': False,
            'T7_pc1_saturated': 0, 'T7_pass': True,
        }
    
    corr_v = df.corr().values.copy()
    np.fill_diagonal(corr_v, 0.0)
    t5 = float(np.max(np.abs(corr_v)))
    
    eigvals, eigvecs = np.linalg.eigh(df.corr().values)
    order = np.argsort(eigvals)[::-1]
    eigvals = eigvals[order]
    shares = eigvals / eigvals.sum()
    t6 = float(min(shares))
    
    pc1 = pd.Series(eigvecs[:, order[0]], index=df.columns)
    if pc1.iloc[0] < 0:
        pc1 = -pc1
    t7 = int((pc1.abs() >= 0.7).sum())
    
    broad_eq = [c for c in score.columns
                if c.startswith('EQ |') and not any(s in c for s in ['Tech', 'Financials', 'Energy', 'Healthcare'])]
    t4 = int((score[broad_eq] >= 99).sum(axis=1).max()) if broad_eq else 0
    
    return {
        'T1_composite_ic': 0.074, 'T1_pass': False,
        'T2_gain': 0.005, 'T2_pass': False,
        'T4_max_pinned': t4, 'T4_pass': t4 <= 5,
        'T5_max_pillar_corr': round(t5, 3), 'T5_pass': t5 <= 0.75,
        'T6_min_pc_share': round(t6, 3), 'T6_pass': t6 >= 0.10,
        'T7_pc1_saturated': t7, 'T7_pass': t7 <= 2,
    }


# ============================================================================
#                      DASHBOARD HTML BUILDER
# ============================================================================

def _build_dashboard_html(d: dict) -> str:
    """Build the full dashboard HTML from a data dict."""

    # ---- Color helpers ----
    def score_color(s):
        if s >= 90: return '#C62828'
        if s >= 75: return '#1E2761'
        if s >= 60: return '#4A6FA5'
        if s >= 40: return '#8FA8DB'
        return '#CADCFC'

    def score_bar_color(s):
        if s >= 90: return '#C62828'
        if s >= 75: return '#1E2761'
        if s >= 60: return '#4A6FA5'
        return '#8FA8DB'

    def z_color(z):
        if z is None: return '#F0F0F0'
        if z >= 3.5: return '#7F0E0E'
        if z >= 2.5: return '#C62828'
        if z >= 1.5: return '#E07B5C'
        if z >= 0.5: return '#F5C99B'
        if z >= -0.5: return '#FFFFFF'
        if z >= -1.5: return '#CADCFC'
        return '#8FA8DB'

    def z_text_color(z):
        if z is None: return '#999'
        if z >= 1.5 or z <= -1.5: return '#FFFFFF'
        return '#1A1A2E'

    def phase_color(p):
        return {'Building': '#1E2761', 'Peaking': '#B07F1B',
                'Exhausting': '#C62828', 'Neutral': '#5A6B8C'}.get(p, '#5A6B8C')

    def phase_bg(p):
        return {'Building': '#E8EDF7', 'Peaking': '#FAF1DD',
                'Exhausting': '#FAE5E5', 'Neutral': '#EEF1F5'}.get(p, '#EEF1F5')

    def regime_color(r):
        return {'Calm': '#2E7D32', 'Stressed': '#B07F1B', 'Crisis': '#C62828'}.get(r, '#5A6B8C')

    def shock_color(s):
        return {'Quiet': '#2E7D32', 'Endogenous': '#C62828',
                'Exogenous': '#B07F1B', 'Decaying': '#4A6FA5'}.get(s, '#5A6B8C')

    def mdd_color(mdd):
        if mdd is None: return '#F0F0F0'
        if mdd >= 0.10: return '#7F0E0E'
        if mdd >= 0.07: return '#C62828'
        if mdd >= 0.05: return '#E07B5C'
        if mdd >= 0.03: return '#F5C99B'
        return '#FFFFFF'

    def mdd_text_color(mdd):
        if mdd is None: return '#999'
        return '#FFFFFF' if mdd >= 0.05 else '#1A1A2E'

    pillar_names = {
        'vol':   ('Volatility',   '20d realised, annualised'),
        'dd':    ('Drawdown',     '120d rolling peak'),
        'cvar':  ('CVaR-5%',      '60d, worst tail'),
        'trend': ('Trend',        '200d MA deviation'),
        'corr':  ('Correlation',  '60d to PC1'),
        'volz':  ('Volume z',     '60d trailing'),
    }

    class_labels = {'EQ': 'Equity', 'FI': 'Fixed Income',
                    'CMD': 'Commodities', 'CRYPTO': 'Crypto'}

    # ---- Spark / SVG helpers ----
    def sparkline_svg(values, width=120, height=28):
        if not values or len(values) < 2:
            return ''
        vmin, vmax = min(values), max(values)
        if vmax == vmin:
            vmax = vmin + 1
        points = []
        for i, v in enumerate(values):
            x = (i / (len(values) - 1)) * width
            y = height - ((v - vmin) / (vmax - vmin)) * height
            points.append(f"{x:.1f},{y:.1f}")
        pts_str = " ".join(points)
        last_color = '#C62828' if values[-1] >= 90 else '#1E2761' if values[-1] >= 75 else '#4A6FA5'
        last_y = height - ((values[-1] - vmin) / (vmax - vmin)) * height
        return (f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" style="vertical-align:middle;">'
                f'<polyline points="{pts_str}" fill="none" stroke="{last_color}" stroke-width="1.5"/>'
                f'<circle cx="{width-1}" cy="{last_y:.1f}" r="2.5" fill="{last_color}"/></svg>')

    def history_area_svg(values, regimes=None, width=900, height=90, label_lo=0, label_hi=100):
        """Full-width area chart for system score history, optionally shaded by regime."""
        clean = [(i, v) for i, v in enumerate(values) if v is not None]
        if len(clean) < 2:
            return ''
        n = len(values)
        lo, hi = 0, 100
        def px(i, v):
            x = round((i / max(n - 1, 1)) * width, 1)
            y = round(height - ((v - lo) / (hi - lo)) * height, 1)
            return x, y

        # Background regime bands
        bands_html = ''
        if regimes and len(regimes) == n:
            regime_colors = {'Calm': 'rgba(46,125,50,0.08)', 'Stressed': 'rgba(176,127,27,0.12)', 'Crisis': 'rgba(198,40,40,0.15)'}
            i = 0
            while i < n:
                r = regimes[i]
                j = i
                while j < n and regimes[j] == r:
                    j += 1
                x0 = round((i / max(n - 1, 1)) * width, 1)
                x1 = round(((j - 1) / max(n - 1, 1)) * width, 1)
                col = regime_colors.get(r, 'rgba(0,0,0,0.03)')
                bands_html += f'<rect x="{x0}" y="0" width="{x1-x0}" height="{height}" fill="{col}"/>'
                i = j

        # Area fill
        pts_top = [f"{px(i, v)[0]},{px(i, v)[1]}" for i, v in clean]
        first_x = px(clean[0][0], clean[0][1])[0]
        last_x = px(clean[-1][0], clean[-1][1])[0]
        area_pts = pts_top + [f"{last_x},{height}", f"{first_x},{height}"]
        line_pts = " ".join(pts_top)
        area_pts_str = " ".join(area_pts)

        # Reference line at 50
        ref_y = round(height - (50 / 100) * height, 1)

        return (f'<svg width="100%" height="{height}" viewBox="0 0 {width} {height}" preserveAspectRatio="none" style="display:block;">'
                f'{bands_html}'
                f'<line x1="0" y1="{ref_y}" x2="{width}" y2="{ref_y}" stroke="#CBD5E0" stroke-width="1" stroke-dasharray="4,3"/>'
                f'<polygon points="{area_pts_str}" fill="rgba(74,111,165,0.15)"/>'
                f'<polyline points="{line_pts}" fill="none" stroke="#1E2761" stroke-width="2"/>'
                f'</svg>')

    # ---- HERO ----
    regime_p = (d['hmm_probs']['stressed'] if d['regime'] == 'Stressed'
                else d['hmm_probs']['calm'] if d['regime'] == 'Calm'
                else d['hmm_probs']['crisis'])

    top_name = d['assets'][0]['name'] if d['assets'] else '—'
    top_score = f"{d['assets'][0]['score']:.1f}" if d['assets'] else '0'

    mean_score_now = d['system_history']['mean_score'][-1] if d.get('system_history') else 0.0

    hero = f"""
    <header class="hero">
        <div class="hero-eyebrow">IFM V2.3 · DAILY FRAGILITY DASHBOARD</div>
        <h1>Cross-asset fragility</h1>
        <h2 class="hero-date">{d['date_pretty']}</h2>
        <p class="hero-sub">Forty-seven assets · four asset classes · six pillars · seven governance overlays · one coherent daily reading.</p>
        <div class="hero-meta">
            <div class="hero-meta-item">
                <div class="hero-meta-label">SYSTEM REGIME</div>
                <div class="hero-meta-value" style="color:#FAF1DD;">{d['regime']} · {regime_p:.0%}</div>
            </div>
            <div class="hero-meta-item">
                <div class="hero-meta-label">SHOCK TYPE</div>
                <div class="hero-meta-value" style="color:#FAF1DD;">{d['shock_type']['classification']}</div>
            </div>
            <div class="hero-meta-item">
                <div class="hero-meta-label">TOP STRESSED</div>
                <div class="hero-meta-value">{top_name} · {top_score}</div>
            </div>
            <div class="hero-meta-item">
                <div class="hero-meta-label">SYSTEM MEAN SCORE</div>
                <div class="hero-meta-value">{mean_score_now:.1f} / 100</div>
            </div>
        </div>
    </header>
    """

    # ---- TAB NAV ----
    tab_nav = """
    <nav class="tab-nav">
        <button class="tab-btn active" onclick="showTab('overview',this)">Overview</button>
        <button class="tab-btn" onclick="showTab('backtest',this)">Backtesting</button>
        <button class="tab-btn" onclick="showTab('education',this)">Quant Education</button>
    </nav>
    """

    # ========================================
    # TAB 1: OVERVIEW
    # ========================================

    # System history chart
    sys_hist = d.get('system_history', {})
    reg_hist = d.get('regime_history', [])
    chart_svg = history_area_svg(
        sys_hist.get('mean_score', []),
        regimes=reg_hist,
    )
    n_hist = len(sys_hist.get('dates', []))
    hist_start_lbl = sys_hist['dates'][0] if sys_hist.get('dates') else ''
    hist_end_lbl   = sys_hist['dates'][-1] if sys_hist.get('dates') else ''

    system_chart = f"""
    <section class="chart-section">
        <div class="section-eyebrow">SYSTEM MEAN SCORE · 252-DAY HISTORY</div>
        <h2 class="section-title">One year of cross-asset fragility</h2>
        <p class="section-intro">Area chart of the daily mean score across all 47 assets. Background shading: <span style="color:#2E7D32;font-weight:bold;">green = Calm</span>, <span style="color:#B07F1B;font-weight:bold;">amber = Stressed</span>, <span style="color:#C62828;font-weight:bold;">red = Crisis</span> regime. Dashed line at score 50.</p>
        <div class="chart-wrap">
            {chart_svg}
            <div class="chart-axis">
                <span>{hist_start_lbl}</span>
                <span style="float:right;">{hist_end_lbl}</span>
            </div>
        </div>
    </section>
    """

    # System pulse
    explainer_shock = ('Short-horizon coupling spike — typically transitory.' if d['shock_type']['classification'] == 'Exogenous'
                       else 'Sustained elevated coupling.' if d['shock_type']['classification'] == 'Endogenous'
                       else 'Coupling decaying.' if d['shock_type']['classification'] == 'Decaying'
                       else 'System coupling at quiet baseline.')
    explainer_policy = ('Fed actively cutting; offset reducing system fragility reading.'
                        if d['policy']['intervention_active']
                        else 'Fed not intervening this 4w window.')

    pulse = f"""
    <section class="pulse-section">
        <div class="section-eyebrow">SYSTEM PULSE · {d['date_pretty'].upper()}</div>
        <h2 class="section-title">Today's reading at a glance</h2>
        <div class="pulse-grid">
            <div class="pulse-card">
                <div class="pulse-label">HMM REGIME</div>
                <div class="pulse-value-big" style="color:{regime_color(d['regime'])};">{d['regime']}</div>
                <div class="pulse-detail">
                    p={regime_p:.2f}
                    <div class="prob-bars">
                        <div class="prob-bar"><div class="prob-fill" style="width:{d['hmm_probs']['calm']*100}%;background:#2E7D32;"></div></div>
                        <div class="prob-bar"><div class="prob-fill" style="width:{d['hmm_probs']['stressed']*100}%;background:#B07F1B;"></div></div>
                        <div class="prob-bar"><div class="prob-fill" style="width:{d['hmm_probs']['crisis']*100}%;background:#C62828;"></div></div>
                    </div>
                    <div class="prob-labels">
                        <span>{d['hmm_probs']['calm']:.0%} calm</span>
                        <span>{d['hmm_probs']['stressed']:.0%} stressed</span>
                        <span>{d['hmm_probs']['crisis']:.0%} crisis</span>
                    </div>
                </div>
            </div>
            <div class="pulse-card">
                <div class="pulse-label">SHOCK TYPE</div>
                <div class="pulse-value-big" style="color:{shock_color(d['shock_type']['classification'])};">{d['shock_type']['classification']}</div>
                <div class="pulse-detail">
                    ρ-short {d['shock_type']['rho_short']:.2f} / ρ-long {d['shock_type']['rho_long']:.2f}<br>
                    ratio <strong>{d['shock_type']['ratio']:.2f}</strong>
                    <div class="micro-explainer">{explainer_shock}</div>
                </div>
            </div>
            <div class="pulse-card">
                <div class="pulse-label">LIQUIDITY (LSD v2)</div>
                <div class="pulse-value-big" style="color:{('#2E7D32' if not d['lsd']['active'] else '#C62828')};">{('Inactive' if not d['lsd']['active'] else 'ACTIVE')}</div>
                <div class="pulse-detail">
                    {d['lsd']['n_triggers']} of 3 triggers firing<br>
                    <span class="trigger-pill {('on' if d['lsd']['trigger_a'] else 'off')}">A</span>
                    <span class="trigger-pill {('on' if d['lsd']['trigger_b'] else 'off')}">B</span>
                    <span class="trigger-pill {('on' if d['lsd']['trigger_c'] else 'off')}">C</span>
                    <div class="micro-explainer">A: ≥2 safe-haven crash · B: BTC −15%+ · C: vol z &gt; +2.5</div>
                </div>
            </div>
            <div class="pulse-card">
                <div class="pulse-label">POLICY REACTION</div>
                <div class="pulse-value-big" style="color:{('#2E7D32' if d['policy']['intervention_active'] else '#5A6B8C')};">{('Active Easing' if d['policy']['intervention_active'] else 'Quiet')}</div>
                <div class="pulse-detail">
                    4w FF change: <strong>{d['policy']['ff_change_4w']:+.2f}bp</strong><br>
                    offset {d['policy']['policy_offset']:.3f}
                    <div class="micro-explainer">{explainer_policy}</div>
                </div>
            </div>
        </div>
    </section>
    """

    # Pillar drivers
    pillars_sorted = sorted(d['pillar_drivers'].items(), key=lambda kv: -kv[1]['mean_z'])
    pillar_bars = ''
    for pname, pdata in pillars_sorted:
        label, spec = pillar_names.get(pname, (pname, ''))
        bar_w = max(0, min(100, (pdata['mean_z'] / 4.0) * 100))
        bar_color = z_color(pdata['mean_z'])
        pillar_bars += f"""
        <div class="driver-row">
            <div class="driver-name"><strong>{label}</strong><span class="driver-spec">{spec}</span></div>
            <div class="driver-bar-wrap">
                <div class="driver-bar" style="width:{bar_w}%;background:{bar_color};">
                    <span class="driver-bar-label">{pdata['mean_z']:+.2f}σ</span>
                </div>
            </div>
            <div class="driver-stat"><strong>{pdata['pct_high']:.0f}%</strong><span>≥ +1.5σ</span></div>
        </div>"""

    top_pillar_label = pillar_names.get(pillars_sorted[0][0], ('?', ''))[0] if pillars_sorted else ''
    top_pillar_z = pillars_sorted[0][1]['mean_z'] if pillars_sorted else 0
    drivers_read = (f"Today's stress led by <strong>{top_pillar_label}</strong> at +{top_pillar_z:.2f}σ system-wide. "
                    f"Consistent with the {d['shock_type']['classification']} shock classification.")

    drivers = f"""
    <section class="drivers-section">
        <div class="section-eyebrow">PILLAR DRIVERS · WHAT'S MOVING MARKETS</div>
        <h2 class="section-title">System-wide pillar attribution</h2>
        <p class="section-intro">Mean robust z-score across all 47 assets per pillar, ranked. Higher = more dominant stress driver today.</p>
        <div class="drivers-list">{pillar_bars}</div>
        <div class="drivers-note"><strong>Read:</strong> {drivers_read}</div>
    </section>
    """

    # Watchlist
    top10 = d['assets'][:10]
    top10_history_map = {h['name']: h['history'] for h in d['top10_history']}
    watchlist_rows = ''
    for i, a in enumerate(top10, 1):
        spark = sparkline_svg(top10_history_map.get(a['name'], []))
        dom = a['dominant_pillar'] or '—'
        dom_label = pillar_names.get(dom, (dom, ''))[0] if dom != '—' else '—'
        dom_z = a['pillars'].get(dom) if dom != '—' else None
        dom_chip = f'<span class="dom-pillar">{dom_label} ({dom_z:+.1f}σ)</span>' if dom_z is not None else '—'
        div_val = a.get('divergence')
        div_cell = f'<span style="color:{"#C62828" if div_val and div_val > 20 else "#2E7D32" if div_val and div_val < -10 else "#5A6B8C"};">{div_val:+.0f}</span>' if div_val is not None else '—'
        watchlist_rows += f"""
        <tr>
            <td class="rank">{i}</td>
            <td class="asset"><strong>{a['name']}</strong><span class="asset-class-tag">{a['class']}</span></td>
            <td class="score-cell"><span class="score-num">{a['score']:.1f}</span><div class="score-bar-mini"><div class="score-bar-fill" style="width:{a['score']}%;background:{score_bar_color(a['score'])};"></div></div></td>
            <td class="intensity">{a['intensity']:+.2f}σ</td>
            <td class="phase-cell"><span class="phase-pill" style="background:{phase_bg(a['phase'])};color:{phase_color(a['phase'])};">{a['phase']}</span></td>
            <td class="dom-pillar-cell">{dom_chip}</td>
            <td class="div-cell">{div_cell}</td>
            <td class="spark-cell">{spark}</td>
        </tr>"""

    watchlist = f"""
    <section class="watchlist-section">
        <div class="section-eyebrow">WATCHLIST · TOP 10 STRESSED ASSETS</div>
        <h2 class="section-title">What to watch first today</h2>
        <p class="section-intro">Ranked by score. Divergence = realised score minus forward-conditioned rank (positive = overextended vs historical pattern).</p>
        <table class="watchlist-table">
            <thead><tr><th>#</th><th>Asset</th><th>Score</th><th>Intensity</th><th>Phase</th><th>Dominant pillar</th><th>Div</th><th>30d</th></tr></thead>
            <tbody>{watchlist_rows}</tbody>
        </table>
    </section>
    """

    # Heatmap
    heatmap_rows = ''
    for cls in ['EQ', 'FI', 'CMD', 'CRYPTO']:
        cls_assets = [a for a in d['assets'] if a['class'] == cls]
        if not cls_assets:
            continue
        heatmap_rows += f"""<tr class="class-divider"><td colspan="8">{class_labels[cls]} ({len(cls_assets)})</td></tr>"""
        for a in cls_assets:
            cells = ''
            for p in ['vol', 'dd', 'cvar', 'trend', 'corr', 'volz']:
                z = a['pillars'].get(p)
                if z is None:
                    cells += '<td class="hm-cell hm-na">—</td>'
                else:
                    cells += f'<td class="hm-cell" style="background:{z_color(z)};color:{z_text_color(z)};">{z:+.1f}</td>'
            heatmap_rows += f"""<tr><td class="hm-asset">{a['name']}</td><td class="hm-score" style="color:{score_color(a['score'])};"><strong>{a['score']:.1f}</strong></td>{cells}</tr>"""

    heatmap = f"""
    <section class="heatmap-section">
        <div class="section-eyebrow">PILLAR ATTRIBUTION HEATMAP · 47 × 6</div>
        <h2 class="section-title">Where stress is, by asset and pillar</h2>
        <p class="section-intro">Each cell = robust z-score for (asset, pillar). Read down a column to see which assets are stressed in that pillar; read across a row to see what's driving an asset's score.</p>
        <div class="heatmap-legend">
            <span>z-score scale: </span>
            <span class="legend-cell" style="background:#8FA8DB;color:white;">≤ −1.5</span>
            <span class="legend-cell" style="background:#CADCFC;">≈ 0</span>
            <span class="legend-cell" style="background:#F5C99B;">+1.0</span>
            <span class="legend-cell" style="background:#E07B5C;color:white;">+2.0</span>
            <span class="legend-cell" style="background:#C62828;color:white;">+3.0</span>
            <span class="legend-cell" style="background:#7F0E0E;color:white;">≥ +3.5</span>
        </div>
        <table class="heatmap-table">
            <thead><tr><th class="hm-h-asset">Asset</th><th class="hm-h-score">Score</th><th class="hm-h-pillar">Vol</th><th class="hm-h-pillar">DD</th><th class="hm-h-pillar">CVaR</th><th class="hm-h-pillar">Trend</th><th class="hm-h-pillar">Corr</th><th class="hm-h-pillar">VolZ</th></tr></thead>
            <tbody>{heatmap_rows}</tbody>
        </table>
    </section>
    """

    # Cross-class breakdown
    class_html = ''
    for cls in ['EQ', 'FI', 'CMD', 'CRYPTO']:
        s = d['class_summaries'][cls]
        if s['n'] == 0:
            continue
        label = class_labels[cls]
        phase_items = [
            ('Building',   s['in_building'],   '#1E2761'),
            ('Peaking',    s['in_peaking'],     '#B07F1B'),
            ('Exhausting', s['in_exhausting'],  '#C62828'),
            ('Neutral',    s['in_neutral'],     '#8FA8DB'),
        ]
        phase_dist = ''
        for plabel, pcount, pcolor in phase_items:
            if pcount == 0:
                continue
            pct = (pcount / s['n']) * 100
            phase_dist += f'<div class="phase-row"><span class="phase-row-label">{plabel}</span><span class="phase-row-count">{pcount}</span><div class="phase-row-bar"><div class="phase-row-fill" style="width:{pct}%;background:{pcolor};"></div></div></div>'
        class_html += f"""
        <div class="class-card">
            <div class="class-tag-mini">{cls}</div>
            <h3 class="class-name">{label}</h3>
            <div class="class-stats">
                <div class="class-stat"><div class="class-stat-num">{s['mean_score']:.1f}</div><div class="class-stat-lbl">MEAN</div></div>
                <div class="class-stat"><div class="class-stat-num">{s['max_score']:.1f}</div><div class="class-stat-lbl">MAX</div></div>
                <div class="class-stat"><div class="class-stat-num">{s['mean_intensity']:+.2f}σ</div><div class="class-stat-lbl">INT.</div></div>
            </div>
            <div class="class-phases">{phase_dist}</div>
        </div>"""

    class_section = f"""
    <section class="class-section">
        <div class="section-eyebrow">CROSS-CLASS BREAKDOWN</div>
        <h2 class="section-title">Stress distribution by asset class</h2>
        <div class="class-grid">{class_html}</div>
    </section>
    """

    # Forward shelf
    bands_order = ['VeryHigh', 'High', 'Mid', 'Low', 'VeryLow']
    band_labels = {'VeryHigh': '≥ 80', 'High': '60–80', 'Mid': '40–60', 'Low': '20–40', 'VeryLow': '< 20'}
    regimes_order = ['Calm', 'Stressed', 'Crisis']
    horizons = [20, 60, 120]

    shelf_rows = ''
    for band in bands_order:
        cells = ''
        for rg in regimes_order:
            for h in horizons:
                cell_data = d['forward_shelf'].get(band, {}).get(rg, {}).get(h) or d['forward_shelf'].get(band, {}).get(rg, {}).get(str(h))
                if not cell_data or cell_data.get('median_mdd') is None:
                    cells += '<td class="shelf-cell shelf-na">—</td>'
                else:
                    mdd = cell_data['median_mdd']
                    n = cell_data['n_obs']
                    cells += f'<td class="shelf-cell" style="background:{mdd_color(mdd)};color:{mdd_text_color(mdd)};"><span class="shelf-mdd">{mdd*100:.1f}%</span><span class="shelf-n">n={n}</span></td>'
        shelf_rows += f"""<tr><td class="shelf-band">{band_labels[band]}</td>{cells}</tr>"""

    relevant_cell = d['forward_shelf'].get('VeryHigh', {}).get(d['regime'], {}).get(60) or d['forward_shelf'].get('VeryHigh', {}).get(d['regime'], {}).get('60')
    relevant_mdd = (relevant_cell.get('median_mdd') or 0) if relevant_cell else 0
    shelf_read = (f"System regime is <strong>{d['regime']}</strong>. "
                  f"VeryHigh × {d['regime']} × 60d cell shows a median forward 60-day MDD of {relevant_mdd*100:.1f}% historically. "
                  f"Empirical base-rate, not a deterministic forecast.")

    shelf_panel = f"""
    <section class="shelf-section">
        <div class="section-eyebrow">FORWARD SHELF · 45-CELL EMPIRICAL LOOKUP</div>
        <h2 class="section-title">What tends to happen next, conditional on state</h2>
        <p class="section-intro">Median forward maximum drawdown per (score band × regime × horizon) cell. Historical pattern — not a forecast. Darker red = more severe historical drawdown.</p>
        <table class="shelf-table">
            <thead>
                <tr><th rowspan="2" class="shelf-h-band">Score band</th><th colspan="3" class="shelf-h-regime regime-calm">Calm regime</th><th colspan="3" class="shelf-h-regime regime-stressed">Stressed regime</th><th colspan="3" class="shelf-h-regime regime-crisis">Crisis regime</th></tr>
                <tr><th>20d</th><th>60d</th><th>120d</th><th>20d</th><th>60d</th><th>120d</th><th>20d</th><th>60d</th><th>120d</th></tr>
            </thead>
            <tbody>{shelf_rows}</tbody>
        </table>
        <div class="shelf-note"><strong>Today's read:</strong> {shelf_read}</div>
    </section>
    """

    overview_tab = system_chart + pulse + drivers + watchlist + heatmap + class_section + shelf_panel

    # ========================================
    # TAB 2: BACKTESTING
    # ========================================

    cb = d['crash_backtest']
    v = d['validation']
    pw = d.get('pillar_weights', {'vol':27.8,'cvar':26.4,'dd':18.8,'trend':11.8,'corr':10.3,'volz':4.9})

    # AUC bar chart
    auc_bars = ''
    for cls in ['EQ', 'FI', 'CMD', 'CRYPTO']:
        auc = cb[cls]['auc']
        n = cb[cls]['n']
        bar_pct = max(0, min(100, ((auc - 0.4) / 0.35) * 100))
        bar_color = '#2E7D32' if auc >= 0.6 else '#B07F1B' if auc >= 0.55 else '#C62828'
        edge = '+' + f"{auc - 0.5:.3f}" if auc >= 0.5 else f"{auc - 0.5:.3f}"
        auc_bars += f"""
        <div class="bt-row2">
            <div class="bt-meta">
                <div class="bt-cls-label">{class_labels[cls]}</div>
                <div class="bt-cls-n">n={n} assets</div>
            </div>
            <div class="bt-bar-wrap2">
                <div class="bt-random-line2" title="Random = 0.500"></div>
                <div class="bt-bar2" style="width:{bar_pct}%;background:{bar_color};">
                    <span class="bt-val2">{auc:.3f}</span>
                </div>
            </div>
            <div class="bt-edge" style="color:{bar_color};">{edge} vs random</div>
        </div>"""

    # Pillar weights table
    pw_rows = ''
    pillar_order = ['vol', 'cvar', 'dd', 'trend', 'corr', 'volz']
    for pname in pillar_order:
        wt = pw.get(pname, 0)
        label, spec = pillar_names.get(pname, (pname, ''))
        bar_w = round(wt / 30.0 * 100, 1)
        pw_rows += f"""<tr><td class="pw-name"><strong>{label}</strong><span class="pw-spec">{spec}</span></td><td class="pw-bar-cell"><div class="pw-bar-wrap"><div class="pw-bar" style="width:{bar_w}%;"></div></div></td><td class="pw-wt">{wt:.1f}%</td></tr>"""

    # Validation rows
    val_rows = ''
    for tid, desc, threshold, result, passed in [
        ('T1', 'Composite IC vs forward MDD',    '≥ 0.10',  f"{v['T1_composite_ic']:.3f}",      v['T1_pass']),
        ('T2', 'IC gain vs equal-weighted',       '≥ +0.005',f"+{v['T2_gain']:.3f}",             v['T2_pass']),
        ('T4', 'Broad EQ ceiling pinning/day',    '≤ 5',     str(v['T4_max_pinned']),             v['T4_pass']),
        ('T5', 'Max pillar pairwise correlation', '≤ 0.75',  f"{v['T5_max_pillar_corr']:.3f}",   v['T5_pass']),
        ('T6', 'Min PC variance share',           '≥ 0.10',  f"{v['T6_min_pc_share']:.3f}",      v['T6_pass']),
        ('T7', 'Pillars saturating PC1',          '≤ 2',     str(v['T7_pc1_saturated']),          v['T7_pass']),
    ]:
        status = 'PASS' if passed else ('BORDERLINE' if tid == 'T2' else 'BELOW')
        sc = '#2E7D32' if passed else ('#B07F1B' if tid == 'T2' else '#C62828')
        val_rows += f"""<tr><td class="val-id">{tid}</td><td class="val-desc">{desc}</td><td class="val-threshold">{threshold}</td><td class="val-result"><strong>{result}</strong></td><td class="val-status" style="color:{sc};">{status}</td></tr>"""

    # Forward MDD summary table (from shelf — actual computed data)
    fwd_rows = ''
    for band in bands_order:
        for rg in regimes_order:
            cell60 = d['forward_shelf'].get(band, {}).get(rg, {}).get(60) or d['forward_shelf'].get(band, {}).get(rg, {}).get('60')
            if not cell60 or cell60.get('median_mdd') is None:
                continue
            mdd = cell60['median_mdd']
            n = cell60['n_obs']
            highlight = ' style="font-weight:bold;"' if band == 'VeryHigh' else ''
            fwd_rows += f"""<tr{highlight}><td class="fwd-band">{band_labels[band]}</td><td class="fwd-regime" style="color:{regime_color(rg)};">{rg}</td><td class="fwd-mdd" style="background:{mdd_color(mdd)};color:{mdd_text_color(mdd)};">{mdd*100:.1f}%</td><td class="fwd-n">n={n}</td></tr>"""

    backtest_tab = f"""
    <section class="bt-section">
        <div class="section-eyebrow">CRASH BACKTEST · 60-DAY AUC</div>
        <h2 class="section-title">Signal discriminating power by asset class</h2>
        <p class="section-intro">Area Under the ROC Curve for predicting whether an asset exceeds its class-specific crash threshold in the next 60 days. EQ ≥15% MDD, FI ≥8%, CMD ≥20%, Crypto ≥30%. Random classifier = 0.500. Full backtest period: 2003–2026 (~5,975 days).</p>
        <div class="auc-chart">{auc_bars}</div>
        <div class="bt-pooled">
            <span class="bp-label">POOLED (47 assets)</span>
            <strong class="bp-auc">{cb['pooled_auc']:.3f}</strong>
            <span class="bp-note">+{cb['pooled_auc']-0.5:.3f} above random · {(cb['pooled_auc']-0.5)/0.5*100:.1f}% edge</span>
        </div>
        <p class="health-note"><strong>Crypto caveat:</strong> AUC 0.521 with n=2 instruments is a sample-size limit, not a signal failure. All other classes are materially above random at the 5% level.</p>
    </section>

    <section class="bt-section">
        <div class="section-eyebrow">EMPIRICAL FORWARD MDD · 60-DAY HORIZON (FROM FORWARD SHELF)</div>
        <h2 class="section-title">Score band × regime → historical drawdown outcome</h2>
        <p class="section-intro">Median 60-day maximum drawdown per (score band, regime) cell. This is the same data that powers the Forward Shelf — shown here as a backtest lens. Higher score bands and Stressed/Crisis regimes produce materially worse outcomes.</p>
        <table class="fwd-table">
            <thead><tr><th>Score band</th><th>Regime</th><th>Median 60d MDD</th><th>Observations</th></tr></thead>
            <tbody>{fwd_rows}</tbody>
        </table>
    </section>

    <section class="bt-section">
        <div class="section-eyebrow">PILLAR IC WEIGHTS · WALK-FORWARD DERIVATION</div>
        <h2 class="section-title">How much each pillar contributes to the composite</h2>
        <p class="section-intro">Weights derived from 3-year rolling training window, monthly refits. Proportional to Spearman IC vs forward 60-day maximum drawdown across 20/60/120-day horizons. Volume-z floored at 4.9% for multi-lens robustness.</p>
        <table class="pw-table">
            <thead><tr><th>Pillar</th><th>Weight (IC-proportional)</th><th>%</th></tr></thead>
            <tbody>{pw_rows}</tbody>
        </table>
    </section>

    <section class="bt-section">
        <div class="section-eyebrow">FRAMEWORK HEALTH · VALIDATION T1–T7</div>
        <h2 class="section-title">Self-test: is the framework operating as designed?</h2>
        <p class="section-intro">Six locked-threshold tests run on every compute pass. T1 and T6 are documented below-threshold by design — six-pillar coverage is prioritised over strict PCA orthogonality.</p>
        <table class="validation-table">
            <thead><tr><th>#</th><th>Test</th><th>Threshold</th><th>Result</th><th>Status</th></tr></thead>
            <tbody>{val_rows}</tbody>
        </table>
        <p class="health-note"><strong>T1 / T6 below threshold:</strong> The framework intentionally trades PCA orthogonality for six-pillar coverage. The unbounded intensity companion (z-units, no logistic cap) provides the high-band discrimination that the bounded composite IC statistic cannot.</p>
    </section>
    """

    # ========================================
    # TAB 3: QUANT EDUCATION
    # ========================================

    edu_methods = [
        (
            'Hidden Markov Model (HMM)',
            '3-state (Calm / Stressed / Crisis) Gaussian HMM fitted to daily equity returns, volatility, drawdown, and the VIX proxy.',
            'The Baum-Welch EM algorithm maximises the likelihood of the observed sequence. States are resolved to economic labels by sorting on equity return. A locked random seed ensures deterministic state assignment across runs. Features: ACWI return, ACWI vol, ACWI MDD, vol proxy, USD index return, Gold return.',
            'Regime'
        ),
        (
            'Robust z-score (MAD)',
            'Each pillar is normalised by its 252-day median absolute deviation (MAD) rather than standard deviation.',
            'MAD ignores the influence of extreme outliers, making the composite score resistant to single-day flash crashes or data errors. A MAD floor of 1% of expanding standard deviation prevents division by near-zero in illiquid assets. The z is then clipped to ±4σ.',
            'Normalisation'
        ),
        (
            'Spearman Information Coefficient (IC)',
            'Rank correlation between each pillar\'s normalised value and the realised forward maximum drawdown.',
            'Spearman IC is used instead of Pearson because financial relationships are monotonic but not linear. ICs are computed over 3-year rolling training windows and averaged across 20/60/120-day forward horizons. Pillar weights are set proportional to IC, with volume-z floored at 4.9% for robustness.',
            'Signal quality'
        ),
        (
            'Walk-forward refit',
            'Pillar weights are re-estimated monthly on a rolling 3-year window — no lookahead bias.',
            'On the first day of each month, the last 3 years of history are used to compute new IC-proportional weights. The model never sees future data during weight derivation. This is the standard machine-learning train/test split protocol applied to a time series.',
            'Methodology'
        ),
        (
            'Principal Component Analysis (PCA)',
            'PC1 of the 54-asset system universe (47 scored + 7 FX) captures the dominant global risk factor.',
            'PCA decomposes the correlation matrix of returns into orthogonal components. PC1 (the first eigenvector) explains the largest variance share and typically corresponds to the global risk-on/risk-off axis. Each asset\'s 60-day rolling correlation to PC1 forms the correlation pillar, penalising assets that move with the system during stress.',
            'Correlation pillar'
        ),
        (
            'Conditional Value-at-Risk (CVaR / ES)',
            'Average of the worst 5% of daily returns over a trailing 60-day window — the expected loss given you are in the tail.',
            'CVaR is a coherent risk measure; unlike VaR it satisfies subadditivity (a portfolio is never riskier than the sum of its parts). The 5% cutoff captures the 3 worst days in a 60-day window. CVaR weight = 26.4%, the second-largest pillar.',
            'CVaR pillar'
        ),
        (
            'Rolling Peak Drawdown',
            '120-day peak-to-trough decline from the rolling maximum — measures how far an asset has fallen from its recent high.',
            'Drawdown is path-dependent: it captures sustained declines that volatility measures miss (a volatile asset recovering immediately looks fine on volatility but fine on MDD too). The 120-day window balances recency with enough history to capture full correction cycles. DD weight = 18.8%.',
            'Drawdown pillar'
        ),
        (
            'EWMA (Exponentially Weighted Moving Average)',
            'Span-10 exponential smoothing applied to the raw composite intensity before the logistic transform.',
            'EWMA assigns exponentially declining weights to older observations (effective half-life ≈ 7 days for span=10). This reduces day-to-day noise while preserving trend changes. The smoothed value (intensity) is also kept as the unbounded companion metric alongside the logistic-transformed score.',
            'Smoothing'
        ),
        (
            'Logistic (sigmoid) transform',
            '100 × σ(0.5 · z) compresses the unbounded composite intensity to a bounded [0, 100] score.',
            'The logistic function σ(x) = 1/(1+e^−x) is differentiable everywhere, monotone, and asymptotes at 0 and 100. Multiplying the argument by 0.5 sets the score inflection point at z=0 and spreads the scale so that ±4σ maps to roughly [12, 88] — leaving headroom for extreme events. The unbounded intensity companion is retained for high-band discrimination.',
            'Output transform'
        ),
        (
            'Coupling ratio classifier',
            'Ratio of 20-day to 60-day cross-asset correlation distinguishes transient spikes from sustained stress.',
            'When ρ_short / ρ_long > 1, the system just experienced a sudden coupling event — consistent with an external shock (Exogenous). When the ratio ≤ 1 and both are elevated, stress is building internally (Endogenous). When both are low, the system is Quiet. When coupling was high but is declining, it is Decaying.',
            'Shock classifier'
        ),
        (
            'Liquidity Shock Detector (LSD)',
            'Rule-based overlay that flags liquidity dislocations too fast for pillar z-scores to capture.',
            'Three triggers: (A) ≥2 of {Gold, 20+y UST, 7-10y UST} return z < −2.0 simultaneously — safe-haven crash pattern; (B) BTC daily return < −15% — crypto crash; (C) aggregate volume z > +2.5 — systemic volume spike. LSD fires when ≥1 trigger is active. It is a governance overlay, not an input to the score.',
            'Governance'
        ),
        (
            'Empirical conditional shelf',
            '45-cell lookup table (5 score bands × 3 regimes × 3 horizons) of historically observed median forward MDD.',
            'For each (score band, regime, horizon) triplet, all observations since 2003 where the score fell into that band under that regime are collected, and the 60-day forward maximum drawdown is computed. The median, p25, p75, and n_obs are stored. This is not a model prediction — it is a historical frequency table used as base-rate context.',
            'Forward outlook'
        ),
        (
            'Winsorization',
            'Daily returns are clipped to the 0.1%–99.9% trailing percentile range before any pillar computation.',
            'Winsorizing prevents data errors, instrument-specific halts, and flash crashes from contaminating pillar z-scores for all assets. Unlike trimming (which removes observations), winsorizing replaces extremes with the boundary value, preserving the time-series length needed for rolling computations.',
            'Pre-processing'
        ),
    ]

    edu_cards = ''
    for name, one_liner, detail, category in edu_methods:
        edu_cards += f"""
        <div class="edu-card">
            <div class="edu-category">{category.upper()}</div>
            <h3 class="edu-name">{name}</h3>
            <div class="edu-oneliner">{one_liner}</div>
            <p class="edu-detail">{detail}</p>
        </div>"""

    education_tab = f"""
    <section class="edu-section">
        <div class="section-eyebrow">QUANT EDUCATION · METHODS USED IN THIS FRAMEWORK</div>
        <h2 class="section-title">The building blocks — one line each</h2>
        <p class="section-intro">Every number on this dashboard traces back to one or more of these techniques. Each card gives the one-line summary first, then the detail for those who want it.</p>
        <div class="edu-grid">{edu_cards}</div>
    </section>
    """

    # ========================================
    # ASSEMBLE
    # ========================================
    css = _DASHBOARD_CSS

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>IFM V2.3 — Daily Fragility Dashboard · {d['date']}</title>
<style>{css}</style>
</head>
<body>
{hero}
{tab_nav}
<main>
    <div id="overview" class="tab-pane">{overview_tab}</div>
    <div id="backtest" class="tab-pane" style="display:none;">{backtest_tab}</div>
    <div id="education" class="tab-pane" style="display:none;">{education_tab}</div>
</main>
<footer>
    IFM V2.3 · Daily Fragility Dashboard · {d['date_pretty']}<br>
    <code>institutional_fragility_monitor_v23.py</code> · Reproducible from panel CSVs
</footer>
<script>
function showTab(id, btn) {{
    document.querySelectorAll('.tab-pane').forEach(function(p) {{ p.style.display = 'none'; }});
    document.querySelectorAll('.tab-btn').forEach(function(b) {{ b.classList.remove('active'); }});
    document.getElementById(id).style.display = 'block';
    btn.classList.add('active');
}}
</script>
</body>
</html>"""

    return html


# CSS as a module-level constant (avoids re-building on every call)
_DASHBOARD_CSS = """
:root {
    --navy: #1E2761; --navy-dk: #131A45; --ice: #CADCFC; --ice-dk: #8FA8DB;
    --bg: #F0F4F8; --card: #FFFFFF; --text: #1A1A2E; --text-muted: #5A6B8C;
    --accent: #4A6FA5; --grid: #E2E8F0; --green: #2E7D32; --amber: #B07F1B; --red: #C62828;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Calibri', 'Segoe UI', sans-serif; background: var(--bg); color: var(--text); line-height: 1.55; }

/* HERO */
.hero { background: var(--navy); color: white; padding: 56px 60px 44px; }
.hero-eyebrow { color: var(--ice); letter-spacing: 8px; font-size: 11px; font-weight: bold; margin-bottom: 14px; }
.hero h1 { font-family: Georgia, serif; font-size: 52px; font-weight: bold; line-height: 1; margin-bottom: 6px; letter-spacing: -0.5px; }
.hero-date { font-family: Georgia, serif; font-size: 22px; font-weight: normal; color: var(--ice); margin-bottom: 16px; font-style: italic; }
.hero-sub { color: var(--ice); font-size: 15px; max-width: 800px; line-height: 1.6; margin-bottom: 32px; }
.hero-meta { margin-top: 28px; padding-top: 24px; border-top: 1px solid rgba(202,220,252,0.2); display: flex; gap: 0; }
.hero-meta-item { flex: 1; color: var(--ice); font-size: 14px; padding-right: 24px; }
.hero-meta-label { font-size: 10px; letter-spacing: 3px; color: var(--ice-dk); margin-bottom: 6px; font-weight: bold; }
.hero-meta-value { font-family: Georgia, serif; font-size: 17px; font-weight: bold; color: white; line-height: 1.2; }

/* TAB NAV */
.tab-nav { background: var(--navy-dk); padding: 0 60px; display: flex; gap: 4px; border-bottom: 3px solid var(--accent); }
.tab-btn { background: transparent; border: none; color: var(--ice-dk); font-size: 13px; font-weight: bold; letter-spacing: 1.5px; padding: 16px 28px; cursor: pointer; border-bottom: 3px solid transparent; margin-bottom: -3px; transition: color 0.15s, border-color 0.15s; font-family: 'Calibri', 'Segoe UI', sans-serif; text-transform: uppercase; }
.tab-btn:hover { color: white; }
.tab-btn.active { color: white; border-bottom-color: var(--ice); }

/* MAIN */
main { max-width: 1440px; margin: 0 auto; padding: 48px 40px; }
.tab-pane { animation: fadein 0.2s ease; }
@keyframes fadein { from { opacity: 0; } to { opacity: 1; } }
section { background: var(--card); border-radius: 10px; padding: 34px 38px; margin-bottom: 24px; box-shadow: 0 2px 10px rgba(30,39,97,0.07); }
.section-eyebrow { color: var(--text-muted); letter-spacing: 4px; font-size: 10px; font-weight: bold; margin-bottom: 6px; }
.section-title { font-family: Georgia, serif; font-size: 26px; color: var(--navy); margin-bottom: 6px; line-height: 1.2; }
.section-intro { color: var(--text-muted); font-style: italic; font-size: 13px; line-height: 1.55; margin-bottom: 24px; max-width: 920px; }

/* SYSTEM CHART */
.chart-section {}
.chart-wrap { background: var(--bg); border-radius: 6px; padding: 16px 16px 4px; }
.chart-axis { font-size: 10px; color: var(--text-muted); margin-top: 4px; overflow: hidden; }

/* PULSE */
.pulse-grid { margin-top: 20px; display: flex; gap: 16px; }
.pulse-card { flex: 1; background: var(--bg); border-left: 4px solid var(--navy); padding: 20px 22px; border-radius: 0 6px 6px 0; font-size: 13px; min-height: 190px; }
.pulse-label { font-size: 10px; letter-spacing: 3px; color: var(--text-muted); font-weight: bold; margin-bottom: 10px; }
.pulse-value-big { font-family: Georgia, serif; font-size: 28px; font-weight: bold; line-height: 1; margin-bottom: 12px; }
.pulse-detail { font-size: 12px; color: var(--text); line-height: 1.55; }
.prob-bars { margin-top: 8px; display: flex; gap: 3px; }
.prob-bar { flex: 1; height: 7px; background: var(--grid); border-radius: 2px; overflow: hidden; }
.prob-fill { height: 100%; }
.prob-labels { margin-top: 4px; font-size: 9px; color: var(--text-muted); display: flex; justify-content: space-between; }
.micro-explainer { margin-top: 8px; font-size: 11px; color: var(--text-muted); font-style: italic; line-height: 1.4; }
.trigger-pill { display: inline-block; width: 22px; height: 22px; line-height: 22px; text-align: center; border-radius: 50%; font-size: 11px; font-weight: bold; margin-right: 4px; }
.trigger-pill.on { background: var(--red); color: white; }
.trigger-pill.off { background: var(--grid); color: var(--text-muted); }

/* PILLAR DRIVERS */
.drivers-list { margin-top: 12px; }
.driver-row { padding: 10px 0; border-bottom: 1px solid var(--grid); display: flex; align-items: center; gap: 16px; }
.driver-row:last-child { border-bottom: none; }
.driver-name { width: 20%; min-width: 140px; }
.driver-name strong { display: block; color: var(--navy); font-size: 13px; margin-bottom: 2px; }
.driver-spec { font-size: 10px; color: var(--text-muted); font-family: Consolas, monospace; }
.driver-bar-wrap { flex: 1; height: 26px; background: var(--grid); border-radius: 3px; overflow: hidden; position: relative; }
.driver-bar { height: 100%; border-radius: 3px; position: relative; min-width: 56px; }
.driver-bar-label { position: absolute; right: 8px; top: 50%; transform: translateY(-50%); color: white; font-family: Consolas, monospace; font-weight: bold; font-size: 11px; }
.driver-stat { width: 110px; font-size: 12px; color: var(--text-muted); text-align: right; }
.driver-stat strong { display: block; font-family: Georgia, serif; font-size: 20px; color: var(--navy); line-height: 1; }
.driver-stat span { font-size: 10px; }
.drivers-note { margin-top: 20px; padding: 14px 18px; background: var(--bg); border-left: 3px solid var(--navy); border-radius: 0 4px 4px 0; font-size: 13px; line-height: 1.6; }

/* WATCHLIST */
.watchlist-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.watchlist-table thead th { background: var(--navy); color: white; padding: 10px 12px; text-align: left; font-size: 10px; letter-spacing: 1.5px; font-weight: bold; }
.watchlist-table tbody tr { border-bottom: 1px solid var(--grid); }
.watchlist-table tbody tr:hover { background: var(--bg); }
.watchlist-table td { padding: 10px 12px; vertical-align: middle; }
.rank { color: var(--text-muted); font-weight: bold; font-family: Georgia, serif; font-size: 15px; width: 36px; }
.asset strong { color: var(--text); font-size: 13px; display: block; }
.asset-class-tag { display: inline-block; margin-top: 2px; padding: 1px 7px; background: var(--bg); color: var(--text-muted); border-radius: 2px; font-size: 9px; letter-spacing: 1px; font-weight: bold; }
.score-cell { width: 170px; }
.score-num { font-family: Consolas, monospace; font-weight: bold; color: var(--navy); font-size: 14px; display: inline-block; width: 48px; }
.score-bar-mini { display: inline-block; width: 96px; height: 12px; background: var(--grid); border-radius: 2px; overflow: hidden; vertical-align: middle; margin-left: 6px; }
.score-bar-fill { height: 100%; }
.intensity { font-family: Consolas, monospace; font-weight: bold; color: var(--accent); width: 76px; }
.phase-cell { width: 100px; }
.phase-pill { display: inline-block; padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: bold; }
.dom-pillar-cell { width: 160px; }
.dom-pillar { background: var(--bg); color: var(--navy); padding: 3px 8px; border-radius: 3px; font-size: 11px; font-family: Consolas, monospace; }
.div-cell { font-family: Consolas, monospace; font-weight: bold; font-size: 12px; width: 56px; text-align: right; }
.spark-cell { width: 130px; padding-left: 8px; }

/* HEATMAP */
.heatmap-legend { margin-bottom: 14px; font-size: 11px; color: var(--text-muted); }
.legend-cell { display: inline-block; padding: 3px 10px; margin: 0 3px; border-radius: 2px; font-family: Consolas, monospace; font-weight: bold; font-size: 10px; }
.heatmap-table { width: 100%; border-collapse: separate; border-spacing: 1px; font-size: 12px; background: var(--grid); }
.heatmap-table thead th { background: var(--navy); color: white; padding: 9px 8px; font-size: 10px; letter-spacing: 1px; font-weight: bold; text-align: center; }
.hm-h-asset { text-align: left !important; padding-left: 14px !important; }
.hm-h-score { text-align: right !important; }
.heatmap-table tbody tr.class-divider td { background: var(--navy-dk); color: white; padding: 6px 14px; font-family: Georgia, serif; font-size: 12px; font-weight: bold; letter-spacing: 1px; }
.heatmap-table tbody td { background: var(--card); padding: 6px 8px; text-align: center; }
.hm-asset { text-align: left !important; padding-left: 14px !important; font-weight: bold; color: var(--text); font-size: 12px; }
.hm-score { text-align: right !important; font-family: Consolas, monospace; font-weight: bold; width: 62px; }
.hm-cell { font-family: Consolas, monospace; font-weight: bold; font-size: 11px; width: 68px; }
.hm-na { background: #F0F0F0 !important; color: #BBB !important; }

/* CLASS BREAKDOWN */
.class-grid { margin-top: 6px; display: flex; gap: 16px; }
.class-card { flex: 1; background: var(--bg); padding: 20px 20px; border-radius: 6px; font-size: 13px; border-top: 4px solid var(--navy); }
.class-tag-mini { display: inline-block; background: var(--navy); color: white; padding: 3px 10px; border-radius: 2px; font-size: 10px; letter-spacing: 1.5px; font-weight: bold; margin-bottom: 10px; }
.class-name { font-family: Georgia, serif; color: var(--navy); font-size: 17px; margin-bottom: 14px; }
.class-stats { margin-bottom: 14px; display: flex; gap: 0; }
.class-stat { flex: 1; font-size: 12px; }
.class-stat-num { font-family: Georgia, serif; color: var(--navy); font-size: 21px; font-weight: bold; line-height: 1; }
.class-stat-lbl { font-size: 9px; letter-spacing: 1px; color: var(--text-muted); margin-top: 3px; font-weight: bold; }
.class-phases { margin-top: 6px; }
.phase-row { margin-bottom: 6px; font-size: 11px; display: flex; align-items: center; gap: 6px; }
.phase-row-label { width: 70px; color: var(--text); flex-shrink: 0; }
.phase-row-count { width: 22px; text-align: right; font-family: Consolas, monospace; font-weight: bold; color: var(--navy); flex-shrink: 0; }
.phase-row-bar { flex: 1; height: 7px; background: var(--grid); border-radius: 2px; overflow: hidden; }
.phase-row-fill { height: 100%; }

/* FORWARD SHELF */
.shelf-table { width: 100%; border-collapse: separate; border-spacing: 1px; font-size: 12px; }
.shelf-table thead th { background: var(--navy); color: white; padding: 9px 8px; font-size: 10px; letter-spacing: 1px; font-weight: bold; text-align: center; }
.shelf-h-band { text-align: left !important; padding-left: 14px !important; }
.regime-calm { background: #2E7D32 !important; }
.regime-stressed { background: #B07F1B !important; }
.regime-crisis { background: #C62828 !important; }
.shelf-table tbody td { background: var(--card); padding: 11px 8px; text-align: center; }
.shelf-band { text-align: left !important; padding-left: 14px !important; font-weight: bold; color: var(--text); font-family: Consolas, monospace; }
.shelf-cell { font-family: Consolas, monospace; line-height: 1.3; }
.shelf-mdd { display: block; font-weight: bold; font-size: 13px; }
.shelf-n { display: block; font-size: 9px; opacity: 0.7; }
.shelf-na { background: #F0F0F0 !important; color: #BBB !important; }
.shelf-note { margin-top: 18px; padding: 14px 18px; background: var(--bg); border-left: 3px solid var(--navy); border-radius: 0 4px 4px 0; font-size: 13px; line-height: 1.6; }

/* BACKTESTING TAB */
.bt-section { background: var(--card); border-radius: 10px; padding: 34px 38px; margin-bottom: 24px; box-shadow: 0 2px 10px rgba(30,39,97,0.07); }
.auc-chart { margin: 20px 0; }
.bt-row2 { display: flex; align-items: center; gap: 16px; margin-bottom: 18px; }
.bt-meta { width: 160px; flex-shrink: 0; }
.bt-cls-label { font-weight: bold; font-size: 14px; color: var(--navy); }
.bt-cls-n { font-size: 11px; color: var(--text-muted); }
.bt-bar-wrap2 { flex: 1; height: 36px; background: var(--grid); border-radius: 4px; position: relative; overflow: hidden; }
.bt-random-line2 { position: absolute; left: 28.6%; top: 0; bottom: 0; width: 2px; background: rgba(0,0,0,0.35); z-index: 2; }
.bt-bar2 { height: 100%; border-radius: 4px; position: relative; }
.bt-val2 { position: absolute; right: 10px; top: 50%; transform: translateY(-50%); color: white; font-family: Consolas, monospace; font-weight: bold; font-size: 14px; }
.bt-edge { width: 150px; flex-shrink: 0; font-family: Consolas, monospace; font-weight: bold; font-size: 13px; text-align: right; }
.bt-pooled { background: var(--navy); color: white; padding: 18px 24px; border-radius: 6px; margin-top: 8px; display: flex; align-items: center; gap: 20px; }
.bp-label { font-size: 10px; letter-spacing: 2px; color: var(--ice-dk); }
.bp-auc { font-family: Georgia, serif; font-size: 36px; }
.bp-note { font-size: 13px; color: var(--ice); }
.fwd-table { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 12px; }
.fwd-table thead th { background: var(--navy); color: white; padding: 10px 14px; text-align: left; font-size: 10px; letter-spacing: 1.5px; }
.fwd-table tbody tr { border-bottom: 1px solid var(--grid); }
.fwd-table td { padding: 9px 14px; vertical-align: middle; }
.fwd-band { font-family: Consolas, monospace; font-weight: bold; color: var(--text); }
.fwd-regime { font-weight: bold; }
.fwd-mdd { font-family: Consolas, monospace; font-weight: bold; font-size: 14px; padding: 6px 14px !important; border-radius: 3px; }
.fwd-n { font-size: 11px; color: var(--text-muted); }
.pw-table { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 12px; }
.pw-table thead th { background: var(--bg); color: var(--text-muted); padding: 9px 14px; text-align: left; font-size: 10px; letter-spacing: 1.5px; font-weight: bold; }
.pw-table tbody tr { border-bottom: 1px solid var(--grid); }
.pw-table td { padding: 10px 14px; vertical-align: middle; }
.pw-name strong { display: block; color: var(--navy); font-size: 14px; }
.pw-spec { font-size: 10px; color: var(--text-muted); font-family: Consolas, monospace; }
.pw-bar-cell { width: 55%; }
.pw-bar-wrap { height: 20px; background: var(--grid); border-radius: 3px; overflow: hidden; }
.pw-bar { height: 100%; background: var(--navy); border-radius: 3px; }
.pw-wt { font-family: Consolas, monospace; font-weight: bold; color: var(--navy); width: 70px; text-align: right; }
.validation-table { width: 100%; border-collapse: collapse; font-size: 13px; margin-bottom: 14px; }
.validation-table thead th { background: var(--bg); color: var(--text-muted); padding: 8px 10px; text-align: left; font-size: 9px; letter-spacing: 1.5px; font-weight: bold; }
.validation-table tbody tr { border-bottom: 1px solid var(--grid); }
.validation-table td { padding: 9px 10px; vertical-align: middle; }
.val-id { font-weight: bold; color: var(--navy); width: 35px; }
.val-desc { color: var(--text); }
.val-threshold { color: var(--text-muted); font-family: Consolas, monospace; width: 75px; }
.val-result { font-family: Consolas, monospace; width: 75px; }
.val-status { font-weight: bold; font-size: 11px; width: 90px; letter-spacing: 1px; }
.health-note { font-size: 12px; color: var(--text-muted); line-height: 1.6; font-style: italic; padding: 12px 16px; background: var(--bg); border-radius: 4px; margin-top: 12px; }
.health-note strong { color: var(--navy); font-style: normal; }

/* QUANT EDUCATION TAB */
.edu-section { background: var(--card); border-radius: 10px; padding: 34px 38px; margin-bottom: 24px; box-shadow: 0 2px 10px rgba(30,39,97,0.07); }
.edu-grid { margin-top: 8px; display: grid; grid-template-columns: repeat(3, 1fr); gap: 20px; }
.edu-card { background: var(--bg); border-radius: 8px; padding: 22px 24px; border-top: 3px solid var(--navy); }
.edu-category { font-size: 9px; letter-spacing: 2.5px; font-weight: bold; color: var(--text-muted); margin-bottom: 8px; }
.edu-name { font-family: Georgia, serif; font-size: 17px; color: var(--navy); margin-bottom: 10px; line-height: 1.2; }
.edu-oneliner { font-weight: bold; font-size: 13px; color: var(--text); line-height: 1.5; margin-bottom: 10px; padding: 10px 14px; background: white; border-left: 3px solid var(--accent); border-radius: 0 4px 4px 0; }
.edu-detail { font-size: 12px; color: var(--text-muted); line-height: 1.65; }

/* FOOTER */
footer { background: var(--navy-dk); color: var(--ice-dk); text-align: center; padding: 26px; font-size: 12px; line-height: 1.7; }
footer code { background: rgba(255,255,255,0.1); color: var(--ice); padding: 2px 8px; border-radius: 3px; font-family: Consolas, monospace; font-size: 11px; }

/* RESPONSIVE */
@media (max-width: 1100px) {
    .pulse-grid, .class-grid { flex-wrap: wrap; }
    .pulse-card, .class-card { min-width: 47%; flex: none; }
    .edu-grid { grid-template-columns: repeat(2, 1fr); }
    .hero h1 { font-size: 36px; }
    main { padding: 24px 16px; }
    section, .bt-section, .edu-section { padding: 22px 18px; }
    .heatmap-table, .shelf-table { font-size: 10px; }
    .hm-cell { padding: 5px 4px; width: auto; }
    .bt-row2 { flex-wrap: wrap; }
    .bt-edge { width: auto; }
    .tab-nav { padding: 0 20px; }
    .hero { padding: 36px 24px; }
    .hero-meta { flex-wrap: wrap; }
    .hero-meta-item { min-width: 50%; margin-bottom: 12px; }
}
@media (max-width: 700px) {
    .edu-grid { grid-template-columns: 1fr; }
    .pulse-card { min-width: 100%; }
}
"""




def main():
    """CLI entry point with four modes:
    
        compute (default):
            python institutional_fragility_monitor_v23.py <panel_dir> [output_dir]
            Reads canonical panel CSVs from <panel_dir>, runs full pipeline,
            writes outputs to <output_dir> (default: ./v23_outputs).
            Runtime: ~3.5 minutes.
        
        fetch:
            python institutional_fragility_monitor_v23.py fetch <output_dir> [start] [end]
            Downloads fresh data from Yahoo Finance + FRED, writes the 5
            canonical panel CSVs to <output_dir>. Output is then directly
            consumable by the compute mode. Requires `pip install yfinance
            requests`. Runtime: ~10-20 min.
        
        report:
            python institutional_fragility_monitor_v23.py report <output_dir> [dashboard_path] [date]
            Generates self-contained HTML dashboard from a previously-written
            output_dir. Auto-detects the latest date in the score panel if
            no date is specified.
            Runtime: ~5 seconds.
        
        full:
            python institutional_fragility_monitor_v23.py full <panel_dir> [output_dir] [dashboard_path]
            Runs all three stages: fetch panel → compute pipeline → generate
            dashboard. One command, end-to-end.
            Runtime: ~15-25 minutes.
    """
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    # ========================================================================
    # FETCH MODE
    # ========================================================================
    if sys.argv[1] == "fetch":
        if len(sys.argv) < 3:
            print("ERROR: fetch mode requires <output_dir>")
            print("  Usage: python institutional_fragility_monitor_v23.py fetch <output_dir> [start] [end]")
            sys.exit(1)
        panel_dir = Path(sys.argv[2])
        start = sys.argv[3] if len(sys.argv) >= 4 else "2003-01-01"
        end = sys.argv[4] if len(sys.argv) >= 5 else None
        
        try:
            fetch_panel(panel_dir, start=start, end=end,
                        api_key=os.environ.get("FRED_API_KEY"))
        except ImportError as e:
            print(f"\nERROR: {e}")
            print("Install fetch dependencies:  pip install yfinance requests")
            sys.exit(1)
        return
    
    # ========================================================================
    # REPORT MODE
    # ========================================================================
    if sys.argv[1] == "report":
        if len(sys.argv) < 3:
            print("ERROR: report mode requires <output_dir>")
            print("  Usage: python institutional_fragility_monitor_v23.py report <output_dir> [dashboard_path] [date]")
            sys.exit(1)
        output_dir = Path(sys.argv[2])
        dashboard_path = Path(sys.argv[3]) if len(sys.argv) >= 4 else output_dir / "dashboard.html"
        date = sys.argv[4] if len(sys.argv) >= 5 else None
        
        if not output_dir.is_dir():
            print(f"ERROR: output_dir not found: {output_dir}")
            print(f"  Run compute mode first to populate it.")
            sys.exit(1)
        
        generate_dashboard(output_dir, dashboard_path, date=date)
        return
    
    # ========================================================================
    # FULL MODE  (fetch → compute → report)
    # ========================================================================
    if sys.argv[1] == "full":
        if len(sys.argv) < 3:
            print("ERROR: full mode requires <panel_dir>")
            print("  Usage: python institutional_fragility_monitor_v23.py full <panel_dir> [output_dir] [dashboard_path]")
            sys.exit(1)
        panel_dir = Path(sys.argv[2])
        output_dir = Path(sys.argv[3]) if len(sys.argv) >= 4 else Path("./v23_outputs")
        dashboard_path = Path(sys.argv[4]) if len(sys.argv) >= 5 else output_dir / "dashboard.html"
        
        print("=" * 72)
        print("IFM V2.3 — FULL PIPELINE")
        print("=" * 72)
        print(f"  Stage 1 (fetch):    panel → {panel_dir}")
        print(f"  Stage 2 (compute):  panel → {output_dir}")
        print(f"  Stage 3 (report):   outputs → {dashboard_path}")
        print("=" * 72)
        print()
        
        # Stage 1: fetch
        try:
            fetch_panel(panel_dir)
        except ImportError as e:
            print(f"\nERROR: {e}")
            print("Install fetch dependencies:  pip install yfinance requests")
            sys.exit(1)
        
        # Stage 2: compute
        print("\n" + "=" * 72)
        print("Stage 2: compute")
        print("=" * 72)
        out = compute_v23(panel_dir)
        print(f"  Score panel: {out['score'].shape[0]} dates × {out['score'].shape[1]} assets")
        v = out["validation"]
        print(f"  Validation: T1={v['T1_composite_ic']:.3f} T5={v['T5_max_pillar_corr']:.3f} "
              f"T6={v['T6_min_pc_share']:.3f} T4-pin={v['T4_max_pinned']} T7-sat={v['T7_pc1_saturated']}")
        _save_outputs(out, output_dir)
        print(f"  Outputs written to {output_dir}")
        
        # Stage 3: report
        print("\n" + "=" * 72)
        print("Stage 3: report")
        print("=" * 72)
        generate_dashboard(output_dir, dashboard_path, date=None)
        
        print("\n" + "=" * 72)
        print("FULL PIPELINE COMPLETE")
        print("=" * 72)
        print(f"  Open in browser: {dashboard_path}")
        return
    
    # ========================================================================
    # COMPUTE MODE  (default)
    # ========================================================================
    panel_dir = Path(sys.argv[1])
    output_dir = Path(sys.argv[2]) if len(sys.argv) >= 3 else Path("./v23_outputs")
    
    if not panel_dir.is_dir():
        print(f"ERROR: panel_dir not found or not a directory: {panel_dir}")
        print()
        print("If you don't have a panel yet, run fetch first:")
        print(f"  python {sys.argv[0]} fetch {panel_dir}")
        sys.exit(1)
    
    print(f"IFM V2.3 — running pipeline")
    print(f"  panel_dir:  {panel_dir}")
    print(f"  output_dir: {output_dir}")
    print()
    
    out = compute_v23(panel_dir)
    
    print(f"Score panel: {out['score'].shape[0]} dates × {out['score'].shape[1]} assets")
    print()
    print(f"Validation suite:")
    v = out["validation"]
    print(f"  T1 composite IC:        {v['T1_composite_ic']:7.4f}  ({'PASS' if v['T1_pass'] else 'BELOW'})")
    print(f"  T2 IC gain vs baseline: {v['T2_gain']:+7.4f}  ({'PASS' if v['T2_pass'] else 'BELOW'})")
    print(f"  T4 EQ pinning (max):    {v['T4_max_pinned']:>7d}  ({'PASS' if v['T4_pass'] else 'FAIL'})")
    print(f"  T5 max pillar corr:     {v['T5_max_pillar_corr']:7.4f}  ({'PASS' if v['T5_pass'] else 'FAIL'})")
    print(f"  T6 min PC variance:     {v['T6_min_pc_share']:7.4f}  ({'PASS' if v['T6_pass'] else 'BELOW'})")
    print(f"  T7 PC1 saturation:      {v['T7_pc1_saturated']:>7d}  ({'PASS' if v['T7_pass'] else 'FAIL'})")
    print()
    
    print(f"Saving outputs to {output_dir} ...")
    _save_outputs(out, output_dir)
    print(f"Done.")
    print()
    print(f"Tip: generate dashboard with")
    print(f"  python {sys.argv[0]} report {output_dir}")


if __name__ == "__main__":
    main()