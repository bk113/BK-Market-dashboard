"""
BK Market Dashboard — Consolidated
====================================
60-instrument universe · 12 asset classes · Performance, Risk & Fragility.

Outputs:
  docs/index.html   3-tab web page (GitHub Pages — auto-updated daily)
  PNG + PDF         visual report
  PPTX              PowerPoint deck
  Email             HTML brief via Gmail

Usage:
  python bk_market_dashboard.py --html                      # Generate web page (10yr history)
  python bk_market_dashboard.py --html --lookback 504       # GitHub Actions (2yr, faster)
  python bk_market_dashboard.py --pptx                      # PowerPoint deck
  python bk_market_dashboard.py --email            # Send HTML email brief
  python bk_market_dashboard.py --html --pptx      # Web page + PowerPoint
  python bk_market_dashboard.py --schedule         # Daily scheduler at 07:00 SGT Mon–Fri
  python bk_market_dashboard.py --now --html       # Run once immediately (testing)

Dependencies:
  pip install yfinance pandas numpy matplotlib schedule pytz python-pptx
"""

from __future__ import annotations

import argparse
import os
import smtplib
import time
import warnings
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import BytesIO

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import Rectangle
import numpy as np
import pandas as pd
import pytz
import schedule
import yfinance as yf

warnings.filterwarnings("ignore")


# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG — EDIT THESE
# ══════════════════════════════════════════════════════════════════════════════

RECIPIENT_EMAIL = "your@email.com"           # Who receives the brief
SENDER_EMAIL    = "your.gmail@gmail.com"     # Your Gmail address
GMAIL_APP_PASS  = "xxxx xxxx xxxx xxxx"      # Gmail App Password (not login password)
                                              # Get: myaccount.google.com > Security > App Passwords
SEND_TIME_SGT   = "07:00"                    # Daily send time (SGT)
OUT_DIR         = "market_dashboard"         # Output folder for PNG/PDF
RISK_FREE_RATE  = 0.045                      # 4.5% annualised risk-free rate

# ══════════════════════════════════════════════════════════════════════════════

SGT = pytz.timezone("Asia/Singapore")


# ── UNIVERSE ──────────────────────────────────────────────────────────────────
# (section_key, ticker, display_name)

UNIVERSE = [
    # ── Equities: US Broad ──
    ("EQ_US",    "ACWI",    "World (ACWI)"),
    ("EQ_US",    "SPY",     "S&P 500"),
    ("EQ_US",    "QQQ",     "Nasdaq 100"),
    ("EQ_US",    "IWM",     "Russell 2000"),
    # ── Equities: US Sectors ──
    ("EQ_SECT",  "XLF",     "Financials"),
    ("EQ_SECT",  "XLE",     "Energy"),
    ("EQ_SECT",  "XLU",     "Utilities"),
    ("EQ_SECT",  "VNQ",     "Real Estate (REITs)"),
    # ── Equities: Developed Markets ──
    ("EQ_DM",    "EFA",     "Europe Dev (EAFE)"),
    ("EQ_DM",    "EZU",     "Eurozone"),
    ("EQ_DM",    "EWU",     "UK"),
    ("EQ_DM",    "EWG",     "Germany"),
    ("EQ_DM",    "EWJ",     "Japan"),
    ("EQ_DM",    "EWA",     "Australia"),
    ("EQ_DM",    "EWS",     "Singapore"),
    # ── Equities: Global Indices ──
    ("EQ_IDX",   "FEZ",     "Euro STOXX 50"),
    ("EQ_IDX",   "EWH",     "Hang Seng / HK"),
    ("EQ_IDX",   "DBJP",    "Nikkei 225 (Japan)"),
    ("EQ_IDX",   "FLGB",    "FTSE 100 (UK)"),
    # ── Equities: Emerging Markets ──
    ("EQ_EM",    "EEM",     "EM Broad"),
    ("EQ_EM",    "FXI",     "China"),
    ("EQ_EM",    "INDA",    "India"),
    ("EQ_EM",    "EWY",     "Korea"),
    ("EQ_EM",    "EWT",     "Taiwan"),
    ("EQ_EM",    "EWZ",     "Brazil"),
    ("EQ_EM",    "EZA",     "South Africa"),
    # ── Defence & Geopolitical ──
    ("DEFENCE",  "ITA",     "US Aerospace & Defence"),
    ("DEFENCE",  "XAR",     "S&P Aerospace & Defence"),
    ("DEFENCE",  "DFEN",    "Defence Bull 3x"),
    # ── Fixed Income & Credit ──
    ("FI",       "BIL",     "Cash (T-Bills)"),
    ("FI",       "SHY",     "Treasuries 1-3Y"),
    ("FI",       "IEF",     "Treasuries 7-10Y"),
    ("FI",       "TLT",     "Treasuries 20Y+"),
    ("FI",       "AGG",     "US Aggregate"),
    ("FI",       "TIP",     "US TIPS"),
    ("FI",       "LQD",     "IG Credit"),
    ("FI",       "HYG",     "HY Credit"),
    ("FI",       "EMB",     "EM USD Sovereigns"),
    ("FI",       "EMLC",    "EM Local Currency"),
    ("FI",       "BKLN",    "Senior Loans"),
    # ── Commodities ──
    ("CMD",      "DBC",     "Broad Commodities"),
    ("CMD",      "GLD",     "Gold"),
    ("CMD",      "SLV",     "Silver"),
    ("CMD",      "USO",     "WTI Oil"),
    ("CMD",      "UNG",     "Natural Gas"),
    ("CMD",      "COPX",    "Copper Miners"),
    ("CMD",      "DBA",     "Agriculture"),
    # ── Fixed Income: EUR & Global ──
    ("FI_INTL",  "IGIB",    "USD IG Credit (Int)"),
    ("FI_INTL",  "IHY",     "Intl High Yield"),
    ("FI_INTL",  "BNDW",    "Global Aggregate"),
    # ── Crypto ──
    ("CRYPTO",   "BTC-USD", "Bitcoin"),
    # ── FX ──
    ("FX",       "UUP",     "US Dollar Index"),
    ("FX",       "EURUSD=X","EUR/USD"),
    ("FX",       "GBPUSD=X","GBP/USD"),
    ("FX",       "JPY=X",   "USD/JPY"),
    ("FX",       "SGD=X",   "USD/SGD"),
    ("FX",       "CNY=X",   "USD/CNY"),
    ("FX",       "AUDUSD=X","AUD/USD"),
    # ── Volatility ──
    ("VOL",      "VIXY",    "VIX Short-Term Futures"),
    ("VOL",      "UVXY",    "Ultra VIX Short-Term"),
]

SECTION_ORDER = ["EQ_US", "EQ_SECT", "EQ_DM", "EQ_IDX", "EQ_EM", "DEFENCE", "FI", "FI_INTL", "CMD", "CRYPTO", "FX", "VOL"]

SECTION_LABELS = {
    "EQ_US":   "EQUITIES — US BROAD",
    "EQ_SECT": "EQUITIES — US SECTORS",
    "EQ_DM":   "EQUITIES — DEVELOPED MARKETS",
    "EQ_IDX":  "EQUITIES — GLOBAL INDICES",
    "EQ_EM":   "EQUITIES — EMERGING MARKETS",
    "DEFENCE": "DEFENCE & GEOPOLITICAL",
    "FI":      "FIXED INCOME & CREDIT",
    "FI_INTL": "FIXED INCOME — EUR & GLOBAL",
    "CMD":     "COMMODITIES",
    "CRYPTO":  "CRYPTO",
    "FX":      "FX",
    "VOL":     "VOLATILITY",
}

N_INSTRUMENTS = len(UNIVERSE)

# ── CURRENCY MAP ──────────────────────────────────────────────────────────────
# Returns are price return in the instrument's local currency
CURRENCY_MAP = {
    "EQ_US": "USD", "EQ_SECT": "USD", "EQ_IDX": "USD",
    "EQ_DM": "USD", "EQ_EM": "USD", "DEFENCE": "USD",
    "FI": "USD", "FI_INTL": "USD", "CMD": "USD",
    "CRYPTO": "USD", "VOL": "USD", "FX": "USD",
}
# Per-ticker currency override for FX pairs
FX_CCY_MAP = {
    "EURUSD=X": "EUR", "GBPUSD=X": "GBP", "JPY=X": "JPY",
    "SGD=X": "SGD", "CNY=X": "CNY", "AUDUSD=X": "AUD",
    "UUP": "USD",
}




# ── COLORS (report) ───────────────────────────────────────────────────────────

BG    = "#0d1117"
CARD  = "#161b22"
DARK  = "#21262d"
WHITE = "#e6edf3"
GREY  = "#8b949e"
ACNT  = "#58a6ff"
RED   = "#f85149"
AMBER = "#e3b341"
GREEN = "#3fb950"


def _ret_color(v: float) -> str:
    if pd.isna(v):   return GREY
    if v >=  0.02:   return "#3fb950"
    if v >=  0.005:  return "#7ee787"
    if v >=  0.0:    return "#a8d5b0"
    if v >= -0.005:  return "#ffa657"
    if v >= -0.02:   return "#ff7b72"
    return RED


def _sharpe_color(v: float) -> str:
    if pd.isna(v): return GREY
    if v > 1.0:    return GREEN
    if v > 0.0:    return AMBER
    return RED


def _rag(dd: float) -> tuple[str, str]:
    """RAG signal based on max drawdown from 52-week high."""
    if pd.isna(dd): return GREY,  "  -  "
    if dd < -0.15:  return RED,   " RED "
    if dd < -0.07:  return AMBER, "AMBER"
    return GREEN, "GREEN"


# ══════════════════════════════════════════════════════════════════════════════
#  DATA  (with price cache for fast daily runs + full history)
# ══════════════════════════════════════════════════════════════════════════════

CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "prices_cache.csv")

def download(lookback_days: int = 2520) -> pd.DataFrame:
    tickers = [t for _, t, _ in UNIVERSE]
    print(f"[Download] {len(tickers)} tickers | lookback={lookback_days} days ...")

    # ── Load cache ─────────────────────────────────────────────────────────────
    cached = None
    if os.path.exists(CACHE_FILE):
        try:
            cached = pd.read_csv(CACHE_FILE, index_col=0, parse_dates=True)
            if cached.index.tz is not None:
                cached.index = cached.index.tz_localize(None)
            print(f"[Cache]  Loaded {len(cached)} days (last: {cached.index[-1].date()})")
        except Exception as e:
            print(f"[Cache]  Load failed: {e}")
            cached = None

    # ── Decide how far back to download ───────────────────────────────────────
    if cached is not None and len(cached) >= 756:
        start = (pd.Timestamp.today() - pd.Timedelta(days=60)).strftime("%Y-%m-%d")
        print(f"[Download] Cache hit — refreshing last 60 days ...")
    else:
        start = (pd.Timestamp.today() - pd.Timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        print(f"[Download] No cache — full {lookback_days}-day download ...")

    raw = yf.download(tickers, start=start, auto_adjust=True, progress=False)
    if raw.empty and cached is None:
        raise RuntimeError("No data returned from Yahoo Finance.")

    if not raw.empty:
        prices_new = raw["Close"] if "Close" in raw.columns else raw.xs("Close", axis=1, level=0)
        if prices_new.index.tz is not None:
            prices_new.index = prices_new.index.tz_localize(None)
        if cached is not None:
            prices = pd.concat([cached, prices_new])
            prices = prices[~prices.index.duplicated(keep="last")].sort_index()
        else:
            prices = prices_new
    else:
        prices = cached

    # ── Save updated cache ─────────────────────────────────────────────────────
    try:
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        prices.to_csv(CACHE_FILE)
        print(f"[Cache]  Saved {len(prices)} days")
    except Exception as e:
        print(f"[Cache]  Save failed: {e}")

    prices = prices.ffill(limit=3).dropna(how="all")
    prices = prices[[t for t in tickers if t in prices.columns]]

    print(f"[Download] {len(prices)} days  |  last close: {prices.index[-1].date()}")
    return prices


def compute_metrics(prices: pd.DataFrame) -> pd.DataFrame:
    today = prices.index[-1]

    def _ret(n: int) -> pd.Series:
        if len(prices) <= n:
            return pd.Series(np.nan, index=prices.columns)
        return prices.iloc[-1] / prices.iloc[-1 - n] - 1

    # YTD vs first trading day of current calendar year
    ytd_slice = prices[prices.index.year == today.year]
    ytd = (
        prices.iloc[-1] / ytd_slice.iloc[0] - 1
        if len(ytd_slice) > 1
        else pd.Series(np.nan, index=prices.columns)
    )

    # Vol as of latest date: 5-day realised vol annualised (captures current regime)
    vol_now  = prices.pct_change().tail(6).std()  * np.sqrt(252)

    # Vol 1 month ago: 20-day window ending ~21 trading days back
    ret_chg  = prices.pct_change()
    vol_1m_ago = (
        ret_chg.iloc[-42:-21].std() * np.sqrt(252)
        if len(ret_chg) >= 42
        else pd.Series(np.nan, index=prices.columns)
    )

    # 20-day annualised vol (short-term, for signal column)
    vol_20d = ret_chg.tail(21).std() * np.sqrt(252)

    # 1-year annualised vol + Sharpe
    daily_ret_1y = prices.pct_change().tail(252)
    vol_1y       = daily_ret_1y.std() * np.sqrt(252)
    ann_ret_1y   = daily_ret_1y.mean() * 252
    sharpe       = ((ann_ret_1y - RISK_FREE_RATE) / vol_1y.replace(0, np.nan)).clip(-5, 5)

    # Max drawdown from 252-day rolling peak
    window  = min(252, len(prices))
    peak    = prices.tail(window).cummax()
    max_dd  = prices.iloc[-1] / peak.iloc[-1] - 1

    # Sparkline data: last 20 trading days, normalised to first value
    spark_window = min(20, len(prices))
    spark_prices = prices.tail(spark_window)

    # Detect if market was closed today
    # Only check equity tickers (FX/Crypto trade 24/7 and skew the signal)
    ret1d_vals = _ret(1)
    eq_tickers = [t for _, t, _ in UNIVERSE if t in prices.columns and
                  t not in ["BTC-USD","EURUSD=X","GBPUSD=X","JPY=X","SGD=X","CNY=X","AUDUSD=X","UUP"]]
    eq_ret1d = ret1d_vals.reindex(eq_tickers).dropna()
    nonzero_eq = (eq_ret1d.abs() > 1e-4).sum()
    market_open_today = nonzero_eq >= max(3, len(eq_ret1d) * 0.15)

    rows = []
    for sec, ticker, name in UNIVERSE:
        if ticker not in prices.columns:
            continue
        rc, rl = _rag(max_dd.get(ticker, np.nan))
        # Normalised sparkline series (percent from 20-day-ago base)
        sp = spark_prices[ticker].dropna()
        spark = list((sp / sp.iloc[0] - 1) * 100) if len(sp) > 1 else []
        rows.append({
            "section":          sec,
            "ticker":           ticker,
            "name":             name,
            "ret_1d":           ret1d_vals.get(ticker, np.nan),
            "ret_1w":           _ret(5).get(ticker, np.nan),
            "ret_1m":           _ret(21).get(ticker, np.nan),
            "ret_3m":           _ret(63).get(ticker, np.nan),
            "ret_ytd":          ytd.get(ticker, np.nan),
            "vol_20d":          vol_20d.get(ticker, np.nan),
            "vol_now":          vol_now.get(ticker, np.nan),
            "vol_1m_ago":       vol_1m_ago.get(ticker, np.nan),
            "max_dd":           max_dd.get(ticker, np.nan),
            "sharpe":           sharpe.get(ticker, np.nan),
            "rag_color":        rc,
            "rag_label":        rl,
            "spark":            spark,
            "market_open":      market_open_today,
            "currency":         FX_CCY_MAP.get(ticker, CURRENCY_MAP.get(sec, "USD")),
        })

    ord_map = {s: i for i, s in enumerate(SECTION_ORDER)}
    df = pd.DataFrame(rows)
    df["_o"] = df["section"].map(ord_map).fillna(99)
    return df.sort_values(["_o", "name"]).drop(columns="_o").reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
#  VISUAL REPORT (PNG + PDF)
# ══════════════════════════════════════════════════════════════════════════════

def _fmt(v: float, signed: bool = True, dec: int = 1, pct: bool = True) -> str:
    if pd.isna(v): return "-"
    if pct:
        return f"{v * 100:+.{dec}f}%" if signed else f"{v * 100:.{dec}f}%"
    return f"{v:+.{dec}f}" if signed else f"{v:.{dec}f}"


def render_report(df: pd.DataFrame, as_of: str, out_dir: str) -> tuple[str, str]:
    # ── Layout constants ──
    FIG_W     = 28.0   # wider canvas
    HDR_ROWS  = 3.2
    COLHDR_R  = 1.2
    SEC_ROWS  = 1.0
    DATA_ROWS = 1.5    # tall rows → readable text
    FOOT_ROWS = 0.8
    ROW_INCH  = 0.36   # inches per row-unit → controls overall height

    n_sections = df["section"].nunique()
    n_assets   = len(df)
    total_h    = HDR_ROWS + COLHDR_R + n_sections * SEC_ROWS + n_assets * DATA_ROWS + FOOT_ROWS
    FIG_H      = total_h * ROW_INCH

    # Font sizes
    FS_TITLE   = 24
    FS_SUB     = 11
    FS_RAG_N   = 22
    FS_RAG_L   = 10
    FS_COLHDR  = 11
    FS_SECHDR  = 10
    FS_NAME    = 11
    FS_TICKER  = 10
    FS_DATA    = 11
    FS_FOOT    = 9

    # Detect market closed (all 1D returns ~0)
    market_open = bool(df["market_open"].iloc[0]) if "market_open" in df.columns else True

    # ── Column x positions (0–100 scale) ──
    # Trend sparkline always shown; 1D suppressed when market closed
    if market_open:
        CX = {
            "name":    1.5,
            "ticker":  27.0,
            "trend":   33.5,
            "1d":      39.5,
            "1w":      46.0,
            "1m":      52.5,
            "3m":      59.0,
            "ytd":     65.5,
            "vol":     72.0,
            "dd":      79.0,
            "sharpe":  86.0,
            "sig":     94.0,
        }
    else:
        CX = {
            "name":    1.5,
            "ticker":  27.0,
            "trend":   33.5,
            "1w":      41.0,
            "1m":      49.0,
            "3m":      57.0,
            "ytd":     65.0,
            "vol":     73.0,
            "dd":      80.5,
            "sharpe":  88.0,
            "sig":     95.5,
        }

    fig = plt.figure(figsize=(FIG_W, FIG_H), facecolor=BG)
    ax  = fig.add_axes([0.0, 0.0, 1.0, 1.0])
    ax.set_xlim(0, 100)
    ax.set_ylim(0, total_h)
    ax.set_facecolor(BG)
    ax.axis("off")

    # ── Header ──
    y     = total_h
    hdr_y = y - HDR_ROWS
    ax.add_patch(Rectangle((0, hdr_y), 100, HDR_ROWS, facecolor="#1c2128", zorder=0))

    ax.text(2, hdr_y + HDR_ROWS * 0.70,
            "BK  MARKET  DASHBOARD",
            fontsize=FS_TITLE, fontweight="bold", color=WHITE,
            ha="left", va="center", family="monospace")
    ax.text(2, hdr_y + HDR_ROWS * 0.26,
            f"As of  {as_of}   |   {N_INSTRUMENTS}-instrument universe   |   "
            f"Source: Yahoo Finance   |   Price return, local currency",
            fontsize=FS_SUB, color=GREY, ha="left", va="center")

    # RAG summary counts (top-right of header)
    n_red   = (df["rag_label"].str.strip() == "RED").sum()
    n_amber = (df["rag_label"].str.strip() == "AMBER").sum()
    n_green = (df["rag_label"].str.strip() == "GREEN").sum()
    for xi, (lbl, cnt, col) in enumerate([
        ("RED",   n_red,   RED),
        ("AMBER", n_amber, AMBER),
        ("GREEN", n_green, GREEN),
    ]):
        xp = 77 + xi * 8
        ax.text(xp, hdr_y + HDR_ROWS * 0.72, str(cnt),
                fontsize=FS_RAG_N, fontweight="bold", color=col, ha="center", va="center")
        ax.text(xp, hdr_y + HDR_ROWS * 0.26, lbl,
                fontsize=FS_RAG_L, color=GREY, ha="center", va="center")

    y = hdr_y

    # ── Column header bar ──
    ch_y = y - COLHDR_R
    ax.add_patch(Rectangle((0, ch_y), 100, COLHDR_R, facecolor=DARK, zorder=0))
    col_hdrs = [
        ("name",   "Asset",      "left"),
        ("ticker", "Ticker",     "center"),
        ("trend",  "20D Trend",  "center"),
    ]
    if market_open:
        col_hdrs.append(("1d", "1D", "center"))
    col_hdrs += [
        ("1w",     "1W",      "center"),
        ("1m",     "1M",      "center"),
        ("3m",     "3M",      "center"),
        ("ytd",    "YTD",     "center"),
        ("vol",    "Vol 20D", "center"),
        ("dd",     "Max DD",  "center"),
        ("sharpe", "Sharpe",  "center"),
        ("sig",    "Signal",  "center"),
    ]
    for key, lbl, ha in col_hdrs:
        ax.text(CX[key], ch_y + COLHDR_R * 0.5, lbl,
                fontsize=FS_COLHDR, fontweight="bold", color=ACNT, ha=ha, va="center")

    y        = ch_y
    prev_sec = None

    # ── Data rows ──
    for _, row in df.iterrows():

        # Section divider
        if row["section"] != prev_sec:
            prev_sec = row["section"]
            s_y      = y - SEC_ROWS
            ax.add_patch(Rectangle((0, s_y), 100, SEC_ROWS, facecolor=CARD, zorder=0))
            ax.text(CX["name"], s_y + SEC_ROWS * 0.5,
                    SECTION_LABELS.get(row["section"], row["section"]),
                    fontsize=FS_SECHDR, fontweight="bold", color=ACNT,
                    ha="left", va="center", alpha=0.9)
            y = s_y

        r_y = y - DATA_ROWS
        ax.add_patch(Rectangle((0, r_y), 100, DATA_ROWS, facecolor=BG, zorder=0))
        ax.axhline(r_y, color=DARK, lw=0.5, xmin=0.01, xmax=0.99)
        mid = r_y + DATA_ROWS * 0.5

        # Name + ticker
        ax.text(CX["name"],   mid, row["name"][:32],
                fontsize=FS_NAME, color=WHITE, ha="left", va="center")
        ax.text(CX["ticker"], mid, row["ticker"],
                fontsize=FS_TICKER, color=GREY, ha="center", va="center", family="monospace")

        # ── Sparkline (20-day trend mini chart) ──────────────────────────────
        spark = row.get("spark", [])
        if len(spark) > 1:
            sp_arr   = np.array(spark)
            sp_color = GREEN if sp_arr[-1] >= 0 else RED
            # Convert data coords → axes (0-100 x, 0-total_h y)
            sp_w   = 4.5   # width in data-x units
            sp_h   = DATA_ROWS * 0.65
            sp_x0  = CX["trend"] - sp_w / 2
            sp_y0  = mid - sp_h / 2
            sp_x1  = CX["trend"] + sp_w / 2
            sp_y1  = mid + sp_h / 2
            # Normalise data to fit in box
            mn, mx = sp_arr.min(), sp_arr.max()
            rng    = mx - mn if mx != mn else 1.0
            xs = np.linspace(sp_x0, sp_x1, len(sp_arr))
            ys = sp_y0 + (sp_arr - mn) / rng * sp_h
            # Shaded fill under/over zero line
            zero_y = sp_y0 + (0 - mn) / rng * sp_h
            zero_y = np.clip(zero_y, sp_y0, sp_y1)
            ax.fill_between(xs, zero_y, ys,
                            where=(ys >= zero_y), color=GREEN, alpha=0.18, linewidth=0)
            ax.fill_between(xs, zero_y, ys,
                            where=(ys < zero_y),  color=RED,   alpha=0.18, linewidth=0)
            ax.plot(xs, ys, color=sp_color, linewidth=0.9, solid_capstyle="round")
            # Zero baseline
            ax.axhline(zero_y, xmin=(sp_x0 / 100), xmax=(sp_x1 / 100),
                       color=GREY, linewidth=0.3, alpha=0.5)

        # Return columns
        ret_cols = []
        if market_open:
            ret_cols.append(("1d", "ret_1d"))
        ret_cols += [("1w", "ret_1w"), ("1m", "ret_1m"), ("3m", "ret_3m"), ("ytd", "ret_ytd")]
        for key, col_key in ret_cols:
            v = row[col_key]
            ax.text(CX[key], mid, _fmt(v),
                    fontsize=FS_DATA, color=_ret_color(v), ha="center", va="center",
                    family="monospace")

        # Vol 20D (unsigned, grey — informational)
        ax.text(CX["vol"], mid, _fmt(row["vol_20d"], signed=False),
                fontsize=FS_DATA, color=GREY, ha="center", va="center", family="monospace")

        # Max drawdown
        v_dd = row["max_dd"]
        ax.text(CX["dd"], mid, _fmt(v_dd),
                fontsize=FS_DATA, color=_ret_color(v_dd), ha="center", va="center",
                family="monospace")

        # Sharpe
        v_sh = row["sharpe"]
        ax.text(CX["sharpe"], mid, _fmt(v_sh, pct=False, dec=2),
                fontsize=FS_DATA, color=_sharpe_color(v_sh), ha="center", va="center",
                family="monospace")

        # Signal (dot + label)
        rc = row["rag_color"]
        ax.text(CX["sig"] - 2.5, mid, "●", fontsize=FS_DATA + 2, color=rc, ha="center", va="center")
        ax.text(CX["sig"] + 2.5, mid, row["rag_label"].strip(),
                fontsize=FS_DATA - 1, color=rc, ha="center", va="center")

        y = r_y

    # ── Footer ──
    ax.text(2, FOOT_ROWS * 0.5,
            "Signal: RED = Max DD < -15%   AMBER = -15% to -7%   GREEN = > -7% from 52-week high   |   "
            "Vol 20D = annualised 20-day vol   |   Sharpe = 1Y excess return / vol  (rf = 4.5%)",
            fontsize=FS_FOOT, color=GREY, ha="left", va="center")
    ax.text(98, FOOT_ROWS * 0.5,
            f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}   |   CONFIDENTIAL",
            fontsize=FS_FOOT, color=GREY, ha="right", va="center")

    # ── Save ──
    os.makedirs(out_dir, exist_ok=True)
    tag      = datetime.now().strftime("%Y%m%d_%H%M")
    png_path = os.path.join(out_dir, f"market_dashboard_{tag}.png")
    pdf_path = os.path.join(out_dir, f"market_dashboard_{tag}.pdf")

    fig.savefig(png_path, dpi=150, bbox_inches="tight", facecolor=BG)
    with PdfPages(pdf_path) as pdf:
        pdf.savefig(fig, bbox_inches="tight", facecolor=BG)
    plt.close(fig)

    return png_path, pdf_path


# ══════════════════════════════════════════════════════════════════════════════
#  EMAIL BRIEF (HTML)
# ══════════════════════════════════════════════════════════════════════════════

def _ec_ret(v):
    """Email color + formatted string for a return (fraction)."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "#6b7280", "-"
    pct  = v * 100
    sign = "+" if pct > 0 else ""
    if pct >=  3.0: return "#065f46", f"{sign}{pct:.2f}%"
    if pct >=  1.5: return "#059669", f"{sign}{pct:.2f}%"
    if pct >=  0.0: return "#374151", f"{sign}{pct:.2f}%"
    if pct >= -1.5: return "#dc2626", f"{sign}{pct:.2f}%"
    if pct >= -3.0: return "#b91c1c", f"{sign}{pct:.2f}%"
    return "#7f1d1d", f"{sign}{pct:.2f}%"


def _ec_vol(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "#6b7280", "-"
    pct = v * 100
    c   = "#dc2626" if pct > 30 else "#d97706" if pct > 18 else "#059669"
    return c, f"{pct:.1f}%"


def _ec_dd(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "#6b7280", "-"
    pct = v * 100
    c   = "#dc2626" if pct < -15 else "#d97706" if pct < -7 else "#059669"
    return c, f"{pct:.1f}%"


def _ec_sharpe(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "#6b7280", "-"
    c = "#059669" if v > 1 else "#d97706" if v > 0 else "#dc2626"
    return c, f"{v:.2f}"


def build_email_html(df: pd.DataFrame) -> str:
    now      = datetime.now(SGT)
    date_str = now.strftime("%a %d %b %Y · %H:%M SGT")

    ret_abs = df["ret_1d"].abs() * 100
    n_alerts  = int((ret_abs >= 3.0).sum())
    n_watches = int(((ret_abs >= 1.5) & (ret_abs < 3.0)).sum())
    n_stable  = int((ret_abs < 1.5).sum())

    def _td(color, val, extra_style=""):
        return (
            f'<td style="padding:7px 9px;font-family:monospace;font-size:11px;'
            f'text-align:right;color:{color};border-right:1px solid #e2e6ea;{extra_style}">'
            f'{val}</td>'
        )

    rows_html = ""
    prev_sec  = None

    for _, row in df.iterrows():
        if row["section"] != prev_sec:
            prev_sec  = row["section"]
            sec_label = SECTION_LABELS.get(row["section"], row["section"])
            rows_html += (
                f'<tr><td colspan="11" style="background:#f1f3f5;font-size:9px;font-weight:700;'
                f'letter-spacing:2px;text-transform:uppercase;color:#6b7280;'
                f'padding:7px 12px;border-top:2px solid #cbd2d9;">{sec_label}</td></tr>'
            )

        c1d,  v1d  = _ec_ret(row["ret_1d"])
        c1w,  v1w  = _ec_ret(row["ret_1w"])
        c1m,  v1m  = _ec_ret(row["ret_1m"])
        c3m,  v3m  = _ec_ret(row["ret_3m"])
        cytd, vytd = _ec_ret(row["ret_ytd"])
        cvol, vvol = _ec_vol(row["vol_20d"])
        cdd,  vdd  = _ec_dd(row["max_dd"])
        csh,  vsh  = _ec_sharpe(row["sharpe"])
        rc         = row["rag_color"]
        rl         = row["rag_label"].strip()

        rows_html += (
            f'<tr style="border-bottom:1px solid #e2e6ea;">'
            f'<td style="padding:7px 9px;font-size:11px;color:#111827;'
            f'border-right:1px solid #e2e6ea;white-space:nowrap;">{row["name"]}</td>'
            f'<td style="padding:7px 9px;font-family:monospace;font-size:11px;font-weight:700;'
            f'color:#374151;border-right:1px solid #e2e6ea;">{row["ticker"]}</td>'
            f'{_td(c1d,v1d)}{_td(c1w,v1w)}{_td(c1m,v1m)}{_td(c3m,v3m)}{_td(cytd,vytd)}'
            f'{_td(cvol,vvol,"border-left:2px solid #dbeafe;")}'
            f'{_td(cdd,vdd)}{_td(csh,vsh)}'
            f'<td style="padding:7px 9px;font-family:monospace;font-size:10px;'
            f'text-align:center;color:{rc};">&#9679; {rl}</td>'
            f'</tr>'
        )

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f4f5f7;font-family:'IBM Plex Sans',Arial,sans-serif;">
<div style="max-width:1000px;margin:0 auto;padding:20px 12px;">

  <!-- HEADER -->
  <div style="background:#111827;padding:18px 24px;">
    <div style="display:flex;justify-content:space-between;align-items:center;">
      <div>
        <div style="font-size:15px;font-weight:700;color:#fff;letter-spacing:1px;font-family:monospace;">
          BK <span style="color:#34d399;">MARKET</span> DASHBOARD
        </div>
        <div style="font-size:9px;color:#6b7280;letter-spacing:2px;margin-top:4px;">
          {N_INSTRUMENTS}-INSTRUMENT UNIVERSE &nbsp;·&nbsp; PERFORMANCE &amp; RISK &amp; FRAGILITY
        </div>
      </div>
      <div style="text-align:right;">
        <div style="font-size:10px;color:#9ca3af;font-family:monospace;">{date_str}</div>
        <div style="font-size:9px;color:#6b7280;font-family:monospace;margin-top:3px;">
          AUTO-BRIEF &nbsp;·&nbsp; {SEND_TIME_SGT} SGT
        </div>
      </div>
    </div>
  </div>

  <!-- SUMMARY STRIP -->
  <div style="background:#fff;border:1px solid #e2e6ea;border-top:none;
    padding:10px 24px;display:flex;gap:28px;margin-bottom:14px;">
    <div>
      <div style="font-size:8px;letter-spacing:2px;text-transform:uppercase;color:#9ca3af;">Alerts &ge;3%</div>
      <div style="font-size:20px;font-weight:700;color:#dc2626;font-family:monospace;">{n_alerts}</div>
    </div>
    <div>
      <div style="font-size:8px;letter-spacing:2px;text-transform:uppercase;color:#9ca3af;">Watch 1.5–3%</div>
      <div style="font-size:20px;font-weight:700;color:#d97706;font-family:monospace;">{n_watches}</div>
    </div>
    <div>
      <div style="font-size:8px;letter-spacing:2px;text-transform:uppercase;color:#9ca3af;">Stable</div>
      <div style="font-size:20px;font-weight:700;color:#059669;font-family:monospace;">{n_stable}</div>
    </div>
    <div>
      <div style="font-size:8px;letter-spacing:2px;text-transform:uppercase;color:#9ca3af;">Instruments</div>
      <div style="font-size:20px;font-weight:700;color:#111827;font-family:monospace;">{len(df)}/{N_INSTRUMENTS}</div>
    </div>
  </div>

  <!-- TABLE -->
  <table style="width:100%;border-collapse:collapse;background:#fff;border:1px solid #e2e6ea;">
    <thead>
      <tr style="background:#111827;">
        <th style="padding:9px 9px;text-align:left;color:#9ca3af;font-size:8px;
          letter-spacing:1.5px;text-transform:uppercase;font-family:monospace;
          min-width:130px;border-right:1px solid #1f2937;">Asset</th>
        <th style="padding:9px 9px;text-align:left;color:#9ca3af;font-size:8px;
          letter-spacing:1.5px;font-family:monospace;border-right:1px solid #1f2937;">Ticker</th>
        <th style="padding:9px 9px;text-align:right;color:#6b9fd4;font-size:8px;
          letter-spacing:1.5px;font-family:monospace;border-right:1px solid #1f2937;">1D</th>
        <th style="padding:9px 9px;text-align:right;color:#9ca3af;font-size:8px;
          font-family:monospace;border-right:1px solid #1f2937;">1W</th>
        <th style="padding:9px 9px;text-align:right;color:#9ca3af;font-size:8px;
          font-family:monospace;border-right:1px solid #1f2937;">1M</th>
        <th style="padding:9px 9px;text-align:right;color:#9ca3af;font-size:8px;
          font-family:monospace;border-right:1px solid #1f2937;">3M</th>
        <th style="padding:9px 9px;text-align:right;color:#9ca3af;font-size:8px;
          font-family:monospace;border-right:1px solid #1f2937;">YTD</th>
        <th style="padding:9px 9px;text-align:right;color:#f87171;font-size:8px;
          font-family:monospace;border-left:2px solid #3a1a1a;
          border-right:1px solid #1f2937;">Vol 20D</th>
        <th style="padding:9px 9px;text-align:right;color:#9ca3af;font-size:8px;
          font-family:monospace;border-right:1px solid #1f2937;">Max DD</th>
        <th style="padding:9px 9px;text-align:right;color:#9ca3af;font-size:8px;
          font-family:monospace;border-right:1px solid #1f2937;">Sharpe</th>
        <th style="padding:9px 9px;text-align:center;color:#9ca3af;font-size:8px;
          font-family:monospace;">Signal</th>
      </tr>
    </thead>
    <tbody>{rows_html}</tbody>
  </table>

  <!-- FOOTER -->
  <div style="margin-top:14px;padding:10px 0;border-top:1px solid #e2e6ea;
    display:flex;justify-content:space-between;align-items:center;">
    <div style="font-size:9px;color:#9ca3af;font-family:monospace;line-height:1.9;">
      Signal: RED &lt; &minus;15% &nbsp;|&nbsp; AMBER &minus;15% to &minus;7% &nbsp;|&nbsp; GREEN &gt; &minus;7% — from 52-week high<br>
      Sharpe = 1Y annualised excess return / vol &nbsp;(rf = 4.5%)<br>
      Prices via Yahoo Finance (15 min delay)
    </div>
    <div style="text-align:right;">
      <div style="font-size:18px;font-weight:700;color:#111827;letter-spacing:-1px;">BK</div>
      <div style="font-size:9px;color:#6b7280;">Institutional Risk &nbsp;·&nbsp; Singapore</div>
      <div style="font-size:8px;color:#9ca3af;margin-top:2px;letter-spacing:1px;">
        CONFIDENTIAL &nbsp;·&nbsp; INTERNAL USE ONLY
      </div>
    </div>
  </div>

</div>
</body>
</html>"""



# ══════════════════════════════════════════════════════════════════════════════
#  BK FRAGILITY ENGINE
# ══════════════════════════════════════════════════════════════════════════════

FRAGILITY_WEIGHTS = {"dd":0.22,"vol":0.15,"cvar":0.20,"trend":0.15,"corr":0.18,"volz":0.10}

def _frag_logistic(x):
    return 1.0 / (1.0 + np.exp(-x))

def _robust_zscore(s, window=252, clip=4.0):
    min_p = max(60, window // 3)
    med   = s.rolling(window, min_periods=min_p).median()
    mad   = s.rolling(window, min_periods=min_p).apply(
        lambda x: np.median(np.abs(x - np.median(x))), raw=True)
    return ((s - med) / (1.4826 * mad.replace(0, 1e-6))).clip(-clip, clip)

def compute_fragility(prices: pd.DataFrame) -> pd.DataFrame:
    rets    = prices.pct_change().replace([np.inf,-np.inf], np.nan)
    wdd     = min(252, len(prices))
    peak    = prices.rolling(wdd, min_periods=20).max()
    dd      = (prices / peak - 1.0).abs()
    vol20   = rets.rolling(20, min_periods=10).std() * np.sqrt(252)

    def _cvar(x):
        q = np.nanquantile(x, 0.05); tail = x[x <= q]
        return abs(np.nanmean(tail)) if len(tail) > 0 else np.nan
    cvar60  = rets.rolling(60, min_periods=20).apply(_cvar, raw=False)
    ma200   = prices.rolling(200, min_periods=50).mean()
    dist200 = (-(prices / ma200 - 1.0)).clip(lower=0)
    wcol    = "ACWI" if "ACWI" in rets.columns else rets.columns[0]
    corr_w  = pd.DataFrame(index=rets.index, columns=rets.columns, dtype=float)
    for c in rets.columns:
        corr_w[c] = rets[c].rolling(60, min_periods=20).corr(rets[wcol]).clip(lower=0)
    vov     = vol20.rolling(20, min_periods=10).std()
    mu_vov  = vov.rolling(60, min_periods=20).mean()
    sd_vov  = vov.rolling(60, min_periods=20).std().replace(0, np.nan)
    volz    = ((vov - mu_vov) / sd_vov).abs()

    zw      = min(756, len(prices) - 1)  # 3-year rolling window for robust calibration
    t2m     = {t: (s, n) for s, t, n in UNIVERSE}
    w       = FRAGILITY_WEIGHTS
    tw      = sum(w.values())
    rows    = []
    for col in prices.columns:
        zd = _robust_zscore(dd[col],      zw)
        zv = _robust_zscore(vol20[col],   zw)
        zc = _robust_zscore(cvar60[col],  zw)
        zt = _robust_zscore(dist200[col], zw)
        zr = _robust_zscore(corr_w[col],  zw)
        zz = _robust_zscore(volz[col],    zw)
        lat = (w["dd"]*zd.fillna(0)+w["vol"]*zv.fillna(0)+w["cvar"]*zc.fillna(0)+
               w["trend"]*zt.fillna(0)+w["corr"]*zr.fillna(0)+w["volz"]*zz.fillna(0))
        # Scale by 0.5 to spread scores — prevents saturation at extremes
        sc  = 100.0 * _frag_logistic(lat.ewm(span=10,adjust=False).mean() * 0.5)
        v   = float(sc.iloc[-1]) if not sc.empty else np.nan
        if pd.isna(v): continue
        rag = "CRISIS" if v>=70 else "STRESSED" if v>=50 else "CALM"
        sec, name = t2m.get(col, ("", col))

        def _p(z, k): return round(float(w[k]*z.iloc[-1]/tw*100),1) if not pd.isna(z.iloc[-1]) else 0.0
        rows.append({"ticker":col,"name":name,"section":sec,"fragility":round(v,1),"rag":rag,
            "pillar_dd":_p(zd,"dd"),"pillar_vol":_p(zv,"vol"),"pillar_cvar":_p(zc,"cvar"),
            "pillar_trend":_p(zt,"trend"),"pillar_corr":_p(zr,"corr"),"pillar_volz":_p(zz,"volz")})

    fdf = pd.DataFrame(rows).sort_values("fragility",ascending=False).reset_index(drop=True)
    if not fdf.empty:
        ss = float(fdf["fragility"].median())
        fdf.attrs["system_score"] = round(ss,1)
        fdf.attrs["regime"] = "CRISIS" if ss>=70 else "STRESSED" if ss>=50 else "CALM"
    return fdf


# ══════════════════════════════════════════════════════════════════════════════
#  3-TAB WEB PAGE  (Performance | Risk | Fragility)
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
#  REGIME ENGINE  (State Machine — deterministic, governance-friendly)
# ══════════════════════════════════════════════════════════════════════════════

def compute_regime(prices: pd.DataFrame) -> dict:
    """
    Compute market regime using deterministic state machine on ACWI (world proxy).
    Returns dict with current regime, timeline, stats and drivers.
    """
    world_col = "ACWI" if "ACWI" in prices.columns else prices.columns[0]
    rets  = prices[world_col].pct_change().replace([np.inf,-np.inf], np.nan)
    vol20 = rets.rolling(20, min_periods=10).std() * np.sqrt(252)
    window_dd = min(2520, len(prices))  # full 10yr window for episodes
    peak  = prices[world_col].rolling(window_dd, min_periods=20).max()
    dd    = prices[world_col] / peak - 1.0

    s = pd.DataFrame({"vol": vol20, "dd": dd}).dropna()
    min_history = 252

    # Ex-ante expanding quantile thresholds (no look-ahead)
    vol70 = s["vol"].expanding(min_periods=min_history).quantile(0.70).shift(1)
    vol90 = s["vol"].expanding(min_periods=min_history).quantile(0.90).shift(1)
    dd30  = s["dd"].expanding(min_periods=min_history).quantile(0.30).shift(1)
    dd10  = s["dd"].expanding(min_periods=min_history).quantile(0.10).shift(1)

    valid  = vol70.notna() & vol90.notna() & dd30.notna() & dd10.notna()
    regime = pd.Series("Calm", index=s.index, dtype="object")
    regime.loc[~valid] = np.nan
    stressed = valid & ((s["vol"] >= vol70) | (s["dd"] <= dd30))
    crisis   = valid & ((s["vol"] >= vol90) | (s["dd"] <= dd10))
    regime.loc[stressed] = "Stressed"
    regime.loc[crisis]   = "Crisis"
    regime = regime.dropna()

    if regime.empty:
        return {"regime": "Calm", "days_in_regime": 0, "timeline": [], "stats": {}, "drivers": {}}

    # Current regime
    current = regime.iloc[-1]

    # Days in current streak
    streak = 1
    for i in range(len(regime)-2, -1, -1):
        if regime.iloc[i] == current:
            streak += 1
        else:
            break

    # Timeline: last 504 trading days (2 years) — daily regime
    timeline_raw = regime.tail(504)
    timeline = []
    for date, reg in timeline_raw.items():
        timeline.append({
            "date": date.strftime("%Y-%m-%d"),
            "regime": reg,
            "color": "#f85149" if reg=="Crisis" else "#e3b341" if reg=="Stressed" else "#3fb950"
        })

    # Stats: days in each regime over full history
    total = len(regime)
    stats = {
        "Calm":     {"days": int((regime=="Calm").sum()),     "pct": round((regime=="Calm").sum()/total*100,1)},
        "Stressed": {"days": int((regime=="Stressed").sum()), "pct": round((regime=="Stressed").sum()/total*100,1)},
        "Crisis":   {"days": int((regime=="Crisis").sum()),   "pct": round((regime=="Crisis").sum()/total*100,1)},
    }

    # Average duration per regime
    durations = {"Calm":[], "Stressed":[], "Crisis":[]}
    cur_reg = regime.iloc[0]; cur_len = 1
    for i in range(1, len(regime)):
        if regime.iloc[i] == cur_reg:
            cur_len += 1
        else:
            durations[cur_reg].append(cur_len)
            cur_reg = regime.iloc[i]; cur_len = 1
    durations[cur_reg].append(cur_len)
    for r in durations:
        stats[r]["avg_duration"] = round(float(np.mean(durations[r])), 0) if durations[r] else 0

    # Drivers: current vol and dd vs historical percentiles
    cur_vol = float(vol20.iloc[-1]) if not pd.isna(vol20.iloc[-1]) else 0
    cur_dd  = float(dd.iloc[-1])    if not pd.isna(dd.iloc[-1])    else 0
    vol_pct = float((vol20.dropna() <= cur_vol).mean() * 100)
    dd_pct  = float((dd.dropna()   >= cur_dd).mean()  * 100)  # pct ABOVE current dd

    # Crisis episodes (drawdown < -15%)
    episodes = []
    in_ep = False; ep_start = None
    for date, val in dd.items():
        if val < -0.15 and not in_ep:
            in_ep = True; ep_start = date
        elif val >= -0.10 and in_ep:
            in_ep = False
            episodes.append({"start": ep_start.strftime("%b %Y"),
                              "end":   date.strftime("%b %Y"),
                              "depth": round(float(dd[ep_start:date].min()*100),1)})
    if in_ep:
        episodes.append({"start": ep_start.strftime("%b %Y"), "end": "Present",
                          "depth": round(float(dd[ep_start:].min()*100),1)})

    return {
        "regime":         current,
        "days_in_regime": streak,
        "timeline":       timeline,
        "stats":          stats,
        "drivers": {
            "vol_now":   round(cur_vol*100, 1),
            "vol_pct":   round(vol_pct, 0),
            "dd_now":    round(cur_dd*100,  1),
            "dd_pct":    round(dd_pct,  0),
        },
        "episodes": episodes[-8:],  # last 8 crisis episodes
    }


# ══════════════════════════════════════════════════════════════════════════════
#  FEAR & GREED ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def compute_fear_greed(prices: pd.DataFrame) -> dict:
    """
    7-component Fear & Greed Index (0=Extreme Fear, 100=Extreme Greed).
    Each component scored 0-100 then equally weighted.
    """
    rets = prices.pct_change().replace([np.inf,-np.inf], np.nan)
    scores = {}
    details = {}

    def _pct_rank(series, val):
        """Where does val sit in historical distribution? 0-100."""
        clean = series.dropna()
        if clean.empty or pd.isna(val): return 50.0
        return float((clean <= val).mean() * 100)

    # ── 1. Volatility: VIXY vs 50D MA (high vol = fear) ──────────────────────
    if "VIXY" in prices.columns:
        vixy = prices["VIXY"].dropna()
        ma50 = vixy.rolling(50, min_periods=20).mean()
        ratio = vixy / ma50 - 1
        cur   = float(ratio.iloc[-1]) if not ratio.empty else 0
        # High ratio = high vol vs norm = fear → invert
        raw = _pct_rank(ratio, cur)
        scores["Volatility"] = max(0, min(100, 100 - raw))
        details["Volatility"] = {"value": f"VIXY {cur*100:+.1f}% vs 50D MA", "score": scores["Volatility"]}

    # ── 2. Market Momentum: SPY vs 125D MA ───────────────────────────────────
    if "SPY" in prices.columns:
        spy  = prices["SPY"].dropna()
        ma125= spy.rolling(125, min_periods=50).mean()
        ratio= spy / ma125 - 1
        cur  = float(ratio.iloc[-1]) if not ratio.empty else 0
        raw  = _pct_rank(ratio, cur)
        scores["Momentum"] = max(0, min(100, raw))
        details["Momentum"] = {"value": f"SPY {cur*100:+.1f}% vs 125D MA", "score": scores["Momentum"]}

    # ── 3. Market Breadth: % instruments above their 50D MA ──────────────────
    ma50_all = prices.rolling(50, min_periods=20).mean()
    above = (prices.iloc[-1] > ma50_all.iloc[-1]).sum()
    total_avail = prices.shape[1]
    breadth_pct = above / total_avail * 100 if total_avail > 0 else 50
    scores["Breadth"] = float(breadth_pct)
    details["Breadth"] = {"value": f"{above}/{total_avail} above 50D MA", "score": scores["Breadth"]}

    # ── 4. Safe Haven Demand: TLT outperformance vs SPY (20D) ────────────────
    if "TLT" in prices.columns and "SPY" in prices.columns:
        tlt_ret = prices["TLT"].pct_change(20)
        spy_ret = prices["SPY"].pct_change(20)
        spread  = tlt_ret - spy_ret  # positive = bonds beating equities = fear
        cur     = float(spread.iloc[-1]) if not spread.dropna().empty else 0
        raw     = _pct_rank(spread.dropna(), cur)
        # High spread (bonds beating) = fear → invert
        scores["Safe Haven"] = max(0, min(100, 100 - raw))
        details["Safe Haven"] = {"value": f"TLT vs SPY 20D: {cur*100:+.1f}%", "score": scores["Safe Haven"]}

    # ── 5. Junk Bond Demand: HYG vs IEF (credit risk appetite) ──────────────
    if "HYG" in prices.columns and "IEF" in prices.columns:
        hyg_ret = prices["HYG"].pct_change(20)
        ief_ret = prices["IEF"].pct_change(20)
        spread  = hyg_ret - ief_ret  # positive = junk beating govt = greed
        cur     = float(spread.iloc[-1]) if not spread.dropna().empty else 0
        raw     = _pct_rank(spread.dropna(), cur)
        scores["Junk Bonds"] = max(0, min(100, raw))
        details["Junk Bonds"] = {"value": f"HYG vs IEF 20D: {cur*100:+.1f}%", "score": scores["Junk Bonds"]}

    # ── 6. Market Strength: % instruments within 5% of 52W high ─────────────
    hi52 = prices.rolling(252, min_periods=100).max()
    near_high = ((prices.iloc[-1] / hi52.iloc[-1]) >= 0.95).sum()
    strength_pct = near_high / total_avail * 100 if total_avail > 0 else 50
    scores["Strength"] = float(strength_pct)
    details["Strength"] = {"value": f"{near_high}/{total_avail} within 5% of 52W high", "score": scores["Strength"]}

    # ── 7. Put/Call Proxy: UVXY vs VIXY ratio (panic hedging) ────────────────
    if "UVXY" in prices.columns and "VIXY" in prices.columns:
        ratio_ts = prices["UVXY"] / prices["VIXY"].replace(0, np.nan)
        ratio_ts = ratio_ts.dropna()
        cur      = float(ratio_ts.iloc[-1]) if not ratio_ts.empty else 1
        raw      = _pct_rank(ratio_ts, cur)
        # High ratio = panic = fear → invert
        scores["Put/Call Proxy"] = max(0, min(100, 100 - raw))
        details["Put/Call Proxy"] = {"value": f"UVXY/VIXY ratio: {cur:.2f}", "score": scores["Put/Call Proxy"]}

    # ── Composite ─────────────────────────────────────────────────────────────
    if not scores:
        return {"score": 50, "label": "Neutral", "emoji": "😐", "details": {}}

    # Equal weight across available components
    score_vals = list(scores.values())
    composite  = float(np.mean(score_vals)) if score_vals else 50.0

    if composite <= 25:   label, emoji, color = "Extreme Fear",  "😱", "#f85149"
    elif composite <= 45: label, emoji, color = "Fear",          "😟", "#ff7b72"
    elif composite <= 55: label, emoji, color = "Neutral",       "😐", "#e3b341"
    elif composite <= 75: label, emoji, color = "Greed",         "😊", "#7ee787"
    else:                 label, emoji, color = "Extreme Greed", "🤑", "#3fb950"

    return {
        "score":   round(composite, 1),
        "label":   label,
        "emoji":   emoji,
        "color":   color,
        "details": details,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  FRAGILITY HISTORICAL TREND ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def compute_fragility_trend(prices: pd.DataFrame) -> dict:
    """
    Compute system-level fragility score timeseries (last 504 days = 2 years).
    Returns daily scores + regime for chart rendering.
    """
    rets    = prices.pct_change().replace([np.inf,-np.inf], np.nan)
    wdd     = min(252, len(prices))
    peak    = prices.rolling(wdd, min_periods=20).max()
    dd      = (prices / peak - 1.0).abs()
    vol20   = rets.rolling(20, min_periods=10).std() * np.sqrt(252)

    def _cvar(x):
        q = np.nanquantile(x, 0.05); tail = x[x <= q]
        return abs(np.nanmean(tail)) if len(tail) > 0 else np.nan
    cvar60  = rets.rolling(60, min_periods=20).apply(_cvar, raw=False)
    ma200   = prices.rolling(200, min_periods=50).mean()
    dist200 = (-(prices / ma200 - 1.0)).clip(lower=0)
    wcol    = "ACWI" if "ACWI" in rets.columns else rets.columns[0]
    corr_w  = pd.DataFrame(index=rets.index, columns=prices.columns, dtype=float)
    for c in prices.columns:
        corr_w[c] = rets[c].rolling(60, min_periods=20).corr(rets[wcol]).clip(lower=0)
    vov     = vol20.rolling(20, min_periods=10).std()
    mu_vov  = vov.rolling(60, min_periods=20).mean()
    sd_vov  = vov.rolling(60, min_periods=20).std().replace(0, np.nan)
    volz    = ((vov - mu_vov) / sd_vov).abs()

    w   = FRAGILITY_WEIGHTS
    zw  = min(756, len(prices) - 1)

    # Compute per-asset latent scores then take cross-sectional median daily
    latents = pd.DataFrame(index=prices.index)
    for col in prices.columns:
        zd = _robust_zscore(dd[col],      zw)
        zv = _robust_zscore(vol20[col],   zw)
        zc = _robust_zscore(cvar60[col],  zw)
        zt = _robust_zscore(dist200[col], zw)
        zr = _robust_zscore(corr_w[col],  zw)
        zz = _robust_zscore(volz[col],    zw)
        lat = (w["dd"]*zd.fillna(0) + w["vol"]*zv.fillna(0) + w["cvar"]*zc.fillna(0) +
               w["trend"]*zt.fillna(0) + w["corr"]*zr.fillna(0) + w["volz"]*zz.fillna(0))
        latents[col] = lat

    sys_lat  = latents.median(axis=1).ewm(span=10, adjust=False).mean()
    sys_score= 100.0 / (1.0 + np.exp(-sys_lat * 0.5))

    # Last 504 days
    trend_raw = sys_score.tail(504).dropna()
    trend = []
    for date, val in trend_raw.items():
        reg = "Crisis" if val >= 70 else "Stressed" if val >= 50 else "Calm"
        trend.append({
            "date":  date.strftime("%Y-%m-%d"),
            "score": round(float(val), 1),
            "regime": reg,
            "color": "#f85149" if reg=="Crisis" else "#e3b341" if reg=="Stressed" else "#3fb950",
        })

    return {
        "trend":       trend,
        "current":     round(float(sys_score.iloc[-1]), 1) if not sys_score.empty else 50,
        "peak_2y":     round(float(sys_score.tail(504).max()), 1),
        "trough_2y":   round(float(sys_score.tail(504).min()), 1),
        "avg_2y":      round(float(sys_score.tail(504).mean()), 1),
    }


def build_web_html(df: pd.DataFrame, frag_df: pd.DataFrame = None, prices: pd.DataFrame = None, regime_data: dict = None, fg_data: dict = None, frag_trend: dict = None) -> str:
    import math
    now         = datetime.now(SGT)
    date_str    = now.strftime("%A, %d %b %Y %H:%M SGT")
    gen_ts      = now.strftime("%Y-%m-%dT%H:%M:%S")
    market_open = bool(df["market_open"].iloc[0]) if "market_open" in df.columns else True
    GA          = "G-XXXXXXXXXX"

    # ── shared cell helpers ───────────────────────────────────────────────────
    def _rc(v, fmt="ret"):
        if pd.isna(v): return '<td class="num gr">-</td>'
        if fmt == "ret":
            p=v*100; s="+" if p>0 else ""
            cl="ps" if p>=2 else "pl" if p>=0 else "ng" if p>=-2 else "nr"
            return f'<td class="num {cl}">{s}{p:.2f}%</td>'
        if fmt == "vol":
            p=v*100
            return f'<td class="num {"nr" if p>30 else "am" if p>18 else "gr"}">{p:.1f}%</td>'
        if fmt == "dd":
            p=v*100
            return f'<td class="num {"nr" if p<-15 else "am" if p<-7 else "ps"}">{p:.1f}%</td>'
        if fmt == "sh":
            return f'<td class="num {"ps" if v>1 else "am" if v>0 else "nr"}">{v:.2f}</td>'
        return f'<td class="num gr">{v}</td>'

    def _sig(rl, rc_col):
        rl=rl.strip()
        dot={"RED":"#f85149","AMBER":"#e3b341","GREEN":"#3fb950"}.get(rl,"#8b949e")
        cl={"RED":"sr","AMBER":"sa","GREEN":"sg"}.get(rl,"")
        return f'<td class="sig {cl}"><span style="color:{dot};">&#9679;</span> {rl}</td>'

    def _srow(sec, cs=14):
        return f'<tr class="sh"><td colspan="{cs}">{SECTION_LABELS.get(sec,sec)}</td></tr>'

    def _bar(nm, val, mx, color):
        w=min(100, abs(val)/mx*100) if mx>0 else 0
        s="+" if val>=0 else ""
        return (f'<div style="display:flex;align-items:center;gap:10px;padding:7px 0;border-bottom:1px solid #21262d;">'
                f'<div style="width:150px;font-size:11px;color:#e6edf3;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{nm}</div>'
                f'<div style="flex:1;background:#21262d;border-radius:3px;height:8px;">'
                f'<div style="width:{w:.0f}%;background:{color};height:8px;border-radius:3px;"></div></div>'
                f'<div style="width:62px;text-align:right;font-family:monospace;font-size:12px;font-weight:700;color:{color};">{s}{val*100:.2f}%</div>'
                f'</div>')

    # ══ TAB 1: PERFORMANCE ════════════════════════════════════════════════════
    nr=int((df["rag_label"].str.strip()=="RED").sum())
    na=int((df["rag_label"].str.strip()=="AMBER").sum())
    ng=int((df["rag_label"].str.strip()=="GREEN").sum())
    tot=len(df)
    if ng>=nr*2:   tone,tc,tb="RISK-ON","#3fb950","#0d2318"
    elif nr>=ng*2: tone,tc,tb="RISK-OFF","#f85149","#2d0f0e"
    else:          tone,tc,tb="MIXED","#e3b341","#2d2106"

    mtd=df[["name","ret_1m"]].dropna(subset=["ret_1m"])
    gain=mtd.nlargest(5,"ret_1m"); loss=mtd.nsmallest(5,"ret_1m")
    gm=gain["ret_1m"].abs().max(); lm=loss["ret_1m"].abs().max()
    gh="".join(_bar(r["name"],r["ret_1m"],gm,"#3fb950") for _,r in gain.iterrows())
    lh="".join(_bar(r["name"],r["ret_1m"],lm,"#f85149") for _,r in loss.iterrows())

    d1th="<th>1D</th>" if market_open else ""
    def _sparkline(spark_data, width=80, height=24):
        """Generate inline SVG sparkline from 20-day normalised data."""
        if not spark_data or len(spark_data) < 2:
            return '<td style="padding:7px 8px;"></td>'
        mn, mx = min(spark_data), max(spark_data)
        rng = mx - mn if mx != mn else 1.0
        pts = []
        for i, v in enumerate(spark_data):
            x = i / (len(spark_data)-1) * width
            y = height - (v - mn) / rng * (height - 4) - 2
            pts.append(f"{x:.1f},{y:.1f}")
        color = "#3fb950" if spark_data[-1] >= spark_data[0] else "#f85149"
        polyline = " ".join(pts)
        last_x, last_y = pts[-1].split(",")
        svg = (f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
               f'style="display:block;">'
               f'<polyline points="{polyline}" fill="none" stroke="{color}" stroke-width="1.5" stroke-linejoin="round"/>'
               f'<circle cx="{last_x}" cy="{last_y}" r="2" fill="{color}"/>'
               f'</svg>')
        return f'<td style="padding:4px 8px;">{svg}</td>'

    pr=""; pv=None
    for _,row in df.iterrows():
        if row["section"]!=pv: pv=row["section"]; pr+=_srow(row["section"])
        d1=_rc(row["ret_1d"]) if market_open else ""
        ccy = row.get("currency","USD")
        spark_td = _sparkline(row.get("spark",[]))
        pr+=(f'<tr><td class="an">{row["name"]}</td><td class="tk">{row["ticker"]}</td>'
             f'<td class="num gr" style="font-size:9px;">{ccy}</td>'
             f'{spark_td}'
             f'{d1}{_rc(row["ret_1w"])}{_rc(row["ret_1m"])}{_rc(row["ret_3m"])}{_rc(row["ret_ytd"])}'
             f'{_sig(row["rag_label"],row["rag_color"])}</tr>')

    perf=(f'<div class="tbar"><div><div class="lbl">MARKET TONE</div>'
          f'<div class="pill" style="background:{tb};color:{tc};border:1px solid {tc};">{tone}</div></div>'
          f'<div class="dvdr"></div><div class="rb">'
          f'<div class="ri"><div class="rn" style="color:#f85149;">{nr}</div><div class="rl">RED</div></div>'
          f'<div class="ri"><div class="rn" style="color:#e3b341;">{na}</div><div class="rl">AMBER</div></div>'
          f'<div class="ri"><div class="rn" style="color:#3fb950;">{ng}</div><div class="rl">GREEN</div></div>'
          f'<div class="ri"><div class="rn" style="color:#e6edf3;">{tot}</div><div class="rl">TOTAL</div></div>'
          f'</div></div>'
          f'<div class="gl"><div class="gc"><div class="gt"><span class="gd" style="background:#3fb950;"></span>'
          f'Top 5 MTD Gainers &nbsp;<span style="color:#8b949e;font-weight:400;">(1-Month)</span></div>{gh}</div>'
          f'<div class="gc"><div class="gt"><span class="gd" style="background:#f85149;"></span>'
          f'Top 5 MTD Losers &nbsp;<span style="color:#8b949e;font-weight:400;">(1-Month)</span></div>{lh}</div></div>'
          f'<div class="tw"><table><thead><tr><th style="text-align:left;">Asset</th><th>Ticker</th><th>CCY</th><th style="min-width:80px;">Trend 20D</th>'
          f'{d1th}<th>1W</th><th>1M</th><th>3M</th><th>YTD</th><th>Signal</th>'
          f'</tr></thead><tbody>{pr}</tbody></table></div>')

    # ══ TAB 2: RISK ═══════════════════════════════════════════════════════════
    def _varrow(now_v, ago_v):
        if pd.isna(now_v) or pd.isna(ago_v) or ago_v==0: return "gr","&#8594;","-"
        chg = (now_v - ago_v) / ago_v
        abs_chg = now_v - ago_v  # absolute change in vol (pp)
        # For low-vol instruments (< 3% annualised), show absolute pp change
        if ago_v < 0.03:
            pp = abs_chg * 100
            pct = f"{pp:+.2f}pp"
        else:
            pct = f"{chg*100:+.1f}%"
        if chg>=0.20:  return "nr","&#11014;&#11014;",pct
        if chg>=0.05:  return "am","&#11014;",pct
        if chg>=-0.05: return "gr","&#8594;",pct
        return "ps","&#11015;",pct

    rising=stable=falling=0
    for _,row in df.iterrows():
        nv=row.get("vol_now",float("nan")); av=row.get("vol_1m_ago",float("nan"))
        if pd.isna(nv) or pd.isna(av) or av==0: continue
        chg=(nv-av)/av
        if chg>=0.05: rising+=1
        elif chg<=-0.05: falling+=1
        else: stable+=1

    vsumm=(f'<div style="display:flex;gap:14px;margin-bottom:14px;flex-wrap:wrap;">'
           f'<div class="vc" style="border-color:#f85149;"><div class="vn" style="color:#f85149;">{rising}</div>'
           f'<div class="vl">VOL RISING &#11014;</div><div class="vs">Change &gt; +5%</div></div>'
           f'<div class="vc" style="border-color:#8b949e;"><div class="vn" style="color:#8b949e;">{stable}</div>'
           f'<div class="vl">VOL STABLE &#8594;</div><div class="vs">&#8722;5% to +5%</div></div>'
           f'<div class="vc" style="border-color:#3fb950;"><div class="vn" style="color:#3fb950;">{falling}</div>'
           f'<div class="vl">VOL EASING &#11015;</div><div class="vs">Change &lt; &#8722;5%</div></div>'
           f'</div>')

    rr=""; rv=None
    for _,row in df.iterrows():
        if row["section"]!=rv: rv=row["section"]; rr+=_srow(row["section"],cs=8)
        cl,arrow,pct=_varrow(row.get("vol_now",float("nan")),row.get("vol_1m_ago",float("nan")))
        vn=f'{row["vol_now"]*100:.1f}%' if not pd.isna(row.get("vol_now",float("nan"))) else "-"
        va=f'{row["vol_1m_ago"]*100:.1f}%' if not pd.isna(row.get("vol_1m_ago",float("nan"))) else "-"
        rr+=(f'<tr><td class="an">{row["name"]}</td><td class="tk">{row["ticker"]}</td>'
             f'<td class="num gr">{vn}</td><td class="num gr">{va}</td>'
             f'<td class="num {cl}" style="font-family:monospace;">{arrow}&nbsp;{pct}</td>'
             f'{_rc(row["max_dd"],"dd")}{_rc(row["sharpe"],"sh")}'
             f'{_sig(row["rag_label"],row["rag_color"])}</tr>')

    risk=(vsumm+
          f'<div class="tw"><table><thead><tr><th style="text-align:left;">Asset</th><th>Ticker</th>'
          f'<th>Vol 20D</th><th>Vol 1M Ago</th><th>30D Change</th>'
          f'<th>Max DD</th><th>Sharpe</th><th>Signal</th></tr></thead><tbody>{rr}</tbody></table></div>'
          f'<div style="margin-top:10px;font-size:9px;color:#8b949e;font-family:monospace;line-height:1.8;">'
          f'&#11014;&#11014; Vol rising &ge;+20% &nbsp;&#183;&nbsp; &#11014; +5% to +20% &nbsp;&#183;&nbsp; '
          f'&#8594; stable &#8722;5% to +5% &nbsp;&#183;&nbsp; &#11015; easing &lt;&#8722;5% &nbsp;&#183;&nbsp; '
          f'Sharpe = 1Y excess return / vol (rf=4.5%)</div>')


    # ── Build Fragility Trend SVG chart ──────────────────────────────────────
    if frag_trend and frag_trend.get("trend"):
        trend_pts  = frag_trend["trend"]
        ft_current = frag_trend.get("current", 50)
        ft_peak    = frag_trend.get("peak_2y", 50)
        ft_trough  = frag_trend.get("trough_2y", 50)
        ft_avg     = frag_trend.get("avg_2y", 50)
        ft_color   = "#f85149" if ft_current>=70 else "#e3b341" if ft_current>=50 else "#3fb950"

        # Build SVG
        svg_w = 900; svg_h = 180; pad_l = 40; pad_r = 10; pad_t = 10; pad_b = 30
        chart_w = svg_w - pad_l - pad_r
        chart_h = svg_h - pad_t - pad_b
        n_pts   = len(trend_pts)

        def _x(i):   return pad_l + i / max(n_pts-1,1) * chart_w
        def _y(val): return pad_t + (1 - val/100) * chart_h

        # Background regime bands
        bands = []
        band_start = 0; band_reg = trend_pts[0]["regime"] if trend_pts else "Calm"
        for i, pt in enumerate(trend_pts):
            if pt["regime"] != band_reg or i == n_pts-1:
                x1 = _x(band_start); x2 = _x(i)
                bc = {"Crisis":"rgba(248,81,73,0.12)","Stressed":"rgba(227,179,65,0.10)","Calm":"rgba(63,185,80,0.06)"}.get(band_reg,"rgba(0,0,0,0)")
                bands.append(f'<rect x="{x1:.1f}" y="{pad_t}" width="{x2-x1:.1f}" height="{chart_h}" fill="{bc}"/>')
                band_start = i; band_reg = pt["regime"]

        # Threshold lines
        y50 = _y(50); y70 = _y(70)
        thresholds = (
            f'<line x1="{pad_l}" y1="{y70:.1f}" x2="{svg_w-pad_r}" y2="{y70:.1f}" stroke="#f85149" stroke-width="0.8" stroke-dasharray="4,3" opacity="0.6"/>'
            f'<text x="{pad_l-4}" y="{y70+3:.1f}" text-anchor="end" font-size="8" fill="#f85149" opacity="0.8">70</text>'
            f'<line x1="{pad_l}" y1="{y50:.1f}" x2="{svg_w-pad_r}" y2="{y50:.1f}" stroke="#e3b341" stroke-width="0.8" stroke-dasharray="4,3" opacity="0.6"/>'
            f'<text x="{pad_l-4}" y="{y50+3:.1f}" text-anchor="end" font-size="8" fill="#e3b341" opacity="0.8">50</text>'
        )

        # Y axis labels
        y_labels = "".join(
            f'<text x="{pad_l-4}" y="{_y(v)+3:.1f}" text-anchor="end" font-size="8" fill="#8b949e">{v}</text>'
            for v in [0, 25, 75, 100]
        )

        # X axis month labels every ~21 pts
        x_labels = ""
        prev_month = ""
        for i, pt in enumerate(trend_pts):
            m = pt["date"][:7]
            if m != prev_month and i % 42 == 0:
                prev_month = m
                x_labels += f'<text x="{_x(i):.1f}" y="{svg_h-4}" text-anchor="middle" font-size="8" fill="#8b949e">{m}</text>'

        # Line path + fill
        pts_str = " ".join(f"{_x(i):.1f},{_y(pt['score']):.1f}" for i, pt in enumerate(trend_pts))
        fill_pts = f"{_x(0):.1f},{pad_t+chart_h} " + pts_str + f" {_x(n_pts-1):.1f},{pad_t+chart_h}"
        line_color = ft_color

        # Last point dot
        last_x = _x(n_pts-1); last_y = _y(ft_current)
        dot = f'<circle cx="{last_x:.1f}" cy="{last_y:.1f}" r="4" fill="{ft_color}" stroke="#0d1117" stroke-width="2"/>'

        svg_parts = (
            f'<svg viewBox="0 0 {svg_w} {svg_h}" width="100%" style="max-width:{svg_w}px;display:block;overflow:visible;">'
            + "".join(bands)
            + thresholds + y_labels + x_labels
            + f'<polyline points="{fill_pts}" fill="{ft_color}" opacity="0.08" stroke="none"/>'
            + f'<polyline points="{pts_str}" fill="none" stroke="{ft_color}" stroke-width="1.8" stroke-linejoin="round" stroke-linecap="round"/>'
            + dot
            + '</svg>'
        )

        frag_trend_html = (
            f'<div class="fc" style="margin-bottom:14px;">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:8px;">'
            f'<div class="lbl">SYSTEM FRAGILITY TREND — LAST 2 YEARS</div>'
            f'<div style="display:flex;gap:16px;">'
            f'<div style="text-align:center;"><div style="font-size:9px;color:#8b949e;">CURRENT</div>'
            f'<div style="font-size:18px;font-weight:700;color:{ft_color};font-family:monospace;">{ft_current:.0f}</div></div>'
            f'<div style="text-align:center;"><div style="font-size:9px;color:#8b949e;">2Y PEAK</div>'
            f'<div style="font-size:18px;font-weight:700;color:#f85149;font-family:monospace;">{ft_peak:.0f}</div></div>'
            f'<div style="text-align:center;"><div style="font-size:9px;color:#8b949e;">2Y AVG</div>'
            f'<div style="font-size:18px;font-weight:700;color:#8b949e;font-family:monospace;">{ft_avg:.0f}</div></div>'
            f'<div style="text-align:center;"><div style="font-size:9px;color:#8b949e;">2Y LOW</div>'
            f'<div style="font-size:18px;font-weight:700;color:#3fb950;font-family:monospace;">{ft_trough:.0f}</div></div>'
            f'</div></div>'
            f'{svg_parts}'
            f'<div style="display:flex;gap:16px;margin-top:6px;font-size:9px;color:#8b949e;">'
            f'<span><span style="color:#f85149;">&#9632;</span> CRISIS &#8805;70</span>'
            f'<span><span style="color:#e3b341;">&#9632;</span> STRESSED 50&#8211;70</span>'
            f'<span><span style="color:#3fb950;">&#9632;</span> CALM &lt;50</span>'
            f'<span style="margin-left:8px;">Dashed lines = regime thresholds</span>'
            f'</div></div>'
        )
    else:
        frag_trend_html = ""


    # ══ TAB 3: FRAGILITY ══════════════════════════════════════════════════════
    if frag_df is not None and not frag_df.empty:
        ss=frag_df.attrs.get("system_score",float(frag_df["fragility"].median()))
        reg=frag_df.attrs.get("regime","CALM")
        rc_={"CRISIS":"#f85149","STRESSED":"#e3b341","CALM":"#3fb950"}.get(reg,"#8b949e")
        rb_={"CRISIS":"#2d0f0e","STRESSED":"#2d2106","CALM":"#0d2318"}.get(reg,"#161b22")
        ncr=int((frag_df["rag"]=="CRISIS").sum())
        nst=int((frag_df["rag"]=="STRESSED").sum())
        nca=int((frag_df["rag"]=="CALM").sum())

        def _arc(deg,r=78,cx=100,cy=88):
            rad=math.radians(180-deg)
            return cx+r*math.cos(rad), cy-r*math.sin(rad)
        ga=min(179,int(ss/100*180)); gc="#f85149" if ss>=70 else "#e3b341" if ss>=50 else "#3fb950"
        ax,ay=_arc(ga); lg=1 if ga>90 else 0
        gauge=(f'<svg viewBox="0 0 200 108" width="190" height="108">'
               f'<path d="M 22 88 A 78 78 0 0 1 178 88" fill="none" stroke="#21262d" stroke-width="13" stroke-linecap="round"/>'
               f'<path d="M 22 88 A 78 78 0 {lg} 1 {ax:.1f} {ay:.1f}" fill="none" stroke="{gc}" stroke-width="13" stroke-linecap="round"/>'
               f'<text x="100" y="76" text-anchor="middle" font-size="24" font-weight="bold" fill="{gc}" font-family="monospace">{ss:.0f}</text>'
               f'<text x="100" y="92" text-anchor="middle" font-size="8" fill="#8b949e" font-family="monospace">/ 100</text>'
               f'<text x="24" y="106" text-anchor="middle" font-size="8" fill="#555">0</text>'
               f'<text x="176" y="106" text-anchor="middle" font-size="8" fill="#555">100</text></svg>')

        t5h=""
        for _,r in frag_df.head(5).iterrows():
            fc="#f85149" if r["rag"]=="CRISIS" else "#e3b341" if r["rag"]=="STRESSED" else "#3fb950"
            bw=min(100,r["fragility"])
            t5h+=(f'<div style="display:flex;align-items:center;gap:10px;padding:7px 0;border-bottom:1px solid #21262d;">'
                  f'<div style="width:150px;font-size:11px;color:#e6edf3;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{r["name"]}</div>'
                  f'<div style="flex:1;background:#21262d;border-radius:3px;height:8px;">'
                  f'<div style="width:{bw:.0f}%;background:{fc};height:8px;border-radius:3px;"></div></div>'
                  f'<div style="width:48px;text-align:right;font-family:monospace;font-size:12px;font-weight:700;color:{fc};">{r["fragility"]:.0f}</div>'
                  f'<div style="width:70px;text-align:center;font-size:9px;font-family:monospace;color:{fc};'
                  f'background:{rb_};border:1px solid {fc};border-radius:10px;padding:1px 6px;">{r["rag"]}</div></div>')

        PL={"pillar_dd":"Drawdown","pillar_vol":"Volatility","pillar_cvar":"Tail Risk",
            "pillar_trend":"Trend","pillar_corr":"Contagion","pillar_volz":"Vol Stress"}
        fr=""
        for _,r in frag_df.iterrows():
            fc="#f85149" if r["rag"]=="CRISIS" else "#e3b341" if r["rag"]=="STRESSED" else "#3fb950"
            bw=min(100,r["fragility"])
            pv={k:r.get(k,0) for k in PL}; top=PL[max(pv,key=pv.get)]
            pc="".join(f'<td class="num {"ps" if r.get(k,0)>1 else "am" if r.get(k,0)>0 else "gr"}">{r.get(k,0):+.1f}</td>' for k in PL)
            fr+=(f'<tr><td class="an">{r["name"]}</td><td class="tk">{r["ticker"]}</td>'
                 f'<td class="num" style="color:{fc};font-weight:700;">{r["fragility"]:.0f}</td>'
                 f'<td style="padding:7px 8px;"><div style="background:#21262d;border-radius:3px;height:6px;width:80px;">'
                 f'<div style="width:{bw:.0f}%;background:{fc};height:6px;border-radius:3px;"></div></div></td>'
                 f'<td style="text-align:center;"><span style="font-size:9px;font-family:monospace;color:{fc};'
                 f'background:{rb_};border:1px solid {fc};border-radius:10px;padding:1px 8px;">{r["rag"]}</span></td>'
                 f'<td class="num gr" style="font-size:10px;">{top}</td>{pc}</tr>')

        frag= frag_trend_html + (f'<div style="display:grid;grid-template-columns:auto 1fr 1fr 1fr 1fr;gap:14px;margin-bottom:14px;align-items:stretch;">'
              f'<div class="fc" style="text-align:center;"><div class="lbl" style="margin-bottom:8px;">SYSTEM FRAGILITY</div>'
              f'{gauge}<div class="pill" style="background:{rb_};color:{rc_};border:1px solid {rc_};margin-top:6px;">{reg}</div></div>'
              f'<div class="fc" style="text-align:center;"><div class="lbl">CRISIS</div>'
              f'<div style="font-size:28px;font-weight:700;color:#f85149;font-family:monospace;">{ncr}</div>'
              f'<div style="font-size:9px;color:#8b949e;margin-top:4px;">Score &#8805; 70</div></div>'
              f'<div class="fc" style="text-align:center;"><div class="lbl">STRESSED</div>'
              f'<div style="font-size:28px;font-weight:700;color:#e3b341;font-family:monospace;">{nst}</div>'
              f'<div style="font-size:9px;color:#8b949e;margin-top:4px;">Score 50&#8211;70</div></div>'
              f'<div class="fc" style="text-align:center;"><div class="lbl">CALM</div>'
              f'<div style="font-size:28px;font-weight:700;color:#3fb950;font-family:monospace;">{nca}</div>'
              f'<div style="font-size:9px;color:#8b949e;margin-top:4px;">Score &lt; 50</div></div>'
              f'<div class="fc" style="text-align:center;"><div class="lbl">TOTAL</div>'
              f'<div style="font-size:28px;font-weight:700;color:#e6edf3;font-family:monospace;">{len(frag_df)}</div>'
              f'<div style="font-size:9px;color:#8b949e;margin-top:4px;">Instruments</div></div></div>'
              f'<div class="fc" style="margin-bottom:14px;">'
              f'<div style="font-size:9px;font-weight:700;letter-spacing:2px;color:#8b949e;margin-bottom:12px;text-transform:uppercase;">&#9888; Top 5 Most Fragile</div>'
              f'{t5h}</div>'
              f'<div class="tw"><table><thead><tr><th style="text-align:left;">Asset</th><th>Ticker</th>'
              f'<th>Score</th><th>Bar</th><th>Status</th><th>Top Driver</th>'
              f'<th>Drawdown</th><th>Volatility</th><th>Tail Risk</th><th>Trend</th><th>Contagion</th><th>Vol Stress</th>'
              f'</tr></thead><tbody>{fr}</tbody></table></div>'
              f'<div style="margin-top:10px;font-size:9px;color:#8b949e;font-family:monospace;line-height:1.8;">'
              f'BK Fragility Framework &#183; Drawdown 22% + CVaR 20% + Contagion 18% + Volatility 15% + Trend 15% + Vol Stress 10% &#183; '
              f'CRISIS &#8805;70 &#183; STRESSED 50&#8211;70 &#183; CALM &lt;50</div>')
    else:
        frag='<div style="padding:40px;text-align:center;color:#8b949e;">Fragility data unavailable.</div>'



    def _build_rsr(df_in):
        """Build Relative Strength Rankings — sorted within each asset class."""
        sections_order = SECTION_ORDER
        html = (
            '<div style="margin-top:14px;">'
            '<div style="font-size:9px;font-weight:700;letter-spacing:2px;color:#8b949e;'
            'text-transform:uppercase;margin-bottom:12px;">RELATIVE STRENGTH RANKINGS — BY ASSET CLASS</div>'
            '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px;">'
        )
        for sec in sections_order:
            sec_df = df_in[df_in["section"]==sec].copy()
            if sec_df.empty: continue
            sec_df = sec_df.dropna(subset=["ret_1m"]).sort_values("ret_1m", ascending=False)
            if sec_df.empty: continue
            sec_label = SECTION_LABELS.get(sec, sec)
            best_ret  = sec_df["ret_1m"].max()
            worst_ret = sec_df["ret_1m"].min()
            rng       = max(abs(best_ret), abs(worst_ret), 0.001)
            rows_html = ""
            for rank, (_, r) in enumerate(sec_df.iterrows(), 1):
                v     = r["ret_1m"]; s = "+" if v>=0 else ""
                color = "#3fb950" if v>=0 else "#f85149"
                bar_w = min(100, abs(v)/rng*100)
                bar_dir = "right" if v>=0 else "left"
                medal = {1:"🥇",2:"🥈",3:"🥉"}.get(rank,"")
                rows_html += (
                    f'<div style="display:flex;align-items:center;gap:8px;padding:5px 0;border-bottom:1px solid #21262d;">'
                    f'<div style="width:16px;font-size:10px;text-align:center;">{medal if rank<=3 else str(rank)}</div>'
                    f'<div style="flex:1;font-size:10px;color:#e6edf3;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{r["name"]}</div>'
                    f'<div style="width:50px;text-align:right;font-family:monospace;font-size:10px;font-weight:700;color:{color};">{s}{v*100:.1f}%</div>'
                    f'</div>'
                )
            html += (
                f'<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px 14px;">'
                f'<div style="font-size:9px;font-weight:700;letter-spacing:1px;color:#58a6ff;'
                f'text-transform:uppercase;margin-bottom:8px;">{sec_label}</div>'
                f'{rows_html}'
                f'<div style="display:flex;justify-content:space-between;margin-top:6px;font-size:9px;color:#8b949e;">'
                f'<span>Best: <span style="color:#3fb950;">{best_ret*100:+.1f}%</span></span>'
                f'<span>Worst: <span style="color:#f85149;">{worst_ret*100:+.1f}%</span></span>'
                f'</div></div>'
            )
        html += '</div></div>'
        return html


    # ══ TAB 4: ANALYSIS — Correlation Heatmap ════════════════════════════════

    # Key 20 representative instruments
    HEATMAP_TICKERS = {
        "S&P 500":      "SPY",
        "Nasdaq 100":   "QQQ",
        "Europe Dev":   "EFA",
        "Euro STOXX 50":"FEZ",
        "EM Broad":     "EEM",
        "China":        "FXI",
        "India":        "INDA",
        "Japan":        "DBJP",
        "Energy":       "XLE",
        "Financials":   "XLF",
        "Defence":      "ITA",
        "Treasuries 10Y":"IEF",
        "Treasuries 20Y":"TLT",
        "HY Credit":    "HYG",
        "Gold":         "GLD",
        "WTI Oil":      "USO",
        "Bitcoin":      "BTC-USD",
        "USD Index":    "UUP",
        "EUR/USD":      "EURUSD=X",
        "VIX Futures":  "VIXY",
    }

    # Build correlation matrix from prices
    def _build_heatmap(prices_df):
        # Filter to available heatmap tickers
        available = {name: tk for name, tk in HEATMAP_TICKERS.items() if tk in prices_df.columns}
        if len(available) < 4:
            return None, None
        tickers  = list(available.values())
        names    = list(available.keys())
        rets     = prices_df[tickers].pct_change().dropna()
        # 60-day rolling correlation — use last 60 days
        corr_df  = rets.tail(60).corr()
        corr_df.columns = names
        corr_df.index   = names
        return corr_df, names

    def _corr_color(v):
        """Map correlation -1..+1 to a red-white-blue color."""
        if v >= 0:
            # white to deep red
            intensity = int(v * 200)
            r = min(255, 200 + intensity // 4)
            g = max(0,   200 - intensity)
            b = max(0,   200 - intensity)
        else:
            # white to deep blue
            intensity = int(-v * 200)
            r = max(0,   200 - intensity)
            g = max(0,   200 - intensity)
            b = min(255, 200 + intensity // 4)
        return f"rgb({r},{g},{b})"

    def _text_color(v):
        return "#111" if abs(v) < 0.5 else "#fff"

    corr_df, hm_names = _build_heatmap(prices)

    if corr_df is not None:
        n = len(hm_names)
        cell_size = 40  # px per cell
        label_w   = 120
        total_w   = label_w + n * cell_size
        total_h   = label_w + n * cell_size
        assert total_w == label_w + n * cell_size  # guard

        # Build SVG heatmap
        svg_parts = [
            f'<svg viewBox="0 0 {total_w} {total_h}" width="100%" '
            f'style="max-width:{total_w}px;font-family:monospace;">'
        ]

        # Column labels (rotated)
        for j, name in enumerate(hm_names):
            x = label_w + j * cell_size + cell_size // 2
            svg_parts.append(
                f'<text x="{x}" y="{label_w - 4}" text-anchor="end" '
                f'transform="rotate(-45,{x},{label_w-4})" '
                f'font-size="9" fill="#8b949e">{name[:12]}</text>'
            )

        # Row labels + cells
        for i, row_name in enumerate(hm_names):
            y_center = label_w + i * cell_size + cell_size // 2

            # Row label
            svg_parts.append(
                f'<text x="{label_w - 6}" y="{y_center + 3}" '
                f'text-anchor="end" font-size="9" fill="#8b949e">{row_name[:14]}</text>'
            )

            for j, col_name in enumerate(hm_names):
                v    = corr_df.loc[row_name, col_name]
                x    = label_w + j * cell_size
                y    = label_w + i * cell_size
                bg   = _corr_color(v)
                tc   = _text_color(v)
                diag = ' opacity="0.6"' if i == j else ''
                svg_parts.append(
                    f'<rect x="{x}" y="{y}" width="{cell_size}" height="{cell_size}" '
                    f'fill="{bg}"{diag} rx="1"/>'
                )
                svg_parts.append(
                    f'<text x="{x + cell_size//2}" y="{y + cell_size//2 + 3}" '
                    f'text-anchor="middle" font-size="8" fill="{tc}">{v:.2f}</text>'
                )

        svg_parts.append('</svg>')
        heatmap_svg = "".join(svg_parts)

        # Colour legend
        legend_svg = (
            '<svg width="260" height="20" style="margin-top:8px;">'
            '<defs><linearGradient id="lg" x1="0" x2="1" y1="0" y2="0">'
            '<stop offset="0%" stop-color="rgb(0,0,255)"/>'
            '<stop offset="50%" stop-color="rgb(200,200,200)"/>'
            '<stop offset="100%" stop-color="rgb(255,0,0)"/>'
            '</linearGradient></defs>'
            '<rect x="30" y="2" width="200" height="12" fill="url(#lg)" rx="2"/>'
            '<text x="28" y="18" text-anchor="end" font-size="9" fill="#8b949e">-1.0</text>'
            '<text x="130" y="18" text-anchor="middle" font-size="9" fill="#8b949e">0</text>'
            '<text x="232" y="18" text-anchor="start" font-size="9" fill="#8b949e">+1.0</text>'
            '</svg>'
        )

        # Top correlations (most correlated pairs, excluding diagonal)
        pairs = []
        for i in range(n):
            for j in range(i+1, n):
                v = corr_df.iloc[i, j]
                pairs.append((v, hm_names[i], hm_names[j]))
        pairs.sort(key=lambda x: abs(x[0]), reverse=True)

        top_pairs_html = ""
        for v, a, b in pairs[:8]:
            color = "#f85149" if v > 0 else "#58a6ff"
            sign  = "+" if v > 0 else ""
            top_pairs_html += (
                f'<div style="display:flex;justify-content:space-between;align-items:center;'
                f'padding:5px 0;border-bottom:1px solid #21262d;">'
                f'<div style="font-size:11px;color:#e6edf3;">{a} <span style="color:#8b949e;">vs</span> {b}</div>'
                f'<div style="font-family:monospace;font-size:12px;font-weight:700;color:{color};">{sign}{v:.2f}</div>'
                f'</div>'
            )

        analysis_tab = (
            # Summary stats
            f'<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:14px;">'
            f'<div class="fc"><div class="lbl">CORRELATION WINDOW</div>'
            f'<div style="font-size:22px;font-weight:700;color:#e6edf3;font-family:monospace;">60D</div>'
            f'<div style="font-size:9px;color:#8b949e;margin-top:4px;">Rolling daily returns</div></div>'
            f'<div class="fc"><div class="lbl">INSTRUMENTS</div>'
            f'<div style="font-size:22px;font-weight:700;color:#e6edf3;font-family:monospace;">{n}</div>'
            f'<div style="font-size:9px;color:#8b949e;margin-top:4px;">Key representatives</div></div>'
            f'<div class="fc"><div class="lbl">AVG CORRELATION</div>'
            f'<div style="font-size:22px;font-weight:700;'
            f'color:{"#f85149" if corr_df.values[corr_df.values < 1].mean() > 0.5 else "#e3b341" if corr_df.values[corr_df.values < 1].mean() > 0.3 else "#3fb950"};font-family:monospace;">'
            f'{corr_df.values[corr_df.values < 1].mean():.2f}</div>'
            f'<div style="font-size:9px;color:#8b949e;margin-top:4px;">Ex-diagonal (high = contagion risk)</div></div>'
            f'</div>'
            # Heatmap
            f'<div class="fc" style="margin-bottom:14px;overflow-x:auto;">'
            f'<div class="lbl" style="margin-bottom:12px;">CROSS-ASSET CORRELATION MATRIX — 60D</div>'
            f'{heatmap_svg}'
            f'<div style="margin-top:6px;">{legend_svg}</div>'
            f'</div>'
            # Top correlated pairs
            f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;">'
            f'<div class="fc"><div class="lbl" style="margin-bottom:10px;">STRONGEST CORRELATIONS</div>'
            f'{top_pairs_html}</div>'
            f'<div class="fc"><div class="lbl" style="margin-bottom:10px;">HOW TO READ</div>'
            f'<div style="font-size:11px;color:#8b949e;line-height:1.9;">'
            f'<span style="color:#f85149;">&#9632;</span> Red = move together (+1.0)<br>'
            f'<span style="color:#aaa;">&#9632;</span> White = no relationship (0.0)<br>'
            f'<span style="color:#58a6ff;">&#9632;</span> Blue = move opposite (&#8722;1.0)<br><br>'
            f'High average correlation = contagion risk<br>'
            f'Diversification works when colours are mixed<br>'
            f'60-day window captures current market regime'
            f'</div></div></div>'
            f'<div style="margin-top:10px;font-size:9px;color:#8b949e;font-family:monospace;">'
            f'Correlation = 60-day rolling Pearson correlation of daily returns &#183; '
            f'Key 20 instruments selected as representatives of each asset class</div>'
        ) + _build_rsr(df)
    else:
        analysis_tab = '<div style="padding:40px;text-align:center;color:#8b949e;">Insufficient data for correlation analysis.</div>' + _build_rsr(df)



    # ══ TAB 5: REGIME ═════════════════════════════════════════════════════════
    if regime_data:
        reg      = regime_data.get("regime","Calm")
        streak   = regime_data.get("days_in_regime", 0)
        stats    = regime_data.get("stats", {})
        drivers  = regime_data.get("drivers", {})
        timeline = regime_data.get("timeline", [])
        episodes = regime_data.get("episodes", [])

        rc_ = {"Crisis":"#f85149","Stressed":"#e3b341","Calm":"#3fb950"}.get(reg,"#8b949e")
        rb_ = {"Crisis":"#2d0f0e","Stressed":"#2d2106","Calm":"#0d2318"}.get(reg,"#161b22")

        reg_desc = {
            "Calm":     "Markets are operating within normal historical ranges. Volatility and drawdowns are contained. Risk appetite is stable.",
            "Stressed": "Elevated volatility or meaningful drawdown detected. Markets are pricing in uncertainty. Monitor closely.",
            "Crisis":   "Extreme volatility or severe drawdown detected. Historical crisis-level conditions. Defensive positioning warranted.",
        }.get(reg, "")

        # ── Regime timeline SVG ───────────────────────────────────────────────
        if timeline:
            tl_w = 900; tl_h = 60; bar_w = max(1, tl_w // len(timeline))
            tl_parts = [f'<svg viewBox="0 0 {tl_w} {tl_h}" width="100%" style="max-width:{tl_w}px;display:block;">']
            for i, pt in enumerate(timeline):
                x = i * bar_w
                tl_parts.append(f'<rect x="{x}" y="0" width="{bar_w+1}" height="{tl_h}" fill="{pt["color"]}" opacity="0.85"/>')
            # Month labels every ~21 bars
            prev_month = ""
            for i, pt in enumerate(timeline):
                month = pt["date"][:7]
                if month != prev_month and i % 21 == 0:
                    prev_month = month
                    x = i * bar_w
                    tl_parts.append(f'<text x="{x+2}" y="{tl_h-4}" font-size="8" fill="#e6edf3" opacity="0.7">{month}</text>')
            tl_parts.append('</svg>')
            timeline_svg = "".join(tl_parts)
        else:
            timeline_svg = "<div style='color:#8b949e;'>Insufficient history for timeline.</div>"

        # ── Stats table ───────────────────────────────────────────────────────
        stats_rows = ""
        for rname, rcolor in [("Calm","#3fb950"),("Stressed","#e3b341"),("Crisis","#f85149")]:
            rs = stats.get(rname, {})
            stats_rows += (
                f'<tr><td style="padding:8px 12px;color:{rcolor};font-weight:700;font-family:monospace;">{rname}</td>'
                f'<td class="num gr">{rs.get("days",0):,}</td>'
                f'<td class="num gr">{rs.get("pct",0):.1f}%</td>'
                f'<td class="num gr">{rs.get("avg_duration",0):.0f} days</td></tr>'
            )

        # ── Episodes table ────────────────────────────────────────────────────
        ep_rows = ""
        for ep in reversed(episodes):
            ep_rows += (
                f'<tr><td style="padding:7px 12px;color:#e6edf3;font-size:11px;">{ep["start"]}</td>'
                f'<td style="padding:7px 12px;color:#8b949e;font-size:11px;">{ep["end"]}</td>'
                f'<td class="num nr" style="font-size:11px;">{ep["depth"]:.1f}%</td></tr>'
            )

        vol_pct_color = "#f85149" if drivers.get("vol_pct",0)>90 else "#e3b341" if drivers.get("vol_pct",0)>70 else "#3fb950"
        dd_pct_color  = "#f85149" if drivers.get("dd_pct",0)<20  else "#e3b341" if drivers.get("dd_pct",0)<40  else "#3fb950"

        regime_tab = (
            # Current regime hero
            f'<div style="background:{rb_};border:2px solid {rc_};border-radius:12px;padding:24px 28px;margin-bottom:14px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:16px;">'
            f'<div>'
            f'<div style="font-size:9px;color:{rc_};letter-spacing:3px;font-family:monospace;margin-bottom:8px;">CURRENT MARKET REGIME</div>'
            f'<div style="font-size:42px;font-weight:700;color:{rc_};font-family:monospace;letter-spacing:2px;">{reg.upper()}</div>'
            f'<div style="font-size:11px;color:#e6edf3;margin-top:8px;max-width:500px;line-height:1.6;">{reg_desc}</div>'
            f'</div>'
            f'<div style="text-align:center;">'
            f'<div style="font-size:9px;color:#8b949e;letter-spacing:2px;margin-bottom:6px;">DAYS IN REGIME</div>'
            f'<div style="font-size:48px;font-weight:700;color:{rc_};font-family:monospace;">{streak}</div>'
            f'<div style="font-size:9px;color:#8b949e;margin-top:4px;">consecutive trading days</div>'
            f'</div></div>'

            # Drivers
            f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px;">'
            f'<div class="fc">'
            f'<div class="lbl">VOL DRIVER — WORLD (ACWI)</div>'
            f'<div style="display:flex;align-items:baseline;gap:10px;margin:8px 0;">'
            f'<div style="font-size:28px;font-weight:700;color:{vol_pct_color};font-family:monospace;">{drivers.get("vol_now",0):.1f}%</div>'
            f'<div style="font-size:12px;color:#8b949e;">annualised vol</div></div>'
            f'<div style="font-size:11px;color:#e6edf3;">At <span style="color:{vol_pct_color};font-weight:700;">{drivers.get("vol_pct",0):.0f}th percentile</span> of 10-year history</div>'
            f'<div style="background:#21262d;border-radius:4px;height:8px;margin-top:10px;">'
            f'<div style="width:{min(100,drivers.get("vol_pct",0)):.0f}%;background:{vol_pct_color};height:8px;border-radius:4px;"></div></div>'
            f'</div>'
            f'<div class="fc">'
            f'<div class="lbl">DRAWDOWN DRIVER — WORLD (ACWI)</div>'
            f'<div style="display:flex;align-items:baseline;gap:10px;margin:8px 0;">'
            f'<div style="font-size:28px;font-weight:700;color:{dd_pct_color};font-family:monospace;">{drivers.get("dd_now",0):.1f}%</div>'
            f'<div style="font-size:12px;color:#8b949e;">from 1Y peak</div></div>'
            f'<div style="font-size:11px;color:#e6edf3;"><span style="color:{dd_pct_color};font-weight:700;">{drivers.get("dd_pct",0):.0f}%</span> of history had smaller drawdowns</div>'
            f'<div style="background:#21262d;border-radius:4px;height:8px;margin-top:10px;">'
            f'<div style="width:{min(100,100-drivers.get("dd_pct",0)):.0f}%;background:{dd_pct_color};height:8px;border-radius:4px;"></div></div>'
            f'</div></div>'

            # Timeline
            f'<div class="fc" style="margin-bottom:14px;">'
            f'<div class="lbl" style="margin-bottom:10px;">REGIME TIMELINE — LAST 2 YEARS</div>'
            f'<div style="display:flex;gap:16px;margin-bottom:8px;">'
            f'<span style="font-size:10px;color:#3fb950;">&#9632; CALM</span>'
            f'<span style="font-size:10px;color:#e3b341;">&#9632; STRESSED</span>'
            f'<span style="font-size:10px;color:#f85149;">&#9632; CRISIS</span>'
            f'</div>'
            f'{timeline_svg}</div>'

            # Stats + Episodes
            f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;">'
            f'<div class="fc">'
            f'<div class="lbl" style="margin-bottom:10px;">REGIME STATISTICS — 10 YEAR HISTORY</div>'
            f'<table style="width:100%;border-collapse:collapse;font-size:12px;">'
            f'<thead><tr><th style="text-align:left;padding:6px 12px;font-size:9px;color:#8b949e;border-bottom:1px solid #30363d;">Regime</th>'
            f'<th style="text-align:right;padding:6px 12px;font-size:9px;color:#8b949e;border-bottom:1px solid #30363d;">Days</th>'
            f'<th style="text-align:right;padding:6px 12px;font-size:9px;color:#8b949e;border-bottom:1px solid #30363d;">% Time</th>'
            f'<th style="text-align:right;padding:6px 12px;font-size:9px;color:#8b949e;border-bottom:1px solid #30363d;">Avg Duration</th>'
            f'</tr></thead><tbody>{stats_rows}</tbody></table></div>'
            f'<div class="fc">'
            f'<div class="lbl" style="margin-bottom:10px;">CRISIS EPISODES — WORLD DRAWDOWN &lt; &#8722;15%</div>'
            f'<table style="width:100%;border-collapse:collapse;font-size:12px;">'
            f'<thead><tr>'
            f'<th style="text-align:left;padding:6px 12px;font-size:9px;color:#8b949e;border-bottom:1px solid #30363d;">Start</th>'
            f'<th style="text-align:left;padding:6px 12px;font-size:9px;color:#8b949e;border-bottom:1px solid #30363d;">End</th>'
            f'<th style="text-align:right;padding:6px 12px;font-size:9px;color:#8b949e;border-bottom:1px solid #30363d;">Peak DD</th>'
            f'</tr></thead><tbody>{ep_rows if ep_rows else "<tr><td colspan=3 style=padding:8px;color:#8b949e;>No episodes detected</td></tr>"}</tbody></table></div></div>'
            f'<div style="margin-top:10px;font-size:9px;color:#8b949e;font-family:monospace;">'
            f'Regime = deterministic state machine on ACWI (World ETF) &#183; '
            f'Crisis: vol &ge; 90th pct OR dd &le; 10th pct &#183; '
            f'Stressed: vol &ge; 70th pct OR dd &le; 30th pct &#183; '
            f'Ex-ante expanding quantile thresholds (no look-ahead bias)</div>'
        )
    else:
        regime_tab = '<div style="padding:40px;text-align:center;color:#8b949e;">Regime data unavailable.</div>'


    # ── Build Fear & Greed HTML ───────────────────────────────────────────────
    if fg_data:
        fg_score  = fg_data.get("score", 50)
        fg_label  = fg_data.get("label", "Neutral")
        fg_emoji  = fg_data.get("emoji", "😐")
        fg_color  = fg_data.get("color", "#e3b341")
        fg_details= fg_data.get("details", {})

        # Gauge needle SVG
        fg_angle  = int(fg_score / 100 * 180)
        fg_bg     = "#2d0f0e" if fg_score<=25 else "#2d1a0e" if fg_score<=45 else "#2d2106" if fg_score<=55 else "#0d2318" if fg_score<=75 else "#052e16"

        # Component bars
        comp_bars = ""
        for comp_name, comp_data in fg_details.items():
            cs = comp_data.get("score", 50)
            cv = comp_data.get("value", "")
            cc = "#f85149" if cs<=25 else "#ff7b72" if cs<=45 else "#e3b341" if cs<=55 else "#7ee787" if cs<=75 else "#3fb950"
            comp_bars += (
                f'<div style="margin-bottom:8px;">'
                f'<div style="display:flex;justify-content:space-between;margin-bottom:3px;">'
                f'<span style="font-size:10px;color:#e6edf3;">{comp_name}</span>'
                f'<span style="font-size:10px;font-family:monospace;color:{cc};">{cs:.0f}</span></div>'
                f'<div style="background:#21262d;border-radius:3px;height:6px;">'
                f'<div style="width:{cs:.0f}%;background:{cc};height:6px;border-radius:3px;"></div></div>'
                f'<div style="font-size:9px;color:#8b949e;margin-top:2px;">{cv}</div>'
                f'</div>'
            )

        # Gradient scale bar
        scale_html = (
            '<div style="margin:10px 0 4px;">'
            '<div style="height:10px;border-radius:5px;background:linear-gradient(to right,#f85149,#ff7b72,#e3b341,#7ee787,#3fb950);position:relative;">'
            f'<div style="position:absolute;left:{fg_score:.0f}%;top:-4px;transform:translateX(-50%);">'
            '<div style="width:3px;height:18px;background:#fff;border-radius:2px;"></div></div></div>'
            '<div style="display:flex;justify-content:space-between;font-size:8px;color:#8b949e;margin-top:3px;">'
            '<span>Extreme Fear</span><span>Fear</span><span>Neutral</span><span>Greed</span><span>Extreme Greed</span>'
            '</div></div>'
        )

        fg_html = (
            f'<div style="background:{fg_bg};border:1px solid {fg_color};border-radius:10px;'
            f'padding:18px 20px;margin-bottom:14px;">'
            f'<div style="display:grid;grid-template-columns:auto 1fr;gap:20px;align-items:start;">'
            # Left: score display
            f'<div style="text-align:center;min-width:140px;">'
            f'<div style="font-size:9px;color:{fg_color};letter-spacing:2px;margin-bottom:6px;font-family:monospace;">FEAR & GREED INDEX</div>'
            f'<div style="font-size:56px;">{fg_emoji}</div>'
            f'<div style="font-size:36px;font-weight:700;color:{fg_color};font-family:monospace;line-height:1;">{fg_score:.0f}</div>'
            f'<div style="font-size:12px;font-weight:700;color:{fg_color};margin-top:4px;">{fg_label}</div>'
            f'{scale_html}'
            f'</div>'
            # Right: components
            f'<div>'
            f'<div style="font-size:9px;color:#8b949e;letter-spacing:1px;text-transform:uppercase;margin-bottom:10px;">COMPONENTS</div>'
            f'{comp_bars}'
            f'</div>'
            f'</div></div>'
        )
    else:
        fg_html = ""


    # ══ TAB 6: SUMMARY — GRAND ACTIONABLE INSIGHT ═════════════════════════════
    # Define rc_ for summary tab (fragility color) — may not be in scope
    _frag_score = frag_df.attrs.get('system_score', 50) if frag_df is not None and not frag_df.empty else 50
    rc_ = '#f85149' if _frag_score >= 70 else '#e3b341' if _frag_score >= 50 else '#3fb950'
    # ══ TAB 6: SUMMARY — GRAND ACTIONABLE INSIGHT ═════════════════════════════
    # Derive key signals
    reg_now   = regime_data.get("regime","Calm") if regime_data else "Calm"
    reg_color = {"Crisis":"#f85149","Stressed":"#e3b341","Calm":"#3fb950"}.get(reg_now,"#8b949e")
    reg_bg    = {"Crisis":"#2d0f0e","Stressed":"#2d2106","Calm":"#0d2318"}.get(reg_now,"#161b22")
    frag_sys  = _frag_score
    frag_reg  = frag_df.attrs.get("regime","CALM") if frag_df is not None and not frag_df.empty else "CALM"

    # Grand verdict
    if reg_now == "Crisis" or frag_sys >= 70:
        verdict = "DEFENSIVE — Reduce Risk Exposure"
        verdict_detail = "Both regime and fragility signal extreme stress. Capital preservation is the priority. Avoid adding risk."
        verdict_color  = "#f85149"; verdict_bg = "#2d0f0e"
        actions = [
            "Reduce equity exposure — rotate to cash, short-duration bonds or gold",
            "Monitor HY credit spreads — widening signals further deterioration",
            "Avoid leveraged positions — volatility regime is elevated",
            "Watch VIX trajectory — sustained >25 confirms crisis regime",
        ]
    elif reg_now == "Stressed" or frag_sys >= 50:
        verdict = "CAUTIOUS — Selective Risk Taking"
        verdict_detail = "Markets are stressed but not in crisis. Opportunities exist but selectivity is essential."
        verdict_color  = "#e3b341"; verdict_bg = "#2d2106"
        actions = [
            "Favour quality over momentum — reduce high-beta positions",
            "Increase diversification — correlation is rising, reducing hedge value",
            "Monitor fragility leaders — highest-scoring instruments signal contagion risk",
            "Consider defensive sectors — utilities, healthcare, short-duration bonds",
        ]
    else:
        verdict = "CONSTRUCTIVE — Risk-On Environment"
        verdict_detail = "Markets are calm with contained volatility and drawdowns. Risk appetite can be maintained or increased."
        verdict_color  = "#3fb950"; verdict_bg = "#0d2318"
        actions = [
            "Maintain or increase risk exposure — regime supports it",
            "Look for laggards with improving momentum within asset classes",
            "Monitor fragility creep — early warning before regime shifts",
            "Diversification less critical in calm regimes but maintain core hedges",
        ]

    # Top 3 opportunities (best MTD green instruments)
    opps = df[df["rag_label"].str.strip()=="GREEN"].nlargest(3,"ret_1m")[["name","ticker","ret_1m","ret_ytd"]]

    # Top 3 risks (most fragile + red signal)
    risks_df = frag_df[frag_df["rag"].isin(["CRISIS","STRESSED"])].head(3) if frag_df is not None and not frag_df.empty else pd.DataFrame()

    opp_cards = ""
    for _, r in opps.iterrows():
        m = r["ret_1m"]; y = r["ret_ytd"] if not pd.isna(r["ret_ytd"]) else 0
        opp_cards += (
            f'<div style="background:#0d2318;border:1px solid #3fb950;border-radius:8px;padding:14px 16px;">'
            f'<div style="font-size:11px;color:#e6edf3;font-weight:600;">{r["name"]}</div>'
            f'<div style="font-family:monospace;font-size:9px;color:#8b949e;margin-top:2px;">{r["ticker"]}</div>'
            f'<div style="font-size:20px;font-weight:700;color:#3fb950;font-family:monospace;margin-top:6px;">+{m*100:.1f}%</div>'
            f'<div style="font-size:9px;color:#8b949e;margin-top:2px;">MTD &nbsp;&#183;&nbsp; YTD: {"+"+str(round(y*100,1))+"%"  if y>=0 else str(round(y*100,1))+"%"}</div>'
            f'</div>'
        )

    risk_cards = ""
    for _, r in risks_df.iterrows():
        fc = "#f85149" if r["rag"]=="CRISIS" else "#e3b341"
        rb = "#2d0f0e" if r["rag"]=="CRISIS" else "#2d2106"
        risk_cards += (
            f'<div style="background:{rb};border:1px solid {fc};border-radius:8px;padding:14px 16px;">'
            f'<div style="font-size:11px;color:#e6edf3;font-weight:600;">{r["name"]}</div>'
            f'<div style="font-family:monospace;font-size:9px;color:#8b949e;margin-top:2px;">{r["ticker"]}</div>'
            f'<div style="font-size:20px;font-weight:700;color:{fc};font-family:monospace;margin-top:6px;">{r["fragility"]:.0f}<span style="font-size:12px;">/100</span></div>'
            f'<div style="font-size:9px;color:#8b949e;margin-top:2px;">Fragility score &nbsp;&#183;&nbsp; {r["rag"]}</div>'
            f'</div>'
        )

    action_items = "".join(
        f'<div style="display:flex;gap:12px;padding:10px 0;border-bottom:1px solid #21262d;">'
        f'<div style="color:{verdict_color};font-size:14px;margin-top:1px;">&#10148;</div>'
        f'<div style="font-size:12px;color:#e6edf3;line-height:1.5;">{a}</div></div>'
        for a in actions
    )

    summary_tab = (
        # Grand verdict
        f'<div style="background:{verdict_bg};border:2px solid {verdict_color};border-radius:12px;'
        f'padding:24px 28px;margin-bottom:14px;">'
        f'<div style="font-size:9px;color:{verdict_color};letter-spacing:3px;font-family:monospace;margin-bottom:8px;">MARKET VERDICT</div>'
        f'<div style="font-size:28px;font-weight:700;color:{verdict_color};font-family:monospace;">{verdict}</div>'
        f'<div style="font-size:12px;color:#e6edf3;margin-top:10px;line-height:1.6;max-width:700px;">{verdict_detail}</div>'
        f'</div>'

        # Signal summary row
        f'<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:14px;">'
        f'<div class="fc" style="text-align:center;">'
        f'<div class="lbl">MARKET TONE</div>'
        f'<div class="pill" style="background:{tb};color:{tc};border:1px solid {tc};margin-top:6px;">{tone}</div>'
        f'</div>'
        f'<div class="fc" style="text-align:center;">'
        f'<div class="lbl">REGIME</div>'
        f'<div class="pill" style="background:{reg_bg};color:{reg_color};border:1px solid {reg_color};margin-top:6px;">{reg_now.upper()}</div>'
        f'</div>'
        f'<div class="fc" style="text-align:center;">'
        f'<div class="lbl">FRAGILITY</div>'
        f'<div style="font-size:26px;font-weight:700;color:{rc_};font-family:monospace;margin-top:4px;">{frag_sys:.0f}</div>'
        f'<div style="font-size:9px;color:#8b949e;">{frag_reg}</div>'
        f'</div>'
        f'<div class="fc" style="text-align:center;">'
        f'<div class="lbl">RAG SIGNALS</div>'
        f'<div style="display:flex;justify-content:center;gap:10px;margin-top:6px;">'
        f'<span style="color:#f85149;font-size:18px;font-weight:700;font-family:monospace;">{nr}</span>'
        f'<span style="color:#8b949e;font-size:18px;">·</span>'
        f'<span style="color:#e3b341;font-size:18px;font-weight:700;font-family:monospace;">{na}</span>'
        f'<span style="color:#8b949e;font-size:18px;">·</span>'
        f'<span style="color:#3fb950;font-size:18px;font-weight:700;font-family:monospace;">{ng}</span>'
        f'</div>'
        f'<div style="font-size:9px;color:#8b949e;margin-top:2px;">RED · AMBER · GREEN</div>'
        f'</div></div>'

        # Fear & Greed
        + fg_html
        # Recommended actions
        + f'<div class="fc" style="margin-bottom:14px;">'
        f'<div class="lbl" style="margin-bottom:4px;">RECOMMENDED ACTIONS</div>'
        f'{action_items}</div>'

        # Opportunities + Risks
        f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px;">'
        f'<div><div style="font-size:9px;font-weight:700;letter-spacing:2px;color:#3fb950;'
        f'text-transform:uppercase;margin-bottom:10px;">&#128200; Top Opportunities (MTD)</div>'
        f'<div style="display:flex;flex-direction:column;gap:8px;">{opp_cards if opp_cards else "<div style=color:#8b949e;>None identified</div>"}</div></div>'
        f'<div><div style="font-size:9px;font-weight:700;letter-spacing:2px;color:#f85149;'
        f'text-transform:uppercase;margin-bottom:10px;">&#9888; Top Risks (Fragility)</div>'
        f'<div style="display:flex;flex-direction:column;gap:8px;">{risk_cards if risk_cards else "<div style=color:#8b949e;>None identified</div>"}</div></div>'
        f'</div>'

        f'<div style="font-size:9px;color:#8b949e;font-family:monospace;line-height:1.8;">'
        f'Verdict = BK proprietary synthesis of Market Tone + Regime State Machine + Fragility Score &#183; '
        f'For informational purposes only &#183; Not investment advice</div>'
    )


    # ══ TAB 7: EDGE — Portfolio Optimisation ══════════════════════════════════
    # Auto-generated regime-aware allocation + commentary
    reg_now_e = regime_data.get("regime","Calm") if regime_data else "Calm"

    # Regime-based suggested allocations
    ALLOCATIONS = {
        "Crisis": [
            ("Cash & T-Bills",      35, "#58a6ff"),
            ("Gold",                25, "#e3b341"),
            ("Govt Bonds (Long)",   20, "#3fb950"),
            ("Equities (Defensive)",10, "#7ee787"),
            ("HY Credit",           5, "#8b949e"),
            ("Alternatives",        5, "#8b949e"),
        ],
        "Stressed": [
            ("Cash & T-Bills",      20, "#58a6ff"),
            ("Gold",                15, "#e3b341"),
            ("Govt Bonds (Long)",   20, "#3fb950"),
            ("Equities (Quality)",  25, "#7ee787"),
            ("IG Credit",           15, "#a5d6a7"),
            ("Alternatives",         5, "#8b949e"),
        ],
        "Calm": [
            ("Global Equities",     45, "#3fb950"),
            ("EM Equities",         10, "#7ee787"),
            ("IG Credit",           15, "#58a6ff"),
            ("Govt Bonds",          10, "#a5d6a7"),
            ("Gold",                10, "#e3b341"),
            ("Alternatives",        10, "#8b949e"),
        ],
    }

    alloc = ALLOCATIONS.get(reg_now_e, ALLOCATIONS["Calm"])
    rc_e  = {"Crisis":"#f85149","Stressed":"#e3b341","Calm":"#3fb950"}.get(reg_now_e,"#8b949e")
    rb_e  = {"Crisis":"#2d0f0e","Stressed":"#2d2106","Calm":"#0d2318"}.get(reg_now_e,"#161b22")

    # Portfolio allocation bars
    alloc_bars = ""
    for asset, pct, color in alloc:
        alloc_bars += (
            f'<div style="display:flex;align-items:center;gap:12px;padding:8px 0;border-bottom:1px solid #21262d;">'
            f'<div style="width:160px;font-size:11px;color:#e6edf3;">{asset}</div>'
            f'<div style="flex:1;background:#21262d;border-radius:3px;height:10px;">'
            f'<div style="width:{pct}%;background:{color};height:10px;border-radius:3px;"></div></div>'
            f'<div style="width:40px;text-align:right;font-family:monospace;font-size:12px;font-weight:700;color:{color};">{pct}%</div>'
            f'</div>'
        )

    # Auto-generated commentary from data
    top_gain  = df.nlargest(1,"ret_1m")["name"].iloc[0] if not df.empty else "N/A"
    top_risk  = frag_df.head(1)["name"].iloc[0] if frag_df is not None and not frag_df.empty else "N/A"
    vol_count = int((df["vol_now"] > df["vol_1m_ago"]).sum()) if "vol_now" in df.columns else 0
    commentary = (
        f"Markets are in a <strong style='color:{rc_e};'>{reg_now_e}</strong> regime. "
        f"Volatility is rising across {vol_count} instruments, led by {top_gain} on the upside. "
        f"The highest fragility risk is concentrated in {top_risk}. "
        f"The suggested allocation below reflects a {reg_now_e.lower()} regime posture — "
        f"{'emphasising capital preservation and safe havens.' if reg_now_e=='Crisis' else 'balancing defence with selective risk-taking.' if reg_now_e=='Stressed' else 'favouring growth assets with diversification.'}"
    )

    # Key metrics for context
    best_asset  = df.nlargest(1,"ret_1m")[["name","ret_1m"]].iloc[0]
    worst_asset = df.nsmallest(1,"ret_1m")[["name","ret_1m"]].iloc[0]

    edge_tab = (
        f'<div style="background:{rb_e};border:2px solid {rc_e};border-radius:10px;padding:18px 24px;margin-bottom:14px;">'
        f'<div style="font-size:9px;color:{rc_e};letter-spacing:3px;font-family:monospace;margin-bottom:6px;">REGIME-AWARE ALLOCATION</div>'
        f'<div style="font-size:22px;font-weight:700;color:{rc_e};font-family:monospace;">{reg_now_e.upper()} REGIME</div>'
        f'<div style="font-size:11px;color:#e6edf3;margin-top:8px;line-height:1.7;">{commentary}</div>'
        f'</div>'
        f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px;">'
        # Allocation
        f'<div class="fc">'
        f'<div class="lbl" style="margin-bottom:12px;">SUGGESTED PORTFOLIO ALLOCATION</div>'
        f'{alloc_bars}'
        f'<div style="font-size:9px;color:#8b949e;margin-top:8px;font-family:monospace;">'
        f'Allocation based on current regime: {reg_now_e} &#183; Rebalance as regime shifts &#183; Not investment advice</div>'
        f'</div>'
        # Key signals for Edge
        f'<div style="display:flex;flex-direction:column;gap:14px;">'
        f'<div class="fc">'
        f'<div class="lbl" style="margin-bottom:8px;">BEST OPPORTUNITY THIS MONTH</div>'
        f'<div style="font-size:18px;font-weight:700;color:#3fb950;font-family:monospace;">{best_asset["name"]}</div>'
        f'<div style="font-size:24px;font-weight:700;color:#3fb950;font-family:monospace;">{best_asset["ret_1m"]*100:+.1f}%</div>'
        f'<div style="font-size:9px;color:#8b949e;margin-top:4px;">MTD Return (1-Month)</div>'
        f'</div>'
        f'<div class="fc">'
        f'<div class="lbl" style="margin-bottom:8px;">HIGHEST RISK TO MONITOR</div>'
        f'<div style="font-size:18px;font-weight:700;color:#f85149;font-family:monospace;">{worst_asset["name"]}</div>'
        f'<div style="font-size:24px;font-weight:700;color:#f85149;font-family:monospace;">{worst_asset["ret_1m"]*100:+.1f}%</div>'
        f'<div style="font-size:9px;color:#8b949e;margin-top:4px;">MTD Return (1-Month)</div>'
        f'</div>'
        f'<div class="fc">'
        f'<div class="lbl" style="margin-bottom:8px;">INSTRUMENTS WITH RISING VOL</div>'
        f'<div style="font-size:32px;font-weight:700;color:#e3b341;font-family:monospace;">{vol_count}</div>'
        f'<div style="font-size:9px;color:#8b949e;margin-top:4px;">of {len(df)} instruments showing elevated volatility vs 1M ago</div>'
        f'</div>'
        f'</div></div>'
        f'<div style="font-size:9px;color:#8b949e;font-family:monospace;line-height:1.8;">'
        f'Edge = regime-aware portfolio intelligence &#183; '
        f'Allocation shifts automatically as market regime changes &#183; '
        f'For informational purposes only &#183; Not investment advice &#183; Past performance is not indicative of future results'
        f'</div>'
    )

    # ══ TAB 8: ABOUT ══════════════════════════════════════════════════════════
    about_tab = (
        f'<div style="max-width:800px;margin:0 auto;">'
        # Header
        f'<div style="background:linear-gradient(135deg,#1c2128,#161b22);border:1px solid #30363d;'
        f'border-radius:12px;padding:32px;margin-bottom:20px;text-align:center;">'
        f'<div style="width:80px;height:80px;border-radius:50%;background:linear-gradient(135deg,#58a6ff,#3fb950);'
        f'margin:0 auto 16px;display:flex;align-items:center;justify-content:center;">'
        f'<div style="font-size:28px;font-weight:700;color:#fff;font-family:monospace;">BK</div></div>'
        f'<div style="font-size:22px;font-weight:700;color:#e6edf3;font-family:monospace;letter-spacing:1px;">Bhavesh Kamdar</div>'
        f'<div style="font-size:12px;color:#58a6ff;margin-top:6px;letter-spacing:2px;text-transform:uppercase;">Founder · BKIQ Markets</div>'
        f'<div style="font-size:11px;color:#8b949e;margin-top:4px;">Singapore</div>'
        f'</div>'
        # Philosophy
        f'<div class="fc" style="margin-bottom:14px;">'
        f'<div class="lbl" style="margin-bottom:12px;">INVESTMENT PHILOSOPHY</div>'
        f'<div style="font-size:13px;color:#e6edf3;line-height:1.9;">'
        f'<em style="color:#58a6ff;">"Markets are not random — they move in regimes. '
        f'Understanding the current regime is the single most important edge an investor can have."</em>'
        f'</div>'
        f'<div style="font-size:12px;color:#8b949e;margin-top:16px;line-height:1.8;">'
        f'I believe that most market participants focus too heavily on returns and not enough on risk regimes. '
        f'A portfolio that performs well in a Calm regime can be catastrophically exposed in a Crisis regime. '
        f'The BK Fragility Framework was built to solve this problem — to give investors early warning '
        f'before fragility becomes crisis, and to provide a disciplined, data-driven framework for '
        f'navigating regime transitions.</div>'
        f'</div>'
        # Framework
        f'<div class="fc" style="margin-bottom:14px;">'
        f'<div class="lbl" style="margin-bottom:12px;">THE BK FRAGILITY FRAMEWORK</div>'
        f'<div style="font-size:12px;color:#8b949e;line-height:1.8;margin-bottom:14px;">'
        f'The BK Fragility Framework is a proprietary multi-factor risk scoring system that measures '
        f'market stress across 6 dimensions, weighted by their empirical contribution to systemic risk:</div>'
        f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">'
        + "".join(
            f'<div style="background:#1c2128;border:1px solid #30363d;border-radius:6px;padding:12px 14px;">'
            f'<div style="font-size:10px;font-weight:700;color:{color};font-family:monospace;">{name}</div>'
            f'<div style="font-size:18px;font-weight:700;color:{color};font-family:monospace;">{pct}%</div>'
            f'<div style="font-size:9px;color:#8b949e;margin-top:2px;">{desc}</div>'
            f'</div>'
            for name, pct, color, desc in [
                ("Drawdown",           22, "#f85149", "Distance from rolling peak"),
                ("CVaR / Tail Risk",   20, "#ff7b72", "Expected loss in worst 5% of days"),
                ("Contagion",          18, "#e3b341", "Correlation to world market (ACWI)"),
                ("Volatility",         15, "#58a6ff", "20-day realised annualised vol"),
                ("Trend",              15, "#7ee787", "Distance below 200-day moving average"),
                ("Volume Stress",      10, "#8b949e", "Liquidity dry-up or panic spike"),
            ]
        )
        + f'</div>'
        f'<div style="font-size:11px;color:#8b949e;margin-top:12px;line-height:1.7;">'
        f'Scores are computed using robust time-series z-scores mapped to 0–100 via logistic function '
        f'with EWMA smoothing. CRISIS ≥ 70 · STRESSED 50–70 · CALM < 50</div>'
        f'</div>'
        # What is BKIQ
        f'<div class="fc" style="margin-bottom:14px;">'
        f'<div class="lbl" style="margin-bottom:12px;">ABOUT BKIQ MARKETS</div>'
        f'<div style="font-size:12px;color:#8b949e;line-height:1.8;">'
        f'BKIQ Markets is a Singapore-based market intelligence platform delivering daily '
        f'institutional-grade analysis across 60 instruments and 12 asset classes. '
        f'The platform monitors global equities, fixed income, commodities, FX, crypto and volatility '
        f'through 6 analytical lenses — Performance, Risk, Fragility, Analysis, Regime and Edge.<br><br>'
        f'Built for family office analysts, private bankers, wealth managers and institutional traders '
        f'who need actionable intelligence delivered before markets open.'
        f'</div>'
        f'</div>'
        # Contact
        f'<div style="text-align:center;padding:20px;border-top:1px solid #30363d;margin-top:6px;">'
        f'<div style="font-size:11px;color:#8b949e;margin-bottom:12px;">Get in touch</div>'
        f'<div style="display:flex;gap:10px;justify-content:center;flex-wrap:wrap;margin-bottom:12px;">'
        f'<a href="https://linkedin.com/in/bhavesh-kamdar" target="_blank" '
        f'style="display:inline-block;background:#0a66c2;color:#fff;font-size:11px;font-weight:700;'
        f'padding:8px 18px;border-radius:6px;text-decoration:none;font-family:monospace;">LinkedIn</a>'
        f'<a href="mailto:bhavesh113@gmail.com" '
        f'style="display:inline-block;background:#1c2128;border:1px solid #30363d;color:#e6edf3;font-size:11px;font-weight:700;'
        f'padding:8px 18px;border-radius:6px;text-decoration:none;font-family:monospace;">bhavesh113@gmail.com</a>'
        f'<a href="tel:+6589474681" '
        f'style="display:inline-block;background:#1c2128;border:1px solid #30363d;color:#e6edf3;font-size:11px;font-weight:700;'
        f'padding:8px 18px;border-radius:6px;text-decoration:none;font-family:monospace;">+65 8947 4681</a>'
        f'</div>'
        f'<div style="font-size:9px;color:#8b949e;margin-top:12px;font-family:monospace;">'
        f'&#169; 2026 BKIQ Markets &#183; Singapore &#183; For informational purposes only &#183; Not investment advice</div>'
        f'</div>'
        f'</div>'
    )


    # ══ ASSEMBLE HTML ══════════════════════════════════════════════════════════
    mn="" if market_open else ' <span style="color:#e3b341;font-size:10px;">&#9888; Markets closed</span>'

    css=(":root{--bg:#0d1117;--ca:#161b22;--dk:#21262d;--br:#30363d;--w:#e6edf3;--g:#8b949e;--ac:#58a6ff;}"
         "*{box-sizing:border-box;margin:0;padding:0;}"
         "body{background:var(--bg);color:var(--w);font-family:'Segoe UI',system-ui,sans-serif;font-size:13px;}"
         ".wrap{max-width:1400px;margin:0 auto;padding:16px 12px;}"
         ".hdr{background:var(--ca);border:1px solid var(--br);border-radius:8px;padding:18px 24px;"
         "margin-bottom:14px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px;}"
         ".logo{font-family:monospace;font-size:20px;font-weight:700;letter-spacing:2px;}"
         ".logo span{color:#3fb950;}"
         ".sub{font-size:10px;color:var(--g);letter-spacing:2px;margin-top:4px;}"
         ".badge{display:inline-flex;align-items:center;gap:6px;background:var(--dk);border:1px solid var(--br);"
         "border-radius:20px;padding:3px 10px;font-size:9px;font-family:monospace;color:var(--g);margin-top:6px;}"
         ".dot{width:6px;height:6px;border-radius:50%;background:#3fb950;animation:pulse 2s infinite;display:inline-block;}"
         "@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.3}}"
         ".tabs{display:flex;gap:2px;margin-bottom:16px;border-bottom:3px solid var(--br);}"
         ".tb{padding:14px 28px;font-size:14px;font-weight:700;font-family:'Segoe UI',sans-serif;letter-spacing:0.3px;"
         "border:none;background:transparent;color:var(--g);cursor:pointer;"
         "border-bottom:3px solid transparent;margin-bottom:-3px;border-radius:6px 6px 0 0;transition:all 0.15s;}"
         ".tb:hover{color:var(--w);background:#1c2128;}"
         ".tb.on{color:var(--ac);border-bottom:3px solid var(--ac);background:#1c2128;}"
         ".tab{display:none;}.tab.on{display:block;}"
         ".tbar{display:flex;align-items:center;gap:12px;background:var(--ca);border:1px solid var(--br);"
         "border-radius:8px;padding:12px 24px;margin-bottom:14px;flex-wrap:wrap;}"
         ".lbl{font-size:9px;color:var(--g);letter-spacing:1px;text-transform:uppercase;margin-bottom:6px;}"
         ".pill{padding:4px 14px;border-radius:20px;font-size:11px;font-weight:700;font-family:monospace;letter-spacing:1px;}"
         ".dvdr{width:1px;height:40px;background:var(--br);margin:0 8px;}"
         ".rb{display:flex;gap:20px;}"
         ".ri{text-align:center;}"
         ".rn{font-size:22px;font-weight:700;font-family:monospace;}"
         ".rl{font-size:9px;color:var(--g);letter-spacing:1px;margin-top:2px;}"
         ".gl{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px;}"
         "@media(max-width:700px){.gl{grid-template-columns:1fr;}}"
         ".gc{background:var(--ca);border:1px solid var(--br);border-radius:8px;padding:16px 18px;}"
         ".gt{font-size:9px;font-weight:700;letter-spacing:2px;color:var(--g);margin-bottom:12px;"
         "display:flex;align-items:center;gap:8px;text-transform:uppercase;}"
         ".gd{width:8px;height:8px;border-radius:50%;display:inline-block;}"
         ".tw{background:var(--ca);border:1px solid var(--br);border-radius:8px;overflow-x:auto;}"
         "table{width:100%;border-collapse:collapse;font-size:12px;}"
         "th{background:#1c2128;padding:10px 8px;font-size:9px;letter-spacing:1px;text-transform:uppercase;"
         "color:var(--g);font-family:monospace;white-space:nowrap;border-bottom:2px solid var(--br);}"
         "th:first-child{text-align:left;padding-left:14px;}"
         "td{padding:7px 8px;border-bottom:1px solid var(--br);white-space:nowrap;}"
         "tr:last-child td{border-bottom:none;}"
         "tr:hover td{background:#1c2128;}"
         "td.an{text-align:left;padding-left:14px;color:var(--w);min-width:150px;}"
         "td.tk{font-family:monospace;font-size:10px;color:var(--g);font-weight:700;}"
         "td.num{font-family:monospace;text-align:right;}"
         "td.sig{font-family:monospace;font-size:10px;text-align:center;}"
         ".ps{color:#3fb950;}.pl{color:#7ee787;}.ng{color:#ff7b72;}.nr{color:#f85149;}"
         ".am{color:#e3b341;}.gr{color:#8b949e;}"
         ".sg{color:#3fb950;}.sa{color:#e3b341;}.sr{color:#f85149;}"
         "tr.sh td{background:#1c2128;font-size:9px;font-weight:700;letter-spacing:2px;"
         "text-transform:uppercase;color:var(--ac);padding:8px 14px;border-top:2px solid var(--br);}"
         ".vc{background:var(--ca);border:1px solid;border-radius:8px;padding:14px 20px;text-align:center;min-width:140px;}"
         ".vn{font-size:26px;font-weight:700;font-family:monospace;}"
         ".vl{font-size:9px;font-weight:700;letter-spacing:1px;margin-top:4px;}"
         ".vs{font-size:9px;color:var(--g);margin-top:2px;}"
         ".fc{background:var(--ca);border:1px solid var(--br);border-radius:8px;padding:16px 20px;}"
         ".footer{margin-top:14px;padding:12px 0;border-top:1px solid var(--br);"
         "display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;}"
         ".fn{font-size:9px;color:var(--g);line-height:1.9;font-family:monospace;}"
         ".fb{font-size:20px;font-weight:700;letter-spacing:-1px;font-family:monospace;}"
         ".fs{font-size:9px;color:var(--g);margin-top:2px;}"
         "@media(max-width:600px){"
         ".logo{font-size:15px;}.hdr{padding:12px 16px;}.wrap{padding:10px 8px;}"
         "th,td{padding:5px 6px;font-size:11px;}td.an{min-width:100px;}"
         ".tb{padding:10px 14px;font-size:11px;font-weight:700;}.rn{font-size:16px;}}")

    return (
        "<!DOCTYPE html><html lang='en'><head>"
        "<meta charset='UTF-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        "<!-- auto-refresh disabled for institutional use -->"
        "<title>BK Market Dashboard</title>"
        f"<script async src='https://www.googletagmanager.com/gtag/js?id={GA}'></script>"
        f"<script>window.dataLayer=window.dataLayer||[];function gtag(){{dataLayer.push(arguments);}}"
        f"gtag('js',new Date());gtag('config','{GA}');</script>"
        f"<style>{css}</style>"
        "</head><body><div class='wrap'>"
        "<div class='hdr'><div>"
        "<div class='logo'>BKIQ <span>MARKETS</span></div>"
        f"<div class='sub'>{N_INSTRUMENTS}-INSTRUMENT UNIVERSE &nbsp;&#183;&nbsp; Intelligence Before the Market Opens</div>"
        f"<div class='badge'><span class='dot'></span> Last updated: {date_str}</div>"
        "</div><div style='text-align:right;'>"
        f"<div style='font-family:monospace;font-size:13px;color:#e6edf3;font-weight:600;'>{date_str}</div>"
        f"<div style='font-size:9px;color:#8b949e;margin-top:4px;'>Auto-refreshes every hour{mn}</div>"
        "<div style='font-size:9px;color:#444d56;margin-top:3px;font-family:monospace;'>07:00 SGT &#183; MON&#8211;FRI</div>"
        "</div></div>"
        "<div class='tabs'>"
        "<button class='tb on' onclick=\"sw('intel',this)\">Intel</button>"
        "<button class='tb' onclick=\"sw('perf',this)\">Performance</button>"
        "<button class='tb' onclick=\"sw('risk',this)\">Risk</button>"
        "<button class='tb' onclick=\"sw('frag',this)\">Fragility</button>"
        "<button class='tb' onclick=\"sw('analysis',this)\">Analysis</button>"
        "<button class='tb' onclick=\"sw('regime',this)\">Regime</button>"
        "<button class='tb' onclick=\"sw('edge',this)\">Edge</button>"
        "<button class='tb' onclick=\"sw('about',this)\">About</button>"
        "</div>"
        f"<div id='t-intel' class='tab on'>{summary_tab}</div>"
        f"<div id='t-perf' class='tab'>{perf}</div>"
        f"<div id='t-risk' class='tab'>{risk}</div>"
        f"<div id='t-frag' class='tab'>{frag}</div>"
        f"<div id='t-analysis' class='tab'>{analysis_tab}</div>"
        f"<div id='t-regime' class='tab'>{regime_tab}</div>"
        f"<div id='t-edge' class='tab'>{edge_tab}</div>"
        f"<div id='t-about' class='tab'>{about_tab}</div>"
        "<div class='footer'><div class='fn'>"
        "Returns are price return in USD (ETF prices) &#183; FX returns reflect USD rate changes &#183; Trend = 20-day normalised sparkline<br>""Signal: RED &lt; &#8722;15% &#183; AMBER &#8722;15% to &#8722;7% &#183; GREEN &gt; &#8722;7% from 52-week high<br>"
        "Fragility: CRISIS &#8805;70 &#183; STRESSED 50&#8211;70 &#183; CALM &lt;50 &#183; BK Fragility Framework<br>"
        f"Generated: {gen_ts} SGT &#183; Prices via Yahoo Finance &#183; Auto-refreshes every hour"
        "</div><div style='text-align:right;'>"
        "<div class='fb'>BK</div>"
        "<div class='fs'>Market Intelligence &#183; Singapore</div>"
        "</div></div></div>"
        "<script>"
        "function sw(n,b){"
        "document.querySelectorAll('.tab').forEach(t=>t.classList.remove('on'));"
        "document.querySelectorAll('.tb').forEach(x=>x.classList.remove('on'));"
        "document.getElementById('t-'+n).classList.add('on');b.classList.add('on');}"
        "</script>"
        "</body></html>"
    )


def send_email(html_body: str) -> bool:
    now      = datetime.now(SGT)
    date_str = now.strftime("%a %d %b %Y")
    subject  = f"BK Market Dashboard · Daily Brief · {date_str}"
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = SENDER_EMAIL
        msg["To"]      = RECIPIENT_EMAIL
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SENDER_EMAIL, GMAIL_APP_PASS)
            server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())

        print(f"[Email]  Sent to {RECIPIENT_EMAIL}")
        return True

    except Exception as e:
        print(f"[Email]  ERROR: {e}")
        print("  Check SENDER_EMAIL, GMAIL_APP_PASS.")
        print("  Get an App Password at: myaccount.google.com > Security > App Passwords")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  POWERPOINT REPORT
# ══════════════════════════════════════════════════════════════════════════════

def render_pptx(df: pd.DataFrame, prices: pd.DataFrame, as_of: str, out_dir: str) -> str:
    try:
        from pptx import Presentation
        from pptx.util import Inches, Pt
        from pptx.dml.color import RGBColor
        from pptx.enum.text import PP_ALIGN
        from pptx.oxml.ns import qn
        from lxml import etree
    except ImportError:
        raise ImportError("python-pptx not installed. Run: pip install python-pptx")

    prs = Presentation()
    prs.slide_width  = Inches(13.33)
    prs.slide_height = Inches(7.5)
    blank = prs.slide_layouts[6]

    # ── Low-level helpers ────────────────────────────────────────────────────

    def _rgb(h: str) -> RGBColor:
        h = h.lstrip("#")
        return RGBColor(int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))

    def _set_bg(slide, h: str):
        fill = slide.background.fill
        fill.solid()
        fill.fore_color.rgb = _rgb(h)

    def _rect(slide, l, t, w, h, fill_h: str):
        shp = slide.shapes.add_shape(1, Inches(l), Inches(t), Inches(w), Inches(h))
        shp.fill.solid()
        shp.fill.fore_color.rgb = _rgb(fill_h)
        shp.line.fill.background()
        return shp

    def _txbox(slide, text, l, t, w, h,
               size=14, bold=False, color="e6edf3",
               align=PP_ALIGN.LEFT, italic=False):
        tb = slide.shapes.add_textbox(Inches(l), Inches(t), Inches(w), Inches(h))
        tf = tb.text_frame
        tf.word_wrap = False
        p = tf.paragraphs[0]
        p.alignment = align
        run = p.add_run()
        run.text = str(text)
        run.font.size = Pt(size)
        run.font.bold = bold
        run.font.italic = italic
        run.font.color.rgb = _rgb(color)
        run.font.name = "Calibri"

    def _set_cell(cell, text, bg_h="0d1117", fg_h="e6edf3",
                  size=10, bold=False, align=PP_ALIGN.CENTER):
        tc   = cell._tc
        tcPr = tc.get_or_add_tcPr()
        for child in list(tcPr):
            tag = child.tag.split("}")[-1]
            if tag in ("solidFill", "gradFill", "pattFill", "noFill"):
                tcPr.remove(child)
        sf = etree.SubElement(tcPr, qn("a:solidFill"))
        sr = etree.SubElement(sf, qn("a:srgbClr"))
        sr.set("val", bg_h.lstrip("#"))
        tf = cell.text_frame
        tf.word_wrap = False
        p  = tf.paragraphs[0]
        p.alignment = align
        p.clear()
        run = p.add_run()
        run.text = str(text)
        run.font.size = Pt(size)
        run.font.bold = bold
        run.font.color.rgb = _rgb(fg_h)
        run.font.name = "Calibri"

    # ── Cell color maps ──────────────────────────────────────────────────────

    def _cc_ret(v):
        if pd.isna(v): return "21262d", "8b949e"
        p = v * 100
        if p >=  3.0: return "065f46", "6ee7b7"
        if p >=  1.0: return "166534", "86efac"
        if p >=  0.0: return "1c3829", "bbf7d0"
        if p >= -1.0: return "3b1111", "fca5a5"
        if p >= -3.0: return "5a1a1a", "f87171"
        return "7f1d1d", "fca5a5"

    def _cc_vol(v):
        if pd.isna(v): return "21262d", "8b949e"
        p = v * 100
        if p > 30: return "7f1d1d", "fca5a5"
        if p > 18: return "78350f", "fde68a"
        return "14532d", "86efac"

    def _cc_dd(v):
        if pd.isna(v): return "21262d", "8b949e"
        p = v * 100
        if p < -15: return "7f1d1d", "fca5a5"
        if p <  -7: return "78350f", "fde68a"
        return "14532d", "86efac"

    def _cc_sharpe(v):
        if pd.isna(v): return "21262d", "8b949e"
        if v >  1.0: return "14532d", "86efac"
        if v >  0.0: return "78350f", "fde68a"
        return "7f1d1d", "fca5a5"

    def _cc_rag(label):
        rl = label.strip()
        if rl == "RED":   return "7f1d1d", "f87171"
        if rl == "AMBER": return "78350f", "fde68a"
        return "14532d", "86efac"

    def _fr(v):  return "—" if pd.isna(v) else f"{v*100:+.1f}%"
    def _fp(v):  return "—" if pd.isna(v) else f"{v*100:.1f}%"
    def _fs(v):  return "—" if pd.isna(v) else f"{v:.2f}"

    # ── Sparkline image helper ────────────────────────────────────────────────

    def _sparkline_img(spark: list, rag_label: str, w_in=1.0, h_in=0.28) -> BytesIO | None:
        """Render a tiny sparkline as a PNG BytesIO for embedding in PPTX."""
        if not spark or len(spark) < 2:
            return None
        sp  = np.array(spark)
        col = "#3fb950" if sp[-1] >= 0 else "#f85149"
        fig2, ax2 = plt.subplots(figsize=(w_in, h_in), facecolor="#0d1117")
        ax2.set_facecolor("#0d1117")
        mn, mx = sp.min(), sp.max()
        rng    = mx - mn if mx != mn else 1.0
        xs     = np.arange(len(sp))
        zero_y = (0 - mn) / rng  # normalised 0-line position
        zero_y = np.clip(zero_y, 0, 1)
        sp_n   = (sp - mn) / rng
        ax2.fill_between(xs, zero_y, sp_n,
                         where=(sp_n >= zero_y), color="#3fb950", alpha=0.25, linewidth=0)
        ax2.fill_between(xs, zero_y, sp_n,
                         where=(sp_n < zero_y),  color="#f85149", alpha=0.25, linewidth=0)
        ax2.plot(xs, sp_n, color=col, linewidth=1.2)
        ax2.axhline(zero_y, color="#8b949e", linewidth=0.4, alpha=0.6)
        ax2.set_xlim(0, len(sp) - 1)
        ax2.set_ylim(-0.05, 1.05)
        ax2.axis("off")
        fig2.subplots_adjust(left=0, right=1, top=1, bottom=0)
        buf2 = BytesIO()
        fig2.savefig(buf2, format="png", dpi=120, bbox_inches="tight", facecolor="#0d1117")
        plt.close(fig2)
        buf2.seek(0)
        return buf2

    # Market open flag
    market_open = bool(df["market_open"].iloc[0]) if "market_open" in df.columns else True

    # ── Shared slide header ──────────────────────────────────────────────────

    def _slide_header(slide, title, subtitle):
        _set_bg(slide, "0d1117")
        _rect(slide, 0, 0, 13.33, 0.07, "58a6ff")
        _rect(slide, 0, 0.07, 13.33, 0.73, "161b22")
        _txbox(slide, title, 0.3, 0.10, 9.5, 0.44, size=20, bold=True, color="e6edf3")
        _txbox(slide, subtitle, 0.3, 0.52, 9.5, 0.26, size=10, color="8b949e")
        _txbox(slide, as_of, 10.2, 0.28, 2.9, 0.30,
               size=10, color="4b5563", align=PP_ALIGN.RIGHT)

    # ── Chart helpers ────────────────────────────────────────────────────────

    def _ytd_chart_img(ytd_slice: pd.DataFrame) -> BytesIO:
        """Render a YTD bar chart for a subset of instruments."""
        n = len(ytd_slice)
        fig_h = max(3.5, n * 0.22)
        fig, ax = plt.subplots(figsize=(12.5, fig_h), facecolor="#0d1117")
        ax.set_facecolor("#161b22")
        bar_colors = ["#f85149" if v < 0 else "#3fb950" for v in ytd_slice["ret_ytd"]]
        bars = ax.barh(ytd_slice["name"], ytd_slice["ret_ytd"] * 100,
                       color=bar_colors, edgecolor="none", height=0.72)
        for bar, val in zip(bars, ytd_slice["ret_ytd"] * 100):
            xpos = val + (0.4 if val >= 0 else -0.4)
            ax.text(xpos, bar.get_y() + bar.get_height() / 2,
                    f"{val:+.1f}%", va="center",
                    ha="left" if val >= 0 else "right",
                    color="#e6edf3", fontsize=9, fontweight="bold")
        ax.axvline(0, color="#8b949e", linewidth=0.8)
        ax.set_xlabel("YTD Return (%)", color="#8b949e", fontsize=10)
        ax.tick_params(colors="#8b949e", labelsize=9)
        for sp in ax.spines.values(): sp.set_edgecolor("#30363d")
        ax.grid(axis="x", color="#21262d", linewidth=0.5, alpha=0.7)
        fig.tight_layout(pad=0.5)
        buf = BytesIO()
        fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="#0d1117")
        plt.close(fig)
        buf.seek(0)
        return buf

    def _risk_scatter_img():
        from matplotlib.lines import Line2D
        plot_df = df.dropna(subset=["vol_20d", "sharpe"]).copy()
        fig, ax = plt.subplots(figsize=(12.5, 5.8), facecolor="#0d1117")
        ax.set_facecolor("#161b22")
        color_map = {"RED": "#f85149", "AMBER": "#e3b341", "GREEN": "#3fb950"}

        # Clip axes to keep chart readable — outliers plotted at boundary with annotation
        VOL_MAX   = 75.0   # cap x-axis at 75% vol
        SHARPE_MIN = -4.0  # cap y-axis at -4 Sharpe

        for _, row in plot_df.iterrows():
            rag = row["rag_label"].strip()
            c   = color_map.get(rag, "#8b949e")
            sz  = max(40, min(350, abs(row["max_dd"]) * 1800)) if not pd.isna(row["max_dd"]) else 60
            vol_x   = min(row["vol_20d"] * 100, VOL_MAX * 0.97)
            sharpe_y = max(row["sharpe"], SHARPE_MIN * 0.97)
            ax.scatter(vol_x, sharpe_y,
                       s=sz, color=c, alpha=0.75, edgecolors="#30363d", linewidths=0.5)
            ax.annotate(row["ticker"],
                        (vol_x, sharpe_y),
                        fontsize=7, color="#e6edf3", alpha=0.85,
                        xytext=(4, 4), textcoords="offset points")
        ax.axhline(0, color="#8b949e", linestyle="--", linewidth=0.8, alpha=0.5)
        ax.axhline(1, color="#3fb950", linestyle=":", linewidth=0.8, alpha=0.4)
        ax.set_xlim(0, VOL_MAX)
        ax.set_ylim(SHARPE_MIN, ax.get_ylim()[1])
        ax.set_xlabel("Volatility 20D Annualised (%)", color="#8b949e", fontsize=10)
        ax.set_ylabel("Sharpe Ratio (1Y)", color="#8b949e", fontsize=10)
        ax.tick_params(colors="#8b949e", labelsize=9)
        for sp in ax.spines.values(): sp.set_edgecolor("#30363d")
        ax.grid(color="#21262d", linewidth=0.5, alpha=0.6)
        legend_elements = [
            Line2D([0], [0], marker="o", color="w", markerfacecolor="#f85149",
                   markersize=9, label="RED  —  Max DD < −15%"),
            Line2D([0], [0], marker="o", color="w", markerfacecolor="#e3b341",
                   markersize=9, label="AMBER  —  Max DD −15% to −7%"),
            Line2D([0], [0], marker="o", color="w", markerfacecolor="#3fb950",
                   markersize=9, label="GREEN  —  Max DD > −7%"),
        ]
        ax.legend(handles=legend_elements, fontsize=9, framealpha=0.4,
                  facecolor="#0d1117", labelcolor="#e6edf3", loc="upper right")
        ax.text(0.01, 0.97, "Bubble size ∝ magnitude of Max Drawdown  ·  Extreme outliers clipped to axis bounds",
                transform=ax.transAxes, fontsize=8, color="#6b7280", va="top")
        fig.tight_layout(pad=0.4)
        buf = BytesIO()
        fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="#0d1117")
        plt.close(fig)
        buf.seek(0)
        return buf

    # ── Grouped returns/risk table ────────────────────────────────────────────

    def _add_grouped_table(slide, sec_keys, col_defs, spark_col_idx=None,
                           slide_title="", slide_subtitle="", rows_per_slide=10):
        """Paginated table — max rows_per_slide data rows per slide.
        First slide is pre-created and passed in; overflow slides auto-created.
        Row height 0.52", font size 11pt for clean readability.
        """
        ROW_H    = 0.52   # generous data row height
        COLHDR_H = 0.36   # column header row
        SECHDR_H = 0.30   # section divider rows

        # Build flat ordered list: ("hdr", label) | ("data", row)
        all_specs = []
        for sec in sec_keys:
            sub = df[df["section"] == sec]
            if sub.empty:
                continue
            all_specs.append(("hdr", SECTION_LABELS.get(sec, sec)))
            for _, row in sub.iterrows():
                all_specs.append(("data", row))

        # Paginate: split on data-row count, carry section header to next page
        pages        = []
        cur_page     = []
        data_count   = 0
        pending_hdr  = None

        for spec in all_specs:
            if spec[0] == "hdr":
                pending_hdr = spec
            else:
                if pending_hdr is not None:
                    cur_page.append(pending_hdr)
                    pending_hdr = None
                cur_page.append(spec)
                data_count += 1
                if data_count >= rows_per_slide:
                    pages.append(cur_page)
                    cur_page    = []
                    data_count  = 0
                    pending_hdr = None
        if cur_page:
            pages.append(cur_page)

        total_pages = len(pages)

        def _render_page(sl, page_specs, page_num):
            n_rows  = len(page_specs) + 1   # +1 for column header row
            n_cols  = len(col_defs)
            total_w = sum(c[1] for c in col_defs)

            # Heights per row type
            row_heights = [COLHDR_H]
            for rtype, _ in page_specs:
                row_heights.append(SECHDR_H if rtype == "hdr" else ROW_H)
            tbl_h = sum(row_heights)

            tbl = sl.shapes.add_table(
                n_rows, n_cols,
                Inches(0.18), Inches(0.88),
                Inches(total_w), Inches(tbl_h)
            ).table

            for ci, (_, cw, _, _) in enumerate(col_defs):
                tbl.columns[ci].width = Inches(cw)
            for ri, rh in enumerate(row_heights):
                tbl.rows[ri].height = Inches(rh)

            # Column header
            for ci, (hdr, _, ha, _) in enumerate(col_defs):
                _set_cell(tbl.cell(0, ci), hdr, bg_h="161b22", fg_h="58a6ff",
                          size=11, bold=True, align=ha)

            # Accumulated top offset for sparkline positioning
            row_top_offset = 0.88 + COLHDR_H

            for ri, (rtype, rdata) in enumerate(page_specs, start=1):
                rh = row_heights[ri]
                if rtype == "hdr":
                    for ci in range(n_cols):
                        txt = rdata if ci == 0 else ""
                        _set_cell(tbl.cell(ri, ci), txt, bg_h="1c2128", fg_h="58a6ff",
                                  size=10, bold=True, align=PP_ALIGN.LEFT)
                else:
                    for ci, (_, cw, ha, fn) in enumerate(col_defs):
                        if spark_col_idx is not None and ci == spark_col_idx:
                            _set_cell(tbl.cell(ri, ci), "", bg_h="0d1117", fg_h="0d1117", size=6)
                            buf_sp = _sparkline_img(
                                rdata.get("spark", []), rdata["rag_label"],
                                w_in=cw * 0.88, h_in=rh * 0.68
                            )
                            if buf_sp:
                                col_left = Inches(0.18 + sum(col_defs[k][1] for k in range(ci)))
                                sp_w_in  = cw * 0.84
                                sp_h_in  = rh * 0.64
                                sp_l = col_left + Inches((cw - sp_w_in) / 2)
                                sp_t = Inches(row_top_offset) + Inches((rh - sp_h_in) / 2)
                                sl.shapes.add_picture(buf_sp, sp_l, sp_t,
                                                      Inches(sp_w_in), Inches(sp_h_in))
                        else:
                            text, bg, fg = fn(rdata)
                            _set_cell(tbl.cell(ri, ci), text, bg_h=bg, fg_h=fg,
                                      size=11, align=ha)
                row_top_offset += rh

            # Page indicator
            if total_pages > 1:
                _txbox(sl, f"Page {page_num} of {total_pages}",
                       10.5, 7.20, 2.6, 0.22, size=9, color="4b5563",
                       align=PP_ALIGN.RIGHT)

        for pi, page_specs in enumerate(pages):
            if pi == 0:
                sl = slide
            else:
                sl = prs.slides.add_slide(blank)
                pg_sub = f"{slide_subtitle}  ·  {pi + 1}/{total_pages}"
                _slide_header(sl, slide_title, pg_sub)
            _render_page(sl, page_specs, pi + 1)

    # ════════════════════════════════════════════════════════════════════════
    #  SLIDE 1 — Cover
    # ════════════════════════════════════════════════════════════════════════
    slide = prs.slides.add_slide(blank)
    _set_bg(slide, "0d1117")
    _rect(slide, 0, 0, 13.33, 0.07, "58a6ff")

    _txbox(slide, "BK MARKET DASHBOARD", 0.7, 1.55, 12.0, 1.2,
           size=52, bold=True, color="e6edf3", align=PP_ALIGN.LEFT)
    _txbox(slide, f"Daily Brief  ·  {as_of}", 0.7, 2.85, 10.0, 0.6,
           size=22, color="8b949e")
    _txbox(slide,
           f"{N_INSTRUMENTS}-Instrument Universe  ·  10 Asset Classes  ·  Returns, Risk & Signal",
           0.7, 3.52, 11.5, 0.45, size=15, color="6b7280")

    n_red_t   = (df["rag_label"].str.strip() == "RED").sum()
    n_amber_t = (df["rag_label"].str.strip() == "AMBER").sum()
    n_green_t = (df["rag_label"].str.strip() == "GREEN").sum()

    # ── Market call narrative ──
    total = n_red_t + n_amber_t + n_green_t
    pct_red   = n_red_t   / total * 100 if total else 0
    pct_green = n_green_t / total * 100 if total else 0
    if pct_red >= 40:
        market_tone = "RISK-OFF"
        tone_color  = "f87171"
        tone_desc   = f"{n_red_t} instruments in drawdown >15% — broad risk-off conditions."
    elif pct_green >= 50:
        market_tone = "RISK-ON"
        tone_color  = "86efac"
        tone_desc   = f"{n_green_t} of {total} instruments within 7% of 52-week highs."
    else:
        market_tone = "MIXED"
        tone_color  = "fde68a"
        tone_desc   = f"Split signals: {n_red_t} RED · {n_amber_t} AMBER · {n_green_t} GREEN."

    _txbox(slide, f"Market Tone: {market_tone}", 0.7, 3.95, 12.0, 0.45,
           size=16, bold=True, color=tone_color)
    _txbox(slide, tone_desc, 0.7, 4.42, 12.0, 0.30, size=11, color="8b949e")

    for xi, (lbl, cnt, bg, fg) in enumerate([
        ("RED",   n_red_t,   "450a0a", "f87171"),
        ("AMBER", n_amber_t, "451a03", "fde68a"),
        ("GREEN", n_green_t, "052e16", "86efac"),
    ]):
        x = 0.7 + xi * 2.9
        _rect(slide, x, 4.90, 2.5, 1.4, bg)
        _txbox(slide, str(cnt), x, 4.92, 2.5, 0.85,
               size=42, bold=True, color=fg, align=PP_ALIGN.CENTER)
        _txbox(slide, lbl, x, 5.80, 2.5, 0.35,
               size=12, color=fg, align=PP_ALIGN.CENTER)

    _txbox(slide,
           "Signal: RED = Max DD < −15%   AMBER = −15% to −7%   GREEN = > −7% from 52-week high",
           0.7, 6.30, 12.0, 0.30, size=9, color="4b5563")
    _txbox(slide, "Source: Yahoo Finance  ·  Price return, local currency  ·  CONFIDENTIAL",
           0.7, 7.12, 12.0, 0.28, size=9, color="374151")

    # ════════════════════════════════════════════════════════════════════════
    #  SLIDES 2+ — Equities Returns (10 rows per slide)
    # ════════════════════════════════════════════════════════════════════════
    def _fv(v):
        """Format vol as XX.X%"""
        return "—" if pd.isna(v) else f"{v*100:.1f}%"

    def _cc_vol_chg(now, ago):
        """Color vol change arrow: red if vol rose, green if fell."""
        if pd.isna(now) or pd.isna(ago): return "21262d", "8b949e"
        return ("7f1d1d", "f87171") if now > ago else ("14532d", "86efac")

    def _vol_chg_str(now, ago):
        """Show vol now + arrow direction vs 1m ago."""
        if pd.isna(now) or pd.isna(ago): return "—"
        arrow = "▲" if now > ago else "▼"
        return f"{now*100:.1f}% {arrow}"

    EQ_TITLE    = "EQUITIES — RETURNS"
    EQ_SUBTITLE = "US Broad · US Sectors · Dev Mkts · EM · Defence  ·  Trend / 1W / 1M / 3M / YTD / Vol / Signal"
    slide = prs.slides.add_slide(blank)
    _slide_header(slide, EQ_TITLE, EQ_SUBTITLE)
    eq_cols = [
        ("Asset",   3.2, PP_ALIGN.LEFT,   lambda r: (r["name"],   "0d1117", "e6edf3")),
        ("Ticker",  0.9, PP_ALIGN.CENTER, lambda r: (r["ticker"], "161b22", "8b949e")),
        ("Trend",   1.4, PP_ALIGN.CENTER, lambda r: ("",          "0d1117", "0d1117")),
    ]
    if market_open:
        eq_cols.append(("1D", 1.0, PP_ALIGN.CENTER,
                        lambda r: (_fr(r["ret_1d"]), *_cc_ret(r["ret_1d"]))))
    eq_cols += [
        ("1W",      1.1, PP_ALIGN.CENTER, lambda r: (_fr(r["ret_1w"]),  *_cc_ret(r["ret_1w"]))),
        ("1M",      1.1, PP_ALIGN.CENTER, lambda r: (_fr(r["ret_1m"]),  *_cc_ret(r["ret_1m"]))),
        ("3M",      1.1, PP_ALIGN.CENTER, lambda r: (_fr(r["ret_3m"]),  *_cc_ret(r["ret_3m"]))),
        ("YTD",     1.2, PP_ALIGN.CENTER, lambda r: (_fr(r["ret_ytd"]), *_cc_ret(r["ret_ytd"]))),
        ("Vol (vs 1M)", 1.4, PP_ALIGN.CENTER,
         lambda r: (_vol_chg_str(r["vol_now"], r["vol_1m_ago"]),
                    *_cc_vol_chg(r["vol_now"], r["vol_1m_ago"]))),
        ("Signal",  1.3, PP_ALIGN.CENTER,
         lambda r: (r["rag_label"].strip(), *_cc_rag(r["rag_label"]))),
    ]
    _add_grouped_table(slide, ["EQ_US", "EQ_SECT", "EQ_DM", "EQ_EM", "DEFENCE"],
                       eq_cols, spark_col_idx=2,
                       slide_title=EQ_TITLE, slide_subtitle=EQ_SUBTITLE,
                       rows_per_slide=10)

    # ════════════════════════════════════════════════════════════════════════
    #  SLIDES — Fixed Income, Commodities & Other (10 rows per slide)
    # ════════════════════════════════════════════════════════════════════════
    FI_TITLE    = "FIXED INCOME, COMMODITIES & OTHER — RETURNS"
    FI_SUBTITLE = "Fixed Income & Credit · Commodities · Crypto · FX · Volatility  ·  Trend / 1W / 1M / 3M / YTD / Vol / Signal"
    slide = prs.slides.add_slide(blank)
    _slide_header(slide, FI_TITLE, FI_SUBTITLE)
    fi_cols = [
        ("Asset",   3.2, PP_ALIGN.LEFT,   lambda r: (r["name"],   "0d1117", "e6edf3")),
        ("Ticker",  0.9, PP_ALIGN.CENTER, lambda r: (r["ticker"], "161b22", "8b949e")),
        ("Trend",   1.4, PP_ALIGN.CENTER, lambda r: ("",          "0d1117", "0d1117")),
    ]
    if market_open:
        fi_cols.append(("1D", 1.0, PP_ALIGN.CENTER,
                        lambda r: (_fr(r["ret_1d"]), *_cc_ret(r["ret_1d"]))))
    fi_cols += [
        ("1W",      1.1, PP_ALIGN.CENTER, lambda r: (_fr(r["ret_1w"]),  *_cc_ret(r["ret_1w"]))),
        ("1M",      1.1, PP_ALIGN.CENTER, lambda r: (_fr(r["ret_1m"]),  *_cc_ret(r["ret_1m"]))),
        ("3M",      1.1, PP_ALIGN.CENTER, lambda r: (_fr(r["ret_3m"]),  *_cc_ret(r["ret_3m"]))),
        ("YTD",     1.2, PP_ALIGN.CENTER, lambda r: (_fr(r["ret_ytd"]), *_cc_ret(r["ret_ytd"]))),
        ("Vol (vs 1M)", 1.4, PP_ALIGN.CENTER,
         lambda r: (_vol_chg_str(r["vol_now"], r["vol_1m_ago"]),
                    *_cc_vol_chg(r["vol_now"], r["vol_1m_ago"]))),
        ("Signal",  1.3, PP_ALIGN.CENTER,
         lambda r: (r["rag_label"].strip(), *_cc_rag(r["rag_label"]))),
    ]
    _add_grouped_table(slide, ["FI", "CMD", "CRYPTO", "FX", "VOL"],
                       fi_cols, spark_col_idx=2,
                       slide_title=FI_TITLE, slide_subtitle=FI_SUBTITLE,
                       rows_per_slide=10)

    # ════════════════════════════════════════════════════════════════════════
    #  SLIDE 4 — Risk Snapshot (scatter: vol vs Sharpe, all instruments)
    # ════════════════════════════════════════════════════════════════════════
    slide = prs.slides.add_slide(blank)
    _slide_header(slide, "RISK SNAPSHOT — ALL INSTRUMENTS",
                  "Volatility (20D ann.) vs Sharpe Ratio (1Y)  ·  "
                  "Bubble size = Max Drawdown magnitude  ·  Colour = Signal")
    buf = _risk_scatter_img()
    if buf:
        slide.shapes.add_picture(buf, Inches(0.2), Inches(0.88),
                                 Inches(12.93), Inches(6.40))

    # ════════════════════════════════════════════════════════════════════════
    #  SLIDES — YTD Performance (15 instruments per slide, sorted best→worst)
    # ════════════════════════════════════════════════════════════════════════
    YTD_PER_SLIDE = 15
    ytd_all = df[["name", "ret_ytd"]].dropna(subset=["ret_ytd"]).copy()
    ytd_all = ytd_all.sort_values("ret_ytd", ascending=False).reset_index(drop=True)
    ytd_pages = [ytd_all.iloc[i:i + YTD_PER_SLIDE]
                 for i in range(0, len(ytd_all), YTD_PER_SLIDE)]
    ytd_total = len(ytd_pages)
    for pi, ytd_slice in enumerate(ytd_pages):
        # Sort slice bottom-to-top for horizontal bar chart
        ytd_slice = ytd_slice.sort_values("ret_ytd").reset_index(drop=True)
        slide = prs.slides.add_slide(blank)
        pg_lbl = f"  ·  {pi + 1}/{ytd_total}" if ytd_total > 1 else ""
        _slide_header(slide,
                      "YTD PERFORMANCE — ALL INSTRUMENTS",
                      f"Year-to-Date Return  ·  Sorted best to worst{pg_lbl}")
        buf = _ytd_chart_img(ytd_slice)
        slide.shapes.add_picture(buf, Inches(0.2), Inches(0.88),
                                 Inches(12.93), Inches(6.40))

    # ── Save ─────────────────────────────────────────────────────────────────
    os.makedirs(out_dir, exist_ok=True)
    tag  = datetime.now().strftime("%Y%m%d_%H%M")
    path = os.path.join(out_dir, f"market_dashboard_{tag}.pptx")
    prs.save(path)
    print(f"[PPTX]   {path}")
    return path


# ══════════════════════════════════════════════════════════════════════════════
#  ORCHESTRATION
# ══════════════════════════════════════════════════════════════════════════════

def _now_sgt() -> str:
    return datetime.now(SGT).strftime("%H:%M:%S SGT")


def run_once(send_email_flag: bool = False, pptx_flag: bool = False,
             html_flag: bool = False, out_dir: str = OUT_DIR,
             lookback_days: int = 2520) -> None:
    print("=" * 60)
    print(f"  BK Market Dashboard  |  {_now_sgt()}")
    print("=" * 60)

    prices = download(lookback_days=lookback_days)
    df     = compute_metrics(prices)

    n_red   = (df["rag_label"].str.strip() == "RED").sum()
    n_amber = (df["rag_label"].str.strip() == "AMBER").sum()
    n_green = (df["rag_label"].str.strip() == "GREEN").sum()
    print(f"[Signal]  RED={n_red}  AMBER={n_amber}  GREEN={n_green}  ({len(df)}/{N_INSTRUMENTS} fetched)")

    as_of    = datetime.now().strftime("%d %B %Y  %H:%M")
    png, pdf = render_report(df, as_of, out_dir)
    print(f"[Report]  PNG: {png}")
    print(f"[Report]  PDF: {pdf}")

    if pptx_flag:
        render_pptx(df, prices, as_of, out_dir)

    if html_flag:
        print("[HTML]   Computing fragility scores...")
        frag_df     = compute_fragility(prices)
        print("[HTML]   Computing market regime...")
        regime_data = compute_regime(prices)
        print("[HTML]   Computing Fear & Greed index...")
        fg_data     = compute_fear_greed(prices)
        print("[HTML]   Computing fragility trend...")
        frag_trend  = compute_fragility_trend(prices)
        web_html    = build_web_html(df, frag_df, prices, regime_data, fg_data, frag_trend)
        docs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "docs")
        os.makedirs(docs_dir, exist_ok=True)
        html_path = os.path.join(docs_dir, "index.html")
        with open(html_path, "w", encoding="utf-8") as fh:
            fh.write(web_html)
        print(f"[HTML]   {html_path}")

    if send_email_flag:
        html = build_email_html(df)
        send_email(html)

    print("[Done]")


def run_scheduler(out_dir: str = OUT_DIR) -> None:
    print("=" * 60)
    print("  BK Market Dashboard · Daily Scheduler")
    print(f"  Send time : {SEND_TIME_SGT} SGT  (Mon–Fri)")
    print(f"  Recipient : {RECIPIENT_EMAIL}")
    print(f"  Output    : {os.path.abspath(out_dir)}")
    print("=" * 60)

    job = lambda: run_once(send_email_flag=True, out_dir=out_dir)
    for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
        getattr(schedule.every(), day).at(SEND_TIME_SGT).do(job)

    print(f"\n[{_now_sgt()}] Scheduler running. Next run: {schedule.next_run()}")
    print("  Press Ctrl+C to stop.\n")

    while True:
        schedule.run_pending()
        time.sleep(30)


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="BK Market Dashboard — visual report + email brief",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python bk_market_dashboard.py                   # PNG + PDF only
  python bk_market_dashboard.py --pptx            # + PowerPoint deck
  python bk_market_dashboard.py --email           # + send HTML email
  python bk_market_dashboard.py --pptx --email    # all outputs
  python bk_market_dashboard.py --schedule        # daily scheduler at 07:00 SGT
  python bk_market_dashboard.py --now --pptx      # run immediately with PPTX
        """,
    )
    parser.add_argument("--email",    action="store_true",
                        help="Send HTML email after generating report")
    parser.add_argument("--pptx",     action="store_true",
                        help="Generate PowerPoint deck (5 slides per asset class)")
    parser.add_argument("--html",     action="store_true",
                        help="Generate docs/index.html for GitHub Pages")
    parser.add_argument("--lookback",  type=int, default=2520, metavar="DAYS",
                        help="Price history lookback in days (default: 2520 = 10yr, use 504 for 2yr)")
    parser.add_argument("--schedule", action="store_true",
                        help=f"Start daily scheduler at {SEND_TIME_SGT} SGT Mon–Fri")
    parser.add_argument("--now",      action="store_true",
                        help="Run once immediately (bypasses scheduler wait)")
    parser.add_argument("--out-dir",  default=OUT_DIR, metavar="DIR",
                        help=f"Output directory for outputs (default: {OUT_DIR}/)")
    args = parser.parse_args()

    if args.schedule and not args.now:
        run_scheduler(out_dir=args.out_dir)
    else:
        run_once(send_email_flag=args.email, pptx_flag=args.pptx,
                 html_flag=args.html, out_dir=args.out_dir,
                 lookback_days=args.lookback)


# ── BACKGROUND EXECUTION ──────────────────────────────────────────────────────
# Mac/Linux:  nohup python bk_market_dashboard.py --schedule &
#             (logs to nohup.out)
# Windows:    pythonw bk_market_dashboard.py --schedule
# Test now:   python bk_market_dashboard.py --now --email
