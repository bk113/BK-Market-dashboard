"""
BK Market Dashboard — Consolidated
====================================
47-instrument universe · Returns, risk metrics, visual report & email brief.

Usage:
  python bk_market_dashboard.py                    # PNG + PDF report only
  python bk_market_dashboard.py --pptx             # PowerPoint deck (5 slides per asset class)
  python bk_market_dashboard.py --email            # report + send email
  python bk_market_dashboard.py --schedule         # daily scheduler at 07:00 SGT Mon–Fri
  python bk_market_dashboard.py --now --email      # run once immediately (testing)

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
    ("EQ_IDX",   "EXW1.DE", "Euro STOXX 50"),
    ("EQ_IDX",   "ISF.L",   "FTSE 100"),
    ("EQ_IDX",   "^N225",   "Nikkei 225"),
    ("EQ_IDX",   "^HSI",    "Hang Seng Index"),
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
    ("FI_INTL",  "LQDE.L",  "EUR IG Credit"),
    ("FI_INTL",  "IHYG.L",  "EUR HY Credit"),
    ("FI_INTL",  "AGGG.L",  "Global Aggregate"),
    # ── Crypto ──
    ("CRYPTO",   "BTC-USD", "Bitcoin"),
    # ── FX ──
    ("FX",       "UUP",     "US Dollar Index"),
    ("FX",       "EURUSD=X","EUR/USD"),
    ("FX",       "GBPUSD=X","GBP/USD"),
    ("FX",       "JPY=X",   "USD/JPY"),
    ("FX",       "SGD=X",   "USD/SGD"),
    ("FX",       "CNH=X",   "USD/CNH"),
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
#  DATA
# ══════════════════════════════════════════════════════════════════════════════

def download(lookback_days: int = 420) -> pd.DataFrame:
    tickers = [t for _, t, _ in UNIVERSE]
    print(f"[Download] {len(tickers)} tickers | last {lookback_days} days ...")
    start = (pd.Timestamp.today() - pd.Timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    raw = yf.download(tickers, start=start, auto_adjust=True, progress=False)
    if raw.empty:
        raise RuntimeError("No data returned from Yahoo Finance.")

    prices = raw["Close"] if "Close" in raw.columns else raw.xs("Close", axis=1, level=0)
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
    sharpe       = (ann_ret_1y - RISK_FREE_RATE) / vol_1y.replace(0, np.nan)

    # Max drawdown from 252-day rolling peak
    window  = min(252, len(prices))
    peak    = prices.tail(window).cummax()
    max_dd  = prices.iloc[-1] / peak.iloc[-1] - 1

    # Sparkline data: last 20 trading days, normalised to first value
    spark_window = min(20, len(prices))
    spark_prices = prices.tail(spark_window)

    # Detect if market was closed today (all 1D returns are ~0)
    ret1d_vals = _ret(1)
    market_open_today = not (ret1d_vals.abs().dropna() < 1e-6).all()

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
          {N_INSTRUMENTS}-INSTRUMENT UNIVERSE &nbsp;·&nbsp; RETURNS &amp; RISK
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

    zw      = min(252, len(prices) - 1)
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
        sc  = 100.0 * _frag_logistic(lat.ewm(span=10,adjust=False).mean())
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

def build_web_html(df: pd.DataFrame, frag_df: pd.DataFrame = None) -> str:
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
            cl="ps" if p>=2 else "pl" if p>=0.5 else "ng" if p>=-2 else "nr"
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

    def _srow(sec, cs=12):
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
    pr=""; pv=None
    for _,row in df.iterrows():
        if row["section"]!=pv: pv=row["section"]; pr+=_srow(row["section"])
        d1=_rc(row["ret_1d"]) if market_open else ""
        pr+=(f'<tr><td class="an">{row["name"]}</td><td class="tk">{row["ticker"]}</td>'
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
          f'<div class="tw"><table><thead><tr><th style="text-align:left;">Asset</th><th>Ticker</th>'
          f'{d1th}<th>1W</th><th>1M</th><th>3M</th><th>YTD</th><th>Signal</th>'
          f'</tr></thead><tbody>{pr}</tbody></table></div>')

    # ══ TAB 2: RISK ═══════════════════════════════════════════════════════════
    def _varrow(now_v, ago_v):
        if pd.isna(now_v) or pd.isna(ago_v) or ago_v==0: return "gr","&#8594;","-"
        chg=(now_v-ago_v)/ago_v; pct=f"{chg*100:+.1f}%"
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

        frag=(f'<div style="display:grid;grid-template-columns:auto 1fr 1fr 1fr 1fr;gap:14px;margin-bottom:14px;align-items:stretch;">'
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
         ".tabs{display:flex;gap:4px;margin-bottom:14px;border-bottom:2px solid var(--br);}"
         ".tb{padding:10px 22px;font-size:12px;font-weight:600;font-family:monospace;letter-spacing:1px;"
         "border:none;background:transparent;color:var(--g);cursor:pointer;"
         "border-bottom:3px solid transparent;margin-bottom:-2px;text-transform:uppercase;}"
         ".tb:hover{color:var(--w);}"
         ".tb.on{color:var(--ac);border-bottom:3px solid var(--ac);}"
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
         ".tb{padding:8px 12px;font-size:10px;}.rn{font-size:16px;}}")

    return (
        "<!DOCTYPE html><html lang='en'><head>"
        "<meta charset='UTF-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        "<meta http-equiv='refresh' content='3600'>"
        "<title>BK Market Dashboard</title>"
        f"<script async src='https://www.googletagmanager.com/gtag/js?id={GA}'></script>"
        f"<script>window.dataLayer=window.dataLayer||[];function gtag(){{dataLayer.push(arguments);}}"
        f"gtag('js',new Date());gtag('config','{GA}');</script>"
        f"<style>{css}</style>"
        "</head><body><div class='wrap'>"
        "<div class='hdr'><div>"
        "<div class='logo'>BK <span>MARKET</span> DASHBOARD</div>"
        f"<div class='sub'>{N_INSTRUMENTS}-INSTRUMENT UNIVERSE &nbsp;&#183;&nbsp; PERFORMANCE &#183; RISK &#183; FRAGILITY</div>"
        f"<div class='badge'><span class='dot'></span> Last updated: {date_str}</div>"
        "</div><div style='text-align:right;'>"
        f"<div style='font-family:monospace;font-size:13px;color:#e6edf3;font-weight:600;'>{date_str}</div>"
        f"<div style='font-size:9px;color:#8b949e;margin-top:4px;'>Auto-refreshes every hour{mn}</div>"
        "<div style='font-size:9px;color:#444d56;margin-top:3px;font-family:monospace;'>07:00 SGT &#183; MON&#8211;FRI</div>"
        "</div></div>"
        "<div class='tabs'>"
        "<button class='tb on' onclick=\"sw('perf',this)\">&#128200; Performance</button>"
        "<button class='tb' onclick=\"sw('risk',this)\">&#128202; Risk</button>"
        "<button class='tb' onclick=\"sw('frag',this)\">&#9889; Fragility</button>"
        "</div>"
        f"<div id='t-perf' class='tab on'>{perf}</div>"
        f"<div id='t-risk' class='tab'>{risk}</div>"
        f"<div id='t-frag' class='tab'>{frag}</div>"
        "<div class='footer'><div class='fn'>"
        "Signal: RED &lt; &#8722;15% &#183; AMBER &#8722;15% to &#8722;7% &#183; GREEN &gt; &#8722;7% from 52-week high<br>"
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
             html_flag: bool = False, out_dir: str = OUT_DIR) -> None:
    print("=" * 60)
    print(f"  BK Market Dashboard  |  {_now_sgt()}")
    print("=" * 60)

    prices = download()
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
        web_html = build_web_html(df)
        docs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "docs")
        os.makedirs(docs_dir, exist_ok=True)
        html_path = os.path.join(docs_dir, "index.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(web_html)
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
                 html_flag=args.html, out_dir=args.out_dir)


# ── BACKGROUND EXECUTION ──────────────────────────────────────────────────────
# Mac/Linux:  nohup python bk_market_dashboard.py --schedule &
#             (logs to nohup.out)
# Windows:    pythonw bk_market_dashboard.py --schedule
# Test now:   python bk_market_dashboard.py --now --email
