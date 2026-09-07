"""
BK/IQ — AI Pulse data refresh.

Pulls a fixed AI-exposure basket via yfinance (same public-data policy as the
rest of the site), computes simple, auditable metrics, and writes
data/ai_pulse.json for the static page to read.

Run daily by .github/workflows/update-ai-pulse.yml — this is the
"autorefresh": GitHub Actions runs on schedule, commits the new JSON,
GitHub Pages serves the updated file. No server needed.
"""

import json
import datetime as dt
import numpy as np
import yfinance as yf

# Fixed basket across the AI value chain. Each entry maps to (layer, primary
# ETF proxy) so the page can show why a name sits where it does — a single
# name can belong to a layer for one reason (revenue) while quietly
# competing in another (e.g. GOOGL's TPUs vs. NVDA). Edit this dict to
# change coverage; the page requires no other changes.
BASKET = {
    "NVDA": ("AI Compute", "SOXX"),
    "AVGO": ("AI Compute", "SOXX"),
    "AMD": ("AI Compute", "SOXX"),
    "MRVL": ("Custom Silicon", "SOXX"),
    "TSM": ("Foundry", "SOXX"),
    "MU": ("Memory", "SOXX"),
    "MSFT": ("Hyperscaler", "XLK"),
    "GOOGL": ("Hyperscaler", "XLK"),
    "AMZN": ("Hyperscaler", "XLY"),
    "CRM": ("AI Application", "IGV"),
    "CRWD": ("AI Application", "IGV"),
    "WDAY": ("AI Application", "IGV"),
    "VRT": ("Power / Infra", "XLI"),
    "ORCL": ("Cloud Compute", "XLK"),
    "CRWV": ("AI Cloud Infra (public)", "—"),
    "LQD": ("Credit — IG", "—"),
    "HYG": ("Credit — HY", "—"),
    "BABA": ("China AI / Low-Cost Models", "KWEB"),
    "BIDU": ("China AI / Low-Cost Models", "KWEB"),
}

LOOKBACK_DAYS = 200  # enough for 3M return + 90D drawdown + 21D vol + 50D MA

# Credit ETFs are included for the fixed-income view but excluded from the
# equity breadth/vol aggregate so that stat stays a read on the AI-equity
# basket specifically, not diluted by bond price behaviour.
CREDIT_TICKERS = {"LQD", "HYG"}


def pct(a, b):
    if b in (0, None) or a is None:
        return None
    return round((a / b - 1) * 100, 2)


def main():
    tickers = list(BASKET.keys())
    raw = yf.download(
        tickers, period=f"{LOOKBACK_DAYS}d", interval="1d",
        auto_adjust=True, progress=False, group_by="ticker",
    )

    rows = []
    uptrend_count = 0
    all_daily_returns = []

    for t in tickers:
        try:
            closes = raw[t]["Close"].dropna()
        except Exception:
            continue
        if len(closes) < 60:
            continue

        last = float(closes.iloc[-1])
        d1 = pct(last, float(closes.iloc[-2])) if len(closes) >= 2 else None
        d1m = pct(last, float(closes.iloc[-22])) if len(closes) >= 22 else None
        d3m = pct(last, float(closes.iloc[-64])) if len(closes) >= 64 else None

        ma50 = float(closes.tail(50).mean())
        in_uptrend = last > ma50
        if in_uptrend and t not in CREDIT_TICKERS:
            uptrend_count += 1

        daily_ret = closes.pct_change().dropna()
        if t not in CREDIT_TICKERS:
            all_daily_returns.append(daily_ret.tail(21))

        # Per-name risk: own 21D annualised vol, not just the basket-pooled
        # figure, so each row carries its own risk read.
        own_vol_21d = float(daily_ret.tail(21).std() * np.sqrt(252) * 100) if len(daily_ret) >= 21 else None

        window = closes.tail(90)
        running_max = window.cummax()
        drawdown = float(((window - running_max) / running_max).min() * 100)

        layer, etf = BASKET[t]
        rows.append({
            "ticker": t,
            "layer": layer,
            "etf": etf,
            "last": round(last, 2),
            "chg_1d": d1,
            "chg_1m": d1m,
            "chg_3m": d3m,
            "vol_21d_ann": round(own_vol_21d, 2) if own_vol_21d is not None else None,
            "above_50dma": bool(in_uptrend),
            "drawdown_90d": round(drawdown, 2),
        })

    equity_rows = [r for r in rows if r["ticker"] not in CREDIT_TICKERS]
    breadth = round(100 * uptrend_count / len(equity_rows), 1) if equity_rows else None

    if all_daily_returns:
        combined = np.concatenate([r.values for r in all_daily_returns])
        ann_vol = round(float(np.std(combined) * np.sqrt(252) * 100), 2)
    else:
        ann_vol = None

    out = {
        "as_of": dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "basket_breadth_above_50dma_pct": breadth,
        "basket_avg_ann_vol_21d_pct": ann_vol,
        "constituents": sorted(rows, key=lambda r: r["layer"]),
        "methodology": (
            "1M/3M returns are trailing 21/63 trading-day price changes. "
            "Vol (per name) = annualised stdev of that name's own trailing "
            "21-day daily returns. Basket vol = same calculation pooled "
            "across the equity subset. Breadth = % of equity subset trading "
            "above its own 50-day moving average. Drawdown = max "
            "peak-to-trough over trailing 90 sessions per name. Descriptive "
            "only; not a signal or recommendation."
        ),
    }

    # NOTE: this repo publishes GitHub Pages from /docs (see docs/CNAME =
    # dashboard.bkiqmarkets.com), not repo root — so the JSON has to live
    # under docs/data/ to actually be reachable by the deployed ai.html.
    with open("docs/data/ai_pulse.json", "w") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote docs/data/ai_pulse.json — {len(rows)} names, breadth {breadth}%")


if __name__ == "__main__":
    main()
