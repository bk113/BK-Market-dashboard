"""
BK/IQ — AI Pulse data refresh.

Pulls a fixed AI-exposure basket via yfinance, computes simple, auditable
metrics, groups them by value-chain layer, generates one short cross-layer
synthesis paragraph via an LLM (Anthropic API), and writes
docs/data/ai_pulse.json for the static page (docs/ai.html) to read.

Run daily by .github/workflows/update-ai-pulse.yml.

DESIGN NOTE on the LLM commentary (added 2026-09-07): everything on this
page except one paragraph is deterministic arithmetic (means, breadth
counts) -- fully auditable, no model involved. The ONE place an LLM is used
is `build_synthesis()`, which writes a short cross-layer "what does today's
pattern actually look like" paragraph -- the one thing pure stats doesn't
do well. Per-layer blurbs are template-generated from the numbers, not the
LLM, to keep the bulk of the page mechanically checkable. The synthesis is
explicitly labelled "AI-generated" on the page with a standing disclaimer,
consistent with this site's existing "observational, not predictive"
compliance stance (see docs/index.html About tab, and the
git history's "Compliance — de-LLMed" pass on other tabs) -- the system
prompt below enforces the same constraint: descriptive only, no calls, no
recommendations, no predictions.

If ANTHROPIC_API_KEY / ANTHROPIC_MODEL aren't set, or the call fails for any
reason, this falls back to a deterministic templated synthesis sentence --
the daily job never hard-fails over the LLM step, and the page always shows
something, clearly labelled either way.
"""

import json
import os
import datetime as dt
import numpy as np
import yfinance as yf

# Fixed basket across the AI value chain. Each entry maps to (layer, primary
# ETF proxy) so the page can show why a name sits where it does — a single
# name can belong to a layer for one reason (revenue) while quietly
# competing in another (e.g. GOOGL's TPUs vs. NVDA). Edit this dict to
# change coverage; the page requires no other changes. Insertion order here
# also drives the page's layer display order (value-chain order).
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

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "").strip()

SYNTHESIS_SYSTEM_PROMPT = """You write ONE short paragraph (3-5 sentences, plain text, no markdown, no bullet points) summarising today's AI-markets basket data for a personal, non-commercial research page.

Hard rules, no exceptions:
- Descriptive only. Report what the numbers show. Never predict, forecast, or imply what will happen next.
- Never recommend a trade, position, or action of any kind. No "consider", "watch for an entry", "may be a good time to", etc.
- Never state or imply causation you were not given. If several layers moved together, you may note the co-movement; do not assert why beyond what's in the data provided.
- Only reference numbers and layer names given to you in the input. Do not invent figures, news, or events.
- If the data is mixed or inconclusive, say so plainly rather than forcing a narrative.
- Plain, direct, analyst-notebook tone. No hype, no superlatives ("massive", "surging", "crashing") unless the actual magnitude in the data warrants a neutral factual descriptor.
- Output ONLY the paragraph. No preamble, no title, no quotes around it."""


def pct(a, b):
    if b in (0, None) or a is None:
        return None
    return round((a / b - 1) * 100, 2)


def _mean(vals):
    vals = [v for v in vals if v is not None]
    return round(float(np.mean(vals)), 2) if vals else None


def build_layers(rows):
    """Group per-name rows into layer-level aggregates, in BASKET's
    insertion order (first-seen layer order = value-chain display order)."""
    layer_order = []
    for t in BASKET:
        layer = BASKET[t][0]
        if layer not in layer_order:
            layer_order.append(layer)

    layers = []
    for layer in layer_order:
        members = [r for r in rows if r["layer"] == layer]
        if not members:
            continue
        equity_members = [r for r in members if r["ticker"] not in CREDIT_TICKERS]
        breadth = (
            round(100 * sum(1 for r in equity_members if r["above_50dma"]) / len(equity_members), 1)
            if equity_members else None
        )
        layers.append({
            "name": layer,
            "tickers": [r["ticker"] for r in members],
            "avg_chg_1d": _mean([r["chg_1d"] for r in members]),
            "avg_chg_1m": _mean([r["chg_1m"] for r in members]),
            "avg_chg_3m": _mean([r["chg_3m"] for r in members]),
            "avg_vol_21d_ann": _mean([r["vol_21d_ann"] for r in members]),
            "breadth_above_50dma_pct": breadth,
            # Template sentence, NOT the LLM -- deterministic, auditable.
            "blurb": _template_layer_blurb(layer, members, breadth),
        })
    return layers


def _template_layer_blurb(layer, members, breadth):
    names = ", ".join(r["ticker"] for r in members)
    avg1m = _mean([r["chg_1m"] for r in members])
    direction = "up" if (avg1m or 0) >= 0 else "down"
    breadth_txt = f", {breadth}% above their 50-day average" if breadth is not None else ""
    avg1m_txt = f"{avg1m:+.1f}%" if avg1m is not None else "n/a"
    return f"{len(members)} name{'s' if len(members) != 1 else ''} ({names}), {direction} {avg1m_txt} over 1M{breadth_txt}."


def build_synthesis(basket_breadth, basket_vol, layers, rows):
    """Returns (text, generated_by) -- generated_by is 'llm' or 'fallback'."""
    movers = sorted(
        [r for r in rows if r["chg_1d"] is not None],
        key=lambda r: abs(r["chg_1d"]), reverse=True,
    )[:3]

    if not ANTHROPIC_API_KEY or not ANTHROPIC_MODEL:
        return (
            "AI-generated synthesis is not configured for this run "
            "(ANTHROPIC_API_KEY / ANTHROPIC_MODEL not set) -- showing raw "
            "figures only below.",
            "fallback",
        )

    payload = {
        "basket_breadth_above_50dma_pct": basket_breadth,
        "basket_avg_ann_vol_21d_pct": basket_vol,
        "layers": [
            {
                "name": l["name"],
                "avg_chg_1d": l["avg_chg_1d"],
                "avg_chg_1m": l["avg_chg_1m"],
                "avg_chg_3m": l["avg_chg_3m"],
                "avg_vol_21d_ann": l["avg_vol_21d_ann"],
                "breadth_above_50dma_pct": l["breadth_above_50dma_pct"],
            }
            for l in layers
        ],
        "largest_1d_movers": [
            {"ticker": r["ticker"], "layer": r["layer"], "chg_1d": r["chg_1d"]}
            for r in movers
        ],
    }

    try:
        import requests
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": ANTHROPIC_MODEL,
                "max_tokens": 300,
                "system": SYNTHESIS_SYSTEM_PROMPT,
                "messages": [
                    {"role": "user", "content": json.dumps(payload, indent=2)}
                ],
            },
            timeout=30,
        )
        resp.raise_for_status()
        text = resp.json()["content"][0]["text"].strip()
        if text:
            return text, "llm"
    except Exception as e:
        print(f"WARNING: LLM synthesis call failed, using fallback: {e}")

    return (
        "AI-generated synthesis unavailable this run (API call failed) -- "
        "showing raw figures only below.",
        "fallback",
    )


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

    layers = build_layers(rows)
    synthesis_text, generated_by = build_synthesis(breadth, ann_vol, layers, rows)

    out = {
        "as_of": dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "basket_breadth_above_50dma_pct": breadth,
        "basket_avg_ann_vol_21d_pct": ann_vol,
        "layers": layers,
        "commentary": {
            "synthesis": synthesis_text,
            "generated_by": generated_by,   # "llm" or "fallback"
            "model": ANTHROPIC_MODEL if generated_by == "llm" else None,
            "disclaimer": (
                "AI-generated summary of the numbers on this page only. "
                "Descriptive, not a forecast, recommendation, or investment advice."
            ),
        },
        "constituents": sorted(rows, key=lambda r: r["layer"]),
        "methodology": (
            "1M/3M returns are trailing 21/63 trading-day price changes. "
            "Vol (per name) = annualised stdev of that name's own trailing "
            "21-day daily returns. Basket vol = same calculation pooled "
            "across the equity subset. Breadth = % of equity subset trading "
            "above its own 50-day moving average. Drawdown = max "
            "peak-to-trough over trailing 90 sessions per name. Layer "
            "aggregates are simple means across that layer's names. Only "
            "the synthesis paragraph above is LLM-generated; every other "
            "figure on this page is direct arithmetic. Descriptive only; "
            "not a signal or recommendation."
        ),
    }

    # NOTE: this repo publishes GitHub Pages from /docs, so the JSON has to
    # live under docs/data/ to actually be reachable by the deployed ai.html.
    with open("docs/data/ai_pulse.json", "w") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote docs/data/ai_pulse.json — {len(rows)} names, {len(layers)} layers, "
          f"breadth {breadth}%, synthesis via {generated_by}")


if __name__ == "__main__":
    main()
