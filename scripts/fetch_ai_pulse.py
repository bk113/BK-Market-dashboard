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
reason, this falls back to `_template_synthesis()` -- a deterministic,
templated reflection computed directly from the numbers already on the page
(the day's largest single-name move, the widest cross-layer 1M divergence,
the breadth spread between the strongest and weakest layer). Not equivalent
to the LLM's synthesis, but a real, auditable statement rather than a
placeholder apology -- the daily job never hard-fails over the LLM step, and
the page always shows something substantive, clearly labelled either way.
"""

import json
import os
import datetime as dt
import numpy as np
import pandas as pd
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
    "005930.KS": ("Memory", "—"),   # Samsung Electronics -- HBM co-leader w/ SK Hynix
    "000660.KS": ("Memory", "—"),   # SK Hynix -- HBM co-leader, Nvidia's primary supplier
    "MSFT": ("Hyperscaler", "XLK"),
    "GOOGL": ("Hyperscaler", "XLK"),
    "AMZN": ("Hyperscaler", "XLY"),
    "META": ("Hyperscaler", "XLC"),
    "ANET": ("Networking", "XLK"),
    "CRM": ("AI Application", "IGV"),
    "CRWD": ("AI Application", "IGV"),
    "WDAY": ("AI Application", "IGV"),
    "PLTR": ("AI Application", "IGV"),
    "VRT": ("Power / Infra", "XLI"),
    "CEG": ("Power / Infra", "XLU"),
    "GEV": ("Power / Infra", "XLI"),
    "ORCL": ("Cloud Compute", "XLK"),
    "CRWV": ("AI Cloud Infra (public)", "—"),
    "LQD": ("Credit — IG", "—"),
    "HYG": ("Credit — HY", "—"),
    "BABA": ("China AI / Low-Cost Models", "KWEB"),
    "BIDU": ("China AI / Low-Cost Models", "KWEB"),
    "TCEHY": ("China AI / Low-Cost Models", "KWEB"),
}

# Tickers priced in a currency other than USD -- everything else defaults to
# USD. Only affects the displayed "last" price; % changes are ratios and
# need no conversion either way.
CURRENCY_OVERRIDE = {
    "005930.KS": "KRW",
    "000660.KS": "KRW",
}

# Non-US-listed tickers whose "1D change" reflects a different market's
# trading session/close than the rest of the basket (see methodology note
# in the output JSON) -- not a data error, just not same-clock as the US
# names it's grouped alongside.
NON_US_SESSION_TICKERS = {"005930.KS", "000660.KS"}

LOOKBACK_DAYS = 200  # enough for 3M return + 90D drawdown + 21D vol + 50D MA

# Credit ETFs are included for the fixed-income view but excluded from the
# equity breadth/vol aggregate so that stat stays a read on the AI-equity
# basket specifically, not diluted by bond price behaviour.
CREDIT_TICKERS = {"LQD", "HYG"}

# Off-the-shelf ETFs an allocator would reach for instead of a custom
# basket. Fetched and scored the same way as the basket itself so the page
# can show, in numbers, whether the custom construction actually diverges
# from "just buy the sector ETF" -- not folded into BASKET/layers, shown
# separately as a benchmark check.
BENCHMARK_TICKERS = {
    "SOXX": "Semiconductors (ETF)",
    "IGV": "Software (ETF)",
    "QQQ": "Nasdaq-100 (ETF)",
}

# Correlation window: longer than the 21D vol window on purpose -- pairwise
# correlation on 21 days is noisy; 63 trading days (~1 quarter) gives a more
# stable crowding read while still being responsive within a quarter.
CORR_WINDOW_DAYS = 63
MIN_CORR_WINDOW_DAYS = 20  # don't report a correlation built on too few days

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


def compute_return_metrics(closes):
    """Shared by both basket names and benchmark ETFs: 1D/1M/3M % change
    plus 21D annualised vol from a Close-price series. Returns None if
    there isn't enough history to compute anything meaningful."""
    if closes is None or len(closes) < 60:
        return None
    last = float(closes.iloc[-1])
    d1 = pct(last, float(closes.iloc[-2])) if len(closes) >= 2 else None
    d1m = pct(last, float(closes.iloc[-22])) if len(closes) >= 22 else None
    d3m = pct(last, float(closes.iloc[-64])) if len(closes) >= 64 else None
    daily_ret = closes.pct_change().dropna()
    vol_21d = (
        float(daily_ret.tail(21).std() * np.sqrt(252) * 100)
        if len(daily_ret) >= 21 else None
    )
    return {
        "chg_1d": d1,
        "chg_1m": d1m,
        "chg_3m": d3m,
        "vol_21d_ann": round(vol_21d, 2) if vol_21d is not None else None,
    }


def fetch_fundamentals(ticker):
    """Trailing/forward P/E, EV/EBITDA, market cap and 5Y-monthly beta from
    yfinance's .info -- one extra network call per name, best-effort. Never
    raises: a single ticker's fundamentals failing (rate limit, missing
    field, delisted-adjacent data) must not fail the whole daily run, so
    every field defaults to None rather than propagating an exception.
    Negative/zero P/E (loss-making trailing or forward estimate) is dropped
    rather than shown, since a negative P/E is not a valuation multiple --
    it's a sign flip that would corrupt any average it's folded into."""
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception as e:
        print(f"WARNING: fundamentals fetch failed for {ticker}: {e}")
        return {"pe_ttm": None, "pe_fwd": None, "ev_ebitda": None, "beta": None, "market_cap": None}

    def _num(key, nd):
        v = info.get(key)
        try:
            v = float(v)
        except (TypeError, ValueError):
            return None
        return round(v, nd) if v == v else None  # v == v is False for NaN

    pe_ttm = _num("trailingPE", 1)
    if pe_ttm is not None and pe_ttm <= 0:
        pe_ttm = None
    pe_fwd = _num("forwardPE", 1)
    if pe_fwd is not None and pe_fwd <= 0:
        pe_fwd = None
    ev_ebitda = _num("enterpriseToEbitda", 1)
    beta = _num("beta", 2)
    market_cap = _num("marketCap", 0)

    return {
        "pe_ttm": pe_ttm,
        "pe_fwd": pe_fwd,
        "ev_ebitda": ev_ebitda,
        "beta": beta,
        "market_cap": market_cap,
    }


def fetch_benchmarks():
    """SOXX/IGV/QQQ scored with the exact same return_metrics function as
    the basket itself, so the comparison is apples-to-apples. Best-effort:
    a benchmark fetch failure drops that ETF (or all of them) rather than
    failing the run -- this block is a comparison aid, not core output."""
    tickers = list(BENCHMARK_TICKERS.keys())
    try:
        raw_b = yf.download(
            tickers, period=f"{LOOKBACK_DAYS}d", interval="1d",
            auto_adjust=True, progress=False, group_by="ticker",
        )
    except Exception as e:
        print(f"WARNING: benchmark fetch failed: {e}")
        return []

    out = []
    for t in tickers:
        try:
            closes = raw_b[t]["Close"].dropna()
        except Exception:
            continue
        metrics = compute_return_metrics(closes)
        if metrics is None:
            continue
        out.append({"ticker": t, "name": BENCHMARK_TICKERS[t], **metrics})
    return out


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
            "n_names": len(members),
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


def _template_synthesis(basket_breadth, basket_vol, layers, movers):
    """Deterministic, template-based synthesis used whenever the LLM path
    isn't configured or fails. Every sentence is computed directly from
    numbers already on the page -- no invented facts, no forecasts, no
    recommendations -- so the fallback still says something rather than
    just apologising for the LLM being off."""
    parts = []

    if basket_breadth is not None and basket_vol is not None:
        parts.append(
            f"{basket_breadth}% of the AI-equity basket trades above its "
            f"50-day average, with basket volatility running at "
            f"{basket_vol}% annualised."
        )

    if movers:
        top = movers[0]
        if top.get("chg_1d") is not None:
            direction = "up" if top["chg_1d"] >= 0 else "down"
            parts.append(
                f"The largest single-day move was {top['ticker']} "
                f"({top['layer']}), {direction} {abs(top['chg_1d']):.1f}%."
            )

    def _tag(l):
        # A 1-2 name "layer" isn't a breadth statistic, it's just that
        # stock's own number -- say so plainly whenever such a layer gets
        # cited, so it isn't read as if it carried the same statistical
        # weight as a multi-name layer.
        n = l.get("n_names")
        return f"{l['name']} (n={n})" if n and n <= 2 else l["name"]

    layers_with_1m = [l for l in layers if l.get("avg_chg_1m") is not None]
    if len(layers_with_1m) >= 2:
        best = max(layers_with_1m, key=lambda l: l["avg_chg_1m"])
        worst = min(layers_with_1m, key=lambda l: l["avg_chg_1m"])
        if best["name"] != worst["name"]:
            parts.append(
                f"Layer performance diverged over the trailing month: "
                f"{_tag(best)} led at {best['avg_chg_1m']:+.1f}%, while "
                f"{_tag(worst)} lagged at {worst['avg_chg_1m']:+.1f}%."
            )

    layers_with_breadth = [l for l in layers if l.get("breadth_above_50dma_pct") is not None]
    if len(layers_with_breadth) >= 2:
        b_hi = max(layers_with_breadth, key=lambda l: l["breadth_above_50dma_pct"])
        b_lo = min(layers_with_breadth, key=lambda l: l["breadth_above_50dma_pct"])
        if b_hi["name"] != b_lo["name"]:
            parts.append(
                f"Breadth was strongest in {_tag(b_hi)} "
                f"({b_hi['breadth_above_50dma_pct']}% of names above their "
                f"50-day average) and weakest in {_tag(b_lo)} "
                f"({b_lo['breadth_above_50dma_pct']}%)."
            )

    if not parts:
        return "Not enough data this run to compute a synthesis."

    return " ".join(parts)


def build_history(prior_json_path, today_entry, max_entries=60):
    """Reads yesterday's committed ai_pulse.json (if it exists) for its own
    'history' array, appends today's breadth/vol snapshot, and caps the
    result to the trailing `max_entries` days. De-duped by date, so a
    same-day manual re-run replaces today's entry rather than doubling it.

    Deliberately stored *inside* the same JSON file the workflow already
    commits (docs/data/ai_pulse.json), not a second file -- avoids any
    change to the GitHub Actions workflow's `git add` step. This is what
    lets the page show a trend, not just a single day's snapshot -- the
    single biggest gap in the original design (see module docstring)."""
    try:
        with open(prior_json_path) as f:
            prior = json.load(f)
        hist = prior.get("history", [])
    except (FileNotFoundError, json.JSONDecodeError):
        hist = []

    hist = [h for h in hist if h.get("date") != today_entry["date"]]
    hist.append(today_entry)
    return hist[-max_entries:]


def build_synthesis(basket_breadth, basket_vol, layers, rows):
    """Returns (text, generated_by) -- generated_by is 'llm' or 'fallback'."""
    movers = sorted(
        [r for r in rows if r["chg_1d"] is not None],
        key=lambda r: abs(r["chg_1d"]), reverse=True,
    )[:3]

    if not ANTHROPIC_API_KEY or not ANTHROPIC_MODEL:
        return _template_synthesis(basket_breadth, basket_vol, layers, movers), "fallback"

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

    return _template_synthesis(basket_breadth, basket_vol, layers, movers), "fallback"


def main():
    tickers = list(BASKET.keys())
    raw = yf.download(
        tickers, period=f"{LOOKBACK_DAYS}d", interval="1d",
        auto_adjust=True, progress=False, group_by="ticker",
    )

    rows = []
    uptrend_count = 0
    all_daily_returns = []
    daily_ret_by_ticker = {}

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
            daily_ret_by_ticker[t] = daily_ret.tail(CORR_WINDOW_DAYS)

        own_vol_21d = float(daily_ret.tail(21).std() * np.sqrt(252) * 100) if len(daily_ret) >= 21 else None

        window = closes.tail(90)
        running_max = window.cummax()
        drawdown = float(((window - running_max) / running_max).min() * 100)

        # Credit ETFs don't have earnings -- Yahoo's .info still returns
        # *something* for trailingPE/forwardPE on LQD/HYG (observed: a
        # bond-ETF "P/E" of ~11x and a forward P/E over 1000x on one run),
        # neither of which is a decision-useful figure for a fixed-income
        # proxy. Skip the fetch entirely rather than display noise dressed
        # up as a real number.
        fundamentals = (
            {"pe_ttm": None, "pe_fwd": None, "ev_ebitda": None, "beta": None, "market_cap": None}
            if t in CREDIT_TICKERS else fetch_fundamentals(t)
        )

        layer, etf = BASKET[t]
        rows.append({
            "ticker": t,
            "layer": layer,
            "etf": etf,
            "currency": CURRENCY_OVERRIDE.get(t, "USD"),
            "non_us_session": t in NON_US_SESSION_TICKERS,
            "last": round(last, 2),
            "chg_1d": d1,
            "chg_1m": d1m,
            "chg_3m": d3m,
            "vol_21d_ann": round(own_vol_21d, 2) if own_vol_21d is not None else None,
            "above_50dma": bool(in_uptrend),
            "drawdown_90d": round(drawdown, 2),
            **fundamentals,
        })

    equity_rows = [r for r in rows if r["ticker"] not in CREDIT_TICKERS]
    breadth = round(100 * uptrend_count / len(equity_rows), 1) if equity_rows else None

    if all_daily_returns:
        combined = np.concatenate([r.values for r in all_daily_returns])
        ann_vol = round(float(np.std(combined) * np.sqrt(252) * 100), 2)
    else:
        ann_vol = None

    # Concentration: top-3-by-market-cap share of the basket. USD-only --
    # mixing KRW market caps (Samsung, SK Hynix) with USD ones without an
    # FX conversion would silently misstate the ratio, so those two are
    # excluded from this specific stat (same caveat pattern already used
    # for their 1D change elsewhere on the page).
    cap_rows = [
        r for r in equity_rows
        if r["currency"] == "USD" and r.get("market_cap")
    ]
    if cap_rows:
        cap_rows_sorted = sorted(cap_rows, key=lambda r: r["market_cap"], reverse=True)
        total_cap = sum(r["market_cap"] for r in cap_rows)
        top3_cap = sum(r["market_cap"] for r in cap_rows_sorted[:3])
        concentration_top3_pct = round(100 * top3_cap / total_cap, 1) if total_cap else None
        concentration_top3_names = [r["ticker"] for r in cap_rows_sorted[:3]]
    else:
        concentration_top3_pct = None
        concentration_top3_names = []

    # Average pairwise correlation across the equity subset, trailing
    # CORR_WINDOW_DAYS -- a rising number here is a crowding signal (names
    # moving together regardless of layer), same spirit as the fragility
    # engine's own correlation pillar, computed independently here.
    avg_pairwise_corr = None
    corr_window_actual = 0
    if len(daily_ret_by_ticker) >= 2:
        returns_df = pd.DataFrame(daily_ret_by_ticker).dropna(how="any")
        corr_window_actual = int(returns_df.shape[0])
        if corr_window_actual >= MIN_CORR_WINDOW_DAYS and returns_df.shape[1] >= 2:
            corr = returns_df.corr()
            n = corr.shape[0]
            upper = corr.values[np.triu_indices(n, k=1)]
            avg_pairwise_corr = round(float(np.mean(upper)), 2)

    # Median, not mean: trailing P/E is a right-skewed distribution by
    # nature (a name near breakeven GAAP earnings can print a P/E in the
    # thousands -- observed live: CRWD at ~5300x on a run where every other
    # name was under 130x). A simple mean lets one such name single-
    # handedly set the headline number; the median is what a basket-wide
    # "how expensive is this" read should actually be reporting.
    pe_vals = [r["pe_ttm"] for r in equity_rows if r.get("pe_ttm") is not None]
    median_pe_ttm = round(float(np.median(pe_vals)), 1) if pe_vals else None
    beta_vals = [r["beta"] for r in equity_rows if r.get("beta") is not None]
    median_beta = round(float(np.median(beta_vals)), 2) if beta_vals else None

    benchmarks_etfs = fetch_benchmarks()
    benchmarks = {
        "basket_avg": {
            "label": f"AI basket (equal-wt, {len(equity_rows)} names)",
            "chg_1d": _mean([r["chg_1d"] for r in equity_rows]),
            "chg_1m": _mean([r["chg_1m"] for r in equity_rows]),
            "chg_3m": _mean([r["chg_3m"] for r in equity_rows]),
            "vol_21d_ann": _mean([r["vol_21d_ann"] for r in equity_rows]),
        },
        "etfs": benchmarks_etfs,
    }

    layers = build_layers(rows)
    synthesis_text, generated_by = build_synthesis(breadth, ann_vol, layers, rows)

    as_of = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    history = build_history(
        "docs/data/ai_pulse.json",
        {
            "date": as_of[:10],
            "as_of": as_of,
            "basket_breadth_above_50dma_pct": breadth,
            "basket_avg_ann_vol_21d_pct": ann_vol,
        },
    )

    out = {
        "as_of": as_of,
        "basket_breadth_above_50dma_pct": breadth,
        "basket_avg_ann_vol_21d_pct": ann_vol,
        "basket_median_pe_ttm": median_pe_ttm,
        "basket_median_beta": median_beta,
        "basket_concentration_top3_pct": concentration_top3_pct,
        "basket_concentration_top3_names": concentration_top3_names,
        "basket_avg_pairwise_corr": avg_pairwise_corr,
        "basket_avg_pairwise_corr_window_days": corr_window_actual,
        "benchmarks": benchmarks,
        "history": history,
        "layers": layers,
        "commentary": {
            "synthesis": synthesis_text,
            "generated_by": generated_by,   # "llm" or "fallback"
            "model": ANTHROPIC_MODEL if generated_by == "llm" else None,
            "disclaimer": (
                "AI-generated summary of the numbers on this page only. "
                "Descriptive, not a forecast, recommendation, or investment advice."
                if generated_by == "llm" else
                "Rule-based summary computed directly from the figures on this "
                "page (no model involved). Descriptive, not a forecast, "
                "recommendation, or investment advice."
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
            "aggregates are simple, equal-weighted means across that "
            "layer's names -- not market-cap-weighted, so a smaller name "
            "carries the same influence on a layer average as a mega-cap "
            "one. A layer backed by only one or two names is labelled "
            "(n=1)/(n=2) wherever cited in the synthesis below, since its "
            "'breadth' figure is really just that name's own number, not a "
            "breadth statistic. Samsung Electronics (005930.KS) and SK "
            "Hynix (000660.KS) trade in KRW on the Korea Exchange, on a "
            "trading session that closes hours before the US session even "
            "opens -- their 1-day changes are not on the same clock as the "
            "rest of the basket, shown for completeness, not for direct "
            "day-over-day comparison. 'history' holds up to the trailing "
            "60 days of basket breadth/vol, appended once per run, so the "
            "page can show a trend rather than a single snapshot. The "
            "synthesis paragraph above is LLM-generated only when "
            "commentary.generated_by = 'llm'; when it reads 'fallback' "
            "(as it does whenever no API key is configured), that "
            "paragraph is template arithmetic like everything else on "
            "this page, not a model output. Descriptive only; not a "
            "signal or recommendation. P/E (ttm) and EV/EBITDA are from "
            "yfinance's own reported fields, best-effort -- a name shows "
            "'-' when the field is missing, the company is loss-making (a "
            "negative P/E is dropped rather than shown, since it isn't a "
            "valuation multiple), or it's one of the two credit ETFs "
            "(LQD/HYG), which are skipped entirely -- a bond ETF has no "
            "earnings, and Yahoo's fields for one aren't a real multiple. "
            "The basket-wide P/E figure is a MEDIAN, not a mean: trailing "
            "P/E is right-skewed by nature (a name near breakeven GAAP "
            "earnings can print a P/E in the thousands) and a simple "
            "average lets one such name single-handedly set the headline "
            "number -- the per-name table still shows that name's actual "
            "reported figure, unrounded down, so nothing is hidden, just "
            "not allowed to distort the summary stat. Beta is Yahoo's "
            "standard 5-year monthly beta versus each name's home-market "
            "index, not a figure BK/IQ computes; the basket figure is also "
            "a median for the same reason. Concentration = top-3-by-market-cap "
            "share of the basket, USD-denominated names only (Samsung and "
            "SK Hynix excluded from this one stat specifically, to avoid "
            "mixing KRW and USD market caps without an FX conversion). "
            "Avg pairwise correlation is computed over the trailing "
            "63 trading days across the equity subset (a longer window "
            "than the 21D vol figure, deliberately, since correlation on "
            "21 days alone is noisy) -- a rising number here means names "
            "are moving together regardless of layer, independent of the "
            "fragility engine's own correlation pillar. The benchmark "
            "block compares the basket's own equal-weighted average "
            "against SOXX/IGV/QQQ using the identical calculation, so any "
            "gap reflects the basket's different membership, not a "
            "different formula."
        ),
    }

    # NOTE: this repo publishes GitHub Pages from /docs, so the JSON has to
    # live under docs/data/ to actually be reachable by the deployed ai.html.
    with open("docs/data/ai_pulse.json", "w") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote docs/data/ai_pulse.json — {len(rows)} names, {len(layers)} layers, "
          f"breadth {breadth}%, median PE {median_pe_ttm}, concentration(top3) "
          f"{concentration_top3_pct}%, avg corr {avg_pairwise_corr}, "
          f"{len(benchmarks_etfs)} benchmark ETFs, synthesis via {generated_by}")


if __name__ == "__main__":
    main()
