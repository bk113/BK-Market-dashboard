"""
fetch_deep_history.py — standalone deep-history fetcher.

Fetches the maximum available price history from Yahoo Finance for every
instrument in the BKIQ registry (excluding synthetic computed tickers that
are never fetched from Yahoo).  Stores results as individual CSVs under
data/deep_history/{ticker}.csv, completely separate from prices_cache.csv
and the live dashboard pipeline.

PURPOSE: offline validation work (T1/T2/T5/T6 crisis-period diagnostics).
         NOT wired into any live pipeline, workflow, or scheduled job.

USAGE:
    python tools/fetch_deep_history.py            # fetch all
    python tools/fetch_deep_history.py SPY GLD    # re-fetch specific tickers

DO NOT import from the live path (bk_market_dashboard.py, build_canonical_panel.py).
"""

import sys
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

# ── Paths ─────────────────────────────────────────────────────────────────────
REPO_ROOT  = Path(__file__).resolve().parent.parent
DATA_DIR   = REPO_ROOT / "data"
OUT_DIR    = DATA_DIR / "deep_history"
MANIFEST   = OUT_DIR / "_manifest.csv"

OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Registry import (read-only; no live-path side effects) ───────────────────
sys.path.insert(0, str(REPO_ROOT))
from layer1_data import INSTRUMENT_REGISTRY

# Synthetic tickers: computed from other instruments, never fetched from Yahoo.
SYNTHETIC_TICKERS = {
    tk for tk, meta in INSTRUMENT_REGISTRY.items()
    if meta.synthetic_source is not None
}

# Full list to fetch: every registered ticker that is not synthetic.
ALL_TICKERS = sorted(
    tk for tk in INSTRUMENT_REGISTRY
    if tk not in SYNTHETIC_TICKERS
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_one(ticker: str) -> pd.DataFrame | None:
    """Download max history for one ticker, return weekday-only adjusted close + volume."""
    try:
        raw = yf.download(
            ticker,
            period="max",
            auto_adjust=True,   # matches live pipeline's adjustment policy
            progress=False,
            actions=False,
        )
    except Exception as exc:
        print(f"  ERROR downloading {ticker}: {exc}")
        return None

    if raw is None or raw.empty:
        print(f"  EMPTY result for {ticker}")
        return None

    # Flatten MultiIndex columns that yfinance sometimes returns for a single ticker
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    raw.index = pd.to_datetime(raw.index)
    raw = raw[raw.index.dayofweek < 5]   # weekdays only
    raw.index.name = "Date"

    keep = [c for c in ("Close", "Volume") if c in raw.columns]
    df = raw[keep].copy()
    df = df[~df["Close"].isna()]          # drop rows with no price
    return df


def load_manifest() -> pd.DataFrame:
    if MANIFEST.exists():
        return pd.read_csv(MANIFEST)
    return pd.DataFrame(columns=["ticker", "start_date", "end_date", "n_rows",
                                  "fetched_at", "status"])


def save_manifest(rows: list[dict]):
    df = pd.DataFrame(rows)
    df.to_csv(MANIFEST, index=False)
    print(f"\n  Manifest written: {MANIFEST.name}  ({len(df)} tickers)")


# ── Main ──────────────────────────────────────────────────────────────────────

def main(target_tickers: list[str] | None = None):
    tickers = target_tickers if target_tickers else ALL_TICKERS
    n = len(tickers)
    print(f"fetch_deep_history.py — fetching {n} tickers → {OUT_DIR}")
    print(f"  auto_adjust=True  |  period=max  |  weekday-only filter")
    print(f"  Synthetic tickers skipped: {sorted(SYNTHETIC_TICKERS)}\n")

    manifest_rows: list[dict] = []
    ok = failed = 0

    for i, ticker in enumerate(tickers, 1):
        safe = ticker.replace("^", "_").replace("=", "_").replace("-", "_")
        out_path = OUT_DIR / f"{safe}.csv"

        print(f"  [{i:>3}/{n}] {ticker:<14}", end="", flush=True)
        df = fetch_one(ticker)

        if df is None or df.empty:
            print("  FAILED")
            manifest_rows.append(dict(
                ticker=ticker, start_date="", end_date="", n_rows=0,
                fetched_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                status="failed",
            ))
            failed += 1
        else:
            df.to_csv(out_path)
            start = str(df.index.min().date())
            end   = str(df.index.max().date())
            rows  = len(df)
            print(f"  {rows:>5} rows  {start} → {end}")
            manifest_rows.append(dict(
                ticker=ticker, start_date=start, end_date=end, n_rows=rows,
                fetched_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                status="ok",
            ))
            ok += 1

        time.sleep(0.3)   # polite rate-limit pause

    save_manifest(manifest_rows)
    print(f"\n  Done: {ok} succeeded, {failed} failed")
    return manifest_rows


def spot_check(manifest_rows: list[dict]):
    """Print a quick sanity table for a handful of representative tickers."""
    print("\n" + "=" * 68)
    print("SPOT-CHECK")
    print("=" * 68)

    checks = [
        ("SPY",     "S&P 500 — should have ~1993+ data"),
        ("GLD",     "Gold ETF — listed Nov 2004"),
        ("ARKK",    "ARK Innovation — listed Oct 2014"),
        ("SOL-USD", "Solana — listed ~2020"),
        ("SOXX",    "Semis — listed Jul 2001"),
        ("SMIN",    "India Small Cap — listed Feb 2012"),
        ("BTC-USD", "Bitcoin — listed ~2014 on Yahoo"),
    ]

    row_by_ticker = {r["ticker"]: r for r in manifest_rows}
    print(f"  {'Ticker':<12} {'Status':<8} {'Start':<12} {'End':<12} {'Rows':>6}  Note")
    for ticker, note in checks:
        r = row_by_ticker.get(ticker)
        if r is None:
            print(f"  {ticker:<12} {'SKIP':<8} {'':12} {'':12} {'':6}  (not fetched)")
            continue
        if r["status"] != "ok":
            print(f"  {ticker:<12} {'FAILED':<8} {'':12} {'':12} {'':6}  {note}")
            continue
        print(f"  {ticker:<12} {'OK':<8} {r['start_date']:<12} {r['end_date']:<12} "
              f"{r['n_rows']:>6}  {note}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        requested = [t.upper() for t in sys.argv[1:]]
        invalid   = [t for t in requested if t not in INSTRUMENT_REGISTRY and
                     t not in {tk.upper() for tk in INSTRUMENT_REGISTRY}]
        if invalid:
            print(f"Unknown tickers (not in registry): {invalid}")
            sys.exit(1)
        manifest_rows = main(requested)
    else:
        manifest_rows = main()

    spot_check(manifest_rows)
