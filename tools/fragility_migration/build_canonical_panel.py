"""
Phase 1: build the canonical panel_v22_{prices,returns,volumes,levels,metadata}.csv
files for the FRAGILITY ENGINE ONLY (decision 2: not a whole-dashboard migration),
from BKIQ's already-cached prices_cache.csv/volumes_cache.csv, using the mapping
built by ticker_mapping.py.

IMPORTANT -- data currency, re-verified 2026-09-07:
  - PRICES/RETURNS come from data/prices_cache.csv. CHECK THE DATE RANGE
    PRINTED BELOW before trusting a run -- as of 2026-09-07 this local file
    was found stopping 2026-04-18 (this repo's local clone had not been
    git-pulled in ~4.5 months), while the copy actually committed to GitHub
    (and served live at dashboard.bkiqmarkets.com) was current through
    2026-09-04. Run `git pull` before running this script, or point
    DATA_DIR at a freshly-pulled clone -- do not trust "already current"
    claims from any prior run without checking the printed date range.
  - VOLUMES: as of 2026-09-07 there is no volumes_cache.csv in the GitHub
    repo at all (404 on raw.githubusercontent.com) -- the daily Action has
    apparently never committed one. The local copy here is also stale
    (last real date 2026-04-17). Real volumes need a fresh yfinance pull
    with network access (see refresh_volumes_cache.py) before Phase 2
    validation numbers can be trusted.
  - LEVELS: uses build_levels_from_fred_manual() when
    tools/fragility_migration/fred_manual/*.csv files are present (real
    FRED data, manually downloaded -- see that function's docstring for the
    expected file names/format), else falls back to a clearly-labelled
    SYNTHETIC STUB (sandbox-testing only, never for production).
"""
import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
# NOTE (fixed 2026-09-08): this script lives at tools/fragility_migration/,
# two directories below the repo root -- not one. BUILD_DIR must therefore be
# .parent THREE times, not two, or DATA_DIR silently points at a
# tools/data/ folder that has never existed, and prices_cache.csv/
# volumes_cache.csv are never found. Confirmed empirically before this fix
# landed (this script had never actually been run against real data yet).
BUILD_DIR = SCRIPT_DIR.parent.parent
DATA_DIR = BUILD_DIR / "data"

# Ticker-specific return caps, generalizing BKIQ's own RETURN_SANITY_MAX
# (bk_market_dashboard.py ~line 742), which today only clips *display*
# metrics (YTD/1M/3M), not the raw returns feeding the fragility engine.
# v23's own docstring calls for "winsorize returns (0.1% / 99.9% trailing)"
# as pipeline step 1 -- but that step is not actually implemented anywhere
# in v23.py (confirmed: no winsorize call in the file). It belongs here, at
# the panel-build layer, once, centrally -- not duplicated per-caller.
WINSOR_LOWER_Q = 0.001
WINSOR_UPPER_Q = 0.999


def load_mapping():
    # ticker_mapping.json lives beside this script (tools/fragility_migration/),
    # per ticker_mapping.py's own write location and the handoff doc's
    # description -- not in DATA_DIR (which is for the panel CSVs and the
    # existing prices_cache.csv/volumes_cache.csv).
    mp = json.load(open(SCRIPT_DIR / "ticker_mapping.json"))
    return mp["mapped"], mp["excluded"]


def add_synthetic_vol_tickers(prices: pd.DataFrame) -> pd.DataFrame:
    """GVZ/OVX as 20D rolling vol of GLD/BNO -- exact match to BKIQ's
    SYNTHETIC_TICKERS logic (bk_market_dashboard.py line 282, 657-662)."""
    SYNTHETIC = {"GVZ": "GLD", "OVX": "BNO"}
    prices = prices.copy()
    for proxy, source in SYNTHETIC.items():
        if proxy in prices.columns:
            continue  # already present in cache
        if source not in prices.columns:
            warnings.warn(f"Cannot build synthetic {proxy}: source {source} missing")
            continue
        rets = prices[source].pct_change()
        rolling_vol = rets.rolling(window=20).std() * (252 ** 0.5) * 100.0
        prices[proxy] = rolling_vol.bfill()
    return prices


def winsorize_returns(returns: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Per-column whole-sample 0.1%/99.9% quantile clip. Returns (clipped, bounds_report)."""
    clipped = returns.copy()
    report_rows = []
    for col in returns.columns:
        s = returns[col].dropna()
        if len(s) < 100:
            continue
        lo, hi = s.quantile(WINSOR_LOWER_Q), s.quantile(WINSOR_UPPER_Q)
        n_clipped = int(((returns[col] < lo) | (returns[col] > hi)).sum())
        clipped[col] = returns[col].clip(lo, hi)
        if n_clipped > 0:
            report_rows.append({"column": col, "lower_bound": round(lo, 4),
                                 "upper_bound": round(hi, 4), "n_clipped": n_clipped})
    return clipped, pd.DataFrame(report_rows).sort_values("n_clipped", ascending=False)


def build_prices_and_returns(mapped_rows):
    prices_raw = pd.read_csv(DATA_DIR / "prices_cache.csv", index_col=0, parse_dates=True)
    # Weekday-only filter FIRST, before any rolling computation. The raw cache
    # carries calendar rows for every day some instrument traded (crypto
    # trades weekends), which pollutes rolling windows for everything else
    # with NaN-window failures (a 20-day rolling std() over raw calendar rows
    # is NaN almost everywhere once ~30% of rows are weekend NaN for
    # non-crypto tickers). Mirrors fetch_panel()'s own dayofweek<5 filter.
    n_before = len(prices_raw)
    prices_raw = prices_raw[prices_raw.index.dayofweek < 5]
    print(f"  Weekday filter: {n_before} -> {len(prices_raw)} rows")
    prices_raw = add_synthetic_vol_tickers(prices_raw)

    ticker_to_name = {r["ticker"]: r["canonical_name"] for r in mapped_rows}
    missing = [t for t in ticker_to_name if t not in prices_raw.columns]
    if missing:
        raise RuntimeError(f"Tickers in mapping but missing from prices_cache.csv: {missing}")

    prices = prices_raw[list(ticker_to_name.keys())].rename(columns=ticker_to_name)
    # canonical column order: scored first (EQ/FI/CMD/CRYPTO), then FX, then VOL
    order = {"EQ": 0, "FI": 1, "CMD": 2, "CRYPTO": 3, "FX": 4, "VOL": 5}
    col_order = sorted(prices.columns, key=lambda c: (order[c.split(" | ")[0]], c))
    prices = prices[col_order]

    returns_raw = prices.pct_change()
    returns, winsor_report = winsorize_returns(returns_raw)
    return prices, returns, winsor_report


def build_volumes(mapped_rows, price_index):
    vol_path = DATA_DIR / "volumes_cache.csv"
    volumes_raw = pd.read_csv(vol_path, index_col=0, parse_dates=True)
    last_date = volumes_raw.index.max()
    staleness_days = (price_index.max() - last_date).days
    print(f"  WARNING: volumes_cache.csv last date = {last_date.date()} "
          f"({staleness_days} days behind prices). Using as-is for the sandbox "
          f"build; production needs a fresh volume fetch (see run_data_refresh.py).")

    ticker_to_name = {r["ticker"]: r["canonical_name"] for r in mapped_rows}
    available = [t for t in ticker_to_name if t in volumes_raw.columns]
    missing = [t for t in ticker_to_name if t not in volumes_raw.columns]
    volumes = volumes_raw[available].rename(columns={t: ticker_to_name[t] for t in available})
    volumes = volumes.reindex(price_index)
    if missing:
        print(f"  {len(missing)} mapped tickers have no volume history at all "
              f"(FX pairs typically don't): {missing[:8]}{'...' if len(missing) > 8 else ''}")
    missing_cols = [ticker_to_name[t] for t in missing if ticker_to_name[t] not in volumes.columns]
    if missing_cols:
        volumes = pd.concat([volumes, pd.DataFrame(np.nan, index=volumes.index, columns=missing_cols)], axis=1)
    volumes = volumes[[ticker_to_name[t] for t in ticker_to_name]]
    return volumes, staleness_days


def build_levels_stub(price_index):
    """SANDBOX-ONLY placeholder -- NOT real data. Purely so compute_v23() can
    run end-to-end here without (a) KeyErrors on levels["Federal Funds Rate"]
    (compute_policy_reaction, hard lookup, no .get() guard), and (b) a
    zero-variance column collapsing fit_hmm_v2()'s standardization step to
    NaN (a perfectly flat placeholder makes (x - mean)/std = 0/0 for that
    column, which survives X.dropna() since the *pre*-standardization value
    isn't NaN -- only the post-standardization one is). A small seeded random
    walk avoids that failure mode while still being obviously fake. MUST be
    replaced by a real fetch_fred_data() run before this ever reaches
    production."""
    rng = np.random.default_rng(20260828)
    n = len(price_index)

    def walk(start, daily_sigma, floor=None):
        steps = rng.normal(0, daily_sigma, n)
        s = pd.Series(start + np.cumsum(steps), index=price_index)
        return s.clip(lower=floor) if floor is not None else s

    levels = pd.DataFrame(index=price_index)
    levels["Federal Funds Rate"] = walk(4.25, 0.002, floor=0.0)
    levels["Secured Overnight Rate"] = walk(4.30, 0.002, floor=0.0)
    levels["Inflation Breakeven 5Y"] = walk(2.3, 0.005, floor=0.0)
    levels["Inflation Breakeven 10Y"] = walk(2.3, 0.005, floor=0.0)
    levels["Inflation 5Y5Y Forward"] = walk(2.3, 0.005, floor=0.0)
    levels["US Treasury 3M Yield"] = walk(4.3, 0.01, floor=0.0)
    levels["US Treasury 2Y Yield"] = walk(4.0, 0.01, floor=0.0)
    levels["US Treasury 5Y Yield"] = walk(4.0, 0.01, floor=0.0)
    levels["US Treasury 10Y Yield"] = walk(4.2, 0.01, floor=0.0)
    levels["US Treasury 30Y Yield"] = walk(4.5, 0.01, floor=0.0)
    levels["VIX"] = walk(16.0, 0.3, floor=9.0)
    levels["TED Spread (legacy)"] = np.nan
    levels["USD Trade Weighted Broad"] = walk(120.0, 0.1, floor=80.0)
    levels["ICE BofA US HY Index OAS"] = walk(3.2, 0.02, floor=1.0)
    return levels


# FRED series code -> v23 canonical name, exactly mirroring v23's own
# FRED_SERIES dict (institutional_fragility_monitor_v23_2026-08-18.py,
# line ~507). Kept as a literal copy here (not imported) so this script has
# zero dependency on v23.py's internals -- decision 3 says v23 stays
# untouched and unimported outside compute_v23() itself.
FRED_MANUAL_DIR = SCRIPT_DIR / "fred_manual"
FRED_CODE_TO_NAME = {
    "T5YIE": "Inflation Breakeven 5Y",
    "T10YIE": "Inflation Breakeven 10Y",
    "T5YIFR": "Inflation 5Y5Y Forward",
    "DGS3MO": "US Treasury 3M Yield",
    "DGS2": "US Treasury 2Y Yield",
    "DGS5": "US Treasury 5Y Yield",
    "DGS10": "US Treasury 10Y Yield",
    "DGS30": "US Treasury 30Y Yield",
    "VIXCLS": "VIX",
    "DFF": "Federal Funds Rate",
    "SOFR": "Secured Overnight Rate",
    "TEDRATE": "TED Spread (legacy)",
    "DTWEXBGS": "USD Trade Weighted Broad",
    "BAMLH0A0HYM2": "ICE BofA US HY Index OAS",
}


FRED_FFILL_LIMIT_DAYS = 5  # covers normal bond-market-holiday gaps, nothing more
FRED_STALE_WARN_DAYS = 30  # last-real-obs gap beyond this vs. the price index end -> flagged


def build_levels_from_fred_manual(price_index, folder: Path = FRED_MANUAL_DIR):
    """Real FRED levels, built from manually-downloaded fredgraph.csv files
    (one per series -- https://fred.stlouisfed.org/graph/fredgraph.csv?id=<CODE>,
    or the 'Download' button on each series' FRED page). Each file must be
    named <CODE>.csv (e.g. DGS10.csv) and sit in `folder`, with FRED's own
    two-column DATE,<CODE> layout ('.' = missing, handled below).

    Forward-filled onto `price_index` (the business-day grid already built
    from prices) so shape/alignment matches build_levels_stub()'s output
    exactly -- this is a drop-in replacement, not a new schema. The forward
    fill is capped at FRED_FFILL_LIMIT_DAYS (see below) -- this matters.

    IMPORTANT -- discontinued/stale series (found live 2026-09-08: TEDRATE's
    last real observation is 2022-01-21, a FRED-side discontinuation after
    the LIBOR phase-out, not a download gap): an *unlimited* ffill would
    silently carry that 2022 value forward as if it were today's TED spread,
    with 0% NaN in any post-hoc data-quality check -- a real misrepresentation
    for a fragility/risk tool, even though v23 itself never hard-reads this
    column in a pillar or governance computation (confirmed by reading
    institutional_fragility_monitor_v23.py -- "TED Spread (legacy)" only
    ever appears in its descriptive metadata table). Capping the ffill at
    FRED_FFILL_LIMIT_DAYS (enough to bridge ordinary bond-market holidays,
    not enough to paper over a dead series) makes any genuinely discontinued
    or broken series show up as NaN-dense in the standard data-quality
    checks automatically -- self-flagging, not hardcoded to TEDRATE by name,
    so a different series stalling out in the future gets caught the same
    way instead of silently riding along as "current."

    Returns (levels_df, missing_codes, staleness_report) -- missing_codes is
    non-empty if any of the 14 expected files weren't found (main() then
    decides whether to fall back to the synthetic stub for just those
    columns or abort); staleness_report is a DataFrame with one row per
    loaded series giving its last real (pre-ffill) observation date and how
    many days that trails the price panel's end date, flagged where that
    exceeds FRED_STALE_WARN_DAYS.
    """
    levels = pd.DataFrame(index=price_index)
    missing = []
    staleness_rows = []
    panel_end = price_index.max()
    for code, name in FRED_CODE_TO_NAME.items():
        fpath = folder / f"{code}.csv"
        if not fpath.exists():
            missing.append(code)
            continue
        raw = pd.read_csv(fpath)
        # FRED's own export names the date column "DATE" or "observation_date"
        # depending on download route (graph CSV vs. series-page CSV).
        date_col = "DATE" if "DATE" in raw.columns else "observation_date"
        val_col = code if code in raw.columns else [c for c in raw.columns if c != date_col][0]
        raw[date_col] = pd.to_datetime(raw[date_col])
        raw[val_col] = pd.to_numeric(raw[val_col], errors="coerce")  # '.' -> NaN
        s = raw.set_index(date_col)[val_col].sort_index()
        s_real = s.dropna()
        last_real_date = s_real.index.max() if len(s_real) else pd.NaT
        staleness_days = (panel_end - last_real_date).days if pd.notna(last_real_date) else None
        staleness_rows.append({
            "code": code, "name": name, "last_real_date": last_real_date,
            "staleness_days_vs_panel_end": staleness_days,
            "flagged_stale": bool(staleness_days is not None and staleness_days > FRED_STALE_WARN_DAYS),
        })
        # Reindex onto the price panel's business-day grid, forward-fill only
        # a short grace window (FRED series are business-day-ish but not
        # identical to the equity calendar -- e.g. bond-market holidays) --
        # NOT an unlimited ffill; see docstring above for why.
        s = s.reindex(s.index.union(price_index)).ffill(limit=FRED_FFILL_LIMIT_DAYS).reindex(price_index)
        levels[name] = s
    return levels, missing, pd.DataFrame(staleness_rows)


def build_metadata(mapped_rows, excluded_rows, levels=None, staleness_report=None):
    rows = []
    for r in mapped_rows:
        role = ("fragility composite (vol, dd, cvar, trend, corr, volz pillars)" if r["scored"]
                else "FX system-context (correlation pillar PC1 only)" if r["prefix"] == "FX"
                else "display only (outside fragility scoring)")
        rows.append({
            "name": r["canonical_name"], "ticker": r["ticker"], "asset_class": r["prefix"],
            "source": "Yahoo Finance", "data_type": "tradeable", "role": role,
            "bkiq_category": r["category"], "bkiq_asset_class_label": r["asset_class"],
        })
    for e in excluded_rows:
        rows.append({
            "name": e["name"], "ticker": e["ticker"], "asset_class": "RATES",
            "source": "Yahoo Finance", "data_type": "excluded", "role": e["reason"],
            "bkiq_category": e["category"], "bkiq_asset_class_label": "",
        })
    # FRED level series -- mirrors v23's own build_metadata (name-pattern
    # classification), plus an explicit staleness call-out so a discontinued
    # series (e.g. TED Spread, dead since 2022) is labeled as such in the
    # one file most likely to be read before trusting the panel, not just
    # buried in a console log.
    stale_by_code = {}
    if staleness_report is not None and not staleness_report.empty:
        stale_by_code = {row["code"]: row for _, row in staleness_report.iterrows()}
    if levels is not None:
        for code, name in FRED_CODE_TO_NAME.items():
            if name not in levels.columns:
                continue
            if "Inflation" in name:    cls = "INFLATION"
            elif "Treasury" in name:   cls = "RATES"
            elif "VIX" in name:        cls = "VOL"
            elif "Funds" in name or "SOFR" in name: cls = "POLICY"
            elif "TED" in name:        cls = "FUNDING"
            elif "USD" in name:        cls = "FX"
            elif "HY" in name:         cls = "CREDIT"
            else:                       cls = "OTHER"
            role = "regime / FSS / governance"
            st = stale_by_code.get(code)
            if st is not None and st.get("flagged_stale"):
                role += (f" -- DISCONTINUED/STALE: last real obs "
                         f"{st['last_real_date'].date()}, "
                         f"{st['staleness_days_vs_panel_end']}d stale, held flat only "
                         f"{FRED_FFILL_LIMIT_DAYS}bd then NaN")
            rows.append({
                "name": name, "ticker": code, "asset_class": cls,
                "source": "FRED", "data_type": "level/yield/spread", "role": role,
                "bkiq_category": "", "bkiq_asset_class_label": "",
            })
    return pd.DataFrame(rows)


def main():
    mapped_rows, excluded_rows = load_mapping()

    print("Building prices/returns...")
    prices, returns, winsor_report = build_prices_and_returns(mapped_rows)
    print(f"  {prices.shape[1]} columns x {prices.shape[0]} rows "
          f"({prices.index.min().date()} -> {prices.index.max().date()})")
    weekend_rows = (prices.index.dayofweek >= 5).sum()
    print(f"  Weekend rows: {weekend_rows} (should be 0)")
    if not winsor_report.empty:
        print(f"  Winsorized {len(winsor_report)} columns (top 5 by clip count):")
        print(winsor_report.head(5).to_string(index=False))

    print("\nBuilding volumes...")
    volumes, vol_staleness_days = build_volumes(mapped_rows, prices.index)

    print("\nBuilding levels...")
    fred_files_present = FRED_MANUAL_DIR.exists() and any(FRED_MANUAL_DIR.glob("*.csv"))
    if fred_files_present:
        levels, missing_codes, staleness_report = build_levels_from_fred_manual(prices.index)
        if missing_codes:
            print(f"  WARNING: {len(missing_codes)}/{len(FRED_CODE_TO_NAME)} FRED series missing "
                  f"from {FRED_MANUAL_DIR} -- filling those with the synthetic stub: {missing_codes}")
            stub = build_levels_stub(prices.index)
            for code in missing_codes:
                name = FRED_CODE_TO_NAME[code]
                levels[name] = stub[name]
        else:
            print(f"  REAL FRED data loaded from {FRED_MANUAL_DIR} "
                  f"({levels.index.min().date()} -> {levels.index.max().date()})")

        if not staleness_report.empty:
            flagged = staleness_report[staleness_report["flagged_stale"]]
            print(f"\n  FRED series staleness check (last real obs vs. panel end "
                  f"{prices.index.max().date()}), ffill capped at {FRED_FFILL_LIMIT_DAYS} business days:")
            print(staleness_report[["code", "name", "last_real_date", "staleness_days_vs_panel_end"]]
                  .to_string(index=False))
            if not flagged.empty:
                print(f"\n  *** {len(flagged)} series exceed the {FRED_STALE_WARN_DAYS}-day staleness "
                      f"threshold and are NOT being carried forward as current -- they will show as "
                      f"NaN-dense below (by design, not a bug). Confirmed cause for TEDRATE: FRED "
                      f"discontinued the series in 2022 (LIBOR phase-out); v23 never hard-reads this "
                      f"column in a pillar/governance computation, only in its descriptive metadata "
                      f"table, so this is a labeling/display-accuracy issue, not a scoring-integrity "
                      f"one. Flagged series:")
                for _, row in flagged.iterrows():
                    print(f"      {row['code']} ({row['name']}): last real obs "
                          f"{row['last_real_date'].date()}, {row['staleness_days_vs_panel_end']} days stale")
            staleness_report.to_csv(DATA_DIR / "fred_staleness_report.csv", index=False)

        nan_pct = (levels.isna().mean() * 100).round(1)
        print("\n  NaN% per column (post-ffill, ffill capped at "
              f"{FRED_FFILL_LIMIT_DAYS} business days -- a discontinued/stale series will correctly "
              "show high NaN% here, not near-0):")
        print(nan_pct.to_string())
        levels_are_stub = False
    else:
        print(f"  No FRED files found at {FRED_MANUAL_DIR} -- using SANDBOX STUB (NOT real data). "
              f"Download the 14 series from FRED and place them there to use real data.")
        levels = build_levels_stub(prices.index)
        staleness_report = None
        missing_codes = list(FRED_CODE_TO_NAME.keys())
        levels_are_stub = True

    print("\nBuilding metadata...")
    metadata = build_metadata(mapped_rows, excluded_rows, levels=levels, staleness_report=staleness_report)

    DATA_DIR.mkdir(exist_ok=True)
    prices.to_csv(DATA_DIR / "panel_v22_prices.csv", index_label="Date")
    returns.to_csv(DATA_DIR / "panel_v22_returns.csv", index_label="Date")
    volumes.to_csv(DATA_DIR / "panel_v22_volumes.csv", index_label="Date")
    levels.to_csv(DATA_DIR / "panel_v22_levels.csv", index_label="Date")
    metadata.to_csv(DATA_DIR / "panel_v22_metadata.csv", index=False)
    winsor_report.to_csv(DATA_DIR / "winsorization_report.csv", index=False)

    print(f"\nWrote 5 canonical panel files + winsorization_report.csv to {DATA_DIR}")
    print(f"\n*** VOLUMES ARE {vol_staleness_days} DAYS STALE — sandbox build only ***")
    if levels_are_stub:
        print("*** LEVELS ARE A SYNTHETIC STUB — sandbox build only ***")
    elif missing_codes:
        print(f"*** LEVELS: {len(missing_codes)}/{len(FRED_CODE_TO_NAME)} series used the synthetic "
              f"stub (missing files: {missing_codes}); the rest are real FRED data ***")
    else:
        print(f"*** LEVELS: real FRED data, all {len(FRED_CODE_TO_NAME)} series loaded "
              f"({'no' if staleness_report is None or staleness_report['flagged_stale'].sum() == 0 else staleness_report['flagged_stale'].sum()} "
              f"flagged as discontinued/stale — see fred_staleness_report.csv) ***")

    # Verification checklist (Phase 1 exit criteria)
    print("\n--- Verification ---")
    hard_lookups = ["EQ | World (ACWI)", "CMD | Gold", "FI | US Treasuries (20+y)",
                     "FI | HY Credit", "CRYPTO | Bitcoin"]
    for col in hard_lookups:
        status = "OK" if col in prices.columns else "MISSING <<<<<"
        print(f"  hard-lookup column '{col}': {status}")
    dxy_candidates = [c for c in returns.columns if "USD Index" in c or "DXY" in c]
    print(f"  USD/DXY-style column for build_hmm_features: {dxy_candidates or 'NONE FOUND <<<<<'}")
    nan_density = returns.isna().mean().sort_values(ascending=False)
    print(f"  Top-5 NaN density columns:\n{nan_density.head(5).to_string()}")


if __name__ == "__main__":
    main()
