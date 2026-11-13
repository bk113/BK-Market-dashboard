"""
Refresh tools/fragility_migration/fred_manual/<CODE>.csv from FRED's keyless
CSV endpoint. Run by .github/workflows/daily_dashboard.yml before
bk_market_dashboard.py.

A file is replaced only if the download parses and its last observation is
not older than the existing file's. Existing rows that predate the download's
first date are carried over, because FRED serves some series as a rolling
window. On any failure the old file is kept and the reason is logged. This
script always exits 0.
"""
import io
import os
import sys
import tempfile
from pathlib import Path

FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={code}"
TIMEOUT_S = 30


def _rows(text):
    """{ISO date: raw value string} for the data rows of a FRED two-column CSV."""
    out = {}
    for line in text.splitlines()[1:]:
        parts = line.strip().split(",")
        if len(parts) == 2 and parts[0]:
            out[parts[0]] = parts[1]
    return out


def _last_obs(df, code):
    """(last real observation date, real observation count) for a FRED two-column frame."""
    import pandas as pd
    date_col = "DATE" if "DATE" in df.columns else "observation_date"
    val_col = code if code in df.columns else [c for c in df.columns if c != date_col][0]
    dates = pd.to_datetime(df[date_col])
    vals = pd.to_numeric(df[val_col], errors="coerce")  # '.' -> NaN
    real = dates[vals.notna()]
    return (real.max() if len(real) else None), int(len(real))


def refresh_one(code, folder):
    """Returns (status, detail). status is 'updated', 'unchanged' or 'kept'."""
    import pandas as pd
    import requests

    resp = requests.get(FRED_URL.format(code=code), timeout=TIMEOUT_S)
    resp.raise_for_status()
    new_df = pd.read_csv(io.StringIO(resp.text))
    if new_df.shape[1] != 2 or code not in new_df.columns:
        return "kept", f"unexpected columns {list(new_df.columns)[:4]}"
    new_last, new_n = _last_obs(new_df, code)
    if new_last is None:
        return "kept", "download has no numeric observations"

    fpath = folder / f"{code}.csv"
    out_text = resp.text
    kept_older = 0
    if fpath.exists():
        old_text = fpath.read_text(encoding="utf-8")
        old_last, _ = _last_obs(pd.read_csv(io.StringIO(old_text)), code)
        if old_last is not None and new_last < old_last:
            return "kept", f"download ends {new_last.date()}, older than existing {old_last.date()}"
        # FRED serves some series (the ICE BofA indices) as a rolling window, so
        # existing rows that predate the download's first date are carried over.
        new_rows = _rows(resp.text)
        first_new = min(new_rows)
        older = {d: v for d, v in _rows(old_text).items() if d < first_new}
        kept_older = len(older)
        merged = {**older, **new_rows}
        header = resp.text.splitlines()[0].strip()
        out_text = header + "\n" + "".join(f"{d},{merged[d]}\n" for d in sorted(merged))
        if out_text.split() == old_text.split():
            return "unchanged", f"already at {old_last.date()}"

    fd, tmp = tempfile.mkstemp(dir=folder, suffix=".csv.tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(out_text)
        os.replace(tmp, fpath)
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    extra = f", {kept_older} older rows carried over" if kept_older else ""
    return "updated", f"now ends {new_last.date()} ({new_n} observations in download{extra})"


def main():
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent / "fragility_migration"))
        from build_canonical_panel import (FRED_CODE_TO_NAME, FRED_DISCONTINUED,
                                           FRED_MANUAL_DIR)
    except Exception as e:
        print(f"[FRED]   could not load series list, nothing refreshed: {e}")
        return

    counts = {"updated": 0, "unchanged": 0, "kept": 0, "skipped": 0}
    for code in FRED_CODE_TO_NAME:
        if code in FRED_DISCONTINUED:
            counts["skipped"] += 1
            print(f"[FRED]   {code:<13} skipped  discontinued (expected), existing file kept")
            continue
        try:
            status, detail = refresh_one(code, FRED_MANUAL_DIR)
        except Exception as e:
            status, detail = "kept", f"download failed: {e}"
        counts[status] += 1
        print(f"[FRED]   {code:<13} {status:<9}{detail}")
    print(f"[FRED]   done: {counts['updated']} updated, {counts['unchanged']} unchanged, "
          f"{counts['kept']} kept after a failure, {counts['skipped']} discontinued")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # never fail the dashboard run
        print(f"[FRED]   refresh aborted, existing files kept: {e}")
    sys.exit(0)
