# BK-Market-dashboard
tracking indices for investments

---

## docs/data/market_pulse.json

Machine-readable market overview written on every dashboard run by `bk_market_dashboard.py`. Intended for the AI Leverage Loop brief to read instead of scraping `docs/index.html`. Intentionally **excludes** the Fragility score, Fragility instruments, regime label, market tone, and BK Composite.

### Top-level fields

| Field | Description |
|---|---|
| `metadata` | Run provenance (see below) |
| `fear_greed` | Fear & Greed composite and 7 components |
| `breadth` | Above-50dma / above-200dma counts, overall and by asset class |
| `moves` | Per-instrument close and changes |
| `headline_moves` | Curated subset of key cross-asset tickers |
| `top_movers` | 5 largest 1-day gainers and 5 largest 1-day losers |
| `errors` | List of section-level error strings if any section failed; absent when clean |

---

### `metadata`

| Field | Type | Unit / Notes |
|---|---|---|
| `generated_at` | string | ISO 8601 UTC timestamp of the run, e.g. `"2026-11-11T23:04:17Z"` |
| `price_as_of_date` | string | YYYY-MM-DD of the latest date with valid closes for ≥ 50 % of instruments |
| `fresh` | bool | `true` if `price_as_of_date` equals the most recently completed NYSE session at run time |
| `instrument_count` | integer | Number of instruments with a valid close on `price_as_of_date` |
| `source` | string | Always `"Yahoo Finance"` |

---

### `fear_greed`

| Field | Type | Unit / Notes |
|---|---|---|
| `score` | float | Composite 0–100 (0 = Extreme Fear, 100 = Extreme Greed); rounded to 2 dp |
| `label` | string | One of: `"Extreme Fear"`, `"Fear"`, `"Neutral"`, `"Greed"`, `"Extreme Greed"` |
| `components` | object | Seven sub-scores (0–100 each): `Volatility`, `Momentum`, `Breadth`, `Safe Haven`, `Junk Bonds`, `Strength`, `Term Structure` |
| `score_1d_ago` | float \| null | F&G score computed on the panel one trading day earlier; null if history is insufficient |
| `score_5d_ago` | float \| null | F&G score computed on the panel five trading days earlier; null if history is insufficient |

---

### `breadth`

| Field | Type | Unit / Notes |
|---|---|---|
| `overall.n_instruments` | integer | Count of breadth-eligible instruments (excludes `RATES` and `VOL` sections) |
| `overall.above_50dma_n` | integer | Count above 50-day moving average |
| `overall.above_50dma_pct` | float | Percentage above 50dma; rounded to 2 dp |
| `overall.above_200dma_n` | integer | Count above 200-day moving average |
| `overall.above_200dma_pct` | float | Percentage above 200dma; rounded to 2 dp |
| `by_asset_class` | object | Same four fields per asset-class key (e.g. `"EQ"`, `"FI"`, `"CMD"`, `"CRYPTO"`, `"FX"`, `"ALT"`) |
| `excluded_sections` | array | Sections omitted from breadth: `["RATES", "VOL"]` |

**Why RATES and VOL are excluded:** sovereign yield indices and volatility indices are not price-return series with meaningful above/below MA breadth signals.

---

### `moves` and `headline_moves`

Each entry is an object:

| Field | Type | Unit / Notes |
|---|---|---|
| `name` | string | Human-readable instrument name |
| `ticker` | string | Yahoo Finance ticker symbol |
| `asset_class` | string | UNIVERSE section (e.g. `"EQ_US"`, `"RATES"`, `"CMD"`, `"FX"`) |
| `close` | float \| null | Last valid close in native units; rounded to 2 dp |
| `unit` | string | `"%"` for price instruments; `"bp"` for yield indices; `"level"` for vol indices |
| `chg_1d` | float \| null | 1-day change. **%** instruments: percentage (e.g. `+1.23`). **bp** instruments: basis-point change (e.g. `+10.5`). **level** instruments: absolute level change. Null if no fresh close yet (official-close-only; never intraday). Rounded to 2 dp for %, 1 dp for bp. |
| `chg_5d` | float \| null | 5-trading-day change, same units as `chg_1d` |
| `chg_mtd` | float \| null | Month-to-date change from first valid close of the current calendar month, same units |

`headline_moves` is a filtered subset of `moves` for key cross-asset tickers: 2Y and 10Y Treasury yield, HYG (HY credit), LQD (IG credit), DXY, USD/JPY, Gold, WTI oil proxy (BNO), VIX, SPY, QQQ, SOXX (semiconductors), Bitcoin.

---

### `top_movers`

| Field | Type | Notes |
|---|---|---|
| `gainers` | array (5) | Largest 1-day % gainers among rankable (investable) instruments, descending |
| `losers` | array (5) | Largest 1-day % losers, ascending |

Each entry has the same fields as `moves`. Yield indices, vol indices, non-investable FX pairs, and computed proxies are excluded (see `RANKING_EXCLUSIONS` in `bk_market_dashboard.py`).

---

### Rounding rules

| Measure | Decimal places |
|---|---|
| Prices and closes | 2 dp |
| Percentage changes | 2 dp |
| Basis-point changes | 1 dp |
| F&G scores | 2 dp |
| Breadth percentages | 2 dp |

`null` in JSON means the value was unavailable (NaN, missing close, insufficient history). There are no `NaN` literals in the file.
