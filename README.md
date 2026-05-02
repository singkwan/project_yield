# Project Yield

Personal financial data analysis platform for equity research. Built on the OpenBB Platform for data ingestion, with local Parquet storage and Polars for analytics.

## Quick start

```bash
# 1. Install
uv sync

# 2. Add your API keys (gitignored — never committed)
cat > config.yaml <<'EOF'
FMP_API_KEY: your_fmp_key_here
MASSIVE_API_KEY: your_polygon_key_here  # optional, used only for cross-validation
EOF

# 3. Use it
uv run python
```

```python
from project_yield import ProjectYield

pyld = ProjectYield()

# Ingest a few tickers (writes Parquet to data/)
pyld.update_data(["MSFT", "AAPL", "NVDA"])

# Trailing ratios (computed locally from Parquet)
pyld.get_ratios("MSFT")
# {'pe_ratio': 36.16, 'forward_pe': 21.57, 'forward_peg': 1.38,
#  'operating_margin': 0.4336, 'net_profit_margin': 0.3524, ...}

# Cross-check our PE against FMP's published value
pyld.get_pe("MSFT")                          # 36.16 — our methodology
pyld.calculator.get_provider_pe_ratio("MSFT") # 24.58 — FMP's annual published

# Risk metrics (Sharpe/Sortino/kurtosis on local price series)
pyld.risk_summary("MSFT")

# Universe-wide discovery (find new tickers)
pyld.discover(filters_per_provider="...")

# Sector-wide valuations
pyld.sector_groups()
```

## How ratios are computed (read this first)

> **Default path: self-computed.** Every `pyld.get_pe()`, `pyld.get_ratios()`, `pyld.get_forward_pe()`, `pyld.screen()`, etc. derives ratios from raw statements stored locally in Parquet — using `RatioCalculator` in [src/project_yield/analysis/ratios.py](src/project_yield/analysis/ratios.py). One methodology, applied identically to trailing and forward.
>
> **FMP's pre-computed ratios are stored on disk too, but only as a second opinion.** They're reachable only through methods that explicitly start with `get_provider_*`, so you can never accidentally mix them with the self-computed values.

| Call | Source | Use it for |
|---|---|---|
| `pyld.get_pe("MSFT")` | **Self-computed** | The canonical PE — your methodology |
| `pyld.get_ratios("MSFT")` | **All self-computed** | Default ratio sheet |
| `pyld.get_forward_pe("MSFT")` | **Self-computed** (price ÷ FMP consensus EPS) | Forward PE consistent with trailing PE |
| `pyld.calculator.get_provider_pe_ratio("MSFT")` | FMP's published value | Cross-check against ours |
| `pyld.client.get_provider_forward_pe("MSFT")` | FMP's published value | Cross-check against ours |

When the two values diverge, that's a methodology gap to investigate (e.g., FMP's annual PE uses fiscal-year-end price, ours uses current price; FMP basic vs diluted EPS, etc.) — not a bug.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    OpenBB Platform (fetch only)                 │
│  yfinance · FMP · Polygon · finviz · quantitative · ...         │
└────────────────────┬────────────────────────────────────────────┘
                     │
        ┌────────────▼─────────────┐
        │ data/openbb_client.py    │ Single client, provider-routed
        └────────────┬─────────────┘
                     │
        ┌────────────▼─────────────┐
        │ data/ingestion.py        │ Per-ticker loop
        └────────────┬─────────────┘
                     │
        ┌────────────▼─────────────┐
        │ data/writer.py           │ Hive-partitioned Parquet
        │   data/prices/ticker=X/  │
        │   data/fundamentals_*/   │
        │   data/ratios_*/         │
        └────────────┬─────────────┘
                     │
        ┌────────────▼─────────────┐
        │ data/reader.py           │ Lazy Polars reads
        └────────────┬─────────────┘
                     │
   ┌─────────────────┼─────────────────┬─────────────────┐
   │                 │                 │                 │
┌──▼──────────┐ ┌────▼────────┐ ┌──────▼──────┐ ┌────────▼────────┐
│ ratios.py   │ │ metrics.py  │ │ risk.py     │ │ visualization/  │
│ Trailing +  │ │ Screening,  │ │ Sharpe,     │ │ Plotly charts   │
│ forward     │ │ comparison  │ │ Sortino,    │ │                 │
│ PE/PEG/...  │ │             │ │ kurtosis... │ │                 │
└─────────────┘ └─────────────┘ └─────────────┘ └─────────────────┘
                     │
        ┌────────────▼─────────────┐
        │ core.py — ProjectYield   │ Public facade
        └──────────────────────────┘
```

## Provider routing

OpenBB is purely a fetch SDK. There's no DB, no scheduler, no built-in cache. We persist everything to local Parquet ourselves. Each fetch endpoint is routed to a specific provider — defaults below, configurable in `config.yaml`.

| Data | Default provider | Why |
|---|---|---|
| Prices (OHLCV) | `fmp` | Same source as fundamentals — consistent splits/dividends |
| Income / balance / cashflow (raw statements) | `fmp` | Broad coverage, standardized schema, also covers forward EPS |
| Forward EPS / consensus estimates | `fmp` | Polygon doesn't expose estimates in OpenBB |
| Sector compare (`equity.compare.groups`) | `finviz` | Only provider OpenBB routes this endpoint to |
| Universe screener (`equity.screener`) | `finviz` | Free, broad coverage |
| **Validation: prices** | `yfinance` | Free, independent of FMP for spot-checks |
| **Validation: fundamentals** | `polygon` | Direct from SEC filings — independent reference |

To swap any of these, edit `config.yaml`:
```yaml
OPENBB_PRICES_PROVIDER: yfinance
OPENBB_FUNDAMENTALS_PROVIDER: polygon
OPENBB_PRICES_VALIDATION_PROVIDER: fmp
```

## Key design decisions

### 1. OpenBB is the single fetch layer; everything else is ours
OpenBB has no storage, no scheduler, no incremental update concept. It's pure fetch-on-demand. We own:
- `ParquetWriter` / `DataReader` (Hive-partitioned local cache)
- `DataIngestion` (per-ticker orchestrator)
- `RatioCalculator` / `MetricsEngine` / `RiskMetrics` (analytics over local data)

This means every ratio query reads from local Parquet — fast, offline, zero API cost. Only fresh fetches and forward-looking metrics hit the network.

### 2. Two function categories in OpenBB, used differently
- **Provider-fetch** (`equity.fundamental.*`, `equity.estimates.*`, `equity.compare.*`, `equity.screener`) — every call hits the provider API. We persist the raw output to Parquet at ingest time.
- **Pure-compute** (`technical.*`, `quantitative.*`) — local computation on data we pass in. We feed them DataFrames from our local Parquet via `DataReader`. No API cost, no provider key needed.

### 3. Self-compute ratios from raw, use provider ratios only for cross-check
The default path for *every* ratio call is self-computation from raw statements in local Parquet (see "How ratios are computed" above). FMP's published ratios are cached too, but they're only reachable through explicitly-named `get_provider_*` methods — never mixed into the default API. This guarantees the trailing-vs-forward comparison is apples-to-apples and our methodology is the single source of truth.

### 4. Forward PE/PEG: extract consensus, compute ratio ourselves
We pull the forward EPS *mean* from `obb.equity.estimates.forward_eps` and divide local current price by it. We don't use FMP's pre-computed `forward_pe` for the actual value — same reason as #3 (consistency with trailing). FMP's value is available via `client.get_provider_forward_pe()` for cross-checking only.

### 5. Cross-validation is on-demand, not stored
`pyld.cross_validate("MSFT")` pulls the same statement from FMP and Polygon, joins on `(fiscal_year, fiscal_period)`, and reports per-field % diff with a `flagged` column. Polygon is *not* persistently stored — no schema drift, no partition complexity. Use it when a number looks suspicious.

### 6. Hard cutover from SimFin
SimFin (free tier) was the previous source. It limited coverage to ~45 tickers and lacked forward-looking data. The migration was a hard cutover — no compatibility shim, no Protocol abstraction. One client, one source-of-truth provider, much less code to maintain.

## Real-world tier limits

These show up in practice, not bugs:

- **FMP Starter** blocks `equity.fundamental.ratios` at `period="quarter"` (annual works). Our cached-ratio code falls back to annual automatically.
- **FMP Starter** caps `equity.estimates.forward_eps` `limit` at 10 fiscal periods.
- **Polygon** doesn't expose pre-computed ratios or analyst estimates in OpenBB at all.
- **Polygon vs FMP fiscal-year labeling**: Polygon labels by calendar year, FMP by fiscal year. For Microsoft (FY starts July), the same Q3 2026 quarter is labeled FY2026 by FMP and FY2025 by Polygon. Cross-validation join keys may not align across the two for off-calendar fiscal years.
- **Polygon doesn't cover Foreign Private Issuers (FPIs)**: Polygon's fundamentals API parses 10-K and 10-Q filings only. NASDAQ/NYSE-listed companies that file **20-F** instead (foreign-incorporated issuers — GRAB, BABA, TSM, SE, BIDU, NIO, ASML, NVO, TM, SONY, etc.) return `Results not found` from Polygon. FMP has them via broader data feeds. Cross-validation against Polygon is only meaningful for US-domestic 10-K/10-Q filers.

## Project structure

```
src/project_yield/
├── config.py                  # Pydantic Settings + YAML loader
├── core.py                    # ProjectYield facade
├── data/
│   ├── openbb_client.py       # OpenBB SDK wrapper, provider-routed
│   ├── ingestion.py           # Per-ticker ingest orchestrator
│   ├── writer.py              # ParquetWriter (Hive-partitioned)
│   ├── reader.py              # DataReader (lazy Polars)
│   └── cross_validate.py      # FMP vs Polygon spot-check helper
├── analysis/
│   ├── ratios.py              # PE, PEG, margins, growth, forward PE/PEG
│   ├── metrics.py             # Screening, comparison, sector groups
│   └── risk.py                # Sharpe, Sortino, kurtosis, skewness
└── visualization/
    └── charts.py              # Plotly charts
```

## Storage layout

All under `data/` (gitignored):
```
data/
├── prices/ticker=MSFT/year=2024/data.parquet     # daily OHLCV, partitioned by ticker+year
├── fundamentals_quarterly/ticker=MSFT/data.parquet
├── fundamentals_annual/ticker=MSFT/data.parquet
├── ratios_quarterly/ticker=MSFT/data.parquet     # FMP pre-computed (when available)
├── ratios_annual/ticker=MSFT/data.parquet
└── metadata/                                     # ticker lists, etc.
```

## Common workflows

```python
# Ingest then explore
pyld = ProjectYield()
pyld.update_data(["MSFT", "AAPL", "GOOG"])

# Side-by-side PE methodology check
print("custom (TTM EPS):", pyld.get_pe("MSFT"))
print("FMP published:", pyld.calculator.get_provider_pe_ratio("MSFT"))
print("forward (NTM):", pyld.get_forward_pe("MSFT"))

# Cross-source validation
diff = pyld.cross_validate("MSFT", statement="income", lookback=4)
flagged = diff.filter(diff["flagged"])  # rows where FMP and Polygon disagree by >1%

# Discover candidates → ingest them
candidates = pyld.discover()  # finviz screener
new_tickers = candidates["symbol"].head(20).to_list()
pyld.update_data(new_tickers)

# Risk metrics on local prices
pyld.risk_summary("MSFT")
# {'sharpe_latest': 0.013, 'sortino_latest': 0.73, 'kurtosis_latest': -0.79, ...}
```

## Tests

```bash
uv run pytest
```

Reader/writer tests cover the storage layer. Analysis layer is exercised via the notebook (`notebooks/explore_data_and_ratios.ipynb`).

## License

MIT
