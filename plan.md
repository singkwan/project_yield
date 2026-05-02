# OpenBB Migration Plan for project_yield (revised)

## Context

**The problem.** The current code base uses SimFin's free tier as its sole data source. Investigation confirmed:

- **Coverage gap is real.** SimFin free tier ships a hardcoded ~50-name S&P 500 subset in `data/ingestion.py:185-216`; only ~45 tickers actually have data on disk.
- **Reliability is mid-tier.** SimFin is real but less institutionally vetted than Polygon/FMP/Intrinio.
- **No forward-looking analysis.** `analysis/ratios.py` computes PE/PEG using **trailing** EPS only. No analyst consensus, no forward EPS, no DCF.

**The clean architecture beneath the bad data source is worth keeping.** `SimFinClient` (fetch) → `ParquetWriter` (cache) → `DataReader` (read) → `RatioCalculator` / `MetricsEngine` (analysis) → `ChartBuilder` (viz) → `ProjectYield` facade. Only the fetch layer needs to change.

**Decisions:**
1. **Hard cutover.** Drop SimFin entirely — no Protocol, no compatibility shim. One data source: OpenBB.
2. **Active subscriptions:** Polygon (rebranded Massive) and FMP Starter for fundamentals + ratios. yfinance (free) for prices.
3. **Wire up both** Polygon and FMP via OpenBB. Treat FMP as primary, use Polygon for cross-validation.
4. **DCF deferred.** Forward PE/PEG in scope (FMP supports forward EPS).

## How OpenBB fits — what we use, what we own

OpenBB Platform (`pip install openbb`) has two distinct function categories that interact with our local Parquet very differently:

**Category A — Provider-fetch functions** (`equity.fundamental.*`, `equity.estimates.*`, `equity.compare.*`, `equity.screener`):
- Hit the provider API every call. No caching between calls (CLI's OBBject registry is in-session only).
- Each call costs API quota + network latency.
- Cannot be pointed at our local Parquet — there's no "local provider" mode.

**Category B — Pure-compute functions** (`technical.*`, `quantitative.*`):
- Accept a `data` parameter (pandas DataFrame or list-of-dicts) plus a `target` column name.
- No API call, no provider key required. Pure local computation.
- Run perfectly against our Parquet data: read → `.to_pandas()` → pass in.

Example for `technical.rsi`:
```python
prices_pl = reader.get_prices(ticker="AAPL", start_date=date(2024,1,1)).collect()
prices_pd = prices_pl.to_pandas()
rsi_df = obb.technical.rsi(data=prices_pd, target="close", length=14).to_df()
```

Example for `quantitative.performance.sharpe_ratio`:
```python
returns_pd = prices_pd.assign(ret=prices_pd["close"].pct_change()).dropna()
sharpe = obb.quantitative.performance.sharpe_ratio(
    data=returns_pd, target="ret", rfr=0.04
).results
```

The implication for Category A (ratios, forward PE, sector compare): if we want them available locally without re-fetching, we **persist them to Parquet at ingest time** alongside raw statements. There is no other shortcut — OpenBB has no concept of "use my local Parquet as a provider."

**Other key facts:**
- **OpenBB has no ingestion pipeline.** Fetch-on-demand, per call. No scheduler, no incremental update primitives. Ingestion is something we build on top.
- **OpenBB has no storage layer.** No DB, no warehouse. The third-party `openbb-store` extension is pickle-based caching for small reusable resources (ticker lists, static assets) — explicitly not for terabytes of analytical time-series. Storage is our responsibility.

**This is good news for `project_yield`.** Your existing Parquet + Hive-partitioned storage is exactly what an OpenBB-based stack would build. We keep it. The architecture after migration:

```
OpenBB (fetch only — wraps yfinance/FMP/Polygon under one interface)
    ↓
src/project_yield/data/openbb_client.py    ← new (the only new file)
    ↓
ParquetWriter            ← keep (storage — OpenBB has no equivalent)
    ↓
DataReader               ← keep (lazy Polars reads from Parquet)
    ↓
RatioCalculator          ← keep + extend with forward PE/PEG
MetricsEngine            ← keep
ChartBuilder             ← keep
    ↓
ProjectYield facade      ← keep + thin wiring change
```

### Provider routing inside OpenBBClient

| Data type | Provider | Why |
|---|---|---|
| Historical prices (OHLCV) | `yfinance` | Free, adequate for daily bars. |
| Quarterly fundamentals (income/balance/cash) | `fmp` | Broad coverage, standardized history. |
| Forward EPS / analyst consensus | `fmp` | Polygon doesn't expose estimates. |
| Cross-validation (on-demand) | `polygon` | Sourced from SEC filings — independent reference. |

## What gets deleted

Hard cutover means real code reduction:

- **Delete** [src/project_yield/data/simfin_client.py](src/project_yield/data/simfin_client.py) (334 lines)
- **Delete** [src/project_yield/data/api_validator.py](src/project_yield/data/api_validator.py) — SimFin-specific validation
- **Delete** the hardcoded ~50-ticker S&P 500 list and `get_sp500_tickers` / `download_sp500` methods in [src/project_yield/data/ingestion.py](src/project_yield/data/ingestion.py) — let the user pass tickers explicitly, or pull S&P 500 constituents from OpenBB
- **Drop** `simfin>=0.9.0` from pyproject.toml
- **Unpin** pandas (`pandas>=2.0.0,<3.0.0` → `pandas>=2.0.0`) — the pin existed for SimFin compatibility

## Implementation plan

### Phase 1: Build OpenBBClient + wire it in + persist FMP ratios (~1.5 days)

**New file:**
- `src/project_yield/data/openbb_client.py` — single class `OpenBBClient` with:
  - `__init__(self, settings, prices_provider="yfinance", fundamentals_provider="fmp", estimates_provider="fmp")` — sets OpenBB credentials from settings into `obb.user.credentials.*`
  - `get_prices(ticker, start_date=None, end_date=None) -> pl.DataFrame` — calls `obb.equity.price.historical(symbol=ticker, provider=...)`, normalizes columns
  - `get_income_statements(ticker, period="quarterly", limit=20) -> pl.DataFrame`
  - `get_balance_sheets(ticker, period="quarterly", limit=20) -> pl.DataFrame`
  - `get_cashflow_statements(ticker, period="quarterly", limit=20) -> pl.DataFrame`
  - `get_fundamentals(ticker, period="quarterly", limit=20) -> pl.DataFrame` — joins the three statements + computes `free_cash_flow`
  - `get_provider_ratios(ticker, period="quarterly", limit=20) -> pl.DataFrame` — wraps `obb.equity.fundamental.ratios` (PE, PB, PS, current ratio, quick ratio, profitability, leverage)
  - `get_provider_metrics(ticker, period="quarterly", limit=20) -> pl.DataFrame` — wraps `obb.equity.fundamental.metrics` (ROE, ROA, ROIC, EPS growth, revenue growth)
  - `get_forward_eps(ticker) -> pl.DataFrame` — `obb.equity.estimates.forward_eps`
  - `get_company_profile(ticker) -> dict`
  - `list_sp500() -> list[str]` — `obb.equity.market_snapshots` or hardcoded fallback

**Provider ratios cached at ingest — for validation, not as the source of truth.** FMP's pre-computed ratios are pulled at ingest time and persisted to Parquet alongside raw statements. The point is to enable side-by-side comparison: *our hand-computed ratios* (the source of truth, derived from raw statements via `RatioCalculator`) vs *FMP's published ratios* (an independent reference). When they diverge, that's a flag to investigate methodology differences (e.g., diluted vs basic shares, restated periods).
- New Parquet partition: `data/ratios_quarterly/ticker=X/data.parquet` (and `ratios_annual/`)
- `ParquetWriter.write_provider_ratios(df, ticker, period)` — new method following the `write_fundamentals_*` pattern
- `DataReader.get_provider_ratios(ticker, period)` — new lazy reader
- Settings: `provider_ratios_quarterly_path`, `provider_ratios_annual_path`
- `DataIngestion` pulls ratios + metrics per ticker alongside fundamentals
- `RatioCalculator` exposes both:
  - Custom methods (`get_pe_ratio`, `get_peg_ratio`, etc.) — primary, derived from raw statements
  - Provider methods (`get_provider_pe_ratio`, `get_provider_peg_ratio`, etc.) — read FMP's value from cached Parquet for comparison only

**Column-mapping contract.** OpenBB's standardized columns differ by provider, but for FMP they're close to:
- Income: `revenue`, `gross_profit`, `operating_income`, `net_income`, `basic_earnings_per_share` / `diluted_earnings_per_share`, `research_and_development_expense`
- Balance: `total_assets`, `total_liabilities`, `total_equity`, `weighted_average_diluted_shares_outstanding`
- Cash: `net_cash_from_operating_activities`, `capital_expenditure`, `net_cash_from_investing_activities`, `net_cash_from_financing_activities`

Build a small mapping dict that targets the column names the existing [src/project_yield/analysis/ratios.py](src/project_yield/analysis/ratios.py) already reads: `revenue`, `net_income`, `eps`, `operating_income`, `gross_profit`, `rd_expense`, `capex`, `shares_outstanding`, `total_assets`, `total_liabilities`, `shareholders_equity`, `operating_cash_flow`, `free_cash_flow`. Plus key dimensions: `ticker`, `report_date`, `fiscal_year`, `fiscal_period`. **Raise on missing required columns** — no silent empty DataFrames.

**Modified files:**
- [src/project_yield/config.py](src/project_yield/config.py) — replace `simfin_api_key` with:
  - `openbb_polygon_api_key: SecretStr`
  - `openbb_fmp_api_key: SecretStr`
  - `openbb_prices_provider: str = "yfinance"`
  - `openbb_fundamentals_provider: str = "fmp"`
  - `openbb_estimates_provider: str = "fmp"`
- [src/project_yield/data/ingestion.py](src/project_yield/data/ingestion.py) — instantiate `OpenBBClient` directly (no Protocol). Rewrite `update_all_data` to per-ticker loop (no more "download all then filter" — that was a SimFin quirk). Delete `get_sp500_tickers` and `download_sp500` (or replace with `OpenBBClient.list_sp500()`).
- [src/project_yield/core.py](src/project_yield/core.py) — `ProjectYield.__init__` instantiates `OpenBBClient`. Remove the SimFin-specific `update_data` default behavior.
- [pyproject.toml](pyproject.toml) — remove `simfin`, unpin pandas, add `openbb`, `openbb-equity`, `openbb-yfinance`, `openbb-polygon`, `openbb-fmp`, `openbb-finviz` (for screener), `openbb-quantitative` (for Phase 6).
- [tests/test_data.py](tests/test_data.py) — fixtures don't depend on SimFin; reader/writer tests should still pass. Update any conftest references.

**Delete:**
- [src/project_yield/data/simfin_client.py](src/project_yield/data/simfin_client.py)
- [src/project_yield/data/api_validator.py](src/project_yield/data/api_validator.py)
- `data/simfin_cache/` directory (gitignored anyway, just stops being written to)

### Phase 2: Cross-validation utility (~0.5 day)

**New file:** `src/project_yield/data/cross_validate.py`

Thin on-demand helper, not a persistent storage path:
- `cross_validate_fundamentals(ticker, period="quarterly", lookback=4) -> pl.DataFrame` — pulls the same statement from FMP and Polygon, joins on `(fiscal_year, fiscal_period)`, returns per-field `% diff` with `flagged` column for `|diff| > 1%`.
- `cross_validate_prices(ticker, lookback_days=30)` — same pattern for daily closes.
- Wire through `ProjectYield.cross_validate(ticker)` for notebook access.

Storage stays single-source (FMP writes to existing Parquet paths). Polygon pulled on-demand for validation only.

### Phase 3: Forward-looking metrics (~0.5 day)

**Methodology principle: self-compute, don't use FMP's pre-computed forward PE.** We extract only the raw consensus inputs (forward EPS mean, EPS growth) from OpenBB and compute the ratios ourselves. This keeps the methodology identical to our trailing PE/PEG (current price ÷ EPS), so the trailing-vs-forward comparison is apples-to-apples.

Extend [src/project_yield/analysis/ratios.py](src/project_yield/analysis/ratios.py):
- `get_forward_pe(ticker, fiscal_period_offset=1) -> float | None` — pulls `obb.equity.estimates.forward_eps(ticker)`, takes the mean estimate for FY+offset, divides current price (from local Parquet) by it. We do **not** call `obb.equity.estimates.forward_pe` for the value.
- `get_forward_peg(ticker) -> float | None` — forward PE ÷ analyst-implied forward EPS growth (from successive forward EPS estimates).

For validation: also extend `OpenBBClient` with `get_provider_forward_pe(ticker)` that pulls FMP's pre-computed `obb.equity.estimates.forward_pe` so the notebook can sanity-check our self-computed value against FMP's. Same pattern as the trailing-ratio cross-check (custom vs provider).

Surface on `ProjectYield` facade and add to `MetricsEngine.calculate_all_ratios()`.

### Phase 4: Sector comparisons via OpenBB (~0.5 day)

Replace [src/project_yield/analysis/metrics.py](src/project_yield/analysis/metrics.py) `MetricsEngine.get_sector_averages` (which today requires many ingested tickers to compute averages locally) with a wrapper that delegates to `obb.equity.compare.groups`.

- New method on `OpenBBClient`: `get_sector_groups(group="sector", metric="valuation") -> pl.DataFrame`
- Rewrite `MetricsEngine.get_sector_averages` to delegate to it (or add `get_sector_groups` alongside, deprecate the old one)
- Pulls on-demand (no caching) — sector data is queried rarely and changes daily anyway
- Surface on facade: `ProjectYield.sector_groups(group="sector")`

### Phase 5: Discovery screener (~0.5 day)

Add a universe-wide screener for finding new tickers to ingest, distinct from the existing `screen()` which only filters already-ingested data.

- New method on `OpenBBClient`: `screen_universe(**filters) -> pl.DataFrame` wrapping `obb.equity.screener` (provider `finviz` or `fmp`)
- New facade method: `ProjectYield.discover(market_cap_min=None, pe_max=None, sector=None, recommendation=None, ...) -> pl.DataFrame`
- Returns ticker list + headline metrics; user can then `pyld.update_data(tickers=discovered)` to ingest

### Phase 6: Risk / quantitative metrics wrapper (~0.5 day)

New file: `src/project_yield/analysis/risk.py` with `RiskMetrics` class.

- Methods read prices from `DataReader`, compute returns, delegate to OpenBB pure-compute functions (no API calls):
  - `sharpe_ratio(ticker, rfr=0.04, window=None) -> float | pl.DataFrame` — `obb.quantitative.performance.sharpe_ratio`
  - `sortino_ratio(ticker, target_return=0.0) -> float | pl.DataFrame` — `obb.quantitative.performance.sortino_ratio`
  - `kurtosis(ticker, window=None)` — `obb.quantitative.kurtosis`
  - `skewness(ticker, window=None)` — `obb.quantitative.skew`
  - `normality_test(ticker)` — `obb.quantitative.normality`
- New facade methods on `ProjectYield`: `sharpe(ticker)`, `sortino(ticker)`, `risk_summary(ticker)`
- New dependency: `openbb-quantitative`

### Phase 7: Validation (~0.5 day)

1. Run [notebooks/explore_data_and_ratios.ipynb](notebooks/explore_data_and_ratios.ipynb) end-to-end against OpenBB+FMP. Sanity-check MSFT trailing PE/op margin against publicly reported numbers (yfinance, Yahoo Finance) since SimFin output is no longer the reference.
2. **Cross-source ratio comparison**: in the notebook, side-by-side `pyld.get_pe("MSFT")` (custom) vs `pyld.calculator.get_provider_pe_ratio("MSFT")` (cached FMP). Should match closely; large diffs flag a methodology mismatch.
3. `pyld.cross_validate("MSFT")` — eyeball FMP-vs-Polygon diff for raw statements.
4. `pyld.sector_groups(group="sector")` — confirm sector roll-up returns reasonable data.
5. `pyld.discover(pe_max=15, sector="technology")` — confirm screener returns candidates.
6. `pyld.sharpe("MSFT")` and `pyld.sortino("MSFT")` — confirm risk metrics return floats.
7. Pull tickers SimFin couldn't (e.g. mid/small cap) — confirm OpenBB+FMP delivers wider coverage.

### Phase 8 (deferred): Bottom-up DCF

Out of scope this round. When picked back up, inputs are ready: historical FCF from Parquet, forward growth from FMP estimates, plus a new `DCFModel` class in `src/project_yield/analysis/dcf.py` taking WACC + terminal-growth as parameters.

## Critical files to read before implementing

- [src/project_yield/data/simfin_client.py](src/project_yield/data/simfin_client.py) — the method shapes to mirror in `OpenBBClient` (then delete this file)
- [src/project_yield/data/reader.py](src/project_yield/data/reader.py) — DataFrame schema consumers depend on
- [src/project_yield/analysis/ratios.py](src/project_yield/analysis/ratios.py) — column-name contract for fundamentals
- [src/project_yield/data/ingestion.py](src/project_yield/data/ingestion.py) — rewrite to per-ticker loop, drop SP500 list
- [src/project_yield/core.py](src/project_yield/core.py) — facade wiring point

## Verification

1. **Smoke test**: `pyld = ProjectYield()`, `pyld.update_data(["MSFT", "AAPL", "NVDA"])` → Parquet files written under `data/prices/`, `data/fundamentals_quarterly/`, `data/ratios_quarterly/`.
2. **Trailing parity vs Yahoo Finance**: `pyld.get_ratios("MSFT")` PE / op margin / revenue growth match Yahoo Finance within ~5%.
3. **Custom vs cached FMP ratios** (trailing): `pyld.get_pe("MSFT")` (self-computed) vs `pyld.calculator.get_provider_pe_ratio("MSFT")` (cached FMP) should match within ~5%.
4. **Forward metrics live**: `pyld.get_forward_pe("MSFT")` (self-computed from consensus EPS) and `pyld.get_forward_peg("MSFT")` return floats. Cross-check against `pyld.client.get_provider_forward_pe("MSFT")` (FMP's pre-computed forward PE) — should match closely; large diffs flag methodology differences (e.g., FY+1 vs NTM definition).
5. **Cross-validation**: `pyld.cross_validate("MSFT")` shows minimal flagged fields on a major ticker.
6. **Sector roll-up**: `pyld.sector_groups(group="sector")` returns valuation metrics by sector.
7. **Discovery**: `pyld.discover(pe_max=15)` returns a candidate ticker list.
8. **Risk metrics**: `pyld.sharpe("MSFT")` and `pyld.sortino("MSFT")` return floats.
9. **Coverage win**: pull a small/mid-cap ticker SimFin didn't have → succeeds.
10. **Existing tests pass**: [tests/test_data.py](tests/test_data.py) reader/writer tests still green.
11. **Notebook re-run**: [notebooks/explore_data_and_ratios.ipynb](notebooks/explore_data_and_ratios.ipynb) Run All — all charts render.

## Out of scope

- Bottom-up DCF (Phase 8, deferred)
- Technical indicators wrapper (`obb.technical.*` for RSI/MACD/Bollinger/etc.) — explicitly excluded; can be added later as a follow-up phase if needed
- Plotly / Polars / Parquet replacement — none of those are problems
- AGPLv3 license review — only relevant if `project_yield` is distributed publicly
