# OpenBB Migration Plan for project_yield

## Context

**The problem.** The current code base uses SimFin's free tier as its sole data source. Investigation confirmed both stated concerns:

- **Coverage gap is real.** SimFin free tier is the root cause of "missing tickers." `data/ingestion.py:185-216` ships a hardcoded ~50-name S&P 500 subset; `data/api_validator.py:325-332` only requires 3/5 sample tickers to pass; only ~45 tickers actually have data on disk.
- **Reliability is mid-tier.** SimFin is real but less institutionally vetted than Polygon/FMP/Intrinio. Empty-DataFrame and `None` fallbacks across `simfin_client.py` and `ratios.py` silently mask gaps rather than raising.
- **No forward-looking analysis.** `analysis/ratios.py` computes PE/PEG using **trailing** EPS only. There is no analyst consensus, no forward EPS, and no DCF logic anywhere in the code.

**What's good.** The architecture is clean and worth keeping. `SimFinClient` (fetch) → `ParquetWriter` (cache) → `DataReader` (read) → `RatioCalculator` / `MetricsEngine` (analysis) → `ChartBuilder` (viz) → `ProjectYield` facade (`core.py`). Only the fetch layer needs to change. Storage, ratios, screening, charts can all stay.

**The decisions.** You confirmed:
1. Provider abstraction, not hard cutover — keep SimFin around as a fallback during validation.
2. Active subscriptions: **Polygon (Massive)** and **FMP Starter** for fundamentals + ratios. **yfinance** (free) for prices.
3. Wire up **both** Polygon and FMP, treat one as primary and use the other for cross-validation / fallback.
4. DCF deferred. Forward PE/PEG now in scope (FMP supports forward EPS).

## Recommended approach: OpenBB Platform as the single abstraction layer, with dual providers

OpenBB Platform (`pip install openbb`) wraps ~100 third-party providers behind standardized data models. Polygon, FMP, and yfinance are all first-class OpenBB providers — provider is a single parameter on every call. This means:

- One dependency, one mental model, one set of standardized columns.
- Both your paid subscriptions (Polygon + FMP) drop in via env-var API keys.
- Cross-provider validation is trivial: same call, change one parameter.

### Provider routing

| Data type | Primary | Secondary (cross-check) | Why |
|---|---|---|---|
| Historical prices (OHLCV) | `yfinance` | `polygon` (spot-check) | yfinance is free and adequate for daily bars. Polygon as a sanity check on splits/dividends. |
| Quarterly fundamentals (income/balance/cash) | **`fmp`** | `polygon` | FMP gives broader coverage and standardized history; Polygon sources directly from SEC filings — ideal for cross-validation. |
| Financial ratios (built-in) | `fmp` | `polygon` | Same as above. |
| Forward EPS / analyst consensus | `fmp` | (none free) | Polygon doesn't expose estimates. |
| Company metadata | `fmp` | `polygon` | Either works. |

**Why FMP as fundamentals primary** (vs Polygon): FMP also covers forward estimates, so making it primary keeps a single provider for both trailing and forward metrics. Polygon's value-add is filing accuracy, which we leverage in the cross-validation layer.

## Implementation plan

### Phase 1: Provider abstraction + OpenBB wired up (~1-2 days)

**New files:**
- `src/project_yield/data/provider.py` — `DataProvider` Protocol defining: `get_prices`, `get_income_statements`, `get_balance_sheets`, `get_cashflow_statements`, `list_available_tickers`, `get_companies`. Optional methods returning `None` when unsupported: `get_forward_eps`, `get_analyst_estimates`.
- `src/project_yield/data/openbb_client.py` — concrete impl. Constructor takes `prices_provider`, `fundamentals_provider`, `estimates_provider` (defaults: `"yfinance"`, `"fmp"`, `"fmp"`). Each method passes `provider=` through to the matching `obb.*` call. Maps OpenBB's standardized columns → the column names `RatioCalculator` already expects. Converts pandas → Polars at the boundary.

**Modified files:**
- [src/project_yield/data/simfin_client.py](src/project_yield/data/simfin_client.py) — make it conform to the new Protocol (mostly already does). No behavior change.
- [src/project_yield/config.py](src/project_yield/config.py) — add:
  - `data_provider: Literal["simfin", "openbb"]` (default `"simfin"` initially, flip to `"openbb"` after validation)
  - `openbb_polygon_api_key: SecretStr | None`
  - `openbb_fmp_api_key: SecretStr | None`
  - `openbb_prices_provider: str = "yfinance"`
  - `openbb_fundamentals_provider: str = "fmp"`
  - `openbb_estimates_provider: str = "fmp"`
- [src/project_yield/data/ingestion.py](src/project_yield/data/ingestion.py) — accept a `DataProvider` instance instead of constructing `SimFinClient` directly. Drop the hardcoded ~50-ticker S&P 500 list — let the provider report coverage.
- [src/project_yield/core.py](src/project_yield/core.py) — `ProjectYield.__init__` instantiates `OpenBBClient` or `SimFinClient` based on settings.
- [pyproject.toml](pyproject.toml) — add `openbb`, `openbb-yfinance`, `openbb-polygon`, `openbb-fmp`.

**Critical column-mapping work.** OpenBB's standardized fundamentals schema is not 1:1 with SimFin. `OpenBBClient` must produce DataFrames with the same column names `RatioCalculator` reads — see [src/project_yield/analysis/ratios.py](src/project_yield/analysis/ratios.py) for the contract: `revenue`, `net_income`, `eps`, `operating_income`, `gross_profit`, `rd_expense`, `capex`, `shares_outstanding`, `total_assets`, `total_liabilities`, `shareholders_equity`, `operating_cash_flow`, `free_cash_flow`, plus `report_date`, `fiscal_year`, `fiscal_period`. Build a small mapping dict and assert required columns present — raise loudly on missing rather than silent empty-DataFrame fallbacks.

### Phase 2: Cross-validation utility (~0.5 day)

**New file:** `src/project_yield/data/cross_validate.py`

A thin helper, not a persistent storage path — keeps complexity out of the hot path:
- `cross_validate_fundamentals(ticker, period="quarterly", lookback=4) -> pl.DataFrame`
  - Pulls the same statement from FMP and Polygon, joins on `(fiscal_year, fiscal_period)`, computes per-field `% diff`.
  - Returns a long-form DataFrame: `field`, `fmp_value`, `polygon_value`, `pct_diff`, `flagged` (`True` if `|pct_diff| > 1%`).
- `cross_validate_prices(ticker, lookback_days=30)` — same pattern for daily closes.
- Wired through `ProjectYield.cross_validate(ticker)` so it's accessible from the notebook.

Storage stays single-source (FMP-primary writes to existing parquet paths). Polygon is pulled on-demand for validation only — avoids partition-by-source schema changes to `ParquetWriter`/`DataReader`.

### Phase 3: Forward-looking metrics (~0.5 day, in scope)

Now that FMP is wired up, forward metrics are cheap to add. Extend [src/project_yield/analysis/ratios.py](src/project_yield/analysis/ratios.py):
- `get_forward_pe(ticker, fiscal_period_offset=1)` — uses `equity.estimates.forward_eps` mean for next FY (or +N FY).
- `get_forward_peg(ticker)` — forward PE / analyst forward EPS growth (instead of historical CAGR).

Surface on `ProjectYield` facade and add to `MetricsEngine.calculate_all_ratios()`.

### Phase 4: Validation & cutover (~0.5 day)

1. Run [notebooks/explore_data_and_ratios.ipynb](notebooks/explore_data_and_ratios.ipynb) against OpenBB+FMP. MSFT trailing PE/op-margin should match SimFin output within ~5%. Investigate larger drift.
2. `pyld.cross_validate("MSFT")` — eyeball the FMP-vs-Polygon diff. Look for systematic bias on revenue/net_income (likely reporting-period alignment) vs noise.
3. Confirm wider coverage on tickers SimFin doesn't have today.
4. Flip default `data_provider="openbb"` in `config.py`.
5. Keep `simfin_client.py` and the `simfin` dep one more release as a safety net — schedule a follow-up agent to delete (`/schedule` candidate).

### Phase 5 (deferred): Bottom-up DCF

Out of scope this round. When picked back up, inputs are ready: historical FCF from `equity.fundamental.cash`, forward growth from FMP estimates, plus a new `DCFModel` class in `src/project_yield/analysis/dcf.py` taking WACC + terminal-growth as parameters.

## Critical files to read before implementing

- [src/project_yield/data/simfin_client.py](src/project_yield/data/simfin_client.py) — current contract to mirror in the Protocol
- [src/project_yield/data/reader.py](src/project_yield/data/reader.py) — DataFrame schema consumers depend on
- [src/project_yield/analysis/ratios.py](src/project_yield/analysis/ratios.py) — column-name contract for fundamentals
- [src/project_yield/data/ingestion.py](src/project_yield/data/ingestion.py) — rip out the hardcoded ticker list here
- [src/project_yield/core.py](src/project_yield/core.py) — facade wiring point

## Verification

1. **Smoke test**: `pyld = ProjectYield(provider="openbb")`, `pyld.update_data(["MSFT", "AAPL", "NVDA"])` → parquet files written under `data/prices/` and `data/fundamentals_quarterly/`.
2. **Trailing parity**: `pyld.get_ratios("MSFT")` returns a dict shaped identically to the SimFin path. PE, op margin, revenue growth match current notebook output (within ~5%).
3. **Forward metrics live**: `pyld.get_forward_pe("MSFT")` and `pyld.get_forward_peg("MSFT")` return floats sourced from FMP analyst consensus.
4. **Cross-validation**: `pyld.cross_validate("MSFT")` produces a DataFrame comparing FMP vs Polygon — `flagged` should be False for most fields on a major ticker.
5. **Coverage win**: pull a ticker not in current SimFin parquet data → succeeds. Run `pyld.screen(...)` across 100+ tickers → coverage dramatically wider than current ~45.
6. **Existing tests pass**: [tests/test_data.py](tests/test_data.py) (reader/writer tests) still green against the new provider.
7. **Notebook re-run**: [notebooks/explore_data_and_ratios.ipynb](notebooks/explore_data_and_ratios.ipynb) Run All — all charts render, ratio tables populate, no errors.

## Out of scope

- Bottom-up DCF (Phase 5, deferred)
- Persistent multi-source storage / per-source partitioning — cross-validation is on-demand, not stored, to avoid schema churn in `ParquetWriter`/`DataReader`
- Plotly / Polars / Parquet replacement — none of those are problems
- AGPLv3 license review — only relevant if `project_yield` is ever distributed publicly
