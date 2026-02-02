# Project Yield - Implementation Plan

**Version**: 2.1 | **Status**: Planning Phase

---

## Project Overview

### Vision
High-performance financial data analysis platform for stock market research using Polars + Parquet.

### Phase 1 Scope
- Download daily stock prices for S&P 500 (2020-present)
- Fetch quarterly/annual financial reports
- Store in partitioned Parquet files (cloud-ready)
- Calculate 6 core financial ratios (PE, PEG, margins, growth)
- Interactive Plotly visualizations
- Jupyter notebook workflow

### Success Criteria
1. Complete S&P 500 historical data in Parquet files
2. Accurate 6 core ratio calculations (validated ±5% vs Yahoo Finance)
3. Sub-100ms query for single-stock analysis
4. 100% test coverage for ratio calculations
5. Working Jupyter notebooks

---

## Architecture Decisions

### Polars + Parquet (No Database)
**Why**: Simpler (no DB layer), no SQL needed, cloud-native (change `data/` → `s3://`), equal/better performance for time series, Hive partitioning for 5-10x query speedup.

### Other Decisions
| Decision | Choice | Rationale |
|----------|--------|-----------|
| Config | Pydantic Settings + `.env` | Type-safe, single source of truth |
| Metadata | Parquet files | Consistent format, queryable |
| Architecture | Facade + components | Simple API, advanced access available |
| Filesystem | Native WSL | 5-10x faster I/O than /mnt/c/ |
| PEG Ratio | 5-year EPS CAGR | Industry standard convention |

---

## Technology Stack

| Category | Tool | Purpose |
|----------|------|---------|
| Data Processing | Polars 1.18+ | DataFrame library, lazy execution |
| Data Storage | PyArrow 18.1+ | Parquet I/O |
| Data Source | SimFin | Financial data API |
| Validation | Pydantic 2.10+ | Data models, settings |
| Visualization | Plotly 5.24+ | Interactive charts |
| Package Mgmt | uv | Fast dependency management |
| Testing | pytest 8.3+ | Unit/integration tests |
| Code Quality | ruff, mypy | Linting, type checking |

---

## Data Storage

### Directory Structure
```
data/
├── prices/                    # Hive partitioned by ticker + year
│   └── ticker=AAPL/year=2024/data.parquet
├── fundamentals_quarterly/    # Partitioned by ticker
│   └── ticker=AAPL/data.parquet
├── fundamentals_annual/
│   └── ticker=AAPL/data.parquet
└── metadata/
    ├── companies.parquet
    ├── ticker_history.parquet
    ├── corporate_actions.parquet
    └── data_updates.parquet
```

### Schemas (Key Fields)
**prices**: ticker, date, open, high, low, close, adjusted_close, volume

**fundamentals**: ticker, company_id, fiscal_period, report_date, revenue, gross_profit, operating_income, net_income, eps, shares_outstanding, total_assets, total_liabilities, shareholders_equity, operating_cash_flow, capex, free_cash_flow

---

## Implementation Phases

### Phase 0: Validation (Day 1-2)
**Goal**: Validate SimFin API

- [ ] Store SimFin API key in `.env`
- [ ] Implement APIValidator class (`validation/api_validator.py`) - runnable without notebook
- [ ] Test S&P 500 coverage, data completeness, rate limits
- [ ] Document API capabilities and limitations

---

### Phase 1A: Foundation (Day 3-5)
**Goal**: Working project structure with Parquet storage

- [ ] Create project structure in native WSL
- [ ] Setup `pyproject.toml` with uv
- [ ] Implement Pydantic Settings (`config.py`)
- [ ] Implement ParquetWriter (`data/writer.py`) - write partitioned Parquet files
- [ ] Implement DataReader (`data/reader.py`) - Polars-based lazy queries for prices/fundamentals
- [ ] Write unit tests

---

### Phase 1B: Data Ingestion (Week 2)
**Goal**: Download S&P 500 historical data

- [ ] Implement SimFinClient (`data/simfin_client.py`) - rate limiting, caching, retries
- [ ] Download S&P 500 ticker list
- [ ] Implement DataIngestion (`data/ingestion.py`) - `update_all_data()`, incremental updates
- [ ] Download historical data (2020-present): daily prices, quarterly/annual fundamentals
- [ ] Data validation and quality checks

**Output**: ~2,500 price files, 500 fundamentals files each

---

### Phase 1C: Analysis Engine (Week 3)
**Goal**: Calculate financial ratios accurately

- [ ] Implement RatioCalculator (`analysis/ratios.py`):
  - PE Ratio (Trailing): Price / TTM EPS
  - PEG Ratio: PE / 5-year EPS CAGR
  - Operating Margin: Operating Income / Revenue (TTM)
  - EBITDA Margin: EBITDA / Revenue (TTM)
  - Net Profit Margin: Net Income / Revenue (TTM)
  - Revenue Growth Rate: YoY revenue growth
  - EPS Growth Rate: YoY EPS growth
  - R&D Intensity: R&D Expense / Revenue (TTM)
  - CapEx Ratio: Capital Expenditure / Revenue (TTM)
- [ ] Handle missing data gracefully
- [ ] Implement MetricsEngine (`analysis/metrics.py`)
- [ ] Create ProjectYield facade (`core.py`)
- [ ] Write comprehensive unit tests (validate ±5% vs Yahoo Finance)

---

### Phase 1D: Visualization (Week 4)
**Goal**: Interactive charts and notebooks

- [ ] Implement ChartBuilder (`visualization/charts.py`) - price charts, ratio evolution, comparisons
- [ ] Create example notebooks: data exploration, ratio analysis, stock screening
- [ ] Documentation

---

## Testing Strategy

**Unit Tests** (80% coverage): Individual functions with mock data
- `test_ratios.py`, `test_data.py`, `test_simfin_client.py`

**Integration Tests**: End-to-end workflows with real API

**Regression Tests**: Compare against Yahoo Finance (±5% tolerance)

```bash
uv run pytest                     # All tests
uv run pytest -m "not slow"       # Fast tests only
uv run pytest --cov=project_yield # With coverage
```

---

## Performance Targets

| Operation | Target |
|-----------|--------|
| Single stock ratio | < 100ms |
| Screen all S&P 500 | < 5 seconds |
| Chart generation | < 1 second |
| Full data update | < 5 minutes |

**Scale**: 500 stocks × 5 years × 252 days = ~630K price records (~20MB compressed)

---

## Ratio Formulas

| Ratio | Formula |
|-------|---------|
| PE (Trailing) | Price / TTM EPS (sum of last 4 quarters) |
| PEG | PE / (5-year EPS CAGR × 100) |
| Operating Margin | TTM Operating Income / TTM Revenue |
| EBITDA Margin | TTM EBITDA / TTM Revenue |
| Net Profit Margin | TTM Net Income / TTM Revenue |
| Revenue Growth | (Current Q Revenue / Same Q Last Year) - 1 |
| EPS Growth | (Current Q EPS / Same Q Last Year) - 1 |
| R&D Intensity | TTM R&D Expense / TTM Revenue |
| CapEx Ratio | TTM Capital Expenditure / TTM Revenue |

**CAGR**: (Ending / Beginning)^(1/n) - 1

---

## Phase 0 Validation Results

**API validated successfully.** Key findings:

| Metric | Value |
|--------|-------|
| US Companies | 6,521 |
| Price Data | 6.2M rows, 5,864 tickers (2020-03-05 to 2025-02-06) |
| Fundamentals | 52,304 rows, 3,728 tickers |
| R&D Column | `Research & Development` (in income statement) |
| CapEx Column | `Change in Fixed Assets & Intangibles` (in cashflow) |

**Free tier limitations:**
- ~3,700 tickers with fundamentals vs 5,800+ with prices
- Some major companies (AAPL, GOOGL) missing from free tier fundamentals
- GOOGL stored as GOOG

**Answers to open questions:**
1. S&P 500 coverage: ~4/5 sample tickers found, ~5,800 tickers with price data
2. EBITDA: Not directly available, calculate from Operating Income + D&A
3. Forward PE: Not available in free tier
4. Update frequency: Daily updates available
5. Rate limits: Bulk download works without issues

---

## Future Phases (Not in Scope)

| Phase | Focus |
|-------|-------|
| Phase 2 | LLM integration, natural language queries |
| Phase 3 | IBKR portfolio integration |
| Phase 4 | Advanced screening engine |
| Phase 5 | Web interface, multi-user, cloud deployment |

---

## References

- [Polars Docs](https://docs.pola.rs/)
- [SimFin API](https://simfin.readthedocs.io/)
- [PEG Ratio](https://www.wallstreetprep.com/knowledge/peg-ratio/)
