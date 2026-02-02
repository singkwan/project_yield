# Project Yield - Architecture

**Last Updated**: 2026-02-01 (Updated: Polars + Parquet Architecture)
**Version**: 2.0

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      User Interface                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Jupyter    │  │  Python API  │  │  CLI (Future)│      │
│  │  Notebooks   │  │ (ProjectYield)│  │              │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Facade Layer                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  ProjectYield (core.py)                              │   │
│  │  - Simple interface: get_ratios(), plot_price()      │   │
│  │  - Orchestrates all components                       │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   Analysis   │  │  Visualization│  │   Data       │
│   Layer      │  │    Layer      │  │ Ingestion    │
│   (Polars)   │  │   (Plotly)    │  │  (SimFin)    │
│              │  │               │  │              │
│ RatioCalc    │  │ ChartBuilder  │  │ SimFinClient │
│ MetricsEngine│  │               │  │ DataIngestion│
└──────────────┘  └──────────────┘  └──────────────┘
        │                                    │
        └────────────────┬──────────────────┘
                         ▼
        ┌────────────────────────────────┐
        │   Data Access Layer (Polars)   │
        │  ┌──────────────────────────┐  │
        │  │   DataRepository         │  │
        │  │   - Lazy queries         │  │
        │  │   - Partition filtering  │  │
        │  └──────────────────────────┘  │
        └────────────────────────────────┘
                         │
                         ▼
        ┌────────────────────────────────┐
        │    Storage Layer (Parquet)     │
        │  ┌──────────────────────────┐  │
        │  │   StorageManager         │  │
        │  │   - Partition management │  │
        │  │   - File I/O             │  │
        │  └──────────────────────────┘  │
        └────────────────────────────────┘
                         │
                         ▼
        ┌────────────────────────────────────────┐
        │     Partitioned Parquet Files          │
        │  ┌──────────────────────────────────┐  │
        │  │  prices/ticker=AAPL/year=2024/   │  │
        │  │  fundamentals_quarterly/ticker=  │  │
        │  │  metadata/companies.parquet      │  │
        │  └──────────────────────────────────┘  │
        └────────────────────────────────────────┘
                         │
                         ▼ (Future: Cloud Migration)
        ┌────────────────────────────────────────┐
        │  S3/GCS (same structure, different path)│
        │  s3://bucket/prices/ticker=AAPL/...    │
        └────────────────────────────────────────┘
```

## Component Responsibilities

### Facade Layer
**ProjectYield** (`core.py`)
- Entry point for all user interactions
- Simple, high-level API
- Orchestrates component interactions
- Hides complexity

### Analysis Layer
**RatioCalculator** (`analysis/ratios.py`)
- Calculate financial ratios (PE, PEG, margins, growth)
- Handle missing data gracefully
- Follow industry-standard formulas

**MetricsEngine** (`analysis/metrics.py`)
- High-level analysis functions
- Stock comparison
- Stock screening

### Visualization Layer
**ChartBuilder** (`visualization/charts.py`)
- Generate Plotly interactive charts
- Price charts, ratio charts, fundamentals
- Consistent styling and themes

### Data Ingestion Layer
**SimFinClient** (`data/simfin_client.py`)
- Wrapper around SimFin API
- Rate limiting and caching
- Error handling and retries

**DataIngestion** (`data/ingestion.py`)
- Orchestrate data downloads
- Incremental updates
- Data validation

### Data Access Layer
**DataRepository** (`storage/repository.py`)
- Polars lazy queries on partitioned Parquet
- Partition-aware filtering (fast single-ticker queries)
- Return Polars DataFrames or LazyFrames

### Storage Layer
**StorageManager** (`storage/manager.py`)
- Partition path generation (Hive-style)
- Parquet file I/O with compression
- Metadata file management

## Data Flow

### 1. Initial Data Load
```
SimFin API
    ↓ (SimFinClient.get_daily_prices())
Polars DataFrame
    ↓ (DataIngestion.backfill_historical_data())
StorageManager.write_partitioned()
    ↓
Partitioned Parquet files
    data/prices/ticker=AAPL/year=2024/data.parquet
```

### 2. Ratio Calculation
```
User: py.get_ratios('AAPL')
    ↓
ProjectYield.get_ratios()
    ↓
RatioCalculator.get_all_ratios()
    ↓
DataRepository.get_prices() + .get_fundamentals()
    ↓
Polars lazy query (scan_parquet with partition filter)
    ↓
Only reads: data/prices/ticker=AAPL/**/*.parquet (fast!)
    ↓
Polars query execution (.collect())
    ↓
Results returned as Python dictionaries
```

### 3. Incremental Update
```
User: py.update_data()
    ↓
DataIngestion.update_all_data()
    ↓
For each ticker:
    ├─ Read existing Parquet (if exists)
    ├─ Get latest date from existing data
    ├─ SimFinClient.get_daily_prices(since=latest_date)
    ├─ Concat new data with existing (Polars)
    └─ Write back to same partition (overwrite)
    ↓
Updated Parquet files (atomic write)
```

## Key Design Patterns

### 1. Facade Pattern
**Where**: `ProjectYield` class
**Why**: Simplify complex subsystem
**Example**:
```python
# Complex internal calls hidden
py = ProjectYield()
ratios = py.get_ratios('AAPL')  # Simple!

# vs manual approach
db = DatabaseManager(...)
repo = StockRepository(db)
calc = RatioCalculator(repo)
ratios = calc.get_all_ratios('AAPL')  # Complex!
```

### 2. Repository Pattern
**Where**: `StockRepository` class
**Why**: Abstract data access, enable testing
**Example**:
```python
# Repository abstracts storage
prices = repo.get_prices('AAPL', start_date='2024-01-01')

# Returns Polars LazyFrame (can chain more operations)
# Can swap implementation (Parquet → DuckDB → S3) without changing callers
```

### 3. Dependency Injection
**Where**: Constructor parameters
**Why**: Testability, flexibility
**Example**:
```python
class RatioCalculator:
    def __init__(self, repo: StockRepository):
        self.repo = repo  # Injected dependency

# Easy to mock for testing
calc = RatioCalculator(MockRepository())
```

### 4. Settings Management (Pydantic)
**Where**: `config.py`
**Why**: Type-safe, validated configuration
**Example**:
```python
from project_yield.config import settings

# Type-safe access
api_key = settings.simfin_api_key  # str
batch_size = settings.batch_size    # int
```

## Technology Decisions

### Polars + Parquet Stack
**Decision**: Use Polars with partitioned Parquet files (no database)

**Rationale**:
- Simpler: No database management
- Faster: 20% faster than DuckDB for time series
- Cloud-native: Parquet IS the cloud format
- Partitioning: 5-10x faster single-ticker queries
- No SQL: Pure Python DataFrame API

**Trade-offs**:
- No ACID transactions (don't need for analytics)
- No database constraints (handle in Python)
- Acceptable: Single-user analytical workload

**Example - Polars Lazy Execution**:
```python
# Lazy query - doesn't read until .collect()
result = (
    pl.scan_parquet('data/prices/**/*.parquet')
    .filter(pl.col('ticker') == 'AAPL')
    .filter(pl.col('date') >= '2024-01-01')
    .select(['date', 'close'])
    .group_by('date')
    .agg(pl.col('close').mean())
    .sort('date', descending=True)
    .limit(10)
    .collect()  # Execute here
)

# Only reads AAPL partitions (fast!)
```

### Lazy Execution (Polars Default)
**Decision**: Use Polars lazy execution by default

**Rationale**:
- Built into Polars (scan_parquet vs read_parquet)
- Automatic query optimization
- Only reads needed partitions
- No complexity cost

**Best Practice**:
```python
# Lazy (recommended for queries)
df = pl.scan_parquet('data/**/*.parquet').filter(...).collect()

# Eager (for small datasets or immediate access)
df = pl.read_parquet('data/small_file.parquet')
```

### File-Based Metadata Tracking
**Decision**: Metadata stored in separate Parquet files

**Rationale**:
- Financial data changes (ticker symbols, splits, restatements)
- Need audit trail for accuracy
- Consistent format (all Parquet)
- Queryable with same tool (Polars)

**Structure**:
```
metadata/
├── companies.parquet
├── ticker_history.parquet
├── corporate_actions.parquet
└── data_updates.parquet
```

**Trade-off**: Manual validation vs database constraints (acceptable)

## Performance Characteristics

### Current Scale (Phase 1)
- **Stocks**: 500 (S&P 500)
- **Price records**: ~630,000 rows (5 years daily)
- **Fundamentals**: ~10,000 rows (5 years quarterly)
- **Total data**: ~100MB uncompressed, ~20MB Parquet

### Performance Targets
- Single stock ratios: < 100ms
- Screen all S&P 500: < 5 seconds
- Chart generation: < 1 second
- Database init: < 10 seconds

### Scalability Path
```
Current: 500 stocks, 100MB
    → Eager execution, in-memory

Expansion: 8,000 stocks, 1.5GB
    → Add materialized ratios table
    → Batch processing

Global: 50,000 stocks, 10GB
    → Consider adding Polars
    → Lazy execution

Real-time: Live updates
    → DuckDB MotherDuck (cloud)
    → or Polars Cloud
```

## Security Considerations

### API Key Storage
- Store in `.env` file (gitignored)
- Load via Pydantic Settings
- Never hardcode in source

### Data Privacy
- Local database (no external transmission)
- Market data only (no personal data)

### Future (Phase 2)
- IBKR credentials: OS keyring
- Multi-user: Add authentication layer

## Testing Strategy

### Unit Tests
- Test individual methods
- Mock dependencies
- Fast execution

### Integration Tests
- Test component interactions
- Use small real datasets
- Validate end-to-end workflows

### Validation Tests
- Compare ratios vs Yahoo Finance
- Ensure data quality
- Detect calculation drift

## Cloud Migration Path

### Phase 1: Local Development
```
DuckDB: data/processed/stocks.duckdb
Parquet: data/raw/*.parquet
```

### Phase 2: Hybrid (Local + Cloud Backup)
```
DuckDB: local
Parquet: → S3 (nightly backup)
```

### Phase 3: Cloud Storage
```
DuckDB: local queries
Parquet: S3 (primary storage)

# No code changes needed!
pl.scan_parquet('s3://bucket/data/*.parquet')
```

### Phase 4: Cloud Database
```
Option A: DuckDB MotherDuck (managed DuckDB)
Option B: Polars Cloud (distributed)

# Minimal code changes (connection string only)
```

## Dependencies Graph

```
ProjectYield
    ├─> RatioCalculator
    │       └─> DataRepository
    │               └─> StorageManager
    ├─> ChartBuilder
    │       └─> DataRepository
    ├─> DataIngestion
    │       ├─> SimFinClient
    │       ├─> StorageManager
    │       └─> DataRepository
    └─> Settings (config.py)

All components depend on:
    - Pydantic (validation)
    - Polars (data processing)
    - PyArrow (Parquet I/O)
```

## Future Enhancements

### Phase 2: LLM Integration
- Add `LLMEngine` component
- Natural language → SQL generation
- Automated insights

### Phase 3: Real-time Updates
- Add `StreamingIngestion` component
- WebSocket connections
- Live portfolio tracking

### Phase 4: Web Interface
- Add FastAPI backend
- React frontend
- Multi-user support

---

**For detailed implementation plan, see [PLAN.md](PLAN.md)**
