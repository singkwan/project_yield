# Project Yield

High-performance financial data analysis platform for stock market research using Polars + Parquet.

## Quick Start

```bash
# Install dependencies
uv sync

# Create .env from example
cp .env.example .env
# Edit .env with your SimFin API key

# Validate SimFin API
uv run python -m project_yield.validation.api_validator

# Run tests
uv run pytest
```

## Features

- S&P 500 daily price data (2020-present)
- Quarterly/annual financial reports
- 9 financial ratio calculations (PE, PEG, margins, growth, R&D, CapEx)
- Interactive Plotly visualizations
- Cloud-ready Parquet storage with Hive partitioning

## Project Structure

```
src/project_yield/
├── config.py              # Pydantic Settings
├── core.py                # ProjectYield facade
├── validation/            # API validation
├── data/                  # SimFin client, ingestion, Parquet read/write
├── analysis/              # Ratio calculations
└── visualization/         # Plotly charts
```

## License

MIT
