"""Pytest configuration and fixtures."""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from project_yield.config import Settings


@pytest.fixture
def temp_data_dir(tmp_path: Path) -> Path:
    """Create a temporary data directory."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return data_dir


@pytest.fixture
def test_settings(temp_data_dir: Path) -> Settings:
    """Create test settings with temporary data directory."""
    return Settings(
        simfin_api_key="test-key",
        data_path=temp_data_dir,
    )


@pytest.fixture
def sample_prices_df() -> pl.DataFrame:
    """Create sample price data for testing."""
    return pl.DataFrame({
        "date": pl.Series([
            "2024-01-02", "2024-01-03", "2024-01-04",
            "2024-01-05", "2024-01-08", "2024-01-09",
        ]).str.to_date(),
        "open": [150.0, 151.0, 152.0, 151.5, 153.0, 154.0],
        "high": [152.0, 153.0, 154.0, 153.0, 155.0, 156.0],
        "low": [149.0, 150.0, 151.0, 150.0, 152.0, 153.0],
        "close": [151.0, 152.0, 151.5, 152.5, 154.0, 155.0],
        "adjusted_close": [150.5, 151.5, 151.0, 152.0, 153.5, 154.5],
        "volume": [1000000, 1100000, 1050000, 1200000, 1150000, 1300000],
    })


@pytest.fixture
def sample_fundamentals_df() -> pl.DataFrame:
    """Create sample quarterly fundamentals data for testing."""
    return pl.DataFrame({
        "fiscal_period": pl.Series([
            "2023-03-31", "2023-06-30", "2023-09-30", "2023-12-31",
        ]).str.to_date(),
        "fiscal_year": [2023, 2023, 2023, 2023],
        "revenue": [100000.0, 110000.0, 105000.0, 120000.0],
        "gross_profit": [40000.0, 44000.0, 42000.0, 48000.0],
        "operating_income": [25000.0, 27000.0, 26000.0, 30000.0],
        "net_income": [20000.0, 22000.0, 21000.0, 25000.0],
        "eps": [2.0, 2.2, 2.1, 2.5],
        "research_development": [5000.0, 5500.0, 5200.0, 6000.0],
    })
