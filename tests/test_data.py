"""Tests for data reader and writer modules."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from project_yield.config import Settings
from project_yield.data.reader import DataReader
from project_yield.data.writer import ParquetWriter


class TestParquetWriter:
    """Tests for ParquetWriter class."""

    def test_init_creates_directories(self, test_settings: Settings) -> None:
        """Test that initialization creates required directories."""
        writer = ParquetWriter(test_settings)

        assert test_settings.prices_path.exists()
        assert test_settings.fundamentals_quarterly_path.exists()
        assert test_settings.fundamentals_annual_path.exists()
        assert test_settings.metadata_path.exists()

    def test_write_prices_single_year(
        self,
        test_settings: Settings,
        sample_prices_df: pl.DataFrame,
    ) -> None:
        """Test writing price data for a single year."""
        writer = ParquetWriter(test_settings)

        paths = writer.write_prices(sample_prices_df, "AAPL")

        assert len(paths) == 1
        assert paths[0].exists()
        assert "ticker=AAPL" in str(paths[0])
        assert "year=2024" in str(paths[0])

        # Verify data
        df = pl.read_parquet(paths[0])
        assert len(df) == 6
        assert "ticker" in df.columns
        assert df["ticker"][0] == "AAPL"

    def test_write_prices_multiple_years(
        self,
        test_settings: Settings,
    ) -> None:
        """Test writing price data spanning multiple years."""
        writer = ParquetWriter(test_settings)

        df = pl.DataFrame({
            "date": pl.Series([
                "2023-12-29", "2024-01-02", "2024-01-03",
            ]).str.to_date(),
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.5, 101.5, 102.5],
            "adjusted_close": [100.0, 101.0, 102.0],
            "volume": [1000000, 1100000, 1200000],
        })

        paths = writer.write_prices(df, "MSFT")

        assert len(paths) == 2  # 2023 and 2024

    def test_write_prices_empty_df(
        self,
        test_settings: Settings,
    ) -> None:
        """Test writing empty DataFrame returns empty list."""
        writer = ParquetWriter(test_settings)
        empty_df = pl.DataFrame(schema={"date": pl.Date, "close": pl.Float64})

        paths = writer.write_prices(empty_df, "AAPL")

        assert paths == []

    def test_write_fundamentals_quarterly(
        self,
        test_settings: Settings,
        sample_fundamentals_df: pl.DataFrame,
    ) -> None:
        """Test writing quarterly fundamentals."""
        writer = ParquetWriter(test_settings)

        path = writer.write_fundamentals_quarterly(sample_fundamentals_df, "AAPL")

        assert path is not None
        assert path.exists()
        assert "ticker=AAPL" in str(path)

        # Verify data
        df = pl.read_parquet(path)
        assert len(df) == 4
        assert df["ticker"][0] == "AAPL"

    def test_write_metadata(
        self,
        test_settings: Settings,
    ) -> None:
        """Test writing metadata file."""
        writer = ParquetWriter(test_settings)

        metadata = pl.DataFrame({
            "ticker": ["AAPL", "MSFT"],
            "company_name": ["Apple Inc.", "Microsoft Corp."],
        })

        path = writer.write_metadata(metadata, "companies")

        assert path.exists()
        assert path.name == "companies.parquet"

    def test_append_prices(
        self,
        test_settings: Settings,
        sample_prices_df: pl.DataFrame,
    ) -> None:
        """Test appending new prices to existing data."""
        writer = ParquetWriter(test_settings)

        # Write initial data
        writer.write_prices(sample_prices_df, "AAPL")

        # Append new data (includes one duplicate date)
        new_df = pl.DataFrame({
            "date": pl.Series(["2024-01-09", "2024-01-10"]).str.to_date(),
            "open": [155.0, 156.0],
            "high": [157.0, 158.0],
            "low": [154.0, 155.0],
            "close": [156.0, 157.0],
            "adjusted_close": [155.5, 156.5],
            "volume": [1400000, 1500000],
        })

        paths = writer.append_prices(new_df, "AAPL")

        # Read back and verify
        df = pl.read_parquet(paths[0])
        assert len(df) == 7  # 6 original + 1 new (duplicate replaced)
        assert df["close"].max() == 157.0

    def test_delete_ticker(
        self,
        test_settings: Settings,
        sample_prices_df: pl.DataFrame,
        sample_fundamentals_df: pl.DataFrame,
    ) -> None:
        """Test deleting all data for a ticker."""
        writer = ParquetWriter(test_settings)

        # Write data
        writer.write_prices(sample_prices_df, "AAPL")
        writer.write_fundamentals_quarterly(sample_fundamentals_df, "AAPL")

        # Delete
        deleted = writer.delete_ticker("AAPL")

        assert deleted == 2  # prices and quarterly
        assert not (test_settings.prices_path / "ticker=AAPL").exists()


class TestDataReader:
    """Tests for DataReader class."""

    def test_get_prices_single_ticker(
        self,
        test_settings: Settings,
        sample_prices_df: pl.DataFrame,
    ) -> None:
        """Test reading prices for a single ticker."""
        writer = ParquetWriter(test_settings)
        writer.write_prices(sample_prices_df, "AAPL")

        reader = DataReader(test_settings)
        df = reader.get_prices(ticker="AAPL").collect()

        assert len(df) == 6
        assert df["ticker"].unique().to_list() == ["AAPL"]

    def test_get_prices_with_date_filter(
        self,
        test_settings: Settings,
        sample_prices_df: pl.DataFrame,
    ) -> None:
        """Test reading prices with date filters."""
        writer = ParquetWriter(test_settings)
        writer.write_prices(sample_prices_df, "AAPL")

        reader = DataReader(test_settings)
        df = reader.get_prices(
            ticker="AAPL",
            start_date=date(2024, 1, 4),
            end_date=date(2024, 1, 8),
        ).collect()

        assert len(df) == 3  # Jan 4, 5, 8

    def test_get_prices_with_columns(
        self,
        test_settings: Settings,
        sample_prices_df: pl.DataFrame,
    ) -> None:
        """Test reading only specific columns."""
        writer = ParquetWriter(test_settings)
        writer.write_prices(sample_prices_df, "AAPL")

        reader = DataReader(test_settings)
        df = reader.get_prices(
            ticker="AAPL",
            columns=["close", "volume"],
        ).collect()

        # Should include ticker and date even if not requested
        assert set(df.columns) == {"ticker", "date", "close", "volume"}

    def test_get_prices_no_data(
        self,
        test_settings: Settings,
    ) -> None:
        """Test reading prices when no data exists."""
        reader = DataReader(test_settings)
        df = reader.get_prices(ticker="AAPL").collect()

        assert df.is_empty()

    def test_get_fundamentals_quarterly(
        self,
        test_settings: Settings,
        sample_fundamentals_df: pl.DataFrame,
    ) -> None:
        """Test reading quarterly fundamentals."""
        writer = ParquetWriter(test_settings)
        writer.write_fundamentals_quarterly(sample_fundamentals_df, "AAPL")

        reader = DataReader(test_settings)
        df = reader.get_fundamentals_quarterly(ticker="AAPL").collect()

        assert len(df) == 4
        assert "revenue" in df.columns

    def test_get_latest_price(
        self,
        test_settings: Settings,
        sample_prices_df: pl.DataFrame,
    ) -> None:
        """Test getting the most recent price."""
        writer = ParquetWriter(test_settings)
        writer.write_prices(sample_prices_df, "AAPL")

        reader = DataReader(test_settings)
        df = reader.get_latest_price("AAPL")

        assert len(df) == 1
        assert df["date"][0] == date(2024, 1, 9)
        assert df["close"][0] == 155.0

    def test_get_ttm_fundamentals(
        self,
        test_settings: Settings,
        sample_fundamentals_df: pl.DataFrame,
    ) -> None:
        """Test calculating TTM fundamentals."""
        writer = ParquetWriter(test_settings)
        writer.write_fundamentals_quarterly(sample_fundamentals_df, "AAPL")

        reader = DataReader(test_settings)
        ttm = reader.get_ttm_fundamentals("AAPL")

        assert len(ttm) == 1
        assert ttm["quarters_included"][0] == 4
        # TTM revenue = 100000 + 110000 + 105000 + 120000 = 435000
        assert ttm["revenue"][0] == 435000.0

    def test_list_tickers(
        self,
        test_settings: Settings,
        sample_prices_df: pl.DataFrame,
    ) -> None:
        """Test listing available tickers."""
        writer = ParquetWriter(test_settings)
        writer.write_prices(sample_prices_df, "AAPL")
        writer.write_prices(sample_prices_df, "MSFT")
        writer.write_prices(sample_prices_df, "GOOGL")

        reader = DataReader(test_settings)
        tickers = reader.list_tickers()

        assert tickers == ["AAPL", "GOOGL", "MSFT"]

    def test_get_date_range(
        self,
        test_settings: Settings,
        sample_prices_df: pl.DataFrame,
    ) -> None:
        """Test getting date range for a ticker."""
        writer = ParquetWriter(test_settings)
        writer.write_prices(sample_prices_df, "AAPL")

        reader = DataReader(test_settings)
        min_date, max_date = reader.get_date_range("AAPL")

        assert min_date == date(2024, 1, 2)
        assert max_date == date(2024, 1, 9)

    def test_has_data(
        self,
        test_settings: Settings,
        sample_prices_df: pl.DataFrame,
    ) -> None:
        """Test checking if data exists."""
        writer = ParquetWriter(test_settings)
        writer.write_prices(sample_prices_df, "AAPL")

        reader = DataReader(test_settings)

        assert reader.has_data("AAPL", "prices") is True
        assert reader.has_data("AAPL", "quarterly") is False
        assert reader.has_data("MSFT", "prices") is False

    def test_get_metadata(
        self,
        test_settings: Settings,
    ) -> None:
        """Test reading metadata."""
        writer = ParquetWriter(test_settings)
        metadata = pl.DataFrame({"ticker": ["AAPL"], "sector": ["Technology"]})
        writer.write_metadata(metadata, "companies")

        reader = DataReader(test_settings)
        df = reader.get_metadata("companies")

        assert len(df) == 1
        assert df["sector"][0] == "Technology"

    def test_get_metadata_not_found(
        self,
        test_settings: Settings,
    ) -> None:
        """Test reading non-existent metadata."""
        reader = DataReader(test_settings)
        df = reader.get_metadata("nonexistent")

        assert df.is_empty()
