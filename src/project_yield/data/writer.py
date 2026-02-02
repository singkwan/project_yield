"""Parquet writer for partitioned financial data storage."""

from datetime import date
from pathlib import Path

import polars as pl
from loguru import logger

from project_yield.config import Settings, get_settings


class ParquetWriter:
    """Writes financial data to partitioned Parquet files."""

    def __init__(self, settings: Settings | None = None) -> None:
        """Initialize writer with settings."""
        self.settings = settings or get_settings()
        self._ensure_directories()

    def _ensure_directories(self) -> None:
        """Create data directories if they don't exist."""
        for path in [
            self.settings.prices_path,
            self.settings.fundamentals_quarterly_path,
            self.settings.fundamentals_annual_path,
            self.settings.metadata_path,
        ]:
            path.mkdir(parents=True, exist_ok=True)

    def _get_partition_path(
        self,
        base_path: Path,
        ticker: str,
        year: int | None = None,
    ) -> Path:
        """Get the partition path for a ticker (and optionally year).

        Args:
            base_path: Base directory for this data type
            ticker: Stock ticker symbol
            year: Optional year for price data partitioning

        Returns:
            Path to the partition directory
        """
        if year is not None:
            return base_path / f"ticker={ticker}" / f"year={year}"
        return base_path / f"ticker={ticker}"

    def write_prices(
        self,
        df: pl.DataFrame,
        ticker: str,
    ) -> list[Path]:
        """Write price data partitioned by ticker and year.

        Args:
            df: DataFrame with columns: date, open, high, low, close, adjusted_close, volume
            ticker: Stock ticker symbol

        Returns:
            List of paths to written files
        """
        if df.is_empty():
            logger.warning(f"Empty DataFrame for {ticker}, skipping write")
            return []

        # Ensure ticker column exists
        if "ticker" not in df.columns:
            df = df.with_columns(pl.lit(ticker).alias("ticker"))

        # Extract year from date for partitioning
        if "year" not in df.columns:
            df = df.with_columns(pl.col("date").dt.year().alias("year"))

        written_paths = []
        years = df["year"].unique().to_list()

        for year in years:
            year_df = df.filter(pl.col("year") == year)
            partition_path = self._get_partition_path(
                self.settings.prices_path, ticker, year
            )
            partition_path.mkdir(parents=True, exist_ok=True)

            file_path = partition_path / "data.parquet"
            year_df.write_parquet(file_path, compression="snappy")
            written_paths.append(file_path)
            logger.debug(f"Wrote {len(year_df)} rows to {file_path}")

        logger.info(f"Wrote {ticker} prices: {len(df)} rows across {len(years)} years")
        return written_paths

    def write_fundamentals_quarterly(
        self,
        df: pl.DataFrame,
        ticker: str,
    ) -> Path | None:
        """Write quarterly fundamentals partitioned by ticker.

        Args:
            df: DataFrame with quarterly financial data
            ticker: Stock ticker symbol

        Returns:
            Path to written file, or None if empty
        """
        if df.is_empty():
            logger.warning(f"Empty quarterly fundamentals for {ticker}, skipping write")
            return None

        # Ensure ticker column exists
        if "ticker" not in df.columns:
            df = df.with_columns(pl.lit(ticker).alias("ticker"))

        partition_path = self._get_partition_path(
            self.settings.fundamentals_quarterly_path, ticker
        )
        partition_path.mkdir(parents=True, exist_ok=True)

        file_path = partition_path / "data.parquet"
        df.write_parquet(file_path, compression="snappy")

        logger.info(f"Wrote {ticker} quarterly fundamentals: {len(df)} rows to {file_path}")
        return file_path

    def write_fundamentals_annual(
        self,
        df: pl.DataFrame,
        ticker: str,
    ) -> Path | None:
        """Write annual fundamentals partitioned by ticker.

        Args:
            df: DataFrame with annual financial data
            ticker: Stock ticker symbol

        Returns:
            Path to written file, or None if empty
        """
        if df.is_empty():
            logger.warning(f"Empty annual fundamentals for {ticker}, skipping write")
            return None

        # Ensure ticker column exists
        if "ticker" not in df.columns:
            df = df.with_columns(pl.lit(ticker).alias("ticker"))

        partition_path = self._get_partition_path(
            self.settings.fundamentals_annual_path, ticker
        )
        partition_path.mkdir(parents=True, exist_ok=True)

        file_path = partition_path / "data.parquet"
        df.write_parquet(file_path, compression="snappy")

        logger.info(f"Wrote {ticker} annual fundamentals: {len(df)} rows to {file_path}")
        return file_path

    def write_metadata(
        self,
        df: pl.DataFrame,
        name: str,
    ) -> Path:
        """Write metadata to a Parquet file.

        Args:
            df: DataFrame with metadata
            name: Name of the metadata file (without extension)

        Returns:
            Path to written file
        """
        file_path = self.settings.metadata_path / f"{name}.parquet"
        df.write_parquet(file_path, compression="snappy")

        logger.info(f"Wrote metadata '{name}': {len(df)} rows to {file_path}")
        return file_path

    def append_prices(
        self,
        df: pl.DataFrame,
        ticker: str,
    ) -> list[Path]:
        """Append new price data to existing partitions.

        Reads existing data, merges with new data (deduplicating by date),
        and writes back.

        Args:
            df: DataFrame with new price data
            ticker: Stock ticker symbol

        Returns:
            List of paths to written files
        """
        if df.is_empty():
            return []

        # Ensure ticker and year columns
        if "ticker" not in df.columns:
            df = df.with_columns(pl.lit(ticker).alias("ticker"))
        if "year" not in df.columns:
            df = df.with_columns(pl.col("date").dt.year().alias("year"))

        written_paths = []
        years = df["year"].unique().to_list()

        for year in years:
            new_data = df.filter(pl.col("year") == year)
            partition_path = self._get_partition_path(
                self.settings.prices_path, ticker, year
            )
            file_path = partition_path / "data.parquet"

            if file_path.exists():
                # Read existing, merge, deduplicate
                existing = pl.read_parquet(file_path)
                merged = pl.concat([existing, new_data])
                merged = merged.unique(subset=["date"], keep="last").sort("date")
            else:
                partition_path.mkdir(parents=True, exist_ok=True)
                merged = new_data.sort("date")

            merged.write_parquet(file_path, compression="snappy")
            written_paths.append(file_path)
            logger.debug(f"Updated {ticker}/{year}: now {len(merged)} rows")

        return written_paths

    def delete_ticker(self, ticker: str) -> int:
        """Delete all data for a ticker.

        Args:
            ticker: Stock ticker symbol

        Returns:
            Number of files deleted
        """
        import shutil

        deleted = 0

        for base_path in [
            self.settings.prices_path,
            self.settings.fundamentals_quarterly_path,
            self.settings.fundamentals_annual_path,
        ]:
            ticker_path = base_path / f"ticker={ticker}"
            if ticker_path.exists():
                shutil.rmtree(ticker_path)
                deleted += 1
                logger.info(f"Deleted {ticker_path}")

        return deleted
