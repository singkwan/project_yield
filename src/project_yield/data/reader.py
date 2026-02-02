"""Data reader for partitioned Parquet financial data."""

from datetime import date
from pathlib import Path

import polars as pl
from loguru import logger

from project_yield.config import Settings, get_settings


class DataReader:
    """Reads financial data from partitioned Parquet files using lazy execution."""

    def __init__(self, settings: Settings | None = None) -> None:
        """Initialize reader with settings."""
        self.settings = settings or get_settings()

    def _get_parquet_pattern(self, base_path: Path) -> str:
        """Get glob pattern for all Parquet files under a base path."""
        return str(base_path / "**" / "*.parquet")

    def _has_parquet_files(self, base_path: Path) -> bool:
        """Check if any parquet files exist under the base path."""
        return any(base_path.glob("**/*.parquet"))

    def get_prices(
        self,
        ticker: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        columns: list[str] | None = None,
    ) -> pl.LazyFrame:
        """Get price data with optional filtering.

        Uses lazy execution for efficient queries - filters are pushed down
        to only read necessary partitions.

        Args:
            ticker: Filter by ticker symbol (None for all)
            start_date: Filter by start date (inclusive)
            end_date: Filter by end date (inclusive)
            columns: Specific columns to select (None for all)

        Returns:
            LazyFrame with price data
        """
        if not self._has_parquet_files(self.settings.prices_path):
            return pl.LazyFrame()

        pattern = self._get_parquet_pattern(self.settings.prices_path)
        lf = pl.scan_parquet(pattern)

        # Apply filters
        if ticker is not None:
            lf = lf.filter(pl.col("ticker") == ticker)

        if start_date is not None:
            lf = lf.filter(pl.col("date") >= start_date)

        if end_date is not None:
            lf = lf.filter(pl.col("date") <= end_date)

        if columns is not None:
            # Always include ticker and date for context
            cols = list(set(columns) | {"ticker", "date"})
            available = lf.collect_schema().names()
            cols = [c for c in cols if c in available]
            lf = lf.select(cols)

        return lf

    def get_fundamentals_quarterly(
        self,
        ticker: str | None = None,
        columns: list[str] | None = None,
    ) -> pl.LazyFrame:
        """Get quarterly fundamentals data.

        Args:
            ticker: Filter by ticker symbol (None for all)
            columns: Specific columns to select (None for all)

        Returns:
            LazyFrame with quarterly fundamentals
        """
        if not self._has_parquet_files(self.settings.fundamentals_quarterly_path):
            return pl.LazyFrame()

        pattern = self._get_parquet_pattern(self.settings.fundamentals_quarterly_path)
        lf = pl.scan_parquet(pattern)

        if ticker is not None:
            lf = lf.filter(pl.col("ticker") == ticker)

        if columns is not None:
            cols = list(set(columns) | {"ticker"})
            available = lf.collect_schema().names()
            cols = [c for c in cols if c in available]
            lf = lf.select(cols)

        return lf

    def get_fundamentals_annual(
        self,
        ticker: str | None = None,
        columns: list[str] | None = None,
    ) -> pl.LazyFrame:
        """Get annual fundamentals data.

        Args:
            ticker: Filter by ticker symbol (None for all)
            columns: Specific columns to select (None for all)

        Returns:
            LazyFrame with annual fundamentals
        """
        if not self._has_parquet_files(self.settings.fundamentals_annual_path):
            return pl.LazyFrame()

        pattern = self._get_parquet_pattern(self.settings.fundamentals_annual_path)
        lf = pl.scan_parquet(pattern)

        if ticker is not None:
            lf = lf.filter(pl.col("ticker") == ticker)

        if columns is not None:
            cols = list(set(columns) | {"ticker"})
            available = lf.collect_schema().names()
            cols = [c for c in cols if c in available]
            lf = lf.select(cols)

        return lf

    def get_metadata(self, name: str) -> pl.DataFrame:
        """Read a metadata file.

        Args:
            name: Name of the metadata file (without extension)

        Returns:
            DataFrame with metadata (empty if not found)
        """
        file_path = self.settings.metadata_path / f"{name}.parquet"

        if not file_path.exists():
            logger.warning(f"Metadata file not found: {file_path}")
            return pl.DataFrame()

        return pl.read_parquet(file_path)

    def get_latest_price(self, ticker: str) -> pl.DataFrame:
        """Get the most recent price for a ticker.

        Args:
            ticker: Stock ticker symbol

        Returns:
            DataFrame with single row of latest price data
        """
        return (
            self.get_prices(ticker=ticker)
            .sort("date", descending=True)
            .head(1)
            .collect()
        )

    def get_ttm_fundamentals(
        self,
        ticker: str,
        as_of_date: date | None = None,
    ) -> pl.DataFrame:
        """Get trailing twelve months (TTM) fundamentals.

        Sums the last 4 quarters of data for income statement items.

        Args:
            ticker: Stock ticker symbol
            as_of_date: Calculate TTM as of this date (default: latest)

        Returns:
            DataFrame with TTM values
        """
        lf = self.get_fundamentals_quarterly(ticker=ticker)

        if as_of_date is not None:
            # Filter to quarters ending before as_of_date
            lf = lf.filter(pl.col("fiscal_period") <= as_of_date)

        # Get last 4 quarters
        df = lf.sort("fiscal_period", descending=True).head(4).collect()

        if len(df) < 4:
            logger.warning(f"{ticker}: Only {len(df)} quarters available for TTM")

        if df.is_empty():
            return pl.DataFrame()

        # Sum numeric columns for TTM
        numeric_cols = [
            c for c in df.columns
            if df[c].dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]
            and c not in ["fiscal_year", "year"]
        ]

        ttm_values = {col: df[col].sum() for col in numeric_cols}
        ttm_values["ticker"] = ticker
        ttm_values["quarters_included"] = len(df)

        return pl.DataFrame([ttm_values])

    def list_tickers(self, data_type: str = "prices") -> list[str]:
        """List all available tickers.

        Args:
            data_type: One of "prices", "quarterly", "annual"

        Returns:
            List of ticker symbols
        """
        if data_type == "prices":
            base_path = self.settings.prices_path
        elif data_type == "quarterly":
            base_path = self.settings.fundamentals_quarterly_path
        elif data_type == "annual":
            base_path = self.settings.fundamentals_annual_path
        else:
            raise ValueError(f"Unknown data_type: {data_type}")

        tickers = []
        for path in base_path.iterdir():
            if path.is_dir() and path.name.startswith("ticker="):
                ticker = path.name.replace("ticker=", "")
                tickers.append(ticker)

        return sorted(tickers)

    def get_date_range(self, ticker: str) -> tuple[date | None, date | None]:
        """Get the date range of available price data for a ticker.

        Args:
            ticker: Stock ticker symbol

        Returns:
            Tuple of (min_date, max_date), or (None, None) if no data
        """
        df = (
            self.get_prices(ticker=ticker, columns=["date"])
            .select(
                pl.col("date").min().alias("min_date"),
                pl.col("date").max().alias("max_date"),
            )
            .collect()
        )

        if df.is_empty():
            return None, None

        min_date = df["min_date"][0]
        max_date = df["max_date"][0]

        return min_date, max_date

    def has_data(self, ticker: str, data_type: str = "prices") -> bool:
        """Check if data exists for a ticker.

        Args:
            ticker: Stock ticker symbol
            data_type: One of "prices", "quarterly", "annual"

        Returns:
            True if data exists
        """
        if data_type == "prices":
            base_path = self.settings.prices_path
        elif data_type == "quarterly":
            base_path = self.settings.fundamentals_quarterly_path
        elif data_type == "annual":
            base_path = self.settings.fundamentals_annual_path
        else:
            raise ValueError(f"Unknown data_type: {data_type}")

        ticker_path = base_path / f"ticker={ticker}"
        return ticker_path.exists() and any(ticker_path.glob("**/*.parquet"))
