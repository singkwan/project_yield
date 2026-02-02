"""ProjectYield facade - main entry point for the library."""

from datetime import date

import polars as pl
from loguru import logger

from project_yield.analysis.metrics import MetricsEngine
from project_yield.analysis.ratios import RatioCalculator
from project_yield.config import Settings, get_settings
from project_yield.data.ingestion import DataIngestion
from project_yield.data.reader import DataReader
from project_yield.data.writer import ParquetWriter
from project_yield.visualization.charts import ChartBuilder


class ProjectYield:
    """Main facade for Project Yield financial analysis.

    Provides a simple, high-level API for:
    - Data ingestion and management
    - Ratio calculations
    - Stock screening and comparison

    Example:
        py = ProjectYield()

        # Download data for S&P 500
        py.update_data()

        # Get ratios for a ticker
        ratios = py.get_ratios("MSFT")

        # Screen for value stocks
        value_stocks = py.screen(pe_max=20, operating_margin_min=0.1)

        # Compare tickers
        comparison = py.compare(["MSFT", "AAPL", "GOOGL"])
    """

    def __init__(self, settings: Settings | None = None) -> None:
        """Initialize ProjectYield with settings."""
        self.settings = settings or get_settings()
        self._reader = DataReader(self.settings)
        self._writer = ParquetWriter(self.settings)
        self._calculator = RatioCalculator(self.settings)
        self._metrics = MetricsEngine(self.settings)
        self._ingestion = DataIngestion(self.settings)
        self._charts = ChartBuilder(self.settings)

    # --- Data Management ---

    def update_data(
        self,
        tickers: list[str] | None = None,
        start_date: date | None = None,
    ) -> dict:
        """Download and update financial data.

        Args:
            tickers: List of tickers (None for S&P 500)
            start_date: Start date for data (default from settings)

        Returns:
            Summary dict with counts
        """
        if tickers is None:
            return self._ingestion.download_sp500(start_date)
        return self._ingestion.update_all_data(tickers, start_date)

    def update_prices(self, tickers: list[str] | None = None) -> dict:
        """Update only price data (incremental).

        Args:
            tickers: List of tickers (None for all existing)

        Returns:
            Summary dict with counts
        """
        return self._ingestion.update_prices_incremental(tickers)

    def list_tickers(self) -> list[str]:
        """Get list of tickers with data.

        Returns:
            Sorted list of ticker symbols
        """
        return self._reader.list_tickers("prices")

    def data_summary(self) -> dict:
        """Get summary of stored data.

        Returns:
            Dict with counts and date ranges
        """
        return self._ingestion.get_data_summary()

    # --- Price Data ---

    def get_prices(
        self,
        ticker: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pl.DataFrame:
        """Get price data for a ticker.

        Args:
            ticker: Stock ticker symbol
            start_date: Start date filter
            end_date: End date filter

        Returns:
            DataFrame with price data
        """
        return self._reader.get_prices(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
        ).collect()

    def get_latest_price(self, ticker: str) -> dict | None:
        """Get the most recent price for a ticker.

        Args:
            ticker: Stock ticker symbol

        Returns:
            Dict with price data or None
        """
        df = self._reader.get_latest_price(ticker)
        if df.is_empty():
            return None
        return df.row(0, named=True)

    # --- Ratios ---

    def get_ratios(
        self,
        ticker: str,
        as_of_date: date | None = None,
    ) -> dict:
        """Get all financial ratios for a ticker.

        Args:
            ticker: Stock ticker symbol
            as_of_date: Calculate as of this date

        Returns:
            Dict with all ratio values
        """
        return self._calculator.get_all_ratios(ticker, as_of_date)

    def get_pe(self, ticker: str) -> float | None:
        """Get PE ratio for a ticker."""
        return self._calculator.get_pe_ratio(ticker)

    def get_peg(self, ticker: str, years: int = 5) -> float | None:
        """Get PEG ratio for a ticker."""
        return self._calculator.get_peg_ratio(ticker, years)

    # --- Screening & Comparison ---

    def screen(
        self,
        tickers: list[str] | None = None,
        pe_min: float | None = None,
        pe_max: float | None = None,
        peg_min: float | None = None,
        peg_max: float | None = None,
        operating_margin_min: float | None = None,
        operating_margin_max: float | None = None,
        net_profit_margin_min: float | None = None,
        revenue_growth_min: float | None = None,
    ) -> pl.DataFrame:
        """Screen stocks based on ratio criteria.

        Args:
            tickers: List of tickers to screen (None for all)
            pe_min/pe_max: PE ratio bounds
            peg_min/peg_max: PEG ratio bounds
            operating_margin_min/max: Operating margin bounds
            net_profit_margin_min: Minimum net profit margin
            revenue_growth_min: Minimum revenue growth

        Returns:
            DataFrame of stocks meeting criteria
        """
        filters = {}

        if pe_min is not None or pe_max is not None:
            filters["pe_ratio"] = (pe_min, pe_max)
        if peg_min is not None or peg_max is not None:
            filters["peg_ratio"] = (peg_min, peg_max)
        if operating_margin_min is not None or operating_margin_max is not None:
            filters["operating_margin"] = (operating_margin_min, operating_margin_max)
        if net_profit_margin_min is not None:
            filters["net_profit_margin"] = (net_profit_margin_min, None)
        if revenue_growth_min is not None:
            filters["revenue_growth"] = (revenue_growth_min, None)

        return self._metrics.screen_stocks(filters, tickers)

    def compare(
        self,
        tickers: list[str],
        metrics: list[str] | None = None,
    ) -> pl.DataFrame:
        """Compare metrics across tickers.

        Args:
            tickers: List of tickers to compare
            metrics: List of metrics (None for all)

        Returns:
            DataFrame with comparison
        """
        return self._metrics.compare_tickers(tickers, metrics)

    def rank(
        self,
        metric: str,
        tickers: list[str] | None = None,
        ascending: bool = True,
        top_n: int | None = 10,
    ) -> pl.DataFrame:
        """Rank stocks by a metric.

        Args:
            metric: Metric to rank by
            tickers: List of tickers (None for all)
            ascending: Sort ascending
            top_n: Return top N results

        Returns:
            DataFrame with rankings
        """
        return self._metrics.rank_by_metric(metric, tickers, ascending, top_n)

    def valuation_summary(self, ticker: str) -> dict:
        """Get comprehensive valuation summary.

        Args:
            ticker: Stock ticker symbol

        Returns:
            Dict with valuation data
        """
        return self._metrics.get_valuation_summary(ticker)

    # --- Access to underlying components ---

    @property
    def reader(self) -> DataReader:
        """Access the DataReader for custom queries."""
        return self._reader

    @property
    def calculator(self) -> RatioCalculator:
        """Access the RatioCalculator for custom calculations."""
        return self._calculator

    @property
    def metrics(self) -> MetricsEngine:
        """Access the MetricsEngine for batch operations."""
        return self._metrics

    @property
    def charts(self) -> ChartBuilder:
        """Access the ChartBuilder for visualizations."""
        return self._charts
