"""Metrics engine for batch financial analysis."""

from datetime import date

import polars as pl
from loguru import logger

from project_yield.analysis.ratios import RatioCalculator
from project_yield.config import Settings, get_settings
from project_yield.data.reader import DataReader


class MetricsEngine:
    """Engine for batch financial metrics calculations.

    Provides methods to calculate ratios across multiple tickers
    and perform comparative analysis.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        """Initialize engine with settings."""
        self.settings = settings or get_settings()
        self.reader = DataReader(self.settings)
        self.calculator = RatioCalculator(self.settings)

    def calculate_all_ratios(
        self,
        tickers: list[str] | None = None,
        as_of_date: date | None = None,
    ) -> pl.DataFrame:
        """Calculate all ratios for multiple tickers.

        Args:
            tickers: List of tickers (None for all available)
            as_of_date: Calculate as of this date (default: latest)

        Returns:
            DataFrame with ratios for each ticker
        """
        if tickers is None:
            tickers = self.reader.list_tickers("prices")

        results = []
        for ticker in tickers:
            try:
                ratios = self.calculator.get_all_ratios(ticker, as_of_date)
                results.append(ratios)
            except Exception as e:
                logger.warning(f"Error calculating ratios for {ticker}: {e}")
                results.append({"ticker": ticker, "error": str(e)})

        return pl.DataFrame(results)

    def screen_stocks(
        self,
        filters: dict | None = None,
        tickers: list[str] | None = None,
    ) -> pl.DataFrame:
        """Screen stocks based on ratio criteria.

        Args:
            filters: Dict of {ratio_name: (min, max)} tuples
                    Use None for unbounded
            tickers: List of tickers to screen (None for all)

        Returns:
            DataFrame of stocks meeting criteria

        Example:
            filters = {
                "pe_ratio": (0, 25),       # PE between 0 and 25
                "operating_margin": (0.1, None),  # Margin > 10%
            }
        """
        df = self.calculate_all_ratios(tickers)

        if filters is None:
            return df

        for column, (min_val, max_val) in filters.items():
            if column not in df.columns:
                logger.warning(f"Column {column} not in results, skipping")
                continue

            if min_val is not None:
                df = df.filter(pl.col(column) >= min_val)
            if max_val is not None:
                df = df.filter(pl.col(column) <= max_val)

        return df

    def compare_tickers(
        self,
        tickers: list[str],
        metrics: list[str] | None = None,
    ) -> pl.DataFrame:
        """Compare specific metrics across tickers.

        Args:
            tickers: List of tickers to compare
            metrics: List of metrics to include (None for all)

        Returns:
            DataFrame with comparison
        """
        df = self.calculate_all_ratios(tickers)

        if metrics is not None:
            # Always include ticker
            cols = ["ticker"] + [m for m in metrics if m in df.columns]
            df = df.select(cols)

        return df

    def get_sector_averages(
        self,
        tickers: list[str],
    ) -> dict:
        """Calculate average ratios for a group of tickers.

        Args:
            tickers: List of tickers in the sector/group

        Returns:
            Dict with average values for each ratio
        """
        df = self.calculate_all_ratios(tickers)

        if df.is_empty():
            return {}

        # Calculate means for numeric columns
        numeric_cols = [
            c for c in df.columns
            if c not in ["ticker", "as_of_date", "error"]
            and df[c].dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]
        ]

        averages = {}
        for col in numeric_cols:
            values = df[col].drop_nulls()
            if len(values) > 0:
                averages[col] = round(values.mean(), 4)

        averages["ticker_count"] = len(tickers)
        return averages

    def rank_by_metric(
        self,
        metric: str,
        tickers: list[str] | None = None,
        ascending: bool = True,
        top_n: int | None = None,
    ) -> pl.DataFrame:
        """Rank stocks by a specific metric.

        Args:
            metric: Metric to rank by
            tickers: List of tickers (None for all)
            ascending: Sort ascending (True) or descending (False)
            top_n: Return only top N results (None for all)

        Returns:
            DataFrame sorted by metric with rank column
        """
        df = self.calculate_all_ratios(tickers)

        if metric not in df.columns:
            logger.error(f"Metric {metric} not found")
            return pl.DataFrame()

        # Filter out nulls for ranking
        df = df.filter(pl.col(metric).is_not_null())

        # Sort and add rank
        df = df.sort(metric, descending=not ascending)
        df = df.with_row_index("rank", offset=1)

        if top_n is not None:
            df = df.head(top_n)

        return df.select(["rank", "ticker", metric])

    def get_valuation_summary(
        self,
        ticker: str,
    ) -> dict:
        """Get a comprehensive valuation summary for a ticker.

        Args:
            ticker: Stock ticker symbol

        Returns:
            Dict with valuation metrics and context
        """
        ratios = self.calculator.get_all_ratios(ticker)

        # Get price info
        price_df = self.reader.get_latest_price(ticker)
        if not price_df.is_empty():
            ratios["current_price"] = price_df["close"][0]
            ratios["price_date"] = str(price_df["date"][0])

        # Get fundamental info
        ttm = self.reader.get_ttm_fundamentals(ticker)
        if not ttm.is_empty():
            if "revenue" in ttm.columns:
                ratios["ttm_revenue"] = ttm["revenue"][0]
            if "net_income" in ttm.columns:
                ratios["ttm_net_income"] = ttm["net_income"][0]

        return ratios
