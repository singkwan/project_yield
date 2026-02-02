"""Data ingestion module for downloading and storing financial data."""

from datetime import date, datetime

import polars as pl
from loguru import logger

from project_yield.config import Settings, get_settings
from project_yield.data.reader import DataReader
from project_yield.data.simfin_client import SimFinClient
from project_yield.data.writer import ParquetWriter


class DataIngestion:
    """Orchestrates downloading and storing financial data.

    Handles full and incremental updates of price and fundamental data.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        """Initialize ingestion with settings."""
        self.settings = settings or get_settings()
        self.client = SimFinClient(self.settings)
        self.writer = ParquetWriter(self.settings)
        self.reader = DataReader(self.settings)

    def update_all_data(
        self,
        tickers: list[str] | None = None,
        start_date: date | None = None,
        include_fundamentals: bool = True,
    ) -> dict:
        """Download and store all data for specified tickers.

        Args:
            tickers: List of tickers to update (None for all available)
            start_date: Only include data from this date onwards
            include_fundamentals: Whether to download fundamental data

        Returns:
            Summary dict with counts of records processed
        """
        start_time = datetime.now()
        summary = {
            "tickers_processed": 0,
            "prices_written": 0,
            "fundamentals_written": 0,
            "errors": [],
        }

        # Get available tickers if not specified
        if tickers is None:
            tickers = self.client.list_available_tickers()
            logger.info(f"Found {len(tickers)} tickers with price data")

        # Use default start date if not specified
        if start_date is None:
            start_date = date.fromisoformat(self.settings.default_start_date)

        # Download prices for all tickers at once (SimFin bulk download)
        logger.info("Downloading price data...")
        all_prices = self.client.get_prices()

        if all_prices.is_empty():
            logger.error("No price data available")
            summary["errors"].append("No price data available")
            return summary

        # Filter by start date
        all_prices = all_prices.filter(pl.col("date") >= start_date)

        # Download fundamentals if requested
        all_fundamentals_q = pl.DataFrame()
        all_fundamentals_a = pl.DataFrame()

        if include_fundamentals:
            logger.info("Downloading quarterly fundamentals...")
            all_fundamentals_q = self.client.get_fundamentals(period="quarterly")

            logger.info("Downloading annual fundamentals...")
            all_fundamentals_a = self.client.get_fundamentals(period="annual")

        # Process each ticker
        for ticker in tickers:
            try:
                # Write prices
                ticker_prices = all_prices.filter(pl.col("ticker") == ticker)
                if not ticker_prices.is_empty():
                    paths = self.writer.write_prices(ticker_prices, ticker)
                    summary["prices_written"] += len(ticker_prices)

                # Write quarterly fundamentals
                if include_fundamentals and not all_fundamentals_q.is_empty():
                    ticker_fund_q = all_fundamentals_q.filter(pl.col("ticker") == ticker)
                    if not ticker_fund_q.is_empty():
                        self.writer.write_fundamentals_quarterly(ticker_fund_q, ticker)
                        summary["fundamentals_written"] += len(ticker_fund_q)

                # Write annual fundamentals
                if include_fundamentals and not all_fundamentals_a.is_empty():
                    ticker_fund_a = all_fundamentals_a.filter(pl.col("ticker") == ticker)
                    if not ticker_fund_a.is_empty():
                        self.writer.write_fundamentals_annual(ticker_fund_a, ticker)

                summary["tickers_processed"] += 1

                if summary["tickers_processed"] % 100 == 0:
                    logger.info(f"Processed {summary['tickers_processed']}/{len(tickers)} tickers")

            except Exception as e:
                logger.error(f"Error processing {ticker}: {e}")
                summary["errors"].append(f"{ticker}: {e}")

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Ingestion complete: {summary['tickers_processed']} tickers, "
            f"{summary['prices_written']} prices, "
            f"{summary['fundamentals_written']} fundamentals "
            f"in {elapsed:.1f}s"
        )

        return summary

    def update_prices_incremental(self, tickers: list[str] | None = None) -> dict:
        """Update only new price data since last update.

        Args:
            tickers: List of tickers to update (None for all existing)

        Returns:
            Summary dict with counts
        """
        summary = {
            "tickers_updated": 0,
            "new_records": 0,
            "errors": [],
        }

        # Get tickers to update
        if tickers is None:
            tickers = self.reader.list_tickers("prices")

        if not tickers:
            logger.warning("No tickers to update")
            return summary

        # Download fresh price data
        logger.info("Downloading latest prices...")
        all_prices = self.client.get_prices(refresh=True)

        if all_prices.is_empty():
            logger.error("No price data available")
            return summary

        for ticker in tickers:
            try:
                # Get last date we have
                _, max_date = self.reader.get_date_range(ticker)

                if max_date is None:
                    continue

                # Filter to new data only
                ticker_prices = all_prices.filter(
                    (pl.col("ticker") == ticker) & (pl.col("date") > max_date)
                )

                if not ticker_prices.is_empty():
                    self.writer.append_prices(ticker_prices, ticker)
                    summary["new_records"] += len(ticker_prices)
                    summary["tickers_updated"] += 1
                    logger.debug(f"{ticker}: added {len(ticker_prices)} new records")

            except Exception as e:
                logger.error(f"Error updating {ticker}: {e}")
                summary["errors"].append(f"{ticker}: {e}")

        logger.info(
            f"Incremental update: {summary['tickers_updated']} tickers, "
            f"{summary['new_records']} new records"
        )

        return summary

    def get_sp500_tickers(self) -> list[str]:
        """Get list of S&P 500 tickers that have data available.

        Note: SimFin free tier may not have all S&P 500 companies.

        Returns:
            List of available S&P 500 tickers
        """
        # Common S&P 500 tickers (subset for testing)
        sp500_sample = [
            "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA", "BRK.B",
            "UNH", "JNJ", "JPM", "V", "PG", "XOM", "HD", "CVX", "MA", "ABBV",
            "MRK", "LLY", "PFE", "KO", "PEP", "COST", "AVGO", "TMO", "MCD",
            "WMT", "CSCO", "ACN", "ABT", "DHR", "NEE", "VZ", "ADBE", "NKE",
            "TXN", "PM", "CMCSA", "CRM", "INTC", "AMD", "ORCL", "IBM", "QCOM",
            "HON", "UNP", "LOW", "AMGN", "SBUX",
        ]

        # Get available tickers
        available = set(self.client.list_available_tickers())

        # Filter to those available in SimFin
        available_sp500 = [t for t in sp500_sample if t in available]

        # Also check for alternate symbols (GOOGL -> GOOG)
        alternates = {"GOOGL": "GOOG", "BRK.B": "BRK-B"}
        for original, alt in alternates.items():
            if original not in available and alt in available:
                available_sp500.append(alt)

        logger.info(f"Found {len(available_sp500)}/{len(sp500_sample)} S&P 500 tickers available")
        return sorted(available_sp500)

    def download_sp500(self, start_date: date | None = None) -> dict:
        """Download all available S&P 500 data.

        Args:
            start_date: Only include data from this date onwards

        Returns:
            Summary dict with counts
        """
        tickers = self.get_sp500_tickers()
        return self.update_all_data(
            tickers=tickers,
            start_date=start_date,
            include_fundamentals=True,
        )

    def get_data_summary(self) -> dict:
        """Get summary of stored data.

        Returns:
            Dict with counts of tickers and date ranges
        """
        price_tickers = self.reader.list_tickers("prices")
        quarterly_tickers = self.reader.list_tickers("quarterly")
        annual_tickers = self.reader.list_tickers("annual")

        summary = {
            "price_tickers": len(price_tickers),
            "quarterly_tickers": len(quarterly_tickers),
            "annual_tickers": len(annual_tickers),
            "sample_date_ranges": {},
        }

        # Get date range for a few sample tickers
        for ticker in price_tickers[:5]:
            min_date, max_date = self.reader.get_date_range(ticker)
            summary["sample_date_ranges"][ticker] = {
                "min_date": str(min_date) if min_date else None,
                "max_date": str(max_date) if max_date else None,
            }

        return summary
