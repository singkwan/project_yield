"""Data ingestion: pull data per-ticker from OpenBB and persist to Parquet."""

from datetime import date, datetime

import polars as pl
from loguru import logger

from project_yield.config import Settings, get_settings
from project_yield.data.openbb_client import OpenBBClient
from project_yield.data.reader import DataReader
from project_yield.data.writer import ParquetWriter


class DataIngestion:
    """Orchestrates per-ticker downloads from OpenBB into local Parquet.

    For each ticker we pull:
      - Daily prices (range-filtered)
      - Quarterly + annual income, balance, cashflow
      - Quarterly + annual provider ratios + metrics (cached for cross-validation)
    """

    def __init__(
        self,
        settings: Settings | None = None,
        client: OpenBBClient | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.client = client or OpenBBClient(self.settings)
        self.writer = ParquetWriter(self.settings)
        self.reader = DataReader(self.settings)

    def update_all_data(
        self,
        tickers: list[str],
        start_date: date | None = None,
        include_fundamentals: bool = True,
        include_provider_ratios: bool = True,
    ) -> dict:
        """Download and persist all data for the given tickers.

        Args:
            tickers: Tickers to ingest (no implicit S&P 500 default).
            start_date: Start date for prices (default from settings).
            include_fundamentals: Pull income/balance/cashflow.
            include_provider_ratios: Pull FMP-published ratios + metrics for cross-validation.

        Returns:
            Summary dict with counts.
        """
        start_time = datetime.now()
        if start_date is None:
            start_date = date.fromisoformat(self.settings.default_start_date)

        summary = {
            "tickers_processed": 0,
            "prices_written": 0,
            "fundamentals_written": 0,
            "ratios_written": 0,
            "errors": [],
        }

        for i, ticker in enumerate(tickers, 1):
            try:
                self._ingest_one(
                    ticker,
                    start_date,
                    include_fundamentals,
                    include_provider_ratios,
                    summary,
                )
                summary["tickers_processed"] += 1
                if i % 25 == 0:
                    logger.info(f"Processed {i}/{len(tickers)} tickers")
            except Exception as e:
                logger.error(f"Error ingesting {ticker}: {e}")
                summary["errors"].append(f"{ticker}: {e}")

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Ingestion complete: {summary['tickers_processed']}/{len(tickers)} tickers, "
            f"{summary['prices_written']} prices, "
            f"{summary['fundamentals_written']} fundamentals, "
            f"{summary['ratios_written']} ratio rows, "
            f"{len(summary['errors'])} errors in {elapsed:.1f}s"
        )
        return summary

    def _ingest_one(
        self,
        ticker: str,
        start_date: date,
        include_fundamentals: bool,
        include_provider_ratios: bool,
        summary: dict,
    ) -> None:
        prices = self.client.get_prices(ticker, start_date=start_date)
        if not prices.is_empty():
            self.writer.write_prices(prices, ticker)
            summary["prices_written"] += len(prices)

        if include_fundamentals:
            for period in ("quarterly", "annual"):
                fundamentals = self.client.get_fundamentals(ticker, period=period)
                if fundamentals.is_empty():
                    continue
                if period == "quarterly":
                    self.writer.write_fundamentals_quarterly(fundamentals, ticker)
                else:
                    self.writer.write_fundamentals_annual(fundamentals, ticker)
                summary["fundamentals_written"] += len(fundamentals)

        if include_provider_ratios:
            for period in ("quarterly", "annual"):
                try:
                    ratios = self.client.get_provider_ratios(ticker, period=period)
                    metrics = self.client.get_provider_metrics(ticker, period=period)
                except Exception as e:
                    logger.warning(f"{ticker}: provider ratios unavailable for {period}: {e}")
                    continue

                merged = self._merge_ratios_metrics(ratios, metrics)
                if merged.is_empty():
                    continue
                self.writer.write_provider_ratios(merged, ticker, period=period)
                summary["ratios_written"] += len(merged)

    @staticmethod
    def _merge_ratios_metrics(
        ratios: pl.DataFrame, metrics: pl.DataFrame
    ) -> pl.DataFrame:
        """Join FMP ratios + metrics on the period key, dropping duplicate columns."""
        if ratios.is_empty():
            return metrics
        if metrics.is_empty():
            return ratios

        join_cols = [c for c in ("ticker", "fiscal_year", "fiscal_period") if c in ratios.columns and c in metrics.columns]
        if not join_cols:
            return ratios

        metrics_cols = [c for c in metrics.columns if c not in ratios.columns or c in join_cols]
        return ratios.join(metrics.select(metrics_cols), on=join_cols, how="left")

    def update_prices_incremental(self, tickers: list[str] | None = None) -> dict:
        """Pull only prices since the last stored date for each ticker.

        Args:
            tickers: Tickers to refresh (default: all tickers already on disk).
        """
        summary = {"tickers_updated": 0, "new_records": 0, "errors": []}

        if tickers is None:
            tickers = self.reader.list_tickers("prices")
        if not tickers:
            logger.warning("No tickers to update")
            return summary

        for ticker in tickers:
            try:
                _, max_date = self.reader.get_date_range(ticker)
                if max_date is None:
                    continue
                prices = self.client.get_prices(ticker, start_date=max_date)
                prices = prices.filter(pl.col("date") > max_date)
                if not prices.is_empty():
                    self.writer.append_prices(prices, ticker)
                    summary["new_records"] += len(prices)
                    summary["tickers_updated"] += 1
                    logger.debug(f"{ticker}: appended {len(prices)} new rows")
            except Exception as e:
                logger.error(f"Error updating {ticker}: {e}")
                summary["errors"].append(f"{ticker}: {e}")

        logger.info(
            f"Incremental update: {summary['tickers_updated']} tickers, "
            f"{summary['new_records']} new records"
        )
        return summary

    def get_data_summary(self) -> dict:
        """Get summary of stored data."""
        price_tickers = self.reader.list_tickers("prices")
        quarterly_tickers = self.reader.list_tickers("quarterly")
        annual_tickers = self.reader.list_tickers("annual")

        summary = {
            "price_tickers": len(price_tickers),
            "quarterly_tickers": len(quarterly_tickers),
            "annual_tickers": len(annual_tickers),
            "sample_date_ranges": {},
        }

        for ticker in price_tickers[:5]:
            min_date, max_date = self.reader.get_date_range(ticker)
            summary["sample_date_ranges"][ticker] = {
                "min_date": str(min_date) if min_date else None,
                "max_date": str(max_date) if max_date else None,
            }
        return summary
