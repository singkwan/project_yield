"""Data ingestion: pull data per-ticker from OpenBB and persist to Parquet.

Also wires IBKR portfolio sync so held tickers can drive what gets ingested.
"""

from datetime import date, datetime

import polars as pl
from loguru import logger

from project_yield.brokers.flex_csv import load_flex_csv
from project_yield.config import Settings, get_settings
from project_yield.data.openbb_client import OpenBBClient
from project_yield.data.symbology import ibkr_to_yfinance, is_foreign_ticker
from project_yield.data.portfolio import PortfolioReader, PortfolioWriter
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
        broker: object | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.client = client or OpenBBClient(self.settings)
        self.writer = ParquetWriter(self.settings)
        self.reader = DataReader(self.settings)
        self.portfolio_writer = PortfolioWriter(self.settings)
        self.portfolio_reader = PortfolioReader(self.settings)
        self._broker = broker  # Lazy: only constructed when sync_holdings() is called

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
        # Translate broker-native (IBKR) symbols to yfinance form once, here at
        # the boundary. Everything downstream — API calls, partition keys,
        # parquet `ticker` columns — operates on yf_ticker.
        yf_ticker = ibkr_to_yfinance(ticker)
        if yf_ticker != ticker:
            logger.info(f"Translated {ticker} → {yf_ticker} (yfinance form)")

        # Foreign listings aren't on FMP; route them through yfinance.
        use_yfinance = is_foreign_ticker(yf_ticker)
        provider = "yfinance" if use_yfinance else None

        prices = self.client.get_prices(yf_ticker, start_date=start_date, provider=provider)
        if not prices.is_empty():
            self.writer.write_prices(prices, yf_ticker)
            summary["prices_written"] += len(prices)

        if include_fundamentals:
            for period in ("quarterly", "annual"):
                try:
                    fundamentals = self.client.get_fundamentals(
                        yf_ticker, period=period, provider=provider
                    )
                except Exception as e:
                    logger.warning(f"{yf_ticker}: {period} fundamentals unavailable: {e}")
                    continue
                if fundamentals.is_empty():
                    continue
                if period == "quarterly":
                    self.writer.write_fundamentals_quarterly(fundamentals, yf_ticker)
                else:
                    self.writer.write_fundamentals_annual(fundamentals, yf_ticker)
                summary["fundamentals_written"] += len(fundamentals)

        # Provider ratios (FMP/Polygon endpoint) — yfinance doesn't expose this,
        # so skip silently for foreign tickers.
        if include_provider_ratios and not use_yfinance:
            for period in ("quarterly", "annual"):
                try:
                    ratios = self.client.get_provider_ratios(yf_ticker, period=period)
                    metrics = self.client.get_provider_metrics(yf_ticker, period=period)
                except Exception as e:
                    logger.warning(f"{yf_ticker}: provider ratios unavailable for {period}: {e}")
                    continue

                merged = self._merge_ratios_metrics(ratios, metrics)
                if merged.is_empty():
                    continue
                self.writer.write_provider_ratios(merged, yf_ticker, period=period)
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

    # --- IBKR portfolio sync (lazy broker construction) ---

    @property
    def broker(self):
        """Lazy IBKRClient — only constructed when actually needed."""
        if self._broker is None:
            from project_yield.brokers import IBKRClient
            self._broker = IBKRClient(self.settings)
        return self._broker

    def sync_holdings(
        self,
        include_transactions: bool = True,
        include_watchlists: bool = True,
        include_activity: bool = True,
        transaction_lookback_days: int = 365,
    ) -> dict:
        """Pull positions + transactions + watchlists + Flex activity from IBKR.

        Does NOT trigger OpenBB ingestion (per "explicit refresh" decision).
        Use update_held() or update_held_and_watched() for that.

        include_activity (Flex Web Service: dividends, interest, NAV history)
        is independent of OAuth and only requires ibkr_flex_token + query IDs.
        """
        summary: dict = {
            "positions_rows": 0,
            "transactions_rows": 0,
            "watchlists_count": 0,
            "dividends_rows": 0,
            "interest_rows": 0,
            "nav_history_rows": 0,
            "errors": [],
        }

        try:
            positions = self.broker.get_positions()
            if not positions.is_empty():
                self.portfolio_writer.write_positions_snapshot(positions)
                summary["positions_rows"] = len(positions)
        except Exception as e:
            logger.error(f"sync_holdings: positions fetch failed: {e}")
            summary["errors"].append(f"positions: {e}")

        if include_transactions:
            try:
                transactions = self.broker.get_transactions(days=transaction_lookback_days)
                if not transactions.is_empty():
                    self.portfolio_writer.write_transactions(transactions)
                    summary["transactions_rows"] = len(transactions)
            except Exception as e:
                logger.error(f"sync_holdings: transactions fetch failed: {e}")
                summary["errors"].append(f"transactions: {e}")

        if include_watchlists:
            try:
                lists = self.broker.get_watchlists()
                for entry in lists:
                    list_id = entry.get("id") if isinstance(entry, dict) else None
                    if not list_id:
                        continue
                    df = self.broker.get_watchlist(list_id)
                    if not df.is_empty():
                        self.portfolio_writer.write_watchlist(df, list_id)
                        summary["watchlists_count"] += 1
            except Exception as e:
                logger.error(f"sync_holdings: watchlists fetch failed: {e}")
                summary["errors"].append(f"watchlists: {e}")

        if include_activity:
            self._sync_activity(summary)

        logger.info(f"sync_holdings done: {summary}")
        return summary

    def sync_via_flex_csv(self, csv_path) -> dict:
        """Bootstrap from a manually-downloaded Flex CSV file (no API call).

        Use this when IBKR's Flex Web Service is rate-limited and you've
        downloaded the report yourself from the Client Portal as CSV.
        """

        bundle = load_flex_csv(csv_path)
        summary: dict = {
            "positions_rows": 0, "transactions_rows": 0,
            "dividends_rows": 0, "interest_rows": 0, "nav_history_rows": 0,
            "errors": [],
        }
        try:
            self._persist_flex_bundle(bundle, date.today().year, summary)
        except Exception as e:
            logger.error(f"sync_via_flex_csv: persist failed: {e}")
            summary["errors"].append(f"persist: {e}")
        logger.info(f"sync_via_flex_csv done: {summary}")
        return summary

    def sync_via_flex(
        self,
        consolidated: bool = True,
        positions_query_id: str | None = None,
        trades_query_id: str | None = None,
        dividends_query_id: str | None = None,
        interest_query_id: str | None = None,
        nav_query_id: str | None = None,
        consolidated_query_id: str | None = None,
    ) -> dict:
        """Bootstrap portfolio data from Flex Web Service only — no OAuth required.

        Covers everything except watchlists (Flex doesn't expose watchlists).

        consolidated=True: one Flex query returns all sections (recommended —
        avoids IBKR's per-query rate limit). Override consolidated_query_id or
        rely on Settings.ibkr_flex_consolidated_query_id.

        consolidated=False: separate per-section queries. Slower (multiple Flex
        round-trips with rate-limit risk) but works if you can't fit all
        sections into one Flex query.
        """
        summary: dict = {
            "positions_rows": 0, "transactions_rows": 0,
            "dividends_rows": 0, "interest_rows": 0, "nav_history_rows": 0,
            "errors": [],
        }
        year = date.today().year

        if consolidated:
            try:
                bundle = self.broker.get_consolidated_report(consolidated_query_id)
                self._persist_flex_bundle(bundle, year, summary)
            except Exception as e:
                logger.error(f"sync_via_flex consolidated: {e}")
                summary["errors"].append(f"consolidated: {e}")
            logger.info(f"sync_via_flex done: {summary}")
            return summary

        # Per-section path
        try:
            df = self.broker.get_positions_report(positions_query_id)
            if not df.is_empty():
                self.portfolio_writer.write_positions_snapshot(df)
                summary["positions_rows"] = len(df)
        except Exception as e:
            summary["errors"].append(f"positions: {e}")
        try:
            df = self.broker.get_trades_report(trades_query_id)
            if not df.is_empty():
                self.portfolio_writer.write_transactions(df)
                summary["transactions_rows"] = len(df)
        except Exception as e:
            summary["errors"].append(f"trades: {e}")
        for label, fetch, write, qid in (
            ("dividends", self.broker.get_dividends_report, self.portfolio_writer.write_dividends, dividends_query_id),
            ("interest", self.broker.get_interest_report, self.portfolio_writer.write_interest, interest_query_id),
            ("nav_history", self.broker.get_nav_history_report, self.portfolio_writer.write_nav_history, nav_query_id),
        ):
            try:
                df = fetch(qid) if qid else fetch()
                if not df.is_empty():
                    write(df, year)
                    summary[f"{label}_rows"] = len(df)
            except Exception as e:
                summary["errors"].append(f"{label}: {e}")

        logger.info(f"sync_via_flex done: {summary}")
        return summary

    def _persist_flex_bundle(self, bundle: dict, year: int, summary: dict) -> None:
        """Write each section of a consolidated Flex bundle to its Parquet partition.

        For positions: Flex returns one snapshot per `reportDate` in the period,
        so we split and write each date as its own snapshot. positions_latest.parquet
        ends up with only the most recent date's rows (handled by writer overwrite
        order — we write oldest first, newest last).
        """
        positions = bundle["positions"]
        if not positions.is_empty():
            if "reportDate" in positions.columns:
                dates = sorted(positions["reportDate"].drop_nulls().unique().to_list())
                for d in dates:
                    day = positions.filter(pl.col("reportDate") == d)
                    self.portfolio_writer.write_positions_snapshot(day, snapshot_date=d)
                summary["positions_rows"] = len(positions)
                summary["positions_snapshot_days"] = len(dates)
            else:
                self.portfolio_writer.write_positions_snapshot(positions)
                summary["positions_rows"] = len(positions)
        if not bundle["trades"].is_empty():
            self.portfolio_writer.write_transactions(bundle["trades"])
            summary["transactions_rows"] = len(bundle["trades"])
        if not bundle["dividends"].is_empty():
            self.portfolio_writer.write_dividends(bundle["dividends"], year)
            summary["dividends_rows"] = len(bundle["dividends"])
        if not bundle["interest"].is_empty():
            self.portfolio_writer.write_interest(bundle["interest"], year)
            summary["interest_rows"] = len(bundle["interest"])
        if not bundle["nav_history"].is_empty():
            self.portfolio_writer.write_nav_history(bundle["nav_history"], year)
            summary["nav_history_rows"] = len(bundle["nav_history"])

    def _sync_activity(self, summary: dict) -> None:
        """Pull Flex reports (dividends, interest, NAV) and persist by year."""
        year = date.today().year
        for label, fetch, write in (
            ("dividends", self.broker.get_dividends_report, self.portfolio_writer.write_dividends),
            ("interest", self.broker.get_interest_report, self.portfolio_writer.write_interest),
            ("nav_history", self.broker.get_nav_history_report, self.portfolio_writer.write_nav_history),
        ):
            try:
                df = fetch()
                if not df.is_empty():
                    write(df, year)
                    summary[f"{label}_rows"] = len(df)
            except Exception as e:
                logger.warning(f"sync_holdings: {label} fetch skipped: {e}")
                summary["errors"].append(f"{label}: {e}")

    def update_held_tickers(self, start_date: date | None = None) -> dict:
        """Run OpenBB ingestion for tickers in the latest positions snapshot.

        Reads from local Parquet — does NOT re-fetch from IBKR. Call sync_holdings()
        first if you want fresh holdings.
        """
        held = self.portfolio_reader.list_held_tickers()
        if not held:
            logger.warning("No held tickers in latest positions snapshot")
            return {"tickers_processed": 0, "errors": ["no held tickers"]}
        logger.info(f"Updating {len(held)} held tickers via OpenBB")
        return self.update_all_data(held, start_date)

    def update_held_and_watched(self, start_date: date | None = None) -> dict:
        """OpenBB ingestion for the union of held + watched tickers."""
        held = set(self.portfolio_reader.list_held_tickers())
        watched = set(self.portfolio_reader.list_watched_tickers())
        tickers = sorted(held | watched)
        if not tickers:
            logger.warning("No held or watched tickers found")
            return {"tickers_processed": 0, "errors": ["no held or watched tickers"]}
        logger.info(f"Updating {len(tickers)} tickers ({len(held)} held + {len(watched)} watched)")
        return self.update_all_data(tickers, start_date)

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
