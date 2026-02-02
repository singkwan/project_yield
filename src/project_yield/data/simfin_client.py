"""SimFin API client for downloading financial data."""

from pathlib import Path

import polars as pl
import simfin as sf
from loguru import logger

from project_yield.config import Settings, get_settings


class SimFinClient:
    """Client for downloading data from SimFin API.

    Wraps the simfin library with caching, Polars conversion, and error handling.
    SimFin uses bulk downloads - data for all companies is fetched at once and cached.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        """Initialize client with settings."""
        self.settings = settings or get_settings()
        self._configure_simfin()

        # Cached DataFrames (loaded on demand)
        self._prices_df: pl.DataFrame | None = None
        self._income_df: pl.DataFrame | None = None
        self._balance_df: pl.DataFrame | None = None
        self._cashflow_df: pl.DataFrame | None = None
        self._companies_df: pl.DataFrame | None = None

    def _configure_simfin(self) -> None:
        """Configure SimFin library settings."""
        sf.set_api_key(self.settings.simfin_api_key)
        sf.set_data_dir(str(self.settings.data_path / "simfin_cache"))

    def get_companies(self, market: str = "us") -> pl.DataFrame:
        """Get list of all companies.

        Args:
            market: Market code (default: "us")

        Returns:
            DataFrame with company info (ticker, name, industry, etc.)
        """
        if self._companies_df is not None:
            return self._companies_df

        logger.info(f"Downloading company list for market: {market}")
        try:
            pdf = sf.load_companies(market=market)
            pdf = pdf.reset_index()  # Ticker is index, move to column

            self._companies_df = pl.from_pandas(pdf)
            logger.info(f"Loaded {len(self._companies_df)} companies")
            return self._companies_df
        except Exception as e:
            logger.error(f"Failed to load companies: {e}")
            return pl.DataFrame()

    def get_prices(
        self,
        ticker: str | None = None,
        refresh: bool = False,
    ) -> pl.DataFrame:
        """Get daily stock prices.

        Args:
            ticker: Filter by ticker (None for all)
            refresh: Force refresh from API

        Returns:
            DataFrame with price data
        """
        if self._prices_df is None or refresh:
            logger.info("Downloading daily price data...")
            try:
                pdf = sf.load_shareprices(variant="daily", market="us")
                pdf = pdf.reset_index()  # Ticker and Date are multi-index

                # Rename columns to our schema
                column_map = {
                    "Ticker": "ticker",
                    "Date": "date",
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Adj. Close": "adjusted_close",
                    "Volume": "volume",
                }

                self._prices_df = pl.from_pandas(pdf).rename(
                    {k: v for k, v in column_map.items() if k in pdf.columns}
                )
                logger.info(f"Loaded {len(self._prices_df)} price records")
            except Exception as e:
                logger.error(f"Failed to load prices: {e}")
                return pl.DataFrame()

        df = self._prices_df
        if ticker is not None:
            df = df.filter(pl.col("ticker") == ticker)

        return df

    def get_income_statements(
        self,
        ticker: str | None = None,
        period: str = "quarterly",
        refresh: bool = False,
    ) -> pl.DataFrame:
        """Get income statement data.

        Args:
            ticker: Filter by ticker (None for all)
            period: "quarterly" or "annual"
            refresh: Force refresh from API

        Returns:
            DataFrame with income statement data
        """
        if self._income_df is None or refresh:
            logger.info(f"Downloading {period} income statements...")
            try:
                variant = "quarterly" if period == "quarterly" else "annual"
                pdf = sf.load_income(variant=variant, market="us")
                pdf = pdf.reset_index()

                # Rename columns to our schema
                column_map = {
                    "Ticker": "ticker",
                    "SimFinId": "company_id",
                    "Fiscal Year": "fiscal_year",
                    "Fiscal Period": "fiscal_period",
                    "Report Date": "report_date",
                    "Revenue": "revenue",
                    "Gross Profit": "gross_profit",
                    "Operating Income (Loss)": "operating_income",
                    "Net Income": "net_income",
                    "Earnings Per Share, Diluted": "eps",
                    "Research & Development": "rd_expense",
                }

                self._income_df = pl.from_pandas(pdf).rename(
                    {k: v for k, v in column_map.items() if k in pdf.columns}
                )
                logger.info(f"Loaded {len(self._income_df)} income records")
            except Exception as e:
                logger.error(f"Failed to load income statements: {e}")
                return pl.DataFrame()

        df = self._income_df
        if ticker is not None:
            df = df.filter(pl.col("ticker") == ticker)

        return df

    def get_balance_sheets(
        self,
        ticker: str | None = None,
        period: str = "quarterly",
        refresh: bool = False,
    ) -> pl.DataFrame:
        """Get balance sheet data.

        Args:
            ticker: Filter by ticker (None for all)
            period: "quarterly" or "annual"
            refresh: Force refresh from API

        Returns:
            DataFrame with balance sheet data
        """
        if self._balance_df is None or refresh:
            logger.info(f"Downloading {period} balance sheets...")
            try:
                variant = "quarterly" if period == "quarterly" else "annual"
                pdf = sf.load_balance(variant=variant, market="us")
                pdf = pdf.reset_index()

                column_map = {
                    "Ticker": "ticker",
                    "SimFinId": "company_id",
                    "Fiscal Year": "fiscal_year",
                    "Fiscal Period": "fiscal_period",
                    "Report Date": "report_date",
                    "Total Assets": "total_assets",
                    "Total Liabilities": "total_liabilities",
                    "Total Equity": "shareholders_equity",
                    "Shares (Diluted)": "shares_outstanding",
                }

                self._balance_df = pl.from_pandas(pdf).rename(
                    {k: v for k, v in column_map.items() if k in pdf.columns}
                )
                logger.info(f"Loaded {len(self._balance_df)} balance sheet records")
            except Exception as e:
                logger.error(f"Failed to load balance sheets: {e}")
                return pl.DataFrame()

        df = self._balance_df
        if ticker is not None:
            df = df.filter(pl.col("ticker") == ticker)

        return df

    def get_cashflow_statements(
        self,
        ticker: str | None = None,
        period: str = "quarterly",
        refresh: bool = False,
    ) -> pl.DataFrame:
        """Get cash flow statement data.

        Args:
            ticker: Filter by ticker (None for all)
            period: "quarterly" or "annual"
            refresh: Force refresh from API

        Returns:
            DataFrame with cash flow data
        """
        if self._cashflow_df is None or refresh:
            logger.info(f"Downloading {period} cash flow statements...")
            try:
                variant = "quarterly" if period == "quarterly" else "annual"
                pdf = sf.load_cashflow(variant=variant, market="us")
                pdf = pdf.reset_index()

                column_map = {
                    "Ticker": "ticker",
                    "SimFinId": "company_id",
                    "Fiscal Year": "fiscal_year",
                    "Fiscal Period": "fiscal_period",
                    "Report Date": "report_date",
                    "Net Cash from Operating Activities": "operating_cash_flow",
                    "Change in Fixed Assets & Intangibles": "capex",
                    "Net Cash from Investing Activities": "investing_cash_flow",
                    "Net Cash from Financing Activities": "financing_cash_flow",
                }

                self._cashflow_df = pl.from_pandas(pdf).rename(
                    {k: v for k, v in column_map.items() if k in pdf.columns}
                )
                logger.info(f"Loaded {len(self._cashflow_df)} cash flow records")
            except Exception as e:
                logger.error(f"Failed to load cash flow statements: {e}")
                return pl.DataFrame()

        df = self._cashflow_df
        if ticker is not None:
            df = df.filter(pl.col("ticker") == ticker)

        return df

    def get_fundamentals(
        self,
        ticker: str | None = None,
        period: str = "quarterly",
    ) -> pl.DataFrame:
        """Get combined fundamentals (income, balance, cashflow).

        Joins all financial statements into a single DataFrame.

        Args:
            ticker: Filter by ticker (None for all)
            period: "quarterly" or "annual"

        Returns:
            DataFrame with all fundamental data
        """
        income = self.get_income_statements(ticker=ticker, period=period)
        balance = self.get_balance_sheets(ticker=ticker, period=period)
        cashflow = self.get_cashflow_statements(ticker=ticker, period=period)

        if income.is_empty():
            return pl.DataFrame()

        # Join on ticker and fiscal period
        join_cols = ["ticker", "fiscal_year", "fiscal_period"]

        # Start with income as base
        df = income

        # Join balance sheet (exclude duplicate columns)
        if not balance.is_empty():
            balance_cols = [c for c in balance.columns if c not in income.columns or c in join_cols]
            df = df.join(
                balance.select(balance_cols),
                on=join_cols,
                how="left",
            )

        # Join cash flow (exclude duplicate columns)
        if not cashflow.is_empty():
            cashflow_cols = [c for c in cashflow.columns if c not in df.columns or c in join_cols]
            df = df.join(
                cashflow.select(cashflow_cols),
                on=join_cols,
                how="left",
            )

        # Calculate free cash flow if we have the components
        if "operating_cash_flow" in df.columns and "capex" in df.columns:
            df = df.with_columns(
                (pl.col("operating_cash_flow") - pl.col("capex").abs()).alias("free_cash_flow")
            )

        return df

    def list_available_tickers(self) -> list[str]:
        """Get list of all tickers with price data.

        Returns:
            Sorted list of ticker symbols
        """
        prices = self.get_prices()
        if prices.is_empty():
            return []

        tickers = prices["ticker"].unique().to_list()
        # Filter out None values
        return sorted([t for t in tickers if t is not None])

    def clear_cache(self) -> None:
        """Clear all cached DataFrames."""
        self._prices_df = None
        self._income_df = None
        self._balance_df = None
        self._cashflow_df = None
        self._companies_df = None
        logger.info("Cleared SimFin client cache")
