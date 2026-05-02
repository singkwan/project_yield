"""OpenBB Platform client for fetching financial data.

Wraps the openbb SDK into a single class with provider routing:
- Prices via yfinance (free)
- Fundamentals + ratios + metrics via FMP (paid)
- Forward estimates via FMP
- Polygon kept available for cross-validation

Returns Polars DataFrames with column names matching the schema
that ParquetWriter / DataReader / RatioCalculator already expect.
"""

from datetime import date
from typing import Any

import polars as pl
from loguru import logger

from project_yield.config import Settings, get_settings


_PERIOD_MAP = {"quarterly": "quarter", "annual": "annual"}


# Mappings reflect FMP's raw column names (verified against actual responses).
# Order matters when multiple sources map to the same target — first wins.
_INCOME_COLUMN_MAP = {
    "symbol": "ticker",
    "period_ending": "report_date",
    "bottom_line_net_income": "net_income",
    "consolidated_net_income": "net_income",
    "total_operating_income": "operating_income",
    "diluted_earnings_per_share": "eps",
    "basic_earnings_per_share": "eps_basic",
    "research_and_development_expense": "rd_expense",
    "weighted_average_diluted_shares_outstanding": "shares_outstanding",
    "weighted_average_basic_shares_outstanding": "shares_outstanding_basic",
}

_BALANCE_COLUMN_MAP = {
    "symbol": "ticker",
    "period_ending": "report_date",
    "total_common_equity": "shareholders_equity",
    "total_equity": "shareholders_equity",
    "total_stockholders_equity": "shareholders_equity",
}

_CASHFLOW_COLUMN_MAP = {
    "symbol": "ticker",
    "period_ending": "report_date",
    # FMP names
    "net_cash_from_operating_activities": "operating_cash_flow",
    "net_cash_from_continuing_operating_activities": "operating_cash_flow",
    "capital_expenditure": "capex",
    "net_cash_from_investing_activities": "investing_cash_flow",
    "net_cash_from_continuing_investing_activities": "investing_cash_flow",
    "net_cash_from_financing_activities": "financing_cash_flow",
    "net_cash_from_continuing_financing_activities": "financing_cash_flow",
    # Polygon names (have extra "_flow_")
    "net_cash_flow_from_operating_activities": "operating_cash_flow",
    "net_cash_flow_from_operating_activities_continuing": "operating_cash_flow",
    "net_cash_flow_from_investing_activities": "investing_cash_flow",
    "net_cash_flow_from_investing_activities_continuing": "investing_cash_flow",
    "net_cash_flow_from_financing_activities": "financing_cash_flow",
    "net_cash_flow_from_financing_activities_continuing": "financing_cash_flow",
}

_PRICE_COLUMN_MAP = {
    "symbol": "ticker",
    "adj_close": "adjusted_close",
    "dividends": "dividend",
}

_REQUIRED_INCOME = {"ticker", "report_date", "fiscal_year", "revenue", "net_income"}
_REQUIRED_BALANCE = {"ticker", "report_date", "fiscal_year", "total_assets", "shareholders_equity"}
_REQUIRED_CASHFLOW = {"ticker", "report_date", "fiscal_year", "operating_cash_flow"}
_REQUIRED_PRICES = {"ticker", "date", "close"}


class OpenBBClient:
    """Fetches financial data via the OpenBB Platform SDK.

    Provider routing is configured at construction time. Each fetch method
    returns a Polars DataFrame with normalized column names so the rest of
    the pipeline (ParquetWriter, DataReader, RatioCalculator) is unchanged.

    Stateless — no in-memory caching. Persistence is handled by ParquetWriter.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        prices_provider: str | None = None,
        fundamentals_provider: str | None = None,
        estimates_provider: str | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.prices_provider = prices_provider or self.settings.openbb_prices_provider
        self.fundamentals_provider = (
            fundamentals_provider or self.settings.openbb_fundamentals_provider
        )
        self.estimates_provider = (
            estimates_provider or self.settings.openbb_estimates_provider
        )
        self._configure_credentials()

    def _configure_credentials(self) -> None:
        """Push API keys from Settings into OpenBB's credential store."""
        from openbb import obb

        if self.settings.openbb_fmp_api_key:
            obb.user.credentials.fmp_api_key = (
                self.settings.openbb_fmp_api_key.get_secret_value()
            )
        if self.settings.openbb_polygon_api_key:
            obb.user.credentials.polygon_api_key = (
                self.settings.openbb_polygon_api_key.get_secret_value()
            )

    @staticmethod
    def _to_polars(obbject: Any) -> pl.DataFrame:
        """Convert an OpenBB OBBject result to a Polars DataFrame."""
        pdf = obbject.to_df().reset_index()
        return pl.from_pandas(pdf)

    @staticmethod
    def _apply_mapping(df: pl.DataFrame, mapping: dict[str, str]) -> pl.DataFrame:
        """Rename columns per mapping with first-match priority.

        Mapping is iterated in insertion order. For each (src, dst):
          - If `src` is missing, skip.
          - If `dst` already exists in df (filled by a previous source or as a
            passthrough), drop `src` to avoid a collision.
          - Otherwise rename `src → dst`.
        """
        rename: dict[str, str] = {}
        present_targets = set(df.columns)
        to_drop: list[str] = []
        for src, dst in mapping.items():
            if src not in df.columns or src == dst:
                continue
            if dst in present_targets:
                to_drop.append(src)
            else:
                rename[src] = dst
                present_targets.add(dst)
        if to_drop:
            df = df.drop(to_drop)
        if rename:
            df = df.rename(rename)
        return df

    @staticmethod
    def _assert_required(df: pl.DataFrame, required: set[str]) -> None:
        """Raise if any required column is missing."""
        missing = required - set(df.columns)
        if missing:
            raise ValueError(
                f"OpenBB response missing required columns: {sorted(missing)}. "
                f"Available columns: {sorted(df.columns)}"
            )

    @staticmethod
    def _ensure_ticker(df: pl.DataFrame, ticker: str) -> pl.DataFrame:
        if "ticker" not in df.columns:
            return df.with_columns(pl.lit(ticker).alias("ticker"))
        return df

    def _normalize_statement(
        self,
        df: pl.DataFrame,
        ticker: str,
        mapping: dict[str, str],
        required: set[str],
    ) -> pl.DataFrame:
        """Standard normalization: map columns → ensure ticker → derive fiscal_year → assert required."""
        df = self._apply_mapping(df, mapping)
        df = self._ensure_ticker(df, ticker)
        # Polygon returns report_date but not fiscal_year; derive it.
        if "fiscal_year" not in df.columns and "report_date" in df.columns:
            df = df.with_columns(pl.col("report_date").dt.year().alias("fiscal_year"))
        self._assert_required(df, required)
        return df

    def get_prices(
        self,
        ticker: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pl.DataFrame:
        """Fetch daily OHLCV history for a single ticker."""
        from openbb import obb

        logger.info(f"Fetching prices for {ticker} via {self.prices_provider}")
        result = obb.equity.price.historical(
            symbol=ticker,
            provider=self.prices_provider,
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None,
        )
        df = self._to_polars(result)
        df = self._apply_mapping(df, _PRICE_COLUMN_MAP)
        df = self._ensure_ticker(df, ticker)
        self._assert_required(df, _REQUIRED_PRICES)
        if "adjusted_close" not in df.columns:
            df = df.with_columns(pl.col("close").alias("adjusted_close"))
        return df

    def get_income_statements(
        self, ticker: str, period: str = "quarterly", limit: int = 20
    ) -> pl.DataFrame:
        from openbb import obb

        logger.info(
            f"Fetching {period} income for {ticker} via {self.fundamentals_provider}"
        )
        result = obb.equity.fundamental.income(
            symbol=ticker,
            provider=self.fundamentals_provider,
            period=_PERIOD_MAP[period],
            limit=limit,
        )
        df = self._to_polars(result)
        return self._normalize_statement(df, ticker, _INCOME_COLUMN_MAP, _REQUIRED_INCOME)

    def get_balance_sheets(
        self, ticker: str, period: str = "quarterly", limit: int = 20
    ) -> pl.DataFrame:
        from openbb import obb

        logger.info(
            f"Fetching {period} balance for {ticker} via {self.fundamentals_provider}"
        )
        result = obb.equity.fundamental.balance(
            symbol=ticker,
            provider=self.fundamentals_provider,
            period=_PERIOD_MAP[period],
            limit=limit,
        )
        df = self._to_polars(result)
        return self._normalize_statement(df, ticker, _BALANCE_COLUMN_MAP, _REQUIRED_BALANCE)

    def get_cashflow_statements(
        self, ticker: str, period: str = "quarterly", limit: int = 20
    ) -> pl.DataFrame:
        from openbb import obb

        logger.info(
            f"Fetching {period} cashflow for {ticker} via {self.fundamentals_provider}"
        )
        result = obb.equity.fundamental.cash(
            symbol=ticker,
            provider=self.fundamentals_provider,
            period=_PERIOD_MAP[period],
            limit=limit,
        )
        df = self._to_polars(result)
        return self._normalize_statement(df, ticker, _CASHFLOW_COLUMN_MAP, _REQUIRED_CASHFLOW)

    def get_fundamentals(
        self, ticker: str, period: str = "quarterly", limit: int = 20
    ) -> pl.DataFrame:
        """Combined income + balance + cashflow joined on (fiscal_year, fiscal_period).

        Computes free_cash_flow = operating_cash_flow - |capex| when available.
        """
        income = self.get_income_statements(ticker, period, limit)
        balance = self.get_balance_sheets(ticker, period, limit)
        cashflow = self.get_cashflow_statements(ticker, period, limit)

        join_cols = ["ticker", "fiscal_year", "fiscal_period"]
        df = income

        if not balance.is_empty():
            balance_cols = [c for c in balance.columns if c not in df.columns or c in join_cols]
            df = df.join(balance.select(balance_cols), on=join_cols, how="left")

        if not cashflow.is_empty():
            cashflow_cols = [c for c in cashflow.columns if c not in df.columns or c in join_cols]
            df = df.join(cashflow.select(cashflow_cols), on=join_cols, how="left")

        if "operating_cash_flow" in df.columns and "capex" in df.columns:
            df = df.with_columns(
                (pl.col("operating_cash_flow") - pl.col("capex").abs()).alias("free_cash_flow")
            )

        return df

    def get_provider_ratios(
        self, ticker: str, period: str = "quarterly", limit: int = 20
    ) -> pl.DataFrame:
        """FMP's pre-computed financial ratios — for cross-validation, not source of truth."""
        from openbb import obb

        result = obb.equity.fundamental.ratios(
            symbol=ticker,
            provider=self.fundamentals_provider,
            period=_PERIOD_MAP[period],
            limit=limit,
        )
        df = self._to_polars(result)
        if "period_ending" in df.columns and "report_date" not in df.columns:
            df = df.rename({"period_ending": "report_date"})
        if "symbol" in df.columns and "ticker" not in df.columns:
            df = df.rename({"symbol": "ticker"})
        return self._ensure_ticker(df, ticker)

    def get_provider_metrics(
        self, ticker: str, period: str = "quarterly", limit: int = 20
    ) -> pl.DataFrame:
        """FMP's pre-computed key metrics (ROE, ROA, ROIC, growth rates)."""
        from openbb import obb

        result = obb.equity.fundamental.metrics(
            symbol=ticker,
            provider=self.fundamentals_provider,
            period=_PERIOD_MAP[period],
            limit=limit,
        )
        df = self._to_polars(result)
        if "period_ending" in df.columns and "report_date" not in df.columns:
            df = df.rename({"period_ending": "report_date"})
        if "symbol" in df.columns and "ticker" not in df.columns:
            df = df.rename({"symbol": "ticker"})
        return self._ensure_ticker(df, ticker)

    def get_forward_eps(self, ticker: str, limit: int = 10) -> pl.DataFrame:
        """Analyst consensus forward EPS estimates by fiscal period.

        FMP Starter caps `limit` at 10. Default keeps us inside Starter's quota.
        """
        from openbb import obb

        result = obb.equity.estimates.forward_eps(
            symbol=ticker, provider=self.estimates_provider, limit=limit
        )
        df = self._to_polars(result)
        return self._ensure_ticker(df, ticker)

    def get_provider_forward_pe(self, ticker: str) -> pl.DataFrame:
        """FMP's pre-computed forward PE — for cross-checking our self-computed value."""
        from openbb import obb

        result = obb.equity.estimates.forward_pe(
            symbol=ticker, provider=self.estimates_provider
        )
        df = self._to_polars(result)
        return self._ensure_ticker(df, ticker)

    def get_company_profile(self, ticker: str) -> dict:
        from openbb import obb

        result = obb.equity.profile(symbol=ticker, provider=self.fundamentals_provider)
        df = self._to_polars(result)
        if df.is_empty():
            return {}
        return df.row(0, named=True)

    def get_sector_groups(
        self, group: str = "sector", metric: str = "valuation"
    ) -> pl.DataFrame:
        """Sector / industry / country roll-up via obb.equity.compare.groups.

        Only finviz supports this endpoint in OpenBB.
        """
        from openbb import obb

        result = obb.equity.compare.groups(group=group, metric=metric, provider="finviz")
        return self._to_polars(result)

    def screen_universe(self, provider: str = "finviz", **filters: Any) -> pl.DataFrame:
        """Universe-wide ticker screening via obb.equity.screener.

        Default provider is finviz (free, no API key needed for screener).
        Filter names depend on the underlying provider.
        """
        from openbb import obb

        result = obb.equity.screener(provider=provider, **filters)
        return self._to_polars(result)
