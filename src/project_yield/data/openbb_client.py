"""OpenBB Platform client for fetching financial data.

Wraps the openbb SDK into a single class with provider routing:
- Prices + fundamentals + ratios + metrics + estimates via FMP (paid, primary)
- yfinance + polygon kept available as validation providers for CrossValidator
- yfinance also used as a fallback for foreign-exchange tickers (FMP is US-only)

Returns Polars DataFrames with column names matching the schema
that ParquetWriter / DataReader / RatioCalculator already expect.
"""

from datetime import date
from typing import Any

import polars as pl
from loguru import logger
from openbb import obb

from project_yield.config import Settings, get_settings
from project_yield.data import schemas


_PERIOD_MAP = {"quarterly": "quarter", "annual": "annual"}


class OpenBBClient:
    """Single entry point to all financial data the rest of the pipeline consumes.

    Wraps the OpenBB Platform SDK and adds three things on top of it:

    1. **Provider routing** — picked per data type at construction time.
       Defaults (overridable via Settings or constructor kwargs):
         - prices         → FMP       (paid, primary)
         - fundamentals   → FMP       (paid, deepest line-item coverage)
         - estimates      → FMP       (forward EPS / forward PE)
         - ratios/metrics → FMP       (pre-computed, used for cross-validation)
       Validation providers (used only by CrossValidator for spot-checks):
         - prices         → yfinance
         - fundamentals   → polygon

    2. **Schema normalization** — every provider returns slightly different column
       names (`bottom_line_net_income` vs `consolidated_net_income`, `period_ending`
       vs `date`, `symbol` vs no ticker at all). Schemas declared in
       `data.schemas` (PRICE_SCHEMA, INCOME_SCHEMA, BALANCE_SCHEMA,
       CASHFLOW_SCHEMA) collapse those into one canonical shape, with required-
       column assertions that fail loud if a provider silently changes its
       response. To support a new provider, add its source names to the
       relevant Field.sources tuple — no change here.

    3. **Foreign-ticker fallback** — FMP only covers US-listed names. Pass
       `provider="yfinance"` to any fetch method to route foreign tickers
       there instead. The caller is responsible for passing the ticker in the
       form the provider expects (yfinance form for yfinance, bare symbol for
       FMP); translation from broker-native symbology lives in
       `data.symbology`. Use `symbology.is_foreign_ticker(ticker)` at the call
       site to decide when to flip the kwarg.

    Method surface, by data type:
      - prices:       get_prices
      - statements:   get_{income,balance,cashflow}_statements
      - joined:       get_fundamentals   (joined on fiscal_year/period)
      - provider-computed: get_provider_ratios, get_provider_metrics,
                           get_provider_forward_pe   (cross-validation only)
      - estimates:    get_forward_eps
      - reference:    get_company_profile, get_sector_groups, screen_universe

    Stateless. No in-memory caching, no retries, no rate-limit handling — those
    live in ingestion.py / ParquetWriter. API keys come from Settings and are
    pushed into OpenBB's credential store at construction.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        prices_provider: str | None = None,
        fundamentals_provider: str | None = None,
        estimates_provider: str | None = None,
    ) -> None:
        """Build a client with explicit provider routing, falling back to Settings defaults."""
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
    def _ensure_ticker(df: pl.DataFrame, ticker: str) -> pl.DataFrame:
        """Stamp `ticker` onto the DataFrame if the provider didn't return it."""
        if "ticker" not in df.columns:
            return df.with_columns(pl.lit(ticker).alias("ticker"))
        return df

    @staticmethod
    def _force_ticker(df: pl.DataFrame, ticker: str) -> pl.DataFrame:
        """Drop any provider-supplied symbol/ticker column and stamp on `ticker`.

        Used when the API was called with a translated symbol (yfinance's
        exchange-suffixed form) but the canonical key for storage should be
        the caller's original input.
        """
        df = df.drop([c for c in ("symbol", "ticker") if c in df.columns])
        return df.with_columns(pl.lit(ticker).alias("ticker"))

    def _normalize_statement(
        self,
        df: pl.DataFrame,
        ticker: str,
        schema: tuple[schemas.Field, ...],
    ) -> pl.DataFrame:
        """Coerce a raw statement DataFrame into a canonical schema.

        Pipeline: apply schema → backfill join keys → validate. Backfill exists
        because get_fundamentals joins on (ticker, fiscal_year, fiscal_period);
        a null in either key silently drops the row from the join.
        """
        df = schemas.apply(df, schema)
        df = self._ensure_ticker(df, ticker)
        # Backfill fiscal_year when the provider doesn't return it (Polygon
        # fundamentals, yfinance foreign tickers). NOTE: assumes fiscal_year ==
        # calendar year of report_date — wrong for off-calendar fiscal years
        # (e.g. AAPL's FY ends in September). Safe today because the only
        # callers that hit this branch are annual-only foreign tickers.
        if "fiscal_year" not in df.columns and "report_date" in df.columns:
            df = df.with_columns(pl.col("report_date").dt.year().alias("fiscal_year"))
        # "FY" is a sentinel meaning "annual / unknown period" — picked so the
        # join key is non-null, not to encode real period information.
        if "fiscal_period" not in df.columns:
            df = df.with_columns(pl.lit("FY").alias("fiscal_period"))
        schemas.validate(df, schema)
        return df

    def get_prices(
        self,
        ticker: str,
        start_date: date | None = None,
        end_date: date | None = None,
        provider: str | None = None,
    ) -> pl.DataFrame:
        """Daily OHLCV history for a single ticker.

        `ticker` must be in the form the chosen provider expects (yfinance form
        for yfinance — `0700.HK`, not `700`; bare symbol for FMP). Translation
        from broker-native symbology happens upstream in `data.symbology`.
        """
        provider = provider or self.prices_provider
        logger.info(f"Fetching prices for {ticker} via {provider}")
        result = obb.equity.price.historical(
            symbol=ticker,
            provider=provider,
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None,
        )
        df = self._to_polars(result)
        df = self._force_ticker(df, ticker)
        df = schemas.apply(df, schemas.PRICE_SCHEMA)
        schemas.validate(df, schemas.PRICE_SCHEMA)
        if "adjusted_close" not in df.columns:
            df = df.with_columns(pl.col("close").alias("adjusted_close"))
        return df

    def _fetch_statement(
        self,
        endpoint: Any,
        ticker: str,
        period: str,
        limit: int,
        provider: str,
        schema: tuple[schemas.Field, ...],
        label: str,
    ) -> pl.DataFrame:
        """Shared fetch path for income/balance/cashflow statements."""
        logger.info(f"Fetching {period} {label} for {ticker} via {provider}")
        result = endpoint(
            symbol=ticker,
            provider=provider,
            period=_PERIOD_MAP[period],
            limit=limit,
        )
        df = self._to_polars(result)
        df = self._force_ticker(df, ticker)
        return self._normalize_statement(df, ticker, schema)

    def get_income_statements(
        self,
        ticker: str,
        period: str = "quarterly",
        limit: int = 20,
        provider: str | None = None,
    ) -> pl.DataFrame:
        """Income statement history (quarterly or annual). See class docstring for routing."""
        return self._fetch_statement(
            obb.equity.fundamental.income,
            ticker, period, limit,
            provider or self.fundamentals_provider,
            schemas.INCOME_SCHEMA, "income",
        )

    def get_balance_sheets(
        self,
        ticker: str,
        period: str = "quarterly",
        limit: int = 20,
        provider: str | None = None,
    ) -> pl.DataFrame:
        """Balance sheet history (quarterly or annual). See class docstring for routing."""
        return self._fetch_statement(
            obb.equity.fundamental.balance,
            ticker, period, limit,
            provider or self.fundamentals_provider,
            schemas.BALANCE_SCHEMA, "balance",
        )

    def get_cashflow_statements(
        self,
        ticker: str,
        period: str = "quarterly",
        limit: int = 20,
        provider: str | None = None,
    ) -> pl.DataFrame:
        """Cash flow statement history (quarterly or annual). See class docstring for routing."""
        return self._fetch_statement(
            obb.equity.fundamental.cash,
            ticker, period, limit,
            provider or self.fundamentals_provider,
            schemas.CASHFLOW_SCHEMA, "cashflow",
        )

    def get_fundamentals(
        self,
        ticker: str,
        period: str = "quarterly",
        limit: int | None = None,
        provider: str | None = None,
    ) -> pl.DataFrame:
        """Combined income + balance + cashflow joined on (fiscal_year, fiscal_period).

        Computes free_cash_flow = operating_cash_flow - |capex| when available.
        Income is required; balance/cashflow failures are tolerated with a
        warning so a partial result still gets persisted (yfinance in particular
        often serves income but not the others for foreign tickers).
        """
        provider = provider or self.fundamentals_provider
        # yfinance caps fundamentals depth — asking for more just wastes the call.
        if limit is None:
            limit = 5 if provider == "yfinance" else 20

        income = self.get_income_statements(ticker, period, limit, provider=provider)
        try:
            balance = self.get_balance_sheets(ticker, period, limit, provider=provider)
        except Exception as e:
            logger.warning(f"{ticker}: balance sheet unavailable via {provider}: {e}")
            balance = pl.DataFrame()
        try:
            cashflow = self.get_cashflow_statements(ticker, period, limit, provider=provider)
        except Exception as e:
            logger.warning(f"{ticker}: cash flow unavailable via {provider}: {e}")
            cashflow = pl.DataFrame()

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

        result = obb.equity.estimates.forward_eps(
            symbol=ticker, provider=self.estimates_provider, limit=limit
        )
        df = self._to_polars(result)
        return self._ensure_ticker(df, ticker)

    def get_provider_forward_pe(self, ticker: str) -> pl.DataFrame:
        """FMP's pre-computed forward PE — for cross-checking our self-computed value."""

        result = obb.equity.estimates.forward_pe(
            symbol=ticker, provider=self.estimates_provider
        )
        df = self._to_polars(result)
        return self._ensure_ticker(df, ticker)

    def get_company_profile(self, ticker: str) -> dict:
        """Single-row company profile (sector, industry, description, etc.) as a dict."""

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

        result = obb.equity.compare.groups(group=group, metric=metric, provider="finviz")
        return self._to_polars(result)

    def screen_universe(self, provider: str = "finviz", **filters: Any) -> pl.DataFrame:
        """Universe-wide ticker screening via obb.equity.screener.

        Default provider is finviz (free, no API key needed for screener).
        Filter names depend on the underlying provider.
        """

        result = obb.equity.screener(provider=provider, **filters)
        return self._to_polars(result)
