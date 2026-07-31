"""ProjectYield facade - main entry point for the library."""

from datetime import date

import polars as pl

from project_yield.analysis.metrics import MetricsEngine
from project_yield.analysis.portfolio import PortfolioAnalysis
from project_yield.analysis.ratios import RatioCalculator
from project_yield.analysis.risk import RiskMetrics
from project_yield.config import Settings, get_settings
from project_yield.data.cross_validate import CrossValidator
from project_yield.data.ingestion import DataIngestion
from project_yield.data.openbb_client import OpenBBClient
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
        self._client = OpenBBClient(self.settings)
        self._reader = DataReader(self.settings)
        self._writer = ParquetWriter(self.settings)
        self._calculator = RatioCalculator(self.settings, client=self._client)
        self._metrics = MetricsEngine(self.settings, client=self._client)
        self._ingestion = DataIngestion(self.settings, client=self._client)
        self._charts = ChartBuilder(self.settings)
        self._risk = RiskMetrics(self.settings, reader=self._reader)
        self._cross_validator: CrossValidator | None = None
        self._portfolio_analysis = PortfolioAnalysis(
            self.settings,
            portfolio_reader=self._ingestion.portfolio_reader,
            data_reader=self._reader,
        )

    @property
    def cross_validator(self) -> CrossValidator:
        """Lazy CrossValidator (instantiates a second OpenBB client for Polygon)."""
        if self._cross_validator is None:
            self._cross_validator = CrossValidator(self.settings, fmp_client=self._client)
        return self._cross_validator

    def cross_validate(
        self,
        ticker: str,
        statement: str = "income",
        period: str = "quarterly",
        lookback: int = 4,
    ) -> "pl.DataFrame":
        """Compare FMP vs Polygon for a fundamentals statement. See CrossValidator."""
        return self.cross_validator.cross_validate_fundamentals(
            ticker, statement=statement, period=period, lookback=lookback
        )

    def cross_validate_prices(
        self, ticker: str, lookback_days: int = 30
    ) -> "pl.DataFrame":
        """Compare FMP vs Polygon daily closes."""
        return self.cross_validator.cross_validate_prices(ticker, lookback_days=lookback_days)

    # --- Data Management ---

    def update_data(
        self,
        tickers: list[str],
        start_date: date | None = None,
    ) -> dict:
        """Download and update financial data for the given tickers.

        Args:
            tickers: Explicit list of tickers to ingest. Required — no S&P 500 default
                     (use ProjectYield.discover() for universe-wide discovery).
            start_date: Start date for prices (default from settings).

        Returns:
            Summary dict with counts.
        """
        return self._ingestion.update_all_data(tickers, start_date)

    # --- IBKR portfolio sync ---

    def sync_holdings(
        self,
        include_transactions: bool = True,
        include_watchlists: bool = True,
        include_activity: bool = True,
        transaction_lookback_days: int = 365,
    ) -> dict:
        """Pull positions, transactions, watchlists, and Flex activity from IBKR.

        Does not trigger OpenBB ingestion — call update_held() or
        update_held_and_watched() afterward to refresh prices/fundamentals
        for the held / watched tickers.
        """
        return self._ingestion.sync_holdings(
            include_transactions=include_transactions,
            include_watchlists=include_watchlists,
            include_activity=include_activity,
            transaction_lookback_days=transaction_lookback_days,
        )

    def sync_via_flex_csv(self, csv_path) -> dict:
        """Bootstrap from a manually-downloaded Flex CSV file (no API call)."""
        return self._ingestion.sync_via_flex_csv(csv_path)

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
        """Bootstrap portfolio data from Flex Web Service alone (no OAuth needed).

        Pass query IDs explicitly OR set the corresponding fields in
        Settings (config.yaml). Use consolidated=True with a single Flex
        query containing all sections to dodge IBKR's per-query rate limit.
        """
        return self._ingestion.sync_via_flex(
            consolidated=consolidated,
            positions_query_id=positions_query_id,
            trades_query_id=trades_query_id,
            dividends_query_id=dividends_query_id,
            interest_query_id=interest_query_id,
            nav_query_id=nav_query_id,
            consolidated_query_id=consolidated_query_id,
        )

    # --- Portfolio analysis (Phase 4) ---

    def current_value(self) -> pl.DataFrame:
        """Latest positions joined with latest local prices for live MV / PnL."""
        return self._portfolio_analysis.current_value()

    def position_pnl(self, ticker: str, as_of: date | None = None) -> dict:
        """Per-ticker realized + unrealized PnL via transaction-replay."""
        return self._portfolio_analysis.position_pnl(ticker, as_of)

    def portfolio_value_history(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        source: str = "nav",
    ) -> pl.DataFrame:
        """Portfolio value over time (Flex NAV by default; transaction-replay fallback)."""
        return self._portfolio_analysis.portfolio_value_history(start_date, end_date, source=source)  # type: ignore[arg-type]

    def ytd_performance(self, year: int | None = None) -> dict:
        """YTD return %."""
        return self._portfolio_analysis.ytd_performance(year)

    def monthly_performance(self, months: int = 12) -> pl.DataFrame:
        """Monthly returns for the last N months."""
        return self._portfolio_analysis.monthly_performance(months)

    def winners(self, top_n: int = 10, by: str = "dollar") -> pl.DataFrame:
        """Top N winning positions by $ or % unrealized PnL."""
        return self._portfolio_analysis.winners(top_n=top_n, by=by)  # type: ignore[arg-type]

    def losers(self, top_n: int = 10, by: str = "dollar") -> pl.DataFrame:
        """Top N losing positions by $ or % unrealized PnL."""
        return self._portfolio_analysis.losers(top_n=top_n, by=by)  # type: ignore[arg-type]

    def holdings_summary(self) -> pl.DataFrame:
        """Concentration view: ticker, asset_class, currency, MV, weight %."""
        return self._portfolio_analysis.holdings_summary()

    # --- Flex Web Service activity (Phase 5) ---

    def dividends(self, ticker: str | None = None, year: int | None = None) -> pl.DataFrame:
        """Cached dividend events from Flex (year filter optional)."""
        return self._ingestion.portfolio_reader.get_dividends(ticker=ticker, year=year).collect()

    def interest_income(self, year: int | None = None) -> pl.DataFrame:
        """Cached interest events from Flex."""
        return self._ingestion.portfolio_reader.get_interest(year=year).collect()

    def nav_history(self, year: int | None = None) -> pl.DataFrame:
        """Cached daily NAV from Flex."""
        return self._ingestion.portfolio_reader.get_nav_history(year=year).collect()

    def dividend_yield(self, ticker: str) -> float | None:
        """TTM dividend / current price (uses local prices + Flex dividends)."""
        from datetime import date as _date, timedelta
        ttm_start = _date.today() - timedelta(days=365)
        df = self.dividends(ticker=ticker)
        if df.is_empty():
            return None
        date_col = next((c for c in ("dateTime", "exDate", "payDate") if c in df.columns), None)
        amt_col = next((c for c in ("amount", "netAmount", "grossAmount") if c in df.columns), None)
        if date_col is None or amt_col is None:
            return None
        ttm = df.filter(pl.col(date_col) >= ttm_start)
        total = float(ttm[amt_col].drop_nulls().sum() or 0)
        if total <= 0:
            return None
        latest = self._reader.get_latest_price(ticker)
        if latest.is_empty():
            return None
        # Per-share dividend yield: total $ dividends / current value of held shares.
        # If we have the position quantity, use that; else just total / current price * 1 share.
        pos = self._ingestion.portfolio_reader.get_positions_latest()
        qty = 1.0
        if not pos.is_empty():
            row = pos.filter(pl.col("ticker") == ticker)
            if not row.is_empty() and "quantity" in row.columns:
                qty = float(row["quantity"][0]) or 1.0
        price = float(latest["close"][0])
        return round((total / qty) / price, 4)

    def update_held(self, start_date: date | None = None) -> dict:
        """OpenBB ingestion for tickers in the latest held positions snapshot.

        Reads from local Parquet; call sync_holdings() first to refresh.
        """
        return self._ingestion.update_held_tickers(start_date)

    def update_held_and_watched(self, start_date: date | None = None) -> dict:
        """OpenBB ingestion for the union of held and watched tickers."""
        return self._ingestion.update_held_and_watched(start_date)

    def held_tickers(self) -> list[str]:
        """Tickers in the latest IBKR positions snapshot (non-zero quantity)."""
        return self._ingestion.portfolio_reader.list_held_tickers()

    def watched_tickers(self) -> list[str]:
        """Union of tickers across all IBKR watchlists."""
        return self._ingestion.portfolio_reader.list_watched_tickers()

    @property
    def broker(self):
        """Direct access to IBKRClient (lazy — constructed on first use)."""
        return self._ingestion.broker

    @property
    def portfolio(self):
        """PortfolioReader for direct queries against IBKR Parquet (positions, transactions, dividends)."""
        return self._ingestion.portfolio_reader

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

    def get_forward_pe(self, ticker: str, fiscal_period_offset: int = 1) -> float | None:
        """Self-computed forward PE (current price ÷ FMP consensus EPS for FY+offset)."""
        return self._calculator.get_forward_pe(ticker, fiscal_period_offset)

    def get_forward_peg(self, ticker: str) -> float | None:
        """Self-computed forward PEG."""
        return self._calculator.get_forward_peg(ticker)

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

    def sector_groups(
        self, group: str = "sector", metric: str = "valuation"
    ) -> pl.DataFrame:
        """Sector / industry / country roll-up via OpenBB (no local ingestion required)."""
        return self._metrics.get_sector_groups(group=group, metric=metric)

    def sharpe(self, ticker: str, rfr: float = 0.04, window: int = 252) -> pl.DataFrame:
        """Rolling Sharpe ratio (computed locally via openbb-quantitative)."""
        return self._risk.sharpe_ratio(ticker, rfr=rfr, window=window)

    def sortino(self, ticker: str, window: int = 252) -> pl.DataFrame:
        """Rolling Sortino ratio (computed locally)."""
        return self._risk.sortino_ratio(ticker, window=window)

    def risk_summary(self, ticker: str, window: int = 252) -> dict:
        """Latest Sharpe / Sortino / kurtosis / skew for a ticker."""
        return self._risk.risk_summary(ticker, window=window)

    def discover(self, **filters) -> pl.DataFrame:
        """Universe-wide ticker discovery via obb.equity.screener.

        Returns candidate tickers + headline metrics. Pass results to update_data()
        to ingest into local Parquet.

        Common filters: market_cap_min, pe_max, sector, recommendation, etc.
        Exact filter names depend on the underlying provider (finviz/fmp).
        """
        return self._client.screen_universe(**filters)

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
    def client(self) -> OpenBBClient:
        """Access the OpenBBClient for direct provider calls."""
        return self._client

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
