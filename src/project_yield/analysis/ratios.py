"""Financial ratio calculations."""

from datetime import date

import polars as pl
from loguru import logger

from project_yield.config import Settings, get_settings
from project_yield.data.openbb_client import OpenBBClient
from project_yield.data.reader import DataReader


class RatioCalculator:
    """Calculates financial ratios from price and fundamental data.

    All trailing ratios use TTM data where applicable. Forward ratios are
    self-computed from analyst consensus EPS (pulled via OpenBBClient) divided
    by the latest local price — methodology consistent with trailing PE/PEG.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        client: OpenBBClient | None = None,
    ) -> None:
        """Initialize calculator with settings."""
        self.settings = settings or get_settings()
        self.reader = DataReader(self.settings)
        self._client = client  # lazily created for forward-looking ratios

    @property
    def client(self) -> OpenBBClient:
        """Lazy OpenBBClient — only constructed if forward metrics are requested."""
        if self._client is None:
            self._client = OpenBBClient(self.settings)
        return self._client

    def get_pe_ratio(
        self,
        ticker: str,
        as_of_date: date | None = None,
    ) -> float | None:
        """Calculate trailing PE ratio.

        PE = Price / TTM EPS

        Args:
            ticker: Stock ticker symbol
            as_of_date: Calculate as of this date (default: latest)

        Returns:
            PE ratio or None if data unavailable
        """
        # Get price
        if as_of_date:
            price_df = (
                self.reader.get_prices(ticker=ticker, end_date=as_of_date)
                .sort("date", descending=True)
                .head(1)
                .collect()
            )
        else:
            price_df = self.reader.get_latest_price(ticker)

        if price_df.is_empty():
            logger.warning(f"{ticker}: No price data for PE calculation")
            return None

        price = price_df["close"][0]

        # Get TTM EPS (calculate from net income / shares if eps not available)
        ttm = self.reader.get_ttm_fundamentals(ticker, as_of_date)
        if ttm.is_empty():
            logger.warning(f"{ticker}: No fundamental data for PE calculation")
            return None

        eps = self._get_ttm_eps(ttm)
        if eps is None or eps == 0:
            logger.warning(f"{ticker}: EPS is zero or None")
            return None

        pe = price / eps
        return round(pe, 2)

    def _get_ttm_eps(self, ttm: pl.DataFrame) -> float | None:
        """Extract or calculate TTM EPS from fundamentals.

        Args:
            ttm: TTM fundamentals DataFrame

        Returns:
            TTM EPS value or None
        """
        # Try direct EPS column first
        if "eps" in ttm.columns:
            eps = ttm["eps"][0]
            if eps is not None:
                return eps

        # Calculate from net income and shares
        if "net_income" in ttm.columns and "shares_outstanding" in ttm.columns:
            net_income = ttm["net_income"][0]
            shares = ttm["shares_outstanding"][0]

            if net_income is not None and shares is not None and shares > 0:
                return net_income / shares

        return None

    def get_peg_ratio(
        self,
        ticker: str,
        years: int = 5,
        as_of_date: date | None = None,
    ) -> float | None:
        """Calculate PEG ratio.

        PEG = PE / (EPS Growth Rate * 100)

        Uses CAGR of EPS over specified years.

        Args:
            ticker: Stock ticker symbol
            years: Years for EPS growth calculation (default: 5)
            as_of_date: Calculate as of this date (default: latest)

        Returns:
            PEG ratio or None if data unavailable
        """
        pe = self.get_pe_ratio(ticker, as_of_date)
        if pe is None or pe <= 0:
            return None

        eps_cagr = self._calculate_eps_cagr(ticker, years, as_of_date)
        if eps_cagr is None or eps_cagr <= 0:
            logger.warning(f"{ticker}: Cannot calculate PEG - negative or zero growth")
            return None

        # PEG = PE / (growth rate as percentage)
        peg = pe / (eps_cagr * 100)
        return round(peg, 2)

    def get_operating_margin(
        self,
        ticker: str,
        as_of_date: date | None = None,
    ) -> float | None:
        """Calculate operating margin.

        Operating Margin = TTM Operating Income / TTM Revenue

        Args:
            ticker: Stock ticker symbol
            as_of_date: Calculate as of this date (default: latest)

        Returns:
            Operating margin as decimal (0.15 = 15%) or None
        """
        ttm = self.reader.get_ttm_fundamentals(ticker, as_of_date)
        if ttm.is_empty():
            return None

        if "operating_income" not in ttm.columns or "revenue" not in ttm.columns:
            logger.warning(f"{ticker}: Missing data for operating margin")
            return None

        revenue = ttm["revenue"][0]
        operating_income = ttm["operating_income"][0]

        if revenue is None or revenue == 0:
            return None

        margin = operating_income / revenue
        return round(margin, 4)

    def get_net_profit_margin(
        self,
        ticker: str,
        as_of_date: date | None = None,
    ) -> float | None:
        """Calculate net profit margin.

        Net Profit Margin = TTM Net Income / TTM Revenue

        Args:
            ticker: Stock ticker symbol
            as_of_date: Calculate as of this date (default: latest)

        Returns:
            Net profit margin as decimal or None
        """
        ttm = self.reader.get_ttm_fundamentals(ticker, as_of_date)
        if ttm.is_empty():
            return None

        if "net_income" not in ttm.columns or "revenue" not in ttm.columns:
            logger.warning(f"{ticker}: Missing data for net profit margin")
            return None

        revenue = ttm["revenue"][0]
        net_income = ttm["net_income"][0]

        if revenue is None or revenue == 0:
            return None

        margin = net_income / revenue
        return round(margin, 4)

    def get_gross_margin(
        self,
        ticker: str,
        as_of_date: date | None = None,
    ) -> float | None:
        """Calculate gross margin.

        Gross Margin = TTM Gross Profit / TTM Revenue

        Args:
            ticker: Stock ticker symbol
            as_of_date: Calculate as of this date (default: latest)

        Returns:
            Gross margin as decimal or None
        """
        ttm = self.reader.get_ttm_fundamentals(ticker, as_of_date)
        if ttm.is_empty():
            return None

        if "gross_profit" not in ttm.columns or "revenue" not in ttm.columns:
            logger.warning(f"{ticker}: Missing data for gross margin")
            return None

        revenue = ttm["revenue"][0]
        gross_profit = ttm["gross_profit"][0]

        if revenue is None or revenue == 0:
            return None

        margin = gross_profit / revenue
        return round(margin, 4)

    def get_revenue_growth(
        self,
        ticker: str,
        as_of_date: date | None = None,
    ) -> float | None:
        """Calculate year-over-year revenue growth.

        Compares most recent quarter to same quarter last year.

        Args:
            ticker: Stock ticker symbol
            as_of_date: Calculate as of this date (default: latest)

        Returns:
            Revenue growth rate as decimal or None
        """
        return self._calculate_yoy_growth(ticker, "revenue", as_of_date)

    def get_eps_growth(
        self,
        ticker: str,
        as_of_date: date | None = None,
    ) -> float | None:
        """Calculate year-over-year EPS growth.

        Compares most recent quarter to same quarter last year.

        Args:
            ticker: Stock ticker symbol
            as_of_date: Calculate as of this date (default: latest)

        Returns:
            EPS growth rate as decimal or None
        """
        lf = self.reader.get_fundamentals_quarterly(ticker=ticker)
        if as_of_date:
            lf = lf.filter(pl.col("report_date") <= as_of_date)

        df = lf.sort("report_date", descending=True).collect()

        if df.is_empty():
            return None

        # Need at least 5 quarters
        if len(df) < 5:
            return None

        # Calculate EPS
        df = self._add_calculated_eps(df)
        if "calc_eps" not in df.columns:
            return None

        current = df["calc_eps"][0]
        year_ago = df["calc_eps"][4]

        if year_ago is None or year_ago == 0 or current is None:
            return None

        growth = (current - year_ago) / abs(year_ago)
        return round(growth, 4)

    def get_rd_intensity(
        self,
        ticker: str,
        as_of_date: date | None = None,
    ) -> float | None:
        """Calculate R&D intensity.

        R&D Intensity = TTM R&D Expense / TTM Revenue

        Args:
            ticker: Stock ticker symbol
            as_of_date: Calculate as of this date (default: latest)

        Returns:
            R&D intensity as decimal or None
        """
        ttm = self.reader.get_ttm_fundamentals(ticker, as_of_date)
        if ttm.is_empty():
            return None

        if "rd_expense" not in ttm.columns or "revenue" not in ttm.columns:
            logger.warning(f"{ticker}: Missing data for R&D intensity")
            return None

        revenue = ttm["revenue"][0]
        rd_expense = ttm["rd_expense"][0]

        if revenue is None or revenue == 0 or rd_expense is None:
            return None

        intensity = abs(rd_expense) / revenue  # R&D is sometimes negative
        return round(intensity, 4)

    def get_capex_ratio(
        self,
        ticker: str,
        as_of_date: date | None = None,
    ) -> float | None:
        """Calculate CapEx ratio.

        CapEx Ratio = TTM Capital Expenditure / TTM Revenue

        Args:
            ticker: Stock ticker symbol
            as_of_date: Calculate as of this date (default: latest)

        Returns:
            CapEx ratio as decimal or None
        """
        ttm = self.reader.get_ttm_fundamentals(ticker, as_of_date)
        if ttm.is_empty():
            return None

        if "capex" not in ttm.columns or "revenue" not in ttm.columns:
            logger.warning(f"{ticker}: Missing data for CapEx ratio")
            return None

        revenue = ttm["revenue"][0]
        capex = ttm["capex"][0]

        if revenue is None or revenue == 0 or capex is None:
            return None

        ratio = abs(capex) / revenue  # CapEx is typically negative in cash flow
        return round(ratio, 4)

    def get_forward_pe(
        self, ticker: str, fiscal_period_offset: int = 1
    ) -> float | None:
        """Self-computed forward PE = current price ÷ consensus EPS for FY+offset.

        Pulls analyst consensus forward EPS from OpenBB (FMP), takes the mean
        for the requested fiscal period offset, divides current price (from local
        Parquet) by it. We do NOT use FMP's pre-computed forward PE — the
        methodology stays identical to trailing PE.
        """
        eps = self._get_forward_eps_mean(ticker, fiscal_period_offset)
        if eps is None or eps == 0:
            return None

        price_df = self.reader.get_latest_price(ticker)
        if price_df.is_empty():
            logger.warning(f"{ticker}: no local price for forward PE")
            return None
        price = price_df["close"][0]
        return round(price / eps, 2)

    def get_forward_peg(self, ticker: str) -> float | None:
        """Forward PEG = forward PE ÷ analyst-implied forward EPS growth.

        Growth comes from the ratio of FY+1 to FY+0 (current FY) consensus EPS.
        """
        forward_pe = self.get_forward_pe(ticker, fiscal_period_offset=1)
        if forward_pe is None:
            return None

        eps_now = self._get_forward_eps_mean(ticker, 0)
        eps_next = self._get_forward_eps_mean(ticker, 1)
        if eps_now is None or eps_next is None or eps_now <= 0:
            return None

        growth = (eps_next - eps_now) / abs(eps_now)
        if growth <= 0:
            logger.warning(f"{ticker}: non-positive forward EPS growth, PEG undefined")
            return None
        return round(forward_pe / (growth * 100), 2)

    def _get_forward_eps_mean(
        self, ticker: str, fiscal_period_offset: int
    ) -> float | None:
        """Mean consensus EPS for the requested offset from `obb.equity.estimates.forward_eps`.

        offset=0 means the current FY, offset=1 means next FY, etc.
        """
        try:
            df = self.client.get_forward_eps(ticker)
        except Exception as e:
            logger.warning(f"{ticker}: forward EPS fetch failed: {e}")
            return None
        if df.is_empty():
            return None

        candidates = ["mean", "estimated_eps_mean", "eps_avg", "eps_mean"]
        eps_col = next((c for c in candidates if c in df.columns), None)
        if eps_col is None:
            logger.warning(
                f"{ticker}: forward EPS DataFrame missing mean column; saw {df.columns}"
            )
            return None

        sort_col = next(
            (c for c in ("date", "period_ending", "fiscal_year") if c in df.columns),
            None,
        )
        if sort_col is not None:
            df = df.sort(sort_col)

        if fiscal_period_offset >= df.height:
            return None
        value = df[eps_col][fiscal_period_offset]
        return float(value) if value is not None else None

    def get_provider_ratios_latest(self, ticker: str) -> dict:
        """Latest row of cached FMP-published ratios for a ticker.

        Tries quarterly first, falls back to annual (FMP Starter blocks the
        quarterly ratios endpoint). Returns empty dict if neither is cached.
        """
        for period in ("quarterly", "annual"):
            lf = self.reader.get_provider_ratios(ticker=ticker, period=period)
            df = lf.collect()
            if df.is_empty():
                continue
            sort_col = next(
                (c for c in ("report_date", "fiscal_year") if c in df.columns), None
            )
            if sort_col is not None:
                df = df.sort(sort_col, descending=True)
            return df.head(1).row(0, named=True)
        return {}

    def get_provider_pe_ratio(self, ticker: str) -> float | None:
        """FMP-published PE for cross-validation against get_pe_ratio()."""
        return self._provider_metric(ticker, ["price_to_earnings", "pe_ratio", "pe"])

    def get_provider_pb_ratio(self, ticker: str) -> float | None:
        """FMP-published price-to-book."""
        return self._provider_metric(
            ticker, ["price_to_book", "price_to_book_value", "pb_ratio"]
        )

    def get_provider_ps_ratio(self, ticker: str) -> float | None:
        """FMP-published price-to-sales."""
        return self._provider_metric(ticker, ["price_to_sales", "ps_ratio"])

    def get_provider_roe(self, ticker: str) -> float | None:
        """FMP-published return on equity."""
        return self._provider_metric(ticker, ["return_on_equity", "roe"])

    def get_provider_roa(self, ticker: str) -> float | None:
        """FMP-published return on assets."""
        return self._provider_metric(ticker, ["return_on_assets", "roa"])

    def _provider_metric(
        self, ticker: str, candidate_columns: list[str]
    ) -> float | None:
        """Pull a single metric from cached provider ratios, trying each candidate column."""
        latest = self.get_provider_ratios_latest(ticker)
        if not latest:
            return None
        for col in candidate_columns:
            if col in latest and latest[col] is not None:
                value = latest[col]
                return round(float(value), 4) if isinstance(value, (int, float)) else None
        logger.debug(
            f"{ticker}: none of {candidate_columns} present in provider ratios"
        )
        return None

    def get_all_ratios(
        self,
        ticker: str,
        as_of_date: date | None = None,
    ) -> dict:
        """Calculate all available ratios for a ticker.

        Args:
            ticker: Stock ticker symbol
            as_of_date: Calculate as of this date (default: latest)

        Returns:
            Dict with all ratio values
        """
        return {
            "ticker": ticker,
            "as_of_date": str(as_of_date) if as_of_date else "latest",
            "pe_ratio": self.get_pe_ratio(ticker, as_of_date),
            "peg_ratio": self.get_peg_ratio(ticker, as_of_date=as_of_date),
            "forward_pe": self.get_forward_pe(ticker),
            "forward_peg": self.get_forward_peg(ticker),
            "operating_margin": self.get_operating_margin(ticker, as_of_date),
            "net_profit_margin": self.get_net_profit_margin(ticker, as_of_date),
            "gross_margin": self.get_gross_margin(ticker, as_of_date),
            "revenue_growth": self.get_revenue_growth(ticker, as_of_date),
            "eps_growth": self.get_eps_growth(ticker, as_of_date),
            "rd_intensity": self.get_rd_intensity(ticker, as_of_date),
            "capex_ratio": self.get_capex_ratio(ticker, as_of_date),
        }

    def _calculate_eps_cagr(
        self,
        ticker: str,
        years: int,
        as_of_date: date | None = None,
    ) -> float | None:
        """Calculate compound annual growth rate of EPS.

        CAGR = (Ending / Beginning)^(1/n) - 1

        Args:
            ticker: Stock ticker symbol
            years: Number of years
            as_of_date: Calculate as of this date

        Returns:
            CAGR as decimal or None
        """
        lf = self.reader.get_fundamentals_quarterly(ticker=ticker)
        if as_of_date:
            lf = lf.filter(pl.col("report_date") <= as_of_date)

        df = lf.sort("report_date", descending=True).collect()

        if df.is_empty():
            return None

        # Need at least years * 4 quarters
        required_quarters = years * 4
        if len(df) < required_quarters:
            logger.warning(f"{ticker}: Only {len(df)} quarters, need {required_quarters} for {years}-year CAGR")
            return None

        # Calculate EPS for each quarter
        df = self._add_calculated_eps(df)
        if "calc_eps" not in df.columns:
            return None

        # Get TTM EPS for current and n years ago
        current_ttm = df.head(4)["calc_eps"].sum()
        past_ttm = df.slice(required_quarters - 4, 4)["calc_eps"].sum()

        if past_ttm is None or past_ttm <= 0 or current_ttm is None or current_ttm <= 0:
            return None

        # CAGR formula
        cagr = (current_ttm / past_ttm) ** (1 / years) - 1
        return round(cagr, 4)

    def _add_calculated_eps(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add calculated EPS column to DataFrame.

        Args:
            df: DataFrame with fundamentals data

        Returns:
            DataFrame with calc_eps column
        """
        if "eps" in df.columns:
            return df.with_columns(pl.col("eps").alias("calc_eps"))

        if "net_income" in df.columns and "shares_outstanding" in df.columns:
            return df.with_columns(
                (pl.col("net_income") / pl.col("shares_outstanding")).alias("calc_eps")
            )

        return df

    def _calculate_yoy_growth(
        self,
        ticker: str,
        column: str,
        as_of_date: date | None = None,
    ) -> float | None:
        """Calculate year-over-year growth for a metric.

        Args:
            ticker: Stock ticker symbol
            column: Column name to calculate growth for
            as_of_date: Calculate as of this date

        Returns:
            Growth rate as decimal or None
        """
        lf = self.reader.get_fundamentals_quarterly(ticker=ticker)
        if as_of_date:
            lf = lf.filter(pl.col("report_date") <= as_of_date)

        df = lf.sort("report_date", descending=True).collect()

        if df.is_empty() or column not in df.columns:
            return None

        # Need at least 5 quarters (current + 4 quarters ago)
        if len(df) < 5:
            return None

        current = df[column][0]
        year_ago = df[column][4]  # Same quarter last year

        if year_ago is None or year_ago == 0 or current is None:
            return None

        growth = (current - year_ago) / abs(year_ago)
        return round(growth, 4)
