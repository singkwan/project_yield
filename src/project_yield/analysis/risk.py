"""Risk / quantitative metrics powered by openbb-quantitative.

These functions are pure local computation — no API calls. We read prices from
the local Parquet via DataReader, derive returns, and hand the resulting pandas
DataFrame to obb.quantitative.* which does the math. Output is converted back to
Polars for consistency with the rest of the pipeline.
"""

from datetime import date
from typing import Any

import polars as pl
from loguru import logger

from project_yield.config import Settings, get_settings
from project_yield.data.reader import DataReader


class RiskMetrics:
    """Sharpe, Sortino, kurtosis, skew, normality — computed locally via openbb-quantitative."""

    def __init__(
        self,
        settings: Settings | None = None,
        reader: DataReader | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.reader = reader or DataReader(self.settings)

    def _prices_pandas(
        self,
        ticker: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ):
        """Load prices from Parquet → pandas DataFrame with 'date' and 'close' columns.

        OpenBB's quantitative.* endpoints expect raw prices (target='close')
        and compute returns internally — we don't pre-compute them.
        """
        prices = (
            self.reader.get_prices(
                ticker=ticker,
                start_date=start_date,
                end_date=end_date,
                columns=["date", "close"],
            )
            .sort("date")
            .collect()
        )
        if prices.is_empty():
            logger.warning(f"{ticker}: no local price data for risk metrics")
            return None
        return prices.to_pandas()

    @staticmethod
    def _to_polars(obbject: Any) -> pl.DataFrame:
        """Convert OBBject result back to a Polars DataFrame."""
        return pl.from_pandas(obbject.to_df().reset_index())

    def sharpe_ratio(
        self,
        ticker: str,
        rfr: float = 0.04,
        window: int = 252,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pl.DataFrame:
        """Rolling Sharpe ratio. Default rfr is 4% (US T-bill region)."""
        from openbb import obb

        prices_pd = self._prices_pandas(ticker, start_date, end_date)
        if prices_pd is None:
            return pl.DataFrame()
        result = obb.quantitative.performance.sharpe_ratio(
            data=prices_pd, target="close", rfr=rfr, window=window
        )
        return self._to_polars(result)

    def sortino_ratio(
        self,
        ticker: str,
        target_return: float = 0.0,
        window: int = 252,
        adjusted: bool = False,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pl.DataFrame:
        """Rolling Sortino ratio."""
        from openbb import obb

        prices_pd = self._prices_pandas(ticker, start_date, end_date)
        if prices_pd is None:
            return pl.DataFrame()
        result = obb.quantitative.performance.sortino_ratio(
            data=prices_pd,
            target="close",
            target_return=target_return,
            window=window,
            adjusted=adjusted,
        )
        return self._to_polars(result)

    def kurtosis(
        self,
        ticker: str,
        window: int = 252,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pl.DataFrame:
        from openbb import obb

        prices_pd = self._prices_pandas(ticker, start_date, end_date)
        if prices_pd is None:
            return pl.DataFrame()
        result = obb.quantitative.rolling.kurtosis(
            data=prices_pd, target="close", window=window
        )
        return self._to_polars(result)

    def skewness(
        self,
        ticker: str,
        window: int = 252,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pl.DataFrame:
        from openbb import obb

        prices_pd = self._prices_pandas(ticker, start_date, end_date)
        if prices_pd is None:
            return pl.DataFrame()
        result = obb.quantitative.rolling.skew(
            data=prices_pd, target="close", window=window
        )
        return self._to_polars(result)

    def normality_test(
        self,
        ticker: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pl.DataFrame:
        from openbb import obb

        prices_pd = self._prices_pandas(ticker, start_date, end_date)
        if prices_pd is None:
            return pl.DataFrame()
        result = obb.quantitative.normality(data=prices_pd, target="close")
        return self._to_polars(result)

    def risk_summary(
        self,
        ticker: str,
        rfr: float = 0.04,
        window: int = 252,
    ) -> dict:
        """Compact dict of the most recent Sharpe / Sortino / kurtosis / skew."""
        sharpe = self.sharpe_ratio(ticker, rfr=rfr, window=window)
        sortino = self.sortino_ratio(ticker, window=window)
        kurt = self.kurtosis(ticker, window=window)
        skew = self.skewness(ticker, window=window)

        return {
            "ticker": ticker,
            "sharpe_latest": _last_numeric(sharpe),
            "sortino_latest": _last_numeric(sortino),
            "kurtosis_latest": _last_numeric(kurt),
            "skewness_latest": _last_numeric(skew),
            "window": window,
        }


def _last_numeric(df: pl.DataFrame) -> float | None:
    """Pull the last numeric value from a one-or-two-column rolling result."""
    if df.is_empty():
        return None
    numeric_cols = [
        c for c in df.columns
        if df[c].dtype in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
        and c != "date"
    ]
    if not numeric_cols:
        return None
    series = df[numeric_cols[0]].drop_nulls()
    if series.is_empty():
        return None
    value = series[-1]
    return float(value) if value is not None else None
