"""On-demand cross-validation: pull the same data from FMP and Polygon, compare.

Not a persistent storage path. Used for spot-checking the primary (FMP) source
against an independent reference (Polygon) when a number looks suspicious.
"""

from datetime import date, timedelta

import polars as pl
from loguru import logger

from project_yield.config import Settings, get_settings
from project_yield.data.openbb_client import OpenBBClient


_NUMERIC_DTYPES = {pl.Float64, pl.Float32, pl.Int64, pl.Int32}
_DROP_FIELDS = {"ticker", "fiscal_year", "fiscal_period", "report_date"}


class CrossValidator:
    """Pull the same data from FMP and Polygon and report per-field divergence."""

    def __init__(
        self,
        settings: Settings | None = None,
        fmp_client: OpenBBClient | None = None,
        polygon_client: OpenBBClient | None = None,
        flag_threshold: float = 0.01,
    ) -> None:
        self.settings = settings or get_settings()
        self.fmp = fmp_client or OpenBBClient(self.settings)
        self.polygon = polygon_client or OpenBBClient(
            self.settings,
            fundamentals_provider=self.settings.openbb_fundamentals_validation_provider,
            estimates_provider="fmp",
        )
        self.flag_threshold = flag_threshold

    def cross_validate_fundamentals(
        self,
        ticker: str,
        statement: str = "income",
        period: str = "quarterly",
        lookback: int = 4,
    ) -> pl.DataFrame:
        """Pull `statement` from FMP and Polygon, return per-field diff for the most recent `lookback` periods.

        statement: "income" | "balance" | "cashflow"
        Returns long-form: fiscal_year, fiscal_period, field, fmp_value, polygon_value, pct_diff, flagged.
        """
        getter = {
            "income": "get_income_statements",
            "balance": "get_balance_sheets",
            "cashflow": "get_cashflow_statements",
        }.get(statement)
        if getter is None:
            raise ValueError(f"Unknown statement: {statement}")

        try:
            fmp_df = getattr(self.fmp, getter)(ticker, period=period, limit=lookback)
        except Exception as e:
            logger.error(f"FMP fetch failed for {ticker} {statement}: {e}")
            return pl.DataFrame()
        try:
            poly_df = getattr(self.polygon, getter)(ticker, period=period, limit=lookback)
        except Exception as e:
            logger.error(f"Polygon fetch failed for {ticker} {statement}: {e}")
            return pl.DataFrame()

        return self._diff_frames(fmp_df, poly_df, key=("fiscal_year", "fiscal_period"))

    def cross_validate_prices(
        self, ticker: str, lookback_days: int = 30
    ) -> pl.DataFrame:
        """Compare daily closes from the primary prices provider vs the validation provider.

        Primary defaults to FMP (Settings.openbb_prices_provider).
        Validation defaults to yfinance (Settings.openbb_prices_validation_provider).
        """
        end = date.today()
        start = end - timedelta(days=lookback_days)
        primary = self.settings.openbb_prices_provider
        validator = self.settings.openbb_prices_validation_provider
        try:
            primary_df = self.fmp.get_prices(ticker, start_date=start, end_date=end)
            validator_df = OpenBBClient(
                self.settings, prices_provider=validator
            ).get_prices(ticker, start_date=start, end_date=end)
        except Exception as e:
            logger.error(f"Price fetch failed for {ticker}: {e}")
            return pl.DataFrame()

        primary_slim = primary_df.select(["date", "close"]).rename({"close": f"{primary}_close"})
        validator_slim = validator_df.select(["date", "close"]).rename({"close": f"{validator}_close"})
        merged = primary_slim.join(validator_slim, on="date", how="inner")
        merged = merged.with_columns(
            (
                (pl.col(f"{primary}_close") - pl.col(f"{validator}_close"))
                / pl.col(f"{validator}_close")
            ).alias("pct_diff")
        )
        merged = merged.with_columns(
            (pl.col("pct_diff").abs() > self.flag_threshold).alias("flagged")
        )
        return merged.sort("date", descending=True)

    def _diff_frames(
        self,
        fmp_df: pl.DataFrame,
        poly_df: pl.DataFrame,
        key: tuple[str, ...],
    ) -> pl.DataFrame:
        """Long-form diff between matching rows on `key`. Numeric fields only."""
        if fmp_df.is_empty() or poly_df.is_empty():
            logger.warning("One of the frames is empty — nothing to diff")
            return pl.DataFrame()

        join_cols = [c for c in key if c in fmp_df.columns and c in poly_df.columns]
        if not join_cols:
            logger.error(f"No common key columns in {key}")
            return pl.DataFrame()

        fmp_renamed = fmp_df.rename({c: f"fmp__{c}" for c in fmp_df.columns if c not in join_cols})
        poly_renamed = poly_df.rename({c: f"polygon__{c}" for c in poly_df.columns if c not in join_cols})
        merged = fmp_renamed.join(poly_renamed, on=join_cols, how="inner")
        if merged.is_empty():
            logger.warning(f"No matching {key} rows between FMP and Polygon")
            return pl.DataFrame()

        rows: list[dict] = []
        candidate_fields = [
            c.removeprefix("fmp__") for c in merged.columns if c.startswith("fmp__")
        ]
        for record in merged.iter_rows(named=True):
            for field in candidate_fields:
                if field in _DROP_FIELDS:
                    continue
                fmp_val = record.get(f"fmp__{field}")
                poly_val = record.get(f"polygon__{field}")
                if fmp_val is None or poly_val is None:
                    continue
                if not isinstance(fmp_val, (int, float)) or not isinstance(poly_val, (int, float)):
                    continue
                if poly_val == 0:
                    pct = None
                    flagged = fmp_val != 0
                else:
                    pct = (fmp_val - poly_val) / abs(poly_val)
                    flagged = abs(pct) > self.flag_threshold
                rows.append(
                    {
                        **{k: record[k] for k in join_cols},
                        "field": field,
                        "fmp_value": float(fmp_val),
                        "polygon_value": float(poly_val),
                        "pct_diff": pct,
                        "flagged": flagged,
                    }
                )

        return pl.DataFrame(rows)
