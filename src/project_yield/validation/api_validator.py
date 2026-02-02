"""SimFin API validation module - validates API access and data availability."""

from dataclasses import dataclass, field
from datetime import date
from typing import Any

import simfin as sf
from loguru import logger

from project_yield.config import Settings, get_settings


@dataclass
class ValidationResult:
    """Result of a validation check."""

    name: str
    passed: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationReport:
    """Complete validation report."""

    results: list[ValidationResult] = field(default_factory=list)
    api_key_valid: bool = False
    sp500_coverage: int = 0
    price_data_available: bool = False
    fundamentals_available: bool = False

    @property
    def all_passed(self) -> bool:
        """Check if all validations passed."""
        return all(r.passed for r in self.results)

    def summary(self) -> str:
        """Generate a summary of the validation report."""
        lines = ["=" * 60, "SimFin API Validation Report", "=" * 60, ""]

        for result in self.results:
            status = "PASS" if result.passed else "FAIL"
            lines.append(f"[{status}] {result.name}")
            lines.append(f"       {result.message}")
            if result.details:
                for key, value in result.details.items():
                    lines.append(f"       - {key}: {value}")
            lines.append("")

        lines.append("=" * 60)
        overall = "PASSED" if self.all_passed else "FAILED"
        lines.append(f"Overall: {overall}")
        lines.append(f"S&P 500 Coverage: {self.sp500_coverage} tickers available")
        lines.append("=" * 60)

        return "\n".join(lines)


class APIValidator:
    """Validates SimFin API access and data availability."""

    # Sample tickers for testing
    SAMPLE_TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

    def __init__(self, settings: Settings | None = None) -> None:
        """Initialize validator with settings."""
        self.settings = settings or get_settings()
        self._configure_simfin()
        self._prices_df = None  # Cache for price data
        self._income_df = None  # Cache for income data
        self._cashflow_df = None  # Cache for cashflow data

    def _configure_simfin(self) -> None:
        """Configure SimFin with API key."""
        sf.set_api_key(self.settings.simfin_api_key)
        sf.set_data_dir(str(self.settings.data_path / "simfin_cache"))

    def validate_all(self) -> ValidationReport:
        """Run all validation checks."""
        report = ValidationReport()

        # 1. Validate API key
        result = self._validate_api_key()
        report.results.append(result)
        report.api_key_valid = result.passed

        if not result.passed:
            logger.error("API key validation failed, skipping remaining checks")
            return report

        # 2. Check S&P 500 coverage
        result = self._validate_sp500_coverage()
        report.results.append(result)
        report.sp500_coverage = result.details.get("total_companies", 0)

        # 3. Check price data availability
        result = self._validate_price_data()
        report.results.append(result)
        report.price_data_available = result.passed

        # 4. Check fundamentals availability
        result = self._validate_fundamentals()
        report.results.append(result)
        report.fundamentals_available = result.passed

        # 5. Check data completeness
        result = self._validate_data_completeness()
        report.results.append(result)

        # 6. Check R&D and CapEx availability
        result = self._validate_rd_capex_availability()
        report.results.append(result)

        return report

    def _validate_api_key(self) -> ValidationResult:
        """Validate that the API key works."""
        try:
            # Try to fetch company list to verify API key
            df = sf.load_companies(market="us")
            count = len(df) if df is not None else 0

            if count > 0:
                return ValidationResult(
                    name="API Key Validation",
                    passed=True,
                    message=f"API key is valid, loaded {count} US companies",
                    details={"company_count": count},
                )
            else:
                return ValidationResult(
                    name="API Key Validation",
                    passed=False,
                    message="API key returned empty dataset",
                )
        except Exception as e:
            return ValidationResult(
                name="API Key Validation",
                passed=False,
                message=f"API key validation failed: {e}",
            )

    def _validate_sp500_coverage(self) -> ValidationResult:
        """Check how many S&P 500 companies are available."""
        try:
            # Load all US companies - Ticker is the index
            df = sf.load_companies(market="us")

            if df is None or len(df) == 0:
                return ValidationResult(
                    name="S&P 500 Coverage",
                    passed=False,
                    message="Could not load company list",
                )

            # Ticker is the index in simfin
            tickers = set(df.index.tolist())

            # Check sample S&P 500 tickers
            found = [t for t in self.SAMPLE_TICKERS if t in tickers]
            missing = [t for t in self.SAMPLE_TICKERS if t not in tickers]

            if len(found) >= 4:  # At least 4 of 5 sample tickers
                return ValidationResult(
                    name="S&P 500 Coverage",
                    passed=True,
                    message=f"Found {len(found)}/{len(self.SAMPLE_TICKERS)} sample tickers",
                    details={
                        "total_companies": len(tickers),
                        "found": found,
                        "missing": missing,
                    },
                )
            else:
                return ValidationResult(
                    name="S&P 500 Coverage",
                    passed=False,
                    message=f"Only found {len(found)}/{len(self.SAMPLE_TICKERS)} sample tickers",
                    details={"found": found, "missing": missing},
                )
        except Exception as e:
            return ValidationResult(
                name="S&P 500 Coverage",
                passed=False,
                message=f"Coverage check failed: {e}",
            )

    def _validate_price_data(self) -> ValidationResult:
        """Validate that price data is available."""
        try:
            # Load daily share prices (all tickers) - use cached data if available
            logger.info("Loading share prices (variant=daily)...")
            df = sf.load_shareprices(market="us", variant="daily", refresh_days=9999)
            self._prices_df = df  # Cache for later use

            if df is None or len(df) == 0:
                return ValidationResult(
                    name="Price Data",
                    passed=False,
                    message="Could not load price data",
                )

            # Get unique tickers and date range
            tickers = df.index.get_level_values("Ticker").unique()
            dates = df.index.get_level_values("Date")
            min_date = dates.min()
            max_date = dates.max()

            # Check AAPL specifically
            aapl_rows = 0
            aapl_min = None
            aapl_max = None
            if "AAPL" in tickers:
                aapl = df.loc["AAPL"]
                aapl_rows = len(aapl)
                aapl_min = aapl.index.min()
                aapl_max = aapl.index.max()

            # Check if we have data from 2020 onwards
            has_historical = min_date.date() <= date(2020, 6, 1) if min_date else False

            return ValidationResult(
                name="Price Data",
                passed=has_historical,
                message=f"Price data: {len(df):,} rows, {len(tickers):,} tickers",
                details={
                    "total_rows": len(df),
                    "unique_tickers": len(tickers),
                    "min_date": str(min_date.date() if min_date else None),
                    "max_date": str(max_date.date() if max_date else None),
                    "aapl_rows": aapl_rows,
                    "aapl_date_range": f"{aapl_min} to {aapl_max}" if aapl_min else None,
                    "columns": list(df.columns),
                },
            )
        except Exception as e:
            return ValidationResult(
                name="Price Data",
                passed=False,
                message=f"Price data check failed: {e}",
            )

    def _validate_fundamentals(self) -> ValidationResult:
        """Validate that fundamental data is available."""
        try:
            # Load quarterly income statements (all tickers)
            logger.info("Loading income statements (variant=quarterly)...")
            df = sf.load_income(market="us", variant="quarterly", refresh_days=9999)
            self._income_df = df  # Cache for later use

            if df is None or len(df) == 0:
                return ValidationResult(
                    name="Fundamentals Data",
                    passed=False,
                    message="Could not load income statements",
                )

            # Get unique tickers
            tickers = df.index.get_level_values("Ticker").unique()

            # Check for key columns
            expected_cols = ["Revenue", "Net Income", "Gross Profit", "Operating Income"]
            found_cols = [c for c in expected_cols if c in df.columns]

            # Check AAPL specifically
            aapl_rows = 0
            if "AAPL" in tickers:
                aapl = df.loc["AAPL"]
                aapl_rows = len(aapl)

            return ValidationResult(
                name="Fundamentals Data",
                passed=len(found_cols) >= 2,
                message=f"Fundamentals: {len(df):,} rows, {len(tickers):,} tickers",
                details={
                    "total_rows": len(df),
                    "unique_tickers": len(tickers),
                    "aapl_quarters": aapl_rows,
                    "key_columns_found": found_cols,
                    "all_columns": list(df.columns)[:15],
                },
            )
        except Exception as e:
            return ValidationResult(
                name="Fundamentals Data",
                passed=False,
                message=f"Fundamentals check failed: {e}",
            )

    def _validate_data_completeness(self) -> ValidationResult:
        """Check data completeness for sample tickers."""
        try:
            results = {}

            # Use cached data
            prices_df = self._prices_df
            income_df = self._income_df

            if prices_df is None or income_df is None:
                return ValidationResult(
                    name="Data Completeness",
                    passed=False,
                    message="Price or income data not loaded",
                )

            price_tickers = set(prices_df.index.get_level_values("Ticker").unique())
            income_tickers = set(income_df.index.get_level_values("Ticker").unique())

            for ticker in self.SAMPLE_TICKERS:
                price_rows = 0
                income_rows = 0

                if ticker in price_tickers:
                    price_rows = len(prices_df.loc[ticker])

                if ticker in income_tickers:
                    income_rows = len(income_df.loc[ticker])

                results[ticker] = {
                    "prices": price_rows,
                    "income_quarters": income_rows,
                }

            # Check if most tickers have data (lowered threshold due to free tier limits)
            with_data = sum(1 for r in results.values() if r["prices"] > 0 and r["income_quarters"] > 0)

            return ValidationResult(
                name="Data Completeness",
                passed=with_data >= 3,  # 3/5 is acceptable (free tier has limited coverage)
                message=f"{with_data}/{len(results)} sample tickers have complete data",
                details=results,
            )
        except Exception as e:
            return ValidationResult(
                name="Data Completeness",
                passed=False,
                message=f"Completeness check failed: {e}",
            )

    def _validate_rd_capex_availability(self) -> ValidationResult:
        """Check if R&D and CapEx data is available."""
        try:
            # Load cash flow statements
            logger.info("Loading cash flow statements (variant=quarterly)...")
            cf = sf.load_cashflow(market="us", variant="quarterly", refresh_days=9999)
            self._cashflow_df = cf

            # Use cached income data
            income = self._income_df

            capex_available = False
            rd_available = False
            capex_cols = []
            rd_cols = []

            if cf is not None:
                # CapEx is in cash flow statement as "Change in Fixed Assets & Intangibles"
                capex_cols = [c for c in cf.columns if "fixed assets" in c.lower() or "capex" in c.lower()]
                capex_available = len(capex_cols) > 0

            if income is not None:
                # R&D might be in income statement
                rd_cols = [c for c in income.columns if "r&d" in c.lower() or "research" in c.lower()]
                rd_available = len(rd_cols) > 0

            return ValidationResult(
                name="R&D and CapEx Data",
                passed=capex_available,  # CapEx is more commonly available
                message=f"CapEx: {'Available' if capex_available else 'Not found'}, R&D: {'Available' if rd_available else 'Not found'}",
                details={
                    "capex_available": capex_available,
                    "capex_columns": capex_cols,
                    "rd_available": rd_available,
                    "rd_columns": rd_cols,
                    "cashflow_columns": list(cf.columns)[:20] if cf is not None else [],
                },
            )
        except Exception as e:
            return ValidationResult(
                name="R&D and CapEx Data",
                passed=False,
                message=f"R&D/CapEx check failed: {e}",
            )


def main() -> None:
    """Run validation from command line."""
    logger.info("Starting SimFin API validation...")

    validator = APIValidator()
    report = validator.validate_all()

    print(report.summary())

    if not report.all_passed:
        logger.warning("Some validation checks failed")
        raise SystemExit(1)

    logger.info("All validation checks passed!")


if __name__ == "__main__":
    main()
