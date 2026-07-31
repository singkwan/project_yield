"""Configuration management using Pydantic Settings.

Settings are loaded with this priority (highest first):
  1. Environment variables
  2. .env file
  3. config.yaml (project root)
  4. Field defaults
"""

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import AliasChoices, Field, SecretStr, field_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_CONFIG_YAML_PATH = _PROJECT_ROOT / "config.yaml"


class _YamlConfigSource(PydanticBaseSettingsSource):
    """Pydantic settings source that reads from config.yaml at the project root."""

    def __init__(self, settings_cls: type["Settings"]) -> None:
        super().__init__(settings_cls)
        self._data: dict[str, Any] = {}
        if _CONFIG_YAML_PATH.exists():
            with _CONFIG_YAML_PATH.open("r") as fh:
                loaded = yaml.safe_load(fh) or {}
            self._data = {str(k).upper(): v for k, v in loaded.items()}

    def get_field_value(self, field, field_name):  # type: ignore[override]
        aliases: list[str] = []
        validation_alias = field.validation_alias
        if isinstance(validation_alias, AliasChoices):
            aliases = [str(c) for c in validation_alias.choices]
        elif isinstance(validation_alias, str):
            aliases = [validation_alias]
        aliases.append(field_name.upper())

        for alias in aliases:
            key = alias.upper()
            if key in self._data:
                return self._data[key], alias, False
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for field_name, field in self.settings_cls.model_fields.items():
            value, key, _ = self.get_field_value(field, field_name)
            if value is not None:
                result[field_name] = value
        return result


class Settings(BaseSettings):
    """Application settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    # OpenBB provider credentials (you bring your own subscriptions)
    openbb_fmp_api_key: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("openbb_fmp_api_key", "fmp_api_key"),
        description="Financial Modeling Prep API key (used for fundamentals + estimates)",
    )
    openbb_polygon_api_key: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "openbb_polygon_api_key", "polygon_api_key", "massive_api_key"
        ),
        description="Polygon.io / Massive.com API key (used for cross-validation)",
    )

    # Provider routing — which OpenBB provider services which data type.
    # Primaries are paid (FMP) for consistency with fundamentals.
    # Validation providers are used only by CrossValidator for spot-checks.
    openbb_prices_provider: str = Field(default="fmp")
    openbb_fundamentals_provider: str = Field(default="fmp")
    openbb_estimates_provider: str = Field(default="fmp")
    openbb_prices_validation_provider: str = Field(default="yfinance")
    openbb_fundamentals_validation_provider: str = Field(default="polygon")

    # IBKR — OAuth 1.0a credentials (per ibind wiki).
    # All seven fields below are required for ibind to call the Client Portal API.
    ibkr_account_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("ibkr_account_id", "account_id"),
        description="Optional explicit IBKR account ID. If unset, resolved via portfolio_accounts.",
    )
    ibkr_oauth_consumer_key: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "ibkr_oauth_consumer_key", "ibind_oauth1a_consumer_key", "ibkr_consumer_key"
        ),
        description="9-character OAuth 1.0a consumer key from IBKR registration",
    )
    ibkr_oauth_access_token: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "ibkr_oauth_access_token", "ibind_oauth1a_access_token", "ibkr_access_token"
        ),
        description="OAuth 1.0a access token generated in IBKR portal",
    )
    ibkr_oauth_access_token_secret: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "ibkr_oauth_access_token_secret",
            "ibind_oauth1a_access_token_secret",
            "ibkr_token_secret",
        ),
        description="OAuth 1.0a access token secret",
    )
    ibkr_oauth_encryption_key_fp: Path | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "ibkr_oauth_encryption_key_fp", "ibind_oauth1a_encryption_key_fp"
        ),
        description="Path to PEM file with the encryption private key (decrypts session token)",
    )
    ibkr_oauth_signature_key_fp: Path | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "ibkr_oauth_signature_key_fp", "ibind_oauth1a_signature_key_fp"
        ),
        description="Path to PEM file with the signature private key (signs requests)",
    )
    ibkr_oauth_dh_prime: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "ibkr_oauth_dh_prime", "ibind_oauth1a_dh_prime"
        ),
        description="Diffie-Hellman prime (hex string). Optional if dh_param_fp is set.",
    )
    ibkr_oauth_dh_param_fp: Path | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "ibkr_oauth_dh_param_fp", "ibkr_dh_param_fp", "ibkr_dhparam_fp"
        ),
        description="Path to dhparam.pem; if set, dh_prime hex is extracted from it on the fly.",
    )

    # IBKR — Flex Web Service (separate from OAuth, token-based)
    ibkr_flex_token: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("ibkr_flex_token", "flex_token"),
    )
    # Per-section query IDs (use these if you have one Flex query per data type)
    ibkr_flex_dividends_query_id: str | None = Field(default=None)
    ibkr_flex_interest_query_id: str | None = Field(default=None)
    ibkr_flex_nav_query_id: str | None = Field(default=None)
    ibkr_flex_positions_query_id: str | None = Field(default=None)
    ibkr_flex_trades_query_id: str | None = Field(default=None)
    # Consolidated query ID (use this if you have ONE Flex query containing all sections)
    ibkr_flex_consolidated_query_id: str | None = Field(default=None)

    # Data Storage
    data_path: Path = Field(default=Path("data"), description="Root path for data storage")

    # Logging
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(default="INFO")
    log_file: Path = Field(default=Path("logs/project_yield.log"))

    # Data Refresh
    default_start_date: str = Field(default="2020-01-01")
    batch_size: int = Field(default=50, ge=1, le=500)

    @field_validator(
        "openbb_fmp_api_key",
        "openbb_polygon_api_key",
        "ibkr_oauth_consumer_key",
        "ibkr_oauth_access_token",
        "ibkr_oauth_access_token_secret",
        "ibkr_oauth_dh_prime",
        "ibkr_flex_token",
        mode="before",
    )
    @classmethod
    def _coerce_secret_to_str(cls, v):
        """YAML parses unquoted all-digit values as int; SecretStr needs str."""
        return None if v is None else str(v)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        """Layer config.yaml between dotenv and defaults."""
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            _YamlConfigSource(settings_cls),
            file_secret_settings,
        )

    @property
    def prices_path(self) -> Path:
        """Path to prices Parquet files."""
        return self.data_path / "prices"

    @property
    def fundamentals_quarterly_path(self) -> Path:
        """Path to quarterly fundamentals Parquet files."""
        return self.data_path / "fundamentals_quarterly"

    @property
    def fundamentals_annual_path(self) -> Path:
        """Path to annual fundamentals Parquet files."""
        return self.data_path / "fundamentals_annual"

    @property
    def provider_ratios_quarterly_path(self) -> Path:
        """Path to FMP-published quarterly ratios (cached for cross-validation)."""
        return self.data_path / "ratios_quarterly"

    @property
    def provider_ratios_annual_path(self) -> Path:
        """Path to FMP-published annual ratios (cached for cross-validation)."""
        return self.data_path / "ratios_annual"

    @property
    def metadata_path(self) -> Path:
        """Path to metadata Parquet files."""
        return self.data_path / "metadata"

    # IBKR portfolio storage paths
    @property
    def portfolio_path(self) -> Path:
        """Root path for IBKR portfolio data (positions, transactions, watchlists)."""
        return self.data_path / "portfolio"

    @property
    def positions_snapshots_path(self) -> Path:
        """Daily snapshots of positions, partitioned by snapshot_date."""
        return self.portfolio_path / "positions"

    @property
    def positions_latest_path(self) -> Path:
        """Convenience single-file with the most recent positions snapshot."""
        return self.portfolio_path / "positions_latest.parquet"

    @property
    def transactions_path(self) -> Path:
        """All-time transactions, append + dedup on tx_id."""
        return self.portfolio_path / "transactions.parquet"

    @property
    def watchlists_path(self) -> Path:
        """Per-watchlist files partitioned by list_id."""
        return self.portfolio_path / "watchlists"

    @property
    def activity_path(self) -> Path:
        """Root path for Flex Web Service output (dividends, interest, nav_history)."""
        return self.data_path / "activity"

    @property
    def dividends_path(self) -> Path:
        """Dividend events, partitioned by year."""
        return self.activity_path / "dividends"

    @property
    def interest_path(self) -> Path:
        """Interest accruals + cash interest events, partitioned by year."""
        return self.activity_path / "interest"

    @property
    def nav_history_path(self) -> Path:
        """Daily NAV time series from Flex, partitioned by year."""
        return self.activity_path / "nav_history"


def get_settings() -> Settings:
    """Get application settings instance."""
    return Settings()
