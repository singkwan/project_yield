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
from pydantic import AliasChoices, Field, SecretStr
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

    # Data Storage
    data_path: Path = Field(default=Path("data"), description="Root path for data storage")

    # Logging
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(default="INFO")
    log_file: Path = Field(default=Path("logs/project_yield.log"))

    # Data Refresh
    default_start_date: str = Field(default="2020-01-01")
    batch_size: int = Field(default=50, ge=1, le=500)

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


def get_settings() -> Settings:
    """Get application settings instance."""
    return Settings()
