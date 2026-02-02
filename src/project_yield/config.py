"""Configuration management using Pydantic Settings."""

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables and .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # SimFin API
    simfin_api_key: str = Field(description="SimFin API key for financial data")

    # Data Storage
    data_path: Path = Field(default=Path("data"), description="Root path for data storage")

    # Logging
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(default="INFO")
    log_file: Path = Field(default=Path("logs/project_yield.log"))

    # Data Refresh
    default_start_date: str = Field(default="2020-01-01")
    batch_size: int = Field(default=50, ge=1, le=500)

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
    def metadata_path(self) -> Path:
        """Path to metadata Parquet files."""
        return self.data_path / "metadata"


def get_settings() -> Settings:
    """Get application settings instance."""
    return Settings()
