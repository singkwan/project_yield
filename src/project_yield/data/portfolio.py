"""Portfolio storage layer — writer + reader for IBKR-sourced data.

Mirrors ParquetWriter / DataReader patterns. Hive-partitioned where useful:
- positions/snapshot_date=YYYY-MM-DD/data.parquet  (daily snapshots)
- transactions.parquet                              (single file, append + dedup)
- watchlists/list_id=X/data.parquet                 (one file per list)
- activity/dividends/year=YYYY/data.parquet         (yearly partition)
- activity/interest/year=YYYY/data.parquet
- activity/nav_history/year=YYYY/data.parquet

The reader's `_scan_*` helpers return None when no data exists so callers can
short-circuit before applying filters that would fail on an empty schema —
same pattern as DataReader.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
from loguru import logger

from project_yield.config import Settings, get_settings


class PortfolioWriter:
    """Writes IBKR portfolio + activity data to partitioned Parquet."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._ensure_directories()

    def _ensure_directories(self) -> None:
        for path in [
            self.settings.portfolio_path,
            self.settings.positions_snapshots_path,
            self.settings.watchlists_path,
            self.settings.activity_path,
            self.settings.dividends_path,
            self.settings.interest_path,
            self.settings.nav_history_path,
        ]:
            path.mkdir(parents=True, exist_ok=True)

    def write_positions_snapshot(
        self, df: pl.DataFrame, snapshot_date: date | None = None
    ) -> Path | None:
        """Write a positions snapshot for a given date and update positions_latest.parquet.

        Idempotent: re-writing the same snapshot_date overwrites the prior write.
        """
        if df.is_empty():
            logger.warning("Empty positions snapshot, skipping write")
            return None
        snapshot_date = snapshot_date or date.today()
        if "snapshot_date" not in df.columns:
            df = df.with_columns(pl.lit(snapshot_date).alias("snapshot_date"))

        partition = self.settings.positions_snapshots_path / f"snapshot_date={snapshot_date.isoformat()}"
        partition.mkdir(parents=True, exist_ok=True)
        file_path = partition / "data.parquet"
        df.write_parquet(file_path, compression="snappy")

        df.write_parquet(self.settings.positions_latest_path, compression="snappy")
        logger.info(f"Wrote positions snapshot: {len(df)} rows for {snapshot_date}")
        return file_path

    def write_transactions(self, df: pl.DataFrame) -> Path | None:
        """Append + dedup transactions on tx_id."""
        if df.is_empty():
            logger.warning("Empty transactions, skipping write")
            return None
        if "tx_id" not in df.columns:
            raise ValueError("transactions DataFrame must have a 'tx_id' column for dedup")

        target = self.settings.transactions_path
        if target.exists():
            existing = pl.read_parquet(target)
            merged = pl.concat([existing, df], how="diagonal_relaxed")
            merged = merged.unique(subset=["tx_id"], keep="last")
        else:
            merged = df

        sort_col = next((c for c in ("trade_date", "tx_id") if c in merged.columns), None)
        if sort_col is not None:
            merged = merged.sort(sort_col)
        merged.write_parquet(target, compression="snappy")
        logger.info(f"Transactions: {len(df)} new rows -> {len(merged)} total")
        return target

    def write_watchlist(self, df: pl.DataFrame, list_id: str) -> Path | None:
        if df.is_empty():
            logger.warning(f"Empty watchlist {list_id}, skipping")
            return None
        partition = self.settings.watchlists_path / f"list_id={list_id}"
        partition.mkdir(parents=True, exist_ok=True)
        file_path = partition / "data.parquet"
        df.write_parquet(file_path, compression="snappy")
        logger.info(f"Wrote watchlist {list_id}: {len(df)} rows")
        return file_path

    def write_dividends(self, df: pl.DataFrame, year: int) -> Path | None:
        return self._write_yearly(df, year, self.settings.dividends_path, "dividends")

    def write_interest(self, df: pl.DataFrame, year: int) -> Path | None:
        return self._write_yearly(df, year, self.settings.interest_path, "interest")

    def write_nav_history(self, df: pl.DataFrame, year: int) -> Path | None:
        return self._write_yearly(df, year, self.settings.nav_history_path, "nav_history")

    @staticmethod
    def _write_yearly(df: pl.DataFrame, year: int, base_path: Path, label: str) -> Path | None:
        if df.is_empty():
            logger.warning(f"Empty {label} for {year}, skipping")
            return None
        partition = base_path / f"year={year}"
        partition.mkdir(parents=True, exist_ok=True)
        file_path = partition / "data.parquet"
        df.write_parquet(file_path, compression="snappy")
        logger.info(f"Wrote {label} for {year}: {len(df)} rows")
        return file_path


class PortfolioReader:
    """Reads IBKR portfolio + activity data with lazy Polars scans."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    @staticmethod
    def _scan(base_path: Path) -> pl.LazyFrame | None:
        """Glob-scan parquet under base_path; return None if no files."""
        if not base_path.exists():
            return None
        files = list(base_path.glob("**/*.parquet"))
        if not files:
            return None
        return pl.scan_parquet(str(base_path / "**" / "*.parquet"))

    @staticmethod
    def _scan_one(file_path: Path) -> pl.LazyFrame | None:
        if not file_path.exists():
            return None
        return pl.scan_parquet(file_path)

    # --- positions ---

    def get_positions_latest(self) -> pl.DataFrame:
        """Latest snapshot as an eager DataFrame (small, single file)."""
        if not self.settings.positions_latest_path.exists():
            return pl.DataFrame()
        return pl.read_parquet(self.settings.positions_latest_path)

    def get_positions_snapshots(
        self, start_date: date | None = None, end_date: date | None = None
    ) -> pl.LazyFrame:
        """All historical snapshots as a LazyFrame, optionally date-filtered."""
        scanned = self._scan(self.settings.positions_snapshots_path)
        if scanned is None:
            return pl.LazyFrame()
        lf = scanned
        if start_date is not None:
            lf = lf.filter(pl.col("snapshot_date") >= start_date)
        if end_date is not None:
            lf = lf.filter(pl.col("snapshot_date") <= end_date)
        return lf

    def list_held_tickers(self) -> list[str]:
        """Tickers currently held (non-zero quantity in latest snapshot)."""
        df = self.get_positions_latest()
        if df.is_empty() or "ticker" not in df.columns:
            return []
        if "quantity" in df.columns:
            df = df.filter(pl.col("quantity") != 0)
        return sorted(df["ticker"].drop_nulls().unique().to_list())

    # --- transactions ---

    def get_transactions(
        self,
        ticker: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pl.LazyFrame:
        scanned = self._scan_one(self.settings.transactions_path)
        if scanned is None:
            return pl.LazyFrame()
        lf = scanned
        if ticker is not None:
            lf = lf.filter(pl.col("ticker") == ticker)
        if start_date is not None:
            lf = lf.filter(pl.col("trade_date") >= start_date)
        if end_date is not None:
            lf = lf.filter(pl.col("trade_date") <= end_date)
        return lf

    # --- watchlists ---

    def list_watchlist_ids(self) -> list[str]:
        if not self.settings.watchlists_path.exists():
            return []
        return sorted(
            p.name.split("=", 1)[1]
            for p in self.settings.watchlists_path.iterdir()
            if p.is_dir() and p.name.startswith("list_id=")
        )

    def get_watchlist(self, list_id: str) -> pl.DataFrame:
        path = self.settings.watchlists_path / f"list_id={list_id}" / "data.parquet"
        if not path.exists():
            return pl.DataFrame()
        return pl.read_parquet(path)

    def list_watched_tickers(self) -> list[str]:
        """Union of tickers across all watchlists."""
        tickers: set[str] = set()
        for list_id in self.list_watchlist_ids():
            df = self.get_watchlist(list_id)
            if not df.is_empty() and "ticker" in df.columns:
                tickers.update(df["ticker"].drop_nulls().unique().to_list())
        return sorted(tickers)

    # --- activity (dividends, interest, NAV history) ---

    def get_dividends(
        self, ticker: str | None = None, year: int | None = None
    ) -> pl.LazyFrame:
        return self._scan_activity(self.settings.dividends_path, ticker=ticker, year=year)

    def get_interest(
        self, ticker: str | None = None, year: int | None = None
    ) -> pl.LazyFrame:
        return self._scan_activity(self.settings.interest_path, ticker=ticker, year=year)

    def get_nav_history(self, year: int | None = None) -> pl.LazyFrame:
        return self._scan_activity(self.settings.nav_history_path, ticker=None, year=year)

    @staticmethod
    def _scan_activity(
        base_path: Path, ticker: str | None, year: int | None
    ) -> pl.LazyFrame:
        if year is not None:
            file_path = base_path / f"year={year}" / "data.parquet"
            if not file_path.exists():
                return pl.LazyFrame()
            lf = pl.scan_parquet(file_path)
        else:
            scanned = PortfolioReader._scan(base_path)
            if scanned is None:
                return pl.LazyFrame()
            lf = scanned
        if ticker is not None:
            lf = lf.filter(pl.col("ticker") == ticker)
        return lf
