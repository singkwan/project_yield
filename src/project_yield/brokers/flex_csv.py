"""Parse a manually-downloaded IBKR Flex Activity Query CSV.

IBKR's Flex CSV export concatenates multiple sections into one file:
each section starts with a header row (column names quoted), followed by
data rows with the same column count. Different sections have different
column sets and different counts of columns.

This module sniffs each section by characteristic columns and returns
DataFrames matching the same shape as the XML path
(`brokers.ibkr_flex._flex_to_polars`), so they can be written through
the existing PortfolioWriter without changes.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

import polars as pl
from loguru import logger


_HEADER_FIRST_CELL = "ClientAccountID"


def _section_type(header: list[str]) -> str | None:
    """Sniff the section type from CSV header column names.

    IBKR Flex CSV uses different field names than the XML export:
      - Open Positions: Quantity + MarkPrice (no TradeID)
      - Trades: Quantity + TradeID + TradePrice
      - Cash Transactions: Amount + Type + Description (no Quantity)
      - NAV summary by date: StartingCash + LevelOfDetail (240+ cols)
      - Change in NAV: StartingValue + EndingValue + TWR
      - Account Info: Name + AccountType + DateOpened
    """
    cols = set(header)
    # Account info first — small + distinctive
    if {"Name", "AccountType", "DateOpened"}.issubset(cols):
        return "account_info"
    # ChangeInNAV is the day-level NAV time series we want (one row per day with
    # StartingValue/EndingValue/TWR). Route this to nav_history.
    if {"StartingValue", "EndingValue", "TWR"}.issubset(cols):
        return "nav_history"
    # The 240-col Statement of Funds breakdown is interesting but not what
    # portfolio_value_history needs. Skip for now.
    if "StartingCash" in cols and "LevelOfDetail" in cols:
        return None
    # Trades: have TradeID and TradePrice and Quantity
    if {"TradeID", "TradePrice", "Quantity"}.issubset(cols):
        return "trades"
    # Open positions: Quantity + MarkPrice and NO TradeID (else would be trade)
    if {"Quantity", "MarkPrice"}.issubset(cols) and "TradeID" not in cols:
        return "positions"
    # Cash transactions: Amount + Type + Description, no Quantity (else dividend accrual)
    if {"Amount", "Type", "Description"}.issubset(cols) and "Quantity" not in cols:
        return "cash_transactions"
    return None


def _iter_sections(csv_path: Path) -> Iterable[tuple[str, list[str], list[list[str]]]]:
    """Yield (section_type, header, rows) for every section in the CSV.

    Sections of unrecognized type are still yielded with section_type=None
    so callers can ignore them deliberately.
    """
    with csv_path.open(newline="") as fh:
        reader = csv.reader(fh)
        current_header: list[str] | None = None
        current_type: str | None = None
        current_rows: list[list[str]] = []
        for row in reader:
            if not row:
                continue
            if row[0] == _HEADER_FIRST_CELL:
                # Flush the previous section before starting a new one
                if current_header is not None:
                    yield current_type, current_header, current_rows  # type: ignore[misc]
                current_header = row
                current_type = _section_type(row)
                current_rows = []
            else:
                if current_header is None:
                    continue  # data before any header — skip
                current_rows.append(row)
        if current_header is not None:
            yield current_type, current_header, current_rows  # type: ignore[misc]


def _to_polars(header: list[str], rows: list[list[str]]) -> pl.DataFrame:
    """Build a Polars DataFrame from a header + rows, padding short rows with None."""
    if not rows:
        return pl.DataFrame()
    width = len(header)
    norm = [r + [""] * (width - len(r)) if len(r) < width else r[:width] for r in rows]
    df = pl.DataFrame({col: [r[i] for r in norm] for i, col in enumerate(header)})
    # Replace empty strings with None for cleanliness
    return df.with_columns([
        pl.when(pl.col(c) == "").then(None).otherwise(pl.col(c)).alias(c)
        for c in df.columns
    ])


def _coerce_numeric(df: pl.DataFrame, cols: Iterable[str]) -> pl.DataFrame:
    for c in cols:
        if c in df.columns:
            df = df.with_columns(pl.col(c).cast(pl.Float64, strict=False))
    return df


def _coerce_date(df: pl.DataFrame, cols: Iterable[str]) -> pl.DataFrame:
    """IBKR Flex CSV dates are YYYYMMDD or YYYY-MM-DD; date-times are YYYYMMDD;HHMMSS."""
    for c in cols:
        if c in df.columns:
            df = df.with_columns(
                pl.col(c).cast(pl.Utf8, strict=False).str.slice(0, 10).alias(f"_tmp_{c}")
            )
            # Try YYYY-MM-DD first, then YYYYMMDD on remaining nulls
            df = df.with_columns(
                pl.when(pl.col(f"_tmp_{c}").str.contains("-"))
                .then(pl.col(f"_tmp_{c}").str.to_date(format="%Y-%m-%d", strict=False))
                .otherwise(pl.col(f"_tmp_{c}").str.slice(0, 8).str.to_date(format="%Y%m%d", strict=False))
                .alias(c)
            ).drop(f"_tmp_{c}")
    return df


def _normalize_positions_csv(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df
    rename = {
        "Symbol": "ticker",
        "Quantity": "quantity",  # CSV uses Quantity (XML used Position)
        "CostBasisPrice": "avg_cost",
        "AssetClass": "asset_class",
        "CurrencyPrimary": "currency",
        "ReportDate": "reportDate",
        "MarkPrice": "markPrice",
        "FifoPnlUnrealized": "fifoPnlUnrealized",
    }
    df = df.rename({k: v for k, v in rename.items() if k in df.columns})
    df = _coerce_numeric(
        df,
        ["quantity", "avg_cost", "markPrice", "fifoPnlUnrealized", "FXRateToBase"],
    )
    df = _coerce_date(df, ["reportDate"])
    return df


def _normalize_trades_csv(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df
    rename = {
        "Symbol": "ticker",
        "TradeID": "tx_id",
        "Buy/Sell": "side",  # CSV uses literal "Buy/Sell" with slash (XML used BuySell)
        "TradePrice": "price",
        "IBCommission": "fees",
        "Quantity": "quantity",
        "TradeDate": "trade_date",
        "DateTime": "dateTime",
        "SettleDateTarget": "settlement_date",
        "CurrencyPrimary": "currency",
        "AssetClass": "asset_class",
    }
    df = df.rename({k: v for k, v in rename.items() if k in df.columns})
    df = _coerce_numeric(df, ["quantity", "price", "fees", "NetCash", "TradeMoney"])
    df = _coerce_date(df, ["trade_date", "settlement_date", "dateTime"])
    return df


def _normalize_cash_transactions_csv(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df
    rename = {
        "Symbol": "ticker",
        "Type": "type",
        "Amount": "amount",
        "CurrencyPrimary": "currency",
        "DateTime": "dateTime",
        "ReportDate": "reportDate",
        "SettleDate": "settleDate",
        "ExDate": "exDate",
        "TransactionID": "transactionID",
        "TradeID": "tradeID",
        "Description": "description",
    }
    df = df.rename({k: v for k, v in rename.items() if k in df.columns})
    df = _coerce_numeric(df, ["amount"])
    df = _coerce_date(df, ["dateTime", "reportDate", "settleDate", "exDate"])
    return df


def _normalize_nav_history_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize ChangeInNAV rows to the (reportDate, total) schema portfolio_value_history expects.

    ChangeInNAV gives StartingValue/EndingValue per period. With "Statement by
    Day" enabled, each day produces one row, so EndingValue at ToDate IS the
    daily NAV.
    """
    if df.is_empty():
        return df
    rename = {}
    if "ToDate" in df.columns:
        rename["ToDate"] = "reportDate"
    if "EndingValue" in df.columns:
        rename["EndingValue"] = "total"
    df = df.rename(rename)
    df = _coerce_date(df, ["reportDate"])
    df = _coerce_numeric(df, ["total", "StartingValue", "Mtm", "Realized",
                              "ChangeInUnrealized", "DepositsWithdrawals",
                              "Dividends", "Interest", "Commissions"])
    return df


def load_flex_csv(csv_path: str | Path) -> dict[str, pl.DataFrame]:
    """Parse an IBKR Flex Activity CSV into the same dict shape as get_consolidated_report.

    Concatenates all data rows of the same section type (since IBKR repeats
    each section per statement period when 'Statement by Day' is enabled).

    Returns: {"positions", "trades", "cash_transactions", "dividends",
              "interest", "nav_history"} — each a Polars DataFrame
              (empty for sections not present in the file).
    """
    csv_path = Path(csv_path)
    grouped: dict[str, list[pl.DataFrame]] = {
        "positions": [], "trades": [], "cash_transactions": [], "nav_history": [],
    }
    section_count: dict[str, int] = {}
    for stype, header, rows in _iter_sections(csv_path):
        section_count[stype or "other"] = section_count.get(stype or "other", 0) + 1
        if stype is None or stype not in grouped:
            continue
        df = _to_polars(header, rows)
        if df.is_empty():
            continue
        if stype == "positions":
            grouped["positions"].append(_normalize_positions_csv(df))
        elif stype == "trades":
            grouped["trades"].append(_normalize_trades_csv(df))
        elif stype == "cash_transactions":
            grouped["cash_transactions"].append(_normalize_cash_transactions_csv(df))
        elif stype == "nav_history":
            grouped["nav_history"].append(_normalize_nav_history_csv(df))

    logger.info(f"Flex CSV section counts: {section_count}")

    def concat(frames: list[pl.DataFrame]) -> pl.DataFrame:
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames, how="diagonal_relaxed")

    positions = concat(grouped["positions"])
    trades = concat(grouped["trades"]).unique(subset=["tx_id"], keep="first") if grouped["trades"] else pl.DataFrame()
    cash = concat(grouped["cash_transactions"])
    nav = concat(grouped["nav_history"])
    if not nav.is_empty() and "reportDate" in nav.columns:
        nav = nav.unique(subset=["reportDate"], keep="last").sort("reportDate")

    # Split cash transactions into dividends / interest by type substring
    if cash.is_empty() or "type" not in cash.columns:
        dividends = pl.DataFrame()
        interest = pl.DataFrame()
    else:
        dividends = cash.filter(
            pl.col("type").str.contains("(?i)dividend|payment.*lieu", strict=False)
        )
        interest = cash.filter(pl.col("type").str.contains("(?i)interest", strict=False))

    return {
        "positions": positions,
        "trades": trades,
        "dividends": dividends,
        "interest": interest,
        "nav_history": nav,
    }
