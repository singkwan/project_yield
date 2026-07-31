"""IBKR Flex Web Service client — token-based REST + XML reports.

Separate from CPAPI: different auth (single token), different endpoints
(gdcdyn.interactivebrokers.com), different latency (30s-3min per query),
and different rate limits (~10 min cooldown per query).

Two-call protocol:
  1. SendRequest -> reference code
  2. GetStatement (polled) -> XML (or CSV) report

Successful responses are cached to `.flex_cache/` per-query so reruns within
`cache_max_age_seconds` skip the network call. Avoids tripping IBKR's
per-query rate limit during development iteration.
"""

from __future__ import annotations

import os
import tempfile
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import polars as pl
import requests
from loguru import logger

from project_yield.brokers.flex_csv import load_flex_csv
from project_yield.config import Settings, get_settings


_FLEX_BASE = "https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService"


class IBKRFlexClient:
    """Token-based client for IBKR Flex Web Service (no OAuth)."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    # --- Per-section reports ---

    def get_dividends_report(self, query_id: str | None = None) -> pl.DataFrame:
        """Run the dividends Flex Query, parse XML to Polars.

        Note: Flex query date range is configured in IBKR Client Portal, not
        passed here. To change the period, edit the query in the portal.
        """
        qid = query_id or self.settings.ibkr_flex_dividends_query_id
        return _flex_to_polars(self._fetch_flex_xml(qid), tag="CashTransaction", filter_type="Dividends")

    def get_interest_report(self, query_id: str | None = None) -> pl.DataFrame:
        qid = query_id or self.settings.ibkr_flex_interest_query_id
        # Interest shows up under both CashTransaction (broker interest paid/received)
        # and InterestAccruals (accrued, not yet paid). Pull the cash side here.
        return _flex_to_polars(
            self._fetch_flex_xml(qid),
            tag="CashTransaction",
            type_contains="Interest",
        )

    def get_nav_history_report(self, query_id: str | None = None) -> pl.DataFrame:
        qid = query_id or self.settings.ibkr_flex_nav_query_id
        return _flex_to_polars(self._fetch_flex_xml(qid), tag="EquitySummaryByReportDateInBase")

    def get_positions_report(self, query_id: str | None = None) -> pl.DataFrame:
        """Open positions snapshot via Flex (workable when OAuth/CPAPI is unavailable).

        Maps Flex columns to the schema PortfolioWriter.write_positions_snapshot expects.
        """
        qid = query_id or self.settings.ibkr_flex_positions_query_id
        df = _flex_to_polars(self._fetch_flex_xml(qid), tag="OpenPosition")
        return _normalize_positions(df)

    def get_trades_report(self, query_id: str | None = None) -> pl.DataFrame:
        """Trades (transactions) via Flex.

        Maps Flex columns to the transactions schema (tx_id, ticker, side, quantity, etc.).
        """
        qid = query_id or self.settings.ibkr_flex_trades_query_id
        df = _flex_to_polars(self._fetch_flex_xml(qid), tag="Trade")
        return _normalize_trades(df)

    def get_consolidated_report(self, query_id: str | None = None) -> dict[str, pl.DataFrame]:
        """One Flex call returns all sections — splits the response into multiple DataFrames.

        Format-agnostic: detects XML vs CSV from the response and routes to
        the right parser. Use this when you've configured a SINGLE Flex query
        containing Open Positions + Trades + Cash Transactions + ChangeInNAV.

        Returns dict with keys: positions, trades, dividends, interest, nav_history.
        """
        qid = query_id or self.settings.ibkr_flex_consolidated_query_id
        text = self._fetch_flex_xml(qid)
        if text.lstrip().startswith("<"):
            return {
                "positions": _normalize_positions(_flex_to_polars(text, tag="OpenPosition")),
                "trades": _normalize_trades(_flex_to_polars(text, tag="Trade")),
                "dividends": _flex_to_polars(text, tag="CashTransaction", filter_type="Dividends"),
                "interest": _flex_to_polars(text, tag="CashTransaction", type_contains="Interest"),
                "nav_history": _flex_to_polars(text, tag="EquitySummaryByReportDateInBase"),
            }
        # CSV path — write to a temp file and reuse the CSV loader
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as fh:
            fh.write(text)
            tmp_path = fh.name
        return load_flex_csv(tmp_path)

    def _fetch_flex_xml(
        self, query_id: str | None, cache_max_age_seconds: int = 600, force_refresh: bool = False
    ) -> str:
        """Two-call Flex protocol: SendRequest -> wait -> GetStatement.

        Caches the most recent successful response to `.flex_cache/` per-query
        so reruns within `cache_max_age_seconds` (default 10 min) skip the
        network call. Pass force_refresh=True to bypass.
        """
        if self.settings.ibkr_flex_token is None:
            raise RuntimeError("ibkr_flex_token not set in Settings")
        if not query_id:
            raise RuntimeError("Flex query_id missing — set the *_query_id field in Settings")
        token = self.settings.ibkr_flex_token.get_secret_value()

        # Cache lives outside data/ so it survives data-directory wipes during iteration
        cache_dir = Path(".flex_cache")
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = cache_dir / f"flex_{query_id}.xml"
        if not force_refresh and cache_file.exists():
            age = time.time() - os.path.getmtime(cache_file)
            if age < cache_max_age_seconds:
                logger.info(f"Flex cache hit for query {query_id} (age {age:.0f}s)")
                return cache_file.read_text()

        send = requests.get(
            f"{_FLEX_BASE}.SendRequest",
            params={"t": token, "q": query_id, "v": "3"},
            timeout=30,
        )
        send.raise_for_status()
        root = ET.fromstring(send.text)
        status = (root.findtext("Status") or "").strip()
        if status != "Success":
            err = root.findtext("ErrorMessage") or send.text
            raise RuntimeError(f"Flex SendRequest failed: status={status} err={err}")
        ref_code = root.findtext("ReferenceCode")
        if not ref_code:
            raise RuntimeError("Flex SendRequest returned no ReferenceCode")

        # IBKR docs: polling can take 1-30 sec for small queries, 1-3 min for
        # heavy ones (180-day all-sections). Up to 20 attempts with growing
        # backoff covers ~3 min total wait.
        for attempt in range(20):
            time.sleep(min(3 + attempt, 15))
            resp = requests.get(
                f"{_FLEX_BASE}.GetStatement",
                params={"t": token, "q": ref_code, "v": "3"},
                timeout=120,
            )
            if resp.status_code != 200:
                logger.debug(f"Flex GetStatement HTTP {resp.status_code} (attempt {attempt+1})")
                continue
            text = resp.text
            # Format-agnostic success check: XML report, OR CSV that starts with
            # the universal IBKR account-ID column header.
            if "<FlexQueryResponse" in text or text.startswith('"ClientAccountID"'):
                cache_file.write_text(text)
                logger.info(f"Cached Flex response to {cache_file} ({len(text)} chars, {'XML' if text.lstrip().startswith('<') else 'CSV'})")
                return text
            # The "not ready" response from GetStatement is itself a tiny XML
            # status doc; if we see Status=Fail, surface the error.
            if "<Status>Fail</Status>" in text:
                err = ET.fromstring(text).findtext("ErrorMessage") or text[:200]
                raise RuntimeError(f"Flex GetStatement failed: {err}")
            logger.debug(f"Flex statement not ready yet (attempt {attempt+1}); retrying")
        raise RuntimeError("Flex GetStatement did not return a report after 20 attempts")


def _normalize_positions(df: pl.DataFrame) -> pl.DataFrame:
    """Map Flex OpenPosition columns -> the schema PortfolioWriter expects."""
    if df.is_empty():
        return df
    rename = {}
    if "position" in df.columns:
        rename["position"] = "quantity"
    if "costBasisPrice" in df.columns:
        rename["costBasisPrice"] = "avg_cost"
    if "assetCategory" in df.columns:
        rename["assetCategory"] = "asset_class"
    if rename:
        df = df.rename(rename)
    for col in ("quantity", "avg_cost", "markPrice", "fifoPnlUnrealized"):
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
    return df


def _normalize_trades(df: pl.DataFrame) -> pl.DataFrame:
    """Map Flex Trade columns -> the schema PortfolioWriter expects (incl. tx_id for dedup)."""
    if df.is_empty():
        return df
    rename = {}
    if "tradeID" in df.columns:
        rename["tradeID"] = "tx_id"
    if "buySell" in df.columns:
        rename["buySell"] = "side"
    if "tradePrice" in df.columns:
        rename["tradePrice"] = "price"
    if "ibCommission" in df.columns:
        rename["ibCommission"] = "fees"
    if "tradeDate" in df.columns:
        rename["tradeDate"] = "trade_date"
    if "settleDateTarget" in df.columns:
        rename["settleDateTarget"] = "settlement_date"
    if rename:
        df = df.rename(rename)
    for col in ("quantity", "price", "fees", "netCash"):
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
    return df


def _flex_to_polars(
    xml_text: str,
    tag: str,
    filter_type: str | None = None,
    type_contains: str | None = None,
) -> pl.DataFrame:
    """Pull all `<tag>` elements from a Flex XML report into a Polars DataFrame.

    Each Flex element is attribute-only (no nested children for the data rows
    we care about), so element.attrib is a flat dict ready for Polars.

    filter_type: keep only rows where attrib['type'] == filter_type
    type_contains: keep rows where filter_type substring is in attrib['type']
    """
    root = ET.fromstring(xml_text)
    rows: list[dict[str, Any]] = []
    for el in root.iter(tag):
        attribs = dict(el.attrib)
        t = attribs.get("type", "")
        if filter_type is not None and t != filter_type:
            continue
        if type_contains is not None and type_contains.lower() not in t.lower():
            continue
        rows.append(attribs)
    if not rows:
        return pl.DataFrame()

    df = pl.from_dicts(rows, infer_schema_length=None)
    # Coerce numeric-looking columns. Cast string→float where convertible;
    # leave anything that fails to parse as null (strict=False).
    _NUMERIC_COLS = {
        "amount", "quantity", "tradePrice", "grossAmount", "withholdingTax", "netAmount",
        "position", "costBasisPrice", "costBasisMoney", "markPrice", "fifoPnlUnrealized",
        "positionValue", "percentOfNAV", "ibCommission", "netCash", "tradeMoney",
        "cost", "proceeds", "fxRateToBase",
        # NAV summary columns
        "cash", "cashLong", "cashShort", "stock", "stockLong", "stockShort",
        "options", "optionsLong", "optionsShort", "bonds", "bondsLong", "bondsShort",
        "funds", "fundsLong", "fundsShort", "total", "totalLong", "totalShort",
        "dividendAccruals", "interestAccruals",
    }
    for col in _NUMERIC_COLS & set(df.columns):
        df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
    # IBKR Flex uses YYYYMMDD (no separators). dateTime can be YYYYMMDD;HHMMSS
    # or YYYYMMDD HHMMSS; either way the first 8 chars are the date.
    for col in ("dateTime", "tradeDate", "reportDate", "settleDate", "exDate", "payDate"):
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).str.slice(0, 8).str.to_date(format="%Y%m%d", strict=False)
            )
    # Normalize the symbol→ticker convention used elsewhere in the codebase
    if "symbol" in df.columns and "ticker" not in df.columns:
        df = df.rename({"symbol": "ticker"})
    return df
