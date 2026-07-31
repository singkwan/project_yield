"""IBKR coordinator — composes CPAPI (live data) and Flex (historical/income).

Single entry point for callers. Routing rules per the source-of-truth table:
  - CPAPI is primary for: live positions, account summary, ledger, watchlists,
    short-window transactions, period performance %.
  - Flex is primary for: full transaction history, dividends, interest,
    daily NAV time series, point-in-time positions when CPAPI is unavailable.

Both sub-clients are constructed lazily so missing credentials for one path
don't break the other. For any ibind / Flex method not surfaced here, reach
into `.cpapi` or `.flex` directly:
    pyld.broker.cpapi._ibind.<any-ibind-method>(...)
    pyld.broker.flex.<any-flex-method>(...)
"""

from __future__ import annotations

import polars as pl

from project_yield.brokers.ibkr_cpapi import IBKRCPAPIClient
from project_yield.brokers.ibkr_flex import IBKRFlexClient
from project_yield.config import Settings, get_settings


class IBKRClient:
    """Facade composing IBKRCPAPIClient + IBKRFlexClient."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._cpapi: IBKRCPAPIClient | None = None
        self._flex: IBKRFlexClient | None = None

    # --- Lazy sub-client access ---

    @property
    def cpapi(self) -> IBKRCPAPIClient:
        if self._cpapi is None:
            self._cpapi = IBKRCPAPIClient(self.settings)
        return self._cpapi

    @property
    def flex(self) -> IBKRFlexClient:
        if self._flex is None:
            self._flex = IBKRFlexClient(self.settings)
        return self._flex

    @property
    def account_id(self) -> str:
        """CPAPI-resolved account ID (Flex queries bind to it implicitly in the portal)."""
        return self.cpapi.account_id

    # --- CPAPI-primary delegates (live, sub-second) ---

    def get_account_summary(self) -> pl.DataFrame:
        return self.cpapi.get_account_summary()

    def get_ledger(self) -> pl.DataFrame:
        return self.cpapi.get_ledger()

    def get_positions(self) -> pl.DataFrame:
        return self.cpapi.get_positions()

    def get_transactions(self, conids: list[int] | None = None, days: int = 90) -> pl.DataFrame:
        """CPAPI transaction_history — short-window, per-conid. Use Flex for full history."""
        return self.cpapi.get_transactions(conids=conids, days=days)

    def get_performance(self, period: str = "YTD") -> pl.DataFrame:
        return self.cpapi.get_performance(period=period)

    def get_watchlists(self) -> list[dict]:
        return self.cpapi.get_watchlists()

    def get_watchlist(self, list_id: str) -> pl.DataFrame:
        return self.cpapi.get_watchlist(list_id)

    # --- Flex-primary delegates (slow, full history) ---

    def get_dividends_report(self, query_id: str | None = None) -> pl.DataFrame:
        return self.flex.get_dividends_report(query_id=query_id)

    def get_interest_report(self, query_id: str | None = None) -> pl.DataFrame:
        return self.flex.get_interest_report(query_id=query_id)

    def get_nav_history_report(self, query_id: str | None = None) -> pl.DataFrame:
        return self.flex.get_nav_history_report(query_id=query_id)

    def get_positions_report(self, query_id: str | None = None) -> pl.DataFrame:
        return self.flex.get_positions_report(query_id=query_id)

    def get_trades_report(self, query_id: str | None = None) -> pl.DataFrame:
        return self.flex.get_trades_report(query_id=query_id)

    def get_consolidated_report(self, query_id: str | None = None) -> dict[str, pl.DataFrame]:
        return self.flex.get_consolidated_report(query_id=query_id)

    # --- Cross-source orchestration ---

    def sync_fast(self) -> dict[str, pl.DataFrame]:
        """Live snapshot via CPAPI only — sub-second. Safe for interactive use."""
        return {
            "positions": self.get_positions(),
            "account_summary": self.get_account_summary(),
            "ledger": self.get_ledger(),
        }

    def sync_full(self, consolidated_query_id: str | None = None) -> dict[str, pl.DataFrame]:
        """Fast path + Flex consolidated query. Slow (~1-3 min); one rate-limit hit."""
        out = self.sync_fast()
        out.update(self.flex.get_consolidated_report(query_id=consolidated_query_id))
        return out
