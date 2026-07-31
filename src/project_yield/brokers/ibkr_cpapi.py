"""IBKR Client Portal API client — thin Polars adapter around ibind (OAuth 1.0a).

Live, sub-second access to positions, account summary, ledger, watchlists,
short-window transaction history, and period performance %. Construction
mirrors ibind's `examples/rest_08_oauth.py`: build an `OAuth1aConfig` from
Settings and pass to `IbkrClient(use_oauth=True, ...)`. ibind handles
`oauth_init` and live-session-token maintenance internally.

The underlying `IbkrClient` is exposed as `self._ibind` so callers can reach
any ibind method we haven't wrapped.
"""

from __future__ import annotations

import subprocess
from functools import cached_property
from pathlib import Path
from typing import Any

import polars as pl
from ibind import IbkrClient
from ibind.oauth.oauth1a import OAuth1aConfig
from loguru import logger

from project_yield.config import Settings, get_settings


class IBKRCPAPIClient:
    """OAuth 1.0a client for IBKR Client Portal API via ibind."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.__ibind: IbkrClient | None = None

    @property
    def _ibind(self) -> IbkrClient:
        """Lazy ibind client — only constructs (and triggers OAuth) on first use."""
        if self.__ibind is None:
            self.__ibind = self._build_ibind()
        return self.__ibind

    def _build_ibind(self) -> IbkrClient:
        s = self.settings
        missing = [
            name
            for name, val in {
                "ibkr_oauth_consumer_key": s.ibkr_oauth_consumer_key,
                "ibkr_oauth_access_token": s.ibkr_oauth_access_token,
                "ibkr_oauth_access_token_secret": s.ibkr_oauth_access_token_secret,
                "ibkr_oauth_encryption_key_fp": s.ibkr_oauth_encryption_key_fp,
                "ibkr_oauth_signature_key_fp": s.ibkr_oauth_signature_key_fp,
            }.items()
            if val is None
        ]
        if missing:
            raise RuntimeError(
                "Missing required IBKR OAuth 1.0a credentials in Settings: "
                f"{missing}. Add to config.yaml."
            )

        dh_prime = self._resolve_dh_prime()
        if dh_prime is None:
            raise RuntimeError(
                "Missing dh_prime. Set IBKR_OAUTH_DH_PRIME (hex) or "
                "IBKR_OAUTH_DH_PARAM_FP (path to dhparam.pem) in config.yaml."
            )

        oauth_config = OAuth1aConfig(
            consumer_key=s.ibkr_oauth_consumer_key.get_secret_value(),
            access_token=s.ibkr_oauth_access_token.get_secret_value(),
            access_token_secret=s.ibkr_oauth_access_token_secret.get_secret_value(),
            encryption_key_fp=str(s.ibkr_oauth_encryption_key_fp),
            signature_key_fp=str(s.ibkr_oauth_signature_key_fp),
            dh_prime=dh_prime,
        )
        logger.info("Initializing ibind IbkrClient with OAuth 1.0a")
        return IbkrClient(use_oauth=True, oauth_config=oauth_config)

    def _resolve_dh_prime(self) -> str | None:
        """Use explicit hex if set, otherwise extract from dhparam.pem."""
        if self.settings.ibkr_oauth_dh_prime is not None:
            return self.settings.ibkr_oauth_dh_prime.get_secret_value()
        if self.settings.ibkr_oauth_dh_param_fp is not None:
            return _extract_dh_prime_hex(self.settings.ibkr_oauth_dh_param_fp)
        return None

    @cached_property
    def account_id(self) -> str:
        """Resolve account ID once. Use Settings override if set, else first account from API."""
        if self.settings.ibkr_account_id:
            return self.settings.ibkr_account_id
        result = self._ibind.portfolio_accounts()
        accounts = result.data if hasattr(result, "data") else result
        if not accounts:
            raise RuntimeError("portfolio_accounts() returned no accounts")
        return accounts[0]["accountId"]

    # --- Account / portfolio ---

    def get_account_summary(self) -> pl.DataFrame:
        result = self._ibind.portfolio_summary(self.account_id)
        return _to_polars(result.data)

    def get_ledger(self) -> pl.DataFrame:
        result = self._ibind.get_ledger(self.account_id)
        return _to_polars(result.data)

    def get_positions(self) -> pl.DataFrame:
        result = self._ibind.positions2(self.account_id)
        return _to_polars(result.data)

    def get_transactions(self, conids: list[int] | None = None, days: int = 90) -> pl.DataFrame:
        result = self._ibind.transaction_history(
            account_ids=[self.account_id], conids=conids or [], days=days
        )
        return _to_polars(result.data)

    def get_performance(self, period: str = "YTD") -> pl.DataFrame:
        result = self._ibind.account_performance(account_ids=[self.account_id], period=period)
        return _to_polars(result.data)

    # --- Watchlists ---

    def get_watchlists(self) -> list[dict]:
        result = self._ibind.get_all_watchlists()
        data = result.data if hasattr(result, "data") else result
        return data.get("data", {}).get("user_lists", []) if isinstance(data, dict) else data

    def get_watchlist(self, list_id: str) -> pl.DataFrame:
        result = self._ibind.get_watchlist_information(list_id)
        return _to_polars(result.data)


def _extract_dh_prime_hex(pem_path: Path) -> str:
    """Use openssl to dump DH parameters and parse the prime as hex.

    Avoids pulling in `cryptography` just for this one operation.
    """
    out = subprocess.run(
        ["openssl", "dhparam", "-in", str(pem_path), "-text", "-noout"],
        capture_output=True,
        check=True,
        text=True,
    ).stdout

    in_prime = False
    hex_chunks: list[str] = []
    for line in out.splitlines():
        stripped = line.strip()
        if stripped.startswith("P:") or stripped.startswith("prime:"):
            in_prime = True
            continue
        if in_prime:
            if stripped.startswith("G:") or stripped.startswith("generator:"):
                break
            hex_chunks.append(stripped.replace(":", "").replace(" ", ""))
    return "".join(hex_chunks)


def _to_polars(data: Any) -> pl.DataFrame:
    """Convert ibind responses (list of dicts, dict, or scalar) to Polars."""
    if data is None:
        return pl.DataFrame()
    if isinstance(data, list):
        return pl.DataFrame() if not data else pl.from_dicts(data)
    if isinstance(data, dict):
        return pl.from_dicts([data])
    return pl.DataFrame()
