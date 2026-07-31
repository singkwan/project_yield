# IBKR Integration Plan for project_yield

## Context

Connect your IBKR brokerage account to project_yield so the platform tracks your real portfolio alongside the existing OpenBB-based research data. Goals:

1. Fetch current positions + holdings + average cost
2. Fetch transactions (cost basis, performance reconstruction)
3. Fetch watchlists
4. Use held / watched tickers to drive what the OpenBB pipeline ingests (held tickers always have fresh data)
5. **Portfolio performance** — YTD, monthly, biggest winners/losers
6. **Dividend / interest income detail** — especially for bond funds where you currently lack visibility

**Non-goals:**
- Order placement / live trading (read-only)
- Multi-account support (one account)
- Real-time WebSocket streaming (polling REST is enough for portfolio analysis)

## Library + auth choice

After surveying the IBKR access landscape (TWS API, raw `ibapi`, `ib_async`, CP Gateway, Flex Web Service, IBKR Web API OAuth 2.0), the chosen stack:

| Component | Used for | Why |
|---|---|---|
| **`ibind`** (Voyz, OAuth 1.0a) | Live positions, transactions, watchlists, account summary | Headless REST against IBKR Client Portal API. No Java gateway, no Docker, no daily login. Modern, hands-free. |
| **Flex Web Service** | Dividend / interest detail | Purpose-built for income reports; only path that gives per-coupon and per-dividend granularity needed for bond fund yield analysis. Self-service token, no gateway. |

Rejected alternatives (decision log):
- **OAuth 2.0 Web API** — institutional-only for individual users, no ETA for retail. Apply via `webapionboarding@ibkr.com` if you want it long-term, but not blocking.
- **`ib_async` (TWS API socket)** — most mature wrapper but requires IB Gateway running (Java) and IBC for headless re-login. Workable but heavier than ibind+OAuth.
- **CP Gateway directly** — same endpoints as ibind exposes, but requires running `clientportal.gw` Java + manual login per session. ibind eliminates this with OAuth 1.0a.
- **`ibind + ibeam`** — fallback if OAuth 1.0a setup proves painful. Same `ibind` library, but ibeam manages a Docker-headless CP Gateway. Requires Docker.

**Setup overhead:** One-time OAuth 1.0a configuration in IBKR Client Portal (~30-60 min). Generates consumer key, access token, signing material. After that, fully hands-free.

## Architecture

```
┌────────────────────────────────────────────────────┐
│  ibind (CPAPI REST + OAuth 1.0a) +                 │
│  Flex Web Service (REST + token, XML reports)      │
└────────────────────┬───────────────────────────────┘
                     │
        ┌────────────▼─────────────┐
        │ brokers/ibkr_client.py   │ Single client wrapping both
        └────────────┬─────────────┘
                     │
        ┌────────────▼─────────────┐
        │ data/portfolio.py        │ PortfolioWriter / PortfolioReader
        └────────────┬─────────────┘
                     │
   ┌─────────────────┼──────────────────┐
   │                 │                  │
┌──▼───────────┐  ┌──▼─────────────┐  ┌─▼──────────────┐
│ analysis/    │  │ data/          │  │ visualization/ │
│ portfolio.py │  │ ingestion.py   │  │ (existing)     │
│              │  │ (extended)     │  │                │
└──────────────┘  └────────────────┘  └────────────────┘
                     │
   ┌─────────────────▼─────────────┐
   │ ProjectYield facade           │ + portfolio methods (flat, no sub-facade)
   └───────────────────────────────┘
```

Mirrors the existing `OpenBBClient → ParquetWriter → DataReader → analysis → facade` pattern. New code lives alongside without touching OpenBB-side logic.

## Storage layout

```
data/
├── portfolio/
│   ├── positions/snapshot_date=2026-05-02/data.parquet     # one row per ticker, daily
│   ├── positions_latest.parquet                             # convenience: most recent snapshot
│   ├── transactions.parquet                                 # all-time, append + dedup on tx_id
│   ├── account_summary.parquet                              # NetLiq, cash, currency
│   └── watchlists/list_id=Tech/data.parquet                 # one file per IBKR watchlist
└── activity/                                                # Flex Web Service output
    ├── dividends/year=2026/data.parquet
    ├── interest/year=2026/data.parquet
    ├── nav_history/year=2026/data.parquet                   # daily NAV from Flex (one row per trading day)
    └── corporate_actions/year=2026/data.parquet
```

Daily-snapshot partitioning of `positions/` lets `analysis/portfolio.py` reconstruct historical position values by joining quantity at snapshot date with prices from the existing `data/prices/` Parquet. No need to store derived market values per day.

## Decisions confirmed with user

1. **API**: `ibind` (OAuth 1.0a) + Flex Web Service
2. **Refresh trigger**: Explicit only — `pyld.update_held()` must be called. No surprise IBKR API calls during research.
3. **Performance method**: Transaction-replay against local price Parquet. Works retroactively on full transaction history; no need to start daily snapshots before getting historical data.

## Implementation plan

### Phase 1: ibind client + auth + Settings (~1 day)

**Design principle: thin adapter, not a re-wrapper.** ibind already has named methods for every Client Portal endpoint we need (via PortfolioMixin, WatchlistMixin, AccountsMixin, etc.). Our `IBKRClient` should:
1. Construct ibind's `IbkrClient` with OAuth 1.0a credentials from Settings
2. Call `oauth_init()` once to establish the session
3. For each method we use: one-line delegation to ibind, then Polars conversion + column normalization
4. Expose the underlying ibind client as `self._ibind` so power users can call any other ibind method directly

**New module:** `src/project_yield/brokers/`

- `brokers/__init__.py`
- `brokers/ibkr_client.py` — `IBKRClient`:
  - `__init__(self, settings)` — builds `ibind.IbkrClient(...)` from Settings, calls `oauth_init()`
  - `account_id` (property) — resolves once via `self._ibind.portfolio_accounts()` and caches
  - Thin adapters that delegate to ibind, returning Polars:
    - `get_positions()` → `self._ibind.positions2(account_id)` → Polars
    - `get_account_summary()` → `self._ibind.portfolio_summary(account_id)` → Polars
    - `get_ledger()` → `self._ibind.get_ledger(account_id)` → Polars (multi-currency cash)
    - `get_transactions(conids=None, days=90)` → `self._ibind.transaction_history(account_ids, conids, days=days)` → Polars
    - `get_watchlists()` → `self._ibind.get_all_watchlists()` → list of `{id, name}` dicts
    - `get_watchlist(list_id)` → `self._ibind.get_watchlist_information(list_id)` → Polars (rows = tickers)
    - `get_performance(period="YTD")` → `self._ibind.account_performance(account_ids, period)` → Polars
  - Flex Web Service methods (NOT in ibind — we add):
    - `get_dividends_report(start_date, end_date)` (added in Phase 5)
    - `get_interest_report(start_date, end_date)` (added in Phase 5)
  - `_to_polars(data)` — generic helper: `pl.from_dicts(data)` with empty-list guard

ibind methods we'll specifically use (full inventory in mixin docs):
- PortfolioMixin: `portfolio_accounts`, `positions2`, `portfolio_summary`, `get_ledger`, `transaction_history`, `account_performance`
- WatchlistMixin: `get_all_watchlists`, `get_watchlist_information`
- SessionMixin: `oauth_init`

If we need an endpoint not adapted yet, the user can reach `pyld.broker._ibind.<any_method>()` directly without us writing a wrapper.

**Modified files:**
- [src/project_yield/config.py](src/project_yield/config.py) — add:
  - `ibkr_account_id: str | None`
  - `ibkr_oauth_consumer_key: SecretStr | None`
  - `ibkr_oauth_access_token: SecretStr | None`
  - `ibkr_oauth_access_token_secret: SecretStr | None`
  - `ibkr_oauth_signature_key_path: Path | None` (path to PEM/DER signing key)
  - `ibkr_oauth_dh_prime: SecretStr | None` (DH prime for OAuth 1.0a session if required by ibind config)
  - Path properties: `portfolio_path`, `activity_path`, `watchlists_path`
- [pyproject.toml](pyproject.toml) — add `ibind>=0.1.23`. Drop `ib-async>=1.0.0` from phase2 deps (not using TWS).

**Smoke test:** `IBKRClient().get_account_summary()` returns NetLiq, cash, currency.

**README addition:** "IBKR OAuth 1.0a setup" section with step-by-step Client Portal screenshots / paths.

### Phase 2: Portfolio storage layer (~0.5 day)

**New file:** `src/project_yield/data/portfolio.py` — `PortfolioWriter` and `PortfolioReader`. Mirror `ParquetWriter` / `DataReader` patterns:
- `write_positions_snapshot(df, snapshot_date)` — append-only to `positions/snapshot_date=YYYY-MM-DD/data.parquet`, also overwrite `positions_latest.parquet`
- `write_transactions(df)` — append + dedup on `tx_id`
- `write_dividends(df, year)` / `write_interest(df, year)` — partition by year
- `write_watchlist(df, list_id)` — overwrite per list (small data)
- Reader counterparts: `get_positions(snapshot_date=None)`, `get_transactions(ticker=None, start_date=None)`, `get_dividends(ticker=None, year=None)`, `get_watchlists()`, `get_watchlist(list_id)`
- Use the same defensive `_scan_ticker` / empty-LazyFrame guard pattern as existing reader

Schema for positions: `ticker, asset_class, quantity, avg_cost, currency, snapshot_date`.
Schema for transactions: `tx_id, ticker, side, quantity, price, fees, currency, trade_date, settlement_date`.

### Phase 3: Ingestion integration (~0.5 day)

**Modified:** [src/project_yield/data/ingestion.py](src/project_yield/data/ingestion.py)

Add convenience methods that pull from IBKR and feed into existing OpenBB ingestion:
- `update_held_tickers(start_date=None)` — read latest positions from Parquet → call `update_all_data(tickers=held_tickers)`
- `update_held_and_watched(start_date=None)` — union of held + all watchlist tickers → `update_all_data(...)`
- `sync_holdings()` — pull from IBKR + write snapshot. Does NOT trigger ingestion automatically (per "explicit refresh" decision).

**Surface on facade** ([src/project_yield/core.py](src/project_yield/core.py)):
- `pyld.sync_holdings()` — pull positions + transactions + watchlists from IBKR, write to Parquet
- `pyld.update_held()` — convenience for `sync_holdings()` then `update_all_data(held_tickers)`
- `pyld.update_held_and_watched()`
- `@property broker` — direct access to `IBKRClient`

### Phase 4: Portfolio performance analysis (~1 day)

**New file:** `src/project_yield/analysis/portfolio.py` — `PortfolioAnalysis` class. Reads from portfolio Parquet AND existing `data/prices/`:

Core trick: for any date in your transaction history, derive position quantity as cumulative sum of buys minus sells up to that date. Multiply by close price from local Parquet → market value. Subtract cost basis from cumulative buys → unrealized PnL.

- `current_value()` — latest snapshot joined with latest prices
- `position_history(ticker, start_date=None)` — quantity, MV, PnL time series for one ticker (transaction-replay against local prices)
- `portfolio_value_history(start_date=None, source="nav")` — total portfolio value over time. `source="nav"` uses the Flex NAV history (authoritative, includes deposits/fees); `source="reconstruct"` uses transaction-replay (works without Flex)
- `ytd_performance()` — uses Flex NAV history when available (cleanest), falls back to reconstructed values
- `monthly_performance(months=12)` — month-end NAV from Flex, monthly returns derived
- `winners_losers(period="ytd", top_n=10)` — sorted by absolute $ and % gain
- `position_pnl(ticker)` — realized + unrealized PnL using transactions and prices
- `holdings_summary()` — concentration by asset class, currency

**Surface on facade:**
- `pyld.portfolio_performance(period="ytd")`, `pyld.winners(top_n=10)`, `pyld.losers(top_n=10)`, `pyld.position_pnl(ticker)`, `pyld.holdings_summary()`

### Phase 5: Flex Web Service for income detail + NAV history (~1 day)

**Extends `IBKRClient`:**

Three pre-configured Flex Queries (one-time setup in Client Portal — Performance & Reports → Flex Queries):
- "PY_Dividends" — Dividends + Withholding sections
- "PY_Interest" — Interest Accruals + relevant Cash Transactions sections
- "PY_NAV_History" — Change in NAV + NAV Summary sections; gives daily NAV values across the report period in one query (saves us from reconstructing NAV from positions × prices)

**Snapshot model note**: Flex returns one report per query run. "Open Positions" is point-in-time at report end. For multi-day position time series, we use the transaction-replay approach in Phase 4 (cheaper than running Flex daily for a year). Flex's strength is the income reports and the daily NAV time series, both of which it gives in one query.

Two-call Flex protocol:
1. `POST` to `https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.SendRequest?t=<token>&q=<queryId>&v=3` → reference code
2. After short wait: `POST` to `.../FlexStatementService.GetStatement?t=<token>&q=<refcode>&v=3` → XML report

**`IBKRClient` methods:**
- `get_dividends_report(start_date, end_date)` — runs PY_Dividends query, polls, parses XML to Polars
- `get_interest_report(start_date, end_date)` — same for PY_Interest
- `get_nav_history_report(start_date, end_date)` — runs PY_NAV_History, returns DataFrame with `date, starting_nav, ending_nav, deposits, dividends, realized_pnl, unrealized_pnl, fees, ending_nav`

**Persistence:** call from `sync_holdings()` to also write `data/activity/dividends/`, `data/activity/interest/`, and `data/activity/nav_history/` partitions.

**New facade methods:**
- `pyld.dividends(ticker=None, year=None)` — query the cached dividend table
- `pyld.dividend_yield(ticker)` — TTM dividend ÷ current price (uses local prices)
- `pyld.interest_income(year=None, source=None)` — total interest grouped by source
- `pyld.bond_yield_breakdown()` — per-bond-fund coupon income for the year

**Settings additions:**
- `ibkr_flex_token: SecretStr | None`
- `ibkr_flex_dividends_query_id: str | None`
- `ibkr_flex_interest_query_id: str | None`
- `ibkr_flex_nav_query_id: str | None`

### Phase 6: Validation (~0.5 day)

1. `pyld.broker.get_account_summary()` returns sane NetLiq.
2. `pyld.sync_holdings()` writes positions + transactions + watchlists to Parquet.
3. `pyld.update_held()` triggers OpenBB ingestion for every held ticker.
4. `pyld.holdings_summary()` shows concentration by asset class.
5. `pyld.portfolio_performance()` returns sane YTD %.
6. `pyld.winners(top_n=5)` lists 5 best performers.
7. `pyld.dividends(year=2026)` shows every dividend received YTD.
8. `pyld.bond_yield_breakdown()` shows coupon income from bond fund holdings.
9. End-to-end smoke notebook: `notebooks/portfolio.ipynb` (new) covering all of the above with charts.

## Critical files to read before implementing

- [src/project_yield/data/openbb_client.py](src/project_yield/data/openbb_client.py) — pattern to mirror for `IBKRClient` (settings injection, normalization, error handling)
- [src/project_yield/data/writer.py](src/project_yield/data/writer.py) — Hive partitioning convention
- [src/project_yield/data/reader.py](src/project_yield/data/reader.py) — lazy scan + `_scan_ticker` empty-schema guard
- [src/project_yield/config.py](src/project_yield/config.py) — Settings + AliasChoices + YAML loader
- [src/project_yield/core.py](src/project_yield/core.py) — facade composition pattern (no sub-facades; flat methods + `@property` for direct access)
- [src/project_yield/data/ingestion.py](src/project_yield/data/ingestion.py) — orchestration extension point
- [Voyz/ibind README](https://github.com/Voyz/ibind) — ibind usage patterns
- [IBKR OAuth 1.0a Wiki](https://github.com/Voyz/ibind/wiki/OAuth-1.0a) — exact setup steps for credentials
- [IBKR Flex Web Service docs](https://www.interactivebrokers.com/campus/ibkr-api-page/flex-web-service/) — Flex setup
- [Activity Flex Query Reference](https://www.ibkrguides.com/reportingreference/reportguide/activity%20flex%20query%20reference.htm) — XML field names

## Setup checklist (one-time)

1. **OAuth 1.0a credentials** in IBKR Client Portal (per [ibind wiki](https://github.com/Voyz/ibind/wiki/OAuth-1.0a)) — **DONE by user**.
   - Tokens may take minutes-to-hours to propagate after generation; if Phase 1 smoke test fails with auth errors, wait and retry before assuming a code bug.
   - Defer end-to-end auth verification to the Phase 1 smoke test (`pyld.broker.get_account_summary()`).
2. **Flex Web Service** in IBKR Client Portal — **TODO before Phase 5**:
   - Performance & Reports → Flex Queries → create "PY_Dividends" and "PY_Interest" Activity Flex Queries
   - Flex Web Service Configuration → generate token (1 year duration)
   - Note the query IDs and token; add to `config.yaml`

## Out of scope

- Order placement / live trading
- Multi-account support
- Real-time WebSocket streaming (ibind supports it; not needed for portfolio analysis)
- Tax lot accounting beyond what IBKR's transactions report includes
- Currency conversion for non-USD positions (display in native currency)
- Auto-refresh on a schedule (manual `pyld.sync_holdings()` only — per user decision)
