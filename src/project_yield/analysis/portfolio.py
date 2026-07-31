"""Portfolio performance analysis — runs against local Parquet only.

Core trick: for any historical date, derive position quantity by replaying
transactions (cumulative buys − sells up to that date), then multiply by
the close price on that date from the existing data/prices/ Parquet. This
lets us reconstruct full-history portfolio value without daily snapshots,
as long as transactions are complete.

When Flex NAV history is also cached (Phase 5), portfolio_value_history()
defaults to that since it includes deposits/withdrawals/fees that pure
transaction-replay can't capture.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Literal

import polars as pl
from loguru import logger

from project_yield.config import Settings, get_settings
from project_yield.data.portfolio import PortfolioReader
from project_yield.data.reader import DataReader


class PortfolioAnalysis:
    """Performance, attribution, and concentration analytics on local portfolio data."""

    def __init__(
        self,
        settings: Settings | None = None,
        portfolio_reader: PortfolioReader | None = None,
        data_reader: DataReader | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.portfolio = portfolio_reader or PortfolioReader(self.settings)
        self.prices = data_reader or DataReader(self.settings)

    # --- helpers ---

    def _latest_prices(self, tickers: list[str]) -> dict[str, float]:
        """Most recent close per ticker from local Parquet."""
        out: dict[str, float] = {}
        for t in tickers:
            df = self.prices.get_latest_price(t)
            if not df.is_empty() and "close" in df.columns:
                out[t] = float(df["close"][0])
        return out

    def _price_on_or_before(self, ticker: str, target: date) -> float | None:
        """Last close at or before target date (handles weekends/holidays).

        Returns None when no local price data exists for this ticker.
        """
        lf = self.prices.get_prices(ticker=ticker, end_date=target, columns=["date", "close"])
        if "date" not in lf.collect_schema().names():
            return None
        df = lf.sort("date", descending=True).head(1).collect()
        if df.is_empty():
            return None
        return float(df["close"][0])

    @staticmethod
    def _signed_quantity(side: pl.Expr, qty: pl.Expr) -> pl.Expr:
        """Convert side ('BUY'/'SELL') + quantity into signed delta."""
        return pl.when(side.str.to_uppercase() == "SELL").then(-qty).otherwise(qty)

    def _quantity_at(self, ticker: str, target: date) -> float:
        """Cumulative net quantity for ticker as of target date (transaction-replay)."""
        df = (
            self.portfolio.get_transactions(ticker=ticker, end_date=target)
            .collect()
        )
        if df.is_empty():
            return 0.0
        signed = self._signed_quantity(pl.col("side"), pl.col("quantity"))
        return float(df.with_columns(signed.alias("_q"))["_q"].sum())

    # --- current value ---

    def current_value(self) -> pl.DataFrame:
        """Latest positions joined with latest prices for live MV.

        Multi-currency: applies FXRateToBase from the IBKR positions snapshot
        so all market_value / cost_basis / unrealized_pnl numbers are in the
        account's base currency (USD for most users). Native-currency
        last_price and avg_cost are still surfaced for transparency.

        Returns: ticker, currency, quantity, avg_cost, last_price,
                 market_value (base ccy), cost_basis (base ccy),
                 unrealized_pnl (base ccy), unrealized_pct, fx_rate
        """
        pos = self.portfolio.get_positions_latest()
        if pos.is_empty() or "ticker" not in pos.columns:
            return pl.DataFrame()

        prices = self._latest_prices(pos["ticker"].to_list())
        rows = []
        for r in pos.iter_rows(named=True):
            t = r["ticker"]
            qty = float(r.get("quantity") or 0)
            avg = float(r.get("avg_cost") or 0)  # native ccy
            ccy = r.get("currency")
            fx_raw = r.get("FXRateToBase")
            try:
                fx = float(fx_raw) if fx_raw is not None else 1.0
            except (TypeError, ValueError):
                fx = 1.0
            last = prices.get(t)  # native ccy
            mv_native = (qty * last) if last is not None else None
            cost_native = qty * avg
            mv_base = (mv_native * fx) if mv_native is not None else None
            cost_base = cost_native * fx
            pnl_base = (mv_base - cost_base) if mv_base is not None else None
            rows.append({
                "ticker": t,
                "currency": ccy,
                "quantity": qty,
                "avg_cost": avg,
                "last_price": last,
                "fx_rate": fx,
                "market_value": mv_base,
                "cost_basis": cost_base,
                "unrealized_pnl": pnl_base,
                "unrealized_pct": (pnl_base / cost_base) if (pnl_base is not None and cost_base) else None,
            })
        return pl.DataFrame(rows)

    # --- per-ticker history ---

    def position_pnl(self, ticker: str, as_of: date | None = None) -> dict:
        """Realized + unrealized PnL for one ticker (transaction-replay).

        Realized = sum of (sell_price − avg_cost_at_sale) × sold_qty over all sells.
        Unrealized = (last_price − weighted_avg_cost) × current_qty.
        Avg cost is recomputed on each buy (weighted by quantity); sells don't
        change avg cost (FIFO/LIFO not tracked — IBKR's transactions report has
        the lot detail if you need it).
        """
        target = as_of or date.today()
        tx = (
            self.portfolio.get_transactions(ticker=ticker, end_date=target)
            .sort("trade_date")
            .collect()
        )
        if tx.is_empty():
            return {"ticker": ticker, "quantity": 0, "realized_pnl": 0.0, "unrealized_pnl": None}

        qty = 0.0
        cost = 0.0  # total cost basis at current avg
        realized = 0.0
        for r in tx.iter_rows(named=True):
            side = (r.get("side") or "").upper()
            q = float(r["quantity"])
            p = float(r["price"])
            f = float(r.get("fees") or 0)
            if side == "BUY":
                cost += q * p + f
                qty += q
            elif side == "SELL":
                if qty > 0:
                    avg = cost / qty
                    realized += (p - avg) * q - f
                    cost -= avg * q
                    qty -= q
                else:
                    realized += (p * q) - f  # short — simplification
                    qty -= q

        last = self._latest_prices([ticker]).get(ticker)
        unreal = ((last - (cost / qty)) * qty) if (qty > 0 and last is not None and cost > 0) else None
        return {
            "ticker": ticker,
            "quantity": qty,
            "avg_cost": (cost / qty) if qty > 0 else None,
            "last_price": last,
            "market_value": (qty * last) if (qty > 0 and last is not None) else None,
            "realized_pnl": round(realized, 2),
            "unrealized_pnl": round(unreal, 2) if unreal is not None else None,
        }

    # --- portfolio-wide value over time ---

    def portfolio_value_history(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        source: Literal["nav", "reconstruct"] = "nav",
    ) -> pl.DataFrame:
        """Portfolio value (NAV) over time.

        source="nav" uses Flex NAV history (authoritative — includes
        deposits/fees/withdrawals). Falls back to reconstruct if no Flex
        cache exists.

        source="reconstruct" computes from transaction-replay × historical
        prices. Doesn't capture cash deposits/fees but works without Flex.
        """
        if source == "nav":
            nav = self.portfolio.get_nav_history().collect()
            if not nav.is_empty():
                # Normalize Flex NAV schema to (date, market_value)
                date_col = next((c for c in ("reportDate", "date") if c in nav.columns), None)
                value_col = next((c for c in ("total", "ending_nav", "market_value") if c in nav.columns), None)
                if date_col is None or value_col is None:
                    logger.warning(f"NAV cache present but missing date/value columns: {nav.columns[:10]}")
                else:
                    df = nav.select([
                        pl.col(date_col).alias("date"),
                        pl.col(value_col).alias("market_value"),
                    ]).filter(pl.col("market_value").is_not_null())
                    if start_date is not None:
                        df = df.filter(pl.col("date") >= start_date)
                    if end_date is not None:
                        df = df.filter(pl.col("date") <= end_date)
                    return df.unique(subset=["date"]).sort("date")
            logger.info("No Flex NAV cache; falling back to transaction-replay")

        return self._reconstruct_value_history(start_date, end_date)

    def _reconstruct_value_history(
        self, start_date: date | None, end_date: date | None
    ) -> pl.DataFrame:
        end_date = end_date or date.today()
        tx = self.portfolio.get_transactions(end_date=end_date).collect()
        if tx.is_empty():
            return pl.DataFrame()
        first_tx = tx["trade_date"].min()
        start = start_date or first_tx
        if isinstance(start, str):
            start = date.fromisoformat(start)
        if isinstance(end_date, str):
            end_date = date.fromisoformat(end_date)

        # Sample monthly to keep it reasonable. Daily is doable but expensive.
        sample_dates: list[date] = []
        cur = start
        while cur <= end_date:
            sample_dates.append(cur)
            cur = _next_month_start(cur)
        if sample_dates[-1] != end_date:
            sample_dates.append(end_date)

        tickers = tx["ticker"].unique().to_list()
        rows = []
        for d in sample_dates:
            total_mv = 0.0
            for t in tickers:
                qty = self._quantity_at(t, d)
                if qty == 0:
                    continue
                price = self._price_on_or_before(t, d)
                if price is not None:
                    total_mv += qty * price
            rows.append({"date": d, "market_value": round(total_mv, 2), "source": "reconstruct"})
        return pl.DataFrame(rows)

    # --- period returns ---

    def ytd_performance(self, year: int | None = None) -> dict:
        """YTD return = (value now − value at Jan 1) / value at Jan 1.

        Uses NAV history if available; else reconstructs.
        """
        year = year or date.today().year
        start = date(year, 1, 1)
        end = date.today()
        hist = self.portfolio_value_history(start_date=start, end_date=end)
        if hist.is_empty() or len(hist) < 2:
            return {"year": year, "start_value": None, "end_value": None, "return_pct": None}
        col = "market_value" if "market_value" in hist.columns else "ending_nav"
        start_val = float(hist[col][0])
        end_val = float(hist[col][-1])
        ret = (end_val - start_val) / start_val if start_val else None
        return {
            "year": year,
            "start_value": round(start_val, 2),
            "end_value": round(end_val, 2),
            "return_pct": round(ret, 4) if ret is not None else None,
        }

    def monthly_performance(self, months: int = 12) -> pl.DataFrame:
        """Monthly returns for the last `months` months.

        Aggregates the daily NAV series down to month-end values and
        computes month-over-month percent change. Returns one row per month.
        """
        end = date.today()
        start = _months_ago(end, months + 1)
        hist = self.portfolio_value_history(start_date=start, end_date=end)
        if hist.is_empty():
            return pl.DataFrame()
        col = "market_value" if "market_value" in hist.columns else "ending_nav"
        # Take last NAV value per (year, month) — i.e. month-end value
        monthly = (
            hist.with_columns([
                pl.col("date").dt.year().alias("_y"),
                pl.col("date").dt.month().alias("_m"),
            ])
            .sort("date")
            .group_by(["_y", "_m"])
            .agg([pl.col("date").last(), pl.col(col).last()])
            .sort(["_y", "_m"])
            .drop(["_y", "_m"])
            .with_columns(pl.col(col).pct_change().alias("monthly_return_pct"))
        )
        return monthly

    # --- attribution ---

    def winners_losers(
        self,
        period: Literal["ytd", "all"] = "ytd",
        top_n: int = 10,
        by: Literal["pct", "dollar"] = "dollar",
    ) -> pl.DataFrame:
        """Top winners + bottom losers across current holdings.

        Returns ticker, market_value, unrealized_pnl, unrealized_pct sorted by
        descending dollar (or pct) gain. Tail rows are losers.
        """
        cv = self.current_value()
        if cv.is_empty():
            return pl.DataFrame()
        sort_col = "unrealized_pnl" if by == "dollar" else "unrealized_pct"
        ranked = cv.filter(pl.col(sort_col).is_not_null()).sort(sort_col, descending=True)
        winners = ranked.head(top_n).with_columns(pl.lit("winner").alias("category"))
        losers = ranked.tail(top_n).with_columns(pl.lit("loser").alias("category"))
        return pl.concat([winners, losers]).select(
            ["category", "ticker", "market_value", "cost_basis", "unrealized_pnl", "unrealized_pct"]
        )

    def winners(self, top_n: int = 10, by: Literal["pct", "dollar"] = "dollar") -> pl.DataFrame:
        return self.winners_losers(top_n=top_n, by=by).filter(pl.col("category") == "winner")

    def losers(self, top_n: int = 10, by: Literal["pct", "dollar"] = "dollar") -> pl.DataFrame:
        return self.winners_losers(top_n=top_n, by=by).filter(pl.col("category") == "loser")

    # --- concentration ---

    def holdings_summary(self) -> pl.DataFrame:
        """Concentration by asset class and currency in the latest snapshot."""
        cv = self.current_value()
        if cv.is_empty():
            return pl.DataFrame()
        pos = self.portfolio.get_positions_latest()
        # Join asset_class / currency from positions onto the value table
        join_cols = [c for c in ("asset_class", "currency") if c in pos.columns]
        if join_cols:
            cv = cv.join(pos.select(["ticker", *join_cols]), on="ticker", how="left")
        total_mv = cv["market_value"].drop_nulls().sum() or 0
        cv = cv.with_columns(
            (pl.col("market_value") / total_mv if total_mv else pl.lit(None)).alias("weight_pct")
        )
        return cv.sort("market_value", descending=True, nulls_last=True)


def _next_month_start(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def _months_ago(d: date, n: int) -> date:
    y = d.year + (d.month - 1 - n) // 12
    m = (d.month - 1 - n) % 12 + 1
    return date(y, m, 1)
