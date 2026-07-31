"""Canonical schemas for OpenBB-sourced financial data.

Every provider (FMP, yfinance, Polygon, …) returns the same financial concepts
under different column names. Each schema below describes ONE target shape:
the canonical column we use everywhere downstream, plus the provider-specific
source columns that should produce it.

Read each `Field` as: "this is what we call it; these are the source names we
accept, in priority order." When multiple sources are present in the same
DataFrame, the first listed wins and the rest are dropped (so we never get
duplicate columns).

Add a new provider by appending its source names to the relevant Field.sources
tuples — no client code changes needed.
"""
from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class Field:
    """One canonical column in a normalized schema.

    canonical: the column name everything downstream uses.
    sources:   provider columns that may produce it, in priority order. First
               source present in the DataFrame wins; the others are dropped.
               Empty means no provider ships this field directly — it must be
               backfilled by the caller before validate() runs (e.g.
               fiscal_year derived from report_date).
    required:  if True, validate() raises when this column is missing after
               apply() + any caller-side backfilling.
    """

    canonical: str
    sources: tuple[str, ...] = ()
    required: bool = False


def apply(df: pl.DataFrame, schema: tuple[Field, ...]) -> pl.DataFrame:
    """Rename source columns to canonical names per `schema` (first-source-wins).

    For each Field:
      - if the canonical column already exists, drop any redundant alias columns
      - else find the first source column present in df and rename it; drop
        any other listed sources also present, to avoid duplicate-name collisions
    """
    rename: dict[str, str] = {}
    drop: set[str] = set()
    present = set(df.columns)
    for fld in schema:
        if fld.canonical in present:
            drop.update(s for s in fld.sources if s in present and s != fld.canonical)
            continue
        for i, src in enumerate(fld.sources):
            if src in present:
                rename[src] = fld.canonical
                present.add(fld.canonical)
                drop.update(
                    other for other in fld.sources[i + 1:] if other in present
                )
                break
    if drop:
        df = df.drop([c for c in drop if c in df.columns])
    if rename:
        df = df.rename(rename)
    return df


def validate(df: pl.DataFrame, schema: tuple[Field, ...]) -> None:
    """Raise ValueError if any required field is missing from df."""
    missing = [fld.canonical for fld in schema if fld.required and fld.canonical not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}. "
            f"Available: {sorted(df.columns)}"
        )


# -- Schemas, one per OpenBB endpoint --------------------------------------
#
# Provider attribution lives in inline comments next to each source name so
# the origin is visible without nesting the dict.
#
# Convention: list the most authoritative provider's source name first, so
# first-source-wins picks it when multiple are present.

PRICE_SCHEMA: tuple[Field, ...] = (
    Field("ticker",         sources=("symbol",), required=True),  # universal
    Field("date",           required=True),                       # passthrough
    Field("close",          required=True),                       # passthrough
    Field("adjusted_close", sources=("adj_close",)),               # FMP/yfinance
    Field("dividend",       sources=("dividends",)),               # yfinance plural
)

INCOME_SCHEMA: tuple[Field, ...] = (
    Field("ticker",      sources=("symbol",), required=True),
    Field("report_date", sources=("period_ending",), required=True),  # FMP
    Field("fiscal_year", required=True),                              # backfilled from report_date
    Field("revenue", sources=(
        "total_revenue",      # yfinance
        "operating_revenue",  # yfinance variant
    ), required=True),
    Field("net_income", sources=(
        "bottom_line_net_income",   # FMP — post-tax, post-NCI
        "consolidated_net_income",  # FMP — pre-NCI variant
    ), required=True),
    Field("operating_income", sources=(
        "total_operating_income",              # FMP
        "total_operating_income_as_reported",  # yfinance
    )),
    Field("eps",       sources=("diluted_earnings_per_share",)),       # FMP
    Field("eps_basic", sources=("basic_earnings_per_share",)),         # FMP
    Field("rd_expense", sources=(
        "research_and_development_expense",  # FMP
        "research_and_development",          # yfinance shorter form
    )),
    Field("shares_outstanding",       sources=("weighted_average_diluted_shares_outstanding",)),
    Field("shares_outstanding_basic", sources=("weighted_average_basic_shares_outstanding",)),
)

BALANCE_SCHEMA: tuple[Field, ...] = (
    Field("ticker",       sources=("symbol",), required=True),
    Field("report_date",  sources=("period_ending",), required=True),
    Field("fiscal_year",  required=True),
    Field("total_assets", required=True),  # universally named
    Field("shareholders_equity", sources=(
        "total_common_equity",        # FMP
        "total_equity",               # yfinance
        "total_stockholders_equity",  # Polygon
    ), required=True),
)

# Cash flow is the messiest: each provider ships both a "total" and a
# "_continuing" variant, AND naming diverges (FMP `net_cash_from_*` vs
# Polygon `net_cash_flow_from_*`). Up to four source columns per concept;
# we treat continuing/total as interchangeable and take whichever appears.
CASHFLOW_SCHEMA: tuple[Field, ...] = (
    Field("ticker",      sources=("symbol",), required=True),
    Field("report_date", sources=("period_ending",), required=True),
    Field("fiscal_year", required=True),
    Field("operating_cash_flow", sources=(
        "net_cash_from_operating_activities",                    # FMP total
        "net_cash_from_continuing_operating_activities",         # FMP continuing
        "net_cash_flow_from_operating_activities",               # Polygon total
        "net_cash_flow_from_operating_activities_continuing",    # Polygon continuing
    ), required=True),
    Field("investing_cash_flow", sources=(
        "net_cash_from_investing_activities",
        "net_cash_from_continuing_investing_activities",
        "net_cash_flow_from_investing_activities",
        "net_cash_flow_from_investing_activities_continuing",
    )),
    Field("financing_cash_flow", sources=(
        "net_cash_from_financing_activities",
        "net_cash_from_continuing_financing_activities",
        "net_cash_flow_from_financing_activities",
        "net_cash_flow_from_financing_activities_continuing",
    )),
    Field("capex", sources=("capital_expenditure",)),  # FMP verbose → canonical short
)
