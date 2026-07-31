"""Cross-source ticker translation.

The pipeline uses yfinance-form tickers as the canonical key everywhere
downstream — partition names, joins, in-memory DataFrames. Translation from
other sources happens at the ingestion boundary (e.g. when reading positions
out of an IBKR export).

Add new mappers (`bloomberg_to_yfinance`, `tradingview_to_yfinance`, etc.)
here as siblings of `ibkr_to_yfinance` when needed.
"""


def ibkr_to_yfinance(ticker: str) -> str:
    """Map an IBKR-style ticker to yfinance's exchange-suffixed format.

    yfinance disambiguates same-symbol-on-different-exchange via suffix:
      .HK = Hong Kong, .KS = Korea, .SI = Singapore, .L = London,
      .AS = Amsterdam, .T = Tokyo, .SS = Shanghai, .SZ = Shenzhen.

    Heuristic mapping for the IBKR symbols we've observed:
      - already has a dot suffix (000660.KS, BABA.HK) -> use as-is
      - all-digit (Hong Kong style: 700, 1810, 3415) -> zero-pad to 4 digits + .HK
      - C6L -> C6L.SI (Singapore Airlines)
      - IWDA -> IWDA.AS (UCITS, listed on Amsterdam)
      - default: pass through (US tickers like AAPL stay AAPL)

    The all-digit→.HK rule is fragile: Korean / Tokyo tickers can also be
    bare digits in some exports. IBKR happens to ship those with the suffix
    already (`005930.KS`), but if that ever stops being true, add an explicit
    override before the heuristic fires.
    """
    if "." in ticker:
        return ticker
    if ticker.isdigit():
        return f"{int(ticker):04d}.HK"
    overrides = {"C6L": "C6L.SI", "IWDA": "IWDA.AS"}
    return overrides.get(ticker, ticker)


def is_foreign_ticker(ticker: str) -> bool:
    """True if the ticker is non-US (and thus must be routed via yfinance, not FMP).

    Operates on yfinance-form tickers: any non-US listing has an exchange
    suffix (`.HK`, `.KS`, `.SI`, `.L`, …). yfinance uses dashes (not dots)
    for US share-class tickers like `BRK-B`, so a literal dot reliably means
    non-US.
    """
    return "." in ticker
