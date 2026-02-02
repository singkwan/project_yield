"""Chart builder for financial data visualization using Plotly."""

from datetime import date

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import polars as pl

from project_yield.analysis.metrics import MetricsEngine
from project_yield.config import Settings, get_settings
from project_yield.data.reader import DataReader


class ChartBuilder:
    """Builds interactive charts for financial data using Plotly."""

    def __init__(self, settings: Settings | None = None) -> None:
        """Initialize chart builder with settings."""
        self.settings = settings or get_settings()
        self.reader = DataReader(self.settings)
        self.metrics = MetricsEngine(self.settings)

        # Default chart styling
        self.default_template = "plotly_white"
        self.colors = px.colors.qualitative.Set2

    def price_chart(
        self,
        ticker: str,
        start_date: date | None = None,
        end_date: date | None = None,
        include_volume: bool = True,
    ) -> go.Figure:
        """Create a price chart with optional volume.

        Args:
            ticker: Stock ticker symbol
            start_date: Start date for chart
            end_date: End date for chart
            include_volume: Whether to include volume subplot

        Returns:
            Plotly Figure object
        """
        df = self.reader.get_prices(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
        ).collect()

        if df.is_empty():
            return self._empty_chart(f"No price data for {ticker}")

        df = df.sort("date")

        if include_volume:
            fig = make_subplots(
                rows=2, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.03,
                row_heights=[0.7, 0.3],
            )

            # Candlestick chart
            fig.add_trace(
                go.Candlestick(
                    x=df["date"].to_list(),
                    open=df["open"].to_list(),
                    high=df["high"].to_list(),
                    low=df["low"].to_list(),
                    close=df["close"].to_list(),
                    name="Price",
                ),
                row=1, col=1,
            )

            # Volume bars
            colors = ["red" if c < o else "green"
                     for c, o in zip(df["close"].to_list(), df["open"].to_list())]
            fig.add_trace(
                go.Bar(
                    x=df["date"].to_list(),
                    y=df["volume"].to_list(),
                    marker_color=colors,
                    name="Volume",
                ),
                row=2, col=1,
            )

            fig.update_layout(
                title=f"{ticker} Price Chart",
                xaxis_rangeslider_visible=False,
                template=self.default_template,
                height=600,
            )
        else:
            fig = go.Figure(
                go.Candlestick(
                    x=df["date"].to_list(),
                    open=df["open"].to_list(),
                    high=df["high"].to_list(),
                    low=df["low"].to_list(),
                    close=df["close"].to_list(),
                    name="Price",
                )
            )
            fig.update_layout(
                title=f"{ticker} Price Chart",
                xaxis_rangeslider_visible=False,
                template=self.default_template,
            )

        return fig

    def price_line(
        self,
        tickers: list[str],
        start_date: date | None = None,
        end_date: date | None = None,
        normalize: bool = False,
    ) -> go.Figure:
        """Create a line chart comparing multiple tickers.

        Args:
            tickers: List of ticker symbols
            start_date: Start date for chart
            end_date: End date for chart
            normalize: If True, normalize prices to start at 100

        Returns:
            Plotly Figure object
        """
        fig = go.Figure()

        for i, ticker in enumerate(tickers):
            df = self.reader.get_prices(
                ticker=ticker,
                start_date=start_date,
                end_date=end_date,
                columns=["close"],
            ).collect()

            if df.is_empty():
                continue

            df = df.sort("date")
            prices = df["close"].to_list()
            dates = df["date"].to_list()

            if normalize and prices:
                base = prices[0]
                prices = [p / base * 100 for p in prices]

            fig.add_trace(
                go.Scatter(
                    x=dates,
                    y=prices,
                    name=ticker,
                    line=dict(color=self.colors[i % len(self.colors)]),
                )
            )

        title = "Price Comparison"
        if normalize:
            title += " (Normalized to 100)"

        fig.update_layout(
            title=title,
            xaxis_title="Date",
            yaxis_title="Price" if not normalize else "Indexed Price",
            template=self.default_template,
            hovermode="x unified",
        )

        return fig

    def ratio_comparison(
        self,
        tickers: list[str],
        metrics: list[str] | None = None,
    ) -> go.Figure:
        """Create a bar chart comparing ratios across tickers.

        Args:
            tickers: List of ticker symbols
            metrics: List of metrics to compare (None for default set)

        Returns:
            Plotly Figure object
        """
        if metrics is None:
            metrics = ["pe_ratio", "operating_margin", "net_profit_margin", "gross_margin"]

        df = self.metrics.compare_tickers(tickers, metrics)

        if df.is_empty():
            return self._empty_chart("No data available")

        fig = go.Figure()

        for i, metric in enumerate(metrics):
            if metric not in df.columns:
                continue

            fig.add_trace(
                go.Bar(
                    name=self._format_metric_name(metric),
                    x=df["ticker"].to_list(),
                    y=df[metric].to_list(),
                    marker_color=self.colors[i % len(self.colors)],
                )
            )

        fig.update_layout(
            title="Financial Ratio Comparison",
            barmode="group",
            xaxis_title="Ticker",
            yaxis_title="Value",
            template=self.default_template,
        )

        return fig

    def margin_chart(
        self,
        ticker: str,
    ) -> go.Figure:
        """Create a chart showing margin trends over time.

        Args:
            ticker: Stock ticker symbol

        Returns:
            Plotly Figure object
        """
        df = self.reader.get_fundamentals_quarterly(ticker=ticker).collect()

        if df.is_empty():
            return self._empty_chart(f"No fundamental data for {ticker}")

        df = df.sort("fiscal_period")

        # Calculate margins
        if "gross_profit" in df.columns and "revenue" in df.columns:
            df = df.with_columns(
                (pl.col("gross_profit") / pl.col("revenue")).alias("gross_margin")
            )
        if "operating_income" in df.columns and "revenue" in df.columns:
            df = df.with_columns(
                (pl.col("operating_income") / pl.col("revenue")).alias("operating_margin")
            )
        if "net_income" in df.columns and "revenue" in df.columns:
            df = df.with_columns(
                (pl.col("net_income") / pl.col("revenue")).alias("net_margin")
            )

        fig = go.Figure()

        margin_cols = [
            ("gross_margin", "Gross Margin"),
            ("operating_margin", "Operating Margin"),
            ("net_margin", "Net Margin"),
        ]

        for i, (col, name) in enumerate(margin_cols):
            if col in df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=df["fiscal_period"].to_list(),
                        y=df[col].to_list(),
                        name=name,
                        line=dict(color=self.colors[i]),
                    )
                )

        fig.update_layout(
            title=f"{ticker} Margin Trends",
            xaxis_title="Quarter",
            yaxis_title="Margin",
            yaxis_tickformat=".1%",
            template=self.default_template,
            hovermode="x unified",
        )

        return fig

    def revenue_chart(
        self,
        ticker: str,
    ) -> go.Figure:
        """Create a chart showing revenue and earnings over time.

        Args:
            ticker: Stock ticker symbol

        Returns:
            Plotly Figure object
        """
        df = self.reader.get_fundamentals_quarterly(ticker=ticker).collect()

        if df.is_empty():
            return self._empty_chart(f"No fundamental data for {ticker}")

        df = df.sort("fiscal_period")

        fig = make_subplots(specs=[[{"secondary_y": True}]])

        # Revenue bars
        if "revenue" in df.columns:
            fig.add_trace(
                go.Bar(
                    x=df["fiscal_period"].to_list(),
                    y=df["revenue"].to_list(),
                    name="Revenue",
                    marker_color=self.colors[0],
                ),
                secondary_y=False,
            )

        # Net income line
        if "net_income" in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df["fiscal_period"].to_list(),
                    y=df["net_income"].to_list(),
                    name="Net Income",
                    line=dict(color=self.colors[1], width=3),
                ),
                secondary_y=True,
            )

        fig.update_layout(
            title=f"{ticker} Revenue & Earnings",
            template=self.default_template,
            hovermode="x unified",
        )
        fig.update_yaxes(title_text="Revenue", secondary_y=False)
        fig.update_yaxes(title_text="Net Income", secondary_y=True)

        return fig

    def ranking_chart(
        self,
        metric: str,
        tickers: list[str] | None = None,
        top_n: int = 10,
        ascending: bool = True,
    ) -> go.Figure:
        """Create a horizontal bar chart ranking stocks by a metric.

        Args:
            metric: Metric to rank by
            tickers: List of tickers (None for all)
            top_n: Number of stocks to show
            ascending: Sort ascending

        Returns:
            Plotly Figure object
        """
        df = self.metrics.rank_by_metric(metric, tickers, ascending, top_n)

        if df.is_empty():
            return self._empty_chart(f"No data for {metric}")

        # Reverse for horizontal bar chart
        df = df.reverse()

        fig = go.Figure(
            go.Bar(
                x=df[metric].to_list(),
                y=df["ticker"].to_list(),
                orientation="h",
                marker_color=self.colors[0],
                text=[f"{v:.2f}" if v else "" for v in df[metric].to_list()],
                textposition="outside",
            )
        )

        fig.update_layout(
            title=f"Top {top_n} by {self._format_metric_name(metric)}",
            xaxis_title=self._format_metric_name(metric),
            yaxis_title="Ticker",
            template=self.default_template,
        )

        return fig

    def sector_radar(
        self,
        ticker: str,
        benchmark_tickers: list[str],
    ) -> go.Figure:
        """Create a radar chart comparing a stock to sector averages.

        Args:
            ticker: Stock ticker symbol
            benchmark_tickers: List of tickers for sector average

        Returns:
            Plotly Figure object
        """
        from project_yield.analysis.ratios import RatioCalculator

        calc = RatioCalculator(self.settings)
        stock_ratios = calc.get_all_ratios(ticker)
        sector_avg = self.metrics.get_sector_averages(benchmark_tickers)

        metrics = ["operating_margin", "net_profit_margin", "gross_margin", "rd_intensity", "capex_ratio"]

        stock_values = []
        sector_values = []
        labels = []

        for m in metrics:
            if m in stock_ratios and stock_ratios[m] is not None:
                stock_values.append(stock_ratios[m])
                sector_values.append(sector_avg.get(m, 0))
                labels.append(self._format_metric_name(m))

        if not labels:
            return self._empty_chart("Insufficient data for radar chart")

        fig = go.Figure()

        fig.add_trace(
            go.Scatterpolar(
                r=stock_values,
                theta=labels,
                fill="toself",
                name=ticker,
            )
        )

        fig.add_trace(
            go.Scatterpolar(
                r=sector_values,
                theta=labels,
                fill="toself",
                name="Sector Average",
            )
        )

        fig.update_layout(
            title=f"{ticker} vs Sector",
            polar=dict(radialaxis=dict(visible=True)),
            template=self.default_template,
        )

        return fig

    def _empty_chart(self, message: str) -> go.Figure:
        """Create an empty chart with a message."""
        fig = go.Figure()
        fig.add_annotation(
            text=message,
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16),
        )
        fig.update_layout(template=self.default_template)
        return fig

    def _format_metric_name(self, metric: str) -> str:
        """Format metric name for display."""
        return metric.replace("_", " ").title()
