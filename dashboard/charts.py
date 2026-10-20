"""Plotly figures. Dark, transparent backgrounds so they sit on Streamlit's theme."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from tables import DOWN, UP

HOUR_MS = 3_600_000
GRID = "rgba(139,147,161,0.15)"
MAX_FILLED_HOURS = 5_000   # don't pad gaps for very long ranges


def candles_frame(candles: list[dict]) -> pd.DataFrame:
    """API candles (any order) -> float DataFrame sorted oldest first, for plotting."""
    df = pd.DataFrame(candles)
    if df.empty:
        return df
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = df[col].astype(float)
    return df.sort_values("open_time").reset_index(drop=True)


def with_hourly_gaps(df: pd.DataFrame) -> pd.DataFrame:
    """One row per hour between the first and last candle; missing hours are empty (NaN).

    Two reasons. (1) Honest charts: a market-closed night or a pipeline outage shows
    up as a visible gap instead of two candles squeezed side by side. (2) Plotly sizes
    a candle from the closest spacing between candles, so a chart of a few far-apart
    candles would otherwise draw huge slabs; with hourly spacing every candle is the
    same slim width however sparse the data is.
    """
    # One empty slot either side as well: with a single candle there is no spacing
    # to measure at all, and Plotly would draw it hair-thin.
    one_hour = pd.Timedelta(hours=1)
    full = pd.date_range(df["open_time"].min() - one_hour, df["open_time"].max() + one_hour, freq="h")
    if len(full) > MAX_FILLED_HOURS:
        return df
    return df.set_index("open_time").reindex(full).rename_axis("open_time").reset_index()


def candlestick_with_volume(df: pd.DataFrame, title: str) -> go.Figure:
    """Candlesticks on top, volume bars below, sharing one time axis."""
    g = with_hourly_gaps(df)
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03,
                        row_heights=[0.74, 0.26])
    fig.add_trace(go.Candlestick(
        x=g["open_time"], open=g["open"], high=g["high"], low=g["low"], close=g["close"],
        increasing=dict(line=dict(color=UP), fillcolor=UP),
        decreasing=dict(line=dict(color=DOWN), fillcolor=DOWN),
        name="OHLC",
    ), row=1, col=1)

    colors = [UP if c >= o else DOWN for o, c in zip(g["open"], g["close"])]   # NaN rows: never drawn
    fig.add_trace(go.Bar(
        x=g["open_time"], y=g["volume"], name="Volume",
        marker=dict(color=colors, opacity=0.85),
        width=[HOUR_MS * 0.7] * len(g),     # same slim width as the candles
    ), row=2, col=1)

    fig.update_layout(
        title=dict(text=title, x=0.01, font=dict(size=18)),
        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        height=640, showlegend=False, hovermode="x unified",
        margin=dict(l=10, r=10, t=60, b=10),
        xaxis=dict(rangeslider=dict(visible=False)),
    )
    # Pad the time axis. Without it Plotly zooms to exactly the data, so a single
    # candle (a stock that traded in only one hour) fills the whole chart as a slab.
    first, last = df["open_time"].min(), df["open_time"].max()
    pad = max(pd.Timedelta(hours=6), (last - first) * 0.05)
    fig.update_xaxes(range=[(first - pad).to_pydatetime(), (last + pad).to_pydatetime()])
    fig.update_xaxes(gridcolor=GRID, showspikes=True, spikethickness=1)
    fig.update_yaxes(gridcolor=GRID, row=1, col=1, title_text="Price", side="right")
    fig.update_yaxes(gridcolor=GRID, row=2, col=1, title_text="Volume", side="right")
    return fig
