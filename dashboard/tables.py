"""Turn API candles into a display table with colour-coded changes.

The API sends prices as exact decimal strings. For DISPLAY they become floats
here; nothing in the dashboard does money arithmetic beyond a percentage shown
to two decimals. Anything that must stay exact should keep using the strings.
"""

from __future__ import annotations

import pandas as pd

UP, DOWN, FLAT = "#26a69a", "#ef5350", "#8b93a1"


def fmt_price(v: float) -> str:
    return f"{v:,.2f}" if abs(v) >= 1 else f"{v:,.6f}"


def fmt_qty(v: float) -> str:
    return f"{v:,.4f}".rstrip("0").rstrip(".")


def fmt_change(v: float) -> str:
    # Cents precision normally; extra decimals only for moves too small to show in 2.
    return f"{v:+,.2f}" if v == 0 or abs(v) >= 0.005 else f"{v:+,.6f}"


def overview_frame(latest: list[dict]) -> pd.DataFrame:
    """One row per symbol: that symbol's newest hourly candle and its change within the hour."""
    rows = []
    for c in latest:
        o, h, lo, cl, vol = (float(c[k]) for k in ("open", "high", "low", "close", "volume"))
        rows.append({
            "Symbol": c["symbol"], "Source": c["source"], "Class": c["asset_class"],
            "Candle (UTC)": pd.to_datetime(c["open_time"], utc=True),
            "Open": o, "High": h, "Low": lo, "Close": cl,
            "Change": cl - o, "Change %": (cl - o) / o * 100 if o else 0.0,
            "Volume": vol, "Trades": c["trade_count"],
        })
    df = pd.DataFrame(rows, columns=["Symbol", "Source", "Class", "Candle (UTC)", "Open", "High", "Low",
                                     "Close", "Change", "Change %", "Volume", "Trades"])
    return df.sort_values(["Class", "Symbol", "Source"]).reset_index(drop=True)


def change_color(v: float) -> str:
    colour = UP if v > 0 else DOWN if v < 0 else FLAT
    return f"color: {colour}; font-weight: 600"


def style_overview(df: pd.DataFrame):
    return (
        df.style
        .map(change_color, subset=["Change", "Change %"])
        .format({
            "Candle (UTC)": lambda t: t.strftime("%Y-%m-%d %H:%M"),
            "Open": fmt_price, "High": fmt_price, "Low": fmt_price, "Close": fmt_price,
            "Change": fmt_change, "Change %": "{:+.2f}%",
            "Volume": fmt_qty, "Trades": "{:,}",
        })
    )
