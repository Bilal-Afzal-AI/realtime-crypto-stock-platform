"""Page 3: pick a symbol and a time range; candlestick chart with volume bars below."""

from datetime import UTC, datetime, timedelta

import streamlit as st

import api_client
import charts
import session
import tables

st.title(":material/candlestick_chart: Symbol detail")

PRESETS = {
    "Last 24 hours": timedelta(hours=24),
    "Last 7 days": timedelta(days=7),
    "Last 30 days": timedelta(days=30),
    "All available": None,
    "Custom": "custom",
}


@st.cache_data(ttl=300, show_spinner=False)
def load_symbols(token: str) -> list[dict]:
    # `token` is part of the cache key, so one user's cache entry is never served to another.
    return api_client.symbols(token)


try:
    symbols = load_symbols(session.token())
except api_client.ApiError as exc:
    session.handle_api_error(exc)

if not symbols:
    st.info("No symbols are tracked yet.")
    st.stop()

symbols = [s for s in symbols if s["is_active"]]
labels = {f"{s['symbol']}  ·  {s['source']}  ({s['asset_class']})": s for s in symbols}
default = next((i for i, s in enumerate(labels.values()) if s["symbol"] == "BTCUSDT"), 0)

col_symbol, col_range = st.columns([2, 3])
choice = col_symbol.selectbox("Symbol", list(labels), index=default)
preset = col_range.radio("Time range", list(PRESETS), index=3, horizontal=True)

now = datetime.now(UTC)
start = end = None
if PRESETS[preset] == "custom":
    picked = st.date_input("Dates (UTC)", value=(now.date() - timedelta(days=2), now.date()),
                           max_value=now.date())
    if len(picked) == 2:
        start = datetime.combine(picked[0], datetime.min.time(), tzinfo=UTC)
        end = datetime.combine(picked[1] + timedelta(days=1), datetime.min.time(), tzinfo=UTC)  # end exclusive
    else:
        st.info("Pick an end date to finish the range.")
        st.stop()
elif PRESETS[preset] is not None:
    start = now - PRESETS[preset]

sym = labels[choice]
try:
    # Newest 1000 candles in range (the API's maximum); charts.candles_frame re-sorts oldest first.
    data = api_client.candles(session.token(), sym["symbol"], source=sym["source"],
                              start=start, end=end, limit=1000, order="desc")
except api_client.ApiError as exc:
    session.handle_api_error(exc)

if not data:
    st.info(f"No candles for {sym['symbol']} in this range. Try a wider range; stocks only trade in US market hours.")
    st.stop()

df = charts.candles_frame(data)
first, last = df.iloc[0], df.iloc[-1]
change = last["close"] - first["open"]

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Last close", tables.fmt_price(last["close"]),
          f"{change / first['open'] * 100:+.2f}% over range" if first["open"] else None)
m2.metric("Range high", tables.fmt_price(df["high"].max()))
m3.metric("Range low", tables.fmt_price(df["low"].min()))
m4.metric("Total volume", tables.fmt_qty(df["volume"].sum()))
m5.metric("Candles", f"{len(df):,}")

title = f"{sym['symbol']} · {sym['source']} · 1h candles (UTC)"
st.plotly_chart(charts.candlestick_with_volume(df, title), use_container_width=True)

with st.expander("Show candle data"):
    table = df.rename(columns={"open_time": "Candle (UTC)", "open": "Open", "high": "High", "low": "Low",
                               "close": "Close", "volume": "Volume", "trade_count": "Trades"})
    st.dataframe(table[["Candle (UTC)", "Open", "High", "Low", "Close", "Volume", "Trades"]]
                 .sort_values("Candle (UTC)", ascending=False),
                 hide_index=True, use_container_width=True)
