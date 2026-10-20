"""Page 2: the newest hourly candle for every symbol, refreshed every 30 seconds."""

from datetime import UTC, datetime

import streamlit as st

import api_client
import session
import tables

REFRESH_SECONDS = 30

st.title(":material/monitoring: Market overview")


# A fragment is a piece of the page that Streamlit can re-run ON ITS OWN. With
# run_every=30, Streamlit's front end sets a timer and asks the server to re-run
# just this function every 30 seconds. The rest of the page is not re-run, widgets
# elsewhere keep their state, and the browser never reloads. There is no sleep()
# loop, so nothing blocks: between refreshes the server does no work for this
# page, and the UI stays fully responsive.
@st.fragment(run_every=REFRESH_SECONDS)
def live_table() -> None:
    try:
        latest = api_client.latest_candles(session.token())
    except api_client.ApiError as exc:
        session.handle_api_error(exc)   # expired token -> back to login; otherwise a message

    if not latest:
        st.info("No candles in the warehouse yet. Run the pipeline and check back.")
        return

    df = tables.overview_frame(latest)
    st.dataframe(tables.style_overview(df), hide_index=True, use_container_width=True,
                 height=min(80 + 36 * len(df), 520))

    newest = df["Candle (UTC)"].max()
    age_h = (datetime.now(UTC) - newest.to_pydatetime()).total_seconds() / 3600
    left, right = st.columns([3, 1])
    left.caption(
        f":material/sync: Refreshed {datetime.now(UTC):%H:%M:%S} UTC, then every {REFRESH_SECONDS}s. "
        f"Newest candle in the warehouse starts at {newest:%Y-%m-%d %H:%M} UTC ({age_h:.1f}h ago). "
        "Candles are hourly, so values change when the pipeline loads a new hour. "
        "*Change* compares the candle's close with its open."
    )
    right.button("Refresh now", icon=":material/refresh:")   # a click re-runs just this fragment


live_table()
