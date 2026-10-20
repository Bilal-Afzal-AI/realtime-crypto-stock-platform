"""Market dashboard entry point.

Run (from this folder, so Streamlit finds .streamlit/config.toml):

    cd dashboard
    streamlit run app.py

The API must be running at API_BASE_URL (default http://localhost:8000).

This file only decides WHICH pages exist. Signed out, the only page is the
login screen; signed in, the two data pages appear. Because the data pages
aren't registered at all until login, there is nothing to reach by typing a URL.
"""

import streamlit as st

import session

st.set_page_config(page_title="Market Dashboard", page_icon=":material/candlestick_chart:", layout="wide")

session.check_expiry()

if session.is_signed_in():
    pages = [
        st.Page("views/overview.py", title="Market overview", icon=":material/monitoring:", default=True),
        st.Page("views/symbol_detail.py", title="Symbol detail", icon=":material/candlestick_chart:",
                url_path="symbol"),
    ]
    with st.sidebar:
        st.caption(f"Signed in as **{session.username()}**")
        st.button("Sign out", on_click=session.sign_out, icon=":material/logout:")
else:
    pages = [st.Page("views/login.py", title="Sign in", icon=":material/login:")]

st.navigation(pages).run()
