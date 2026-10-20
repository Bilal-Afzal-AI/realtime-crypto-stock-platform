"""Login state, kept in Streamlit's session state.

`st.session_state` is a dictionary that lives in the SERVER's memory, one per
browser tab/connection. The token is stored there rather than in a cookie:

- It never goes back to the browser. A cookie is sent with every request and can
  be read by any script on the page (unless HttpOnly) or by browser extensions,
  and cookies are what cross-site request forgery attacks ride on. A token that
  stays on the server can't be stolen from the browser, because it isn't there.
- Streamlit has no built-in cookie API for setting secure, HttpOnly cookies
  from Python; the usual workarounds add a third-party component that runs JS
  in the page, exactly the exposure we want to avoid.
- The trade-off: a session lives only as long as the browser tab's connection.
  Refresh the page (F5) or open a new tab and you sign in again. For a dashboard
  that's acceptable, and it doubles as an automatic "log out when I close the tab".
"""

from __future__ import annotations

import time

import streamlit as st

_KEYS = ("token", "username", "expires_at")
EXPIRY_MARGIN_S = 5  # treat the token as expired slightly early rather than send a doomed request


def sign_in(token: str, expires_in: int, username: str) -> None:
    st.session_state.update(token=token, username=username,
                            expires_at=time.time() + expires_in - EXPIRY_MARGIN_S)


def sign_out(message: str | None = None) -> None:
    for key in _KEYS:
        st.session_state.pop(key, None)
    st.cache_data.clear()                       # never keep one user's cached data for the next
    if message:
        st.session_state["flash"] = message     # shown once, on the login page


def is_signed_in() -> bool:
    return bool(st.session_state.get("token")) and time.time() < st.session_state.get("expires_at", 0)


def check_expiry() -> None:
    """Called on every run. A token past its expiry sends the user back to login."""
    if st.session_state.get("token") and not is_signed_in():
        sign_out("Your session expired. Please sign in again.")


def token() -> str:
    return st.session_state["token"]


def username() -> str:
    return st.session_state.get("username", "")


def handle_api_error(exc: Exception) -> None:
    """Turn an API failure into the right on-screen behaviour. Never returns normally."""
    import api_client

    if isinstance(exc, api_client.SessionExpired):
        sign_out("Your session expired. Please sign in again.")
        st.rerun(scope="app")                   # app.py now shows the login page
    elif isinstance(exc, api_client.RateLimited):
        st.warning(f"You're making requests too quickly. Waiting {exc.retry_after}s before the next refresh.",
                   icon=":material/hourglass_top:")
    elif isinstance(exc, api_client.ApiUnavailable):
        st.error(str(exc), icon=":material/cloud_off:")
    else:
        st.error(str(exc), icon=":material/error:")
    st.stop()
