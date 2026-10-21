"""Page 1: sign in (or create an account). Stores the JWT in session state."""

import streamlit as st

import api_client
import session
import theme

_, centre, _ = st.columns([1, 1.2, 1])

with centre:
    theme.brand_header(compact=True)

    # One-time notice, e.g. "Your session expired".
    if flash := st.session_state.pop("flash", None):
        st.warning(flash, icon=":material/schedule:")

    sign_in_tab, register_tab = st.tabs(["Sign in", "Create account"])

    with sign_in_tab:
        with st.form("login"):
            username = st.text_input("Username", autocomplete="username")
            password = st.text_input("Password", type="password", autocomplete="current-password")
            submitted = st.form_submit_button("Sign in", type="primary", use_container_width=True)
        if submitted:
            if not username or not password:
                st.error("Enter your username and password.")
            else:
                try:
                    token = api_client.login(username, password)
                except api_client.ApiError as exc:   # wrong credentials, rate limited, API down
                    st.error(str(exc))
                else:
                    session.sign_in(token["access_token"], token["expires_in"], username.strip().lower())
                    st.rerun()                      # app.py now registers the data pages

    with register_tab:
        with st.form("register"):
            new_username = st.text_input("Choose a username", help="3-32 characters: letters, digits, underscore")
            new_password = st.text_input("Choose a password", type="password", help="At least 10 characters")
            created = st.form_submit_button("Create account", use_container_width=True)
        if created:
            try:
                api_client.register(new_username, new_password)
            except api_client.ApiError as exc:
                st.error(str(exc))
            else:
                st.success("Account created. Switch to the Sign in tab to log in.")
