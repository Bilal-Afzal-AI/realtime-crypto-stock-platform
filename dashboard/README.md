# Dashboard

Streamlit app for the platform's hourly candles. It talks only to the backend API
(`backend/`), never to a database.

## Run it

The stack from `infra/` and the API must be up:

```bash
# terminal 1, repo root
uvicorn backend.app.main:app --port 8000

# terminal 2
cd dashboard            # so Streamlit finds .streamlit/config.toml
streamlit run app.py    # http://127.0.0.1:8501
```

`API_BASE_URL` overrides where the API is (default `http://127.0.0.1:8000`). The default
uses `127.0.0.1` rather than `localhost` because on Windows `localhost` tries IPv6 first
and waiting for that to fail slowed every request by a few seconds.

## Pages

| Page | What |
|---|---|
| Sign in | Log in or create an account. The JWT is kept in `st.session_state` |
| Market overview | Newest hourly candle for every symbol, green/red change, refreshes itself every 30 s |
| Symbol detail | Pick a symbol and a range; candlestick chart with a volume bar chart below it |

Signed out, the only page that exists is Sign in; the data pages are registered only after login.

## How it fits together

- `app.py` chooses which pages exist; `views/` holds the pages.
- `api_client.py` is the only code that makes HTTP calls. It turns every failure into one of
  `SessionExpired`, `RateLimited`, `ApiUnavailable`, `ApiError`. `session.handle_api_error`
  shows each one properly; an expired token signs the user out and returns to the login page
  with a notice, never a raw error.
- The 30 s refresh is a `@st.fragment(run_every=30)`: only that block re-runs, on a browser-side
  timer, so the page stays responsive. See the comments in `views/overview.py`.
- The token lives in server-side session state, not a cookie; see `session.py` for why.
- Streamlit listens on 127.0.0.1 only (`.streamlit/config.toml`). Its default is every network
  interface, which would serve the dashboard over plain HTTP to your whole LAN.

## Notes

- Candles are hourly, so the numbers change when the pipeline loads a new hour, not every 30 s.
  *Change* on the overview is the latest candle's close minus its open.
- Missing hours show as gaps in the chart (markets closed, pipeline not run) rather than
  being squeezed together.
- Prices arrive from the API as exact decimal strings and become floats only for display.
- Screenshots of each page are in `docs/screenshots/`.
