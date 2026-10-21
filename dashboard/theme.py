"""Branding and card styling shared by every page.

Streamlit's theme (.streamlit/config.toml) sets colours, but it has no knobs for
a branded header or for giving `st.metric` a card look. The rest is a small
block of CSS injected once per run from app.py. Since app.py runs before every
page, the styles apply everywhere and no page has to remember to add them.

The CSS targets Streamlit's `data-testid` attributes. Those are the most stable
hooks Streamlit exposes, but they are not a public API. If an upgrade renames
one, the dashboard still works and only loses that bit of styling.
"""

from __future__ import annotations

import streamlit as st

PROJECT_NAME = "Real-Time Crypto & Stock Data Platform"
TAGLINE = "Live crypto and US stock trades, streamed through Kafka and served as hourly candles."

# Keep in sync with primaryColor in .streamlit/config.toml. Violet sits apart from
# the teal/red used for up/down candles, so the accent never reads as "price moved".
ACCENT = "#7c5cff"
ACCENT_2 = "#22c3e6"

# A small candlestick drawn inline, so the header needs no image file or network request.
_LOGO_SVG = """
<svg viewBox="0 0 32 32" width="26" height="26" aria-hidden="true">
  <line x1="9" y1="5" x2="9" y2="27" stroke="#fff" stroke-width="1.6" stroke-linecap="round"/>
  <rect x="5.5" y="10" width="7" height="11" rx="1.5" fill="#fff"/>
  <line x1="22" y1="3" x2="22" y2="23" stroke="#fff" stroke-width="1.6" stroke-linecap="round" opacity=".85"/>
  <rect x="18.5" y="7" width="7" height="9" rx="1.5" fill="none" stroke="#fff" stroke-width="1.6" opacity=".85"/>
</svg>
"""

_CSS = f"""
<style>
:root {{
  --brand-accent: {ACCENT};
  --brand-accent-2: {ACCENT_2};
  --card-bg: #141922;
  --card-border: rgba(139, 147, 161, 0.18);
  --muted: #8b93a1;
}}

/* ---- Brand header ---- */
.brand-header {{
  display: flex; align-items: center; gap: 0.9rem;
  padding: 0.9rem 1.1rem; margin: 0 0 1.25rem 0;
  border: 1px solid var(--card-border); border-radius: 14px;
  background:
    radial-gradient(120% 140% at 0% 0%, rgba(124, 92, 255, 0.22), transparent 55%),
    radial-gradient(120% 140% at 100% 100%, rgba(34, 195, 230, 0.12), transparent 55%),
    var(--card-bg);
}}
.brand-logo {{
  flex: 0 0 auto; display: grid; place-items: center;
  width: 46px; height: 46px; border-radius: 12px;
  background: linear-gradient(135deg, var(--brand-accent), var(--brand-accent-2));
  box-shadow: 0 6px 18px rgba(124, 92, 255, 0.35);
}}
.brand-text {{ min-width: 0; }}
.brand-name {{
  font-size: 1.35rem; font-weight: 700; line-height: 1.2; letter-spacing: -0.01em;
  background: linear-gradient(90deg, #ffffff, #c9c2ff);
  -webkit-background-clip: text; background-clip: text; color: transparent;
}}
.brand-tagline {{ color: var(--muted); font-size: 0.9rem; margin-top: 0.15rem; }}
.brand-badges {{ margin-left: auto; display: flex; gap: 0.4rem; flex-wrap: wrap; justify-content: flex-end; }}
.brand-badge {{
  font-size: 0.72rem; font-weight: 600; letter-spacing: 0.04em; text-transform: uppercase;
  padding: 0.22rem 0.55rem; border-radius: 999px; white-space: nowrap;
  color: #d9d3ff; background: rgba(124, 92, 255, 0.16); border: 1px solid rgba(124, 92, 255, 0.35);
}}
.brand-header.compact {{ flex-direction: column; text-align: center; padding: 1.4rem 1.1rem; }}
.brand-header.compact .brand-badges {{ margin-left: 0; justify-content: center; }}
@media (max-width: 640px) {{
  .brand-badges {{ display: none; }}
  .brand-name {{ font-size: 1.1rem; }}
}}

/* ---- Metric cards: st.metric styled as a card with an accent edge ---- */
[data-testid="stMetric"] {{
  background: linear-gradient(180deg, rgba(124, 92, 255, 0.07), rgba(124, 92, 255, 0) 60%), var(--card-bg);
  border: 1px solid var(--card-border);
  border-left: 3px solid var(--brand-accent);
  border-radius: 12px;
  padding: 0.85rem 1rem;
  width: 100%;          /* Streamlit sizes st.metric to its content; a card should fill its column */
  transition: border-color 0.15s ease, transform 0.15s ease;
}}
/* Side by side, cards share a row height whether or not they have a delta line.
   Below 640px Streamlit stacks columns, so equal heights would only add empty space. */
@media (min-width: 641px) {{ [data-testid="stMetric"] {{ min-height: 7.4rem; }} }}
[data-testid="stMetric"]:hover {{ border-color: rgba(124, 92, 255, 0.55); transform: translateY(-1px); }}
[data-testid="stMetricLabel"] p {{
  color: var(--muted); font-size: 0.78rem; font-weight: 600;
  letter-spacing: 0.05em; text-transform: uppercase;
}}
[data-testid="stMetricValue"] {{ font-size: 1.55rem; font-weight: 650; font-variant-numeric: tabular-nums; }}
[data-testid="stMetricDelta"] {{ font-size: 0.82rem; }}

/* Page titles sit under the brand header, so make them section headings rather than a second hero. */
[data-testid="stMainBlockContainer"] h1 {{ font-size: 1.6rem; padding-top: 0; }}
</style>
"""


def inject_css() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)


def brand_header(compact: bool = False) -> None:
    """The project's header. `compact` stacks it vertically for the narrow sign-in column."""
    badges = "".join(f'<span class="brand-badge">{b}</span>' for b in ("Crypto", "US stocks", "1h OHLCV"))
    html = f"""
        <div class="brand-header{' compact' if compact else ''}">
          <div class="brand-logo">{_LOGO_SVG}</div>
          <div class="brand-text">
            <div class="brand-name">{PROJECT_NAME}</div>
            <div class="brand-tagline">{TAGLINE}</div>
          </div>
          <div class="brand-badges">{badges}</div>
        </div>
    """
    # Flatten to one line: Markdown treats lines indented 4+ spaces as a code block,
    # which would print the HTML as text instead of rendering it.
    st.markdown("".join(line.strip() for line in html.splitlines()), unsafe_allow_html=True)
