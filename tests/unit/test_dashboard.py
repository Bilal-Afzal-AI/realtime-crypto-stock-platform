"""Dashboard logic that doesn't need a browser: API error mapping, table colours, chart traces."""

import sys
from pathlib import Path

import httpx
import pandas as pd
import pytest

# The dashboard is run as scripts from its own folder, so its modules import each other by plain name.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "dashboard"))

import api_client  # noqa: E402
import charts  # noqa: E402
import tables  # noqa: E402


def api(handler):
    api_client._transport = httpx.MockTransport(handler)


@pytest.fixture(autouse=True)
def _reset_transport():
    yield
    api_client._transport = None


def test_expired_token_raises_session_expired():
    api(lambda r: httpx.Response(401, json={"detail": "Invalid or expired token"}))
    with pytest.raises(api_client.SessionExpired):
        api_client.latest_candles("tok")


def test_wrong_password_is_a_plain_error_not_session_expired():
    api(lambda r: httpx.Response(401, json={"detail": "Incorrect username or password"}))
    with pytest.raises(api_client.ApiError, match="Incorrect username or password") as exc:
        api_client.login("alice", "x")
    assert not isinstance(exc.value, api_client.SessionExpired)


def test_rate_limit_carries_retry_after():
    api(lambda r: httpx.Response(429, headers={"retry-after": "42"}, json={"detail": "slow down"}))
    with pytest.raises(api_client.RateLimited) as exc:
        api_client.symbols("tok")
    assert exc.value.retry_after == 42


def test_unreachable_api_and_server_errors_become_friendly_errors():
    def boom(request):
        raise httpx.ConnectError("refused")
    api(boom)
    with pytest.raises(api_client.ApiUnavailable, match="Is it running"):
        api_client.symbols("tok")
    def slow(request):
        raise httpx.ReadTimeout("too slow")
    api(slow)
    with pytest.raises(api_client.ApiUnavailable, match="may be busy"):
        api_client.symbols("tok")
    api(lambda r: httpx.Response(500, text="Traceback..."))
    with pytest.raises(api_client.ApiError, match="internal error"):
        api_client.symbols("tok")


def test_token_is_sent_as_bearer_and_candle_params_are_passed():
    seen = {}

    def handler(request):
        seen["auth"] = request.headers["authorization"]
        seen["params"] = dict(request.url.params)
        return httpx.Response(200, json={"symbol": "BTCUSDT", "count": 0, "candles": []})
    api(handler)
    assert api_client.candles("abc", "BTCUSDT", source="binance", limit=50, order="asc") == []
    assert seen["auth"] == "Bearer abc"
    assert seen["params"] == {"limit": "50", "order": "asc", "source": "binance"}


LATEST = [
    {"source": "binance", "symbol": "BTCUSDT", "asset_class": "crypto", "open_time": "2026-10-19T19:00:00Z",
     "open": "81930.01000000", "high": "81930.01000000", "low": "81864.40000000", "close": "81864.40000000",
     "volume": "3.42696000", "trade_count": 1399},
    {"source": "finnhub", "symbol": "AAPL", "asset_class": "equity", "open_time": "2026-10-19T19:00:00Z",
     "open": "343.00000000", "high": "343.15000000", "low": "343.00000000", "close": "343.10000000",
     "volume": "2570.00000000", "trade_count": 35},
]


def test_overview_changes_and_colours():
    df = tables.overview_frame(LATEST)
    btc, aapl = df.set_index("Symbol").loc["BTCUSDT"], df.set_index("Symbol").loc["AAPL"]
    assert btc["Change"] == pytest.approx(-65.61) and btc["Change %"] < 0
    assert aapl["Change"] == pytest.approx(0.10) and aapl["Change %"] > 0
    assert tables.UP in tables.change_color(aapl["Change"])
    assert tables.DOWN in tables.change_color(btc["Change"])
    assert tables.FLAT in tables.change_color(0.0)
    html = tables.style_overview(df).to_html()
    assert "81,864.40" in html and "-65.61" in html and "+0.03%" in html


def test_formatting_helpers():
    assert tables.fmt_price(82714.76) == "82,714.76"
    assert tables.fmt_price(0.000123) == "0.000123"
    assert tables.fmt_qty(2570.0) == "2,570"
    assert tables.fmt_qty(3.42696) == "3.427"


def test_chart_has_candles_and_matching_volume_bars():
    btc = LATEST[0]
    df = charts.candles_frame([btc, {**btc, "open_time": "2026-10-19T05:00:00Z"},
                               {**btc, "open_time": "2026-10-19T07:00:00Z", "open": "100", "close": "110"}])
    assert list(df["open_time"]) == sorted(df["open_time"])      # oldest first whatever the API order
    fig = charts.candlestick_with_volume(df, "t")
    candle, bars = fig.data
    assert type(candle).__name__ == "Candlestick" and type(bars).__name__ == "Bar"
    # 04:00..20:00 is 17 hourly slots (one empty slot either side); only 3 hold a candle
    assert len(candle.x) == len(bars.x) == 17
    assert pd.Series(candle.close).notna().sum() == 3
    # colour follows direction: 07:00 closed above its open, 05:00 and 19:00 below
    colors = {pd.Timestamp(t).hour: c for t, c in zip(bars.x, bars.marker.color)}
    assert colors[7] == tables.UP and colors[5] == tables.DOWN and colors[19] == tables.DOWN


def test_single_candle_chart_is_padded_so_it_is_not_a_slab():
    df = charts.candles_frame([LATEST[1]])                       # AAPL, one hour of data
    fig = charts.candlestick_with_volume(df, "t")
    lo, hi = fig.layout.xaxis.range
    assert (pd.Timestamp(hi) - pd.Timestamp(lo)) >= pd.Timedelta(hours=12)


def test_gap_filling_is_skipped_for_huge_ranges():
    rows = [{**LATEST[0], "open_time": "2020-01-01T00:00:00Z"}, {**LATEST[0], "open_time": "2026-01-01T00:00:00Z"}]
    df = charts.candles_frame(rows)
    assert len(charts.with_hourly_gaps(df)) == 2
