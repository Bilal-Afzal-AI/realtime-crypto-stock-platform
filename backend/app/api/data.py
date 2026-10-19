"""Read-only market data, served from the WAREHOUSE database.

Every route here requires a valid token and counts against the caller's rate
limit (the `limit_data` dependency does both). None of them uses the auth
database connection.

All SQL uses bound parameters (:name), never string formatting, so user input
can't change the shape of a query. The one dynamic piece, sort direction, comes
from a fixed lookup, not from the request text.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import Connection, text

from backend.app.core.db import get_warehouse_conn
from backend.app.core.rate_limit import limit_data
from backend.app.models.schemas import Candle, CandleList, LatestCandle, SymbolOut

router = APIRouter(tags=["data"], dependencies=[Depends(limit_data)])

CANDLE_COLUMNS = "o.source, o.symbol, o.open_time, o.open, o.high, o.low, o.close, o.volume, o.trade_count"
SORT = {"asc": "ASC", "desc": "DESC"}


def _as_utc(value: datetime | None) -> datetime | None:
    """A timestamp without a zone is taken to be UTC, which is what the warehouse stores."""
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


@router.get("/symbols", response_model=list[SymbolOut], summary="Tracked symbols")
def list_symbols(conn: Connection = Depends(get_warehouse_conn)):
    rows = conn.execute(text(
        "SELECT source, symbol, asset_class, quote_currency, display_name, is_active "
        "FROM symbols ORDER BY asset_class, symbol, source"))
    return [SymbolOut(**r._mapping) for r in rows]


# Declared BEFORE /candles/{symbol}: routes match in order, and "latest" would
# otherwise be read as a symbol called "latest".
@router.get("/candles/latest", response_model=list[LatestCandle],
            summary="Most recent hourly candle for every active symbol")
def latest_candles(conn: Connection = Depends(get_warehouse_conn)):
    # DISTINCT ON keeps the first row per (source, symbol) in the ORDER BY, i.e.
    # the newest candle; the primary key index (source, symbol, open_time) serves it.
    rows = conn.execute(text(
        f"SELECT DISTINCT ON (o.source, o.symbol) {CANDLE_COLUMNS}, s.asset_class "
        "FROM ohlcv_1h o JOIN symbols s USING (source, symbol) "
        "WHERE s.is_active "
        "ORDER BY o.source, o.symbol, o.open_time DESC"))
    return [LatestCandle(**r._mapping) for r in rows]


@router.get("/candles/{symbol}", response_model=CandleList, summary="Hourly candles for one symbol")
def get_candles(
    symbol: str,
    source: str | None = Query(None, description="Only this feed, e.g. binance or finnhub. "
                                                 "Omit to include every feed that carries the symbol."),
    start: datetime | None = Query(None, description="Earliest candle open time, inclusive (ISO 8601; no zone = UTC)"),
    end: datetime | None = Query(None, description="Latest candle open time, EXCLUSIVE (ISO 8601; no zone = UTC)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum candles to return"),
    order: Literal["asc", "desc"] = Query("desc", description="desc = newest first (so `limit` keeps the latest)"),
    conn: Connection = Depends(get_warehouse_conn),
):
    symbol = symbol.upper()
    source = source.lower() if source else None
    start, end = _as_utc(start), _as_utc(end)
    if start and end and start >= end:
        raise HTTPException(422, "start must be earlier than end")

    known = conn.execute(
        text("SELECT 1 FROM symbols WHERE symbol = :symbol AND (CAST(:source AS text) IS NULL OR source = :source)"),
        {"symbol": symbol, "source": source}).first()
    if known is None:
        raise HTTPException(404, f"Unknown symbol {symbol}" + (f" for source {source}" if source else ""))

    rows = conn.execute(text(
        f"SELECT {CANDLE_COLUMNS} FROM ohlcv_1h o "
        "WHERE o.symbol = :symbol "
        "  AND (CAST(:source AS text) IS NULL OR o.source = :source) "
        "  AND (CAST(:start AS timestamptz) IS NULL OR o.open_time >= :start) "
        "  AND (CAST(:end AS timestamptz) IS NULL OR o.open_time < :end) "
        f"ORDER BY o.open_time {SORT[order]}, o.source "
        "LIMIT :limit"),
        {"symbol": symbol, "source": source, "start": start, "end": end, "limit": limit})
    candles = [Candle(**r._mapping) for r in rows]
    return CandleList(symbol=symbol, count=len(candles), candles=candles)
