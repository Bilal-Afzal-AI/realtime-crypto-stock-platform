"""Request and response shapes. These are the API's public contract."""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field, field_validator

USERNAME_RE = re.compile(r"^[a-z0-9_]{3,32}$")


class RegisterRequest(BaseModel):
    username: str = Field(description="3-32 characters: letters, digits, underscore. Stored lowercase.")
    password: str = Field(min_length=10, description="At least 10 characters, at most 72 bytes.")

    @field_validator("username")
    @classmethod
    def _username(cls, v: str) -> str:
        v = v.strip().lower()
        if not USERNAME_RE.fullmatch(v):
            raise ValueError("username must be 3-32 characters: letters, digits, underscore")
        return v

    @field_validator("password")
    @classmethod
    def _password_bytes(cls, v: str) -> str:
        if len(v.encode("utf-8")) > 72:
            raise ValueError("password must be at most 72 bytes (a bcrypt limit)")
        return v


class UserOut(BaseModel):
    id: int
    username: str
    created_at: datetime


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int = Field(description="Seconds until the token expires")


class SymbolOut(BaseModel):
    source: str
    symbol: str
    asset_class: str
    quote_currency: str
    display_name: str | None
    is_active: bool


# Prices and volumes are Decimal and serialize as JSON STRINGS ("82714.76000000").
# A JSON number would be parsed into a binary float by most clients, bringing back
# the rounding errors NUMERIC in the warehouse exists to avoid. Clients that
# chart can convert at the edge; clients that calculate money should not.
class Candle(BaseModel):
    source: str
    symbol: str
    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trade_count: int


class LatestCandle(Candle):
    asset_class: str


class CandleList(BaseModel):
    symbol: str
    count: int
    candles: list[Candle]
