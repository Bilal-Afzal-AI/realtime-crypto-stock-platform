"""Typed settings, read from environment variables and the repo's git-ignored .env.

Loading config in one validated place means a missing or weak value stops the
app at startup with a clear message, instead of surfacing later as a confusing
500 on the first request.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_FILE = Path(__file__).resolve().parents[3] / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    # Two databases on purpose (see backend/ddl/01_users.sql): accounts live in
    # `backend`, market data in `market`, each with its own login role. The API
    # reads the warehouse through a READ-ONLY role.
    backend_database_url: str
    warehouse_readonly_url: str

    # Token signing. Only HMAC algorithms: they use one shared secret, which is
    # all a single service needs. Asymmetric keys (RS256) matter once other
    # services must verify tokens without being able to mint them.
    jwt_secret_key: str = Field(min_length=32)
    jwt_algorithm: Literal["HS256", "HS384", "HS512"] = "HS256"
    jwt_issuer: str = "market-data-api"
    access_token_expire_minutes: int = Field(default=30, ge=1, le=24 * 60)

    # Rate limits, requests per minute.
    rate_limit_data_per_minute: int = Field(default=60, ge=1)      # per user
    rate_limit_login_per_minute: int = Field(default=10, ge=1)     # per client IP
    rate_limit_register_per_minute: int = Field(default=5, ge=1)   # per client IP

    @field_validator("jwt_secret_key")
    @classmethod
    def _not_a_placeholder(cls, v: str) -> str:
        if v.strip().lower().startswith("change-me"):
            raise ValueError("JWT_SECRET_KEY is still the placeholder; generate a real one: "
                             "python -c \"import secrets; print(secrets.token_urlsafe(48))\"")
        return v


@lru_cache
def get_settings() -> Settings:
    return Settings()
