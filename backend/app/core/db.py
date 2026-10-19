"""Database connections: one engine per database, never shared.

Route handlers ask for the connection they need. Data endpoints only ever get
the warehouse connection, so they have no way to touch the users table even by
mistake, and the auth endpoints never touch the warehouse.
"""

from __future__ import annotations

from collections.abc import Iterator
from functools import lru_cache

from sqlalchemy import Connection, Engine, create_engine

from backend.app.core.config import get_settings


@lru_cache
def backend_engine() -> Engine:
    return create_engine(
        get_settings().backend_database_url,
        pool_size=5, max_overflow=5, pool_pre_ping=True,
        connect_args={"application_name": "market-api-auth"},
    )


@lru_cache
def warehouse_engine() -> Engine:
    return create_engine(
        get_settings().warehouse_readonly_url,
        pool_size=5, max_overflow=5, pool_pre_ping=True,
        # The role is already read-only with a 5 s statement timeout; asking for
        # it here too means the app stays safe even if pointed at a different role.
        connect_args={"application_name": "market-api-data",
                      "options": "-c default_transaction_read_only=on -c statement_timeout=5000"},
    )


def get_backend_conn() -> Iterator[Connection]:
    """Auth database connection; commits on success, rolls back on error."""
    with backend_engine().begin() as conn:
        yield conn


def get_warehouse_conn() -> Iterator[Connection]:
    with warehouse_engine().connect() as conn:
        yield conn
