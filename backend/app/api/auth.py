"""Register and log in. The only routes that touch the auth database."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy import Connection, text
from sqlalchemy.exc import IntegrityError

from backend.app.auth.security import (
    burn_password_check,
    create_access_token,
    hash_password,
    verify_password,
)
from backend.app.core.config import Settings, get_settings
from backend.app.core.db import get_backend_conn
from backend.app.core.rate_limit import limit_login, limit_register
from backend.app.models.schemas import RegisterRequest, TokenOut, UserOut

router = APIRouter(prefix="/auth", tags=["auth"])

UNIQUE_VIOLATION = "23505"


@router.post("/register", response_model=UserOut, status_code=status.HTTP_201_CREATED,
             dependencies=[Depends(limit_register)])
def register(body: RegisterRequest, conn: Connection = Depends(get_backend_conn)) -> UserOut:
    try:
        row = conn.execute(
            text("INSERT INTO users (username, password_hash) VALUES (:u, :h) "
                 "RETURNING id, username, created_at"),
            {"u": body.username, "h": hash_password(body.password)},
        ).one()
    except IntegrityError as exc:
        # Rely on the UNIQUE constraint rather than "SELECT, then INSERT": two
        # simultaneous sign-ups for one name would both pass a SELECT.
        if getattr(exc.orig, "sqlstate", None) == UNIQUE_VIOLATION:
            raise HTTPException(status.HTTP_409_CONFLICT, "Username already taken") from None
        raise
    return UserOut(id=row.id, username=row.username, created_at=row.created_at)


@router.post("/login", response_model=TokenOut, dependencies=[Depends(limit_login)])
def login(form: OAuth2PasswordRequestForm = Depends(),
          conn: Connection = Depends(get_backend_conn),
          settings: Settings = Depends(get_settings)) -> TokenOut:
    """Exchange username + password (form fields, per the OAuth2 spec) for a token."""
    username = form.username.strip().lower()
    user = conn.execute(
        text("SELECT id, username, password_hash, is_active FROM users WHERE username = :u"),
        {"u": username},
    ).one_or_none()

    if user is None:
        burn_password_check(form.password)       # same cost as a real check
    if user is None or not user.is_active or not verify_password(form.password, user.password_hash):
        # Same answer for "no such user", "wrong password" and "disabled": telling
        # them apart helps an attacker enumerate accounts.
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Incorrect username or password",
                            headers={"WWW-Authenticate": "Bearer"})

    conn.execute(text("UPDATE users SET last_login_at = now() WHERE id = :id"), {"id": user.id})
    token, expires_in = create_access_token(user.id, user.username, settings)
    return TokenOut(access_token=token, expires_in=expires_in)
