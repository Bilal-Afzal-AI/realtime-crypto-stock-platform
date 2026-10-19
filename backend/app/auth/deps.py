"""FastAPI dependency that turns "Authorization: Bearer <token>" into a user.

Note what it does NOT do: it never queries a database. Everything needed is in
the signed token, which is what "stateless" means. Any server holding the
secret can verify any token, so adding servers needs no shared session store.
The price is that a token cannot be revoked before it expires; that is why
tokens are short-lived (30 minutes by default). Revocation before then (a
banned user, a stolen token) needs server-side state again: a deny-list of
token ids in Redis, or refresh tokens that ARE looked up in the database.
"""

from __future__ import annotations

from dataclasses import dataclass

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from backend.app.auth.security import decode_access_token
from backend.app.core.config import Settings, get_settings

# tokenUrl makes the "Authorize" button in /docs call our login endpoint.
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


@dataclass(frozen=True)
class CurrentUser:
    id: int
    username: str


def get_current_user(token: str = Depends(oauth2_scheme),
                     settings: Settings = Depends(get_settings)) -> CurrentUser:
    unauthorized = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        claims = decode_access_token(token, settings)
        return CurrentUser(id=int(claims["sub"]), username=str(claims.get("username", "")))
    except (jwt.PyJWTError, ValueError):
        # One message for every failure: expired, tampered, wrong issuer... A
        # detailed reason only helps someone probing the token.
        raise unauthorized from None
