"""Password hashing (bcrypt) and access tokens (JWT).

Passwords
---------
bcrypt is deliberately slow (cost factor 12 is roughly a quarter of a second per
hash), which turns an attacker's "try a billion guesses" into years of compute,
and it salts every hash with fresh random bytes, so two users with the same
password get different hashes and precomputed tables are useless. Never use a
fast hash like SHA-256 for passwords: GPUs compute billions per second.

bcrypt only reads the first 72 bytes of input. Longer passwords are rejected at
the API boundary (see models/schemas.py) instead of being silently truncated.

Tokens
------
A JWT is three base64 parts: header.payload.signature. The payload carries the
claims (who, until when) and the signature is an HMAC of header+payload under
our secret. Anyone can READ a token (so never put secrets in it); nobody
without the secret can CHANGE it, because the signature would stop matching.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from functools import lru_cache

import bcrypt
import jwt

from backend.app.core.config import Settings

BCRYPT_ROUNDS = 12


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=BCRYPT_ROUNDS)).decode("ascii")


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("ascii"))
    except ValueError:  # malformed hash or over-long password
        return False


@lru_cache
def _dummy_hash() -> str:
    return hash_password("not-a-real-password")


def burn_password_check(password: str) -> None:
    """Spend the same time a real check would.

    Login for an unknown username must not return faster than login with a
    wrong password, or the response time tells an attacker which usernames exist.
    """
    verify_password(password, _dummy_hash())


def create_access_token(user_id: int, username: str, settings: Settings) -> tuple[str, int]:
    now = datetime.now(UTC)
    lifetime = timedelta(minutes=settings.access_token_expire_minutes)
    payload = {
        "sub": str(user_id),
        "username": username,
        "iss": settings.jwt_issuer,
        "iat": now,
        "exp": now + lifetime,
    }
    token = jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)
    return token, int(lifetime.total_seconds())


def decode_access_token(token: str, settings: Settings) -> dict:
    """Verify signature, expiry and issuer; raises jwt.PyJWTError if anything is off.

    `algorithms` is pinned to OUR algorithm. Never let the token's own header
    choose: the classic attack sends `alg: none` (no signature) and hopes the
    server believes the token about how to check itself.
    """
    return jwt.decode(
        token,
        settings.jwt_secret_key,
        algorithms=[settings.jwt_algorithm],
        issuer=settings.jwt_issuer,
        options={"require": ["exp", "iat", "sub", "iss"]},
    )
