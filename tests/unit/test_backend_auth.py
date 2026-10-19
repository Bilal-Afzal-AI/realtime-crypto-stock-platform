"""Auth building blocks and the rate limiter. No database or network needed."""

from datetime import UTC, datetime, timedelta

import jwt
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from backend.app.auth import security
from backend.app.core.config import Settings
from backend.app.core.rate_limit import SlidingWindowLimiter
from backend.app.main import app
from backend.app.models.schemas import RegisterRequest


@pytest.fixture
def settings():
    return Settings(backend_database_url="postgresql+psycopg://x:y@localhost/x",
                    warehouse_readonly_url="postgresql+psycopg://x:y@localhost/x",
                    jwt_secret_key="k" * 48)


# --- passwords ---------------------------------------------------------------

def test_password_hash_is_salted_and_verifies():
    a, b = security.hash_password("correct horse battery"), security.hash_password("correct horse battery")
    assert a != b                                    # fresh salt every time
    assert a.startswith("$2b$12$")                   # bcrypt, cost 12
    assert security.verify_password("correct horse battery", a)
    assert not security.verify_password("wrong password!!", a)
    assert not security.verify_password("anything", "not-a-hash")


def test_register_request_validation():
    assert RegisterRequest(username="  Alice_1 ", password="long-enough-pw").username == "alice_1"
    for bad in ({"username": "ab", "password": "long-enough-pw"},
                {"username": "has space", "password": "long-enough-pw"},
                {"username": "alice", "password": "short"},
                {"username": "alice", "password": "x" * 73}):
        with pytest.raises(ValidationError):
            RegisterRequest(**bad)


# --- tokens ------------------------------------------------------------------

def test_token_round_trip(settings):
    token, expires_in = security.create_access_token(7, "alice", settings)
    claims = security.decode_access_token(token, settings)
    assert (claims["sub"], claims["username"], expires_in) == ("7", "alice", 30 * 60)


def test_tampered_expired_and_unsigned_tokens_are_rejected(settings):
    token, _ = security.create_access_token(7, "alice", settings)
    head, payload, sig = token.split(".")
    with pytest.raises(jwt.PyJWTError):
        security.decode_access_token(f"{head}.{payload}.{sig[:-3]}abc", settings)

    other = settings.model_copy(update={"jwt_secret_key": "z" * 48})   # signed with a different secret
    with pytest.raises(jwt.PyJWTError):
        security.decode_access_token(security.create_access_token(7, "a", other)[0], settings)

    past = datetime.now(UTC) - timedelta(minutes=1)
    expired = jwt.encode({"sub": "7", "iss": settings.jwt_issuer, "iat": past - timedelta(hours=1), "exp": past},
                         settings.jwt_secret_key, algorithm="HS256")
    with pytest.raises(jwt.ExpiredSignatureError):
        security.decode_access_token(expired, settings)

    unsigned = jwt.encode({"sub": "7", "iss": settings.jwt_issuer, "iat": past, "exp": datetime.now(UTC) + timedelta(hours=1)},
                          key=None, algorithm="none")
    with pytest.raises(jwt.PyJWTError):                     # the classic alg=none attack
        security.decode_access_token(unsigned, settings)


def test_placeholder_secret_is_refused():
    with pytest.raises(ValidationError):
        Settings(backend_database_url="x", warehouse_readonly_url="x", jwt_secret_key="change-me" + "x" * 30)


# --- routes are protected (these requests never reach a database) ------------

@pytest.mark.parametrize("path", ["/symbols", "/candles/latest", "/candles/BTCUSDT"])
def test_data_endpoints_require_a_token(path):
    client = TestClient(app)
    assert client.get(path).status_code == 401
    assert client.get(path, headers={"Authorization": "Bearer not.a.token"}).status_code == 401
    assert client.get("/health").status_code == 200


# --- rate limiter ------------------------------------------------------------

def test_sliding_window_limit_and_recovery():
    lim = SlidingWindowLimiter(window=60)
    results = [lim.check("u", limit=3, now=t) for t in (0, 1, 2, 3)]
    assert [r.allowed for r in results] == [True, True, True, False]
    assert results[2].remaining == 0
    assert results[3].retry_after == 58                     # first hit (t=0) frees up at t=60
    assert lim.check("u", limit=3, now=59).allowed is False
    assert lim.check("u", limit=3, now=60.5).allowed is True  # t=0 aged out


def test_sliding_window_has_no_boundary_burst():
    lim = SlidingWindowLimiter(window=60)
    for t in (50, 51, 52):
        assert lim.check("u", limit=3, now=t).allowed
    # A fixed per-minute counter would reset at t=60 and allow 3 more instantly.
    assert not lim.check("u", limit=3, now=61).allowed


def test_callers_are_limited_independently():
    lim = SlidingWindowLimiter(window=60)
    assert lim.check("alice", 1, now=0).allowed
    assert not lim.check("alice", 1, now=1).allowed
    assert lim.check("bob", 1, now=1).allowed
