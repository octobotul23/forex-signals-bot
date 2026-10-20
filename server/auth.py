"""Passwords, session cookies, role checks and connector tokens."""
import hashlib
import secrets
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError
from fastapi import Depends, HTTPException, Request, Response
from sqlalchemy import select
from sqlalchemy.orm import Session

from .db import ConnectorToken, User, get_db, utcnow
from .settings import COOKIE_SECURE, SECRET_KEY, SESSION_DAYS

COOKIE = "fx_session"
_hasher = PasswordHasher()


# ── passwords ──────────────────────────────────────────────────
def hash_password(password: str) -> str:
    return _hasher.hash(password)


def verify_password(hashed: str, password: str) -> bool:
    try:
        return _hasher.verify(hashed, password)
    except (VerifyMismatchError, InvalidHashError):
        return False


def validate_password(password: str):
    if len(password) < 8:
        raise HTTPException(400, "password_too_short")
    if len(password) > 200:
        raise HTTPException(400, "password_too_long")


def temp_password() -> str:
    return secrets.token_urlsafe(9)


# ── sessions ───────────────────────────────────────────────────
def issue_session(response: Response, user: User):
    exp = datetime.now(timezone.utc) + timedelta(days=SESSION_DAYS)
    token = jwt.encode({"sub": str(user.id), "ver": user.token_version, "exp": exp}, SECRET_KEY, algorithm="HS256")
    response.set_cookie(COOKIE, token, max_age=SESSION_DAYS * 86400, httponly=True, secure=COOKIE_SECURE,
                        samesite="lax", path="/")


def clear_session(response: Response):
    response.delete_cookie(COOKIE, path="/")


def user_from_token(db: Session, token: str | None) -> User | None:
    if not token:
        return None
    try:
        data = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
    except jwt.PyJWTError:
        return None
    user = db.get(User, int(data.get("sub", 0)))
    if not user or user.token_version != data.get("ver"):
        return None
    return user


def current_user(request: Request, db: Session = Depends(get_db)) -> User:
    user = user_from_token(db, request.cookies.get(COOKIE))
    if not user:
        raise HTTPException(401, "not_logged_in")
    if user.is_banned:
        raise HTTPException(403, "banned")
    return user


def active_user(user: User = Depends(current_user)) -> User:
    """Logged in AND (staff, or client with a paid/trial subscription)."""
    if not user.subscription_active:
        raise HTTPException(402, "subscription_inactive")
    return user


def staff_user(user: User = Depends(current_user)) -> User:
    if not user.is_staff:
        raise HTTPException(403, "admins_only")
    return user


def founder_user(user: User = Depends(current_user)) -> User:
    if user.role != "founder":
        raise HTTPException(403, "founder_only")
    return user


# ── who may manage whom ────────────────────────────────────────
def can_manage(actor: User, target: User) -> bool:
    """Founder manages everyone (incl. self). Admins manage clients only. Nobody else touches the founder."""
    if actor.role == "founder":
        return True
    if actor.role == "admin":
        return target.role == "client"
    return False


# ── simple in-memory rate limiting (single-process server) ─────
_hits: dict[str, list[float]] = defaultdict(list)


def rate_limit(key: str, limit: int, window_seconds: int):
    now = time.time()
    hits = [t for t in _hits[key] if now - t < window_seconds]
    if len(hits) >= limit:
        raise HTTPException(429, "too_many_attempts")
    hits.append(now)
    _hits[key] = hits


def client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for", "")
    return fwd.split(",")[0].strip() if fwd else (request.client.host if request.client else "?")


# ── connector tokens ───────────────────────────────────────────
def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def new_connector_token(db: Session, user: User, label: str) -> str:
    token = "fxc_" + secrets.token_urlsafe(32)
    db.add(ConnectorToken(user_id=user.id, token_hash=hash_token(token), label=label[:80] or "My PC"))
    db.commit()
    return token


def user_from_connector_token(db: Session, token: str) -> User | None:
    row = db.scalar(select(ConnectorToken).where(ConnectorToken.token_hash == hash_token(token),
                                                 ConnectorToken.revoked.is_(False)))
    if not row:
        return None
    row.last_used_at = utcnow()
    db.commit()
    return db.get(User, row.user_id)
