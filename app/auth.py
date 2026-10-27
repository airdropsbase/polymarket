"""
Simple session auth + whitelist access checks.

Env:
  APP_SECRET          — required for sessions & credential encryption
  ADMIN_USERNAME      — seed admin (default: admin)
  ADMIN_PASSWORD      — seed admin password (default: changeme)
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os
import secrets
from datetime import datetime, timezone
from typing import Optional

from fastapi import Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import User, utcnow

logger = logging.getLogger(__name__)

# Unique per app instance so two ports on the same host do not log each other out.
# Set POLYBOT_SESSION_COOKIE in .env (e.g. polybot_session_8081).
SESSION_COOKIE = (
    os.environ.get("POLYBOT_SESSION_COOKIE", "").strip()
    or f"polybot_session_{os.environ.get('PORT', os.environ.get('APP_PORT', '8080'))}"
)


def cookie_secure(request: Optional[Request] = None) -> bool:
    """True behind HTTPS / Fly proxy so browsers accept the session cookie."""
    if os.environ.get("FORCE_SECURE_COOKIES", "").lower() in ("1", "true", "yes"):
        return True
    if os.environ.get("FLY_APP_NAME") or os.environ.get("FLY_MACHINE_ID"):
        return True
    if request is not None:
        proto = (request.headers.get("x-forwarded-proto") or "").split(",")[0].strip().lower()
        if proto == "https":
            return True
        if request.url.scheme == "https":
            return True
    return False


def set_session_cookie(response, token: str, request: Optional[Request] = None, max_age: int = 72 * 3600) -> None:
    response.set_cookie(
        SESSION_COOKIE,
        token,
        httponly=True,
        samesite="lax",
        secure=cookie_secure(request),
        max_age=max_age,
        path="/",
    )
_SESSION_STORE: dict[str, dict] = {}  # token -> {user_id, exp}


def _app_secret() -> bytes:
    s = os.environ.get("APP_SECRET", "").strip()
    if not s:
        s = "dev-only-change-me-in-production"
    return s.encode("utf-8")


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 120_000)
    return f"pbkdf2:{salt}:{dk.hex()}"


def verify_password(password: str, stored: Optional[str]) -> bool:
    if not stored or not str(stored).strip():
        return False
    if not stored or not password:
        return False
    try:
        algo, salt, hexhash = stored.split(":", 2)
        if algo != "pbkdf2":
            return False
        dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 120_000)
        return hmac.compare_digest(dk.hex(), hexhash)
    except Exception:
        return False


def needs_password_setup(user: Optional[User]) -> bool:
    return bool(user and user.role != "admin" and user.status == "active" and not (user.password_hash or "").strip())


def encrypt_secret(plain: str) -> str:
    """Lightweight reversible encoding with APP_SECRET (not for high-security vaults)."""
    if not plain:
        return ""
    key = hashlib.sha256(_app_secret()).digest()
    data = plain.encode("utf-8")
    out = bytes(b ^ key[i % len(key)] for i, b in enumerate(data))
    return base64.urlsafe_b64encode(out).decode("ascii")


def decrypt_secret(enc: str) -> str:
    if not enc:
        return ""
    try:
        key = hashlib.sha256(_app_secret()).digest()
        data = base64.urlsafe_b64decode(enc.encode("ascii"))
        out = bytes(b ^ key[i % len(key)] for i, b in enumerate(data))
        return out.decode("utf-8")
    except Exception:
        return ""


def create_session(user_id: int, hours: int = 72) -> str:
    token = secrets.token_urlsafe(32)
    exp = datetime.now(timezone.utc).timestamp() + hours * 3600
    _SESSION_STORE[token] = {"user_id": user_id, "exp": exp}
    return token


def destroy_session(token: Optional[str]) -> None:
    if token and token in _SESSION_STORE:
        del _SESSION_STORE[token]


def _session_user_id(request: Request) -> Optional[int]:
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        return None
    data = _SESSION_STORE.get(token)
    if not data:
        return None
    if datetime.now(timezone.utc).timestamp() > data["exp"]:
        destroy_session(token)
        return None
    return int(data["user_id"])


def get_current_user(request: Request, db: Session) -> Optional[User]:
    uid = _session_user_id(request)
    if not uid:
        return None
    return db.query(User).filter(User.id == uid).first()


def user_has_access(user: Optional[User], now: Optional[datetime] = None) -> bool:
    """Whitelist + date window + status."""
    if user is None:
        return False
    if user.role == "admin":
        # Admin always allowed into admin UI; trading still needs active if you want
        if user.status == "suspended":
            return False
        return True
    if user.status != "active":
        return False
    now = now or datetime.now(timezone.utc)
    # normalize naive DB timestamps as UTC
    af = user.access_from
    at = user.access_to
    if af is not None and af.tzinfo is None:
        af = af.replace(tzinfo=timezone.utc)
    if at is not None and at.tzinfo is None:
        at = at.replace(tzinfo=timezone.utc)
    if af is not None and now < af:
        return False
    if at is not None and now > at:
        return False
    return True


def refresh_expired_users(db: Session) -> int:
    """Mark active users past access_to as expired."""
    now = datetime.now(timezone.utc)
    n = 0
    for u in db.query(User).filter(User.status == "active", User.access_to.isnot(None)).all():
        at = u.access_to
        if at.tzinfo is None:
            at = at.replace(tzinfo=timezone.utc)
        if now > at:
            u.status = "expired"
            n += 1
    if n:
        db.commit()
    return n


def require_user(request: Request, db: Session = Depends(get_db)) -> User:
    user = get_current_user(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="Login required")
    if not user_has_access(user):
        raise HTTPException(status_code=403, detail="Access revoked or expired")
    return user


def require_admin(request: Request, db: Session = Depends(get_db)) -> User:
    user = get_current_user(request, db)
    if not user or user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    if user.status == "suspended":
        raise HTTPException(status_code=403, detail="Suspended")
    return user


def login_redirect_if_needed(request: Request, db: Session) -> Optional[RedirectResponse]:
    """Return redirect to /login if not authenticated (for HTML pages)."""
    user = get_current_user(request, db)
    if user and user_has_access(user):
        return None
    return RedirectResponse(url="/login", status_code=303)


def admin_login_redirect_if_needed(request: Request, db: Session) -> Optional[RedirectResponse]:
    """Admin HTML pages only — never send users to a discoverable admin login from public UI."""
    user = get_current_user(request, db)
    if user and user.role == "admin" and user.status != "suspended":
        return None
    return RedirectResponse(url="/admin/login", status_code=303)


def ensure_seed_admin(db: Session) -> None:
    """Create default admin if no admin exists."""
    existing = db.query(User).filter(User.role == "admin").first()
    if existing:
        return
    username = os.environ.get("ADMIN_USERNAME", "admin").strip() or "admin"
    password = os.environ.get("ADMIN_PASSWORD", "changeme").strip() or "changeme"
    admin = User(
        display_name="Administrator",
        username=username,
        password_hash=hash_password(password),
        role="admin",
        status="active",
        access_from=None,
        access_to=None,
        notes="Seeded admin — change password after first login",
    )
    db.add(admin)
    db.commit()
    logger.info("Seeded admin user '%s' — change default password!", username)
