"""Multi-user whitelist helpers: CRUD, access, monthly PnL, days active."""
from __future__ import annotations

import logging
from calendar import monthrange
from datetime import datetime, timezone, timedelta
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.auth import hash_password, encrypt_secret, decrypt_secret, user_has_access, refresh_expired_users
from app.models import User, UserSetting, UserCredential, UserTrade, utcnow

logger = logging.getLogger(__name__)


def ensure_user_rows(db: Session, user: User) -> None:
    """Create UserSetting + UserCredential rows. Migrates schema if columns missing."""
    def _try():
        if not db.query(UserSetting).filter(UserSetting.user_id == user.id).first():
            db.add(UserSetting(user_id=user.id, mode="shadow", order_type="FAK", position_size_usd=1.0, scale_entries=1, shadow_balance=50.0, auto_trade=True))
        if not db.query(UserCredential).filter(UserCredential.user_id == user.id).first():
            db.add(UserCredential(user_id=user.id))
        db.commit()

    try:
        _try()
    except Exception as e:
        logger.warning("ensure_user_rows failed, migrating schema: %s", e)
        try:
            db.rollback()
        except Exception:
            pass
        try:
            from app.database import init_db
            init_db()
        except Exception:
            logger.exception("init_db in ensure_user_rows")
        try:
            _try()
        except Exception:
            logger.exception("ensure_user_rows retry failed user_id=%s", getattr(user, "id", None))
            try:
                db.rollback()
            except Exception:
                pass



def list_users(db: Session) -> list[User]:
    refresh_expired_users(db)
    return db.query(User).order_by(User.display_name.asc()).all()


def get_user(db: Session, user_id: int) -> Optional[User]:
    return db.query(User).filter(User.id == user_id).first()


def register_username(db: Session, username: str) -> User:
    """
    Public self-registration: username only → status=pending.
    Admin approves later; user sets password after approval.
    """
    username = (username or "").strip().lower()
    if not username or len(username) < 3:
        raise ValueError("Username must be at least 3 characters")
    if not username.replace("_", "").replace("-", "").isalnum():
        raise ValueError("Username: letters, numbers, _ and - only")
    if username in ("admin", "root", "system"):
        raise ValueError("This username is reserved")
    existing = db.query(User).filter(User.username == username).first()
    if existing:
        return existing  # return existing so UI can show status
    # DB may still have password_hash NOT NULL from older schema — use placeholder
    # that verify_password always rejects until user sets a real password.
    u = User(
        display_name=username,
        username=username,
        password_hash="",  # empty = not set; login rejects
        role="user",
        status="pending",
        access_from=None,
        access_to=None,
        notes=None,
    )
    db.add(u)
    db.commit()
    db.refresh(u)
    try:
        ensure_user_rows(db, u)
    except Exception:
        logger.exception("ensure_user_rows after register username=%s", username)
    return u


def set_user_password(db: Session, user: User, password: str) -> User:
    if not password or len(password) < 6:
        raise ValueError("Password must be at least 6 characters")
    user.password_hash = hash_password(password)
    db.commit()
    db.refresh(user)
    return user


def approve_user(
    db: Session,
    user: User,
    *,
    display_name: Optional[str] = None,
    access_from: Optional[datetime] = None,
    access_to: Optional[datetime] = None,
    notes: Optional[str] = None,
    status: str = "active",
) -> User:
    """Admin approves a pending user — no password required from admin."""
    if display_name is not None:
        user.display_name = display_name.strip() or user.username
    user.status = status
    if access_from is not None:
        user.access_from = access_from
    if access_to is not None:
        user.access_to = access_to
    if notes is not None:
        user.notes = notes
    db.commit()
    db.refresh(user)
    ensure_user_rows(db, user)
    return user


def create_user(
    db: Session,
    *,
    display_name: str,
    username: str,
    password: str = "",
    access_from: Optional[datetime] = None,
    access_to: Optional[datetime] = None,
    notes: Optional[str] = None,
    status: str = "pending",
) -> User:
    """Legacy helper — prefer register_username + approve_user."""
    username = username.strip().lower()
    if db.query(User).filter(User.username == username).first():
        raise ValueError("Username already exists")
    u = User(
        display_name=display_name.strip() or username,
        username=username,
        password_hash=hash_password(password) if password else None,
        role="user",
        status=status,
        access_from=access_from,
        access_to=access_to,
        notes=notes,
    )
    db.add(u)
    db.commit()
    db.refresh(u)
    try:
        ensure_user_rows(db, u)
    except Exception:
        logger.exception("ensure_user_rows after register username=%s", username)
    return u


def update_user(
    db: Session,
    user: User,
    *,
    display_name: Optional[str] = None,
    status: Optional[str] = None,
    access_from: Optional[datetime] = None,
    access_to: Optional[datetime] = None,
    clear_access_to: bool = False,
    notes: Optional[str] = None,
    new_password: Optional[str] = None,
) -> User:
    if display_name is not None:
        user.display_name = display_name.strip()
    if status is not None:
        user.status = status
    if access_from is not None:
        user.access_from = access_from
    if clear_access_to:
        user.access_to = None
    elif access_to is not None:
        user.access_to = access_to
    if notes is not None:
        user.notes = notes
    if new_password:
        user.password_hash = hash_password(new_password)
    db.commit()
    db.refresh(user)
    return user


def parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s or not str(s).strip():
        return None
    s = str(s).strip().replace("Z", "")
    try:
        if "T" in s:
            dt = datetime.fromisoformat(s)
        else:
            dt = datetime.fromisoformat(s + "T00:00:00")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def user_mtd_pnl(db: Session, user_id: int, when: Optional[datetime] = None) -> float:
    when = when or datetime.now(timezone.utc)
    start = when.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    last_day = monthrange(start.year, start.month)[1]
    end = start.replace(day=last_day, hour=23, minute=59, second=59)
    q = (
        db.query(func.coalesce(func.sum(UserTrade.pnl), 0.0))
        .filter(
            UserTrade.user_id == user_id,
            UserTrade.result.in_(("win", "loss")),
            UserTrade.resolved_at >= start,
            UserTrade.resolved_at <= end,
        )
        .scalar()
    )
    return float(q or 0.0)


def user_days_active(
    db: Session,
    user_id: int,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
) -> int:
    q = db.query(UserTrade).filter(
        UserTrade.user_id == user_id,
        UserTrade.result.in_(("win", "loss")),
    )
    if date_from:
        q = q.filter(UserTrade.resolved_at >= date_from)
    if date_to:
        q = q.filter(UserTrade.resolved_at <= date_to)
    rows = q.all()
    days = set()
    for r in rows:
        if r.resolved_at:
            d = r.resolved_at
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
            days.add(d.date())
    return len(days)


def user_trade_stats(db: Session, user_id: int) -> dict:
    rows = (
        db.query(UserTrade)
        .filter(UserTrade.user_id == user_id, UserTrade.result.in_(("win", "loss")))
        .all()
    )
    wins = sum(1 for r in rows if r.result == "win")
    losses = sum(1 for r in rows if r.result == "loss")
    decided = wins + losses
    pnl = sum(float(r.pnl or 0) for r in rows)
    return {
        "trades": decided,
        "wins": wins,
        "losses": losses,
        "win_rate": (100.0 * wins / decided) if decided else 0.0,
        "pnl": round(pnl, 2),
        "mtd_pnl": round(user_mtd_pnl(db, user_id), 2),
        "days_active": user_days_active(db, user_id),
    }


def admin_user_rows(db: Session) -> list[dict]:
    """Rows for admin users table."""
    refresh_expired_users(db)
    out = []
    for u in list_users(db):
        st = user_trade_stats(db, u.id) if u.role != "admin" else {
            "trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
            "pnl": 0.0, "mtd_pnl": 0.0, "days_active": 0,
        }
        # for admin still compute if they have trades
        if u.role == "admin":
            st = user_trade_stats(db, u.id)
        out.append({
            "user": u,
            "has_access": user_has_access(u),
            "mtd_pnl": st["mtd_pnl"],
            "days_active": st["days_active"],
            "win_rate": st["win_rate"],
            "trades": st["trades"],
            "pnl": st["pnl"],
        })
    return out


def save_credentials(
    db: Session,
    user_id: int,
    *,
    api_key: str = "",
    api_secret: str = "",
    passphrase: str = "",
    funder_address: str = "",
    signature_type: int = 3,
    session_authorized: bool | None = None,
) -> UserCredential:
    """
    Safer path: user only stores funder + optional L2 keys.
    Session key is bot-generated (generate_session_key) — never the user's main key.
    """
    row = db.query(UserCredential).filter(UserCredential.user_id == user_id).first()
    if not row:
        row = UserCredential(user_id=user_id)
        db.add(row)
    if (api_key or "").strip():
        row.api_key = api_key.strip()
    if (api_secret or "").strip():
        row.api_secret_enc = encrypt_secret(api_secret.strip())
    if (passphrase or "").strip():
        row.passphrase_enc = encrypt_secret(passphrase.strip())
    if (funder_address or "").strip():
        row.funder_address = funder_address.strip()
    row.signature_type = int(signature_type if signature_type is not None else 3)
    if session_authorized is not None:
        row.session_authorized = bool(session_authorized)
    # Ready when funder exists and (session key OR L2 keys for shadow)
    row.has_credentials = bool(
        row.funder_address
        and (row.session_key_enc or (row.api_key and row.api_secret_enc and row.passphrase_enc))
    )
    row.updated_at = utcnow()
    db.commit()
    db.refresh(row)
    return row


def generate_session_key(db: Session, user_id: int) -> dict:
    """
    Create a fresh Session Key EOA for THIS user_id only.
    Never shares admin/operator keys. Stored under user_credentials.user_id.
    """
    from eth_account import Account
    from app.database import init_db

    user_id = int(user_id)
    row = db.query(UserCredential).filter(UserCredential.user_id == user_id).first()
    if not row:
        try:
            init_db()
        except Exception:
            pass
        row = UserCredential(user_id=user_id)
        db.add(row)
        try:
            db.commit()
            db.refresh(row)
        except Exception as e:
            db.rollback()
            # raw insert if ORM columns mismatch
            try:
                from sqlalchemy import text
                db.execute(
                    text("INSERT INTO user_credentials (user_id, signature_type, has_credentials) VALUES (:uid, 1, 0)"),
                    {"uid": user_id},
                )
                db.commit()
                row = db.query(UserCredential).filter(UserCredential.user_id == user_id).first()
            except Exception as e2:
                db.rollback()
                raise ValueError(f"Cannot create credentials row for user {user_id}: {e2}") from e

    if not row:
        raise ValueError(f"No user_credentials row for user_id={user_id}")

    # Confirm row belongs to this user (never admin row)
    if int(row.user_id) != user_id:
        raise ValueError("Credential row user mismatch — refusing session generate")

    acct = Account.create()
    pk = acct.key.hex()
    if not pk.startswith("0x"):
        pk = "0x" + pk
    addr = acct.address

    row.session_key_enc = encrypt_secret(pk)
    row.session_address = addr
    row.session_authorized = False
    # Deposit + session → POLY_1271 type 3
    row.signature_type = 3
    row.has_credentials = bool((row.funder_address or "").strip() and row.session_key_enc)
    try:
        row.updated_at = utcnow()
    except Exception:
        pass
    db.commit()
    db.refresh(row)

    logger.info("[SESSION] generated for user_id=%s address=%s funder=%s", user_id, addr, row.funder_address)

    auth_result = None
    note = (
        "Session key created for your account. Authorize this address on Polymarket "
        "for your Deposit Wallet (CLOB scope), then Mark authorized."
    )
    if row.funder_address:
        try:
            from app import polymarket as poly
            if poly.builder_configured():
                auth_result = poly.authorize_session_key(
                    deposit_wallet=row.funder_address,
                    session_address=addr,
                    scopes=["CLOB"],
                )
                if auth_result.get("success"):
                    row.session_authorized = True
                    row.has_credentials = True
                    db.commit()
                    db.refresh(row)
                    note = "Session key authorized via Builder API — live autotrade ready."
                else:
                    note = (
                        f"Session created. Builder authorize did not complete "
                        f"({auth_result.get('error', 'unknown')}). "
                        "Authorize on Polymarket then Mark authorized."
                    )
        except Exception as e:
            logger.warning("[SESSION] builder authorize attempt: %s", e)
            note = f"Session created. Authorize manually on Polymarket ({e})."

    return {
        "session_address": addr,
        "user_id": user_id,
        "funder": row.funder_address,
        "authorized": bool(row.session_authorized),
        "builder_auth": auth_result,
        "note": note,
    }



def mark_session_authorized(db: Session, user_id: int, authorized: bool = True) -> UserCredential:
    """
    Manual mark only allowed when Builder already authorized OR user acknowledges
    external Polymarket authorize. Prefer setup_managed_trading_wallet.
    """
    row = db.query(UserCredential).filter(UserCredential.user_id == user_id).first()
    if not row:
        raise ValueError("No credentials row")
    if not row.session_address:
        raise ValueError("Generate a session key first")
    if authorized and getattr(row, "builder_auth_error", None):
        # Still allow manual mark but keep error for debug
        logger.warning(
            "[SESSION] manual mark authorized user=%s despite builder_auth_error=%s",
            user_id, (row.builder_auth_error or "")[:120],
        )
    row.session_authorized = bool(authorized)
    row.has_credentials = bool(row.funder_address and row.session_key_enc)
    row.updated_at = utcnow()
    db.commit()
    db.refresh(row)
    return row


def setup_managed_trading_wallet(db: Session, user_id: int) -> dict:
    """
    Proper multi-user path (isolated from admin .env):

    1) Create deposit-wallet **owner** EOA (encrypted; exportable by user)
    2) Deploy deposit wallet via Builder WALLET-CREATE (from=owner)
    3) Create **session** EOA (trade-only)
    4) Authorize session with owner signature + Builder HMAC

    Admin/operator keys are never read or written here.
    """
    from eth_account import Account
    from app import polymarket as poly
    from app.database import init_db

    user_id = int(user_id)
    if not poly.builder_configured():
        return {
            "success": False,
            "error": (
                "Builder API not configured on server. Set POLYMARKET_BUILDER_API_KEY, "
                "POLYMARKET_BUILDER_SECRET, POLYMARKET_BUILDER_PASSPHRASE in operator .env "
                "(admin only — not used as trading keys)."
            ),
        }

    row = db.query(UserCredential).filter(UserCredential.user_id == user_id).first()
    if not row:
        try:
            init_db()
        except Exception:
            pass
        row = UserCredential(user_id=user_id)
        db.add(row)
        db.commit()
        db.refresh(row)

    if int(row.user_id) != user_id:
        return {"success": False, "error": "Credential row user mismatch"}

    steps = []

    # 1) Owner key
    owner_pk = None
    try:
        if row.owner_key_enc:
            owner_pk = decrypt_secret(row.owner_key_enc)
    except Exception:
        owner_pk = None
    if not owner_pk or not row.owner_address:
        acct = Account.create()
        owner_pk = acct.key.hex()
        if not owner_pk.startswith("0x"):
            owner_pk = "0x" + owner_pk
        row.owner_key_enc = encrypt_secret(owner_pk)
        row.owner_address = acct.address
        db.commit()
        db.refresh(row)
        steps.append(f"owner created {row.owner_address}")
    else:
        steps.append(f"owner existing {row.owner_address}")

    owner_addr = row.owner_address

    # 2) Deploy deposit wallet
    deposit = (row.funder_address or "").strip() or None
    deploy_res = None
    if not row.wallet_deployed or not deposit:
        deploy_res = poly.deploy_deposit_wallet(owner_addr)
        steps.append(f"deploy: {deploy_res.get('success')} {deploy_res.get('error') or deploy_res.get('state')}")
        if deploy_res.get("deposit_wallet"):
            deposit = deploy_res["deposit_wallet"]
            row.funder_address = deposit
            row.wallet_deployed = True
            row.builder_managed = True
            row.signature_type = 3
            db.commit()
            db.refresh(row)
        elif deploy_res.get("success"):
            # Submitted but address unknown — keep owner, mark managed attempt
            row.builder_managed = True
            row.wallet_deployed = bool(deploy_res.get("transaction_id"))
            row.builder_auth_error = (
                f"Deploy submitted (tx={deploy_res.get('transaction_id')}) but "
                "deposit address not returned yet. Re-run setup or set funder manually "
                "if you can resolve the address from Relayer."
            )
            db.commit()
            if not deposit:
                return {
                    "success": False,
                    "error": row.builder_auth_error,
                    "steps": steps,
                    "deploy": deploy_res,
                    "owner_address": owner_addr,
                }
        else:
            row.builder_auth_error = deploy_res.get("error") or "deploy failed"
            db.commit()
            return {
                "success": False,
                "error": row.builder_auth_error,
                "steps": steps,
                "deploy": deploy_res,
                "owner_address": owner_addr,
            }
    else:
        steps.append(f"deposit existing {deposit}")

    # 3) Session key
    acct_s = Account.create()
    sess_pk = acct_s.key.hex()
    if not sess_pk.startswith("0x"):
        sess_pk = "0x" + sess_pk
    sess_addr = acct_s.address
    row.session_key_enc = encrypt_secret(sess_pk)
    row.session_address = sess_addr
    row.session_authorized = False
    row.signature_type = 3
    row.funder_address = deposit
    row.builder_managed = True
    db.commit()
    db.refresh(row)
    steps.append(f"session created {sess_addr}")

    # 4) Authorize with owner signature
    auth = poly.authorize_session_key_with_owner(
        owner_private_key=owner_pk,
        deposit_wallet=deposit,
        session_address=sess_addr,
        scopes=["CLOB"],
    )
    steps.append(f"authorize: {auth.get('success')} {auth.get('error') or auth.get('method')}")

    if not auth.get("success"):
        # Fallback: builder-only (may 403 on non-builder wallets)
        auth2 = poly.authorize_session_key(
            deposit_wallet=deposit,
            session_address=sess_addr,
            scopes=["CLOB"],
        )
        steps.append(f"authorize_fallback: {auth2.get('success')} {auth2.get('error')}")
        auth = auth2 if auth2.get("success") else auth

    if auth.get("success"):
        row.session_authorized = True
        row.has_credentials = True
        row.builder_auth_error = None
        row.wallet_deployed = True
        db.commit()
        db.refresh(row)
        return {
            "success": True,
            "owner_address": owner_addr,
            "deposit_wallet": deposit,
            "session_address": sess_addr,
            "authorized": True,
            "steps": steps,
            "note": (
                "Managed wallet ready. Deposit pUSD to the deposit address. "
                "Bot trades with session key only. Export owner key from Account to withdraw."
            ),
            "deploy": deploy_res,
            "auth": auth,
        }

    # Session authorize blocked (typical: Builder 403) — still usable via owner signer
    row.session_authorized = False
    row.has_credentials = bool(deposit and (row.session_key_enc or row.owner_key_enc))
    row.builder_auth_error = auth.get("error") or "authorize failed"
    row.wallet_deployed = True
    row.builder_managed = True
    row.signature_type = 3
    db.commit()
    return {
        "success": True,  # deposit+owner ready; session optional
        "owner_address": owner_addr,
        "deposit_wallet": deposit,
        "session_address": sess_addr,
        "authorized": False,
        "signer_mode": "owner",
        "steps": steps,
        "error": row.builder_auth_error,
        "deploy": deploy_res,
        "auth": auth,
        "note": (
            "Deposit wallet ready. Builder session-authorize returned 403 (not allowed) — "
            "live trading will use the OWNER key for type-3 orders until Polymarket enables "
            "session keys for your Builder (builder@polymarket.com). "
            f"Fund deposit {deposit}, then set Live. Export owner key from Account to withdraw."
        ),
    }


def get_user_trading_context(db: Session, user_id: int) -> dict | None:
    """
    Trading context for connected users (never operator .env keys).

    Preference:
      1) Session key if Builder-authorized (trade-only, safest)
      2) Owner key for managed deposit wallets when session authorize is blocked
         (Builder 403) — owner is the deposit wallet controller; type 3 orders
      3) Explicit L2 api_key/secret if user pasted them

    Live readiness: funder + (authorized session OR managed owner key).
    """
    row = db.query(UserCredential).filter(UserCredential.user_id == user_id).first()
    if not row or not row.funder_address:
        return None
    try:
        secret = decrypt_secret(row.api_secret_enc) if row.api_secret_enc else None
        phrase = decrypt_secret(row.passphrase_enc) if row.passphrase_enc else None
        session_pk = decrypt_secret(row.session_key_enc) if row.session_key_enc else None
        owner_pk = decrypt_secret(row.owner_key_enc) if getattr(row, "owner_key_enc", None) else None
    except Exception as e:
        logger.warning("decrypt credentials failed user=%s: %s", user_id, e)
        return None

    session_ok = bool(session_pk and row.session_authorized and not (getattr(row, "builder_auth_error", None) or "").startswith("HTTP 403"))
    # Managed path: owner can sign type-3 deposit orders without session authorize
    managed = bool(getattr(row, "builder_managed", False) and owner_pk and row.funder_address)
    use_owner = managed and not session_ok

    signing_pk = session_pk if session_ok else (owner_pk if use_owner else session_pk)
    signer_mode = "session" if session_ok else ("owner" if use_owner else ("session_unauth" if session_pk else "none"))

    return {
        "user_id": user_id,
        "api_key": row.api_key,
        "api_secret": secret,
        "api_passphrase": phrase,
        "private_key": signing_pk,
        "session_address": row.session_address,
        "session_authorized": bool(row.session_authorized),
        "owner_address": getattr(row, "owner_address", None),
        "funder": row.funder_address,
        "signature_type": int(row.signature_type if row.signature_type is not None else 3),
        "has_session_key": bool(session_pk),
        "has_owner_key": bool(owner_pk),
        "has_private_key": bool(signing_pk),
        "signer_mode": signer_mode,
        "builder_managed": bool(getattr(row, "builder_managed", False)),
        "live_ready": bool(
            row.funder_address
            and signing_pk
            and (session_ok or use_owner)
        ),
    }


def export_owner_private_key(db: Session, user_id: int) -> dict:
    """Return decrypted owner key once for the account owner (withdraw / backup)."""
    row = db.query(UserCredential).filter(UserCredential.user_id == user_id).first()
    if not row or not getattr(row, "owner_key_enc", None):
        return {"success": False, "error": "No managed owner key on this account"}
    try:
        pk = decrypt_secret(row.owner_key_enc)
    except Exception as e:
        return {"success": False, "error": str(e)}
    return {
        "success": True,
        "owner_address": row.owner_address,
        "private_key": pk,
        "deposit_wallet": row.funder_address,
        "note": "This key controls the deposit wallet (withdraw). Store offline; bot keeps an encrypted copy for managed trading until you rotate.",
    }


def list_autotrade_users(db: Session) -> list[tuple]:
    """
    [(User, UserSetting, trading_context), ...] for independent execution.
    """
    out = []
    for u in active_trading_users(db):
        us = get_user_setting(db, u.id)
        if not getattr(us, "auto_trade", True):
            continue
        if not getattr(us, "enabled_btc", True):
            continue
        ctx = get_user_trading_context(db, u.id)
        if not ctx:
            continue
        out.append((u, us, ctx))
    return out


def get_user_setting(db: Session, user_id: int) -> UserSetting:
    try:
        row = db.query(UserSetting).filter(UserSetting.user_id == user_id).first()
    except Exception as e:
        # Missing columns (e.g. scale_entries) — migrate and retry
        logger.warning("get_user_setting query failed, migrating: %s", e)
        try:
            db.rollback()
        except Exception:
            pass
        try:
            from app.database import init_db
            init_db()
        except Exception:
            logger.exception("init_db during get_user_setting")
        row = db.query(UserSetting).filter(UserSetting.user_id == user_id).first()
    if not row:
        row = UserSetting(
            user_id=user_id, mode="shadow", order_type="FAK",
            position_size_usd=1.0, scale_entries=1, shadow_balance=50.0,
            max_contract_price=0.95, enabled_btc=True, enabled_eth=False,
            auto_trade=True,
        )
        db.add(row)
        try:
            db.commit()
            db.refresh(row)
        except Exception as e:
            db.rollback()
            logger.warning("create UserSetting failed: %s", e)
            # last resort: migrate again
            try:
                from app.database import init_db
                init_db()
                row = UserSetting(user_id=user_id)
                db.add(row)
                db.commit()
                db.refresh(row)
            except Exception:
                logger.exception("UserSetting create after migrate failed")
                raise
    # Align legacy user rows with late-window favorite ceiling
    try:
        if row.max_contract_price is None or float(row.max_contract_price) < 0.95:
            row.max_contract_price = 0.95
            db.commit()
            db.refresh(row)
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
    return row


def active_trading_users(db: Session) -> list[User]:
    """Users allowed to receive auto-trades right now (active + access window)."""
    refresh_expired_users(db)
    users = db.query(User).filter(User.status == "active").all()
    return [u for u in users if user_has_access(u) and (u.role or "user") in ("user", "admin")]
