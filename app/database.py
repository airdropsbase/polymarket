"""
Database setup with automatic schema management.

Supports:
  - SQLite (default local): sqlite:///./polymarket_bot.db
  - Supabase Postgres via Session Pooler (IPv4) — recommended for Fly.io

Supabase Session mode (port 5432) connection string example:
  postgresql://postgres.PROJECT_REF:PASSWORD@aws-0-REGION.pooler.supabase.com:5432/postgres?sslmode=require

Set as Fly secret:
  fly secrets set DATABASE_URL="postgresql://postgres.xxx:yyy@aws-0-xxx.pooler.supabase.com:5432/postgres?sslmode=require"
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from sqlalchemy.pool import StaticPool, NullPool, QueuePool

logger = logging.getLogger(__name__)

# Prefer durable path in containers (Fly/Docker). Override with DATABASE_URL secret.
_default_sqlite = "sqlite:////app/data/polymarket_bot.db"
if not Path("/app/data").exists():
    _default_sqlite = "sqlite:///./data/polymarket_bot.db"
DATABASE_URL = os.environ.get("DATABASE_URL", _default_sqlite).strip()

# Wasmer Edge / container: ensure SQLite parent directory exists
if DATABASE_URL.startswith("sqlite:///"):
    _db_path = DATABASE_URL.replace("sqlite:///", "", 1)
    if _db_path and _db_path != ":memory:":
        import pathlib
        pathlib.Path(_db_path).parent.mkdir(parents=True, exist_ok=True)

# Heroku / some dashboards still emit postgres://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Ensure SSL for Supabase / remote Postgres if not already specified
if (
    DATABASE_URL.startswith("postgresql")
    and "sslmode=" not in DATABASE_URL
    and ("supabase" in DATABASE_URL or "pooler" in DATABASE_URL)
):
    sep = "&" if "?" in DATABASE_URL else "?"
    DATABASE_URL = f"{DATABASE_URL}{sep}sslmode=require"

connect_args: dict = {}
engine_kwargs: dict = {"pool_pre_ping": True}

if DATABASE_URL.startswith("sqlite"):
    connect_args = {"check_same_thread": False}
    if ":memory:" in DATABASE_URL:
        engine_kwargs["poolclass"] = StaticPool
else:
    # Supabase Session Pooler already multiplexes connections.
    # Keep a small app-side pool to avoid exhausting the pooler.
    # Session mode (5432) supports prepared statements; Transaction mode (6543) does not.
    is_pooler = "pooler" in DATABASE_URL or "supabase" in DATABASE_URL
    pool_size = int(os.environ.get("DB_POOL_SIZE", "3" if is_pooler else "5"))
    max_overflow = int(os.environ.get("DB_MAX_OVERFLOW", "2" if is_pooler else "5"))

    # Optional: NullPool if you want every checkout = new connection (rare)
    if os.environ.get("DB_NULL_POOL", "").lower() in ("1", "true", "yes"):
        engine_kwargs["poolclass"] = NullPool
    else:
        engine_kwargs["poolclass"] = QueuePool
        engine_kwargs["pool_size"] = pool_size
        engine_kwargs["max_overflow"] = max_overflow
        engine_kwargs["pool_recycle"] = int(os.environ.get("DB_POOL_RECYCLE", "280"))  # < 5 min Supabase idle

    # TCP keepalives help long-lived session pooler links on Fly
    connect_args.update(
        {
            "connect_timeout": int(os.environ.get("DB_CONNECT_TIMEOUT", "15")),
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 5,
        }
    )

engine = create_engine(DATABASE_URL, connect_args=connect_args, **engine_kwargs)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _is_postgres() -> bool:
    return DATABASE_URL.startswith("postgresql")


def init_db():
    """Create all tables. Safe to call on every startup (idempotent)."""
    from app import models  # noqa: F401 — register models

    logger.info(
        "DB init — backend=%s pooler=%s",
        "postgresql" if _is_postgres() else "sqlite",
        "pooler" in DATABASE_URL or "supabase" in DATABASE_URL,
    )
    Base.metadata.create_all(bind=engine)
    _run_light_migrations()


def _run_light_migrations():
    """Add missing columns without Alembic for simple deploys."""
    if not (DATABASE_URL.startswith("sqlite") or _is_postgres()):
        return
    with engine.connect() as conn:
        try:
            if DATABASE_URL.startswith("sqlite"):
                cols = {
                    row[1]
                    for row in conn.execute(text("PRAGMA table_info(settings)")).fetchall()
                }
            else:
                cols = {
                    row[0]
                    for row in conn.execute(
                        text(
                            "SELECT column_name FROM information_schema.columns "
                            "WHERE table_schema = 'public' AND table_name = 'settings'"
                        )
                    ).fetchall()
                }
            additions = {
                "order_type": "VARCHAR(8) DEFAULT 'FAK'",
                "position_size_usd": "FLOAT DEFAULT 1.0",
                "scale_entries": "INTEGER DEFAULT 1",
                "shadow_balance": "FLOAT DEFAULT 50.0",
                "mode": "VARCHAR(16) DEFAULT 'shadow'",
                "max_contract_price": "FLOAT DEFAULT 0.95",
                "enabled_15m": "BOOLEAN DEFAULT TRUE",
                "enabled_5m": "BOOLEAN DEFAULT TRUE",
                "enabled_1h": "BOOLEAN DEFAULT FALSE",
                "enabled_btc": "BOOLEAN DEFAULT TRUE",
                "enabled_eth": "BOOLEAN DEFAULT FALSE",
                "live_day_start_pusd": "FLOAT",
                "live_day_start_date": "VARCHAR(16)",
                "live_all_time_start_pusd": "FLOAT",
            }
            for col, typedef in additions.items():
                if col not in cols:
                    logger.info("Migrating: ADD COLUMN settings.%s", col)
                    conn.execute(text(f"ALTER TABLE settings ADD COLUMN {col} {typedef}"))
            conn.commit()
        except Exception as e:
            logger.debug("light migration settings skip/err: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass

        # signals unique constraint helpers — ignore if already present
        try:
            if _is_postgres():
                conn.execute(
                    text(
                        "CREATE UNIQUE INDEX IF NOT EXISTS uq_signal_pair_tf_time "
                        "ON signals (pair, timeframe, signal_time)"
                    )
                )
                conn.commit()
        except Exception as e:
            logger.debug("signal index migration: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass

        # signals.user_id for multi-user (optional)
        try:
            if DATABASE_URL.startswith("sqlite"):
                sig_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(signals)")).fetchall()}
            else:
                sig_cols = {
                    row[0]
                    for row in conn.execute(
                        text(
                            "SELECT column_name FROM information_schema.columns "
                            "WHERE table_schema='public' AND table_name='signals'"
                        )
                    ).fetchall()
                }
            if "user_id" not in sig_cols:
                logger.info("Migrating: ADD COLUMN signals.user_id")
                conn.execute(text("ALTER TABLE signals ADD COLUMN user_id INTEGER"))
                conn.commit()
        except Exception as e:
            logger.debug("signals.user_id migration: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass

        # shadow_ledger.user_id
        try:
            if DATABASE_URL.startswith("sqlite"):
                led_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(shadow_ledger)")).fetchall()}
            else:
                led_cols = {
                    row[0]
                    for row in conn.execute(
                        text(
                            "SELECT column_name FROM information_schema.columns "
                            "WHERE table_schema='public' AND table_name='shadow_ledger'"
                        )
                    ).fetchall()
                }
            if "user_id" not in led_cols:
                logger.info("Migrating: ADD COLUMN shadow_ledger.user_id")
                conn.execute(text("ALTER TABLE shadow_ledger ADD COLUMN user_id INTEGER"))
                conn.commit()
        except Exception as e:
            logger.debug("shadow_ledger.user_id migration: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass



        # users.password_hash must be nullable (pending join has no password yet)
        try:
            if DATABASE_URL.startswith("sqlite"):
                info = conn.execute(text("PRAGMA table_info(users)")).fetchall()
                # row: cid, name, type, notnull, dflt_value, pk
                ph = next((r for r in info if r[1] == "password_hash"), None)
                if ph is not None and int(ph[3] or 0) == 1:
                    logger.info("Migrating: rebuild users table so password_hash is nullable")
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS users_new (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            display_name VARCHAR(128) NOT NULL,
                            username VARCHAR(64) NOT NULL UNIQUE,
                            password_hash VARCHAR(256),
                            role VARCHAR(16) DEFAULT 'user',
                            status VARCHAR(16) DEFAULT 'pending',
                            access_from DATETIME,
                            access_to DATETIME,
                            notes TEXT,
                            created_at DATETIME,
                            last_login_at DATETIME
                        )
                    """))
                    conn.execute(text("""
                        INSERT OR IGNORE INTO users_new
                        (id, display_name, username, password_hash, role, status,
                         access_from, access_to, notes, created_at, last_login_at)
                        SELECT id, display_name, username, password_hash, role, status,
                               access_from, access_to, notes, created_at, last_login_at
                        FROM users
                    """))
                    conn.execute(text("DROP TABLE users"))
                    conn.execute(text("ALTER TABLE users_new RENAME TO users"))
                    conn.execute(text("CREATE INDEX IF NOT EXISTS ix_users_status ON users (status)"))
                    conn.execute(text("CREATE INDEX IF NOT EXISTS ix_users_username ON users (username)"))
                    conn.commit()
            else:
                # Postgres
                conn.execute(text("ALTER TABLE users ALTER COLUMN password_hash DROP NOT NULL"))
                conn.commit()
        except Exception as e:
            logger.debug("users password_hash nullable migration: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass

        # user_settings columns (per-user trading prefs)
        try:
            if DATABASE_URL.startswith("sqlite"):
                us_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(user_settings)")).fetchall()}
            else:
                us_cols = {
                    row[0]
                    for row in conn.execute(
                        text(
                            "SELECT column_name FROM information_schema.columns "
                            "WHERE table_schema='public' AND table_name='user_settings'"
                        )
                    ).fetchall()
                }
            us_additions = {
                "mode": "VARCHAR(16) DEFAULT 'shadow'",
                "order_type": "VARCHAR(8) DEFAULT 'FAK'",
                "position_size_usd": "FLOAT DEFAULT 1.0",
                "scale_entries": "INTEGER DEFAULT 1",
                "shadow_balance": "FLOAT DEFAULT 50.0",
                "max_contract_price": "FLOAT DEFAULT 0.95",
                "enabled_btc": "BOOLEAN DEFAULT 1",
                "enabled_eth": "BOOLEAN DEFAULT 0",
                "auto_trade": "BOOLEAN DEFAULT 1",
            }
            if us_cols:  # table exists
                for col, typedef in us_additions.items():
                    if col not in us_cols:
                        logger.info("Migrating: ADD COLUMN user_settings.%s", col)
                        conn.execute(text(f"ALTER TABLE user_settings ADD COLUMN {col} {typedef}"))
                conn.commit()
        except Exception as e:
            logger.debug("user_settings migration: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass

        # user_credentials.private_key_enc for independent live fills
        try:
            if DATABASE_URL.startswith("sqlite"):
                uc_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(user_credentials)")).fetchall()}
            else:
                uc_cols = {
                    row[0]
                    for row in conn.execute(
                        text(
                            "SELECT column_name FROM information_schema.columns "
                            "WHERE table_schema='public' AND table_name='user_credentials'"
                        )
                    ).fetchall()
                }
            if "private_key_enc" not in uc_cols:
                logger.info("Migrating: ADD COLUMN user_credentials.private_key_enc")
                conn.execute(text("ALTER TABLE user_credentials ADD COLUMN private_key_enc TEXT"))
                conn.commit()
        except Exception as e:
            logger.debug("user_credentials.private_key_enc migration: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass


        # user_credentials session-key columns (safer multi-user path)
        try:
            if DATABASE_URL.startswith("sqlite"):
                uc_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(user_credentials)")).fetchall()}
            else:
                uc_cols = {
                    row[0]
                    for row in conn.execute(
                        text(
                            "SELECT column_name FROM information_schema.columns "
                            "WHERE table_schema='public' AND table_name='user_credentials'"
                        )
                    ).fetchall()
                }
            for col, typedef in (
                ("session_key_enc", "TEXT"),
                ("session_address", "VARCHAR(64)"),
                ("session_authorized", "BOOLEAN DEFAULT FALSE"),
            ):
                if col not in uc_cols:
                    logger.info("Migrating: ADD COLUMN user_credentials.%s", col)
                    conn.execute(text(f"ALTER TABLE user_credentials ADD COLUMN {col} {typedef}"))
            conn.commit()
        except Exception as e:
            logger.debug("user_credentials session migration: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass

        # user_trades.entry_price
        try:
            if DATABASE_URL.startswith("sqlite"):
                ut_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(user_trades)")).fetchall()}
            else:
                ut_cols = {
                    row[0]
                    for row in conn.execute(
                        text(
                            "SELECT column_name FROM information_schema.columns "
                            "WHERE table_schema='public' AND table_name='user_trades'"
                        )
                    ).fetchall()
                }
            if "entry_price" not in ut_cols:
                logger.info("Migrating: ADD COLUMN user_trades.entry_price")
                conn.execute(text("ALTER TABLE user_trades ADD COLUMN entry_price FLOAT"))
                conn.commit()
            if "signal_price" not in ut_cols:
                logger.info("Migrating: ADD COLUMN user_trades.signal_price")
                conn.execute(text("ALTER TABLE user_trades ADD COLUMN signal_price FLOAT"))
                conn.commit()
        except Exception as e:
            logger.debug("user_trades.entry_price/signal_price migration: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass

        # signals.signal_price (price at signal open)
        try:
            if DATABASE_URL.startswith("sqlite"):
                sig_cols2 = {row[1] for row in conn.execute(text("PRAGMA table_info(signals)")).fetchall()}
            else:
                sig_cols2 = {
                    row[0]
                    for row in conn.execute(
                        text(
                            "SELECT column_name FROM information_schema.columns "
                            "WHERE table_schema='public' AND table_name='signals'"
                        )
                    ).fetchall()
                }
            if "signal_price" not in sig_cols2:
                logger.info("Migrating: ADD COLUMN signals.signal_price")
                conn.execute(text("ALTER TABLE signals ADD COLUMN signal_price FLOAT"))
                conn.commit()
        except Exception as e:
            logger.debug("signals.signal_price migration: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass


        # signals.strategy_id (S3x vs Combined filter)
        try:
            if DATABASE_URL.startswith("sqlite"):
                sig_cols3 = {row[1] for row in conn.execute(text("PRAGMA table_info(signals)")).fetchall()}
            else:
                sig_cols3 = {
                    row[0]
                    for row in conn.execute(
                        text(
                            "SELECT column_name FROM information_schema.columns "
                            "WHERE table_schema='public' AND table_name='signals'"
                        )
                    ).fetchall()
                }
            if "strategy_id" not in sig_cols3:
                logger.info("Migrating: ADD COLUMN signals.strategy_id")
                conn.execute(text("ALTER TABLE signals ADD COLUMN strategy_id VARCHAR(64)"))
                conn.commit()
        except Exception as e:
            logger.debug("signals.strategy_id migration: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass


        # user_trades.strategy_id (S3x vs Combined filter)
        try:
            if DATABASE_URL.startswith("sqlite"):
                ut_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(user_trades)")).fetchall()}
            else:
                ut_cols = {
                    row[0]
                    for row in conn.execute(
                        text(
                            "SELECT column_name FROM information_schema.columns "
                            "WHERE table_schema='public' AND table_name='user_trades'"
                        )
                    ).fetchall()
                }
            if "strategy_id" not in ut_cols:
                logger.info("Migrating: ADD COLUMN user_trades.strategy_id")
                conn.execute(text("ALTER TABLE user_trades ADD COLUMN strategy_id VARCHAR(64)"))
                conn.commit()
        except Exception as e:
            logger.debug("user_trades.strategy_id migration: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass

        # user_credentials managed Builder wallet columns
        try:
            if DATABASE_URL.startswith("sqlite"):
                uc_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(user_credentials)")).fetchall()}
            else:
                uc_cols = {
                    row[0]
                    for row in conn.execute(
                        text(
                            "SELECT column_name FROM information_schema.columns "
                            "WHERE table_schema='public' AND table_name='user_credentials'"
                        )
                    ).fetchall()
                }
            for col, typedef in (
                ("owner_key_enc", "TEXT"),
                ("owner_address", "VARCHAR(64)"),
                ("wallet_deployed", "BOOLEAN DEFAULT FALSE"),
                ("builder_managed", "BOOLEAN DEFAULT FALSE"),
                ("builder_auth_error", "TEXT"),
            ):
                if col not in uc_cols:
                    logger.info("Migrating: ADD COLUMN user_credentials.%s", col)
                    conn.execute(text(f"ALTER TABLE user_credentials ADD COLUMN {col} {typedef}"))
            conn.commit()
        except Exception as e:
            logger.debug("user_credentials managed wallet migration: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass
