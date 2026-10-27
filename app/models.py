"""SQLAlchemy models — single-tenant settings + multi-user whitelist."""
from __future__ import annotations

from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, Float, Boolean, DateTime, Text, Index, UniqueConstraint
)
from app.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class Setting(Base):
    """System / operator bot settings (original single-tenant row)."""
    __tablename__ = "settings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    mode = Column(String(16), default="shadow")  # shadow | live
    order_type = Column(String(8), default="FAK")  # FAK | GTC
    position_size_usd = Column(Float, default=1.0)  # $1 per execution
    scale_entries = Column(Integer, default=1)  # 1 or 5 — how many $1 fills per window
    shadow_balance = Column(Float, default=50.0)
    max_contract_price = Column(Float, default=0.95)  # late-favorite ceiling
    enabled_15m = Column(Boolean, default=True)
    enabled_5m = Column(Boolean, default=True)
    enabled_1h = Column(Boolean, default=False)
    enabled_btc = Column(Boolean, default=True)
    enabled_eth = Column(Boolean, default=False)  # strategy is BTC-only
    # On-chain daily PnL baseline (UTC day)
    live_day_start_pusd = Column(Float, nullable=True)
    live_day_start_date = Column(String(16), nullable=True)  # YYYY-MM-DD UTC
    live_all_time_start_pusd = Column(Float, nullable=True)  # first seen on-chain baseline
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class User(Base):
    """
    Whitelisted account.
    Only active users inside access_from → access_to may use the bot.
    """
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    display_name = Column(String(128), nullable=False)  # admin label
    username = Column(String(64), nullable=False, unique=True)
    password_hash = Column(String(256), nullable=True)  # null until user sets after approval
    role = Column(String(16), default="user")  # admin | user
    status = Column(String(16), default="pending")  # pending | active | suspended | expired
    access_from = Column(DateTime, nullable=True)  # UTC; null = immediate
    access_to = Column(DateTime, nullable=True)  # UTC; null = no expiry
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=utcnow)
    last_login_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_users_status", "status"),
        Index("ix_users_username", "username"),
    )


class UserSetting(Base):
    """Per-user trading preferences."""
    __tablename__ = "user_settings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False, unique=True)
    mode = Column(String(16), default="shadow")
    order_type = Column(String(8), default="FAK")
    position_size_usd = Column(Float, default=1.0)  # $1 per execution
    scale_entries = Column(Integer, default=1)  # 1 or 5
    shadow_balance = Column(Float, default=50.0)
    max_contract_price = Column(Float, default=0.95)  # late-favorite ceiling
    enabled_btc = Column(Boolean, default=True)
    enabled_eth = Column(Boolean, default=False)  # strategy is BTC-only
    auto_trade = Column(Boolean, default=True)  # follow bot signals
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class UserCredential(Base):
    """
    Per-user Polymarket trading credentials.

    Managed path (Builder):
      owner_key_enc  — deposit-wallet owner EOA (encrypted; user can export)
      owner_address  — owner EOA address
      funder_address — deposit wallet (deployed via Builder for this owner)
      session_key_enc / session_address — trade-only session signer
      session_authorized — True only after Builder/relayer authorize succeeds
      wallet_deployed / builder_managed — deposit created by this bot's Builder

    Admin/operator never uses this table for live orders (uses .env only).
    """
    __tablename__ = "user_credentials"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False, unique=True)
    api_key = Column(String(128), nullable=True)
    api_secret_enc = Column(Text, nullable=True)
    passphrase_enc = Column(Text, nullable=True)
    # Session key (Polymarket safer path) — trade-only EOA, NOT withdraw key
    session_key_enc = Column(Text, nullable=True)
    session_address = Column(String(64), nullable=True)
    session_authorized = Column(Boolean, default=False)
    funder_address = Column(String(64), nullable=True)  # deposit wallet
    signature_type = Column(Integer, default=3)  # 3 = deposit+session
    has_credentials = Column(Boolean, default=False)
    # Managed Builder path
    owner_key_enc = Column(Text, nullable=True)
    owner_address = Column(String(64), nullable=True)
    wallet_deployed = Column(Boolean, default=False)
    builder_managed = Column(Boolean, default=False)
    builder_auth_error = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class Signal(Base):
    """Global strategy signals (and legacy operator trades)."""
    __tablename__ = "signals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pair = Column(String(16), nullable=False)
    timeframe = Column(String(8), nullable=False)
    direction = Column(String(8), nullable=False)
    signal_time = Column(DateTime, nullable=False)
    entry_time = Column(DateTime, nullable=False)
    candle_close_time = Column(DateTime, nullable=False)
    rsi = Column(Float, nullable=True)  # legacy unused; prefer signal_price
    mode = Column(String(16), default="shadow")
    order_type = Column(String(8), default="FAK")
    position_size_usd = Column(Float, default=1.0)  # $1 per execution
    status = Column(String(16), default="pending")
    result = Column(String(8), nullable=True)
    pnl = Column(Float, default=0.0)
    entry_price = Column(Float, nullable=True)
    signal_price = Column(Float, nullable=True)  # price at signal open (median at decision)
    polymarket_order_id = Column(String(128), nullable=True)
    polymarket_slug = Column(String(128), nullable=True)
    condition_id = Column(String(128), nullable=True)
    token_id = Column(String(128), nullable=True)
    error_message = Column(Text, nullable=True)
    strategy_id = Column(String(64), nullable=True)  # l5_value_lock_5m | l5_value_lock_5m | …
    resolved_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=utcnow)
    # null = system/operator trade; set for multi-user rows if used on this table
    user_id = Column(Integer, nullable=True)

    __table_args__ = (
        Index("ix_signals_entry_time", "entry_time"),
        Index("ix_signals_status", "status"),
        Index("ix_signals_timeframe", "timeframe"),
        Index("ix_signals_strategy_id", "strategy_id"),
        Index("ix_signals_result", "result"),
        Index("ix_signals_user_id", "user_id"),
        Index("uq_signal_pair_tf_time", "pair", "timeframe", "signal_time", unique=True),
    )


class UserTrade(Base):
    """Per-user execution of a global strategy signal."""
    __tablename__ = "user_trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    signal_id = Column(Integer, nullable=True)  # link to global Signal if present
    pair = Column(String(16), nullable=False)
    timeframe = Column(String(8), nullable=False)
    direction = Column(String(8), nullable=False)
    signal_time = Column(DateTime, nullable=False)
    entry_time = Column(DateTime, nullable=False)
    candle_close_time = Column(DateTime, nullable=False)
    mode = Column(String(16), default="shadow")
    position_size_usd = Column(Float, default=1.0)  # $1 per execution
    status = Column(String(16), default="pending")
    result = Column(String(8), nullable=True)
    pnl = Column(Float, default=0.0)
    entry_price = Column(Float, nullable=True)
    signal_price = Column(Float, nullable=True)  # price at signal open
    polymarket_order_id = Column(String(128), nullable=True)
    polymarket_slug = Column(String(128), nullable=True)
    error_message = Column(Text, nullable=True)
    strategy_id = Column(String(64), nullable=True)  # l5_value_lock_5m | l5_value_lock_5m | …
    resolved_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=utcnow)

    __table_args__ = (
        UniqueConstraint("user_id", "pair", "timeframe", "signal_time", name="uq_user_trade_once"),
        Index("ix_user_trades_user", "user_id"),
        Index("ix_user_trades_status", "status"),
        Index("ix_user_trades_entry", "entry_time"),
    )


class ShadowLedger(Base):
    """Shadow balance changes (system or per-user)."""
    __tablename__ = "shadow_ledger"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=True)  # null = system settings balance
    signal_id = Column(Integer, nullable=True)
    change = Column(Float, nullable=False)
    balance_after = Column(Float, nullable=False)
    note = Column(String(256), nullable=True)
    created_at = Column(DateTime, default=utcnow)
