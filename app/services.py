"""Business logic: signal generation, execution, resolution, stats."""
from __future__ import annotations

import logging
import os
import threading
import time as _time_mod
from datetime import datetime, timezone, timedelta
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func

from app.models import Setting, Signal, ShadowLedger
from app import strategy_l5 as strategy, polymarket
from app import telegram_notify as tg

logger = logging.getLogger(__name__)

def _strategy_for_tf(timeframe: str):
    """Route timeframe → strategy module. Only L5 Value Lock (5m) is active."""
    return strategy

def _window_sec_for_tf(timeframe: str) -> int:
    return int(getattr(_strategy_for_tf(timeframe), "WINDOW_SEC", 900))

def _tf_execution_enabled(settings, timeframe: str) -> bool:
    """Toggles control fills only — signals always record.
    15m strategy is disabled system-wide; only 5m strategies execute.
    """
    tf = (timeframe or "15m").lower()
    if tf == "5m":
        return bool(getattr(settings, "enabled_5m", True))
    if tf == "15m":
        return False  # strategy.py disabled system-wide
    if tf == "1h":
        return False
    return True



def _strategy_id_from_item(item: dict) -> str:
    """Normalize strategy identity for history filtering."""
    sid = (item.get("strategy_id") or item.get("strategy") or "").strip()
    if sid:
        return sid
    leg = (item.get("leg") or "").strip()
    if leg.startswith("l5") or "value_lock" in leg:
        return "l5_value_lock_5m"
    tf = (item.get("timeframe") or "").lower()
    if tf == "5m":
        return "l5_value_lock_5m"
    return leg or "l5_value_lock_5m"


def _normalize_pair_code(pair: str) -> str:
    """BTCUSDT / BTC -> BTC; ETHUSDT / ETH -> ETH."""
    p = (pair or "").upper().replace("USDT", "").replace("USD", "").strip()
    return p


def _pair_is_watchable(pair: str) -> bool:
    return _normalize_pair_code(pair) in ("BTC", "ETH")


def _signal_window_ts(sig) -> int:
    tf = (getattr(sig, "timeframe", None) or "15m").lower()
    wsec = _window_sec_for_tf(tf)
    et = sig.entry_time or sig.signal_time
    if et is None:
        return int(time.time()) - (int(time.time()) % wsec)
    if getattr(et, "tzinfo", None) is None:
        et = et.replace(tzinfo=timezone.utc)
    ts = int(et.timestamp())
    return ts - (ts % wsec)

def _parse_max_price_from_signal(sig, direction: str) -> float:
    import re as _re
    msg = sig.error_message or ""
    m = _re.search(r"cap=([0-9.]+)", msg)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    try:
        mp = getattr(sig, "max_price", None)
        if mp is not None and float(mp) > 0:
            return float(mp)
    except Exception:
        pass
    mod = _strategy_for_tf(getattr(sig, "timeframe", None) or "15m")
    key = "DOWN_ENTRY_TH" if (direction or "").upper() == "DOWN" else "UP_ENTRY_TH"
    return float(getattr(mod, key, getattr(mod, "FILL_ENTRY_TH", 0.52)))

def _entry_deadline_ts(sig, window_ts: int) -> int:
    return int(window_ts) + _window_sec_for_tf(getattr(sig, "timeframe", None) or "15m")



def _normalize_order_error(err: str) -> str:
    """Shorten noisy CLOB errors for status/error_message."""
    e = err or ""
    el = e.lower()
    if "no orders found to match with fak" in el or "no match is found" in el:
        return "no FAK match at ≤cap (book empty or ask above max) — still watching"
    if "no discount liquidity" in el:
        return e.split(":")[0] + (":" + e.split(":")[1] if ":" in e else "")
    if len(e) > 220:
        return e[:220] + "…"
    return e

def _is_recoverable_order_error(err: str) -> bool:
    """True = keep watching for discount; False = hard fail (auth/wallet)."""
    e = (err or "").lower()
    if not e:
        return True
    # Strategy: wait for better price
    if "no discount" in e:
        return True
    if "no orders found to match with fak" in e or "fak order" in e:
        return True  # liquidity gap — brief retry ok
    if "watching" in e:
        return True
    # Hard wallet / auth — retrying will not help
    if "maker address not allowed" in e:
        return False
    if "signer address has to be the address of the api key" in e:
        return False
    if "invalid signature" in e or "invalid_signature" in e:
        return False
    if "not enough balance" in e or "insufficient" in e:
        return False
    if "session" in e and ("authoriz" in e or "not allowed" in e):
        return False
    return True  # unknown → retry within window


def _user_live_error_hint(err: str) -> str:
    e = (err or "").lower()
    if "maker address not allowed" in e:
        return (
            "Deposit wallet rejected this order. On Polymarket, authorize THIS bot session key "
            "for your deposit wallet (CLOB scope), then Mark authorized in Account. "
            "Builder API must be configured on the server for auto-authorize."
        )
    if "signer address has to be the address of the api key" in e:
        return (
            "Order signer / API key mismatch on deposit wallet path. "
            "Rotate session key, authorize on Polymarket, Mark authorized, retry."
        )
    return err or "order failed"



def _as_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Normalize DB/Python datetimes to timezone-aware UTC for safe comparison."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


_live_bal_cache = {"data": None, "ts": 0.0}
_LIVE_BAL_TTL = 120.0
_live_trades_cache = {"data": None, "ts": 0.0}
_LIVE_TRADES_TTL = 180.0
_stats_cache = {"data": None, "ts": 0.0, "skip_live": None}
_creds_cache = {"data": None, "ts": 0.0}
_CREDS_TTL = 60.0
_live_refresh_lock = threading.Lock()
_live_refreshing = False

_STATS_TTL = 12.0
_STATS_TTL_LIGHT = 30.0
_process_lock = threading.Lock()
_resolve_lock = threading.Lock()

# Wall-clock of this process start. Redeploy must NOT open signals whose
# entry window began before this boot (avoids re-firing mid-candle positions).
PROCESS_STARTED_AT = datetime.now(timezone.utc)




def _cached_credentials():
    """Re-read env every time for key presence; only soft-cache full status briefly when live_ready."""
    try:
        data = polymarket.credentials_status()
    except Exception as e:
        data = {"live_ready": False, "private_key_set": False, "funder": None, "error": str(e)}
    # Never stick a "not ready" result in cache for 60s — env may have been fixed
    if data.get("live_ready"):
        _creds_cache["data"] = data
        _creds_cache["ts"] = _time_mod.time()
    else:
        _creds_cache["data"] = None
        _creds_cache["ts"] = 0.0
    return data


def refresh_live_caches_async(db_factory=None):
    """Background refresh of on-chain + CLOB caches (never block HTTP)."""
    global _live_refreshing
    if _live_refreshing:
        return
    if not _live_refresh_lock.acquire(blocking=False):
        return
    try:
        if _live_refreshing:
            return
        _live_refreshing = True
    finally:
        _live_refresh_lock.release()

    def _run():
        global _live_refreshing
        try:
            creds = _cached_credentials()
            if not creds.get("live_ready"):
                return
            try:
                bal = polymarket.get_onchain_balance()
                _live_bal_cache["data"] = bal
                _live_bal_cache["ts"] = _time_mod.time()
            except Exception as e:
                logger.warning("bg live balance failed: %s", e)
            try:
                from datetime import datetime, timezone, timedelta
                day_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                act = polymarket.get_user_trades_today(day_start_ts=int(day_start.timestamp()))
                _live_trades_cache["data"] = act
                _live_trades_cache["ts"] = _time_mod.time()
                # lightweight all-time: last 14 days CLOB window (avoid huge pulls)
                start_ts = int((day_start - timedelta(days=14)).timestamp())
                at = polymarket.get_user_trades(
                    start_ts=start_ts,
                    end_ts=int(_time_mod.time()) + 60,
                    limit=2000,
                )
                if at.get("success"):
                    _live_trades_cache["all_time"] = float(at.get("realized_pnl") or 0.0)
                    _live_trades_cache["all_time_ts"] = _time_mod.time()
                    _live_trades_cache["all_time_start_ts"] = start_ts
            except Exception as e:
                logger.warning("bg live trades failed: %s", e)
            # Invalidate stats so next light read can merge new live fields
            _stats_cache["data"] = None
        finally:
            _live_refreshing = False

    threading.Thread(target=_run, name="live-cache-refresh", daemon=True).start()


def get_or_create_settings(db: Session) -> Setting:
    s = db.query(Setting).first()
    if not s:
        s = Setting()
        s.max_contract_price = 0.95
        s.enabled_eth = False
        s.enabled_btc = True
        db.add(s)
        db.commit()
        db.refresh(s)
        return s
    # Align legacy rows with late-window favorite (backtest ceiling)
    changed = False
    if s.max_contract_price is None or float(s.max_contract_price) < 0.95:
        s.max_contract_price = 0.95
        changed = True
    if getattr(s, "enabled_eth", False):
        s.enabled_eth = False
        changed = True
    if changed:
        try:
            db.commit()
            db.refresh(s)
        except Exception:
            db.rollback()
    return s


def update_settings(db: Session, **kwargs) -> Setting:
    s = get_or_create_settings(db)
    for k, v in kwargs.items():
        if hasattr(s, k) and v is not None:
            setattr(s, k, v)
    s.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(s)
    _stats_cache["data"] = None  # invalidate
    return s


def reset_shadow_balance(db: Session, amount: float = 50.0) -> Setting:
    s = get_or_create_settings(db)
    prev = float(s.shadow_balance or 0)
    s.shadow_balance = amount
    db.add(
        ShadowLedger(
            signal_id=None,
            change=amount - prev,
            balance_after=amount,
            note="Balance reset",
        )
    )
    db.commit()
    db.refresh(s)
    _stats_cache["data"] = None
    return s


def _already_recorded(
    db: Session,
    pair: str,
    timeframe: str,
    signal_time: datetime,
    entry_time: datetime | None = None,
) -> bool:
    """
    True if ANY row exists for this bar (including error).
    Prevents re-opening the same BTC/ETH 1h market on every deploy/restart.
    """
    q = db.query(Signal).filter(
        Signal.pair == pair,
        Signal.timeframe == timeframe,
        Signal.signal_time == signal_time,
    )
    if db.query(q.exists()).scalar():
        return True
    if entry_time is not None:
        q2 = db.query(Signal).filter(
            Signal.pair == pair,
            Signal.timeframe == timeframe,
            Signal.entry_time == entry_time,
        )
        if db.query(q2.exists()).scalar():
            return True
    return False


def _count_fills(sig: Signal) -> int:
    oid = getattr(sig, "polymarket_order_id", None) or ""
    if not oid.strip():
        return 0
    return len([x for x in oid.split(",") if x.strip()])





def retry_watching_signals(db: Session) -> list:
    """Poll book; fill when best ask <= TF cap. Correct TF market + window floor."""
    import time as _time
    settings = get_or_create_settings(db)
    _se = int(getattr(settings, "scale_entries", 1) or 1)
    if _se not in (1, 5):
        _se = 1
    max_entries = _se
    stake = float(getattr(settings, "position_size_usd", 1.0) or 1.0)
    mode = (settings.mode or "shadow").lower()
    order_type = (settings.order_type or "FAK").upper()
    now = datetime.now(timezone.utc)
    now_ts = now.timestamp()
    watching = (
        db.query(Signal)
        .filter(Signal.status == "watching")
        .all()
    )
    watching = [s for s in watching if _pair_is_watchable(getattr(s, "pair", "") or "")]
    updated = []
    for sig in watching:
        if not _tf_execution_enabled(settings, getattr(sig, "timeframe", None) or "15m"):
            if "execution disabled" not in (sig.error_message or ""):
                sig.error_message = ((sig.error_message or "") + " | execution disabled in settings").strip(" |")
                try:
                    db.commit()
                except Exception:
                    db.rollback()
            updated.append(sig)
            continue
        et = sig.entry_time or sig.signal_time
        if et is not None and getattr(et, "tzinfo", None) is None:
            et = et.replace(tzinfo=timezone.utc)
        if et is None:
            continue
        if et <= PROCESS_STARTED_AT:
            sig.status = "error"
            sig.error_message = ((sig.error_message or "") + " | abandoned on redeploy").strip(" |")
            try:
                db.commit()
            except Exception:
                db.rollback()
            updated.append(sig)
            continue
        tf = (getattr(sig, "timeframe", None) or "15m").lower()
        window_ts = _signal_window_ts(sig)
        deadline = _entry_deadline_ts(sig, window_ts)
        wsec = _window_sec_for_tf(tf)
        direction = (sig.direction or "DOWN").upper()
        max_px = _parse_max_price_from_signal(sig, direction)
        mod = _strategy_for_tf(tf)
        strat_cap = float(getattr(mod, "FILL_ENTRY_TH", getattr(mod, "UP_ENTRY_TH", max_px)))
        max_px = min(max_px, strat_cap)
        filled = _count_fills(sig)
        if filled >= max_entries:
            sig.status = "pending"
            sig.error_message = None
            db.commit()
            updated.append(sig)
            continue
        if now_ts >= deadline:
            if filled > 0:
                sig.status = "pending"
                sig.error_message = None
            else:
                sig.status = "error"
                sig.error_message = f"cap={max_px:.2f}|entry window closed (t+{wsec}s) with 0 fills"
            db.commit()
            updated.append(sig)
            continue
        remaining = max_entries - filled
        order_ids = [x for x in (sig.polymarket_order_id or "").split(",") if x.strip()]
        total_spent = float(sig.position_size_usd or 0.0) if filled > 0 else 0.0
        last_price = sig.entry_price
        last_error = None
        for attempt in range(5):
            if now_ts >= deadline or len(order_ids) >= max_entries or remaining <= 0:
                break
            try:
                result = polymarket.execute_order(
                    symbol="BTCUSDT" if (sig.pair or "BTC").upper().startswith("BTC") else "ETHUSDT",
                    direction=direction, mode=mode, position_size_usd=stake,
                    timeframe=tf, order_type=order_type, max_contract_price=max_px,
                    candle_open_ts=window_ts,
                )
            except Exception as e:
                last_error = str(e)
                logger.exception("[WATCH] execute failed id=%s", sig.id)
                break
            dbg = result.get("debug") or {}
            logger.info("[WATCH] id=%s attempt=%d tf=%s dir=%s cap=%.4f window=%s ok=%s ask=%s err=%s",
                        sig.id, attempt + 1, tf, direction, max_px, window_ts,
                        result.get("success"), dbg.get("best_ask"), result.get("error"))
            if result.get("success"):
                order_ids.append(str(result.get("order_id") or f"ok-{len(order_ids)}"))
                px = float(result.get("price") or result.get("price_per_contract") or 0) or None
                total_spent += float(result.get("total_spent") or stake)
                last_price = px
                if result.get("token_id") and not sig.token_id:
                    sig.token_id = result.get("token_id")
                if result.get("condition_id") and not sig.condition_id:
                    sig.condition_id = result.get("condition_id")
                remaining = max_entries - len(order_ids)
                logger.info("[WATCH] id=%s FILL %d/%d price=%.4f tf=%s", sig.id, len(order_ids), max_entries, px, tf)
                break
            last_error = result.get("error") or last_error
            if attempt + 1 < 5:
                _time.sleep(0.25)
        if order_ids:
            sig.polymarket_order_id = ",".join(order_ids)
            sig.entry_price = last_price
            sig.position_size_usd = total_spent if total_spent > 0 else stake
        new_filled = len(order_ids)
        if new_filled >= max_entries:
            sig.status = "pending"
            sig.error_message = None
            db.commit()
            try:
                tg.alert_signal_opened(sig)
            except Exception:
                pass
        elif new_filled > filled:
            sig.status = "watching"
            sig.error_message = f"cap={max_px:.2f}|partial {new_filled}/{max_entries}; watching until t+{wsec}s"
            db.commit()
        else:
            sig.error_message = f"cap={max_px:.2f}|watching ≤{max_px:.2f} until t+{wsec}s" + (f": {last_error}" if last_error else "")
            db.commit()
        updated.append(sig)
    return updated


def fan_out_user_trades(db: Session, sig: Signal) -> list:
    """
    Independently execute the same signal for every connected user with
    auto_trade + credentials. Each user uses their own keys/funder/size.
    """
    from app import multiuser
    from app.models import UserTrade
    from sqlalchemy.exc import IntegrityError

    if sig is None or (sig.status not in ("pending", "watching") and not sig.id):
        # still allow fan-out when operator filled or is watching
        pass
    if getattr(sig, "status", None) == "error" and not getattr(sig, "id", None):
        return []

    created = []
    try:
        targets = multiuser.list_autotrade_users(db)
    except Exception:
        logger.exception("[USER] list_autotrade_users failed")
        return []

    if not targets:
        return []

    direction = (sig.direction or "DOWN").upper()
    max_px = (
        float(getattr(strategy, "DOWN_ENTRY_TH", 0.93))
        if direction == "DOWN"
        else float(getattr(strategy, "UP_ENTRY_TH", 0.93))
    )
    pair = sig.pair or "BTC"
    symbol = "BTCUSDT" if pair.upper().startswith("BTC") else "ETHUSDT"

    for user, us, ctx in targets:
        # Skip duplicate
        exists = (
            db.query(UserTrade)
            .filter(
                UserTrade.user_id == user.id,
                UserTrade.pair == pair,
                UserTrade.timeframe == (sig.timeframe or "15m"),
                UserTrade.signal_time == sig.signal_time,
            )
            .first()
        )
        if exists:
            continue

        mode = (us.mode or "shadow").lower()
        # Live: authorized session OR managed owner key (Builder 403 fallback)
        if mode == "live" and not ctx.get("live_ready"):
            mode = "shadow"
            logger.info(
                "[USER] user=%s live not ready (need funder + session/owner) — shadow fill mode=%s",
                user.id, ctx.get("signer_mode"),
            )

        stake = float(us.position_size_usd or 1.0)
        se = int(getattr(us, "scale_entries", 1) or 1)
        if se not in (1, 5):
            se = 1
        order_type = (us.order_type or "FAK").upper()
        user_max = float(getattr(us, "max_contract_price", None) or 0)
        user_max = min(user_max, max_px)

        ok_risk, risk_reason = strategy.risk_allows_new_trade(db, user_id=user.id, stake_usd=stake)
        if not ok_risk:
            logger.info("[USER] skip user=%s risk gate: %s", user.id, risk_reason)
            continue

        ut = UserTrade(
            user_id=user.id,
            signal_id=sig.id,
            pair=pair,
            timeframe=sig.timeframe or "15m",
            direction=direction,
            signal_time=sig.signal_time,
            entry_time=sig.entry_time,
            candle_close_time=sig.candle_close_time,
            mode=mode,
            position_size_usd=stake * se,
            signal_price=getattr(sig, "signal_price", None),
            strategy_id=getattr(sig, "strategy_id", None),
            status="placing",
        )
        db.add(ut)
        try:
            db.commit()
            db.refresh(ut)
        except IntegrityError:
            db.rollback()
            continue
        except Exception:
            db.rollback()
            logger.exception("[USER] reserve UserTrade failed user=%s", user.id)
            continue

        filled = 0
        total_spent = 0.0
        order_ids = []
        last_err = None
        last_price = None
        # entry_time is the Polymarket 15m window open; signal_time is the prior bar
        _et = sig.entry_time or sig.signal_time
        if _et is not None and getattr(_et, 'tzinfo', None) is None:
            from datetime import timezone as _tz
            _et = _et.replace(tzinfo=_tz.utc)
        candle_ts = int(_et.timestamp()) if _et else None
        if sig.entry_time is None and sig.signal_time is not None and candle_ts:
            candle_ts += 900

        for i in range(se):
            try:
                result = polymarket.execute_order(
                    symbol=symbol,
                    direction=direction,
                    mode=mode,
                    position_size_usd=stake,
                    timeframe="15m",
                    order_type=order_type,
                    max_contract_price=user_max,
                    candle_open_ts=candle_ts,
                    user_creds=ctx,
                )
            except Exception as e:
                last_err = str(e)
                logger.exception("[USER] execute failed user=%s", user.id)
                break
            if result.get("success"):
                filled += 1
                px = float(result.get("price") or result.get("price_per_contract") or 0)
                if px <= 0:
                    last_error = "fill missing price from book"
                    logger.warning("[EXEC] success without price — treating as fail")
                    break
                spent = float(result.get("total_spent") or stake)
                total_spent += spent
                last_price = px
                if result.get("order_id"):
                    order_ids.append(str(result["order_id"]))
                if result.get("slug") and not ut.polymarket_slug:
                    ut.polymarket_slug = result.get("slug")
            else:
                last_err = result.get("error") or "unknown"
                logger.warning(
                    "[USER] live attempt failed user=%s signer=%s funder=%s err=%s",
                    user.id,
                    (ctx or {}).get("session_address") or (ctx or {}).get("funder"),
                    (ctx or {}).get("funder"),
                    last_err,
                )
                # stop scale-in on hard failure / no discount
                break

        if filled == 0:
            if _is_recoverable_order_error(last_err or "") and mode in ("live", "shadow"):
                ut.status = "watching"
                ut.error_message = (
                    f"watching ≤{user_max:.2f}: {last_err}" if last_err
                    else f"watching for discount ≤{user_max:.2f}"
                )
                ut.position_size_usd = 0.0
            else:
                ut.status = "error"
                ut.error_message = _user_live_error_hint(last_err or "no fill")
                ut.position_size_usd = 0.0
                logger.warning(
                    "[USER] hard fail user=%s — stop watching: %s",
                    user.id, (ut.error_message or "")[:160],
                )
        else:
            ut.status = "pending"
            ut.position_size_usd = total_spent if total_spent > 0 else stake * filled
            ut.polymarket_order_id = ",".join(order_ids) if order_ids else None
            ut.error_message = None
            # shadow balance per user
            if mode == "shadow":
                try:
                    us.shadow_balance = max(0.0, float(us.shadow_balance or 0.0) - float(ut.position_size_usd))
                except Exception:
                    pass
        try:
            db.commit()
            db.refresh(ut)
            created.append(ut)
            logger.info(
                "[USER] user=%s %s %s fills=%d/%d status=%s spent=%.2f",
                user.id, direction, pair, filled, se, ut.status, float(ut.position_size_usd or 0),
            )
        except Exception:
            db.rollback()
            logger.exception("[USER] commit UserTrade failed user=%s", user.id)

    return created



def retry_watching_user_trades(db: Session) -> list:
    """
    Per-user independent watching loop (parallel-safe).
    Each UserTrade in status=watching retries with THAT user's session key +
    funder + size, inside the same [open+LOOK, open+LOOK+MAX_ENTRY_DELAY] window.
    Operator watching is untouched (retry_watching_signals).
    """
    import time as _time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    # per-trade window length is resolved inside _one via timeframe
    ENTRY_DEADLINE_OFFSET = None  # set per trade
    from app import multiuser
    from app.models import UserTrade
    from app.database import SessionLocal

    now = datetime.now(timezone.utc)
    now_ts = now.timestamp()
    rows = (
        db.query(UserTrade)
        .filter(UserTrade.status == "watching")
        .order_by(UserTrade.id.asc())
        .limit(80)
        .all()
    )
    rows = [ut for ut in rows if _pair_is_watchable(getattr(ut, "pair", "") or "")]
    if not rows:
        return []

    def _one(ut_id: int) -> Optional[str]:
        local = SessionLocal()
        try:
            ut = local.query(UserTrade).filter(UserTrade.id == ut_id).first()
            if not ut or ut.status != "watching":
                return None
            # Polymarket window open = entry_time (signal_time is the prior 15m bar)
            et = ut.entry_time or ut.signal_time
            if et is not None and et.tzinfo is None:
                et = et.replace(tzinfo=timezone.utc)
            if et is None:
                ut.status = "error"
                ut.error_message = "watching: missing entry_time"
                local.commit()
                return f"ut={ut_id} no entry_time"
            if et <= PROCESS_STARTED_AT:
                ut.status = "error"
                ut.error_message = "abandoned on redeploy (window open at process start)"
                local.commit()
                return f"ut={ut_id} abandoned redeploy"
            window_ts = int(et.timestamp())
            if ut.entry_time is None and ut.signal_time is not None:
                window_ts += _window_sec_for_tf(getattr(ut, 'timeframe', None) or '15m')
            deadline = window_ts + _window_sec_for_tf(getattr(ut, 'timeframe', None) or '15m')
            us = multiuser.get_user_setting(local, ut.user_id)
            se = int(getattr(us, "scale_entries", 1) or 1)
            if se not in (1, 5):
                se = 1
            stake = float(us.position_size_usd or 1.0)
            mode = (us.mode or "shadow").lower()
            order_type = (us.order_type or "FAK").upper()
            direction = (ut.direction or "DOWN").upper()
            mod = _strategy_for_tf(getattr(ut, "timeframe", None) or "15m")
            max_px = float(
                getattr(mod, "DOWN_ENTRY_TH", getattr(mod, "FILL_ENTRY_TH", 0.52))
                if direction == "DOWN"
                else getattr(mod, "UP_ENTRY_TH", getattr(mod, "FILL_ENTRY_TH", 0.52))
            )
            user_max = float(getattr(us, "max_contract_price", None) or 0) or max_px
            max_px = min(user_max, max_px)

            # Count existing fills
            order_ids = [x for x in (ut.polymarket_order_id or "").split(",") if x.strip()]
            filled = len(order_ids)
            if filled >= se:
                ut.status = "pending"
                ut.error_message = None
                local.commit()
                return f"ut={ut_id} cap→pending"

            if now_ts >= deadline:
                if filled > 0:
                    ut.status = "pending"
                    ut.error_message = None
                else:
                    ut.status = "error"
                    ut.error_message = (
                        (ut.error_message or "") + " | entry window closed (t+285s) with 0 fills"
                    )
                local.commit()
                return f"ut={ut_id} window closed → {ut.status}"

            ctx = multiuser.get_user_trading_context(local, ut.user_id)
            if mode == "live" and not (ctx and ctx.get("live_ready")):
                mode = "shadow"
                logger.info(
                    "[USER-WATCH] ut=%s user=%s live not ready signer_mode=%s — shadow",
                    ut_id, ut.user_id, (ctx or {}).get("signer_mode"),
                )

            remaining = se - filled
            symbol = "BTCUSDT" if (ut.pair or "BTC").upper().startswith("BTC") else "ETHUSDT"
            candle_ts = window_ts
            total_spent = float(ut.position_size_usd or 0.0)
            last_err = None
            last_price = getattr(ut, 'entry_price', None)

            for _ in range(remaining):
                try:
                    result = polymarket.execute_order(
                        symbol=symbol,
                        direction=direction,
                        mode=mode,
                        position_size_usd=stake,
                        timeframe="15m",
                        order_type=order_type,
                        max_contract_price=user_max,
                        candle_open_ts=candle_ts,
                        user_creds=ctx if mode == "live" else None,
                    )
                except Exception as e:
                    last_err = str(e)
                    logger.exception("[USER-WATCH] ut=%s user=%s", ut_id, ut.user_id)
                    break
                if result.get("success"):
                    filled += 1
                    px = float(result.get("price") or result.get("price_per_contract") or 0) or None
                    spent = float(result.get("total_spent") or stake)
                    total_spent += spent
                    last_price = px
                    if result.get("order_id"):
                        order_ids.append(str(result["order_id"]))
                    if result.get("slug") and not ut.polymarket_slug:
                        ut.polymarket_slug = result.get("slug")
                    _time.sleep(0.3)
                else:
                    last_err = result.get("error") or "unknown"
                    break

            if order_ids:
                ut.polymarket_order_id = ",".join(order_ids)
                if hasattr(ut, 'entry_price'):
                    ut.entry_price = last_price
                ut.position_size_usd = total_spent
            if filled >= se:
                ut.status = "pending"
                ut.error_message = None
                if mode == "shadow" and total_spent > 0:
                    try:
                        us.shadow_balance = max(0.0, float(us.shadow_balance or 0.0) - float(total_spent))
                    except Exception:
                        pass
            elif filled > 0:
                ut.status = "watching"
                ut.error_message = f"partial {filled}/{se}; watching until t+{_window_sec_for_tf(getattr(ut, 'timeframe', None) or '15m')}s"
            else:
                if last_err and not _is_recoverable_order_error(last_err):
                    ut.status = "error"
                    ut.error_message = _user_live_error_hint(last_err)
                    logger.warning(
                        "[USER-WATCH] hard fail ut=%s user=%s — %s",
                        ut_id, ut.user_id, (ut.error_message or "")[:160],
                    )
                else:
                    ut.status = "watching"
                    ut.error_message = (
                        f"watching ≤{user_max:.2f}: {last_err}" if last_err
                        else f"watching for discount ≤{user_max:.2f}"
                    )
            local.commit()
            logger.info(
                "[USER-WATCH] ut=%s user=%s fills=%d/%d status=%s err=%s",
                ut_id, ut.user_id, filled, se, ut.status, (last_err or "")[:80],
            )
            return f"ut={ut_id} fills={filled}"
        except Exception as e:
            logger.exception("[USER-WATCH] worker failed ut=%s: %s", ut_id, e)
            try:
                local.rollback()
            except Exception:
                pass
            return f"ut={ut_id} fail:{e}"
        finally:
            local.close()

    ids = [r.id for r in rows]
    # Detach before parallel workers
    db.expunge_all()
    results = []
    workers = min(6, max(1, len(ids)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_one, i): i for i in ids}
        for fut in as_completed(futs):
            try:
                results.append(fut.result())
            except Exception as e:
                logger.exception("[USER-WATCH] future: %s", e)
    return [r for r in results if r]


def process_new_signals(db: Session) -> list:

    if not _process_lock.acquire(blocking=False):
        logger.info("Scan skipped — another scan is in progress")
        return []
    try:
        return _process_new_signals_inner(db)
    finally:
        _process_lock.release()



def _execute_one_signal(db: Session, settings, item: dict, seen_keys: set) -> object | None:
    """
    Optimized BTC 15m Down-bias strategy.
    Places up to MAX_ENTRIES ($1 each) on the Down token when bias is Down.
    """
    if "error" in item:
        logger.warning("scan error: %s", item["error"])
        return None
    tf = (item.get("timeframe") or "").lower()
    if tf not in ("15m", "5m"):
        return None
    # enabled_5m/15m do NOT block signal recording — only fills (see below).

    pair_u = (item.get("pair") or "").upper().replace("USDT", "")
    if pair_u not in ("BTC", "ETH"):
        return None
    if pair_u == "BTC" and not getattr(settings, "enabled_btc", True):
        logger.info("[EXEC] skip BTC — enabled_btc=False")
        return None
    if pair_u == "ETH" and not getattr(settings, "enabled_eth", False):
        logger.info("[EXEC] skip ETH — enabled_eth=False")
        return None

    key = (item["pair"], item["timeframe"], item["signal_time"].isoformat())
    if key in seen_keys:
        return None
    if _already_recorded(
        db, item["pair"], item["timeframe"], item["signal_time"], item.get("entry_time")
    ):
        logger.info(
            "[EXEC] skip duplicate %s %s signal_time=%s",
            item["pair"], item["timeframe"], item["signal_time"],
        )
        return None

    # Allow multiple concurrent open signals for the same pair/timeframe.
    # A new qualified signal is always recorded and may execute (fill) even if
    # another signal for this pair/TF is still pending / placing / watching.
    # Dedup is only by exact signal_time / entry_time (_already_recorded + unique constraint).
    now_utc = datetime.now(timezone.utc)

    # Boot / redeploy guard (L5 trades the running market whose open is already past):
    # Skip only if the *signal_time* itself is at or before process start.
    # This still blocks replay of signals that occurred before the process came up.
    sig_t = item.get("signal_time")
    if sig_t is not None:
        if sig_t.tzinfo is None:
            sig_t = sig_t.replace(tzinfo=timezone.utc)
        if sig_t <= PROCESS_STARTED_AT:
            logger.info(
                "[EXEC] skip pre-boot signal %s signal_time=%s boot=%s",
                item["pair"], sig_t, PROCESS_STARTED_AT,
            )
            return None

    mode = (settings.mode or "shadow").lower()
    order_type = (settings.order_type or "FAK").upper()
    max_px = float(
        item.get("max_price")
        or (
            getattr(strategy, "DOWN_ENTRY_TH", 0.80)
            if (item.get("direction") or "").upper() == "DOWN"
            else getattr(strategy, "UP_ENTRY_TH", 0.80)
        )
    )
    # scale_entries setting: 1 (single) or 5 (scale-in). Default 1.
    _se = int(getattr(settings, "scale_entries", 1) or 1)
    if _se not in (1, 5):
        _se = 1
    max_entries = int(item.get("max_entries") or _se)
    # Variable-stake strategy: prefer per-signal stake_usd (tiered); else settings
    # Tier stakes can be < $1 (0.25–1.00). Settings size acts as a multiplier scale.
    base = float(getattr(settings, "position_size_usd", 1.0) or 1.0)
    if item.get("stake_usd") is not None:
        tier = float(item["stake_usd"])
        # scale: settings 1.0 → use tier as-is; settings 5 → 5× tier
        stake = max(0.10, tier * max(base, 0.1))
    else:
        stake = max(0.10, base)
    candle_ts = int(item["window_ts"]) if item.get("window_ts") else (
        int(item["entry_time"].timestamp()) if item.get("entry_time") else None
    )

    # Risk: consecutive losses + daily loss cap (operator)
    ok_risk, risk_reason = strategy.risk_allows_new_trade(db, user_id=None, stake_usd=stake)
    if not ok_risk:
        logger.info("[EXEC] skip risk gate: %s", risk_reason)
        return None

    # Reserve one parent signal row for the window
    sig_price = item.get("signal_price")
    if sig_price is not None:
        try:
            sig_price = float(sig_price)
        except (TypeError, ValueError):
            sig_price = None
    strat_id = _strategy_id_from_item(item)
    sig = Signal(
        pair=item["pair"],
        timeframe=item["timeframe"],
        direction=item["direction"],
        signal_time=item["signal_time"],
        entry_time=item["entry_time"],
        candle_close_time=item["candle_close_time"],
        # signal_price is the sole decision price (no RSI in this strategy)
        signal_price=sig_price,  # price at signal open (median at decision)
        mode=mode,
        order_type=order_type,
        position_size_usd=stake * max_entries,  # total planned
        status="placing",
        strategy_id=strat_id,
        error_message=f"strat={strat_id}|cap={max_px:.2f}|",
    )
    db.add(sig)
    try:
        db.commit()
        db.refresh(sig)
    except IntegrityError:
        db.rollback()
        logger.warning("[EXEC] unique conflict — already reserved %s %s", item["pair"], item["signal_time"])
        return None
    except Exception:
        db.rollback()
        logger.exception("[EXEC] failed to reserve signal row")
        return None

    if not _tf_execution_enabled(settings, item.get("timeframe") or "15m"):
        sig.status = "pending"
        sig.error_message = f"cap={max_px:.2f}|signal_only — {item.get('timeframe')} execution disabled in settings"
        sig.position_size_usd = 0.0
        db.commit()
        logger.info("[EXEC] signal id=%s %s %s recorded (execution OFF)", sig.id, item.get("pair"), item.get("timeframe"))
        try:
            from app import telegram_notify as _tg
            _tg.alert_signal_opened(sig)
        except Exception:
            pass
        seen_keys.add(key)
        return sig

    ENTRY_DEADLINE_OFFSET = _window_sec_for_tf(item.get("timeframe") or "15m")

    filled = 0
    total_spent = 0.0
    last_price = None
    order_ids: list[str] = []
    token_id = item.get("token_id")
    slug = item.get("slug")
    condition_id = item.get("condition_id")
    trade_direction = (item.get("direction") or "DOWN").upper()
    last_error = None
    window_ts = int(candle_ts or 0)
    entry_deadline = window_ts + int(ENTRY_DEADLINE_OFFSET)
    # brief multi-attempt within this call (scheduler also retries "watching")
    poll_rounds = 5
    poll_sleep = 1.5

    def _place_one(slot_idx: int) -> bool:
        nonlocal filled, total_spent, last_price, last_error, token_id, slug, condition_id
        # HARD CAP — never exceed max_entries
        if filled >= max_entries:
            return False
        try:
            result = polymarket.execute_order(
                symbol=item.get("symbol") or "BTCUSDT",
                direction=trade_direction,
                mode=mode,
                position_size_usd=stake,
                timeframe=item.get("timeframe") or "15m",
                order_type=order_type,
                max_contract_price=max_px,
                candle_open_ts=candle_ts,
            )
        except Exception as e:
            logger.exception("[EXEC] execute_order crashed on entry %d", slot_idx + 1)
            result = {"success": False, "error": str(e)}

        if result.get("success"):
            # Re-check cap in case of race
            if filled >= max_entries:
                logger.warning("[EXEC] cap reached — ignoring extra fill")
                return False
            filled += 1
            px = float(result.get("price") or result.get("price_per_contract") or 0) or None
            spent = float(result.get("total_spent") or (stake if mode == "shadow" else px * (stake / max(px, 0.01))))
            total_spent += spent
            last_price = px
            if result.get("order_id"):
                order_ids.append(str(result["order_id"]))
            if not token_id:
                token_id = result.get("token_id")
            if not slug:
                slug = result.get("slug")
            if not condition_id:
                condition_id = result.get("condition_id")
            logger.info(
                "[EXEC] entry %d/%d OK dir=%s price=%.3f spent=%.2f",
                filled, max_entries, trade_direction, px, spent,
            )
            return True

        last_error = _normalize_order_error(result.get("error") or "unknown error")
        logger.warning(
            "[EXEC] entry attempt failed dir=%s filled=%d/%d: %s",
            trade_direction, filled, max_entries, last_error,
        )
        return False

    import time as _time
    for round_i in range(poll_rounds):
        if filled >= max_entries:
            break
        now_ts = _time.time()
        if window_ts and now_ts > entry_deadline:
            logger.info("[EXEC] entry window closed (t+%ds) filled=%d/%d", ENTRY_DEADLINE_OFFSET, filled, max_entries)
            break

        remaining_slots = max_entries - filled
        for slot in range(remaining_slots):
            if filled >= max_entries:
                break
            now_ts = _time.time()
            if window_ts and now_ts > entry_deadline:
                break
            ok = _place_one(filled)
            if ok:
                # small gap between scale-in legs
                if filled < max_entries:
                    _time.sleep(0.8)
            else:
                # no fill this attempt — stop this round (likely no discount)
                break

        if filled >= max_entries:
            break
        # Still need fills and window open → short wait then retry (backtest t+90..t+240)
        now_ts = _time.time()
        if window_ts and now_ts >= entry_deadline:
            break
        if round_i < poll_rounds - 1:
            sleep_for = min(poll_sleep, max(0.0, entry_deadline - _time.time()))
            if sleep_for > 0:
                logger.info(
                    "[EXEC] waiting %.0fs for discount liquidity (filled %d/%d, window open until t+%ds)",
                    sleep_for, filled, max_entries, ENTRY_DEADLINE_OFFSET,
                )
                _time.sleep(sleep_for)

    # Persist outcome (prefer live fill data; fall back to signal item)
    sig.polymarket_slug = slug or item.get("slug")
    sig.condition_id = condition_id or item.get("condition_id")
    sig.polymarket_order_id = ",".join(order_ids) if order_ids else None
    sig.token_id = token_id or item.get("token_id")
    if last_price is not None:
        sig.entry_price = last_price
    elif sig.signal_price is not None and filled == 0:
        # keep signal_price as reference even if not yet filled
        pass
    if total_spent > 0:
        sig.position_size_usd = total_spent

    if filled >= max_entries:
        # Fully filled to settings cap — stop; no more orders for this window
        sig.status = "pending"
        sig.error_message = None
        db.commit()
        try:
            settings = get_or_create_settings(db)
            bal = float(settings.shadow_balance or 0.0)
            settings.shadow_balance = max(0.0, bal - total_spent)
            db.commit()
        except Exception:
            logger.exception("[SHADOW] balance update failed")
        logger.info(
            "[EXEC] CAP filled %d/%d spent $%.2f dir=%s — no further entries",
            filled, max_entries, total_spent, trade_direction,
        )
        try:
            tg.alert_signal_opened(sig)
        except Exception:
            logger.debug("telegram open alert failed", exc_info=True)
        return sig

    if filled > 0:
        # Partial fill — mark pending (hold what we have); do not over-fill later beyond cap
        # Keep watching only if we still want remaining scale-in legs and window open
        now_ts = _time.time()
        if window_ts and now_ts < entry_deadline and filled < max_entries:
            sig.status = "watching"
            sig.error_message = f"cap={max_px:.2f}|partial {filled}/{max_entries}; watching until t+{ENTRY_DEADLINE_OFFSET}s"
        else:
            sig.status = "pending"
            sig.error_message = None
        db.commit()
        try:
            settings = get_or_create_settings(db)
            bal = float(settings.shadow_balance or 0.0)
            settings.shadow_balance = max(0.0, bal - total_spent)
            db.commit()
        except Exception:
            logger.exception("[SHADOW] balance update failed")
        logger.info(
            "[EXEC] partial %d/%d spent $%.2f dir=%s status=%s",
            filled, max_entries, total_spent, trade_direction, sig.status,
        )
        try:
            tg.alert_signal_opened(sig)
        except Exception:
            logger.debug("telegram open alert failed", exc_info=True)
        return sig

    # Zero fills
    now_ts = _time.time()
    still_open = bool(window_ts and now_ts < entry_deadline)
    no_discount = last_error and "no discount liquidity" in last_error.lower()
    if still_open and (no_discount or last_error):
        # Keep watching through entry deadline (matches backtest)
        sig.status = "watching"
        sig.error_message = (
            f"cap={max_px:.2f}|watching ≤{max_px:.2f} until t+{ENTRY_DEADLINE_OFFSET}s: {last_error}"
            if last_error else f"cap={max_px:.2f}|watching ≤{max_px:.2f} until t+{ENTRY_DEADLINE_OFFSET}s"
        )
        db.commit()
        logger.info("[EXEC] watching window until t+%ds dir=%s", ENTRY_DEADLINE_OFFSET, trade_direction)
        # No telegram spam on every watch start — optional short note
        return sig

    sig.status = "error"
    sig.error_message = (
        f"all scale-in attempts failed: {last_error}" if last_error else "all scale-in attempts failed"
    )
    db.commit()
    try:
        tg.alert_signal_opened(sig)
    except Exception:
        logger.debug("telegram open/error alert failed", exc_info=True)
    return sig


def _process_new_signals_inner(db: Session) -> list:
    """
    1) Scan all pairs first (fast, local data).
    2) Execute every candidate in parallel so BTC + ETH hit the book
       at the same moment instead of waiting for each other.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from app.database import SessionLocal

    settings = get_or_create_settings(db)
    now = datetime.now(timezone.utc)

    # --- Phase 1: scan L5 Value Lock only ---
    candidates: list[dict] = []

    def _scan_l5(symbol: str) -> list:
        try:
            from app import strategy_l5
            return strategy_l5.scan_one_symbol(symbol, now=now) or []
        except Exception:
            logger.exception("scan_one_symbol L5 %s failed", symbol)
            return []

    pairs = list(getattr(strategy, "ACTIVE_PAIRS", ["BTCUSDT"]))
    with ThreadPoolExecutor(max_workers=max(2, len(pairs))) as pool:
        futs = [pool.submit(_scan_l5, sym) for sym in pairs]
        for fut in as_completed(futs):
            for item in fut.result():
                if item and "error" not in item:
                    candidates.append(item)

    if not candidates:
        return []

    logger.info(
        "[SCAN] %d candidate(s) ready for parallel execute: %s",
        len(candidates),
        ", ".join(f"{c.get('pair')}/{c.get('direction')}" for c in candidates),
    )

    # --- Phase 2: parallel execute (one DB session per thread) ---
    def _run_one(item: dict):
        local = SessionLocal()
        try:
            st = get_or_create_settings(local)
            # per-thread seen set — cross-thread dedup is DB unique constraint
            sig = _execute_one_signal(local, st, item, set())
            if sig is not None and getattr(sig, "id", None):
                try:
                    fan_out_user_trades(local, sig)
                except Exception:
                    logger.exception("[USER] fan-out failed for signal %s", sig.id)
            return sig
        except Exception:
            logger.exception("[EXEC] parallel worker failed for %s", item.get("pair"))
            return None
        finally:
            local.close()

    created = []
    workers = min(4, max(1, len(candidates)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(_run_one, item) for item in candidates]
        for fut in as_completed(futs):
            try:
                sig = fut.result()
            except Exception:
                logger.exception("[EXEC] future failed")
                continue
            if sig is not None:
                created.append(sig)

    if created:
        _stats_cache["data"] = None
        try:
            db.expire_all()
        except Exception:
            pass
    return created


def resolve_pending_signals(db: Session) -> int:
    if not _resolve_lock.acquire(blocking=False):
        logger.debug("Resolve skipped — already running")
        return 0
    try:
        return _resolve_pending_signals_inner(db)
    finally:
        _resolve_lock.release()



def _resolve_pending_signals_inner(db: Session) -> int:
    """
    Resolve all due pending signals together:
      1) Claim every due row
      2) Fetch Polymarket outcomes only (parallel; no Binance)
      3) Apply results + shadow ledger + Telegram
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    now = datetime.now(timezone.utc)

    # Expire watching signals whose candle has closed (no more entry window)
    for wsig in db.query(Signal).filter(Signal.status == "watching").all():
        close = _as_utc(wsig.candle_close_time)
        if close is not None and now >= close:
            fills = _count_fills(wsig)
            if fills > 0:
                wsig.status = "pending"
                wsig.error_message = None
            else:
                wsig.status = "error"
                wsig.error_message = (wsig.error_message or "") + " | expired at candle close with 0 fills"
            logger.info("[RESOLVE] expire watching id=%s fills=%d → %s", wsig.id, fills, wsig.status)
    try:
        db.commit()
    except Exception:
        db.rollback()

    candidates = db.query(Signal).filter(Signal.status == "pending").all()
    # Never resolve before candle_close_time. At/after boundary Polymarket UI
    # already shows the result — we accept PM prices then (no Binance).
    due_ids: list[int] = []
    for sig in candidates:
        close = _as_utc(sig.candle_close_time)
        if close is not None and now >= close:
            due_ids.append(sig.id)

    if not due_ids:
        return 0

    # Claim all due rows up front (stops double resolve)
    claimed_ids: list[int] = []
    for sid in due_ids:
        n = (
            db.query(Signal)
            .filter(Signal.id == sid, Signal.status == "pending")
            .update({"status": "resolving"}, synchronize_session=False)
        )
        if n:
            claimed_ids.append(sid)
    try:
        db.commit()
    except Exception:
        db.rollback()
        return 0

    if not claimed_ids:
        return 0

    rows = {s.id: s for s in db.query(Signal).filter(Signal.id.in_(claimed_ids)).all()}

    def _outcome_for(sig: Signal):
        """Polymarket only — never Binance for trade results."""
        outcome = None
        source = None
        if not (sig.condition_id or sig.polymarket_slug):
            logger.warning(
                "Resolve skip id=%s: missing condition_id/slug (cannot use Polymarket)",
                sig.id,
            )
            return sig.id, None, None
        try:
            poly_meta = polymarket.get_market_resolution(
                slug=sig.polymarket_slug,
                condition_id=sig.condition_id,
            )
            if poly_meta.get("resolved") and poly_meta.get("winning_side"):
                outcome = poly_meta.get("winning_side")
                source = poly_meta.get("source") or "polymarket"
            elif poly_meta.get("pending_price"):
                logger.debug(
                    "PM price pending close id=%s price=%s",
                    sig.id,
                    poly_meta.get("price"),
                )
        except Exception as e:
            logger.debug("PM resolve failed id=%s: %s", sig.id, e)
        return sig.id, outcome, source

    # Parallel outcome fetch — both pairs at once
    outcomes: dict[int, tuple] = {}
    workers = min(4, max(1, len(claimed_ids)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(_outcome_for, rows[sid]) for sid in claimed_ids if sid in rows]
        for fut in as_completed(futs):
            try:
                sid, outcome, source = fut.result()
                outcomes[sid] = (outcome, source)
            except Exception:
                logger.exception("parallel resolve worker failed")

    resolved = 0
    settings = get_or_create_settings(db)

    for sid in claimed_ids:
        sig = rows.get(sid)
        if sig is None:
            continue
        outcome, source = outcomes.get(sid, (None, None))

        if outcome is None:
            close = _as_utc(sig.candle_close_time)
            if close is not None and now > close + timedelta(minutes=30):
                sig.status = "error"
                sig.error_message = "Could not resolve on Polymarket (waiting or missing market)"
                sig.resolved_at = now
            else:
                sig.status = "pending"  # release for next poll
            try:
                db.commit()
            except Exception:
                db.rollback()
            continue

        won = outcome.upper() == sig.direction.upper()
        sig.result = "win" if won else "loss"
        sig.status = "won" if won else "lost"
        sig.resolved_at = now
        if source:
            logger.info("Resolved id=%s %s %s via %s -> %s", sid, sig.pair, sig.direction, source, sig.result)

        # Odds-aware PnL: $stake buys (stake/entry_price) shares; win pays $1/share
        stake = float(sig.position_size_usd or 1.0)
        ep = float(sig.entry_price or sig.signal_price or 0.0)
        if ep <= 0 or ep >= 1:
            ep = 0.85  # safe fallback near validated avg entry
        if won:
            # profit = shares - stake = stake * (1/ep - 1)
            sig.pnl = round(stake * (1.0 / ep - 1.0), 4)
        else:
            sig.pnl = round(-stake, 4)

        # Shadow paper track for every signal (odds-aware)
        settings = get_or_create_settings(db)
        bal = float(settings.shadow_balance or 0.0)
        mode_l = (sig.mode or "shadow").lower()
        prior_open = (
            db.query(ShadowLedger)
            .filter(ShadowLedger.signal_id == sig.id, ShadowLedger.change < 0)
            .first()
        )
        if prior_open is None:
            bal = bal - stake
            settings.shadow_balance = bal
            db.add(
                ShadowLedger(
                    signal_id=sig.id,
                    change=-stake,
                    balance_after=bal,
                    note=f"Open-catchup [{mode_l}] {sig.direction} {sig.pair} stake=${stake:.2f} @ {ep:.3f}",
                )
            )
        if won:
            # return stake + profit = stake / ep  (shares * $1)
            payout = stake / ep
            settings.shadow_balance = float(settings.shadow_balance or 0.0) + payout
            db.add(
                ShadowLedger(
                    signal_id=sig.id,
                    change=payout,
                    balance_after=settings.shadow_balance,
                    note=f"Win [{mode_l}] {sig.direction} {sig.pair} {sig.timeframe} +${sig.pnl:.2f} (payout ${payout:.2f})",
                )
            )
        else:
            db.add(
                ShadowLedger(
                    signal_id=sig.id,
                    change=0.0,
                    balance_after=float(settings.shadow_balance or 0.0),
                    note=f"Loss [{mode_l}] {sig.direction} {sig.pair} {sig.timeframe}",
                )
            )

        try:
            db.commit()
            db.refresh(sig)
        except Exception:
            db.rollback()
            logger.exception("resolve commit failed id=%s", sid)
            continue

        resolved += 1
        logger.info(
            "Resolved %s %s %s → %s pnl=%.2f (%s %s)",
            sig.pair,
            sig.timeframe,
            sig.direction,
            sig.result,
            sig.pnl,
            source or "?",
            outcome,
        )
        try:
            tg.alert_signal_resolved(sig)
        except Exception:
            logger.debug("telegram resolve alert failed", exc_info=True)

        # Resolve linked independent user trades with same outcome
        try:
            from app.models import UserTrade
            uts = (
                db.query(UserTrade)
                .filter(
                    UserTrade.signal_id == sig.id,
                    UserTrade.status.in_(("pending", "placing")),
                )
                .all()
            )
            for ut in uts:
                ut.result = sig.result
                stake = float(ut.position_size_usd or 1.0)
                ep = float(ut.entry_price or ut.signal_price or sig.entry_price or sig.signal_price or 0.0)
                if ep <= 0 or ep >= 1:
                    ep = 0.85
                if sig.result == "win":
                    ut.pnl = round(stake * (1.0 / ep - 1.0), 4)
                elif sig.result == "loss":
                    ut.pnl = round(-stake, 4)
                else:
                    ut.pnl = 0.0
                ut.status = "resolved"
                ut.resolved_at = datetime.now(timezone.utc)
            if uts:
                # shadow credit: payout = stake / entry_price on win
                from app import multiuser as _mu
                for ut in uts:
                    if (ut.mode or "").lower() == "shadow" and ut.result == "win":
                        uset = _mu.get_user_setting(db, ut.user_id)
                        ep = float(ut.entry_price or ut.signal_price or 0.85)
                        if ep <= 0 or ep >= 1:
                            ep = 0.85
                        payout = float(ut.position_size_usd or 0) / ep
                        uset.shadow_balance = float(uset.shadow_balance or 0.0) + payout
                db.commit()
                logger.info("[USER] resolved %d user trades for signal %s → %s", len(uts), sig.id, sig.result)
        except Exception:
            logger.exception("[USER] resolve user trades failed for signal %s", sig.id)

    if resolved:
        _stats_cache["data"] = None
    return resolved





def list_user_trades(
    db: Session,
    user_id: int,
    *,
    date_from=None,
    date_to=None,
    result=None,
    timeframe=None,
    pairs=None,
    strategy=None,
    limit: int = 500,
):
    from app.models import UserTrade
    q = db.query(UserTrade).filter(UserTrade.user_id == user_id)
    if date_from is not None:
        q = q.filter(UserTrade.entry_time >= date_from)
    if date_to is not None:
        q = q.filter(UserTrade.entry_time <= date_to)
    if result:
        q = q.filter(UserTrade.result == result)
    if timeframe:
        q = q.filter(UserTrade.timeframe == timeframe)
    if pairs:
        q = q.filter(UserTrade.pair.in_(pairs))
    if strategy:
        s = str(strategy).strip().lower()
        aliases = {
            "l5": "l5_value_lock_5m",
            "l5_value_lock": "l5_value_lock_5m",
            "l5_value_lock_5m": "l5_value_lock_5m",
            "value_lock": "l5_value_lock_5m",
        }
        sid = aliases.get(s, strategy.strip())
        q = q.filter(UserTrade.strategy_id == sid)
    rows = q.order_by(UserTrade.entry_time.desc()).limit(limit).all()
    # signal-like views for history template
    out = []
    for ut in rows:
        class V: pass
        v = V()
        for k in ("id","pair","timeframe","direction","signal_time","entry_time","candle_close_time",
                  "mode","status","result","pnl","position_size_usd","error_message","polymarket_slug",
                  "strategy_id","entry_price","signal_price"):
            setattr(v, k, getattr(ut, k, None))
        v.rsi = None
        out.append(v)
    return out


def user_history_period_stats(db: Session, user_id: int, **filters) -> dict:
    from app.models import UserTrade
    from sqlalchemy import case
    q = db.query(UserTrade).filter(UserTrade.user_id == user_id)
    if filters.get("date_from") is not None:
        q = q.filter(UserTrade.entry_time >= filters["date_from"])
    if filters.get("date_to") is not None:
        q = q.filter(UserTrade.entry_time <= filters["date_to"])
    if filters.get("result"):
        q = q.filter(UserTrade.result == filters["result"])
    if filters.get("timeframe"):
        q = q.filter(UserTrade.timeframe == filters["timeframe"])
    if filters.get("pairs"):
        q = q.filter(UserTrade.pair.in_(filters["pairs"]))
    if filters.get("strategy"):
        s = str(filters["strategy"]).strip().lower()
        aliases = {
            "l5": "l5_value_lock_5m",
            "l5_value_lock": "l5_value_lock_5m",
            "l5_value_lock_5m": "l5_value_lock_5m",
            "value_lock": "l5_value_lock_5m",
        }
        sid = aliases.get(s, filters["strategy"].strip())
        q = q.filter(UserTrade.strategy_id == sid)
    rows = q.all()
    wins = sum(1 for r in rows if r.result == "win")
    losses = sum(1 for r in rows if r.result == "loss")
    pnl = sum(float(r.pnl or 0) for r in rows if r.result is not None)
    total = len(rows)
    wr = (wins / (wins + losses) * 100.0) if (wins + losses) else 0.0
    return {"total": total, "wins": wins, "losses": losses, "pnl": pnl, "win_rate": round(wr, 1)}


def user_dashboard_stats(db: Session, user_id: int, *, skip_live: bool = True) -> dict:
    """
    Isolated view for a connected user — their UserTrade + UserSetting only.
    Never exposes operator Signal ledger or operator POLYMARKET_* live balance.
    Hardened: never raises (login/dashboard must not 500).
    """
    from app.models import UserTrade, UserCredential
    from app import multiuser
    from sqlalchemy import case, text

    # Safe defaults
    empty = {
        "total": 0, "wins": 0, "losses": 0, "pending": 0, "pnl": 0.0,
    }

    try:
        us = multiuser.get_user_setting(db, user_id)
    except Exception as e:
        logger.warning("user_dashboard_stats settings: %s", e)
        try:
            db.rollback()
        except Exception:
            pass
        class _US:
            mode = "shadow"
            order_type = "FAK"
            position_size_usd = 1.0
            scale_entries = 1
            shadow_balance = 50.0
            max_contract_price = 0.95
            enabled_btc = True
            enabled_eth = False
            auto_trade = True
        us = _US()

    now = datetime.now(timezone.utc)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)

    today = dict(empty)
    alltime = dict(empty)
    recent_view = []

    try:
        def _agg(q):
            row = q.one()
            return {
                "total": int(row[0] or 0),
                "wins": int(row[1] or 0),
                "losses": int(row[2] or 0),
                "pending": int(row[3] or 0),
                "pnl": float(row[4] or 0.0),
            }

        base = db.query(
            func.count(UserTrade.id),
            func.sum(case((UserTrade.result == "win", 1), else_=0)),
            func.sum(case((UserTrade.result == "loss", 1), else_=0)),
            func.sum(case((UserTrade.status.in_(("pending", "watching")), 1), else_=0)),
            func.coalesce(func.sum(case((UserTrade.result.isnot(None), UserTrade.pnl), else_=0.0)), 0.0),
        ).filter(UserTrade.user_id == int(user_id))

        today = _agg(base.filter(UserTrade.entry_time >= day_start, UserTrade.entry_time < day_end))
        alltime = _agg(base)

        recent = (
            db.query(UserTrade)
            .filter(UserTrade.user_id == int(user_id))
            .order_by(UserTrade.entry_time.desc())
            .limit(12)
            .all()
        )
        class _SigView:
            pass
        for ut in recent:
            v = _SigView()
            for k in ("id", "pair", "timeframe", "direction", "signal_time", "entry_time",
                      "candle_close_time", "mode", "status", "result", "pnl", "position_size_usd", "error_message"):
                setattr(v, k, getattr(ut, k, None))
            v.rsi = None
            recent_view.append(v)
    except Exception as e:
        logger.warning("user_dashboard_stats trades: %s", e)
        try:
            db.rollback()
        except Exception:
            pass
        try:
            from app.database import init_db
            init_db()
        except Exception:
            pass

    wr = (today["wins"] / (today["wins"] + today["losses"]) * 100.0) if (today["wins"] + today["losses"]) else 0.0
    all_wr = (alltime["wins"] / (alltime["wins"] + alltime["losses"]) * 100.0) if (alltime["wins"] + alltime["losses"]) else 0.0

    class _S:
        pass
    sview = _S()
    sview.mode = (getattr(us, "mode", None) or "shadow").lower()
    if sview.mode not in ("shadow", "live"):
        sview.mode = "shadow"
    sview.order_type = getattr(us, "order_type", None) or "FAK"
    sview.position_size_usd = float(getattr(us, "position_size_usd", None) or 1.0)
    sview.scale_entries = int(getattr(us, "scale_entries", None) or 1)
    sview.shadow_balance = float(getattr(us, "shadow_balance", None) or 50.0)
    sview.max_contract_price = float(getattr(us, "max_contract_price", None) or 0.95)
    sview.enabled_btc = bool(getattr(us, "enabled_btc", True))
    sview.enabled_eth = bool(getattr(us, "enabled_eth", False))
    sview.enabled_15m = True
    sview.enabled_1h = False
    sview.auto_trade = bool(getattr(us, "auto_trade", True))

    creds = {
        "private_key_set": False,
        "funder": None,
        "signature_type": 3,
        "live_ready": False,
        "session_address": None,
        "session_authorized": False,
        "signer_mode": None,
        "note": "Setup managed wallet on Account.",
    }
    try:
        ctx = multiuser.get_user_trading_context(db, int(user_id))
        if ctx:
            creds["funder"] = ctx.get("funder")
            creds["signature_type"] = int(ctx.get("signature_type") or 3)
            creds["session_address"] = ctx.get("session_address")
            creds["session_authorized"] = bool(ctx.get("session_authorized"))
            creds["private_key_set"] = bool(ctx.get("has_private_key"))
            creds["live_ready"] = bool(ctx.get("live_ready"))
            creds["signer_mode"] = ctx.get("signer_mode")
            creds["owner_address"] = ctx.get("owner_address")
            creds["note"] = (
                f"signer_mode={ctx.get('signer_mode')} — operator keys never used for your account."
            )
    except Exception as e:
        logger.warning("user_dashboard_stats creds: %s", e)
        try:
            db.rollback()
        except Exception:
            pass

    result = {
        "settings": sview,
        "credentials": creds,
        "today": {
            "signals": today["total"],
            "wins": today["wins"],
            "losses": today["losses"],
            "pending": today["pending"],
            "pnl": today["pnl"],
            "pnl_shadow": today["pnl"],
            "win_rate": round(wr, 1),
            "live_pnl": 0.0,
            "live_wins": 0,
            "live_losses": 0,
        },
        "all_time": {
            "signals": alltime["total"],
            "wins": alltime["wins"],
            "losses": alltime["losses"],
            "pnl": alltime["pnl"],
            "win_rate": round(all_wr, 1),
            "live_pnl": 0.0,
        },
        "shadow_balance": float(sview.shadow_balance),
        "live_balance": None,
        "live_onchain_daily_pnl": None,
        "live_onchain_all_time_pnl": None,
        "recent": recent_view,
        "user_scoped": True,
        "user_id": int(user_id),
        "telegram": {"configured": False, "chat_id_set": False, "token_set": False},
    }
    try:
        from app import telegram_notify as tg
        result["telegram"] = tg.status()
    except Exception:
        result["telegram"] = {"configured": False, "chat_id_set": False, "token_set": False}
    try:
        funder = (creds.get("funder") or "").strip()
        if funder:
            from app import polymarket as poly
            result["live_balance"] = poly.get_onchain_balance(funder=funder)
    except Exception as e:
        logger.debug("user live balance: %s", e)
        result["live_balance"] = {"success": False, "error": str(e)[:120]}
    return result



def dashboard_stats(
    db: Session, day: Optional[datetime] = None, *, skip_live: bool = False
) -> dict:
    ttl = _STATS_TTL_LIGHT if skip_live else _STATS_TTL
    if (
        day is None
        and _stats_cache["data"] is not None
        and _stats_cache["skip_live"] == skip_live
        and (_time_mod.time() - _stats_cache["ts"]) < ttl
    ):
        return _stats_cache["data"]

    settings = get_or_create_settings(db)
    now = datetime.now(timezone.utc)
    if day is None:
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        day_start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)

    # Two aggregate queries instead of ~15 count() round-trips
    from sqlalchemy import case, literal

    today_row = (
        db.query(
            func.count(Signal.id),
            func.sum(case((Signal.result == "win", 1), else_=0)),
            func.sum(case((Signal.result == "loss", 1), else_=0)),
            func.sum(case((Signal.status == "pending", 1), else_=0)),
            func.coalesce(func.sum(case((Signal.result.isnot(None), Signal.pnl), else_=0.0)), 0.0),
            func.coalesce(
                func.sum(
                    case(
                        (
                            (Signal.result.isnot(None)) & (func.lower(Signal.mode) == "live"),
                            Signal.pnl,
                        ),
                        else_=0.0,
                    )
                ),
                0.0,
            ),
            func.sum(
                case(
                    (
                        (Signal.result == "win") & (func.lower(Signal.mode) == "live"),
                        1,
                    ),
                    else_=0,
                )
            ),
            func.sum(
                case(
                    (
                        (Signal.result == "loss") & (func.lower(Signal.mode) == "live"),
                        1,
                    ),
                    else_=0,
                )
            ),
            func.sum(
                case(
                    (
                        (Signal.status == "pending") & (func.lower(Signal.mode) == "live"),
                        1,
                    ),
                    else_=0,
                )
            ),
        )
        .filter(Signal.entry_time >= day_start, Signal.entry_time < day_end)
        .one()
    )
    total = int(today_row[0] or 0)
    wins = int(today_row[1] or 0)
    losses = int(today_row[2] or 0)
    pending = int(today_row[3] or 0)
    pnl_today = float(today_row[4] or 0)
    pnl_today_live = float(today_row[5] or 0)
    pnl_today_shadow = pnl_today  # paper mirror of all resolved today
    live_wins = int(today_row[6] or 0)
    live_losses = int(today_row[7] or 0)
    live_pending = int(today_row[8] or 0)
    wr = (wins / (wins + losses) * 100) if (wins + losses) else 0.0

    all_row = (
        db.query(
            func.count(Signal.id),
            func.sum(case((Signal.result == "win", 1), else_=0)),
            func.sum(case((Signal.result == "loss", 1), else_=0)),
            func.coalesce(func.sum(case((Signal.result.isnot(None), Signal.pnl), else_=0.0)), 0.0),
            func.coalesce(
                func.sum(
                    case(
                        (
                            (Signal.result.isnot(None)) & (func.lower(Signal.mode) == "live"),
                            Signal.pnl,
                        ),
                        else_=0.0,
                    )
                ),
                0.0,
            ),
        )
        .one()
    )
    all_total = int(all_row[0] or 0)
    all_wins = int(all_row[1] or 0)
    all_losses = int(all_row[2] or 0)
    pnl_all = float(all_row[3] or 0)
    pnl_all_live = float(all_row[4] or 0)
    all_wr = (all_wins / (all_wins + all_losses) * 100) if (all_wins + all_losses) else 0.0

    creds = _cached_credentials()
    live_bal = None
    live_onchain_daily_pnl = None
    live_onchain_all_time_pnl = None
    live_day_start_pusd = getattr(settings, "live_day_start_pusd", None)
    day_key = day_start.strftime("%Y-%m-%d")

    # UI path: NEVER block on Polymarket/RPC. Serve cache only; refresh in background.
    if not skip_live and creds.get("live_ready"):
        if _live_bal_cache["data"] is not None:
            live_bal = _live_bal_cache["data"]
            # stale → kick background refresh (non-blocking)
            if (_time_mod.time() - _live_bal_cache["ts"]) >= _LIVE_BAL_TTL:
                refresh_live_caches_async()
        else:
            live_bal = None
            refresh_live_caches_async()

        # UTC day baseline: first successful read of the day sets the start balance
        if live_bal and live_bal.get("success"):
            cur = float(live_bal.get("pusd_balance") or 0.0)
            stored_date = getattr(settings, "live_day_start_date", None)
            stored_pusd = getattr(settings, "live_day_start_pusd", None)
            if stored_date != day_key or stored_pusd is None:
                settings.live_day_start_date = day_key
                settings.live_day_start_pusd = cur
                live_day_start_pusd = cur
                live_onchain_daily_pnl = 0.0
            else:
                live_day_start_pusd = float(stored_pusd)
                live_onchain_daily_pnl = round(cur - live_day_start_pusd, 4)

            # All-time on-chain: first successful balance ever is the baseline
            at_base = getattr(settings, "live_all_time_start_pusd", None)
            if at_base is None:
                settings.live_all_time_start_pusd = cur
                at_base = cur
            live_onchain_all_time_pnl = round(cur - float(at_base), 4)

            # Avoid DB commit on hot UI path; baseline can be persisted by background later
            live_bal["day_start_pusd"] = live_day_start_pusd
            live_bal["onchain_daily_pnl"] = live_onchain_daily_pnl
            live_bal["onchain_all_time_pnl"] = live_onchain_all_time_pnl
            live_bal["all_time_start_pusd"] = float(at_base)
            live_bal["day"] = day_key

    # Polymarket Data-API: real CLOB trades for funder today
    live_activity = None
    live_clob_daily_pnl = None
    live_clob_all_time_pnl = None
    if not skip_live and creds.get("live_ready"):
        if (
            _live_trades_cache["data"] is not None
            and (_time_mod.time() - _live_trades_cache["ts"]) < _LIVE_TRADES_TTL
        ):
            live_activity = _live_trades_cache["data"]
        else:
            # no fresh network on request path
            live_activity = _live_trades_cache.get("data")
            refresh_live_caches_async()

        if live_activity and live_activity.get("success"):
            live_clob_daily_pnl = float(live_activity.get("realized_pnl") or 0.0)

        # All-time live PnL from cache only (background job refreshes)
        try:
            at_cache = _live_trades_cache.get("all_time")
            if at_cache is not None:
                live_clob_all_time_pnl = float(at_cache)
            else:
                refresh_live_caches_async()
        except Exception as e:
            logger.warning("all-time live cache read failed: %s", e)

        if live_clob_all_time_pnl is not None:
            live_onchain_all_time_pnl = round(float(live_clob_all_time_pnl), 4)
        else:
            live_onchain_all_time_pnl = round(float(pnl_all_live), 2)

    out = {
        "today": {
            "signals": total,
            "wins": wins,
            "losses": losses,
            "pending": pending,
            "win_rate": round(wr, 2),
            "pnl": round(float(pnl_today), 2),
            "pnl_live": round(float(pnl_today_live), 2),
            "pnl_shadow": round(float(pnl_today_shadow), 2),
            "live_wins": live_wins,
            "live_losses": live_losses,
            "live_pending": live_pending,
        },
        "all_time": {
            "signals": all_total,
            "wins": all_wins,
            "losses": all_losses,
            "win_rate": round(all_wr, 2),
            "pnl": round(float(pnl_all), 2),
            "pnl_live": round(float(pnl_all_live), 2),
            "onchain_pnl": live_onchain_all_time_pnl,
        },
        "settings": {
            "mode": settings.mode,
            "order_type": settings.order_type,
            "position_size_usd": settings.position_size_usd,
            "scale_entries": int(getattr(settings, "scale_entries", 1) or 1),
            "shadow_balance": round(settings.shadow_balance or 0, 2),
            "max_contract_price": settings.max_contract_price,
            "enabled_15m": settings.enabled_15m,
            "enabled_5m": getattr(settings, "enabled_5m", True),
            "enabled_1h": getattr(settings, "enabled_1h", False),
            "enabled_btc": getattr(settings, "enabled_btc", True),
            "enabled_eth": getattr(settings, "enabled_eth", True),
        },
        "credentials": creds,
        "live_balance": live_bal,
        "live_onchain_daily_pnl": live_onchain_daily_pnl,
        "live_clob_daily_pnl": live_clob_daily_pnl,
        "live_onchain_all_time_pnl": live_onchain_all_time_pnl,
        "live_clob_all_time_pnl": live_clob_all_time_pnl,
        "live_pnl_since_ts": _live_trades_cache.get("all_time_start_ts"),
        "live_day_start_pusd": live_day_start_pusd,
        "live_activity": live_activity,
        "telegram": tg.status(),
    }
    if day is None:
        _stats_cache["data"] = out
        _stats_cache["ts"] = _time_mod.time()
        _stats_cache["skip_live"] = skip_live
    return out


def _signal_filter_query(
    db: Session,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    result: Optional[str] = None,
    timeframe: Optional[str] = None,
    pairs: Optional[list] = None,
    strategy: Optional[str] = None,
):
    q = db.query(Signal)
    if date_from:
        q = q.filter(Signal.entry_time >= date_from)
    if date_to:
        q = q.filter(Signal.entry_time <= date_to)
    if result in ("win", "loss"):
        q = q.filter(Signal.result == result)
    if timeframe in ("5m", "15m", "1h"):
        q = q.filter(Signal.timeframe == timeframe)
    if pairs:
        cleaned = [p.upper().replace("USDT", "") for p in pairs if p]
        if cleaned:
            q = q.filter(Signal.pair.in_(cleaned))
    if strategy:
        s = strategy.strip().lower()
        # Accept short aliases from UI
        aliases = {
            "l5": "l5_value_lock_5m",
            "l5_value_lock": "l5_value_lock_5m",
            "l5_value_lock_5m": "l5_value_lock_5m",
            "value_lock": "l5_value_lock_5m",
        }
        sid = aliases.get(s, strategy.strip())
        q = q.filter(Signal.strategy_id == sid)
    return q


def list_signals(
    db: Session,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    result: Optional[str] = None,
    timeframe: Optional[str] = None,
    pairs: Optional[list] = None,
    strategy: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
) -> list:
    q = _signal_filter_query(db, date_from, date_to, result, timeframe, pairs, strategy=strategy)
    return q.order_by(Signal.entry_time.desc()).offset(offset).limit(limit).all()


def history_period_stats(
    db: Session,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    result: Optional[str] = None,
    timeframe: Optional[str] = None,
    pairs: Optional[list] = None,
    strategy: Optional[str] = None,
) -> dict:
    """
    Aggregate win rate / PnL for the selected period + filters.
    Uses full matching set (not the list limit). Pending/error excluded from WR.
    """
    q = _signal_filter_query(db, date_from, date_to, result, timeframe, pairs, strategy=strategy)
    rows = q.all()
    total = len(rows)
    wins = sum(1 for s in rows if s.result == "win")
    losses = sum(1 for s in rows if s.result == "loss")
    pending = sum(1 for s in rows if s.status == "pending")
    errors = sum(1 for s in rows if s.status == "error")
    decided = wins + losses
    wr = (100.0 * wins / decided) if decided else 0.0
    pnl = sum(float(s.pnl or 0) for s in rows if s.result in ("win", "loss"))
    # Per-pair breakdown (always useful when multiple selected)
    by_pair: dict = {}
    for s in rows:
        p = s.pair or "?"
        bucket = by_pair.setdefault(p, {"n": 0, "wins": 0, "losses": 0, "pending": 0, "pnl": 0.0})
        bucket["n"] += 1
        if s.result == "win":
            bucket["wins"] += 1
            bucket["pnl"] += float(s.pnl or 0)
        elif s.result == "loss":
            bucket["losses"] += 1
            bucket["pnl"] += float(s.pnl or 0)
        elif s.status == "pending":
            bucket["pending"] += 1
    pair_stats = []
    for p, b in sorted(by_pair.items()):
        d = b["wins"] + b["losses"]
        pair_stats.append({
            "pair": p,
            "n": b["n"],
            "wins": b["wins"],
            "losses": b["losses"],
            "pending": b["pending"],
            "win_rate": (100.0 * b["wins"] / d) if d else 0.0,
            "pnl": round(b["pnl"], 2),
            "decided": d,
        })
    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "pending": pending,
        "errors": errors,
        "decided": decided,
        "win_rate": round(wr, 2),
        "pnl": round(pnl, 2),
        "pairs": pair_stats,
        "selected_pairs": [p.upper().replace("USDT", "") for p in (pairs or []) if p],
    }




def live_history_trades(
    *,
    date_from=None,
    date_to=None,
    side: str | None = None,
    result: str | None = None,
    pair: str | None = None,
    limit: int = 500,
    funder: str | None = None,
) -> dict:
    """CLOB/on-chain trade history. Pass funder= for a connected user's wallet only."""
    now = datetime.now(timezone.utc)
    if date_to is None:
        end_ts = int(now.timestamp()) + 60
    else:
        end_ts = int(date_to.timestamp())
    if date_from is None:
        start_ts = int((now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=7)).timestamp())
    else:
        start_ts = int(date_from.timestamp())
    return polymarket.get_user_trades(
        user=(funder.strip() if funder else None),
        start_ts=start_ts,
        end_ts=end_ts,
        side=side,
        result=result,
        pair=pair,
        limit=limit,
    )

def send_daily_summary(db: Session) -> dict:
    stats = dashboard_stats(db, skip_live=True)
    day_label = datetime.now(timezone.utc).strftime("%Y-%m-%d UTC")
    result = tg.alert_daily_summary(stats, day_label=day_label)
    if result.get("success"):
        logger.info("Daily Telegram summary sent for %s", day_label)
    elif result.get("skipped"):
        logger.debug("Daily summary skipped — Telegram not configured")
    else:
        logger.warning("Daily summary failed: %s", result.get("error"))
    return result
