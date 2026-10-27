"""
Scheduler for L5 Value Lock (5m running-market strategy).

Timing:
  5m L5 Value Lock:
    - Signal zone: t+232s → t+248s of current 5m window (ideal t+240s)
    - Trades the RUNNING market (opened at current window start, resolves at +300s)
    - No next-market prediction; no artificial time buffer beyond L5 design

  Scan every 1s: strategy gates on its own elapsed window.
  Watch/fill retries while status=watching until target candle close.
  Resolve at every 5m close + 1s poll + catch-up.

Jobs:
  - resolve boundary at every 5m close
  - scan_and_watch every 1s
  - resolve interval 1s
  - daily telegram summary
"""
from __future__ import annotations

import logging
import time as _time
from datetime import datetime, timezone, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.database import SessionLocal
from app import services
from app.models import Signal
import os

logger = logging.getLogger(__name__)
_scheduler: BackgroundScheduler | None = None

_CATCHUP_EVERY_SEC = 1
_CATCHUP_FOR_SEC = 900
_SCAN_INTERVAL_SEC = 1


def _pending_due_count(db) -> int:
    now = datetime.now(timezone.utc)
    n = 0
    for sig in db.query(Signal).filter(Signal.status == "pending").all():
        close = sig.candle_close_time
        if close is None:
            continue
        if close.tzinfo is None:
            close = close.replace(tzinfo=timezone.utc)
        if close <= now:
            n += 1
    return n


def _job_resolve(reason: str = "boundary"):
    db = SessionLocal()
    try:
        due = _pending_due_count(db)
        if due == 0 and reason not in ("boundary", "interval"):
            return
        if due == 0 and reason == "interval":
            return
        n = services.resolve_pending_signals(db)
        if n:
            logger.info("Resolved %d signal(s) [%s]", n, reason)
        elif reason == "boundary":
            logger.debug("Resolve boundary — nothing due yet")
    except Exception:
        logger.exception("resolve job failed")
    finally:
        db.close()


def _job_resolve_boundary():
    _job_resolve(reason="boundary")
    _arm_catchup()


def _job_resolve_catchup():
    _job_resolve(reason="catchup")


def _job_resolve_interval():
    _job_resolve(reason="interval")


def _arm_catchup():
    global _scheduler
    if not _scheduler:
        return
    db = SessionLocal()
    try:
        pending = db.query(Signal).filter(Signal.status == "pending").count()
    finally:
        db.close()

    if pending == 0:
        return

    now = datetime.now(timezone.utc)
    for job in list(_scheduler.get_jobs()):
        if job.id and str(job.id).startswith("resolve_catchup_"):
            try:
                job.remove()
            except Exception:
                pass

    t = _CATCHUP_EVERY_SEC
    while t <= _CATCHUP_FOR_SEC:
        run_at = now + timedelta(seconds=t)
        jid = f"resolve_catchup_{int(run_at.timestamp())}"
        try:
            _scheduler.add_job(
                _job_resolve_catchup,
                DateTrigger(run_date=run_at),
                id=jid,
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )
        except Exception:
            logger.debug("catchup schedule skip %s", jid)
        t += _CATCHUP_EVERY_SEC


def _job_scan_and_watch():
    """
    Every 1s:
      - process_new_signals when L5 decision zone is active
      - retry watching fills until target candle close
    """
    try:
        from app.strategy_l5 import (
            DECISION_START_SEC as D5_START,
            DECISION_END_SEC as D5_END,
            _window_start as window5,
            WINDOW_SEC as W5,
        )
    except Exception:
        D5_START, D5_END, W5 = 232, 248, 300

        def window5(ts=None):
            import time as _t
            t = int(ts if ts is not None else _t.time())
            return (t // 300) * 300

    db = SessionLocal()
    try:
        now_ts = _time.time()
        w5 = window5(now_ts)
        elapsed5 = now_ts - w5

        in_5 = (D5_START - 5) <= elapsed5 <= (D5_END + 5)

        # Heartbeat L5 every second
        try:
            from app import strategy_l5 as _sc5
            _sc5.heartbeat_tick("BTCUSDT")
        except Exception:
            logger.debug("L5 heartbeat tick failed", exc_info=True)

        # New signals in L5 decision zone
        if in_5:
            created = services.process_new_signals(db)
            if created:
                logger.info(
                    "Scan (L5 t+%.0fs) — created %d signal(s)",
                    elapsed5, len(created),
                )
                try:
                    from app import main as main_mod
                    main_mod._last_scan["at"] = datetime.now(timezone.utc).isoformat()
                    main_mod._last_scan["created"] = len(created)
                except Exception:
                    pass

        # Watch retries for any open watching signals (until close)
        try:
            watched = services.retry_watching_signals(db)
            if watched:
                logger.info("Watch retry — updated %d signal(s)", len(watched))
        except Exception:
            logger.exception("retry_watching_signals failed")
        try:
            ures = services.retry_watching_user_trades(db)
            if ures:
                logger.info("User watch retry — %d update(s)", len(ures))
        except Exception:
            logger.exception("retry_watching_user_trades failed")
    except Exception:
        logger.exception("scan/watch job failed")
    finally:
        db.close()


def _job_daily_summary():
    db = SessionLocal()
    try:
        services.send_daily_summary(db)
    except Exception:
        logger.exception("daily summary job failed")
    finally:
        db.close()


def start_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        return _scheduler

    _scheduler = BackgroundScheduler(timezone="UTC")

    # Resolve at every 5m close
    _scheduler.add_job(
        _job_resolve_boundary,
        CronTrigger(minute="*", second=0),
        id="resolve_close_5m",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # 1s resolve poll
    _scheduler.add_job(
        _job_resolve_interval,
        IntervalTrigger(seconds=1),
        id="resolve_interval",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # Scan + watch every 1s
    _scheduler.add_job(
        _job_scan_and_watch,
        IntervalTrigger(seconds=_SCAN_INTERVAL_SEC),
        id="scan_and_watch",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # Daily Telegram summary at 00:05 UTC
    _scheduler.add_job(
        _job_daily_summary,
        CronTrigger(hour=0, minute=5),
        id="daily_summary",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    _scheduler.start()
    logger.info(
        "Scheduler started — L5 Value Lock (zone t+%d…%d of 5m window, running market)",
        232, 248,
    )
    return _scheduler


def stop_scheduler():
    global _scheduler
    if _scheduler:
        try:
            _scheduler.shutdown(wait=False)
        except Exception:
            pass
        _scheduler = None
        logger.info("Scheduler stopped")
