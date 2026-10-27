"""Polymarket strategy bot — FastAPI entrypoint."""
from __future__ import annotations

# Load .env before any app imports read os.environ
import os
from pathlib import Path
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent.parent
for _p in (
    _ROOT / ".env",
    Path.cwd() / ".env",
    Path("/app/.env"),
    Path("/home/ubuntu/.env"),
    Path("/home/ubuntu/polymarket-strategy/.env"),
    Path("/home/ec2-user/polymarket-dual/.env"),
    Path("/home/ec2-user/polymarket-strategy/.env"),
):
    if _p.is_file():
        load_dotenv(_p, override=False)
load_dotenv(override=False)

import csv
import io
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Depends, Request, Form, Query
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import init_db, get_db, SessionLocal
from app import services, scheduler as sched
from app import polymarket, strategy_l5 as strategy
from app.models import Signal, User
from app import telegram_notify as tg
from app import auth, multiuser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("polybot")

# In-memory last-scan diagnostics
_last_scan: dict = {"at": None, "created": 0, "raw": 0, "errors": []}


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    import logging as _log
    _log.getLogger(__name__).info(
        "POLYMARKET env at startup: key=%s funder=%s type=%s",
        "yes" if __import__("os").environ.get("POLYMARKET_PRIVATE_KEY") else "NO",
        (__import__("os").environ.get("POLYMARKET_FUNDER_ADDRESS") or "")[:12] or "NO",
        __import__("os").environ.get("POLYMARKET_SIGNATURE_TYPE") or "default",
    )
    db = SessionLocal()
    try:
        services.get_or_create_settings(db)
        from app.auth import ensure_seed_admin
        ensure_seed_admin(db)
    finally:
        db.close()
    sched.start_scheduler()
    # Do NOT process_new_signals on startup — that re-opens the current
    # 1h window on every deploy. Boundary + safety scans handle entries.
    logger.info("Startup complete — waiting for boundary scans")
    yield
    sched.stop_scheduler()
    logger.info("App stopped")


app = FastAPI(title="Polymarket Multi-TF Bot", lifespan=lifespan)
# Trust X-Forwarded-* from Fly edge proxy (HTTPS scheme, client IP)
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


def _ctx(request: Request, db: Session, page: str, **extra):
    """
    Shared template context.
    Admin → operator Signal/Setting + optional live caches.
    Non-admin → isolated UserTrade + UserSetting only (never operator keys/balance).
    """
    light = page in ("settings", "history", "live_history", "admin", "credentials", "dashboard")
    current_user = auth.get_current_user(request, db)
    is_admin = bool(current_user and getattr(current_user, "role", "") == "admin")

    if current_user and not is_admin:
        stats = services.user_dashboard_stats(db, current_user.id, skip_live=True)
    else:
        stats = services.dashboard_stats(db, skip_live=light)

    snap = None
    if not light and is_admin:
        try:
            snap = _merged_market_snapshot()
        except Exception as e:
            logger.warning("snapshot failed: %s", e)
            snap = {"error": str(e), "pairs": {}}
    base = {
        "request": request,
        "stats": stats,
        "page": page,
        "snapshot": snap or {"pairs": {}},
        "last_scan": _last_scan,
        "now": datetime.now(timezone.utc),
        "current_user": current_user,
        "is_admin": is_admin,
    }
    base.update(extra)
    return base


# ── Pages ────────────────────────────────────────────────────────────────────


def _merged_market_snapshot(force: bool = False) -> dict:
    """L5 Value Lock pulse snapshot."""
    snap = {"pairs": {}, "checked_at": None, "strategies": [], "errors": []}

    def _merge(data: dict, slot: str = "5m") -> None:
        if not data:
            return
        if data.get("strategy"):
            snap["strategies"].append(data.get("strategy"))
        if data.get("checked_at"):
            snap["checked_at"] = data.get("checked_at")
        for pair, pdata in (data.get("pairs") or {}).items():
            entry = snap["pairs"].setdefault(
                pair, {"price": pdata.get("price"), "15m": {"disabled": True}, "5m": {}, "1h": {}}
            )
            if pdata.get("price") is not None:
                entry["price"] = pdata.get("price")
            chunk = pdata.get("5m") or pdata.get(slot) or {}
            if isinstance(chunk, dict) and chunk:
                entry["5m"] = chunk
        if data.get("errors"):
            snap["errors"].extend(data["errors"])

    try:
        from app import strategy_l5 as s5
        _merge(s5.market_snapshot(force=True) or {}, slot="5m")
    except Exception as e:
        logger.exception("L5 market_snapshot failed")
        snap["errors"].append(f"l5:{type(e).__name__}:{e}")
        entry = snap["pairs"].setdefault("BTCUSDT", {"price": None, "15m": {"disabled": True}, "5m": {}, "1h": {}})
        entry["5m"]["error"] = str(e)

    return snap

@app.get("/", response_class=HTMLResponse)
@app.head("/")
def dashboard(request: Request, db: Session = Depends(get_db)):
    user = auth.get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    if user.status == "pending":
        return RedirectResponse(url="/pending", status_code=303)
    if auth.needs_password_setup(user):
        return RedirectResponse(url="/setup-password", status_code=303)
    if user.role != "admin" and not auth.user_has_access(user):
        return RedirectResponse(url="/login", status_code=303)
    if request.method == "HEAD":
        return Response(status_code=200)
    try:
        ctx = _ctx(request, db, "dashboard", recent=[])
    except Exception:
        logger.exception("dashboard context failed user=%s", getattr(user, "id", None))
        try:
            db.rollback()
        except Exception:
            pass
        from app.database import init_db
        try:
            init_db()
        except Exception:
            pass
        ctx = _ctx(request, db, "dashboard", recent=[])
    return templates.TemplateResponse("dashboard.html", ctx)


@app.get("/history", response_class=HTMLResponse)
def history(
    request: Request,
    db: Session = Depends(get_db),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    result: Optional[str] = None,
    timeframe: Optional[str] = None,
    strategy: Optional[str] = None,
    pairs: Optional[list[str]] = Query(None),
):
    df = dt = None
    if date_from:
        try:
            df = datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    if date_to:
        try:
            dt = datetime.fromisoformat(date_to).replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
        except ValueError:
            pass
    # Normalize pair list (support repeated ?pairs=BTC&pairs=ETH)
    pair_list = None
    if pairs:
        pair_list = [p.strip().upper().replace("USDT", "") for p in pairs if p and str(p).strip()]
        pair_list = [p for p in pair_list if p]
        if not pair_list:
            pair_list = None

    user = auth.get_current_user(request, db)
    if user and getattr(user, "role", "") != "admin":
        signals = services.list_user_trades(
            db, user.id,
            date_from=df, date_to=dt, result=result or None,
            timeframe=timeframe or None, pairs=pair_list,
            strategy=strategy or None, limit=500,
        )
        period = services.user_history_period_stats(
            db, user.id,
            date_from=df, date_to=dt, result=result or None,
            timeframe=timeframe or None, pairs=pair_list,
            strategy=strategy or None,
        )
    else:
        signals = services.list_signals(
            db,
            date_from=df,
            date_to=dt,
            result=result or None,
            timeframe=timeframe or None,
            pairs=pair_list,
            strategy=strategy or None,
            limit=500,
        )
        period = services.history_period_stats(
            db,
            date_from=df,
            date_to=dt,
            result=result or None,
            timeframe=timeframe or None,
            pairs=pair_list,
            strategy=strategy or None,
        )
    all_pairs = ["BTC"]  # late-window favorite is BTC-only
    return templates.TemplateResponse(
        "history.html",
        _ctx(
            request, db, "history",
            signals=signals,
            period=period,
            all_pairs=all_pairs,
            filters={
                "date_from": date_from or "",
                "date_to": date_to or "",
                "result": result or "",
                "timeframe": timeframe or "",
                "strategy": strategy or "",
                "pairs": pair_list or [],
            },
        ),
    )




@app.get("/live-history", response_class=HTMLResponse)
def live_history(
    request: Request,
    db: Session = Depends(get_db),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    result: Optional[str] = None,
    side: Optional[str] = None,
    pair: Optional[str] = None,
):
    """On-chain / CLOB trade history with filters."""
    df = dt = None
    if date_from:
        try:
            df = datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    if date_to:
        try:
            dt = datetime.fromisoformat(date_to).replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
        except ValueError:
            pass
    # Default: today UTC if no dates
    if df is None and dt is None:
        now = datetime.now(timezone.utc)
        df = now.replace(hour=0, minute=0, second=0, microsecond=0)
        date_from = df.strftime("%Y-%m-%d")

    user = auth.get_current_user(request, db)
    funder_override = None
    if user and getattr(user, "role", "") != "admin":
        from app.models import UserCredential
        cred = db.query(UserCredential).filter(UserCredential.user_id == user.id).first()
        funder_override = (cred.funder_address if cred else None) or ""
        if not funder_override.strip():
            activity = {
                "success": False,
                "error": "Add your funder address on Account before viewing live history",
                "trades": [],
                "count": 0,
                "volume_usd": 0.0,
                "realized_pnl": 0.0,
                "funder": None,
            }
        else:
            activity = services.live_history_trades(
                date_from=df,
                date_to=dt,
                side=side or None,
                result=result or None,
                pair=pair or None,
                limit=500,
                funder=funder_override,
            )
    else:
        activity = services.live_history_trades(
            date_from=df,
            date_to=dt,
            side=side or None,
            result=result or None,
            pair=pair or None,
            limit=500,
        )
    return templates.TemplateResponse(
        "live_history.html",
        _ctx(
            request, db, "live_history",
            activity=activity,
            filters={
                "date_from": date_from or "",
                "date_to": date_to or "",
                "result": result or "",
                "side": side or "",
                "pair": pair or "",
            },
        ),
    )


@app.get("/history/export")
def history_export(
    request: Request,
    db: Session = Depends(get_db),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    result: Optional[str] = None,
    timeframe: Optional[str] = None,
    strategy: Optional[str] = None,
    pairs: Optional[list[str]] = Query(None),
    fmt: str = Query("csv", pattern="^(csv|json)$"),
):
    """Download full signal history matching current filters (for analysis / sharing)."""
    user = auth.get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    df = dt = None
    if date_from:
        try:
            df = datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    if date_to:
        try:
            dt = datetime.fromisoformat(date_to).replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
        except ValueError:
            pass
    pair_list = None
    if pairs:
        pair_list = [p.strip().upper().replace("USDT", "") for p in pairs if p and str(p).strip()]
        pair_list = [p for p in pair_list if p] or None

    # Same data source as /history: non-admin users get their own trades
    if getattr(user, "role", "") != "admin":
        rows = services.list_user_trades(
            db, user.id,
            date_from=df, date_to=dt, result=result or None,
            timeframe=timeframe or None, pairs=pair_list,
            strategy=strategy or None, limit=20000,
        )
    else:
        rows = services.list_signals(
            db,
            date_from=df,
            date_to=dt,
            result=result or None,
            timeframe=timeframe or None,
            pairs=pair_list,
            strategy=strategy or None,
            limit=20000,
        )
    # Oldest first for chronological analysis
    rows = list(reversed(rows))

    def _iso(v):
        if v is None:
            return ""
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            return v.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        return str(v)

    fields = [
        "id", "pair", "timeframe", "strategy_id", "direction", "signal_time", "entry_time",
        "candle_close_time", "signal_price", "mode", "order_type", "position_size_usd",
        "status", "result", "pnl", "entry_price", "polymarket_slug",
        "condition_id", "polymarket_order_id", "token_id", "error_message",
        "resolved_at", "created_at",
    ]

    def _g(obj, name, default=None):
        return getattr(obj, name, default)

    if fmt == "json":
        payload = []
        for s in rows:
            payload.append({
                "id": _g(s, "id"),
                "pair": _g(s, "pair"),
                "timeframe": _g(s, "timeframe"),
                "strategy_id": _g(s, "strategy_id"),
                "direction": _g(s, "direction"),
                "signal_time": _iso(_g(s, "signal_time")),
                "entry_time": _iso(_g(s, "entry_time")),
                "candle_close_time": _iso(_g(s, "candle_close_time")),
                "signal_price": _g(s, "signal_price"),
                "mode": _g(s, "mode"),
                "order_type": _g(s, "order_type"),
                "position_size_usd": _g(s, "position_size_usd"),
                "status": _g(s, "status"),
                "result": _g(s, "result"),
                "pnl": _g(s, "pnl"),
                "entry_price": _g(s, "entry_price"),
                "polymarket_slug": _g(s, "polymarket_slug"),
                "condition_id": _g(s, "condition_id"),
                "polymarket_order_id": _g(s, "polymarket_order_id"),
                "token_id": _g(s, "token_id"),
                "error_message": _g(s, "error_message"),
                "resolved_at": _iso(_g(s, "resolved_at")),
                "created_at": _iso(_g(s, "created_at")),
            })
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return JSONResponse(
            content={"count": len(payload), "exported_at": stamp, "signals": payload},
            headers={
                "Content-Disposition": f'attachment; filename="signal_history_{stamp}.json"'
            },
        )

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for s in rows:
        sp = _g(s, "signal_price")
        ep = _g(s, "entry_price")
        psz = _g(s, "position_size_usd")
        pnl = _g(s, "pnl")
        err = _g(s, "error_message") or ""
        writer.writerow({
            "id": _g(s, "id") or "",
            "pair": _g(s, "pair") or "",
            "timeframe": _g(s, "timeframe") or "",
            "strategy_id": _g(s, "strategy_id") or "",
            "direction": _g(s, "direction") or "",
            "signal_time": _iso(_g(s, "signal_time")),
            "entry_time": _iso(_g(s, "entry_time")),
            "candle_close_time": _iso(_g(s, "candle_close_time")),
            "signal_price": sp if sp is not None else "",
            "mode": _g(s, "mode") or "",
            "order_type": _g(s, "order_type") or "",
            "position_size_usd": psz if psz is not None else "",
            "status": _g(s, "status") or "",
            "result": _g(s, "result") or "",
            "pnl": pnl if pnl is not None else "",
            "entry_price": ep if ep is not None else "",
            "polymarket_slug": _g(s, "polymarket_slug") or "",
            "condition_id": _g(s, "condition_id") or "",
            "polymarket_order_id": _g(s, "polymarket_order_id") or "",
            "token_id": _g(s, "token_id") or "",
            "error_message": str(err).replace("\n", " ").replace("\r", " "),
            "resolved_at": _iso(_g(s, "resolved_at")),
            "created_at": _iso(_g(s, "created_at")),
        })
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    data = buf.getvalue()
    return Response(
        content=data,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="signal_history_{stamp}.csv"'
        },
    )



@app.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request, db: Session = Depends(get_db)):
    user = auth.get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    try:
        ctx = _ctx(request, db, "settings")
    except Exception:
        logger.exception("settings_page context failed")
        try:
            db.rollback()
        except Exception:
            pass
        ctx = _ctx(request, db, "settings")
    return templates.TemplateResponse("settings.html", ctx)


@app.post("/settings")
def settings_save(
    request: Request,
    db: Session = Depends(get_db),
    mode: str = Form("shadow"),
    order_type: str = Form("FAK"),
    position_size_usd: float = Form(1.0),
    scale_entries: int = Form(1),
    max_contract_price: float = Form(0.95),
    enabled_15m: Optional[str] = Form(None),
    enabled_5m: Optional[str] = Form(None),
    enabled_1h: Optional[str] = Form(None),
    enabled_btc: Optional[str] = Form(None),
    enabled_eth: Optional[str] = Form(None),
):
    try:
        position_size_usd = max(1.0, round(float(position_size_usd), 2))
    except (TypeError, ValueError):
        position_size_usd = 1.0
    try:
        scale_entries = int(scale_entries)
    except (TypeError, ValueError):
        scale_entries = 1
    if scale_entries not in (1, 5):
        scale_entries = 1
    if order_type not in ("FAK", "GTC"):
        order_type = "FAK"
    if mode not in ("shadow", "live"):
        mode = "shadow"
    user = auth.get_current_user(request, db)
    if user and getattr(user, "role", "") != "admin":
        us = multiuser.get_user_setting(db, user.id)
        us.mode = mode
        us.order_type = order_type
        us.position_size_usd = position_size_usd
        us.scale_entries = scale_entries
        us.max_contract_price = max(0.01, min(0.99, float(max_contract_price)))
        us.enabled_btc = enabled_btc == "on"
        us.enabled_eth = enabled_eth == "on"
        db.commit()
    else:
        if mode == "live" and not polymarket.credentials_status().get("live_ready"):
            mode = "shadow"
            return RedirectResponse("/settings?live_blocked=1", status_code=303)
        services.update_settings(
            db,
            mode=mode,
            order_type=order_type,
            position_size_usd=position_size_usd,
            scale_entries=scale_entries,
            max_contract_price=max(0.01, min(0.99, float(max_contract_price))),
            enabled_15m=enabled_15m == "on",
            enabled_5m=enabled_5m == "on",
            enabled_1h=enabled_1h == "on",
            enabled_btc=enabled_btc == "on",
            enabled_eth=enabled_eth == "on",
        )
    return RedirectResponse("/settings?saved=1", status_code=303)



@app.post("/settings/toggle-mode")
def toggle_mode(request: Request, db: Session = Depends(get_db)):
    """Flip shadow ↔ live for THIS user (or operator if admin)."""
    user = auth.get_current_user(request, db)
    is_admin = bool(user and getattr(user, "role", "") == "admin")
    hx = request.headers.get("hx-request")
    accept = (request.headers.get("accept") or "")

    if user and not is_admin:
        us = multiuser.get_user_setting(db, user.id)
        current = (us.mode or "shadow").lower()
        if current == "live":
            new_mode = "shadow"
        else:
            # Live: funder + (Builder-authorized session OR managed owner key)
            ctx = multiuser.get_user_trading_context(db, user.id)
            live_ok = bool(ctx and ctx.get("live_ready"))
            if not live_ok:
                if hx or "application/json" in accept:
                    return JSONResponse(
                        {
                            "ok": False,
                            "mode": "shadow",
                            "error": (
                                "Live needs managed wallet (Setup managed wallet) or "
                                "funder + session. Builder 403 is OK — owner signer is used."
                            ),
                        },
                        status_code=400,
                    )
                return RedirectResponse("/settings?live_blocked=1", status_code=303)
            new_mode = "live"
        us.mode = new_mode
        db.commit()
        db.refresh(us)
        if hx:
            stats = services.user_dashboard_stats(db, user.id)
            return templates.TemplateResponse(
                "partials/mode_badge.html",
                {"request": request, "stats": stats},
            )
        ref = request.headers.get("referer") or "/"
        return RedirectResponse(ref, status_code=303)

    # Admin / operator path
    s = services.get_or_create_settings(db)
    current = (s.mode or "shadow").lower()
    if current == "live":
        new_mode = "shadow"
    else:
        if not polymarket.credentials_status().get("live_ready"):
            if hx or "application/json" in accept:
                return JSONResponse(
                    {
                        "ok": False,
                        "mode": "shadow",
                        "error": "Live requires POLYMARKET_PRIVATE_KEY and POLYMARKET_FUNDER_ADDRESS",
                    },
                    status_code=400,
                )
            return RedirectResponse("/settings?live_blocked=1", status_code=303)
        new_mode = "live"
    services.update_settings(db, mode=new_mode)
    if hx:
        stats = services.dashboard_stats(db, skip_live=True)
        return templates.TemplateResponse(
            "partials/mode_badge.html",
            {"request": request, "stats": stats},
        )
    ref = request.headers.get("referer") or "/"
    return RedirectResponse(ref, status_code=303)


@app.post("/settings/reset-balance")
def reset_balance(db: Session = Depends(get_db)):
    services.reset_shadow_balance(db, 50.0)
    return RedirectResponse("/settings?reset=1", status_code=303)


@app.post("/scan")
def force_scan(db: Session = Depends(get_db)):
    created = services.process_new_signals(db)
    _last_scan["at"] = datetime.now(timezone.utc).isoformat()
    _last_scan["created"] = len(created)
    return RedirectResponse("/?scanned=1", status_code=303)


# ── API ──────────────────────────────────────────────────────────────────────

@app.get("/api/stats")
def api_stats(db: Session = Depends(get_db)):
    return services.dashboard_stats(db)


@app.get("/api/snapshot")
def api_snapshot():
    return _merged_market_snapshot()


@app.get("/api/signals")
def api_signals(
    db: Session = Depends(get_db),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    result: Optional[str] = None,
    timeframe: Optional[str] = None,
    limit: int = Query(100, le=500),
):
    df = dt = None
    if date_from:
        try:
            df = datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    if date_to:
        try:
            dt = datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    rows = services.list_signals(db, df, dt, result, timeframe, limit=limit)
    return [
        {
            "id": s.id,
            "pair": s.pair,
            "timeframe": s.timeframe,
            "strategy_id": getattr(s, "strategy_id", None) or "",
            "direction": s.direction,
            "entry_time": s.entry_time.isoformat() if s.entry_time else None,
            "status": s.status,
            "result": s.result,
            "pnl": s.pnl,
            "rsi": None, "signal_price": getattr(s, "signal_price", None),
            "mode": s.mode,
            "order_type": s.order_type,
            "position_size_usd": s.position_size_usd,
            "slug": s.polymarket_slug,
        }
        for s in rows
    ]


@app.post("/api/scan")
def api_scan(db: Session = Depends(get_db)):
    created = services.process_new_signals(db)
    _last_scan["at"] = datetime.now(timezone.utc).isoformat()
    _last_scan["created"] = len(created)
    return {"created": len(created), "ids": [s.id for s in created], "at": _last_scan["at"]}


@app.post("/api/resolve")
def api_resolve(db: Session = Depends(get_db)):
    n = services.resolve_pending_signals(db)
    return {"resolved": n}


@app.get("/api/balance")
def api_balance(db: Session = Depends(get_db)):
    # Bust short caches so this endpoint reflects current .env / RPC
    services._live_bal_cache["ts"] = 0.0
    services._live_trades_cache["ts"] = 0.0
    services._stats_cache["data"] = None
    stats = services.dashboard_stats(db, skip_live=False)
    return {
        "shadow_balance": stats["settings"]["shadow_balance"],
        "mode": stats["settings"]["mode"],
        "live_balance": stats.get("live_balance"),
        "live_onchain_daily_pnl": stats.get("live_onchain_daily_pnl"),
        "live_day_start_pusd": stats.get("live_day_start_pusd"),
        "today_pnl_live": stats.get("today", {}).get("pnl_live"),
        "today_pnl_shadow": stats.get("today", {}).get("pnl_shadow"),
        "live_activity": stats.get("live_activity"),
        "credentials": stats.get("credentials"),
    }



# ── HTMX live partials (light — no Binance stampede) ──────────────────────

@app.get("/partials/stats", response_class=HTMLResponse)
def partial_stats(request: Request, db: Session = Depends(get_db)):
    # skip_live=False still serves LIVE fields from cache only (no blocking RPC/CLOB)
    user = auth.get_current_user(request, db)
    if user and getattr(user, "role", "") != "admin":
        stats = services.user_dashboard_stats(db, user.id)
    else:
        stats = services.dashboard_stats(db, skip_live=False)
    # kick background refresh so numbers catch up without slowing this response
    try:
        services.refresh_live_caches_async()
    except Exception:
        pass
    return templates.TemplateResponse(
        "partials/stats.html",
        {"request": request, "stats": stats, "page": "dashboard"},
    )


@app.get("/partials/pulse", response_class=HTMLResponse)
def partial_pulse(request: Request, db: Session = Depends(get_db)):
    # Market pulse — always return structured snapshot (never blank "Loading")
    try:
        snap = _merged_market_snapshot()
    except Exception as e:
        logger.exception("pulse snapshot failed")
        snap = {"pairs": {}, "checked_at": None, "strategies": [], "errors": [str(e)]}
    if not isinstance(snap, dict):
        snap = {"pairs": {}, "errors": ["invalid snapshot"]}
    snap.setdefault("pairs", {})
    snap.setdefault("errors", [])
    return templates.TemplateResponse(
        "partials/pulse.html",
        {
            "request": request,
            "snapshot": snap,
            "last_scan": _last_scan,
            "page": "dashboard",
        },
    )


@app.get("/partials/recent", response_class=HTMLResponse)
def partial_recent(request: Request, db: Session = Depends(get_db)):
    user = auth.get_current_user(request, db)
    if user and getattr(user, "role", "") != "admin":
        recent = services.list_user_trades(db, user.id, limit=20)
    else:
        recent = services.list_signals(db, limit=20)
    return templates.TemplateResponse(
        "partials/recent.html",
        {"request": request, "recent": recent, "page": "dashboard", "current_user": user},
    )


@app.get("/partials/alltime", response_class=HTMLResponse)
def partial_alltime(request: Request, db: Session = Depends(get_db)):
    user = auth.get_current_user(request, db)
    if user and getattr(user, "role", "") != "admin":
        stats = services.user_dashboard_stats(db, user.id)
    else:
        stats = services.dashboard_stats(db, skip_live=True)
    return templates.TemplateResponse(
        "partials/alltime.html",
        {"request": request, "stats": stats, "page": "dashboard"},
    )


@app.get("/partials/mode-badge", response_class=HTMLResponse)
def partial_mode_badge(request: Request, db: Session = Depends(get_db)):
    user = auth.get_current_user(request, db)
    if user and getattr(user, "role", "") != "admin":
        stats = services.user_dashboard_stats(db, user.id)
    else:
        stats = services.dashboard_stats(db, skip_live=True)
    return templates.TemplateResponse(
        "partials/mode_badge.html",
        {"request": request, "stats": stats},
    )



@app.post("/api/debug/order")
@app.get("/api/debug/order")
def debug_order(
    request: Request,
    pair: str = "BTC",
    timeframe: str = "15m",
    direction: str = "DOWN",
    size: float = 1.0,
    order_type: str = "FAK",
    live: bool = False,
    db: Session = Depends(get_db),
):
    """
    Dry-run (default) or live test order path. Always posts full result to Telegram.
    Query/body params: pair, timeframe, direction, size, order_type, live=0|1
    """
    import json as _json
    settings = services.get_or_create_settings(db)
    symbol = pair.upper().replace("USDT", "") + "USDT"
    direction = direction.upper()
    timeframe = timeframe if timeframe in ("5m", "15m", "1h") else "15m"
    order_type = (order_type or settings.order_type or "FAK").upper()
    size = max(1.0, round(float(size or settings.position_size_usd or 1.0), 2))
    max_px = float(settings.max_contract_price or 0.95)

    report = {
        "step": "start",
        "symbol": symbol,
        "direction": direction,
        "timeframe": timeframe,
        "order_type": order_type,
        "size_usd": size,
        "max_contract_price": max_px,
        "live_requested": bool(live),
        "creds": polymarket.credentials_status(),
    }

    try:
        market = polymarket.discover_market(symbol, timeframe=timeframe)
        report["market"] = {
            "found": bool(market),
            "slug": (market or {}).get("slug"),
            "condition_id": (market or {}).get("condition_id"),
            "up_token": (market or {}).get("up_token_id"),
            "down_token": (market or {}).get("down_token_id"),
            "tick_size": (market or {}).get("tick_size"),
        }
        if not market:
            report["step"] = "discover_failed"
            tg_res = tg.alert_debug_blob("DEBUG order — no market", report)
            report["telegram"] = tg_res
            return JSONResponse(report, status_code=404)

        token = market["up_token_id"] if direction == "UP" else market["down_token_id"]
        book = polymarket.get_order_book(str(token))
        report["book"] = {
            "error": book.get("error"),
            "tick_size": book.get("tick_size"),
            "min_order_size": book.get("min_order_size"),
            "asks_top5": (book.get("asks") or [])[:5],
            "bids_top5": (book.get("bids") or [])[:5],
        }
        est = polymarket.estimate_buy_market_price(book.get("asks") or [], size, order_type)
        report["estimate"] = est
        report["step"] = "priced"

        if not live:
            report["step"] = "dry_run_ok"
            report["note"] = "Set live=1 to actually POST a FAK/GTC order (real money if live mode)."
            tg_res = tg.alert_debug_blob("DEBUG order dry-run", report)
            report["telegram"] = tg_res
            return JSONResponse(report)

        # Live path — real order
        result = polymarket.place_live_order(
            symbol=symbol,
            direction=direction,
            position_size_usd=size,
            timeframe=timeframe,
            order_type=order_type,
            max_contract_price=max_px,
        )
        report["step"] = "live_result"
        report["result"] = result
        title = "DEBUG order LIVE OK" if result.get("success") else "DEBUG order LIVE FAIL"
        tg_res = tg.alert_debug_blob(title, report)
        report["telegram"] = tg_res
        return JSONResponse(report, status_code=200 if result.get("success") else 400)
    except Exception as e:
        logger.exception("debug_order failed")
        report["step"] = "exception"
        report["error"] = str(e)
        tg_res = tg.alert_debug_blob("DEBUG order EXCEPTION", report)
        report["telegram"] = tg_res
        return JSONResponse(report, status_code=500)


@app.get("/api/debug/last-error")
def debug_last_error(db: Session = Depends(get_db)):
    """Return the most recent signal with status=error and Telegram it."""
    sig = (
        db.query(Signal)
        .filter(Signal.status == "error")
        .order_by(Signal.created_at.desc())
        .first()
    )
    if not sig:
        payload = {"found": False, "message": "No error signals in DB"}
        tg.alert_debug_blob("DEBUG last-error", payload)
        return JSONResponse(payload)
    payload = {
        "found": True,
        "id": sig.id,
        "pair": sig.pair,
        "timeframe": sig.timeframe,
        "direction": sig.direction,
        "status": sig.status,
        "error_message": sig.error_message,
        "mode": sig.mode,
        "order_type": sig.order_type,
        "position_size_usd": sig.position_size_usd,
        "slug": sig.polymarket_slug,
        "order_id": sig.polymarket_order_id,
        "entry_time": str(sig.entry_time),
        "created_at": str(sig.created_at),
    }
    tg.alert_debug_blob("DEBUG last-error signal", payload)
    return JSONResponse(payload)


@app.get("/health")
@app.head("/health")
def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}



# ── Auth (public user login — no admin exposure) ──────────────────────────────


@app.exception_handler(Exception)
async def _unhandled(request: Request, exc: Exception):
    """Log full traceback; avoid blank Internal Server Error with no log line."""
    logger.exception("UNHANDLED %s %s: %s", request.method, request.url.path, exc)
    path = request.url.path or ""
    if path.startswith("/api/") or "application/json" in (request.headers.get("accept") or ""):
        return JSONResponse({"error": str(exc)[:300]}, status_code=500)
    # Auth pages: show message instead of blank 500
    if path in ("/login", "/login/register", "/setup-password"):
        try:
            return templates.TemplateResponse(
                "login.html",
                {"request": request, "error": f"Server error: {str(exc)[:150]}", "info": None, "page": "login"},
                status_code=500,
            )
        except Exception:
            pass
    return HTMLResponse(
        f"<html><body style='font-family:system-ui;background:#0b1220;color:#e2e8f0;padding:24px'>"
        f"<h1>Something went wrong</h1><p>{str(exc)[:200]}</p>"
        f"<p><a href='/login' style='color:#60a5fa'>Back to login</a></p></body></html>",
        status_code=500,
    )


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, db: Session = Depends(get_db)):
    user = auth.get_current_user(request, db)
    if user and user.role == "admin":
        # Admins use /admin/login — don't mix into public app
        return RedirectResponse(url="/admin/users", status_code=303)
    if user and auth.user_has_access(user):
        return RedirectResponse(url="/", status_code=303)
    if user and user.status == "pending":
        return RedirectResponse(url="/pending", status_code=303)
    if user and auth.needs_password_setup(user):
        return RedirectResponse(url="/setup-password", status_code=303)
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "error": None, "info": None, "page": "login"},
    )


@app.post("/login/register")
def login_register(
    request: Request,
    db: Session = Depends(get_db),
    username: str = Form(...),
):
    """Self-register with username only → pending approval."""
    try:
        u = multiuser.register_username(db, username)
    except ValueError as e:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": str(e), "info": None, "page": "login", "prefill": username},
            status_code=400,
        )
    except Exception as e:
        logging.getLogger(__name__).exception("register failed")
        try:
            db.rollback()
        except Exception:
            pass
        # Attempt schema migrate then one retry
        try:
            from app.database import init_db
            init_db()
            u = multiuser.register_username(db, username)
        except Exception as e2:
            logging.getLogger(__name__).exception("register retry failed")
            return templates.TemplateResponse(
                "login.html",
                {
                    "request": request,
                    "error": f"Registration temporarily unavailable. Admin: check DB migration. ({str(e2)[:120]})",
                    "info": None,
                    "page": "login",
                    "prefill": username,
                },
                status_code=500,
            )
    if u.status == "pending":
        token = auth.create_session(u.id)
        resp = RedirectResponse(url="/pending", status_code=303)
        auth.set_session_cookie(resp, token, request=request)
        return resp
    if u.status == "active" and not u.password_hash:
        token = auth.create_session(u.id)
        resp = RedirectResponse(url="/setup-password", status_code=303)
        auth.set_session_cookie(resp, token, request=request)
        return resp
    # Already active with password — ask them to sign in
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error": None,
            "info": "Account already approved. Sign in with your password.",
            "page": "login",
            "prefill": u.username,
        },
    )


@app.post("/login")
def login_submit(
    request: Request,
    db: Session = Depends(get_db),
    username: str = Form(...),
    password: str = Form(...),
):
    """User login — never 500; show error on login page instead."""
    from app.models import utcnow as _utcnow
    try:
        uname = (username or "").strip().lower()
        u = db.query(User).filter(User.username == uname).first()
        err = None
        if not u:
            err = "Account not found. Register with your username first."
        elif u.role == "admin":
            err = "Invalid username or password"
        elif u.status == "pending":
            token = auth.create_session(u.id)
            resp = RedirectResponse(url="/pending", status_code=303)
            auth.set_session_cookie(resp, token, request=request)
            return resp
        elif not (u.password_hash or "").strip():
            if u.status == "active":
                token = auth.create_session(u.id)
                resp = RedirectResponse(url="/setup-password", status_code=303)
                auth.set_session_cookie(resp, token, request=request)
                return resp
            err = "Access denied"
        elif not auth.verify_password(password, u.password_hash):
            err = "Invalid username or password"
        elif not auth.user_has_access(u):
            err = f"Access denied (status: {u.status})"
        if err:
            return templates.TemplateResponse(
                "login.html",
                {"request": request, "error": err, "info": None, "page": "login", "prefill": uname},
                status_code=401,
            )
        try:
            u.last_login_at = _utcnow()
            db.commit()
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
            logger.warning("last_login_at update skipped", exc_info=True)
        token = auth.create_session(u.id)
        # Active users without password go setup; else home
        dest = "/setup-password" if auth.needs_password_setup(u) else "/"
        resp = RedirectResponse(url=dest, status_code=303)
        auth.set_session_cookie(resp, token, request=request)
        return resp
    except Exception as e:
        logger.exception("login_submit failed")
        try:
            db.rollback()
        except Exception:
            pass
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": f"Login temporarily unavailable: {str(e)[:120]}",
                "info": None,
                "page": "login",
                "prefill": (username or "").strip().lower(),
            },
            status_code=500,
        )


@app.get("/pending", response_class=HTMLResponse)
def pending_page(request: Request, db: Session = Depends(get_db)):
    user = auth.get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    if user.role == "admin":
        return RedirectResponse(url="/admin/users", status_code=303)
    if user.status == "active":
        if auth.needs_password_setup(user):
            return RedirectResponse(url="/setup-password", status_code=303)
        return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse(
        "pending.html",
        {"request": request, "user": user, "page": "pending"},
    )


@app.get("/setup-password", response_class=HTMLResponse)
def setup_password_page(request: Request, db: Session = Depends(get_db)):
    user = auth.get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    if user.status != "active":
        return RedirectResponse(url="/pending", status_code=303)
    if user.password_hash:
        return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse(
        "setup_password.html",
        {"request": request, "user": user, "error": None, "page": "setup"},
    )


@app.post("/setup-password")
def setup_password_submit(
    request: Request,
    db: Session = Depends(get_db),
    password: str = Form(...),
    password2: str = Form(...),
):
    user = auth.get_current_user(request, db)
    if not user or user.status != "active":
        return RedirectResponse(url="/login", status_code=303)
    if password != password2:
        return templates.TemplateResponse(
            "setup_password.html",
            {"request": request, "user": user, "error": "Passwords do not match", "page": "setup"},
            status_code=400,
        )
    try:
        multiuser.set_user_password(db, user, password)
    except ValueError as e:
        return templates.TemplateResponse(
            "setup_password.html",
            {"request": request, "user": user, "error": str(e), "page": "setup"},
            status_code=400,
        )
    return RedirectResponse(url="/me/credentials?welcome=1", status_code=303)


@app.get("/logout")
@app.post("/logout")
def logout(request: Request):
    """End session for admin and regular users."""
    token = request.cookies.get(auth.SESSION_COOKIE)
    auth.destroy_session(token)
    # Prefer admin login if they came from admin, else user login
    ref = (request.headers.get("referer") or "")
    dest = "/admin/login" if "/admin" in ref else "/login"
    resp = RedirectResponse(url=dest, status_code=303)
    resp.delete_cookie(auth.SESSION_COOKIE, path="/")
    # Also clear legacy default name if present (older deploys)
    resp.delete_cookie("polybot_session", path="/")
    return resp


# ── Admin (hidden — only /admin/*, never linked in user UI) ───────────────────

@app.get("/admin")
def admin_root():
    return RedirectResponse(url="/admin/users", status_code=303)


@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_page(request: Request, db: Session = Depends(get_db)):
    user = auth.get_current_user(request, db)
    if user and user.role == "admin" and user.status != "suspended":
        return RedirectResponse(url="/admin/users", status_code=303)
    return templates.TemplateResponse(
        "admin_login.html",
        {"request": request, "error": None, "page": "admin_login"},
    )


@app.post("/admin/login")
def admin_login_submit(
    request: Request,
    db: Session = Depends(get_db),
    username: str = Form(...),
    password: str = Form(...),
):
    from app.models import utcnow as _utcnow
    u = db.query(User).filter(User.username == username.strip().lower()).first()
    err = None
    if not u or u.role != "admin" or not auth.verify_password(password, u.password_hash):
        err = "Invalid credentials"
    elif u.status == "suspended":
        err = "Account suspended"
    if err:
        return templates.TemplateResponse(
            "admin_login.html",
            {"request": request, "error": err, "page": "admin_login"},
            status_code=401,
        )
    u.last_login_at = _utcnow()
    db.commit()
    token = auth.create_session(u.id)
    resp = RedirectResponse(url="/admin/users", status_code=303)
    auth.set_session_cookie(resp, token, request=request)
    return resp


def _admin_ctx(request, db, admin, **extra):
    ctx = {
        "request": request,
        "page": "admin",
        "admin": admin,
        "current_user": admin,
        "is_admin": True,
        "stats": services.dashboard_stats(db, skip_live=True),
        "snapshot": {"pairs": {}},
        "last_scan": _last_scan,
        "now": datetime.now(timezone.utc),
    }
    ctx.update(extra)
    return ctx


@app.get("/admin/users", response_class=HTMLResponse)
def admin_users(request: Request, db: Session = Depends(get_db)):
    redir = auth.admin_login_redirect_if_needed(request, db)
    if redir:
        return redir
    admin = auth.get_current_user(request, db)
    rows = multiuser.admin_user_rows(db)
    return templates.TemplateResponse(
        "admin_users.html",
        _admin_ctx(request, db, admin, rows=rows, msg=request.query_params.get("msg")),
    )


@app.get("/admin/users/{user_id}", response_class=HTMLResponse)
def admin_user_edit(request: Request, user_id: int, db: Session = Depends(get_db)):
    redir = auth.admin_login_redirect_if_needed(request, db)
    if redir:
        return redir
    admin = auth.get_current_user(request, db)
    user = multiuser.get_user(db, user_id)
    if not user or user.role == "admin":
        return RedirectResponse(url="/admin/users", status_code=303)
    stats = multiuser.user_trade_stats(db, user.id)
    return templates.TemplateResponse(
        "admin_user_form.html",
        _admin_ctx(request, db, admin, user=user, user_stats=stats, error=None),
    )


@app.post("/admin/users/{user_id}")
def admin_user_update(
    request: Request,
    user_id: int,
    db: Session = Depends(get_db),
    display_name: str = Form(""),
    status: str = Form("active"),
    access_from: str = Form(""),
    access_to: str = Form(""),
    notes: str = Form(""),
    clear_access_to: Optional[str] = Form(None),
):
    redir = auth.admin_login_redirect_if_needed(request, db)
    if redir:
        return redir
    admin = auth.get_current_user(request, db)
    user = multiuser.get_user(db, user_id)
    if not user or user.role == "admin":
        return RedirectResponse(url="/admin/users", status_code=303)
    af = multiuser.parse_dt(access_from)
    at = multiuser.parse_dt(access_to)
    multiuser.update_user(
        db,
        user,
        display_name=display_name or user.username,
        status=status,
        access_from=af,
        access_to=at,
        clear_access_to=bool(clear_access_to),
        notes=notes or None,
    )
    return RedirectResponse(url="/admin/users?msg=Updated+" + user.username, status_code=303)


@app.post("/admin/users/{user_id}/approve")
def admin_user_approve(request: Request, user_id: int, db: Session = Depends(get_db)):
    redir = auth.admin_login_redirect_if_needed(request, db)
    if redir:
        return redir
    user = multiuser.get_user(db, user_id)
    if user and user.role != "admin":
        multiuser.approve_user(db, user, status="active")
    return RedirectResponse(url="/admin/users?msg=Approved", status_code=303)



@app.get("/debug/env")
def debug_env(request: Request, db: Session = Depends(get_db)):
    """Admin-only: show whether env keys are loaded (no secret values)."""
    user = auth.get_current_user(request, db)
    if not user or getattr(user, "role", "") != "admin":
        return JSONResponse({"error": "admin only"}, status_code=403)
    import os
    def present(name: str) -> bool:
        return bool(os.environ.get(name, "").strip())
    st = polymarket.credentials_status()
    return JSONResponse({
        "cwd": __import__("os").getcwd(),
        "env_flags": {
            "POLYMARKET_PRIVATE_KEY": present("POLYMARKET_PRIVATE_KEY"),
            "POLYMARKET_FUNDER_ADDRESS": present("POLYMARKET_FUNDER_ADDRESS"),
            "POLYMARKET_SIGNATURE_TYPE": os.environ.get("POLYMARKET_SIGNATURE_TYPE", ""),
            "APP_SECRET": present("APP_SECRET"),
            "TELEGRAM_BOT_TOKEN": present("TELEGRAM_BOT_TOKEN"),
            "POLYMARKET_BUILDER_API_KEY": present("POLYMARKET_BUILDER_API_KEY"),
        },
        "credentials_status": st,
    })



@app.get("/me/account", response_class=HTMLResponse)
def my_account_simple(request: Request, db: Session = Depends(get_db)):
    """Account summary — user sees only their session/funder; admin sees operator env."""
    redir = auth.login_redirect_if_needed(request, db)
    if redir:
        return redir
    user = auth.get_current_user(request, db)
    is_admin = bool(user and getattr(user, "role", "") == "admin")

    if not is_admin:
        # Redirect users to full credentials UI (session key flow)
        return RedirectResponse(url="/me/credentials", status_code=303)

    st = polymarket.credentials_status()
    html = f"""<!DOCTYPE html><html><head><meta name=viewport content="width=device-width,initial-scale=1">
    <title>Account</title>
    <style>
      body{{font-family:system-ui;background:#0b1220;color:#e2e8f0;padding:16px;max-width:480px;margin:0 auto}}
      a{{color:#60a5fa}} .ok{{color:#34d399}} .bad{{color:#fbbf24}}
      code{{background:#1e293b;padding:2px 6px;border-radius:4px;font-size:12px}}
      .card{{background:#151c2e;border:1px solid #334155;border-radius:12px;padding:16px;margin:12px 0}}
    </style></head><body>
    <p><a href="/">← Dashboard</a></p>
    <h1>Account (operator)</h1>
    <p>{user.display_name if user else ""} · {user.username if user else ""}</p>
    <div class="card">
      <h3>Operator live keys (.env)</h3>
      <p>Private key: <span class="{'ok' if st.get('private_key_set') else 'bad'}">{'Set' if st.get('private_key_set') else 'Not set'}</span></p>
      <p>Funder: <code>{st.get('funder') or '—'}</code></p>
      <p>Signature type: {st.get('signature_type')} ({st.get('signature_type_name')})</p>
      <p>Live ready: <span class="{'ok' if st.get('live_ready') else 'bad'}">{'Yes' if st.get('live_ready') else 'No'}</span></p>
      <p style="font-size:12px;color:#94a3b8">{st.get('note','')}</p>
    </div>
    <div class="card">
      <p style="font-size:12px">Full setup UI: <a href="/me/credentials">/me/credentials</a></p>
      <p style="font-size:12px">Debug: <a href="/debug/env">/debug/env</a></p>
      <p style="margin-top:16px"><a href="/logout">Log out</a></p>
    </div>
    </body></html>"""
    return HTMLResponse(html)




@app.get("/me/debug")
def my_debug(request: Request, db: Session = Depends(get_db)):
    """
    Per-user diagnostics — isolation, credentials, mode, balance, recent errors.
    Available to any logged-in user (own data only). Admin also sees operator flags.
    """
    redir = auth.login_redirect_if_needed(request, db)
    if redir:
        return redir
    user = auth.get_current_user(request, db)
    is_admin = bool(user and getattr(user, "role", "") == "admin")
    out = {
        "ok": True,
        "user": {
            "id": user.id if user else None,
            "username": getattr(user, "username", None),
            "role": getattr(user, "role", None),
            "status": getattr(user, "status", None),
            "has_access": auth.user_has_access(user) if user else False,
            "password_set": bool((getattr(user, "password_hash", None) or "").strip()),
        },
        "isolation": {
            "expected": "user_trades + user_settings + user_credentials for this user_id only",
            "operator_env_keys_used_for_ui": False,
        },
        "settings": {},
        "credentials": {},
        "balance": {},
        "trades": {},
        "operator": {},
        "hints": [],
    }
    try:
        us = multiuser.get_user_setting(db, user.id)
        out["settings"] = {
            "mode": getattr(us, "mode", None),
            "order_type": getattr(us, "order_type", None),
            "position_size_usd": getattr(us, "position_size_usd", None),
            "scale_entries": getattr(us, "scale_entries", None),
            "shadow_balance": getattr(us, "shadow_balance", None),
            "max_contract_price": getattr(us, "max_contract_price", None),
            "enabled_btc": getattr(us, "enabled_btc", None),
            "enabled_eth": getattr(us, "enabled_eth", None),
            "auto_trade": getattr(us, "auto_trade", None),
        }
        if (us.mode or "").lower() == "live":
            out["hints"].append("Mode is LIVE — new accounts should start in shadow unless you toggled it.")
        else:
            out["hints"].append("Mode is shadow (expected default).")
    except Exception as e:
        out["settings"] = {"error": str(e)}
        out["hints"].append("UserSetting read failed — run DB migration.")

    try:
        from sqlalchemy import text
        row = db.execute(
            text(
                "SELECT user_id, funder_address, signature_type, session_address, session_authorized, "
                "session_key_enc, has_credentials, api_key "
                "FROM user_credentials WHERE user_id = :uid LIMIT 1"
            ),
            {"uid": int(user.id)},
        ).mappings().first()
        if row:
            out["credentials"] = {
                "user_id": row.get("user_id"),
                "funder_address": row.get("funder_address"),
                "signature_type": row.get("signature_type"),
                "session_address": row.get("session_address"),
                "session_authorized": bool(row.get("session_authorized")),
                "session_key_present": bool(row.get("session_key_enc")),
                "has_credentials": bool(row.get("has_credentials")),
                "api_key_set": bool(row.get("api_key")),
                "matches_logged_in_user": int(row.get("user_id") or 0) == int(user.id),
            }
            if int(row.get("user_id") or 0) != int(user.id):
                out["hints"].append("CRITICAL: credentials row user_id mismatch.")
            if not row.get("funder_address"):
                out["hints"].append("Save funder on Account.")
            if not row.get("session_key_enc"):
                out["hints"].append("Generate session key on Account.")
            if not row.get("session_authorized"):
                out["hints"].append("Authorize session key on Polymarket then Mark authorized.")
        else:
            out["credentials"] = {"error": "no user_credentials row"}
            out["hints"].append("No credentials row — open Account and save funder.")
    except Exception as e:
        out["credentials"] = {"error": str(e)}

    try:
        funder = (out.get("credentials") or {}).get("funder_address")
        if funder:
            out["balance"] = polymarket.get_onchain_balance(funder=funder)
        else:
            out["balance"] = {"success": False, "error": "no funder"}
    except Exception as e:
        out["balance"] = {"success": False, "error": str(e)}

    try:
        from app.models import UserTrade
        from sqlalchemy import func
        n = db.query(func.count(UserTrade.id)).filter(UserTrade.user_id == user.id).scalar() or 0
        out["trades"] = {"user_trade_count": int(n), "note": "Dashboard All-Time uses user_trades only"}
    except Exception as e:
        out["trades"] = {"error": str(e)}

    # Dashboard stats shape check
    try:
        stats = services.user_dashboard_stats(db, user.id)
        out["dashboard_stats_keys"] = sorted(list(stats.keys()))
        out["dashboard_mode"] = getattr(stats.get("settings"), "mode", None) if stats.get("settings") else None
        out["dashboard_has_all_time"] = "all_time" in stats
        out["dashboard_live_balance_ok"] = bool((stats.get("live_balance") or {}).get("success"))
        if out["dashboard_mode"] != out["settings"].get("mode"):
            out["hints"].append(
                f"Mode mismatch: settings={out['settings'].get('mode')} dashboard={out['dashboard_mode']}"
            )
    except Exception as e:
        out["dashboard_stats_error"] = str(e)
        out["hints"].append(f"user_dashboard_stats failed: {e}")

    if is_admin:
        try:
            st = polymarket.credentials_status()
            out["operator"] = {
                "private_key_set": st.get("private_key_set"),
                "funder": st.get("funder"),
                "live_ready": st.get("live_ready"),
                "builder_configured": st.get("builder_configured"),
                "builder_code_set": st.get("builder_code_set"),
                "signature_type": st.get("signature_type"),
            }
        except Exception as e:
            out["operator"] = {"error": str(e)}
    else:
        out["operator"] = {"note": "hidden for non-admin — operator keys never used for your UI"}
        out["isolation"]["operator_env_keys_used_for_ui"] = False

    return JSONResponse(out)


@app.get("/me/credentials", response_class=HTMLResponse)
def my_credentials(request: Request, db: Session = Depends(get_db)):
    redir = auth.login_redirect_if_needed(request, db)
    if redir:
        return redir
    user = auth.get_current_user(request, db)
    err = None
    cred_view = {
        "funder_address": None,
        "session_address": None,
        "session_authorized": False,
        "signature_type": 1,
        "api_key": None,
        "api_secret_enc": None,
        "passphrase_enc": None,
        "has_credentials": False,
    }
    us = None
    try:
        multiuser.ensure_user_rows(db, user)
        us = multiuser.get_user_setting(db, user.id)
        from app.models import UserCredential
        from sqlalchemy import text
        # Prefer raw select so missing columns do not 500
        try:
            row = db.execute(
                text(
                    "SELECT funder_address, signature_type, api_key, has_credentials, "
                    "api_secret_enc, passphrase_enc "
                    "FROM user_credentials WHERE user_id = :uid LIMIT 1"
                ),
                {"uid": user.id},
            ).mappings().first()
            if row:
                cred_view.update({k: row[k] for k in row.keys()})
        except Exception as e1:
            db.rollback()
            err = f"credentials table read: {e1}"
        # session columns may not exist yet
        try:
            row2 = db.execute(
                text(
                    "SELECT session_address, session_authorized, session_key_enc, "
                    "owner_address, wallet_deployed, builder_managed, builder_auth_error "
                    "FROM user_credentials WHERE user_id = :uid LIMIT 1"
                ),
                {"uid": user.id},
            ).mappings().first()
            if row2:
                cred_view["session_address"] = row2.get("session_address")
                cred_view["session_authorized"] = bool(row2.get("session_authorized"))
                cred_view["session_key_enc"] = row2.get("session_key_enc")
                cred_view["owner_address"] = row2.get("owner_address")
                cred_view["wallet_deployed"] = bool(row2.get("wallet_deployed"))
                cred_view["builder_managed"] = bool(row2.get("builder_managed"))
                cred_view["builder_auth_error"] = row2.get("builder_auth_error")
        except Exception:
            db.rollback()
            try:
                from app.database import init_db
                init_db()
            except Exception:
                pass
            try:
                row2 = db.execute(
                    text(
                        "SELECT session_address, session_authorized, session_key_enc "
                        "FROM user_credentials WHERE user_id = :uid LIMIT 1"
                    ),
                    {"uid": user.id},
                ).mappings().first()
                if row2:
                    cred_view["session_address"] = row2.get("session_address")
                    cred_view["session_authorized"] = bool(row2.get("session_authorized"))
                    cred_view["session_key_enc"] = row2.get("session_key_enc")
            except Exception:
                db.rollback()
    except Exception as e:
        err = str(e)

    class C:
        pass
    cred = C()
    for k, v in cred_view.items():
        setattr(cred, k, v)

    # Fresh mode from DB (avoid stale session object)
    try:
        db.expire_all()
        us = multiuser.get_user_setting(db, user.id)
    except Exception:
        pass

    is_admin = getattr(user, "role", "") == "admin"
    try:
        if is_admin:
            stats = services.dashboard_stats(db, skip_live=True)
        else:
            stats = services.user_dashboard_stats(db, user.id)
    except Exception:
        stats = {
            "settings": us,
            "credentials": {},
            "telegram": {"configured": False, "token_set": False, "chat_id_set": False},
        }

    return templates.TemplateResponse(
        "user_credentials.html",
        {
            "request": request,
            "page": "credentials",
            "user": user,
            "current_user": user,
            "is_admin": is_admin,
            "cred": cred,
            "settings": us,
            "msg": request.query_params.get("msg"),
            "error": request.query_params.get("error") or err,
            "stats": stats,
            "snapshot": {"pairs": {}},
            "last_scan": _last_scan,
            "now": datetime.now(timezone.utc),
        },
    )


@app.post("/me/credentials")
def my_credentials_save(
    request: Request,
    db: Session = Depends(get_db),
    api_key: str = Form(""),
    api_secret: str = Form(""),
    passphrase: str = Form(""),
    funder_address: str = Form(""),
    signature_type: int = Form(3),
):
    redir = auth.login_redirect_if_needed(request, db)
    if redir:
        return redir
    user = auth.get_current_user(request, db)
    if not funder_address.strip():
        return RedirectResponse(
            url="/me/credentials?msg=Deposit+wallet+/+funder+address+required",
            status_code=303,
        )
    multiuser.save_credentials(
        db,
        user.id,
        api_key=api_key,
        api_secret=api_secret,
        passphrase=passphrase,
        funder_address=funder_address,
        signature_type=signature_type,
    )
    return RedirectResponse(url="/me/credentials?msg=Credentials+saved", status_code=303)


@app.post("/me/credentials/session/generate")
def my_session_generate(request: Request, db: Session = Depends(get_db)):
    redir = auth.login_redirect_if_needed(request, db)
    if redir:
        return redir
    user = auth.get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    from sqlalchemy import text
    from urllib.parse import quote
    try:
        funder = None
        try:
            row = db.execute(
                text("SELECT funder_address FROM user_credentials WHERE user_id = :uid LIMIT 1"),
                {"uid": int(user.id)},
            ).mappings().first()
            funder = (row or {}).get("funder_address") if row else None
        except Exception:
            db.rollback()
        if not (funder or "").strip():
            return RedirectResponse(
                url="/me/credentials?msg="
                + quote("Save funder first, or use Setup managed wallet (Builder deploy)"),
                status_code=303,
            )
        info = multiuser.generate_session_key(db, int(user.id))
        addr = info.get("session_address", "")
        uid = info.get("user_id", user.id)
        if info.get("authorized"):
            msg = quote("Session key for user %s: %s (authorized)" % (uid, addr))
        else:
            msg = quote(
                "Session key for user %s: %s — Builder authorize failed; "
                "use Setup managed wallet or fix Builder access" % (uid, addr)
            )
        return RedirectResponse(url="/me/credentials?msg=" + msg, status_code=303)
    except Exception as e:
        logger.exception("session generate failed user=%s", getattr(user, "id", None))
        try:
            db.rollback()
        except Exception:
            pass
        err = quote("Session generate failed: " + str(e)[:100])
        return RedirectResponse(url="/me/credentials?msg=" + err, status_code=303)


@app.post("/me/credentials/managed/setup")
def my_managed_wallet_setup(request: Request, db: Session = Depends(get_db)):
    """
    Proper path: create owner → deploy deposit (Builder) → session → authorize.
    Never touches admin/operator .env trading keys.
    """
    redir = auth.login_redirect_if_needed(request, db)
    if redir:
        return redir
    user = auth.get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    from urllib.parse import quote
    # Admins use operator env for live — managed path is for connected users only
    if getattr(user, "role", "") == "admin":
        return RedirectResponse(
            url="/me/credentials?msg="
            + quote("Admin uses operator .env keys. Managed wallets are for connected users."),
            status_code=303,
        )
    try:
        multiuser.ensure_user_rows(db, user)
        result = multiuser.setup_managed_trading_wallet(db, int(user.id))
        if result.get("success"):
            msg = quote(
                "Managed wallet ready. Deposit=%s Session=%s — fund the deposit address, then set Live."
                % (result.get("deposit_wallet"), result.get("session_address"))
            )
        else:
            msg = quote(
                "Managed setup incomplete: %s"
                % ((result.get("error") or result.get("note") or "unknown")[:180])
            )
        return RedirectResponse(url="/me/credentials?msg=" + msg, status_code=303)
    except Exception as e:
        logger.exception("managed wallet setup failed user=%s", getattr(user, "id", None))
        try:
            db.rollback()
        except Exception:
            pass
        return RedirectResponse(
            url="/me/credentials?msg=" + quote("Managed setup failed: " + str(e)[:120]),
            status_code=303,
        )


@app.post("/me/credentials/export-owner")
def my_export_owner_key(request: Request, db: Session = Depends(get_db)):
    """Show owner private key once (managed wallets). User controls withdraw."""
    redir = auth.login_redirect_if_needed(request, db)
    if redir:
        return redir
    user = auth.get_current_user(request, db)
    if not user or getattr(user, "role", "") == "admin":
        return RedirectResponse(url="/me/credentials?msg=Users+only", status_code=303)
    from urllib.parse import quote
    result = multiuser.export_owner_private_key(db, int(user.id))
    if not result.get("success"):
        return RedirectResponse(
            url="/me/credentials?msg=" + quote(result.get("error") or "export failed"),
            status_code=303,
        )
    # One-time display via query is unsafe in logs — return HTML page instead
    return HTMLResponse(
        f"""<!DOCTYPE html><html><body style="font-family:sans-serif;background:#0b1020;color:#e2e8f0;padding:2rem">
        <h2>Owner private key (keep secret)</h2>
        <p>Address: <code>{result.get('owner_address')}</code></p>
        <p>Deposit: <code>{result.get('deposit_wallet')}</code></p>
        <p style="word-break:break-all;background:#151c2e;padding:1rem;border-radius:8px"><code>{result.get('private_key')}</code></p>
        <p style="color:#94a3b8;font-size:12px">{result.get('note')}</p>
        <p><a href="/me/credentials" style="color:#38bdf8">Back to Account</a></p>
        </body></html>"""
    )


@app.post("/me/credentials/session/authorized")
def my_session_authorized(request: Request, db: Session = Depends(get_db)):
    redir = auth.login_redirect_if_needed(request, db)
    if redir:
        return redir
    user = auth.get_current_user(request, db)
    from urllib.parse import quote
    try:
        multiuser.mark_session_authorized(db, user.id, True)
        return RedirectResponse(
            url="/me/credentials?msg="
            + quote(
                "Session marked authorized locally only. "
                "Live orders need Builder/Polymarket authorize to succeed."
            ),
            status_code=303,
        )
    except Exception as e:
        err = quote(str(e)[:80])
        return RedirectResponse(url="/me/credentials?msg=" + err, status_code=303)



