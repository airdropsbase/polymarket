"""Telegram alerts for signal open / resolve / errors."""
from __future__ import annotations

import json
import logging
import os
import threading
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_API = "https://api.telegram.org"

_alerted_open: set = set()
_alerted_resolved: set = set()
_alert_lock = threading.Lock()
_MAX_TRACKED = 2000


def _token() -> str:
    return os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()


def _chat_id() -> str:
    return os.environ.get("TELEGRAM_CHAT_ID", "").strip()


def is_configured() -> bool:
    return bool(_token() and _chat_id())


def status() -> dict:
    return {
        "configured": is_configured(),
        "chat_id_set": bool(_chat_id()),
        "token_set": bool(_token()),
    }


def send_message(text: str, parse_mode: str = "HTML", disable_preview: bool = True) -> dict:
    token, chat = _token(), _chat_id()
    if not token or not chat:
        return {"success": False, "skipped": True, "error": "Telegram not configured"}

    url = f"{_API}/bot{token}/sendMessage"
    payload = {
        "chat_id": chat,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_preview,
    }
    try:
        resp = requests.post(url, json=payload, timeout=12)
        if resp.ok:
            return {"success": True}
        logger.warning("[TG] send failed %s: %s", resp.status_code, resp.text[:200])
        return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
    except Exception as e:
        logger.warning("[TG] send exception: %s", e)
        return {"success": False, "error": str(e)}


def _track(s: set, sid: Optional[int]) -> bool:
    if sid is None:
        return True
    with _alert_lock:
        if sid in s:
            return False
        s.add(sid)
        if len(s) > _MAX_TRACKED:
            s.clear()
            s.add(sid)
        return True


def alert_signal_opened(sig) -> dict:
    sid = getattr(sig, "id", None)
    if not _track(_alerted_open, sid):
        logger.info("[TG] skip duplicate open alert signal_id=%s", sid)
        return {"success": True, "skipped": True, "reason": "duplicate"}

    mode = (getattr(sig, "mode", None) or "shadow").upper()
    direction = getattr(sig, "direction", "?")
    pair = getattr(sig, "pair", "?")
    tf = getattr(sig, "timeframe", "?")
    sig_px = getattr(sig, "signal_price", None) or getattr(sig, "rsi", None)
    size = getattr(sig, "position_size_usd", None)
    entry_px = getattr(sig, "entry_price", None)
    status_s = getattr(sig, "status", "")
    entry = getattr(sig, "entry_time", None)
    close = getattr(sig, "candle_close_time", None)

    arrow = "🟢 UP" if str(direction).upper() == "UP" else "🔴 DOWN"
    lines = [
        f"<b>L5 Value Lock signal</b> · {mode}",
        f"{arrow}  <b>{pair}</b>  {tf}",
    ]
    if sig_px is not None:
        try:
            lines.append(f"SigPx <code>{float(sig_px):.3f}</code>")
        except (TypeError, ValueError):
            pass
    if size is not None:
        lines.append(f"Stake  <code>${float(size):.2f}</code>")
    if entry_px is not None:
        try:
            lines.append(f"Entry  <code>{float(entry_px):.3f}</code>")
        except (TypeError, ValueError):
            pass
    if entry and close:
        try:
            lines.append(
                f"Window  <code>{entry.strftime('%H:%M')}–{close.strftime('%H:%M')} UTC</code>"
            )
        except Exception:
            pass
    if status_s == "error":
        lines.append("Status  <code>not filled</code>")
    elif status_s and status_s not in ("pending", "open", "placing"):
        lines.append(f"Status  <code>{status_s}</code>")
    err = getattr(sig, "error_message", None)
    if err:
        # Keep telegram readable
        short = err if len(err) <= 280 else err[:277] + "..."
        lines.append(f"⚠️  <code>{short}</code>")

    return send_message("\n".join(lines))


def alert_signal_resolved(sig) -> dict:
    sid = getattr(sig, "id", None)
    if not _track(_alerted_resolved, sid):
        logger.info("[TG] skip duplicate resolve alert signal_id=%s", sid)
        return {"success": True, "skipped": True, "reason": "duplicate"}

    result = (getattr(sig, "result", None) or "").lower()
    direction = getattr(sig, "direction", "?")
    pair = getattr(sig, "pair", "?")
    tf = getattr(sig, "timeframe", "?")
    pnl = getattr(sig, "pnl", None)
    mode = (getattr(sig, "mode", None) or "shadow").upper()
    size = getattr(sig, "position_size_usd", None)
    entry_px = getattr(sig, "entry_price", None)
    entry = getattr(sig, "entry_time", None)
    close = getattr(sig, "candle_close_time", None)

    if result == "win":
        head = "✅ <b>WIN</b>"
    elif result == "loss":
        head = "❌ <b>LOSS</b>"
    else:
        head = f"ℹ️  <b>{(result or 'resolved').upper()}</b>"

    arrow = "UP" if str(direction).upper() == "UP" else "DOWN"
    lines = [
        f"{head} · {mode}",
        f"{arrow}  <b>{pair}</b>  {tf}",
    ]

    if pnl is not None:
        try:
            pv = float(pnl)
            sign = "+" if pv > 0 else ""
            lines.append(f"PnL  <code>{sign}${pv:.2f}</code>")
        except (TypeError, ValueError):
            pass

    if mode == "LIVE":
        if size is not None:
            try:
                lines.append(f"Stake  <code>${float(size):.2f}</code>")
            except (TypeError, ValueError):
                pass
        if entry_px is not None:
            try:
                px = float(entry_px)
                lines.append(f"Fill  <code>{px:.3f}</code>")
                if size is not None and px > 0:
                    sz = float(size)
                    shares = sz / px
                    if result == "win":
                        live_est = shares * (1.0 - px)
                    elif result == "loss":
                        live_est = -sz
                    else:
                        live_est = None
                    if live_est is not None:
                        sign = "+" if live_est > 0 else ""
                        lines.append(f"Live est.  <code>{sign}${live_est:.2f}</code>")
            except (TypeError, ValueError):
                pass

    if entry and close:
        try:
            lines.append(
                f"Window  <code>{entry.strftime('%H:%M')}–{close.strftime('%H:%M')} UTC</code>"
            )
        except Exception:
            pass

    return send_message("\n".join(lines))


def alert_error(title: str, detail: Optional[str] = None) -> dict:
    lines = [f"⚠️  <b>{title}</b>"]
    if detail:
        lines.append(f"<code>{detail[:500]}</code>")
    return send_message("\n".join(lines))


def alert_daily_summary(stats: dict, day_label: str | None = None) -> dict:
    today = stats.get("today") or stats
    signals = int(today.get("signals") or 0)
    wins = int(today.get("wins") or 0)
    losses = int(today.get("losses") or 0)
    pending = int(today.get("pending") or 0)
    wr = float(today.get("win_rate") or 0.0)
    raw_sh = today.get("pnl_shadow")
    pnl_shadow = float(raw_sh if raw_sh is not None else (today.get("pnl") or 0.0))
    label = day_label or "Today"

    sh_str = f"+${pnl_shadow:.2f}" if pnl_shadow > 0 else f"${pnl_shadow:.2f}"
    lines = [
        f"📊 <b>Daily summary</b> · {label}",
        f"Signals  <code>{signals}</code>",
        f"Results  <code>{wins}W / {losses}L</code>"
        + (f" · <code>{pending} open</code>" if pending else ""),
        f"Win rate  <code>{wr:.1f}%</code>",
        f"Shadow PnL  <code>{sh_str}</code>",
    ]

    live_clob = stats.get("live_clob_daily_pnl")
    if live_clob is not None:
        ls = f"+${float(live_clob):.2f}" if float(live_clob) > 0 else f"${float(live_clob):.2f}"
        lines.append(f"Live CLOB PnL  <code>{ls}</code>")

    live_at = stats.get("live_clob_all_time_pnl") or stats.get("live_onchain_all_time_pnl")
    if live_at is not None:
        ats = f"+${float(live_at):.2f}" if float(live_at) > 0 else f"${float(live_at):.2f}"
        lines.append(f"All-time live  <code>{ats}</code>")

    settings = stats.get("settings") or {}
    bal = settings.get("shadow_balance")
    if bal is not None:
        lines.append(f"Shadow bal  <code>${float(bal):.2f}</code>")
    lb = stats.get("live_balance") or {}
    if lb.get("success") and lb.get("pusd_balance") is not None:
        lines.append(f"Live pUSD  <code>${float(lb['pusd_balance']):.2f}</code>")
    mode = settings.get("mode")
    if mode:
        lines.append(f"Mode  <code>{mode}</code>")

    return send_message("\n".join(lines))


def alert_debug_blob(title: str, payload: dict) -> dict:
    body = json.dumps(payload, default=str)[:3500]
    return send_message(f"<b>{title}</b>\n<code>{body}</code>")
