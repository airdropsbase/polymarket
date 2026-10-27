"""
Polymarket BTC 5m — L5 Value Lock
======================================================================
Causal rule-based strategy (no ML, no circuit breaker, no lookahead).

Trades the market that is ALREADY RUNNING at signal time.

Signal timing (exact match to strategy_l5 design):
  - Current 5m window open = W
  - Signal fires at S = W + 240s  (60 s before the running market resolves)
  - Target market = the RUNNING market (opened at W, resolves at W + 300)
  - Position held to resolution

Rule (every number frozen; nothing fitted inside this file):
  1. lead      z30 = ln(P_S / ref) / (vol30 * sqrt(0.5))
                   P_S    close of the Binance 1m candle that ENDS at S
                   ref    Binance price 30 s before the running market's open
                   vol30  rms of the last 30 one-minute log returns
  2. side      UP if z30 > 0, DOWN if z30 < 0
  3. strength  |z30| >= 1.0
  4. price     ask = best available ask (CLOB preferred; else last ask-stream print)
               age <= 15 s, 0.02 < ask < 0.995
  5. value     Phi(|z30|) - ask >= 0.03
  6. guard     vol30 >= 0.25 * median of previous 2016 signal-time vol30 values
               (strategy silent until >= 576 prior values exist)

IMPORTANT — in-memory state:
  `_emitted` and `_vol_hist` are module-level. Correct only if scan runs inside one
  continuously-alive process on ~1s cadence (this bot's scheduler).
"""
from __future__ import annotations

import logging
import math
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import requests

logger = logging.getLogger(__name__)

# ── Universe ─────────────────────────────────────────────────────────────────
ACTIVE_PAIRS = ["BTCUSDT"]
PAIR_TIMEFRAMES = {"BTCUSDT": ("5m",)}

# ── Window / execution (exact L5) ────────────────────────────────────────────
WINDOW_SEC = 300
SIGNAL_OFFSET_SEC = 240          # S = running open + 240 s
SIGNAL_TOLERANCE_SEC = 8         # tight decision zone so 1s scanner catches S
# No artificial fill-price cap: L5 decides via edge gate.  Cap only rejects absurd asks.
FILL_ENTRY_TH = 0.995
UP_ENTRY_TH = FILL_ENTRY_TH
DOWN_ENTRY_TH = FILL_ENTRY_TH
MAX_PRICE_CEILING = 0.995
STAKE_USD = 1.0
MAX_ENTRIES = 1
IMMEDIATE_EXEC = True
STRATEGY_ID = "l5_value_lock_5m"

# Compatibility aliases used by scheduler / services
LOOK_SECONDS = 0
MAX_ENTRY_DELAY = 60             # only 60 s left until resolution after S
EXCLUDE_HOURS: set[int] = set()
MAX_ENTRY_PRICE = FILL_ENTRY_TH
UP_LO = 0.0
UP_HI = 0.995
DOWN_UP_LO = 0.005
DOWN_UP_HI = 1.0
MAX_CONSECUTIVE_LOSSES = 999      # no circuit breaker
MAX_DAILY_LOSS_USD = 9999.0

# Decision zone for scheduler (narrow around signal offset)
DECISION_START_SEC = SIGNAL_OFFSET_SEC - SIGNAL_TOLERANCE_SEC   # 232
DECISION_END_SEC = SIGNAL_OFFSET_SEC + SIGNAL_TOLERANCE_SEC     # 248
ENTRY_GRACE_SECONDS = 60
LATE_START_SEC = DECISION_START_SEC

# ── L5 frozen parameters ─────────────────────────────────────────────────────
LAG_S = 30
VOL_MINUTES = 30
Z_MIN = 1.0
EDGE_MIN = 0.03
ASK_MAX_AGE_S = 15
ASK_LO = 0.02
ASK_HI = 0.995
GUARD_FRAC = 0.25
TRAIL_ROWS = 2016
TRAIL_MIN_ROWS = 576

GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
BINANCE_KLINES = "https://data-api.binance.vision/api/v3/klines"

_snapshot_cache: dict = {"data": None, "ts": 0.0}
_SNAPSHOT_TTL = 1.5
_emitted: dict = {}          # running_market_open_ts -> True
_vol_hist: list[float] = []  # chronological vol30 at each past signal time
_klines_cache: dict = {"data": None, "ts": 0.0, "t0": 0}
_KLINES_TTL = 3.0
_heartbeat: dict = {
    "window_ts": None,
    "elapsed": None,
    "in_zone": False,
    "target_open": None,       # for L5 this is the RUNNING open
    "direction": None,
    "reason": "init",
    "z30": None,
    "vol30": None,
    "ask": None,
    "edge": None,
    "p_model": None,
    "scan_count": 0,
    "signal_count": 0,
    "vol_hist_len": 0,
    "ts": 0.0,
    "iso": None,
}
_last_hb_log_ts = 0.0
_HEARTBEAT_LOG_EVERY_SEC = 30


# ── Math helpers (exact from strategy_l5.py) ──────────────────────────────────
def norm_cdf(x: float) -> float:
    return 0.5 * math.erfc(-x / math.sqrt(2.0))


def vol30_at(closes: np.ndarray, j: int, minutes: int = VOL_MINUTES) -> float:
    """rms of the `minutes` one-minute log returns ending with candle j."""
    if j < minutes or j >= len(closes):
        return 0.0
    r = np.diff(np.log(np.asarray(closes[j - minutes: j + 1], dtype=float)))
    return float(np.sqrt(np.mean(r * r)))


def z_lead(closes: np.ndarray, j: int, vol30: float, lag_s: int = LAG_S) -> float:
    """Lead of Binance price at S over the oracle reference, in remaining-vol units."""
    if j < 5 or vol30 <= 0:
        return 0.0
    w = lag_s / 60.0
    ref = math.exp((1.0 - w) * math.log(closes[j - 4]) + w * math.log(closes[j - 5]))
    unknown_s = max(60 - lag_s, 5)
    return math.log(closes[j] / ref) / (vol30 * math.sqrt(unknown_s / 60.0))


# ── Window helpers ────────────────────────────────────────────────────────────
def _window_start(ts: Optional[float] = None) -> int:
    t = int(ts if ts is not None else time.time())
    return (t // WINDOW_SEC) * WINDOW_SEC


def _utc(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


# ── Binance 1m klines ─────────────────────────────────────────────────────────
def _fetch_klines(limit: int = 2500) -> Tuple[Optional[np.ndarray], Optional[np.ndarray], int]:
    """
    Returns (open_times int64, closes float64, t0 of first candle).
    Cached briefly so the 1s scanner does not hammer Binance.
    """
    now = time.time()
    if (
        _klines_cache["data"] is not None
        and (now - _klines_cache["ts"]) < _KLINES_TTL
    ):
        ot, cl = _klines_cache["data"]
        return ot, cl, _klines_cache["t0"]

    try:
        r = requests.get(
            BINANCE_KLINES,
            params={"symbol": "BTCUSDT", "interval": "1m", "limit": limit},
            timeout=8,
        )
        r.raise_for_status()
        rows = r.json()
        if not rows or len(rows) < VOL_MINUTES + 10:
            return None, None, 0
        open_times = np.array([int(x[0] // 1000) for x in rows], dtype=np.int64)
        closes = np.array([float(x[4]) for x in rows], dtype=np.float64)
        # ensure contiguous 60 s steps
        if not np.all(np.diff(open_times) == 60):
            logger.warning("[L5] Binance 1m series not contiguous")
        _klines_cache["data"] = (open_times, closes)
        _klines_cache["ts"] = now
        _klines_cache["t0"] = int(open_times[0])
        return open_times, closes, int(open_times[0])
    except Exception as e:
        logger.warning("[L5] klines fetch failed: %s", e)
        return None, None, 0


def _seed_vol_hist_if_needed(open_times: np.ndarray, closes: np.ndarray) -> None:
    """On cold start, pre-compute vol30 at every past 5m signal time from klines."""
    global _vol_hist
    if len(_vol_hist) >= TRAIL_MIN_ROWS:
        return
    if open_times is None or closes is None or len(closes) < VOL_MINUTES + 10:
        return
    # Signal times land on :04 / :09 / :14 / … of each 5m window
    # Candle that ends at S has open_time = S - 60
    t0 = int(open_times[0])
    last_ot = int(open_times[-1])
    # Walk every 5m boundary that already has a full candle at S
    first_possible_S = t0 + (VOL_MINUTES + 5) * 60
    # Align first_possible_S to a real signal (window_open + 240)
    # window_open = (S - 240) // 300 * 300
    seeded: list[float] = []
    S = first_possible_S
    # Align S so (S - 240) % 300 == 0
    rem = (S - SIGNAL_OFFSET_SEC) % WINDOW_SEC
    if rem:
        S += WINDOW_SEC - rem
    while S <= last_ot + 60:
        j = (S - 60 - t0) // 60
        if 0 <= j < len(closes):
            v = vol30_at(closes, j)
            if v > 0:
                seeded.append(v)
        S += WINDOW_SEC
    if len(seeded) > len(_vol_hist):
        _vol_hist = seeded[-TRAIL_ROWS:]
        logger.info("[L5] seeded vol_hist with %d values from klines", len(_vol_hist))


# ── Market discovery (RUNNING market) ─────────────────────────────────────────
def get_market_for_window(window_ts: int, pair: str = "BTC") -> Optional[dict]:
    """Fetch the 5m market that opened at window_ts (the running one for L5)."""
    asset = pair.lower()
    slug = f"{asset}-updown-5m-{window_ts}"
    try:
        from app import polymarket as poly
        m = poly.discover_market("BTCUSDT", timeframe="5m", anchor_ts=window_ts)
        if m and m.get("up_token_id") and m.get("down_token_id"):
            return m
    except Exception as e:
        logger.debug("[L5] discover via polymarket failed: %s", e)

    # Direct Gamma fallback
    try:
        r = requests.get(f"{GAMMA_API}/markets", params={"slug": slug}, timeout=6)
        if r.ok:
            data = r.json()
            markets = data if isinstance(data, list) else []
            if markets:
                from app.polymarket import _parse_market_payload
                return _parse_market_payload(markets[0], slug)
    except Exception as e:
        logger.debug("[L5] gamma fallback failed: %s", e)
    return None


def _best_asks_for_market(market: Optional[dict]) -> dict:
    """Return best_ask_up / best_ask_down from CLOB."""
    out = {"best_ask_up": None, "best_ask_down": None, "book_error": None}
    if not market:
        out["book_error"] = "no market"
        return out
    try:
        from app import polymarket as poly
        for side, key in (("UP", "best_ask_up"), ("DOWN", "best_ask_down")):
            tid = market.get("up_token_id") if side == "UP" else market.get("down_token_id")
            if not tid:
                continue
            book = poly.get_order_book(tid)
            if not book:
                continue
            asks = book.get("asks") or []
            if asks:
                # lowest ask
                px = min(float(a.get("price") or a[0] if isinstance(a, (list, tuple)) else a.get("price", 1)) for a in asks)
                out[key] = px
    except Exception as e:
        out["book_error"] = str(e)[:80]
    return out


def _fetch_recent_trades(market: dict, before_ts: int, lookback_s: int = 120) -> List[dict]:
    """Fetch recent trades of the running market with t < before_ts."""
    slug = market.get("slug") or ""
    cid = market.get("condition_id") or ""
    t_lo = before_ts - lookback_s
    trades: List[dict] = []
    seen: set = set()

    def _parse(tr: dict) -> Optional[dict]:
        try:
            t = int(tr.get("timestamp") or tr.get("t") or 0)
            if t < t_lo or t >= before_ts:
                return None
            price = float(tr.get("price") or 0)
            side = str(tr.get("side") or "").upper()
            outcome = str(tr.get("outcome") or tr.get("title") or "").strip()
            if "up" in outcome.lower():
                is_up = True
            elif "down" in outcome.lower():
                is_up = False
            else:
                return None
            is_buy = side in ("BUY", "B")
            return {"t": t, "price": price, "is_buy": is_buy, "is_up": is_up}
        except Exception:
            return None

    try:
        params: dict = {"limit": 100}
        if cid:
            params["market"] = cid
        r = requests.get(f"{DATA_API}/trades", params=params, timeout=6)
        if r.ok:
            rows = r.json() if isinstance(r.json(), list) else []
            for tr in rows:
                p = _parse(tr)
                if not p:
                    continue
                key = (p["t"], p["price"], p["is_buy"], p["is_up"])
                if key in seen:
                    continue
                seen.add(key)
                trades.append(p)
    except Exception as e:
        logger.debug("[L5] trades fetch: %s", e)

    trades.sort(key=lambda x: x["t"])
    return trades


def ask_proxy_from_tape(trades: List[dict], S: int, want_up: bool) -> Optional[Tuple[float, int]]:
    """Last print of the chosen side's ask stream with t < S."""
    best_i = -1
    best_t = -1
    for i, tr in enumerate(trades):
        t = tr["t"]
        if t >= S:
            continue
        # ask stream: BUY chosen side, or SELL opposite side
        match = (tr["is_buy"] and tr["is_up"] == want_up) or (
            (not tr["is_buy"]) and tr["is_up"] != want_up
        )
        if match and t >= best_t:
            best_t = t
            best_i = i
    if best_i < 0:
        return None
    tr = trades[best_i]
    ask = float(tr["price"]) if tr["is_buy"] else 1.0 - float(tr["price"])
    return ask, int(S - tr["t"])


# ── Core L5 evaluation ────────────────────────────────────────────────────────
def evaluate_l5(
    closes: np.ndarray,
    t0: int,
    S: int,
    vol_hist: list[float],
    market: Optional[dict],
    trades: List[dict],
    live_ask: Optional[float] = None,
) -> Tuple[Optional[dict], str]:
    """
    Pure L5 decision at signal time S for the running market.
    Returns (decision_dict or None, reason).
    """
    j = (S - 60 - t0) // 60
    if j < max(VOL_MINUTES, 5) or j >= len(closes):
        return None, "no Binance candle for S"

    hist = np.asarray(vol_hist, dtype=float)[-TRAIL_ROWS:]
    if len(hist) < TRAIL_MIN_ROWS:
        return None, f"warm-up: vol_hist {len(hist)}/{TRAIL_MIN_ROWS}"

    vol = vol30_at(closes, j)
    med = float(np.median(hist)) if len(hist) else 0.0
    if not (vol > 0.0 and vol >= GUARD_FRAC * med):
        return None, "vol guard (stalled feed)"

    z = z_lead(closes, j, vol)
    if abs(z) < Z_MIN:
        return None, "lead too small"

    want_up = z > 0
    side = "UP" if want_up else "DOWN"

    # Prefer live CLOB ask; fall back to tape proxy
    ask = None
    age = 0
    if live_ask is not None and ASK_LO < live_ask < ASK_HI:
        ask = float(live_ask)
        age = 0  # live book is current
    else:
        q = ask_proxy_from_tape(trades, S, want_up)
        if q is None:
            return None, "no print on the chosen side before S"
        ask, age = q
        if age > ASK_MAX_AGE_S:
            return None, "last print too old"
        if not (ASK_LO < ask < ASK_HI):
            return None, "ask outside the allowed range"

    if ask is None or not (ASK_LO < ask < ASK_HI):
        return None, "ask outside the allowed range"

    pm = norm_cdf(abs(z))
    edge = pm - ask
    if edge < EDGE_MIN:
        return None, "model probability does not beat the price by edge_min"

    return {
        "side": side,
        "z30": float(z),
        "p_model": float(pm),
        "ask": float(ask),
        "ask_age_s": int(age),
        "edge": float(edge),
        "vol30": float(vol),
    }, "trade"


# ── Heartbeat / pulse ─────────────────────────────────────────────────────────
def get_heartbeat() -> dict:
    return dict(_heartbeat)


def _update_heartbeat(**kwargs) -> None:
    global _last_hb_log_ts
    _heartbeat.update(kwargs)
    _heartbeat["ts"] = time.time()
    _heartbeat["iso"] = datetime.now(timezone.utc).isoformat()
    _heartbeat["vol_hist_len"] = len(_vol_hist)
    now = _heartbeat["ts"]
    if now - _last_hb_log_ts >= _HEARTBEAT_LOG_EVERY_SEC:
        _last_hb_log_ts = now
        logger.info(
            "[L5 HB] elapsed=%.0fs zone=%s running=%s z=%.2f vol=%.5f ask=%s edge=%s "
            "dir=%s reason=%s scans=%s signals=%s vol_hist=%d",
            _heartbeat.get("elapsed") or -1,
            _heartbeat.get("in_zone"),
            _heartbeat.get("target_open"),
            float(_heartbeat.get("z30") or 0),
            float(_heartbeat.get("vol30") or 0),
            _heartbeat.get("ask"),
            _heartbeat.get("edge"),
            _heartbeat.get("direction"),
            _heartbeat.get("reason"),
            _heartbeat.get("scan_count"),
            _heartbeat.get("signal_count"),
            len(_vol_hist),
        )


def heartbeat_tick(symbol: str = "BTCUSDT") -> dict:
    """Lightweight status update — never emits signals."""
    now_ts = time.time()
    current_open = _window_start(now_ts)
    elapsed = now_ts - current_open
    in_zone = DECISION_START_SEC <= elapsed <= DECISION_END_SEC
    _heartbeat["scan_count"] = int(_heartbeat.get("scan_count") or 0) + 1

    reason = "outside_zone"
    if in_zone:
        reason = "in_zone_waiting_scan"
        if _emitted.get(current_open):
            reason = "already_emitted"

    _update_heartbeat(
        window_ts=current_open,
        elapsed=round(elapsed, 1),
        in_zone=in_zone,
        target_open=current_open,  # L5 targets the RUNNING market
        reason=reason,
    )
    return get_heartbeat()


def market_snapshot(force: bool = False) -> Dict:
    """Pulse payload for L5 (consumed by main / templates)."""
    now_ts = time.time()
    if (
        not force
        and _snapshot_cache["data"] is not None
        and (now_ts - _snapshot_cache["ts"]) < _SNAPSHOT_TTL
    ):
        return _snapshot_cache["data"]

    window_ts = _window_start(now_ts)
    elapsed = now_ts - window_ts
    in_zone = DECISION_START_SEC <= elapsed <= DECISION_END_SEC
    S = window_ts + SIGNAL_OFFSET_SEC

    spot = None
    try:
        r = requests.get(
            "https://data-api.binance.vision/api/v3/ticker/price",
            params={"symbol": "BTCUSDT"},
            timeout=4,
        )
        if r.ok:
            spot = float(r.json().get("price") or 0)
    except Exception:
        pass

    market = get_market_for_window(window_ts, pair="BTC")
    asks = _best_asks_for_market(market)

    # Live feature sample for pulse
    z30 = vol30 = ask = edge = p_model = direction = None
    reason = "outside_zone" if not in_zone else "in_zone"
    open_times, closes, t0 = _fetch_klines()
    if open_times is not None and closes is not None:
        _seed_vol_hist_if_needed(open_times, closes)
        j = (S - 60 - t0) // 60
        if 0 <= j < len(closes):
            vol30 = vol30_at(closes, j)
            if vol30 > 0:
                z30 = z_lead(closes, j, vol30)
                want_up = z30 > 0
                live_ask = asks.get("best_ask_up") if want_up else asks.get("best_ask_down")
                if live_ask is not None:
                    ask = float(live_ask)
                    p_model = norm_cdf(abs(z30))
                    edge = p_model - ask
                    if abs(z30) >= Z_MIN and edge >= EDGE_MIN and ASK_LO < ask < ASK_HI:
                        direction = "UP" if want_up else "DOWN"
                        reason = f"eligible_{direction}"
                    else:
                        reason = "filters_not_met"

    hb = get_heartbeat()
    tf5 = {
        "elapsed": int(elapsed),
        "decision_zone": in_zone,
        "signal_offset": SIGNAL_OFFSET_SEC,
        "signal": direction if in_zone else None,
        "target_window": window_ts,          # RUNNING market open
        "fill_cap": float(FILL_ENTRY_TH),
        "max_price": float(FILL_ENTRY_TH),
        "open_barrier": None,
        "spot": round(spot, 2) if spot is not None else None,
        "best_ask_up": asks.get("best_ask_up"),
        "best_ask_down": asks.get("best_ask_down"),
        "median_up": None,
        "btc_ret_bps": None,
        "strategy_id": STRATEGY_ID,
        "book_error": asks.get("book_error"),
        "paths": ["L5"] if direction else [],
        "tape_source": "clob+running",
        "heartbeat": {
            "reason": hb.get("reason") or reason,
            "scan_count": hb.get("scan_count"),
            "signal_count": hb.get("signal_count"),
            "iso": hb.get("iso"),
            "vol_hist_len": len(_vol_hist),
        },
        "feat": {
            "z30": round(z30, 3) if z30 is not None else None,
            "vol30": round(vol30, 6) if vol30 is not None else None,
            "ask": round(ask, 4) if ask is not None else None,
            "edge": round(edge, 4) if edge is not None else None,
            "p_model": round(p_model, 4) if p_model is not None else None,
        },
    }

    payload = {
        "pairs": {
            "BTCUSDT": {
                "price": spot,
                "15m": {"disabled": True},
                "5m": tf5,
                "1h": {"signal": None},
            }
        },
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "strategy": STRATEGY_ID,
        "strategies": [STRATEGY_ID],
        "errors": [],
    }
    _snapshot_cache["data"] = payload
    _snapshot_cache["ts"] = now_ts
    return payload


# ── Signal construction ───────────────────────────────────────────────────────
def _build_signal_dict(
    *,
    pair: str,
    symbol: str,
    direction: str,
    decision: dict,
    signal_time: datetime,
    entry_time: datetime,
    close_time: datetime,
    window_ts: int,
    market: Optional[dict],
) -> dict:
    token_id = None
    slug = None
    condition_id = None
    if market:
        slug = market.get("slug")
        condition_id = market.get("condition_id")
        if direction.upper() == "UP":
            token_id = market.get("up_token_id")
        else:
            token_id = market.get("down_token_id")

    # Cap the order at the L5 ask (plus a tiny slip allowance) so FAK does not chase
    ask = float(decision["ask"])
    max_px = min(ASK_HI, ask + 0.01)

    return {
        "pair": pair,
        "symbol": symbol,
        "timeframe": "5m",
        "direction": direction.upper(),
        "window_ts": window_ts,                 # RUNNING market open
        "signal_time": signal_time,
        "entry_time": entry_time,
        "entry_deadline": close_time,
        "candle_close_time": close_time,
        "confidence": round(float(decision["edge"]) * 100, 4),
        "score": round(float(decision["z30"]), 4),
        "day_rank": 1,
        "signal_price": round(ask, 4),
        "median_up": round(ask, 4),
        "btc_ret_bps": None,
        "max_entries": MAX_ENTRIES,
        "stake_usd": STAKE_USD,
        "max_price": max_px,
        "max_entry_delay": max(0, int(close_time.timestamp()) - int(time.time())),
        "slug": slug,
        "condition_id": condition_id,
        "token_id": token_id,
        "market": market,
        "leg": "l5_value_lock",
        "horizon_sec": SIGNAL_OFFSET_SEC,
        "feat_z30": decision.get("z30"),
        "feat_vol30": decision.get("vol30"),
        "feat_edge": decision.get("edge"),
        "feat_p_model": decision.get("p_model"),
        "feat_ask_age_s": decision.get("ask_age_s"),
        "strategy_id": STRATEGY_ID,
    }


# ── Main scan entry point ─────────────────────────────────────────────────────
def scan_one_symbol(symbol: str, now: Optional[datetime] = None) -> List[Dict]:
    """
    Core scan. Fires only inside the narrow decision zone around W+240s.
    Target market = the RUNNING 5m window (opened at W, resolves at W+300).
    """
    now = now or datetime.now(timezone.utc)
    now_ts = now.timestamp()
    current_open = _window_start(now_ts)
    elapsed = now_ts - current_open
    S = current_open + SIGNAL_OFFSET_SEC
    close_ts = current_open + WINDOW_SEC

    in_zone = DECISION_START_SEC <= elapsed <= DECISION_END_SEC
    _heartbeat["scan_count"] = int(_heartbeat.get("scan_count") or 0) + 1

    if not in_zone:
        _update_heartbeat(
            window_ts=current_open,
            elapsed=round(elapsed, 1),
            in_zone=False,
            target_open=current_open,
            reason="outside_zone",
            direction=None,
        )
        return []

    if _emitted.get(current_open):
        _update_heartbeat(
            window_ts=current_open,
            elapsed=round(elapsed, 1),
            in_zone=True,
            target_open=current_open,
            reason="already_emitted",
            direction=None,
        )
        return []

    # Fetch data
    open_times, closes, t0 = _fetch_klines()
    if open_times is None or closes is None:
        _update_heartbeat(reason="no_klines", in_zone=True, window_ts=current_open, elapsed=round(elapsed, 1))
        return []

    _seed_vol_hist_if_needed(open_times, closes)

    market = get_market_for_window(current_open, pair="BTC")
    if not market or not market.get("up_token_id"):
        _update_heartbeat(reason="no_running_market", in_zone=True, window_ts=current_open, elapsed=round(elapsed, 1))
        return []

    asks = _best_asks_for_market(market)
    trades = _fetch_recent_trades(market, before_ts=int(S) + 1, lookback_s=120)

    # Compute current vol for history append (even if we do not trade)
    j = (S - 60 - t0) // 60
    cur_vol = vol30_at(closes, j) if 0 <= j < len(closes) else 0.0

    # Determine live ask candidates for both sides (evaluate_l5 picks the right one)
    # We pass the ask of the side that z will choose after computing z inside evaluate.
    # For simplicity compute z first then pick ask.
    if j < max(VOL_MINUTES, 5) or j >= len(closes) or cur_vol <= 0:
        _update_heartbeat(reason="no_candle_or_vol", in_zone=True, window_ts=current_open, elapsed=round(elapsed, 1))
        return []

    z_preview = z_lead(closes, j, cur_vol)
    want_up = z_preview > 0
    live_ask = asks.get("best_ask_up") if want_up else asks.get("best_ask_down")

    decision, reason = evaluate_l5(
        closes=closes,
        t0=t0,
        S=int(S),
        vol_hist=_vol_hist,
        market=market,
        trades=trades,
        live_ask=live_ask,
    )

    # Always record this signal-time's vol for the trailing median (causal)
    if cur_vol > 0:
        _vol_hist.append(cur_vol)
        if len(_vol_hist) > TRAIL_ROWS * 2:
            _vol_hist[:] = _vol_hist[-TRAIL_ROWS:]

    if decision is None:
        _update_heartbeat(
            window_ts=current_open,
            elapsed=round(elapsed, 1),
            in_zone=True,
            target_open=current_open,
            reason=reason,
            direction=None,
            z30=round(z_preview, 3),
            vol30=round(cur_vol, 6),
            ask=live_ask,
            edge=None,
        )
        return []

    # Mark emitted so we never double-fire this running market
    _emitted[current_open] = True
    # prune old keys
    for k in list(_emitted):
        if k < current_open - WINDOW_SEC * 3:
            _emitted.pop(k, None)

    _heartbeat["signal_count"] = int(_heartbeat.get("signal_count") or 0) + 1
    _update_heartbeat(
        window_ts=current_open,
        elapsed=round(elapsed, 1),
        in_zone=True,
        target_open=current_open,
        reason="trade",
        direction=decision["side"],
        z30=round(decision["z30"], 3),
        vol30=round(decision["vol30"], 6),
        ask=round(decision["ask"], 4),
        edge=round(decision["edge"], 4),
        p_model=round(decision["p_model"], 4),
    )

    signal_time = _utc(int(S))
    entry_time = _utc(current_open)          # market already open
    close_time = _utc(close_ts)

    item = _build_signal_dict(
        pair="BTC",
        symbol=symbol,
        direction=decision["side"],
        decision=decision,
        signal_time=signal_time,
        entry_time=entry_time,
        close_time=close_time,
        window_ts=current_open,
        market=market,
    )
    logger.info(
        "[L5 SIGNAL] %s z=%.3f ask=%.4f edge=%.4f p_model=%.3f vol=%.5f market=%s",
        decision["side"],
        decision["z30"],
        decision["ask"],
        decision["edge"],
        decision["p_model"],
        decision["vol30"],
        market.get("slug"),
    )
    return [item]


def scan_latest(pairs: Optional[List[str]] = None) -> List[Dict]:
    pairs = pairs or list(ACTIVE_PAIRS)
    now = datetime.now(timezone.utc)
    out: List[Dict] = []
    for symbol in pairs:
        out.extend(scan_one_symbol(symbol, now=now))
    return out


def risk_allows_new_trade(
    db_session, user_id: Optional[int] = None, stake_usd: float = 1.0
) -> Tuple[bool, str]:
    """No circuit breaker — matches backtested profile."""
    return True, "ok"
