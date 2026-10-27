# Polymarket BTC Bot — L5 Value Lock

Production trading bot for Polymarket **Bitcoin Up or Down 5-minute** markets.

Runs a single causal strategy: **L5 Value Lock**.

| Strategy | Module | Window | Signal time | Target market | Status |
|----------|--------|--------|-------------|---------------|--------|
| **L5 Value Lock** | `app/strategy_l5.py` | **5m** | current open + **240s** | **RUNNING** market | **ACTIVE** |

15m strategies and prior 5m strategies (S3x_MomAlign, COMBINED OR) have been removed.

---

## How Polymarket 5-minute Up/Down markets work

- **Slug**: `btc-updown-5m-{unix_open}`
- Markets open every **300 seconds** on fixed Unix boundaries.
- Each market has two outcome tokens: **Up** and **Down**.
- Resolution is binary against the BTC price path over the 5-minute window (oracle).
- CLOB order book exists for both tokens; the bot places FAK/GTC orders against best ask.
- Markets are often tradable before formal open (pre-market tape), but **L5 only trades the already-running market**.

### Critical design difference vs prior strategies

Prior 5m strategies in this codebase fired at W+240 and bought the **next** market (opens at W+300).

**L5 fires at W+240 and buys the market that is already running** (opened at W, resolves at W+300).  
At that moment ~90 % of the oracle path is visible on Binance while Polymarket quotes can still be stale.

---

## Strategy — L5 Value Lock

| Item | Value |
|------|--------|
| Module | `app/strategy_l5.py` |
| Signal | W + **240s** (60 s before running market resolves) |
| Target | **Running** 5m market (open = W) |
| Stake | $1 |
| ID | `l5_value_lock_5m` |

### Rule (every number frozen)

1. **Lead z-score**  
   \( z_{30} = \ln(P_S / \text{ref}) / (\text{vol}_{30} \sqrt{0.5}) \)  
   - \(P_S\): Binance 1m close that ends at S  
   - \(\text{ref}\): geometric price 30 s before market open  
   - \(\text{vol}_{30}\): RMS of last 30 one-minute log returns  

2. **Side** — UP if \(z_{30}>0\), DOWN if \(z_{30}<0\)

3. **Strength** — \(|z_{30}| \ge 1.0\)

4. **Ask** — live CLOB best ask preferred; else last ask-stream print.  
   Age ≤ 15 s, strictly between 0.02 and 0.995.

5. **Value** — \(\Phi(|z_{30}|) - \text{ask} \ge 0.03\)

6. **Vol guard** — \(\text{vol}_{30} \ge 0.25 \times\) median of previous 2016 signal-time vols.  
   Strategy is silent until ≥ 576 prior values exist (~2 days). On cold start the bot seeds history from recent Binance 1m klines.

No stop-loss, no circuit breaker, no ML. Position held to resolution.

### In-sample evidence (study sample, before fees)

2026-09-06 → 2026-10-09 (32.2 days):  
544 trades · 16.9 / day · hit 88.8 % · mean ask 0.805 · EV **+21.1 %** · PnL +$114.6 · max DD $2.9 · max loss streak 2.

---

## System architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ Scheduler (APScheduler, 1s cadence)                             │
│  ├─ scan_and_watch every 1s                                     │
│  │    • if elapsed in L5 zone (t+232 → t+248 of current 5m)     │
│  │      → services.process_new_signals                          │
│  │         └─ strategy_l5.scan_one_symbol                       │
│  │    • retry_watching_signals / user_trades                    │
│  ├─ resolve at every 5m close + 1s poll + catch-up              │
│  └─ daily Telegram summary (00:05 UTC)                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ services.py                                                     │
│  • process_new_signals → execute (shadow or live CLOB)          │
│  • resolve_pending_signals (oracle / outcome prices)            │
│  • stats, fan-out to multi-user trades, Telegram alerts         │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
        polymarket.py    strategy_l5.py   telegram_notify.py
        (CLOB, Gamma,    (z30, edge,      (signal / fill /
         discovery)       vol guard)       resolve / daily)
```

### Signal → execution flow at S = W+240

1. Scheduler detects L5 decision zone (elapsed ∈ [232, 248]).
2. `strategy_l5.scan_one_symbol` loads Binance 1m closes, seeds/updates `vol_hist`, discovers the **running** market, reads CLOB asks + recent tape.
3. Pure L5 gates run. If all pass → one signal dict with `window_ts = W` (running open), `max_price ≈ ask + 1¢`.
4. `services._execute_one_signal` records the Signal row and, if `enabled_5m` and mode=live, places a FAK/GTC order on the chosen token.
5. Position is held; resolve job marks win/loss when the market closes at W+300.

There is **no extra time buffer** beyond the narrow scan zone needed for a 1-second poller. L5 itself decides only once per running market (`_emitted`).

---

## Repository structure

```
polymarket-bot/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI app, dashboard, pulse, settings routes
│   ├── scheduler.py         # APScheduler jobs (scan / resolve / daily)
│   ├── services.py          # Signal execution, resolution, stats, user fan-out
│   ├── strategy_l5.py       # ★ L5 Value Lock (only active strategy)
│   ├── polymarket.py        # Gamma + CLOB client, market discovery, orders
│   ├── database.py          # SQLAlchemy engine, migrations
│   ├── models.py            # Setting, Signal, User, UserTrade, …
│   ├── auth.py              # Session auth, password setup
│   ├── multiuser.py         # Per-user credentials & managed wallets
│   └── telegram_notify.py   # Telegram alerts
├── templates/               # Jinja2 UI (dashboard, history, settings, pulse)
├── static/css/app.css
├── requirements.txt
├── Dockerfile
├── fly.toml
├── start_ec2.sh
├── .env.example
├── MANAGED_WALLET.md
└── README.md
```

### Removed modules

- `app/strategy.py` (15m dual-regime)
- `app/strategy_s3x_momalign.py`
- `app/strategy_combined.py`

---

## Settings that matter

| Setting | Effect |
|---------|--------|
| `mode` | `shadow` (paper) or `live` (real CLOB orders) |
| `order_type` | `FAK` (default) or `GTC` |
| `position_size_usd` | Stake scale (strategy default $1) |
| `enabled_5m` | Master toggle for live fills of L5 |
| `enabled_btc` | Must be true (strategy is BTC-only) |
| `enabled_15m` | Hard-disabled |

Operator settings live in the `settings` table; per-user overrides in `user_settings`.

---

## Telegram

On signal / fill / resolve / daily summary the bot posts via `telegram_notify.py` when `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` are set.

---

## Deployment notes

- Process must stay alive continuously: `_emitted` and `_vol_hist` are in-memory.
- On restart, vol history is re-seeded from recent Binance 1m klines (requires ≥ ~576 prior 5m signal times ≈ 2 days of data).
- Needs outbound access to Binance data API, Gamma, and Polymarket CLOB.
- Live trading requires valid CLOB credentials (operator `.env` or per-user managed wallet).

```bash
# Local
cp .env.example .env   # fill secrets
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000

# Docker / Fly / EC2 — see Dockerfile, fly.toml, start_ec2.sh
```

---

## Safety / limits (read before going live)

- Fills use the live CLOB best ask; L5 still requires the value edge ≥ 3 ¢.
- Strategy parameters were frozen on a 32-day 2026 sample; only out-of-sample data can confirm them.
- If the resolution oracle’s lag relative to Binance changes, the edge disappears.
- Fees are not modelled inside the strategy module.
- Cheap asks contributed a large share of historical PnL; they are also the most likely to be stale.

---


---

## Deploy on Wasmer Edge

Wasmer Edge runs this FastAPI + APScheduler bot as a long-lived WASIX Python process.

### Prerequisites

1. [Create a Wasmer account](https://wasmer.io/signup)
2. Install the CLI:
   ```bash
   curl https://get.wasmer.io -sSfL | sh
   wasmer login
   ```

### Configure

1. Edit `app.yaml` and set `owner` to your Wasmer username/namespace.
2. (Optional) Change `name` if `polymarket-bot-l5` is taken.

### Secrets (recommended)

Never put private keys in `app.yaml`. Create secrets after the first deploy:

```bash
wasmer app secret create --app polymarket-bot-l5 APP_SECRET
wasmer app secret create --app polymarket-bot-l5 ADMIN_PASSWORD
wasmer app secret create --app polymarket-bot-l5 POLYMARKET_PRIVATE_KEY
wasmer app secret create --app polymarket-bot-l5 POLYMARKET_FUNDER_ADDRESS
# optional
wasmer app secret create --app polymarket-bot-l5 DATABASE_URL
wasmer app secret create --app polymarket-bot-l5 TELEGRAM_BOT_TOKEN
wasmer app secret create --app polymarket-bot-l5 TELEGRAM_CHAT_ID
```

Default `DATABASE_URL` in `app.yaml` is SQLite under `/app/data/` (persists only for the instance lifetime unless you attach a volume). For production use a remote Postgres URL as a secret.

### Deploy

From the project root:

```bash
wasmer deploy
```

The CLI builds the package from `wasmer.toml`, uploads it, and starts the app.  
URL form: `https://<name>-<owner>.wasmer.app` (or the short name if available).

Redeploy after code changes with the same command.

### Local Wasmer run (optional)

```bash
wasmer run . --net --env PORT=8080 --env PYTHONPATH=/app
```

### Important Wasmer notes

| Topic | Detail |
|-------|--------|
| **Process model** | One uvicorn worker; APScheduler runs inside the same process (1s scan loop). |
| **Entrypoint** | `app.main:app` (ASGI). |
| **Port** | Edge expects the process to listen on port **80** inside the container. |
| **SQLite** | Default path `/app/data/polymarket_bot.db`. Use Postgres secret for durable multi-instance storage. |
| **Native deps** | `numpy`, `pandas`, `requests`, `fastapi` work on Wasmer Python. `web3` / `py-clob-client-v2` may need the Wasmer package index builds; if live CLOB fails to import, run **shadow** mode first and verify logs. |
| **Networking** | Outbound HTTPS to Binance, Gamma, and Polymarket CLOB must be allowed (default on Edge). |
| **Cold start** | L5 vol history is re-seeded from Binance 1m klines; strategy stays silent until ≥576 prior signal vols (~2 days of data). |

### Files added for Wasmer

```
wasmer.toml      # package + uvicorn command
app.yaml         # Edge app name, owner, env defaults
.wasmerignore    # exclude .env, venv, db files from package
data/            # SQLite directory (mapped under /app/data)
```


---

## Deploy on Fly.io

Requires **`Dockerfile`** and **`fly.toml` at the GitHub repo root** (same branch Fly builds).

### 1. Push these files to GitHub

```
Dockerfile
fly.toml
requirements-fly.txt
requirements.txt
app/
templates/
static/
...
```

### 2. CLI deploy

```bash
# Install flyctl once: https://fly.io/docs/hands-on/install-flyctl/
fly auth login
cd polymarket-bot   # repo root with Dockerfile

fly launch --copy-config --name polymarket-bot-l5 --region ams --no-deploy
fly secrets set APP_SECRET="long-random" ADMIN_USERNAME="admin" ADMIN_PASSWORD="strong"
# optional live trading:
# fly secrets set POLYMARKET_PRIVATE_KEY=0x... POLYMARKET_FUNDER_ADDRESS=0x...
fly deploy
```

### 3. UI deploy

Fly dashboard → New app → connect GitHub repo → branch **main** →  
build type **Dockerfile** → deploy.

If you see *Could not find a Dockerfile*, the file is missing from that branch — push it first.

### Fly vs Wasmer

| | Wasmer | Fly |
|--|--------|-----|
| Build | `requirements.txt` (no web3) | `Dockerfile` + `requirements-fly.txt` |
| Live CLOB | Shadow only | Full (web3 + py-clob) |
| Process | uvicorn :80 | gunicorn + uvicorn worker :8080 |


## License / disclaimer

For research and operational use by the operator only. Trading binary markets involves risk of total loss of stake.
