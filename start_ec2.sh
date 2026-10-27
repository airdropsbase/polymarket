#!/usr/bin/env bash
# Start Polymarket bot on EC2 (port 8081)
set -euo pipefail

APP_DIR="${APP_DIR:-/home/ec2-user/polymarket-dual}"
cd "$APP_DIR"

if [[ ! -f .env ]]; then
  echo "ERROR: $APP_DIR/.env not found"
  exit 1
fi

# Activate venv
if [[ -f .venv/bin/activate ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

# Stop previous instance on 8081
# Only kill THIS port — never all uvicorn processes
fuser -k 8081/tcp 2>/dev/null || true
pkill -f "uvicorn app.main:app --host 0.0.0.0 --port 8081" 2>/dev/null || true
sleep 1

# Load .env safely (handles quotes, spaces, export lines)
set -a
# shellcheck disable=SC1091
source <(sed -e '/^[[:space:]]*#/d' -e '/^[[:space:]]*$/d' -e 's/^export //' .env | sed 's/\r$//')
set +a

# Quick sanity check
echo "APP_DIR=$APP_DIR"
echo "POLYMARKET_PRIVATE_KEY set: $([ -n "${POLYMARKET_PRIVATE_KEY:-}" ] && echo yes || echo NO)"
echo "POLYMARKET_FUNDER_ADDRESS: ${POLYMARKET_FUNDER_ADDRESS:-(empty)}"
echo "POLYMARKET_SIGNATURE_TYPE: ${POLYMARKET_SIGNATURE_TYPE:-(default)}"
echo "APP_SECRET set: $([ -n "${APP_SECRET:-}" ] && echo yes || echo NO)"

nohup uvicorn app.main:app --host 0.0.0.0 --port 8081 >> bot.log 2>&1 &
echo "Started PID $!"
sleep 1
tail -n 20 bot.log
