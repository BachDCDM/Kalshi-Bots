#!/usr/bin/env bash
# Run the BTC 15m bot against Kalshi's paper/demo API.
# Usage: ./run_paper.sh
#
# Parameters (from .env.paper):
#   entry=10¢  exit=20¢  window=7min  demo API
#
# Data goes to btc15m_data/trades.db (same file; clear via control panel if needed).

set -euo pipefail
cd "$(dirname "$0")"

if [[ ! -f .env.paper ]]; then
  echo "Missing .env.paper — copy .env.paper.example to .env.paper and add demo API credentials from https://demo.kalshi.co" >&2
  exit 1
fi

# Load .env then override from .env.paper inside Python (see KALSHI_USE_ENV_FILE).
export KALSHI_USE_ENV_FILE=.env.paper

set -a
# shellcheck source=/dev/null
source .env.paper
set +a

exec .venv/bin/python btc15m_bot.py
