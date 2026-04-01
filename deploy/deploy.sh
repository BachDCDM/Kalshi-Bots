#!/usr/bin/env bash
# Run on the server from repo root after git pull.
set -euo pipefail
cd "$(dirname "$0")/.."
git pull
# Optional: restart bots + dashboard (ignore if units not installed yet)
systemctl --user restart kalshi-btc15m.service 2>/dev/null || true
systemctl --user restart kalshi-weather-bot.service 2>/dev/null || true
systemctl --user restart kalshi-control-panel.service 2>/dev/null || true
echo "Deploy done."
