# Kalshi Shock-Fade Mean-Reversion Trading Bot

An autonomous trading bot for Kalshi prediction markets that implements a shock-fade mean-reversion strategy for basketball Game Winner contracts.

## Strategy Overview

The bot detects unusually large, fast price movements ("shocks") in live markets and fades them (takes the opposite side), exiting when the price reverts toward a short-term baseline. The entire signal is computed from Kalshi prices and timestamps only—no external sports feeds required.

### Key Concept

When rapid one-sided order flow causes the YES price to temporarily overshoot, the bot:
1. Detects the shock (fast, large price move)
2. Confirms overreaction (price far from recent baseline)
3. Fades the move (buys the opposite side)
4. Exits on reversion, time stop, or adverse continuation

## Implementation Status

✅ **Complete Implementation** - All modules have been implemented:
- Configuration management with Pydantic
- Kalshi API client with RSA-PSS authentication
- Market data management with rolling buffers
- Signal calculations (MID, BASELINE, VOL_60, RET_10, DELTA, thresholds)
- State machine with full entry/exit logic
- Order and position management
- Structured logging with separate event streams
- Main orchestration with graceful shutdown

## Installation

### Prerequisites

- Python 3.11 or higher (Python 3.13.2 recommended)
- Kalshi trading account with API access
- API key and **RSA private key** from Kalshi

### Important: Authentication Method

**Critical**: This implementation uses **RSA-PSS with SHA256** authentication (not HMAC-SHA256). You need an RSA private key in PEM format for the `KALSHI_API_SECRET` environment variable.

### Setup

1. Navigate to the project directory:
   ```bash
   cd "/Users/keithryan/Downloads/Kalshi bot"
   ```

2. Activate the existing Python virtual environment:
   ```bash
   source ~/jupyter_env/bin/activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Configure environment:
   ```bash
   cp .env.example .env
   # Edit .env with your API credentials
   ```

   **Important**: Set `KALSHI_API_SECRET` to your RSA private key in PEM format (as a single-line string with \n for line breaks, or as a multiline string).

5. Set your target market:
   ```bash
   # Find available markets on Kalshi
   # Example: KXNBA-25JAN14-GSW-YES for NBA game
   ```

## Configuration

### Required Environment Variables

| Variable | Description |
|----------|-------------|
| `KALSHI_API_KEY` | Your Kalshi API key |
| `KALSHI_API_SECRET` | Your Kalshi API secret |
| `KALSHI_API_BASE_URL` | API base URL (prod or demo) |
| `KALSHI_WS_URL` | WebSocket URL |
| `TARGET_MARKET_TICKER` | Market ticker to trade |

### Strategy Parameters

All parameters have sensible defaults but can be overridden in `.env`:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `BASELINE_WINDOW` | 120s | EMA window for "normal" price |
| `VOLATILITY_WINDOW` | 60s | Window for volatility calculation |
| `SHOCK_WINDOW` | 10s | Window for detecting fast moves |
| `MIN_SHOCK` | 6¢ | Floor for shock threshold |
| `SHOCK_MULTIPLIER` | 3× | Shock = 3 × recent volatility |
| `MIN_DEVIATION` | 4¢ | Floor for overreaction threshold |
| `EXIT_BAND_FLOOR` | 1.5¢ | Reversion exit threshold |
| `MAX_HOLD_TIME` | 180s | Time stop for positions |
| `COOLDOWN_DURATION` | 60s | Pause between trades |
| `MIN_TTS` | 180s | No entries in final 3 minutes |

## Usage

### Running the Bot

```bash
python -m src.main
```

### Running Tests

```bash
pytest tests/
```

### Graceful Shutdown

Press `Ctrl+C` to initiate graceful shutdown. The bot will:
1. Cancel any pending orders
2. Close any open positions
3. Save final state to logs
4. Exit cleanly

## Architecture

```
src/
├── main.py              # Entry point and orchestration
├── config.py            # Configuration management
├── kalshi_client.py     # Kalshi API wrapper
├── market_data.py       # Order book and data buffers
├── signals.py           # Signal calculations
├── strategy.py          # State machine and trading logic
├── order_manager.py     # Order lifecycle management
├── position_manager.py  # Position tracking and P&L
├── logger.py            # Structured logging
└── utils.py             # Utilities
```

### State Machine

The bot operates as a state machine with five states:

- **FLAT**: No position, monitoring for entry signals
- **ENTERING**: Order submitted, waiting for fill
- **LONG_YES**: Holding YES contracts (faded a down shock)
- **LONG_NO**: Holding NO contracts (faded an up shock)
- **COOLDOWN**: 60-second pause after closing a trade

## Logging and Analysis

Logs are written to the `logs/` directory in JSON Lines format for easy analysis:

- `trades.jsonl` - All trade entries and exits
- `shocks.jsonl` - All detected shocks
- `signals.jsonl` - Market data snapshots (sampled)
- `errors.jsonl` - Errors and exceptions

### Key Metrics to Track

- Win rate (% of trades that exit on reversion vs time stop)
- Average P&L per trade (net of fees)
- Shock detection accuracy
- Average hold time

## Risk Warnings

⚠️ **This is experimental trading software. Use at your own risk.**

- Start with the Kalshi demo environment
- Use small position sizes initially
- Monitor the bot actively during operation
- Understand that prediction markets can move against you rapidly
- Past performance does not guarantee future results

## Development

### Adding New Features

See `claude.md` for detailed development guidelines, coding standards, and v1 improvement ideas.

### Testing

All signal calculations and state transitions should have unit tests. Mock the Kalshi API for strategy tests to avoid real API calls.

## License

[Your license here]

## Disclaimer

This software is provided for educational purposes only. Trading prediction markets involves risk of loss. The authors are not responsible for any financial losses incurred through use of this software.
