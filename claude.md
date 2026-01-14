# Kalshi Shock-Fade Mean-Reversion Trading Bot

## Project Overview

This is an autonomous trading bot for Kalshi prediction markets that implements a shock-fade mean-reversion strategy. The bot detects unusually large, fast price movements ("shocks") in basketball Game Winner contracts, takes the opposite side (fades the move), and exits when the price reverts toward a short-term baseline.

**Key Principle**: The bot uses ONLY Kalshi market data (order book, trades, metadata). No external sports feeds.

## Architecture

```
kalshi-bot/
├── src/
│   ├── main.py                 # Entry point, orchestration
│   ├── config.py               # Configuration and parameters
│   ├── kalshi_client.py        # Kalshi API wrapper (REST + WebSocket)
│   ├── market_data.py          # Order book management, data buffers
│   ├── signals.py              # Signal calculations (MID, BASELINE, VOL, etc.)
│   ├── strategy.py             # State machine and trading logic
│   ├── order_manager.py        # Order submission, tracking, cancellation
│   ├── position_manager.py     # Position tracking and P&L
│   ├── logger.py               # Structured logging for analysis
│   └── utils.py                # Time helpers, math utilities
├── tests/
│   ├── test_signals.py
│   ├── test_strategy.py
│   └── test_state_machine.py
├── logs/                       # Trade logs and signal logs
├── .env                        # API credentials (gitignored)
├── requirements.txt
└── README.md
```

## Core Components

### 1. State Machine States
- `FLAT`: No open position
- `ENTERING`: Limit order submitted, awaiting fill
- `LONG_YES`: Holding YES contracts
- `LONG_NO`: Holding NO contracts
- `COOLDOWN`: 60-second pause after closing position

### 2. Rolling Data Buffers
Maintain circular buffers for:
- MID prices (at least 120 seconds of history)
- 1-second MID changes (at least 60 seconds for VOL_60)
- Timestamps for each data point

### 3. Signal Calculations
```python
MID = (YES_BID + YES_ASK) / 2
RET_10 = MID(now) - MID(10 seconds ago)
BASELINE = 2-minute EMA of MID
VOL_60 = std dev of 1-second MID changes over last 60 seconds
DELTA = MID - BASELINE
SHOCK_TH = max(0.06, 3 * VOL_60)
DELTA_TH = max(0.04, 2 * VOL_60)
EXIT_BAND = max(0.015, VOL_60)
```

## Default Parameters (v0)

| Parameter | Value | Description |
|-----------|-------|-------------|
| `UPDATE_CADENCE` | 1s | Signal computation frequency |
| `BASELINE_WINDOW` | 120s | EMA window for baseline |
| `VOLATILITY_WINDOW` | 60s | Window for VOL_60 calculation |
| `SHOCK_WINDOW` | 10s | Window for detecting fast moves |
| `MIN_SHOCK` | 0.06 | Floor for shock threshold (6¢) |
| `SHOCK_MULTIPLIER` | 3.0 | Shock threshold = 3 × VOL_60 |
| `MIN_DEVIATION` | 0.04 | Floor for deviation threshold (4¢) |
| `DEVIATION_MULTIPLIER` | 2.0 | Deviation threshold = 2 × VOL_60 |
| `ENTRY_FILL_TIMEOUT` | 5s | Cancel unfilled orders after 5s |
| `EXIT_BAND_FLOOR` | 0.015 | Reversion exit threshold (1.5¢) |
| `MAX_HOLD_TIME` | 180s | Time stop for positions |
| `REPEAT_SHOCK_WINDOW` | 30s | Exit on adverse shock within 30s |
| `COOLDOWN_DURATION` | 60s | Pause after closing trade |
| `MIN_TTS` | 180s | No entries in last 3 minutes |
| `DATA_STALE_THRESHOLD` | 5s | Flatten if no updates for 5s |

## Coding Standards

### Python Style
- Python 3.11+
- Use type hints for all functions
- Use dataclasses for structured data
- Use enums for state machine states
- Async/await for WebSocket and concurrent operations
- Follow PEP 8 naming conventions

### Error Handling
- Never crash on API errors - log and recover
- Implement exponential backoff for reconnections
- Always have a path to flatten positions on critical errors
- Validate all API responses before processing

### Logging Requirements
Log to both console and file. Required log events:
- Every order book update (optional, can be sampled)
- Every shock detection with all parameters
- Every trade entry/exit with full context
- State transitions
- Errors and reconnections

### Testing
- Unit tests for all signal calculations
- Unit tests for state machine transitions
- Mock the Kalshi API for strategy tests
- Test edge cases: missing data, stale data, rapid state changes

## Kalshi API Integration

### Authentication
- Use API key + secret from environment variables
- Implement proper request signing
- Handle token refresh if using session tokens

### REST Endpoints Needed
- `GET /markets/{ticker}` - Market metadata, close time
- `GET /markets/{ticker}/orderbook` - Current order book
- `GET /portfolio/positions` - Current positions
- `POST /portfolio/orders` - Submit orders
- `DELETE /portfolio/orders/{order_id}` - Cancel orders

### WebSocket Streams
- Subscribe to order book updates for target market
- Subscribe to trade stream (optional, for VWAP in v1)
- Handle reconnection gracefully

## Safety Requirements

1. **Position Limits**: Never exceed configured max position size
2. **Data Staleness**: Flatten immediately if no updates for 5+ seconds
3. **TTS Gate**: No new entries when TTS < 180 seconds
4. **Graceful Shutdown**: Flatten all positions on SIGINT/SIGTERM
5. **Rate Limiting**: Respect Kalshi API rate limits

## Entry/Exit Logic Summary

### Entry Conditions (ALL must be true)
1. State is FLAT (not in COOLDOWN)
2. TTS >= 180 seconds
3. Have 120s of MID history and 60s of VOL history
4. Shock detected: |RET_10| >= SHOCK_TH
5. Overreaction confirmed: |DELTA| >= DELTA_TH
6. Shock and DELTA in same direction

### Entry Action
- Shock Up + DELTA > 0 → Buy NO (fade bullish jump)
- Shock Down + DELTA < 0 → Buy YES (fade bearish dump)
- Submit limit at best ask, cancel after 5s if unfilled

### Exit Conditions (ANY triggers exit)
1. Reversion: |DELTA| <= EXIT_BAND
2. Time stop: 180 seconds in trade
3. Repeat shock against position within 30s
4. Data staleness: no updates for 5+ seconds

## Development Phases

### Phase 1: Foundation
- Kalshi API client (REST + WebSocket)
- Order book data management
- Basic logging infrastructure

### Phase 2: Signals
- Rolling buffer implementation
- All signal calculations (MID, BASELINE, VOL_60, etc.)
- Unit tests for signal math

### Phase 3: Strategy
- State machine implementation
- Entry/exit logic
- Order management

### Phase 4: Integration
- End-to-end testing with paper trading
- Logging and analysis tools
- Error handling hardening

## Environment Variables

```
KALSHI_API_KEY=your_api_key
KALSHI_API_SECRET=your_api_secret
KALSHI_API_BASE_URL=https://trading-api.kalshi.com
KALSHI_WS_URL=wss://trading-api.kalshi.com
TARGET_MARKET_TICKER=your_target_market
MAX_POSITION_SIZE=10
LOG_LEVEL=INFO
```

## Important Notes

- All prices are in decimal (0.50 = 50 cents)
- Kalshi uses cents in some API responses - normalize to decimal internally
- The bot should be resilient to WebSocket disconnections
- Log everything needed to evaluate edge post-hoc
- Start with conservative position sizes for v0
