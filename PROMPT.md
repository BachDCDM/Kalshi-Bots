# Claude Code Prompt: Build Kalshi Shock-Fade Trading Bot

## Context

You are building an autonomous trading bot for Kalshi prediction markets. The bot implements a "shock-fade" mean-reversion strategy that:
1. Detects unusually large, fast price movements (shocks) in basketball Game Winner contracts
2. Fades the move (takes the opposite side)
3. Exits when price reverts toward a short-term baseline

**Critical**: This bot uses ONLY Kalshi market data (order book, trades, metadata). No external sports feeds.

Read the `claude.md` file in this project for full architectural context, parameters, and coding standards.

---

## Task: Build the Complete Trading Bot

### Step 1: Project Setup

Create the project structure:
```
kalshi-bot/
├── src/
│   ├── __init__.py
│   ├── main.py
│   ├── config.py
│   ├── kalshi_client.py
│   ├── market_data.py
│   ├── signals.py
│   ├── strategy.py
│   ├── order_manager.py
│   ├── position_manager.py
│   ├── logger.py
│   └── utils.py
├── tests/
│   ├── __init__.py
│   ├── test_signals.py
│   ├── test_strategy.py
│   └── test_state_machine.py
├── .env.example
├── requirements.txt
└── README.md
```

Create `requirements.txt` with:
- `httpx` (async HTTP client)
- `websockets` (WebSocket client)
- `python-dotenv` (environment variables)
- `numpy` (numerical calculations)
- `pytest` and `pytest-asyncio` (testing)

---

### Step 2: Configuration Module (`config.py`)

Create a configuration class with all strategy parameters:

```python
# All parameters from the spec with defaults:
UPDATE_CADENCE = 1.0          # seconds
BASELINE_WINDOW = 120         # seconds (2-minute EMA)
VOLATILITY_WINDOW = 60        # seconds
SHOCK_WINDOW = 10             # seconds
MIN_SHOCK = 0.06              # 6 cents
SHOCK_MULTIPLIER = 3.0        # 3 × VOL_60
MIN_DEVIATION = 0.04          # 4 cents
DEVIATION_MULTIPLIER = 2.0    # 2 × VOL_60
ENTRY_FILL_TIMEOUT = 5.0      # seconds
EXIT_BAND_FLOOR = 0.015       # 1.5 cents
MAX_HOLD_TIME = 180           # seconds
REPEAT_SHOCK_WINDOW = 30      # seconds
COOLDOWN_DURATION = 60        # seconds
MIN_TTS = 180                 # seconds (3 minutes)
DATA_STALE_THRESHOLD = 5.0    # seconds
```

Load API credentials from environment variables. Use a dataclass or Pydantic model for type safety.

---

### Step 3: Kalshi API Client (`kalshi_client.py`)

Implement an async client for the Kalshi API:

#### REST Methods:
- `get_market(ticker: str)` - Get market metadata including close time
- `get_orderbook(ticker: str)` - Get current order book
- `get_positions()` - Get account positions
- `submit_order(ticker: str, side: str, price: float, size: int)` - Submit limit order
- `cancel_order(order_id: str)` - Cancel an order
- `get_order_status(order_id: str)` - Check order status

#### WebSocket:
- Connect to order book stream
- Parse order book update messages
- Emit updates via callback or async queue
- Handle reconnection with exponential backoff

#### Authentication:
- Sign requests using HMAC-SHA256 with API secret
- Include required headers: `KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-SIGNATURE`, `KALSHI-ACCESS-TIMESTAMP`

Reference Kalshi API docs for exact endpoint paths and message formats.

---

### Step 4: Market Data Module (`market_data.py`)

Implement rolling data buffers and order book management:

#### OrderBook class:
```python
@dataclass
class OrderBook:
    yes_bid: float | None
    yes_ask: float | None
    no_bid: float | None
    no_ask: float | None
    timestamp: float
    
    @property
    def mid(self) -> float | None:
        if self.yes_bid and self.yes_ask:
            return (self.yes_bid + self.yes_ask) / 2
        return None
```

#### RollingBuffer class:
- Fixed-size circular buffer with timestamps
- Methods: `add(value, timestamp)`, `get_value_at(seconds_ago)`, `get_values_since(seconds_ago)`
- Efficient lookups by timestamp

#### MarketDataManager class:
- Maintains rolling buffers for MID prices
- Tracks last update timestamp
- Provides `is_stale(threshold)` method
- Stores at least 120 seconds of history

---

### Step 5: Signals Module (`signals.py`)

Implement all signal calculations:

```python
class SignalCalculator:
    def __init__(self, config: Config, data_manager: MarketDataManager):
        ...
    
    def calculate_mid(self, orderbook: OrderBook) -> float | None:
        """MID = (YES_BID + YES_ASK) / 2"""
        
    def calculate_baseline(self) -> float | None:
        """2-minute EMA of MID prices"""
        
    def calculate_vol_60(self) -> float | None:
        """Std dev of 1-second MID changes over last 60 seconds"""
        
    def calculate_ret_10(self) -> float | None:
        """MID(now) - MID(10 seconds ago)"""
        
    def calculate_delta(self) -> float | None:
        """MID - BASELINE"""
        
    def calculate_shock_threshold(self) -> float:
        """max(0.06, 3 * VOL_60)"""
        
    def calculate_delta_threshold(self) -> float:
        """max(0.04, 2 * VOL_60)"""
        
    def calculate_exit_band(self) -> float:
        """max(0.015, VOL_60)"""
        
    def detect_shock(self) -> str | None:
        """Returns 'UP', 'DOWN', or None"""
        
    def detect_overreaction(self) -> str | None:
        """Returns 'UP', 'DOWN', or None"""
```

#### EMA Calculation:
```python
def ema(values: list[float], span: int) -> float:
    """
    Exponential moving average with given span.
    alpha = 2 / (span + 1)
    """
```

---

### Step 6: State Machine (`strategy.py`)

Implement the trading state machine:

```python
from enum import Enum, auto

class State(Enum):
    FLAT = auto()
    ENTERING = auto()
    LONG_YES = auto()
    LONG_NO = auto()
    COOLDOWN = auto()

class Strategy:
    def __init__(self, config, signal_calculator, order_manager, position_manager, logger):
        self.state = State.FLAT
        self.entry_time: float | None = None
        self.entry_price: float | None = None
        self.cooldown_start: float | None = None
        self.last_shock_time: float | None = None
        self.last_shock_direction: str | None = None
        self.pending_order_id: str | None = None
        
    async def on_orderbook_update(self, orderbook: OrderBook, tts: float):
        """Main strategy loop - called on each order book update"""
        
    def check_entry_eligibility(self, tts: float) -> bool:
        """Check TTS gate, state gate, and data gate"""
        
    async def try_entry(self, shock_direction: str):
        """Submit entry order based on shock direction"""
        
    async def check_exit_conditions(self) -> str | None:
        """Check all exit conditions, return exit reason or None"""
        
    async def execute_exit(self, reason: str):
        """Close position and transition to COOLDOWN"""
```

#### State Transitions:
- FLAT → ENTERING: Entry signal triggered, order submitted
- ENTERING → LONG_YES/LONG_NO: Order filled
- ENTERING → FLAT: Order timeout (5s), order cancelled
- LONG_YES/LONG_NO → COOLDOWN: Exit triggered
- COOLDOWN → FLAT: 60 seconds elapsed

---

### Step 7: Order Manager (`order_manager.py`)

Handle order lifecycle:

```python
class OrderManager:
    def __init__(self, kalshi_client, config, logger):
        ...
    
    async def submit_limit_order(
        self, 
        ticker: str, 
        side: Literal["yes", "no"], 
        price: float, 
        size: int
    ) -> str:
        """Submit order, return order_id"""
        
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel order, return success"""
        
    async def wait_for_fill(self, order_id: str, timeout: float) -> bool:
        """Poll for fill status, return True if filled"""
        
    async def market_exit(self, ticker: str, side: str, size: int):
        """Exit position at market (use aggressive limit)"""
```

---

### Step 8: Position Manager (`position_manager.py`)

Track positions and P&L:

```python
@dataclass
class Position:
    ticker: str
    side: Literal["yes", "no"]
    size: int
    entry_price: float
    entry_time: float

class PositionManager:
    def __init__(self, kalshi_client, logger):
        self.positions: dict[str, Position] = {}
        
    async def sync_positions(self):
        """Sync with Kalshi account positions"""
        
    def record_entry(self, ticker, side, size, price):
        """Record new position"""
        
    def record_exit(self, ticker, exit_price) -> float:
        """Record exit, return realized P&L"""
        
    def get_position(self, ticker) -> Position | None:
        """Get current position for ticker"""
```

---

### Step 9: Logger (`logger.py`)

Structured logging for post-hoc analysis:

```python
class TradingLogger:
    def __init__(self, log_dir: str = "logs"):
        ...
    
    def log_update(self, mid, baseline, vol_60, ret_10, delta, tts):
        """Log market data update (can be sampled)"""
        
    def log_shock(self, shock_type, shock_th, ret_10, overreaction, trade_attempted):
        """Log every shock detection"""
        
    def log_trade_entry(self, side, price, size, signals: dict):
        """Log trade entry with all context"""
        
    def log_trade_exit(self, side, entry_price, exit_price, reason, pnl, hold_time):
        """Log trade exit with full details"""
        
    def log_state_transition(self, old_state, new_state, reason):
        """Log state machine transitions"""
        
    def log_error(self, error_type, message, context: dict):
        """Log errors"""
```

Output to both:
- Console (human-readable)
- JSON lines file (machine-parseable for analysis)

---

### Step 10: Main Entry Point (`main.py`)

Orchestrate everything:

```python
async def main():
    # Load configuration
    config = Config.from_env()
    
    # Initialize components
    kalshi_client = KalshiClient(config)
    data_manager = MarketDataManager(config)
    signal_calculator = SignalCalculator(config, data_manager)
    logger = TradingLogger()
    position_manager = PositionManager(kalshi_client, logger)
    order_manager = OrderManager(kalshi_client, config, logger)
    strategy = Strategy(config, signal_calculator, order_manager, position_manager, logger)
    
    # Setup graceful shutdown
    setup_signal_handlers(strategy)
    
    # Connect and run
    await kalshi_client.connect_websocket(config.target_market)
    
    async for orderbook in kalshi_client.orderbook_stream():
        data_manager.update(orderbook)
        tts = calculate_tts(config.market_close_time)
        await strategy.on_orderbook_update(orderbook, tts)

def setup_signal_handlers(strategy):
    """Handle SIGINT/SIGTERM - flatten positions and exit gracefully"""
```

---

### Step 11: Tests

#### test_signals.py:
- Test MID calculation with various order book states
- Test EMA calculation against known values
- Test volatility calculation
- Test shock detection thresholds
- Test edge cases (missing data, zero volatility)

#### test_strategy.py:
- Test entry eligibility conditions
- Test entry logic for shock up/down scenarios
- Test all exit conditions
- Test cooldown behavior

#### test_state_machine.py:
- Test all valid state transitions
- Test invalid transition prevention
- Test timeout handling

---

### Step 12: Documentation

Create `README.md` with:
- Project overview
- Setup instructions
- Configuration guide
- Running instructions
- Log analysis tips
- Risk warnings

Create `.env.example` with all required environment variables (no real values).

---

## Important Implementation Notes

### Price Normalization
- Kalshi may return prices in cents or decimal depending on endpoint
- Normalize ALL prices to decimal internally (0.50 = 50 cents)
- Be explicit about units in variable names if needed

### Thread Safety
- Use asyncio for concurrency, not threads
- Order book updates and strategy logic should be sequential
- Only the WebSocket connection runs concurrently

### Error Recovery
```python
# Pattern for resilient WebSocket connection
async def run_with_reconnect():
    while True:
        try:
            await connect_and_run()
        except WebSocketError:
            logger.log_error("websocket", "Disconnected, reconnecting...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
```

### Position Safety
- Always verify position state with Kalshi before trading
- Never assume local state is correct after errors
- Implement position limits as a hard cap

### Graceful Shutdown
```python
async def shutdown(strategy):
    logger.info("Shutdown signal received")
    if strategy.state in (State.LONG_YES, State.LONG_NO):
        await strategy.execute_exit("shutdown")
    await kalshi_client.close()
```

---

## Verification Checklist

Before considering the bot complete, verify:

- [ ] All signal calculations match the spec formulas exactly
- [ ] State machine handles all transitions correctly
- [ ] Entry only occurs when ALL conditions are met
- [ ] Exit triggers on ANY exit condition
- [ ] Cooldown prevents re-entry for 60 seconds
- [ ] TTS gate blocks entries in last 3 minutes
- [ ] Data staleness triggers position flatten
- [ ] Logs contain all required fields for analysis
- [ ] Graceful shutdown flattens positions
- [ ] All tests pass
- [ ] No hardcoded credentials in code

---

## Getting Started Command

After creating all files, the bot should be runnable with:

```bash
# Install dependencies
pip install -r requirements.txt

# Copy and configure environment
cp .env.example .env
# Edit .env with your credentials

# Run tests
pytest tests/

# Run the bot
python -m src.main
```

---

Build this step by step, testing each component before moving to the next. Start with config and the Kalshi client, then data management, then signals, then the strategy state machine, and finally tie it all together in main.py.
