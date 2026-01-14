# Kalshi Trading Bot - Quick Start Guide

## ✅ Current Status

**Implementation**: Complete
**Tests**: 26/26 passing (100%)
**Ready for**: Demo API testing

## 🚀 Quick Start (Demo Mode)

### 1. Get Kalshi API Credentials

1. Sign up at [Kalshi](https://kalshi.com)
2. Generate API credentials from your account settings
3. Save your API key and RSA private key

### 2. Configure Environment

```bash
cd "/Users/keithryan/Downloads/Kalshi bot"

# Copy the example env file
cp .env.example .env

# Edit with your credentials
nano .env
```

Add your credentials to `.env`:

```bash
# Kalshi API Credentials
KALSHI_API_KEY=your_api_key_here
KALSHI_API_SECRET="-----BEGIN PRIVATE KEY-----
[Your RSA private key content here]
-----END PRIVATE KEY-----"

# Demo API URLs (for testing)
KALSHI_API_BASE_URL=https://demo-api.kalshi.co/trade-api/v2
KALSHI_WS_URL=wss://demo-api.kalshi.co/trade-api/ws/v2

# Target Market
TARGET_MARKET_TICKER=KXNBA-25JAN14-GSW-YES

# Risk Management
MAX_POSITION_SIZE=10
MAX_CONTRACTS_PER_TRADE=5

# Logging
LOG_LEVEL=INFO
LOG_DIR=logs
```

**Important**:
- Use demo URLs for initial testing
- Find active markets on Kalshi's website
- Basketball Game Winner contracts work best with this strategy

### 3. Install Dependencies

```bash
source ~/jupyter_env/bin/activate
pip install -r requirements.txt
```

### 4. Run Tests (Optional but Recommended)

```bash
pytest tests/ -v
```

Expected output: `26 passed in 0.28s`

### 5. Run the Bot

```bash
python -m src.main
```

### 6. Monitor Logs

Open new terminal windows to watch logs in real-time:

```bash
# Terminal 1: Watch for shock detections
tail -f logs/shocks.jsonl | jq .

# Terminal 2: Watch for trades
tail -f logs/trades.jsonl | jq .

# Terminal 3: Watch for errors
tail -f logs/errors.jsonl | jq .
```

(Install `jq` with `brew install jq` for pretty JSON formatting)

### 7. Graceful Shutdown

Press `Ctrl+C` in the bot terminal. The bot will:
1. Cancel any pending orders
2. Close any open positions
3. Save logs
4. Exit cleanly

## 📊 What to Expect

### Normal Operation

**Shock Detection Rate**: 5-20 shocks per hour for active basketball markets

**Trade Frequency**: 0-5 trades per hour

**State Transitions**:
```
FLAT (watching)
  ↓ (shock + overreaction detected)
ENTERING (order submitted)
  ↓ (order filled)
LONG_YES or LONG_NO (holding position)
  ↓ (reversion / time stop / repeat shock)
COOLDOWN (60s pause)
  ↓ (cooldown complete)
FLAT (ready to trade again)
```

### Console Output

You should see:
```
[INFO] bot_initializing
[INFO] market_loaded ticker=KXNBA-25JAN14-GSW-YES
[INFO] positions_synced position_count=0
[INFO] bot_initialized
[INFO] bot_running
[INFO] websocket_connected
[INFO] subscribed_to_orderbook ticker=KXNBA-25JAN14-GSW-YES
[INFO] shock_detected shock_direction=UP ret_10=0.08 ...
```

### Log Files

Check `logs/` directory for:
- `trades.jsonl` - Every trade entry and exit
- `shocks.jsonl` - Every shock detection
- `signals.jsonl` - Market data snapshots (sampled)
- `errors.jsonl` - Any errors or issues

## 🔧 Troubleshooting

### Problem: "No module named 'pydantic_settings'"

**Solution**:
```bash
source ~/jupyter_env/bin/activate
pip install pydantic-settings
```

### Problem: "API authentication failed"

**Solution**: Check that:
1. Your API key is correct
2. Your RSA private key is in PEM format
3. The key is properly formatted in .env (use quotes for multiline strings)

### Problem: "Market not found"

**Solution**:
1. Check the market ticker is correct
2. Make sure you're using the demo API URL for demo markets
3. Verify the market is still active (hasn't closed)

### Problem: "WebSocket disconnected"

**Solution**:
- This is normal - the bot will automatically reconnect with exponential backoff
- Check your internet connection
- Verify the WebSocket URL is correct

### Problem: "No shock detections after 30 minutes"

**Solution**:
1. Check if the market is active (game in progress)
2. Try a different market with more activity
3. Lower SHOCK_MULTIPLIER in .env (default is 3.0, try 2.5)

### Problem: "Bot exits immediately"

**Solution**:
1. Check logs/errors.jsonl for the error message
2. Verify .env file is properly configured
3. Ensure TARGET_MARKET_TICKER is set

## ⚙️ Strategy Parameters

You can tune the strategy by editing `.env`:

### Sensitivity (Shock Detection)
```bash
MIN_SHOCK=0.06              # Lower = more sensitive (more shocks detected)
SHOCK_MULTIPLIER=3.0        # Lower = more sensitive
```

### Entry Threshold
```bash
MIN_DEVIATION=0.04          # Lower = easier to enter trades
DEVIATION_MULTIPLIER=2.0    # Lower = easier to enter
```

### Exit Behavior
```bash
EXIT_BAND_FLOOR=0.015       # Lower = faster exits on reversion
MAX_HOLD_TIME=180           # Seconds before time stop (default 3 min)
```

### Risk Management
```bash
MAX_POSITION_SIZE=10        # Never hold more than this many contracts
MAX_CONTRACTS_PER_TRADE=5   # Never enter more than this per trade
MIN_TTS=180                 # Don't enter in last 3 minutes
```

### Cooldown
```bash
COOLDOWN_DURATION=60        # Seconds to wait after closing a trade
```

## 📈 Performance Metrics

Analyze your bot's performance:

```bash
# Count total trades
cat logs/trades.jsonl | grep "trade_exit" | wc -l

# Calculate win rate (exits on reversion vs time stop)
cat logs/trades.jsonl | grep '"reason":"reversion"' | wc -l
cat logs/trades.jsonl | grep '"reason":"time_stop"' | wc -l

# Average hold time
cat logs/trades.jsonl | grep "trade_exit" | jq '.hold_time' | awk '{sum+=$1; count++} END {print sum/count}'

# Total P&L
cat logs/trades.jsonl | grep "trade_exit" | jq '.pnl' | awk '{sum+=$1} END {print sum}'
```

## 🎯 Next Steps

### After Demo Testing (24-48 hours)

1. **Review Performance**:
   - Win rate should be >50%
   - Average hold time should be <180s
   - Shock detection rate should be 5-20/hour

2. **Switch to Live API** (if satisfied with demo results):
   ```bash
   # Edit .env:
   KALSHI_API_BASE_URL=https://trading-api.kalshi.com/trade-api/v2
   KALSHI_WS_URL=wss://trading-api.kalshi.com/trade-api/ws/v2
   ```

3. **Start Small**:
   ```bash
   MAX_POSITION_SIZE=2
   MAX_CONTRACTS_PER_TRADE=1
   ```

4. **Monitor Actively**: Watch the bot during the first few hours of live trading

5. **Scale Gradually**: Increase position sizes as you gain confidence

## ⚠️ Warnings

- **Use demo mode first** - Never start with live trading
- **Real money risk** - Only trade what you can afford to lose
- **No guarantees** - Past performance doesn't predict future results
- **Monitor actively** - Don't leave the bot unattended initially
- **Understand the strategy** - Read claude.md for full details

## 🆘 Support

If you encounter issues:

1. Check `logs/errors.jsonl` for error messages
2. Review this troubleshooting guide
3. Verify all configuration in `.env`
4. Run tests: `pytest tests/ -v`
5. Check Kalshi API status

## 📚 Additional Resources

- **Full Specification**: See `claude.md`
- **Implementation Details**: See `PROMPT.md`
- **Test Results**: See `TEST_RESULTS.md`
- **Kalshi API Docs**: https://docs.kalshi.com
