"""
Structured logging module for Kalshi trading bot.

Uses structlog for structured logging to both console and JSON files.
Separate log files for different event types: trades, shocks, signals, errors.
"""

import json
import sys
from pathlib import Path
from typing import Any, Optional
import structlog

from src.utils import get_timestamp, format_timestamp


class JSONLinesLogger:
    """Writes JSON lines to a file."""

    def __init__(self, file_path: Path):
        """
        Initialize JSON lines logger.

        Args:
            file_path: Path to log file
        """
        self.file_path = file_path
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.file_handle = open(file_path, "a", encoding="utf-8")

    def write(self, event_dict: dict[str, Any]):
        """
        Write event dict as JSON line.

        Args:
            event_dict: Event dictionary to log
        """
        self.file_handle.write(json.dumps(event_dict) + "\n")
        self.file_handle.flush()

    def close(self):
        """Close log file."""
        if self.file_handle:
            self.file_handle.close()


class TradingLogger:
    """Main trading logger with separate streams for different event types."""

    def __init__(self, log_dir: str = "logs"):
        """
        Initialize trading logger.

        Args:
            log_dir: Directory for log files
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Configure structlog
        structlog.configure(
            processors=[
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.JSONRenderer()
            ],
            wrapper_class=structlog.make_filtering_bound_logger(logging_level=20),  # INFO
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=True,
        )

        self.logger = structlog.get_logger()

        # Create separate log files
        self.trades_log = JSONLinesLogger(self.log_dir / "trades.jsonl")
        self.shocks_log = JSONLinesLogger(self.log_dir / "shocks.jsonl")
        self.signals_log = JSONLinesLogger(self.log_dir / "signals.jsonl")
        self.errors_log = JSONLinesLogger(self.log_dir / "errors.jsonl")

        # Sample counter for market updates
        self.update_counter = 0
        self.update_sample_rate = 10  # Log every 10th update

        self.log_info("logger_initialized", log_dir=str(self.log_dir))

    def log_info(self, event: str, **kwargs):
        """Log info-level event."""
        self.logger.info(event, **kwargs)

    def log_update(
        self,
        mid: Optional[float],
        baseline: Optional[float],
        vol_60: Optional[float],
        ret_10: Optional[float],
        delta: Optional[float],
        tts: float,
        **kwargs
    ):
        """
        Log market data update (sampled).

        Only logs every Nth update to avoid excessive I/O.

        Args:
            mid: Current MID price
            baseline: Baseline (2-min EMA)
            vol_60: 60-second volatility
            ret_10: 10-second return
            delta: MID - BASELINE
            tts: Time to settlement
            **kwargs: Additional context
        """
        self.update_counter += 1

        if self.update_counter % self.update_sample_rate == 0:
            event_dict = {
                "timestamp": get_timestamp(),
                "timestamp_iso": format_timestamp(get_timestamp()),
                "event": "market_update",
                "mid": mid,
                "baseline": baseline,
                "vol_60": vol_60,
                "ret_10": ret_10,
                "delta": delta,
                "tts": tts,
                **kwargs
            }
            self.signals_log.write(event_dict)

    def log_shock(
        self,
        shock_direction: str,
        shock_th: float,
        ret_10: float,
        delta: Optional[float] = None,
        delta_th: Optional[float] = None,
        entry_attempted: bool = False,
        entry_reason: Optional[str] = None,
        vol_60: Optional[float] = None
    ):
        """
        Log shock detection.

        Logs EVERY shock detection for analysis.

        Args:
            shock_direction: "UP" or "DOWN"
            shock_th: Shock threshold used
            ret_10: 10-second return that triggered shock
            delta: Current DELTA
            delta_th: Delta threshold
            entry_attempted: Whether entry was attempted
            entry_reason: Reason for entry/no-entry decision
            vol_60: Current volatility
        """
        event_dict = {
            "timestamp": get_timestamp(),
            "timestamp_iso": format_timestamp(get_timestamp()),
            "event": "shock_detected",
            "shock_direction": shock_direction,
            "shock_th": shock_th,
            "ret_10": ret_10,
            "delta": delta,
            "delta_th": delta_th,
            "entry_attempted": entry_attempted,
            "entry_reason": entry_reason,
            "vol_60": vol_60
        }
        self.shocks_log.write(event_dict)
        self.logger.info("shock_detected", **event_dict)

    def log_trade_entry(
        self,
        side: str,
        price: float,
        size: int,
        signals_dict: dict[str, Any]
    ):
        """
        Log trade entry with full signal context.

        Args:
            side: "yes" or "no"
            price: Entry price
            size: Position size
            signals_dict: All signals at entry time
        """
        event_dict = {
            "timestamp": get_timestamp(),
            "timestamp_iso": format_timestamp(get_timestamp()),
            "event": "trade_entry",
            "side": side,
            "entry_price": price,
            "size": size,
            "signals": signals_dict
        }
        self.trades_log.write(event_dict)
        self.logger.info("trade_entry", side=side, price=price, size=size)

    def log_trade_exit(
        self,
        side: str,
        entry_price: float,
        exit_price: float,
        reason: str,
        pnl: Optional[float],
        hold_time: Optional[float],
        **kwargs
    ):
        """
        Log trade exit with P&L.

        Args:
            side: "yes" or "no"
            entry_price: Entry price
            exit_price: Exit price
            reason: Exit reason (reversion, time_stop, etc.)
            pnl: Realized P&L
            hold_time: Hold time in seconds
            **kwargs: Additional context
        """
        event_dict = {
            "timestamp": get_timestamp(),
            "timestamp_iso": format_timestamp(get_timestamp()),
            "event": "trade_exit",
            "side": side,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "reason": reason,
            "pnl": pnl,
            "hold_time": hold_time,
            **kwargs
        }
        self.trades_log.write(event_dict)
        self.logger.info("trade_exit", reason=reason, pnl=pnl, hold_time=hold_time)

    def log_state_transition(self, old_state: str, new_state: str, reason: str):
        """
        Log state machine transition.

        Args:
            old_state: Previous state
            new_state: New state
            reason: Reason for transition
        """
        self.logger.info(
            "state_transition",
            old_state=old_state,
            new_state=new_state,
            reason=reason
        )

    def log_error(
        self,
        error_type: str,
        message: str,
        context: Optional[dict[str, Any]] = None,
        exc_info: bool = False
    ):
        """
        Log error event.

        Args:
            error_type: Type/category of error
            message: Error message
            context: Additional context dict
            exc_info: Whether to include exception info
        """
        event_dict = {
            "timestamp": get_timestamp(),
            "timestamp_iso": format_timestamp(get_timestamp()),
            "event": "error",
            "error_type": error_type,
            "message": message,
            "context": context or {}
        }
        self.errors_log.write(event_dict)

        if exc_info:
            self.logger.error(error_type, message=message, context=context, exc_info=True)
        else:
            self.logger.error(error_type, message=message, context=context)

    def close(self):
        """Close all log files."""
        self.trades_log.close()
        self.shocks_log.close()
        self.signals_log.close()
        self.errors_log.close()
        self.log_info("logger_closed")
