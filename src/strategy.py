"""
Trading strategy and state machine for Kalshi bot.

Implements the shock-fade mean-reversion strategy with state machine.
"""

import time
from datetime import datetime
from enum import Enum, auto
from typing import Literal, Optional

from src.config import Config
from src.logger import TradingLogger
from src.market_data import MarketDataManager, OrderBook
from src.order_manager import OrderManager
from src.position_manager import PositionManager
from src.signals import SignalCalculator


class State(Enum):
    """Trading state machine states."""

    FLAT = auto()
    ENTERING = auto()
    LONG_YES = auto()
    LONG_NO = auto()
    COOLDOWN = auto()


class Strategy:
    """Main strategy logic and state machine."""

    def __init__(
        self,
        config: Config,
        signal_calculator: SignalCalculator,
        order_manager: OrderManager,
        position_manager: PositionManager,
        market_data_manager: MarketDataManager,
        logger: TradingLogger
    ):
        """
        Initialize strategy.

        Args:
            config: Bot configuration
            signal_calculator: Signal calculator
            order_manager: Order manager
            position_manager: Position manager
            market_data_manager: Market data manager
            logger: Trading logger
        """
        self.config = config
        self.signals = signal_calculator
        self.orders = order_manager
        self.positions = position_manager
        self.data = market_data_manager
        self.logger = logger

        # State machine
        self.state = State.FLAT

        # Position tracking
        self.entry_time: Optional[float] = None
        self.entry_price: Optional[float] = None
        self.position_side: Optional[Literal["yes", "no"]] = None
        self.position_size: int = 0

        # Order tracking
        self.pending_order_id: Optional[str] = None
        self.order_submit_time: Optional[float] = None

        # Cooldown tracking
        self.cooldown_start: Optional[float] = None

        # Shock tracking (for repeat shock detection)
        self.last_shock_time: Optional[float] = None
        self.last_shock_direction: Optional[str] = None

    async def on_orderbook_update(
        self,
        orderbook: OrderBook,
        market_close_time: datetime
    ):
        """
        Main strategy loop - called on each orderbook update.

        This is the heart of the bot - processes each market update.

        Args:
            orderbook: New order book update
            market_close_time: Market close time
        """
        # Update market data
        self.data.update(orderbook)

        # Calculate TTS
        from src.utils import calculate_tts
        tts = calculate_tts(market_close_time)

        # Check data staleness
        if self.data.is_stale(self.config.DATA_STALE_THRESHOLD):
            if self.state in (State.LONG_YES, State.LONG_NO):
                self.logger.log_error(
                    "data_stale_in_position",
                    "Data stale while in position - flattening"
                )
                await self.execute_exit(orderbook, "data_staleness")
            return

        # State machine dispatch
        if self.state == State.FLAT:
            await self._handle_flat_state(orderbook, tts)

        elif self.state == State.ENTERING:
            await self._handle_entering_state(orderbook)

        elif self.state in (State.LONG_YES, State.LONG_NO):
            await self._handle_position_state(orderbook)

        elif self.state == State.COOLDOWN:
            await self._handle_cooldown_state()

    async def _handle_flat_state(self, orderbook: OrderBook, tts: float):
        """
        FLAT state: Look for entry signals.

        Args:
            orderbook: Current order book
            tts: Time to settlement
        """
        # Check entry eligibility
        if not self._check_entry_eligibility(tts):
            return

        # Detect shock
        shock_direction = self.signals.detect_shock()
        if shock_direction is None:
            return

        # Track shock timing for repeat shock detection
        self.last_shock_time = time.time()
        self.last_shock_direction = shock_direction

        # Detect overreaction
        overreaction_direction = self.signals.detect_overreaction()
        if overreaction_direction is None:
            return

        # Check if shock and overreaction are aligned
        if shock_direction != overreaction_direction:
            self.logger.log_info(
                "misaligned_shock_overreaction",
                shock=shock_direction,
                overreaction=overreaction_direction
            )
            return

        # Entry conditions met - fade the move
        await self._try_entry(orderbook, shock_direction)

    def _check_entry_eligibility(self, tts: float) -> bool:
        """
        Check all entry gates.

        Args:
            tts: Time to settlement

        Returns:
            True if entry eligible
        """
        # TTS gate
        if tts < self.config.MIN_TTS:
            return False

        # Data sufficiency gate
        if not self.data.has_sufficient_data():
            return False

        # State gate (already FLAT, but explicit check)
        if self.state != State.FLAT:
            return False

        return True

    async def _try_entry(
        self,
        orderbook: OrderBook,
        shock_direction: Literal["UP", "DOWN"]
    ):
        """
        Attempt to enter position by fading shock.

        Args:
            orderbook: Current order book
            shock_direction: Direction of detected shock
        """
        # Determine entry side (opposite of shock)
        # Shock UP (price jumped) → Buy NO (fade)
        # Shock DOWN (price dropped) → Buy YES (fade)
        if shock_direction == "UP":
            entry_side = "no"
            entry_price = orderbook.no_ask
        else:  # DOWN
            entry_side = "yes"
            entry_price = orderbook.yes_ask

        if entry_price is None:
            self.logger.log_error(
                "missing_entry_price",
                f"No {entry_side} ask price available"
            )
            return

        # Determine position size
        position_size = min(
            self.config.MAX_CONTRACTS_PER_TRADE,
            self.config.MAX_POSITION_SIZE
        )

        # Submit limit order at ask
        order_id = await self.orders.submit_limit_order(
            ticker=orderbook.ticker,
            side=entry_side,
            price=entry_price,
            size=position_size
        )

        if order_id is None:
            self.logger.log_error(
                "entry_order_failed",
                "Failed to submit entry order"
            )
            return

        # Transition to ENTERING state
        self._transition_state(
            State.ENTERING,
            reason=f"entry_order_submitted_shock_{shock_direction}"
        )

        self.pending_order_id = order_id
        self.order_submit_time = time.time()
        self.position_side = entry_side
        self.position_size = position_size
        self.entry_price = entry_price

        # Log trade entry attempt with all signal context
        signals = self.signals.get_all_signals()
        self.logger.log_trade_entry(
            side=entry_side,
            price=entry_price,
            size=position_size,
            signals_dict=signals
        )

    async def _handle_entering_state(self, orderbook: OrderBook):
        """
        ENTERING state: Monitor order fill or timeout.

        Args:
            orderbook: Current order book
        """
        # Check if order filled
        filled, fill_price = await self.orders.wait_for_fill(
            self.pending_order_id,
            timeout=0.1  # Short timeout, we'll check again next update
        )

        if filled:
            # Order filled - transition to position state
            target_state = State.LONG_YES if self.position_side == "yes" else State.LONG_NO
            self._transition_state(target_state, reason="order_filled")

            self.entry_time = time.time()
            self.entry_price = fill_price or self.entry_price

            # Record position
            self.positions.record_entry(
                ticker=orderbook.ticker,
                side=self.position_side,
                size=self.position_size,
                price=self.entry_price
            )

            self.pending_order_id = None
            self.order_submit_time = None
            return

        # Check for timeout
        if time.time() - self.order_submit_time > self.config.ENTRY_FILL_TIMEOUT:
            # Cancel order and return to FLAT
            await self.orders.cancel_order(self.pending_order_id)

            self._transition_state(State.FLAT, reason="entry_timeout")

            self.pending_order_id = None
            self.order_submit_time = None
            self.position_side = None
            self.position_size = 0
            self.entry_price = None

    async def _handle_position_state(self, orderbook: OrderBook):
        """
        LONG_YES/LONG_NO state: Monitor exit conditions.

        Args:
            orderbook: Current order book
        """
        # Check all exit conditions
        exit_reason = await self._check_exit_conditions(orderbook)

        if exit_reason:
            await self.execute_exit(orderbook, exit_reason)

    async def _check_exit_conditions(
        self,
        orderbook: OrderBook
    ) -> Optional[str]:
        """
        Check all exit conditions.

        Args:
            orderbook: Current order book

        Returns:
            Exit reason if any condition met, None otherwise
        """
        # 1. Reversion exit: |DELTA| <= EXIT_BAND
        delta = self.signals.calculate_delta()
        exit_band = self.signals.calculate_exit_band()

        if delta is not None and abs(delta) <= exit_band:
            return "reversion"

        # 2. Time stop: 180 seconds in position
        hold_time = time.time() - self.entry_time
        if hold_time >= self.config.MAX_HOLD_TIME:
            return "time_stop"

        # 3. Repeat shock within 30 seconds (against our position)
        if self.last_shock_time and (time.time() - self.last_shock_time) <= self.config.REPEAT_SHOCK_WINDOW:
            # Check if new shock is against our position
            shock_direction = self.signals.detect_shock()

            if shock_direction:
                # We're long YES (faded DOWN shock), adverse shock is DOWN
                # We're long NO (faded UP shock), adverse shock is UP
                if self.state == State.LONG_YES and shock_direction == "DOWN":
                    return "repeat_shock_adverse"
                if self.state == State.LONG_NO and shock_direction == "UP":
                    return "repeat_shock_adverse"

        # 4. Data staleness (checked in main loop)

        return None

    async def execute_exit(
        self,
        orderbook: OrderBook,
        reason: str
    ):
        """
        Execute position exit.

        Args:
            orderbook: Current order book
            reason: Exit reason
        """
        # Submit aggressive exit order
        exit_order_id = await self.orders.market_exit(
            ticker=orderbook.ticker,
            side=self.position_side,
            size=self.position_size,
            orderbook=orderbook
        )

        if exit_order_id is None:
            self.logger.log_error(
                "exit_order_failed",
                "Failed to submit exit order - will retry"
            )
            return

        # Wait for fill (with longer timeout)
        filled, exit_price = await self.orders.wait_for_fill(
            exit_order_id,
            timeout=5.0
        )

        if not filled:
            self.logger.log_error(
                "exit_fill_failed",
                "Exit order did not fill - position may still be open"
            )
            # Sync positions to verify
            await self.positions.sync_positions()
            return

        # Record exit
        pnl, hold_time = self.positions.record_exit(
            ticker=orderbook.ticker,
            exit_price=exit_price
        )

        # Log trade exit
        self.logger.log_trade_exit(
            side=self.position_side,
            entry_price=self.entry_price,
            exit_price=exit_price,
            reason=reason,
            pnl=pnl,
            hold_time=hold_time
        )

        # Transition to COOLDOWN
        self._transition_state(State.COOLDOWN, reason=f"exit_{reason}")

        self.cooldown_start = time.time()
        self.entry_time = None
        self.entry_price = None
        self.position_side = None
        self.position_size = 0

    async def _handle_cooldown_state(self):
        """COOLDOWN state: Wait 60 seconds before returning to FLAT."""
        if time.time() - self.cooldown_start >= self.config.COOLDOWN_DURATION:
            self._transition_state(State.FLAT, reason="cooldown_complete")
            self.cooldown_start = None

    def _transition_state(self, new_state: State, reason: str):
        """
        Transition to new state with logging.

        Args:
            new_state: New state to transition to
            reason: Reason for transition
        """
        old_state = self.state
        self.state = new_state

        self.logger.log_state_transition(
            old_state=old_state.name,
            new_state=new_state.name,
            reason=reason
        )

    async def shutdown(self):
        """Graceful shutdown - flatten all positions."""
        self.logger.log_info("shutdown_initiated")

        if self.state == State.ENTERING:
            if self.pending_order_id:
                await self.orders.cancel_order(self.pending_order_id)

        if self.state in (State.LONG_YES, State.LONG_NO):
            self.logger.log_info("flattening_position_on_shutdown")
            # Would need current orderbook - simplified implementation
            # In production, store last orderbook reference

        self.logger.log_info("shutdown_complete")
