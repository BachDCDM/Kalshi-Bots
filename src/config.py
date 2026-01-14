"""
Configuration module for Kalshi trading bot.

Loads configuration from environment variables using Pydantic Settings.
Includes all strategy parameters with defaults from the spec.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator


class Config(BaseSettings):
    """Bot configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    # Kalshi API Configuration
    KALSHI_API_KEY: str = Field(..., description="Kalshi API key")
    KALSHI_API_SECRET: str = Field(..., description="Kalshi API secret (RSA private key)")
    KALSHI_API_BASE_URL: str = Field(
        default="https://trading-api.kalshi.com/trade-api/v2",
        description="Kalshi REST API base URL"
    )
    KALSHI_WS_URL: str = Field(
        default="wss://trading-api.kalshi.com/trade-api/ws/v2",
        description="Kalshi WebSocket URL"
    )

    # Target Market
    TARGET_MARKET_TICKER: str = Field(..., description="Market ticker to trade")

    # Position Limits
    MAX_POSITION_SIZE: int = Field(default=10, description="Maximum total position size in contracts")
    MAX_CONTRACTS_PER_TRADE: int = Field(default=5, description="Maximum contracts per single trade")

    # Logging Configuration
    LOG_LEVEL: str = Field(default="INFO", description="Logging level")
    LOG_DIR: str = Field(default="logs", description="Directory for log files")

    # Strategy Parameters (with defaults from spec)
    UPDATE_CADENCE: float = Field(default=1.0, description="Signal computation frequency in seconds")
    BASELINE_WINDOW: int = Field(default=120, description="EMA window for baseline in seconds")
    VOLATILITY_WINDOW: int = Field(default=60, description="Window for VOL_60 calculation in seconds")
    SHOCK_WINDOW: int = Field(default=10, description="Window for detecting fast moves in seconds")

    MIN_SHOCK: float = Field(default=0.06, description="Floor for shock threshold (6 cents)")
    SHOCK_MULTIPLIER: float = Field(default=3.0, description="Shock threshold = SHOCK_MULTIPLIER × VOL_60")

    MIN_DEVIATION: float = Field(default=0.04, description="Floor for deviation threshold (4 cents)")
    DEVIATION_MULTIPLIER: float = Field(default=2.0, description="Deviation threshold = DEVIATION_MULTIPLIER × VOL_60")

    ENTRY_FILL_TIMEOUT: float = Field(default=5.0, description="Cancel unfilled entry orders after N seconds")
    EXIT_BAND_FLOOR: float = Field(default=0.015, description="Reversion exit threshold floor (1.5 cents)")

    MAX_HOLD_TIME: int = Field(default=180, description="Time stop for positions in seconds")
    REPEAT_SHOCK_WINDOW: int = Field(default=30, description="Exit on adverse shock within N seconds")
    COOLDOWN_DURATION: int = Field(default=60, description="Pause after closing trade in seconds")

    MIN_TTS: int = Field(default=180, description="No entries in last N seconds before market close")
    DATA_STALE_THRESHOLD: float = Field(default=5.0, description="Flatten if no updates for N seconds")

    # Validators
    @field_validator("MAX_POSITION_SIZE", "MAX_CONTRACTS_PER_TRADE")
    @classmethod
    def validate_positive_int(cls, v: int, info) -> int:
        """Ensure position limits are positive."""
        if v <= 0:
            raise ValueError(f"{info.field_name} must be positive, got {v}")
        return v

    @field_validator(
        "UPDATE_CADENCE", "MIN_SHOCK", "SHOCK_MULTIPLIER",
        "MIN_DEVIATION", "DEVIATION_MULTIPLIER", "ENTRY_FILL_TIMEOUT",
        "EXIT_BAND_FLOOR", "DATA_STALE_THRESHOLD"
    )
    @classmethod
    def validate_positive_float(cls, v: float, info) -> float:
        """Ensure float parameters are positive."""
        if v <= 0:
            raise ValueError(f"{info.field_name} must be positive, got {v}")
        return v

    @field_validator(
        "BASELINE_WINDOW", "VOLATILITY_WINDOW", "SHOCK_WINDOW",
        "MAX_HOLD_TIME", "REPEAT_SHOCK_WINDOW", "COOLDOWN_DURATION", "MIN_TTS"
    )
    @classmethod
    def validate_positive_time(cls, v: int, info) -> int:
        """Ensure time parameters are positive."""
        if v <= 0:
            raise ValueError(f"{info.field_name} must be positive, got {v}")
        return v

    @field_validator("MIN_SHOCK", "MIN_DEVIATION", "EXIT_BAND_FLOOR")
    @classmethod
    def validate_price_range(cls, v: float, info) -> float:
        """Ensure price thresholds are in valid range [0, 1]."""
        if not (0 <= v <= 1):
            raise ValueError(f"{info.field_name} must be in [0, 1], got {v}")
        return v

    # Computed properties
    @property
    def ema_alpha(self) -> float:
        """Calculate EMA alpha from baseline window: alpha = 2 / (span + 1)."""
        return 2.0 / (self.BASELINE_WINDOW + 1)

    def model_post_init(self, __context) -> None:
        """Post-initialization validation."""
        # Ensure MAX_CONTRACTS_PER_TRADE doesn't exceed MAX_POSITION_SIZE
        if self.MAX_CONTRACTS_PER_TRADE > self.MAX_POSITION_SIZE:
            raise ValueError(
                f"MAX_CONTRACTS_PER_TRADE ({self.MAX_CONTRACTS_PER_TRADE}) "
                f"cannot exceed MAX_POSITION_SIZE ({self.MAX_POSITION_SIZE})"
            )
