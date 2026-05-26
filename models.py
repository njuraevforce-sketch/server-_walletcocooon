from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class BotMode(str, Enum):
    OFF = "off"
    HYBRID_SCAN = "hybrid_scan"
    CALENDAR_ARMED = "calendar_armed"
    VOLATILITY_SCAN = "volatility_scan"
    VOLATILITY_ARMED = "volatility_armed"
    MANUAL_ARMED = "manual_armed"
    IN_TRADE = "in_trade"
    PAUSED = "paused"


class EventImpact(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class NewsEvent(BaseModel):
    provider_id: str
    provider: str = "manual"
    title: str
    country: str = "US"
    currency: str = "USD"
    impact: EventImpact = EventImpact.HIGH
    event_time_utc: datetime
    previous: Optional[str] = None
    estimate: Optional[str] = None
    actual: Optional[str] = None
    raw: Dict[str, Any] = Field(default_factory=dict)


class BotSettings(BaseModel):
    # Market
    symbol: str = "BTCUSDT"
    timeframe: str = "1m"
    leverage: float = 2.0
    isolated_margin: bool = True
    hedge_mode: bool = False
    live_mode: bool = False

    # Engine modes
    calendar_enabled: bool = True
    volatility_auto_enabled: bool = True
    manual_arm_enabled: bool = True
    high_impact_only: bool = True
    
    # Quant / Order Flow Filters (НОВЫЕ ПАРАМЕТРЫ)
    min_trend_adx: float = 20.0
    dynamic_risk_sizing: bool = True
    max_imbalance_against: float = 0.35

    # Multi-pair / Scanner
    scan_symbols: List[str] = Field(default_factory=lambda: ["BTC/USDT", "ETH/USDT", "SOL/USDT"])
    max_symbols_per_scan: int = 8
    multi_scan_concurrency: int = 3
    trade_selected_symbol: bool = True

    # Connections
    ws_enabled: bool = True
    ws_public_enabled: bool = True
    ws_private_enabled: bool = True
    ws_reconnect_seconds: float = 3.0
    rest_fallback_enabled: bool = True
    rest_fallback_sync_seconds: float = 7.0

    # Risk Management
    account_equity_usd: float = 1000.0
    risk_per_event_pct: float = 0.0025
    max_daily_loss_pct: float = 0.01
    max_event_loss_pct: float = 0.004
    daily_profit_lock_pct: float = 0.025
    max_notional_usd: float = 500.0
    max_trades_per_day: int = 100
    max_consecutive_losses: int = 3

    # Timing
    event_cooldown_minutes: int = 25
    volatility_cooldown_minutes: int = 18
    pre_arm_seconds: int = 180
    post_event_wait_seconds: int = 90

    # Filtering
    allowed_countries: List[str] = Field(default_factory=lambda: ["US"])
    allowed_keywords: List[str] = Field(default_factory=lambda: [
        "CPI", "Core CPI", "PPI", "Core PPI", "PCE", "Core PCE",
        "FOMC", "Fed Interest Rate Decision", "Federal Funds Rate", "Fed Chair Powell",
        "Non Farm Payrolls", "Nonfarm Payrolls", "NFP",
        "Unemployment Rate", "Initial Jobless Claims",
        "GDP", "Retail Sales", "ISM", "PMI", "JOLTS"
    ])

    # Volatility Scanner
    scan_interval_seconds: float = 2.0
    auto_arm_score: float = 88.0
    notify_score: float = 70.0
    auto_arm_delay_seconds: int = 5
    auto_post_wait_seconds: int = 75
    volatility_lookback_minutes: int = 8
    compression_lookback_minutes: int = 24
    min_range_expansion_ratio: float = 1.10
    max_chase_candle_atr: float = 1.80
    max_wick_ratio: float = 0.62
    range_lookback_minutes: int = 8
    min_pre_range_usd: float = 80.0
    max_pre_range_usd: float = 950.0
    entry_buffer_atr: float = 0.18
    min_entry_buffer_usd: float = 35.0
    max_spread_bps: float = 8.0
    min_volume_spike_ratio: float = 1.30
    min_atr_expansion_ratio: float = 1.18

    # Exit logic in R multiples
    stop_atr_mult: float = 1.05
    min_stop_usd: float = 120.0
    max_stop_usd: float = 850.0
    tp1_enabled: bool = True
    tp1_r: float = 1.05
    tp1_close_pct: float = 0.25
    tp2_enabled: bool = False
    tp2_r: float = 2.80
    breakeven_after_r: float = 0.75
    
    # Trailing Stop
    trailing_mode: bool = True
    exchange_trailing_sl_enabled: bool = True
    trailing_start_r: float = 1.15
    trailing_atr_mult: float = 0.85
    trailing_update_interval_seconds: float = 0.7
    trailing_min_step_atr: float = 0.15
    trailing_min_step_usd: float = 8.0
    fee_buffer_bps: float = 12.0

    hard_timeout_seconds: int = 540
    stale_trade_exit_seconds: int = 100
    stale_trade_min_r: float = 0.20

    # Operational safety
    poll_interval_seconds: float = 0.75
    order_watch_interval_seconds: float = 0.35
    emergency_flatten_on_error: bool = True

    # V7/V8 live guard: real money protection rules
    hard_exchange_sl_required: bool = True
    flatten_if_exchange_sl_fails: bool = True
    cancel_tp_if_sl_fails: bool = True
    kill_switch_closes_positions: bool = True
    max_entry_slippage_bps: float = 35.0
    double_fill_emergency_flatten: bool = True


# ---- PAYLOADS ДЛЯ FASTAPI (ТЕПЕРЬ НА МЕСТЕ) ----

class SettingsPayload(BaseModel):
    settings: Optional[Dict[str, Any]] = None

class ManualEventPayload(BaseModel):
    provider_id: str

class ManualArmNowPayload(BaseModel):
    arm_delay_seconds: int = 3
    post_wait_seconds: int = 75
    note: str = ""
