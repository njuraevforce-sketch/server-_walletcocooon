from typing import Optional
from pydantic import BaseModel, Field

class SettingsUpdate(BaseModel):
    symbol: Optional[str] = None
    timeframe: Optional[str] = None
    paper_mode: Optional[bool] = None
    leverage: Optional[float] = Field(None, ge=1, le=20)
    account_equity: Optional[float] = Field(None, gt=0)
    risk_per_trade: Optional[float] = Field(None, gt=0, le=0.03)
    max_daily_loss_pct: Optional[float] = Field(None, gt=0, le=0.1)
    max_trades_per_day: Optional[int] = Field(None, ge=1, le=50)
    cooldown_minutes: Optional[int] = Field(None, ge=0, le=1440)
    min_volume_ratio: Optional[float] = Field(None, ge=0.5, le=10)
    max_spread_pct: Optional[float] = Field(None, gt=0, le=0.01)
    min_rr: Optional[float] = Field(None, ge=1, le=5)
    sl_atr_mult: Optional[float] = Field(None, ge=0.2, le=5)
    tp_atr_mult: Optional[float] = Field(None, ge=0.2, le=10)
    require_retest: Optional[bool] = None
    allow_long: Optional[bool] = None
    allow_short: Optional[bool] = None
