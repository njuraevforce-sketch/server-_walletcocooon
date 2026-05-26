from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Tuple, Any

from models import BotSettings


@dataclass
class RiskDecision:
    allowed: bool
    reason: str
    amount: float = 0.0
    notional: float = 0.0
    risk_usd: float = 0.0
    stop_distance: float = 0.0


def live_trading_allowed(settings: BotSettings) -> Tuple[bool, str]:
    if not settings.live_mode:
        return False, "settings.live_mode=false"
    if os.environ.get("LIVE_TRADING_UNLOCK", "false").lower() != "true":
        return False, "LIVE_TRADING_UNLOCK=false"
    if os.environ.get("EXCHANGE_STOPS_VERIFIED", "false").lower() != "true":
        return False, "EXCHANGE_STOPS_VERIFIED=false"
    return True, "live enabled"


def compute_stop_distance(settings: BotSettings, atr14: float, pre_range: float) -> float:
    raw = max(settings.min_stop_usd, atr14 * settings.stop_atr_mult, pre_range * 0.55)
    return min(raw, settings.max_stop_usd)


def compute_order_size(settings: BotSettings, price: float, stop_distance: float, live_balance_usd: float = 0.0, score: float = 0.0) -> RiskDecision:
    equity = live_balance_usd if live_balance_usd and live_balance_usd > 0 else settings.account_equity_usd
    
    base_risk_pct = settings.risk_per_event_pct
    risk_multiplier = 1.0

    # Асимметричный риск-менеджмент: масштабируем объем от качества сетапа
    if settings.dynamic_risk_sizing and score > 0:
        if score >= 90.0:
            risk_multiplier = 2.0  # Идеальный сетап: берем двойной риск
        elif score >= 85.0:
            risk_multiplier = 1.5  # Отличный сетап: берем полуторный риск
        elif score < settings.auto_arm_score:
            risk_multiplier = 0.5  # Сомнительный сетап: режем риск вдвое

    actual_risk_pct = base_risk_pct * risk_multiplier
    risk_usd = equity * actual_risk_pct

    if stop_distance <= 0:
        return RiskDecision(False, "stop distance is zero or negative")

    amount = risk_usd / stop_distance
    notional = amount * price

    if notional > settings.max_notional_usd:
        amount = settings.max_notional_usd / price
        notional = amount * price
        risk_usd = amount * stop_distance

    if amount <= 0:
        return RiskDecision(False, "calculated amount is zero")

    return RiskDecision(
        allowed=True,
        reason=f"risk OK (mult={risk_multiplier:.1f}x)",
        amount=amount,
        notional=notional,
        risk_usd=risk_usd,
        stop_distance=stop_distance,
    )


def validate_market_for_event(settings: BotSettings, metrics: Dict[str, Any], spread_bps: float) -> Tuple[bool, str]:
    if spread_bps > settings.max_spread_bps:
        return False, f"spread too wide: {spread_bps:.2f}bps"
    if metrics["range"] < settings.min_pre_range_usd:
        return False, f"pre-range too small: {metrics['range']:.2f}"
    if metrics["range"] > settings.max_pre_range_usd:
        return False, f"pre-range too wide: {metrics['range']:.2f}"
    if metrics["volume_spike"] < settings.min_volume_spike_ratio:
        return False, f"volume spike too weak: {metrics['volume_spike']:.2f}"
    if metrics["atr_expansion"] < settings.min_atr_expansion_ratio:
        return False, f"ATR expansion too weak: {metrics['atr_expansion']:.2f}"
    if metrics.get("last_body_atr", 0) > settings.max_chase_candle_atr:
        return False, f"last candle already too extended: {metrics.get('last_body_atr', 0):.2f} ATR"
    if metrics.get("last_wick_ratio", 0) > settings.max_wick_ratio:
        return False, f"too many wicks / fakeout risk: {metrics.get('last_wick_ratio', 0):.2f}"
        
    # --- КВАНТ-ФИЛЬТРЫ ---
    adx_val = float(metrics.get("adx14") or 0.0)
    if adx_val > 0 and adx_val < settings.min_trend_adx:
        return False, f"market is flat (ADX {adx_val:.1f} < {settings.min_trend_adx})"

    return True, "market accepted"


def daily_limits_ok(settings: BotSettings, today_pnl: float, today_trades: int, consecutive_losses: int) -> Tuple[bool, str]:
    equity = settings.account_equity_usd
    if today_trades >= settings.max_trades_per_day:
        return False, "max trades per day reached"
    if consecutive_losses >= settings.max_consecutive_losses:
        return False, "max consecutive losses reached"
    if today_pnl < -(equity * settings.max_daily_loss_pct):
        return False, f"daily loss limit reached: {today_pnl:.2f}"
    if today_pnl >= (equity * settings.daily_profit_lock_pct):
        return False, f"daily profit lock reached: {today_pnl:.2f}"
    return True, "limits ok"
