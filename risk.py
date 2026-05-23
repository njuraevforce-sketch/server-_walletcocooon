from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Tuple

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


def compute_order_size(settings: BotSettings, price: float, stop_distance: float, live_balance_usd: float = 0.0) -> RiskDecision:
    equity = live_balance_usd if live_balance_usd and live_balance_usd > 0 else settings.account_equity_usd
    if equity <= 0 or price <= 0 or stop_distance <= 0:
        return RiskDecision(False, "bad equity/price/stop")

    risk_usd = equity * settings.risk_per_event_pct
    max_event_loss = equity * settings.max_event_loss_pct
    risk_usd = min(risk_usd, max_event_loss)
    raw_amount = risk_usd / stop_distance
    max_amount_by_notional = settings.max_notional_usd / price
    amount = min(raw_amount, max_amount_by_notional)
    notional = amount * price

    if amount <= 0:
        return RiskDecision(False, "calculated amount is zero")
    if notional < 5:
        return RiskDecision(False, "notional below practical minimum")
    return RiskDecision(True, "ok", amount=amount, notional=notional, risk_usd=risk_usd, stop_distance=stop_distance)


def validate_market_for_event(settings: BotSettings, metrics: Dict[str, float], spread_bps: float) -> Tuple[bool, str]:
    if spread_bps > settings.max_spread_bps:
        return False, f"spread too wide: {spread_bps:.2f} bps"
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
    return True, "market accepted"


def daily_limits_ok(settings: BotSettings, today_pnl: float, today_trades: int, consecutive_losses: int) -> Tuple[bool, str]:
    equity = settings.account_equity_usd
    if today_trades >= settings.max_trades_per_day:
        return False, "max trades per day reached"
    if consecutive_losses >= settings.max_consecutive_losses:
        return False, "max consecutive losses reached"
    if today_pnl <= -(equity * settings.max_daily_loss_pct):
        return False, "max daily loss reached"
    if today_pnl >= equity * settings.daily_profit_lock_pct:
        return False, "daily profit target reached; locking gains"
    return True, "limits ok"
