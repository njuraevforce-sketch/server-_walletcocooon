from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import database as db
from calendar_client import fetch_calendar, filter_events_for_crypto
from exchange_client import (
    ccxt_symbol,
    cancel_all_safely,
    cancel_safely,
    close_position_market,
    configure_symbol,
    fetch_balance_usdt,
    flatten_symbol_positions,
    fetch_ohlcv,
    get_exchange,
    get_last_price,
    get_spread_bps,
    order_amount_precision,
    place_reduce_trigger,
    place_trigger_entry,
    price_precision,
    extract_order_id,
    fetch_symbol_position,
)
from indicators import pre_event_metrics, volatility_metrics, score_volatility, clamp, orderbook_imbalance
from models import BotMode, BotSettings, NewsEvent, EventImpact, ManualArmNowPayload
from risk import compute_order_size, compute_stop_distance, live_trading_allowed, validate_market_for_event, daily_limits_ok
import bitget_ws

engine_task: Optional[asyncio.Task] = None
engine_stop = asyncio.Event()
_last_vol_arm_at: Optional[datetime] = None
_last_shock_arm_at: Optional[datetime] = None
_last_hot_log_at: Optional[datetime] = None

VOLATILITY_PROVIDERS = {"volatility_scanner", "manual_volatility", "volume_shock"}

VOLUME_SHOCK_DEFAULTS: Dict[str, Any] = {
    "volume_shock_enabled": True,
    "volume_shock_min_score": 75.0,
    "volume_shock_min_volume_spike": 3.0,
    "volume_shock_min_body_atr": 0.85,
    "volume_shock_min_range_expansion": 0.70,
    "volume_shock_max_wick_ratio": 0.45,
    "volume_shock_max_spread_bps": 8.0,
    "volume_shock_min_stop_bps": 12.0,
    "volume_shock_max_stop_bps": 120.0,
    "volume_shock_stop_atr_mult": 1.05,
    "volume_shock_min_entry_buffer_bps": 3.0,
    "volume_shock_max_entry_buffer_bps": 35.0,
    "volume_shock_entry_buffer_atr": 0.18,
    "volume_shock_lookback_minutes": 3,
    "volume_shock_order_life_seconds": 35,
    "volume_shock_cooldown_minutes": 10,
}


SYMBOL_PROFILES: Dict[str, Dict[str, Any]] = {
    "BTC": {
        "max_chase_candle_atr": 1.95,
    },
    "ETH": {
        "min_pre_range_usd": 3.0,
        "max_pre_range_usd": 90.0,
        "min_entry_buffer_usd": 0.70,
        "min_stop_usd": 4.0,
        "max_stop_usd": 55.0,
        "trailing_min_step_usd": 0.40,
        "max_spread_bps": 12.0,
        "volume_shock_min_score": 80.0,
        "volume_shock_min_volume_spike": 2.2,
        "volume_shock_min_body_atr": 0.70,
        "volume_shock_min_range_expansion": 0.65,
        "volume_shock_max_wick_ratio": 0.50,
        "volume_shock_min_stop_bps": 18.0,
        "volume_shock_max_stop_bps": 260.0,
        "volume_shock_entry_buffer_atr": 0.12,
        "volume_shock_min_entry_buffer_bps": 1.5,
        "volume_shock_max_entry_buffer_bps": 25.0,
        "volume_shock_order_life_seconds": 25,
        "volume_shock_cooldown_minutes": 10,
    },
    "SOL": {
        "min_pre_range_usd": 0.25,
        "max_pre_range_usd": 8.0,
        "min_entry_buffer_usd": 0.05,
        "min_stop_usd": 0.35,
        "max_stop_usd": 4.0,
        "trailing_min_step_usd": 0.05,
        "max_spread_bps": 18.0,
        "volume_shock_min_score": 80.0,
        "volume_shock_min_volume_spike": 2.0,
        "volume_shock_min_body_atr": 0.70,
        "volume_shock_min_range_expansion": 0.65,
        "volume_shock_max_wick_ratio": 0.50,
        "volume_shock_min_stop_bps": 35.0,
        "volume_shock_max_stop_bps": 470.0,
        "volume_shock_entry_buffer_atr": 0.12,
        "volume_shock_min_entry_buffer_bps": 5.0,
        "volume_shock_max_entry_buffer_bps": 35.0,
        "volume_shock_order_life_seconds": 25,
        "volume_shock_cooldown_minutes": 10,
    },
}

# --- NON-BLOCKING DB HELPERS (FAST LOOP) ---
def fire_log(level: str, event_type: str, message: str, data: dict = None):
    asyncio.create_task(asyncio.to_thread(db.log_event, level, event_type, message, data or {}))

def fire_update_trade(oid: str, payload: dict):
    asyncio.create_task(asyncio.to_thread(db.update_trade_by_client_oid, oid, payload))

def fire_state(mode: str, is_active: bool, reason: str):
    asyncio.create_task(asyncio.to_thread(db.set_runtime_state, mode, is_active, reason))
# -------------------------------------------


def base_symbol(symbol: str) -> str:
    s = str(symbol or "BTC").upper().replace(":USDT", "").replace("/", "")
    return s[:-4] if s.endswith("USDT") else s
    

def profile_overrides_for_symbol(symbol: str) -> Dict[str, Any]:
    return dict(SYMBOL_PROFILES.get(base_symbol(symbol), {}))


def with_symbol_profile(settings: BotSettings, symbol: str) -> BotSettings:
    updates: Dict[str, Any] = {"symbol": symbol}
    for key, value in profile_overrides_for_symbol(symbol).items():
        if hasattr(settings, key):
            updates[key] = value
    try:
        return settings.model_copy(update=updates)
    except Exception:
        data = settings.model_dump() if hasattr(settings, "model_dump") else dict(settings)
        data.update(updates)
        return BotSettings(**data)


def sget(settings: BotSettings, name: str, default: Any = None) -> Any:
    if hasattr(settings, name):
        value = getattr(settings, name)
        if value is not None:
            return value
    profile = profile_overrides_for_symbol(getattr(settings, "symbol", "BTC/USDT"))
    if name in profile:
        return profile[name]
    return VOLUME_SHOCK_DEFAULTS.get(name, default)


@dataclass
class ArmedPlan:
    event: NewsEvent
    metrics: Dict[str, float]
    buy_trigger: float
    sell_trigger: float
    stop_distance: float
    amount: float
    risk_usd: float
    notional: float
    buy_order_id: Optional[str] = None
    sell_order_id: Optional[str] = None
    buy_stop_loss: Optional[float] = None
    sell_stop_loss: Optional[float] = None
    buy_client_oid: str = ""
    sell_client_oid: str = ""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def event_from_row(row: Dict[str, Any]) -> NewsEvent:
    data = dict(row)
    if isinstance(data.get("event_time_utc"), str):
        data["event_time_utc"] = datetime.fromisoformat(data["event_time_utc"].replace("Z", "+00:00"))
    allowed = {k: v for k, v in data.items() if k in NewsEvent.model_fields}
    return NewsEvent(**allowed)


def synthetic_event(kind: str, delay_seconds: int, title: str, raw: Optional[Dict[str, Any]] = None) -> NewsEvent:
    ts = utc_now() + timedelta(seconds=max(0, delay_seconds))
    return NewsEvent(
        provider_id=f"{kind}-{uuid.uuid4().hex[:14]}",
        provider=kind,
        title=title,
        country="GLOBAL",
        currency="BTC",
        impact=EventImpact.CRITICAL,
        event_time_utc=ts,
        raw=raw or {},
    )


def score_volume_shock(metrics: Dict[str, float], spread_bps: float, settings: BotSettings) -> Dict[str, Any]:
    vol = float(metrics.get("volume_spike") or 0.0)
    body = float(metrics.get("last_body_atr") or 0.0)
    range_exp = float(metrics.get("range_expansion") or 0.0)
    wick = float(metrics.get("last_wick_ratio") or 1.0)
    direction = float(metrics.get("last_direction") or 0.0)

    vol_score = clamp(vol / max(0.01, sget(settings, "volume_shock_min_volume_spike")) * 32.0, 0, 34)
    body_score = clamp(body / max(0.01, sget(settings, "volume_shock_min_body_atr")) * 20.0, 0, 22)
    range_score = clamp(range_exp / max(0.01, sget(settings, "volume_shock_min_range_expansion")) * 16.0, 0, 18)
    spread_score = clamp((sget(settings, "volume_shock_max_spread_bps") - spread_bps) / max(0.01, sget(settings, "volume_shock_max_spread_bps")) * 14.0, 0, 14)
    wick_score = clamp((sget(settings, "volume_shock_max_wick_ratio") - wick) / max(0.01, sget(settings, "volume_shock_max_wick_ratio")) * 12.0, 0, 12)
    shock_score = round(vol_score + body_score + range_score + spread_score + wick_score, 2)

    reasons: List[str] = []
    if not bool(sget(settings, "volume_shock_enabled")):
        reasons.append("volume shock disabled")
    if spread_bps > sget(settings, "volume_shock_max_spread_bps"):
        reasons.append(f"spread too wide {spread_bps:.2f}bps")
    if vol < sget(settings, "volume_shock_min_volume_spike"):
        reasons.append(f"volume shock weak {vol:.2f}")
    if body < sget(settings, "volume_shock_min_body_atr"):
        reasons.append(f"body impulse weak {body:.2f} ATR")
    if range_exp < sget(settings, "volume_shock_min_range_expansion"):
        reasons.append(f"range impulse weak {range_exp:.2f}")
    if wick > sget(settings, "volume_shock_max_wick_ratio"):
        reasons.append(f"wick/fakeout risk {wick:.2f}")
    if direction == 0:
        reasons.append("no candle direction")
    if shock_score < sget(settings, "volume_shock_min_score"):
        reasons.append(f"shock score too low {shock_score:.1f}")

    valid = not reasons
    return {
        "volume_shock_score": shock_score,
        "volume_shock_state": "SHOCK_ARM" if valid else "SHOCK_WATCH" if shock_score >= settings.notify_score else "NO_SHOCK",
        "volume_shock_valid": bool(valid),
        "volume_shock_should_arm": bool(valid),
        "volume_shock_reason": "; ".join(reasons) if reasons else "volume shock accepted",
        "volume_shock_direction": direction,
        "shock_volume_score": round(vol_score, 2),
        "shock_body_score": round(body_score, 2),
        "shock_range_score": round(range_score, 2),
        "shock_spread_score": round(spread_score, 2),
        "shock_wick_score": round(wick_score, 2),
    }


def validate_volume_shock(settings: BotSettings, metrics: Dict[str, float], spread_bps: float) -> tuple[bool, str]:
    shock = score_volume_shock(metrics, spread_bps, settings)
    return bool(shock.get("volume_shock_valid")), str(shock.get("volume_shock_reason") or "")


def compute_volume_shock_stop_distance(settings: BotSettings, metrics: Dict[str, float], price: float) -> float:
    min_stop = price * sget(settings, "volume_shock_min_stop_bps") / 10000.0
    max_stop = max(min_stop, price * sget(settings, "volume_shock_max_stop_bps") / 10000.0)
    raw = max(
        float(metrics.get("atr14") or 0.0) * sget(settings, "volume_shock_stop_atr_mult"),
        min_stop,
        float(metrics.get("range") or 0.0) * 0.75,
    )
    return min(raw, max_stop)


def compute_volume_shock_entry_buffer(settings: BotSettings, metrics: Dict[str, float], price: float) -> float:
    min_buffer = price * sget(settings, "volume_shock_min_entry_buffer_bps") / 10000.0
    max_buffer = max(min_buffer, price * sget(settings, "volume_shock_max_entry_buffer_bps") / 10000.0)
    raw = max(float(metrics.get("atr14") or 0.0) * sget(settings, "volume_shock_entry_buffer_atr"), min_buffer)
    return min(raw, max_buffer)


async def sync_calendar(settings: BotSettings) -> List[NewsEvent]:
    events = await fetch_calendar(days_ahead=5)
    filtered = filter_events_for_crypto(events, settings)
    for ev in filtered:
        db.upsert_news_event(ev)
    db.log_event("info", "calendar_sync", f"Calendar synced: {len(filtered)} relevant events", {"count": len(filtered)})
    return filtered


async def analyze_market(symbol: str, settings: BotSettings) -> Dict[str, Any]:
    effective = with_symbol_profile(settings, symbol)
    exchange = await get_exchange()
    ohlcv = await fetch_ohlcv(exchange, effective.symbol, effective.timeframe, limit=160)
    spread_bps = await get_spread_bps(exchange, effective.symbol)
    metrics = volatility_metrics(ohlcv, effective.volatility_lookback_minutes, effective.compression_lookback_minutes)
    
    # КВАНТ-ФИЛЬТРЫ: Собираем данные стакана и дельты прямо в сканер
    ob = bitget_ws.get_orderbook(effective.symbol)
    imbalance = orderbook_imbalance(ob) if ob else 0.0
    cvd = bitget_ws.get_cvd(effective.symbol)
    
    metrics["adx14"] = metrics.get("adx14", 0.0) # От индикатора
    event_valid, event_reason = validate_market_for_event(effective, metrics, spread_bps)
    score = score_volatility(metrics, spread_bps, effective)
    shock = score_volume_shock(metrics, spread_bps, effective)
    
    return {
        "valid_for_sniper": bool(event_valid),
        "valid_for_news_sniper": bool(event_valid),
        "event_reason": event_reason,
        "reason": score["reason"],
        "spread_bps": spread_bps,
        "orderbook_imbalance": round(imbalance, 3),
        "cvd": round(cvd, 2),
        "symbol_profile": base_symbol(effective.symbol),
        "effective_min_pre_range_usd": getattr(effective, "min_pre_range_usd", None),
        "effective_min_stop_usd": getattr(effective, "min_stop_usd", None),
        "effective_min_entry_buffer_usd": getattr(effective, "min_entry_buffer_usd", None),
        **metrics,
        **score,
        **shock,
    }


async def analyze_markets(settings: BotSettings) -> Dict[str, Any]:
    raw_symbols = settings.scan_symbols or [settings.symbol]
    symbols = list(dict.fromkeys([s.strip() for s in raw_symbols if str(s).strip()]))[: max(1, int(settings.max_symbols_per_scan))]
    sem = asyncio.Semaphore(max(1, int(settings.multi_scan_concurrency)))

    async def one(sym: str) -> Dict[str, Any]:
        async with sem:
            try:
                market = await analyze_market(sym, settings)
                market["symbol"] = sym
                return market
            except Exception as e:
                return {"symbol": sym, "status": "error", "error": str(e), "volatility_score": 0, "valid_for_sniper": False, "should_arm": False}

    markets = await asyncio.gather(*(one(sym) for sym in symbols))
    markets_sorted = sorted(markets, key=lambda x: float(x.get("volatility_score") or 0), reverse=True)
    shock_sorted = sorted(markets, key=lambda x: float(x.get("volume_shock_score") or 0), reverse=True)
    best = markets_sorted[0] if markets_sorted else {}
    best_shock = shock_sorted[0] if shock_sorted else {}
    return {"symbols": symbols, "best": best, "best_shock": best_shock, "markets": markets_sorted}


async def build_armed_plan(exchange, settings: BotSettings, event: NewsEvent) -> ArmedPlan:
    settings = with_symbol_profile(settings, settings.symbol)
    await configure_symbol(exchange, settings.symbol, settings.leverage, settings.isolated_margin)
    is_shock = event.provider == "volume_shock"

    snapshot_used = False
    snapshot = {}
    if is_shock:
        try:
            raw = event.raw or {}
            candidate = raw.get("market") if isinstance(raw, dict) else None
            if isinstance(candidate, dict) and str(candidate.get("symbol") or settings.symbol) == str(settings.symbol):
                needed = ("high", "low", "last", "atr14", "range", "volume_spike", "range_expansion", "last_body_atr", "last_wick_ratio", "last_direction", "spread_bps")
                if all(candidate.get(k) is not None for k in needed):
                    snapshot = candidate
                    snapshot_used = True
        except Exception:
            snapshot = {}
            snapshot_used = False

    ob = bitget_ws.get_orderbook(settings.symbol)
    imbalance = orderbook_imbalance(ob) if ob else 0.0
    cvd = bitget_ws.get_cvd(settings.symbol)

    if is_shock and snapshot_used:
        spread_bps = float(snapshot.get("spread_bps") or 999.0)
        metrics: Dict[str, Any] = {}
        for key, value in snapshot.items():
            if isinstance(value, (int, float, bool)):
                metrics[key] = value
        shock = score_volume_shock(metrics, spread_bps, settings)
        metrics = {
            **metrics,
            **{k: v for k, v in shock.items() if isinstance(v, (int, float, bool))},
            "spread_bps": spread_bps,
            "orderbook_imbalance": imbalance,
            "cvd": cvd,
            "mode": "volume_shock_snapshot",
            "snapshot_used": True,
        }
        ok, reason = validate_volume_shock(settings, metrics, spread_bps)
        if not ok:
            raise RuntimeError(f"snapshot rejected: {reason}")
        buffer = compute_volume_shock_entry_buffer(settings, metrics, float(metrics["last"]))
        stop_distance = compute_volume_shock_stop_distance(settings, metrics, float(metrics["last"]))
        score = {"volatility_score": float(snapshot.get("volatility_score") or 0)}
    else:
        ohlcv = await fetch_ohlcv(exchange, settings.symbol, settings.timeframe, limit=160)
        spread_bps = await get_spread_bps(exchange, settings.symbol)
        if is_shock:
            lookback = max(1, int(sget(settings, "volume_shock_lookback_minutes")))
        elif event.provider in VOLATILITY_PROVIDERS:
            lookback = settings.volatility_lookback_minutes
        else:
            lookback = settings.range_lookback_minutes

        metrics = volatility_metrics(ohlcv, lookback, settings.compression_lookback_minutes)
        score = score_volatility(metrics, spread_bps, settings)
        shock = score_volume_shock(metrics, spread_bps, settings)
        metrics = {
            **metrics,
            **{k: v for k, v in score.items() if isinstance(v, (int, float))},
            **{k: v for k, v in shock.items() if isinstance(v, (int, float, bool))},
            "spread_bps": spread_bps,
            "orderbook_imbalance": imbalance,
            "cvd": cvd,
            "mode": "volume_shock" if is_shock else "normal_sniper",
            "snapshot_used": False,
        }

        if is_shock:
            ok, reason = validate_volume_shock(settings, metrics, spread_bps)
            if not ok:
                raise RuntimeError(reason)
            buffer = compute_volume_shock_entry_buffer(settings, metrics, metrics["last"])
            stop_distance = compute_volume_shock_stop_distance(settings, metrics, metrics["last"])
        else:
            ok, reason = validate_market_for_event(settings, metrics, spread_bps)
            if not ok:
                raise RuntimeError(reason)
            if event.provider in {"volatility_scanner", "manual_volatility"} and float(score["volatility_score"]) < settings.notify_score:
                raise RuntimeError(f"volatility score too low: {score['volatility_score']}")
            buffer = max(settings.min_entry_buffer_usd, metrics["atr14"] * settings.entry_buffer_atr)
            stop_distance = compute_stop_distance(settings, metrics["atr14"], metrics["range"])

    live_balance = await fetch_balance_usdt(exchange)
    
    # Динамический риск: множитель объема от качества сетапа
    risk = compute_order_size(settings, float(metrics["last"]), stop_distance, live_balance, float(score.get("volatility_score") or 0))
    if not risk.allowed:
        raise RuntimeError(risk.reason)

    amount = order_amount_precision(exchange, settings.symbol, risk.amount)
    buy_trigger = price_precision(exchange, settings.symbol, float(metrics["high"]) + buffer)
    sell_trigger = price_precision(exchange, settings.symbol, float(metrics["low"]) - buffer)
    buy_stop_loss = price_precision(exchange, settings.symbol, buy_trigger - stop_distance)
    sell_stop_loss = price_precision(exchange, settings.symbol, sell_trigger + stop_distance)

    uid = uuid.uuid4().hex[:12]
    return ArmedPlan(
        event=event,
        metrics={
            **metrics,
            "entry_buffer": buffer,
            "volatility_score": float(score.get("volatility_score") or 0),
            "volume_shock_score": float(metrics.get("volume_shock_score") or 0),
            "symbol_profile": base_symbol(settings.symbol),
            "buy_stop_loss": buy_stop_loss,
            "sell_stop_loss": sell_stop_loss,
        },
        buy_trigger=buy_trigger,
        sell_trigger=sell_trigger,
        stop_distance=stop_distance,
        amount=amount,
        risk_usd=risk.risk_usd,
        notional=risk.notional,
        buy_stop_loss=buy_stop_loss,
        sell_stop_loss=sell_stop_loss,
        buy_client_oid=f"vhs-buy-{uid}",
        sell_client_oid=f"vhs-sell-{uid}",
    )


async def place_armed_orders(exchange, settings: BotSettings, plan: ArmedPlan) -> ArmedPlan:
    live_allowed, live_reason = live_trading_allowed(settings)
    if not live_allowed:
        raise RuntimeError(f"Real order blocked: {live_reason}")

    buy = await place_trigger_entry(
        exchange=exchange,
        symbol=settings.symbol,
        direction="long",
        amount=plan.amount,
        trigger_price=plan.buy_trigger,
        client_oid=plan.buy_client_oid,
        hedge_mode=settings.hedge_mode,
        isolated=settings.isolated_margin,
        stop_loss_price=plan.buy_stop_loss,
    )
    sell = await place_trigger_entry(
        exchange=exchange,
        symbol=settings.symbol,
        direction="short",
        amount=plan.amount,
        trigger_price=plan.sell_trigger,
        client_oid=plan.sell_client_oid,
        hedge_mode=settings.hedge_mode,
        isolated=settings.isolated_margin,
        stop_loss_price=plan.sell_stop_loss,
    )
    plan.buy_order_id = str(buy.get("id") or buy.get("orderId") or plan.buy_client_oid)
    plan.sell_order_id = str(sell.get("id") or sell.get("orderId") or plan.sell_client_oid)

    strategy_name = "volume_shock_runner" if plan.event.provider == "volume_shock" else "volatility_hunter" if plan.event.provider in {"volatility_scanner", "manual_volatility"} else "news_volatility_sniper"
    
    db.create_trade({
        "client_oid": plan.buy_client_oid,
        "exchange_order_id": plan.buy_order_id,
        "symbol": settings.symbol,
        "order_side": "buy",
        "status": "armed",
        "strategy": strategy_name,
        "event_provider_id": plan.event.provider_id,
        "event_title": plan.event.title,
        "trigger_price": plan.buy_trigger,
        "amount": plan.amount,
        "risk_usd": plan.risk_usd,
        "meta": {"plan": plan.metrics, "paired_oid": plan.sell_client_oid, "preset_sl": plan.buy_stop_loss},
    })
    db.create_trade({
        "client_oid": plan.sell_client_oid,
        "exchange_order_id": plan.sell_order_id,
        "symbol": settings.symbol,
        "order_side": "sell",
        "status": "armed",
        "strategy": strategy_name,
        "event_provider_id": plan.event.provider_id,
        "event_title": plan.event.title,
        "trigger_price": plan.sell_trigger,
        "amount": plan.amount,
        "risk_usd": plan.risk_usd,
        "meta": {"plan": plan.metrics, "paired_oid": plan.buy_client_oid, "preset_sl": plan.sell_stop_loss},
    })
    
    fire_log("warning", "orders_armed", "LIVE trigger traps placed", {
        "event": plan.event.title,
        "provider": plan.event.provider,
        "buy_trigger": plan.buy_trigger,
        "sell_trigger": plan.sell_trigger,
        "amount": plan.amount,
        "score": plan.metrics.get("volatility_score"),
    })
    return plan


def infer_filled(order: Dict[str, Any]) -> bool:
    status = str(order.get("status") or "").lower()
    filled = float(order.get("filled") or 0)
    return status in ("closed", "filled") or filled > 0


async def wait_for_breakout(exchange, settings: BotSettings, plan: ArmedPlan) -> Optional[str]:
    post_wait = sget(settings, "volume_shock_order_life_seconds") if plan.event.provider == "volume_shock" else settings.auto_post_wait_seconds if plan.event.provider in VOLATILITY_PROVIDERS else settings.post_event_wait_seconds
    deadline = plan.event.event_time_utc + timedelta(seconds=post_wait)
    mode = BotMode.VOLATILITY_ARMED.value if plan.event.provider in VOLATILITY_PROVIDERS else BotMode.CALENDAR_ARMED.value
    
    fire_state(mode, True, f"armed traps: {plan.event.title}")

    last_rest_check = 0.0

    while utc_now() <= deadline and not engine_stop.is_set():
        px = await get_last_price(exchange, settings.symbol)
        
        # --- QUANT WALL DEFENSE (Анти-Spoofing Защита) ---
        # Проверяем стакан в реальном времени. Если цена летит к пробою, а там бетонная стена против нас — отменяем ловушку.
        ob = bitget_ws.get_orderbook(settings.symbol)
        imbalance = orderbook_imbalance(ob) if ob else 0.0
        max_imb = getattr(settings, "max_imbalance_against", 0.35)
        
        # Защита Лонга (если до триггера менее 0.15%, но сверху сильная стена Ask)
        if plan.buy_order_id and px > 0 and (plan.buy_trigger - px) / px < 0.0015:
            if imbalance < -max_imb:
                fire_log("warning", "quant_wall_defense", "Proactively cancelled LONG trap due to heavy Ask wall", {"imbalance": imbalance, "trigger": plan.buy_trigger, "price": px})
                await cancel_safely(exchange, plan.buy_order_id, settings.symbol)
                plan.buy_order_id = None
                fire_update_trade(plan.buy_client_oid, {"status": "cancelled", "close_reason": "quant_wall_defense"})

        # Защита Шорта (если до триггера менее 0.15%, но снизу сильная стена Bid)
        if plan.sell_order_id and px > 0 and (px - plan.sell_trigger) / px < 0.0015:
            if imbalance > max_imb:
                fire_log("warning", "quant_wall_defense", "Proactively cancelled SHORT trap due to heavy Bid wall", {"imbalance": imbalance, "trigger": plan.sell_trigger, "price": px})
                await cancel_safely(exchange, plan.sell_order_id, settings.symbol)
                plan.sell_order_id = None
                fire_update_trade(plan.sell_client_oid, {"status": "cancelled", "close_reason": "quant_wall_defense"})
                
        # Если обе ловушки отменены из-за стен в стакане — выходим из ожидания
        if not plan.buy_order_id and not plan.sell_order_id:
            fire_log("info", "traps_removed", "Both traps were cancelled by Quant defense", {})
            break
        # ------------------------------------------------
        
        crossed = False
        if plan.buy_order_id and px >= plan.buy_trigger:
            crossed = True
        if plan.sell_order_id and px <= plan.sell_trigger:
            crossed = True
        
        now_ts = time.time()
        
        if crossed or (now_ts - last_rest_check > 1.5):
            try:
                pos = await fetch_symbol_position(exchange, settings.symbol)
                last_rest_check = now_ts
                
                if float(pos.get("amount") or 0) > 0 and pos.get("direction") in ("long", "short"):
                    direction = str(pos.get("direction"))
                    entry_px = pos.get("entry") or px
                    
                    if direction == "long":
                        if plan.sell_order_id:
                            await cancel_safely(exchange, plan.sell_order_id, settings.symbol)
                        fire_update_trade(plan.buy_client_oid, {"status": "active", "execution_price": entry_px})
                        fire_update_trade(plan.sell_client_oid, {"status": "cancelled"})
                        fire_log("warning", "breakout_position_detected", "Long position detected after trigger trap", {"symbol": settings.symbol, "amount": pos.get("amount"), "entry": entry_px, "preset_sl": plan.buy_stop_loss})
                        return "long"
                    else:
                        if plan.buy_order_id:
                            await cancel_safely(exchange, plan.buy_order_id, settings.symbol)
                        fire_update_trade(plan.sell_client_oid, {"status": "active", "execution_price": entry_px})
                        fire_update_trade(plan.buy_client_oid, {"status": "cancelled"})
                        fire_log("warning", "breakout_position_detected", "Short position detected after trigger trap", {"symbol": settings.symbol, "amount": pos.get("amount"), "entry": entry_px, "preset_sl": plan.sell_stop_loss})
                        return "short"
            except Exception as e:
                fire_log("error", "watch_position", f"Position watch error: {e}", {})
                
        await asyncio.sleep(0.05)

    if plan.buy_order_id:
        await cancel_safely(exchange, plan.buy_order_id, settings.symbol)
        fire_update_trade(plan.buy_client_oid, {"status": "expired"})
    if plan.sell_order_id:
        await cancel_safely(exchange, plan.sell_order_id, settings.symbol)
        fire_update_trade(plan.sell_client_oid, {"status": "expired"})
        
    fire_log("info", "no_breakout", "Window passed without valid breakout; traps cancelled", {"event": plan.event.title})
    return None


async def attach_exchange_protection(exchange, settings: BotSettings, direction: str, amount: float, entry: float, stop_distance: float) -> Dict[str, Any]:
    if direction == "long":
        sl = price_precision(exchange, settings.symbol, entry - stop_distance)
        tp1 = price_precision(exchange, settings.symbol, entry + stop_distance * settings.tp1_r)
        tp2 = price_precision(exchange, settings.symbol, entry + stop_distance * settings.tp2_r)
    else:
        sl = price_precision(exchange, settings.symbol, entry + stop_distance)
        tp1 = price_precision(exchange, settings.symbol, entry - stop_distance * settings.tp1_r)
        tp2 = price_precision(exchange, settings.symbol, entry - stop_distance * settings.tp2_r)

    tp1_amount = 0.0
    if settings.tp1_enabled and settings.tp1_close_pct > 0:
        tp1_amount = order_amount_precision(exchange, settings.symbol, amount * settings.tp1_close_pct)

    out: Dict[str, Any] = {
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp1_amount": tp1_amount,
        "exchange_sl": None,
        "exchange_tp1": None,
        "exchange_tp2": None,
    }
    try:
        out["exchange_sl"] = await place_reduce_trigger(
            exchange, settings.symbol, direction, amount, sl, "stop_loss", f"vhs-sl-{uuid.uuid4().hex[:10]}", settings.hedge_mode
        )
        out["exchange_sl_id"] = extract_order_id(out["exchange_sl"])
    except Exception as e:
        fire_log("error", "protect_sl_failed", f"Exchange SL failed; manual guard active: {e}", {"sl": sl})

    if tp1_amount > 0:
        try:
            out["exchange_tp1"] = await place_reduce_trigger(
                exchange, settings.symbol, direction, tp1_amount, tp1, "take_profit", f"vhs-tp1-{uuid.uuid4().hex[:10]}", settings.hedge_mode
            )
            out["exchange_tp1_id"] = extract_order_id(out["exchange_tp1"])
            fire_log("info", "exchange_tp1_placed", "Exchange TP1 reduce trigger placed", {"tp1": tp1, "amount": tp1_amount})
        except Exception as e:
            fire_log("error", "protect_tp1_failed", f"Exchange TP1 failed; local TP1 fallback active: {e}", {"tp1": tp1, "amount": tp1_amount})

    if settings.tp2_enabled:
        try:
            out["exchange_tp2"] = await place_reduce_trigger(
                exchange, settings.symbol, direction, amount, tp2, "take_profit", f"vhs-tp2-{uuid.uuid4().hex[:10]}", settings.hedge_mode
            )
            out["exchange_tp2_id"] = extract_order_id(out["exchange_tp2"])
        except Exception as e:
            fire_log("error", "protect_tp2_failed", f"Exchange TP2 failed; manual guard active: {e}", {"tp2": tp2})

    if not out.get("exchange_sl") and settings.cancel_tp_if_sl_fails:
        for key, label in (("exchange_tp1", "TP1"), ("exchange_tp2", "TP2")):
            if out.get(key):
                try:
                    await cancel_safely(exchange, str(out[key].get("id") or out[key].get("orderId")), settings.symbol)
                    fire_log("warning", "orphan_tp_cancelled", f"{label} cancelled because exchange SL was not confirmed", {"tp1": tp1, "tp2": tp2})
                except Exception as e:
                    fire_log("error", "orphan_tp_cancel_failed", f"Could not cancel {label} after SL failure: {e}", {})
    return out


async def update_exchange_stop(exchange, settings: BotSettings, direction: str, amount: float, new_stop: float, old_order_id: Optional[str]) -> Optional[str]:
    if not settings.exchange_trailing_sl_enabled or amount <= 0:
        return old_order_id
    try:
        new_sl = await place_reduce_trigger(
            exchange, settings.symbol, direction, amount, price_precision(exchange, settings.symbol, new_stop),
            "stop_loss", f"vhs-trail-sl-{uuid.uuid4().hex[:10]}", settings.hedge_mode
        )
        new_id = extract_order_id(new_sl)
        if old_order_id:
            await cancel_safely(exchange, old_order_id, settings.symbol)
        fire_log("info", "trailing_sl_updated", "Exchange SL moved by trailing manager", {"new_stop": new_stop, "old_order_id": old_order_id, "new_order_id": new_id})
        return new_id
    except Exception as e:
        fire_log("error", "trailing_sl_update_failed", f"Could not update exchange trailing SL: {e}", {"new_stop": new_stop})
        return old_order_id


async def manage_position(exchange, settings: BotSettings, plan: ArmedPlan, direction: str) -> None:
    fire_state(BotMode.IN_TRADE.value, True, f"{direction} active")
    entry = await get_last_price(exchange, settings.symbol)
    side_close = "sell" if direction == "long" else "buy"
    remaining = plan.amount
    tp1_done = False
    trailing_active = False
    stop_price = entry - plan.stop_distance if direction == "long" else entry + plan.stop_distance
    last_stop_sent = stop_price
    last_stop_update_at = 0.0
    current_sl_order_id: Optional[str] = None
    preset_sl_price = plan.buy_stop_loss if direction == "long" else plan.sell_stop_loss
    tp1_price = entry + plan.stop_distance * settings.tp1_r if direction == "long" else entry - plan.stop_distance * settings.tp1_r
    tp2_price = entry + plan.stop_distance * settings.tp2_r if direction == "long" else entry - plan.stop_distance * settings.tp2_r
    best_price = entry
    started = time.time()
    last_protection = await attach_exchange_protection(exchange, settings, direction, remaining, entry, plan.stop_distance)
    current_sl_order_id = last_protection.get("exchange_sl_id")
    current_tp1_order_id = last_protection.get("exchange_tp1_id")
    current_tp2_order_id = last_protection.get("exchange_tp2_id")
    has_exchange_guard = bool(last_protection.get("exchange_sl")) or bool(preset_sl_price)
    
    if bool(preset_sl_price) and not last_protection.get("exchange_sl"):
        fire_log("warning", "preset_sl_guard_active", "Position protected by stop-loss preset on trigger order", {"direction": direction, "preset_sl": preset_sl_price})

    if settings.hard_exchange_sl_required and not has_exchange_guard:
        active_oid = plan.buy_client_oid if direction == "long" else plan.sell_client_oid
        msg = "Exchange-side SL was not confirmed; position will be flattened immediately"
        fire_log("critical", "no_exchange_sl_flatten", msg, {
            "direction": direction,
            "entry": entry,
            "amount": remaining,
            "planned_stop_distance": plan.stop_distance,
        })
        if settings.flatten_if_exchange_sl_fails:
            try:
                px = await get_last_price(exchange, settings.symbol)
                await close_position_market(exchange, settings.symbol, side_close, remaining, direction if settings.hedge_mode else None)
                pnl = (px - entry) * remaining if direction == "long" else (entry - px) * remaining
                fee_est = plan.notional * 0.0008 * 2
                fire_update_trade(active_oid, {
                    "status": "closed",
                    "pnl": pnl - fee_est,
                    "close_price": px,
                    "close_reason": "no_exchange_sl",
                    "meta": {"entry": entry, "last": px, "gross_pnl": pnl, "fee_est": fee_est, "score": plan.metrics.get("volatility_score")},
                })
                fire_state(BotMode.PAUSED.value, False, "paused: exchange SL failed and position was flattened")
                return
            except Exception as e:
                fire_log("critical", "no_exchange_sl_flatten_failed", f"Could not flatten after SL failure: {e}", {})
                if settings.emergency_flatten_on_error:
                    try:
                        await flatten_symbol_positions(exchange, settings.symbol, settings.hedge_mode)
                    except Exception as inner:
                        fire_log("critical", "symbol_flatten_failed", f"Symbol flatten failed: {inner}", {})
                return

    fire_log("warning", "position_active", "Breakout position is active", {
        "direction": direction,
        "entry": entry,
        "amount": remaining,
        "sl": stop_price,
        "tp1": tp1_price,
        "tp2": tp2_price,
        "exchange_protection": bool(has_exchange_guard)
    })

    realized = 0.0
    last_sync_time = time.time()
    live_amount = remaining

    try:
        while not engine_stop.is_set():
            price = await get_last_price(exchange, settings.symbol)
            now_ts = time.time()

            if now_ts - last_sync_time > 2.0:
                live_pos = await fetch_symbol_position(exchange, settings.symbol)
                live_amount = float(live_pos.get("amount") or 0)
                last_sync_time = now_ts

            if live_amount <= 0:
                active_oid = plan.buy_client_oid if direction == "long" else plan.sell_client_oid
                fire_update_trade(active_oid, {
                    "status": "closed",
                    "close_price": price,
                    "close_reason": "external_or_manual_close",
                    "meta": {"entry": entry, "last": price, "manual_or_exchange_closed": True, "score": plan.metrics.get("volatility_score")},
                })
                fire_log("warning", "position_closed_externally", "Position is no longer open on exchange; marked closed in database", {"direction": direction, "price": price})
                break

            if settings.tp1_enabled and not tp1_done and live_amount < remaining:
                closed_amount = order_amount_precision(exchange, settings.symbol, max(0.0, remaining - live_amount))
                if closed_amount > 0:
                    pnl = (price - entry) * closed_amount if direction == "long" else (entry - price) * closed_amount
                    realized += pnl
                    remaining = order_amount_precision(exchange, settings.symbol, live_amount)
                    tp1_done = True
                    stop_price = entry
                    current_sl_order_id = await update_exchange_stop(exchange, settings, direction, remaining, stop_price, current_sl_order_id)
                    last_stop_sent = stop_price
                    last_stop_update_at = now_ts
                    fire_log("info", "tp1_exchange_detected", "Exchange TP1 filled; stop moved to breakeven", {"price": price, "pnl": pnl, "closed_amount": closed_amount, "remaining": remaining, "sl": stop_price})

            if direction == "long":
                r = (price - entry) / plan.stop_distance
                best_price = max(best_price, price)
                hit_stop = price <= stop_price
                hit_tp1 = price >= tp1_price
                hit_tp2 = price >= tp2_price
                trail_candidate = best_price - max(plan.metrics.get("atr14", 0) * settings.trailing_atr_mult, plan.stop_distance * 0.35)
                if r >= settings.trailing_start_r and settings.trailing_mode:
                    stop_price = max(stop_price, trail_candidate)
                    trailing_active = True
                if r >= settings.breakeven_after_r:
                    stop_price = max(stop_price, entry)
            else:
                r = (entry - price) / plan.stop_distance
                best_price = min(best_price, price)
                hit_stop = price >= stop_price
                hit_tp1 = price <= tp1_price
                hit_tp2 = price <= tp2_price
                trail_candidate = best_price + max(plan.metrics.get("atr14", 0) * settings.trailing_atr_mult, plan.stop_distance * 0.35)
                if r >= settings.trailing_start_r and settings.trailing_mode:
                    stop_price = min(stop_price, trail_candidate)
                    trailing_active = True
                if r >= settings.breakeven_after_r:
                    stop_price = min(stop_price, entry)

            elapsed = now_ts - started
            stale_exit = elapsed >= settings.stale_trade_exit_seconds and r < settings.stale_trade_min_r
            timeout_exit = elapsed >= settings.hard_timeout_seconds

            stop_step = max(float(plan.metrics.get("atr14", 0) or 0) * settings.trailing_min_step_atr, settings.trailing_min_step_usd)
            stop_moved_enough = abs(stop_price - last_stop_sent) >= stop_step
            if (trailing_active or r >= settings.breakeven_after_r) and stop_moved_enough and (now_ts - last_stop_update_at) >= settings.trailing_update_interval_seconds:
                current_sl_order_id = await update_exchange_stop(exchange, settings, direction, remaining, stop_price, current_sl_order_id)
                last_stop_sent = stop_price
                last_stop_update_at = now_ts

            if settings.tp1_enabled and settings.tp1_close_pct > 0 and hit_tp1 and not tp1_done and not current_tp1_order_id:
                close_amount = order_amount_precision(exchange, settings.symbol, remaining * settings.tp1_close_pct)
                if close_amount > 0:
                    await close_position_market(exchange, settings.symbol, side_close, close_amount, direction if settings.hedge_mode else None)
                    pnl = (price - entry) * close_amount if direction == "long" else (entry - price) * close_amount
                    realized += pnl
                    remaining = order_amount_precision(exchange, settings.symbol, remaining - close_amount)
                    tp1_done = True
                    stop_price = entry
                    current_sl_order_id = await update_exchange_stop(exchange, settings, direction, remaining, stop_price, current_sl_order_id)
                    last_stop_sent = stop_price
                    last_stop_update_at = now_ts
                    fire_log("info", "tp1_local_fallback", "TP1 partial profit taken by local fallback; stop moved to breakeven", {"price": price, "pnl": pnl, "remaining": remaining})

            exit_by_tp2 = bool(settings.tp2_enabled and hit_tp2)
            if hit_stop or exit_by_tp2 or stale_exit or timeout_exit:
                reason = "trailing_stop" if hit_stop and trailing_active else "stop" if hit_stop else "tp2" if exit_by_tp2 else "stale_exit" if stale_exit else "timeout"
                
                cancel_tasks = []
                if current_tp1_order_id:
                    cancel_tasks.append(cancel_safely(exchange, current_tp1_order_id, settings.symbol))
                if current_tp2_order_id:
                    cancel_tasks.append(cancel_safely(exchange, current_tp2_order_id, settings.symbol))
                
                if cancel_tasks:
                    await asyncio.gather(*cancel_tasks, return_exceptions=True)
                    fire_log("info", "exit_tp_cancelled", "TP orders cancelled before final exit", {"reason": reason})
                
                if remaining > 0:
                    await close_position_market(exchange, settings.symbol, side_close, remaining, direction if settings.hedge_mode else None)
                    pnl = (price - entry) * remaining if direction == "long" else (entry - price) * remaining
                    realized += pnl
                
                fee_est = plan.notional * 0.0008 * 2
                net_pnl = realized - fee_est
                active_oid = plan.buy_client_oid if direction == "long" else plan.sell_client_oid
                
                fire_update_trade(active_oid, {
                    "status": "closed",
                    "pnl": net_pnl,
                    "close_price": price,
                    "close_reason": reason,
                    "meta": {"entry": entry, "last": price, "gross_pnl": realized, "fee_est": fee_est, "tp1_done": tp1_done, "trailing_active": trailing_active, "last_stop": stop_price, "score": plan.metrics.get("volatility_score")},
                })
                fire_log("warning" if net_pnl < 0 else "info", "position_closed", f"Position closed: {reason}", {"net_pnl": net_pnl, "price": price})
                break

            await asyncio.sleep(0.05)
    except Exception as e:
        fire_log("error", "manage_position_error", f"Position manager error: {e}", {})
        if settings.emergency_flatten_on_error and remaining > 0:
            try:
                await close_position_market(exchange, settings.symbol, side_close, remaining, direction if settings.hedge_mode else None)
                fire_log("warning", "emergency_flatten", "Emergency flatten executed after manager error", {"direction": direction, "amount": remaining})
            except Exception as inner:
                fire_log("critical", "emergency_flatten_failed", f"Emergency flatten failed: {inner}", {})


async def prepare_and_trade_event(settings: BotSettings, event: NewsEvent) -> bool:
    mode = BotMode.VOLATILITY_ARMED.value if event.provider in VOLATILITY_PROVIDERS else BotMode.CALENDAR_ARMED.value
    db.upsert_news_event(event)
    db.mark_event_status(event.provider_id, "arming", "Preparing trigger traps")
    exchange = await get_exchange()
    armed = False
    try:
        ok, reason = daily_limits_ok(settings, db.todays_pnl(), db.todays_trade_count(), db.consecutive_losses())
        if not ok:
            raise RuntimeError(reason)

        plan = await build_armed_plan(exchange, settings, event)
        await place_armed_orders(exchange, settings, plan)
        armed = True
        direction = await wait_for_breakout(exchange, settings, plan)
        if direction:
            await manage_position(exchange, settings, plan, direction)
            db.mark_event_status(event.provider_id, "traded", f"Breakout direction: {direction}")
        else:
            db.mark_event_status(event.provider_id, "no_trade", "No breakout in allowed window")
    except Exception as e:
        db.log_event("error", "trade_failed", f"Trade failed: {e}", {"event": event.model_dump(mode="json"), "armed": armed})
        db.mark_event_status(event.provider_id, "failed", str(e))
        try:
            await cancel_all_safely(exchange, settings.symbol)
        except Exception:
            pass
    finally:
        if not engine_stop.is_set():
            db.set_runtime_state(mode, True, "waiting for next opportunity")
    return armed


async def maybe_auto_arm_volatility(settings: BotSettings) -> bool:
    global _last_vol_arm_at, _last_shock_arm_at, _last_hot_log_at
    if not settings.volatility_auto_enabled:
        return False

    scan = await analyze_markets(settings) if settings.scan_symbols else {"best": await analyze_market(settings.symbol, settings), "best_shock": {}, "markets": []}
    market = scan.get("best") or {}
    selected_symbol = market.get("symbol") or settings.symbol
    score = float(market.get("volatility_score") or 0)
    state = str(market.get("state") or "COLD")

    shock_market = scan.get("best_shock") or {}
    shock_symbol = shock_market.get("symbol") or selected_symbol
    shock_score = float(shock_market.get("volume_shock_score") or 0)
    shock_valid = bool(shock_market.get("volume_shock_valid") or shock_market.get("volume_shock_should_arm"))

    reason = f"best {selected_symbol} score {score:.1f} / {state}"
    if bool(sget(settings, "volume_shock_enabled")):
        reason += f" | shock {shock_symbol} {shock_score:.1f}"
    fire_state(BotMode.VOLATILITY_SCAN.value, True, reason)

    if score >= settings.notify_score or shock_score >= settings.notify_score:
        if not _last_hot_log_at or (utc_now() - _last_hot_log_at).total_seconds() > 60:
            fire_log("info", "multi_symbol_volatility_watch", f"Best normal {selected_symbol} score {score:.1f}; best shock {shock_symbol} score {shock_score:.1f}", {"best": market, "best_shock": shock_market, "top": (scan.get("markets") or [])[:5]})
            _last_hot_log_at = utc_now()

    if bool(sget(settings, "volume_shock_enabled")) and shock_valid:
        if _last_shock_arm_at and (utc_now() - _last_shock_arm_at).total_seconds() < sget(settings, "volume_shock_cooldown_minutes") * 60:
            pass
        else:
            ok, limit_reason = daily_limits_ok(settings, db.todays_pnl(), db.todays_trade_count(), db.consecutive_losses())
            if not ok:
                fire_log("warning", "volume_shock_blocked", limit_reason, shock_market)
                return False
            shock_settings = with_symbol_profile(settings, shock_symbol if settings.trade_selected_symbol else settings.symbol)
            event = synthetic_event(
                "volume_shock",
                0,
                f"VOLUME SHOCK {shock_symbol} shock={shock_score:.1f}",
                raw={"market": shock_market, "scan": scan},
            )
            armed = await prepare_and_trade_event(shock_settings, event)
            if armed:
                _last_shock_arm_at = utc_now()
                fire_log("info", "volume_shock_cooldown_started", "Volume shock cooldown started after successful orders_armed", {"symbol": shock_settings.symbol, "score": shock_score})
                return True
            return False

    if _last_vol_arm_at and (utc_now() - _last_vol_arm_at).total_seconds() < settings.volatility_cooldown_minutes * 60:
        return False

    if score >= settings.auto_arm_score and market.get("valid_for_sniper"):
        ok, limit_reason = daily_limits_ok(settings, db.todays_pnl(), db.todays_trade_count(), db.consecutive_losses())
        if not ok:
            fire_log("warning", "auto_arm_blocked", limit_reason, market)
            return False
        normal_settings = with_symbol_profile(settings, selected_symbol if settings.trade_selected_symbol else settings.symbol)
        event = synthetic_event(
            "volatility_scanner",
            normal_settings.auto_arm_delay_seconds,
            f"AUTO VOLATILITY HUNT {selected_symbol} score={score:.1f}",
            raw={"market": market, "scan": scan},
        )
        armed = await prepare_and_trade_event(normal_settings, event)
        if armed:
            _last_vol_arm_at = utc_now()
            fire_log("info", "volatility_cooldown_started", "Volatility cooldown started after successful orders_armed", {"symbol": normal_settings.symbol, "score": score})
            return True
        return False
    return False

async def engine_loop() -> None:
    db.set_runtime_state(BotMode.HYBRID_SCAN.value, True, "hybrid engine started")
    last_sync = datetime.fromtimestamp(0, tz=timezone.utc)
    while not engine_stop.is_set():
        try:
            settings = db.get_settings()
            if not settings.calendar_enabled and not settings.volatility_auto_enabled:
                fire_state(BotMode.PAUSED.value, False, "calendar and volatility scanner disabled")
                await asyncio.sleep(10)
                continue

            ok, reason = daily_limits_ok(settings, db.todays_pnl(), db.todays_trade_count(), db.consecutive_losses())
            if not ok:
                fire_state(BotMode.PAUSED.value, False, reason)
                await asyncio.sleep(15)
                continue

            selected: Optional[NewsEvent] = None
            if settings.calendar_enabled:
                if (utc_now() - last_sync).total_seconds() > 900:
                    await sync_calendar(settings)
                    last_sync = utc_now()
                rows = db.list_upcoming_events(limit=20)
                upcoming = [event_from_row(row) for row in rows]
                now = utc_now()
                arm_window_end = now + timedelta(seconds=settings.pre_arm_seconds)
                for ev in upcoming:
                    if ev.event_time_utc < now - timedelta(seconds=5):
                        continue
                    if now <= ev.event_time_utc <= arm_window_end:
                        selected = ev
                        break

            if selected:
                seconds_to_event = (selected.event_time_utc - utc_now()).total_seconds()
                if seconds_to_event > 1:
                    fire_state(BotMode.CALENDAR_ARMED.value, True, f"arming news soon: {selected.title} in {seconds_to_event:.0f}s")
                    await asyncio.sleep(max(0.2, min(5, seconds_to_event - 1)))
                await prepare_and_trade_event(settings, selected)
                await asyncio.sleep(settings.event_cooldown_minutes * 60)
                continue

            armed = await maybe_auto_arm_volatility(settings)
            if not armed:
                fire_state(BotMode.HYBRID_SCAN.value, True, "no news; scanning volatility")
                await asyncio.sleep(settings.scan_interval_seconds)
        except asyncio.CancelledError:
            break
        except Exception as e:
            fire_log("error", "engine_loop", f"Engine loop error: {e}", {})
            await asyncio.sleep(5)
    fire_state(BotMode.OFF.value, False, "engine stopped")


async def start_engine() -> Dict[str, Any]:
    global engine_task, engine_stop
    if engine_task and not engine_task.done():
        return {"status": "already_running"}
    settings = db.get_settings()
    live_allowed, live_reason = live_trading_allowed(settings)
    if not live_allowed:
        raise RuntimeError(f"LIVE blocked: {live_reason}. Need settings.live_mode=true, LIVE_TRADING_UNLOCK=true, EXCHANGE_STOPS_VERIFIED=true.")
    engine_stop.clear()
    if settings.ws_enabled:
        symbols = settings.scan_symbols or [settings.symbol]
        await bitget_ws.start(symbols, settings.ws_public_enabled, settings.ws_private_enabled, settings.ws_reconnect_seconds)
    engine_task = asyncio.create_task(engine_loop())
    return {"status": "started", "mode": "hybrid_scan", "calendar_enabled": settings.calendar_enabled, "volatility_auto_enabled": settings.volatility_auto_enabled, "ws": bitget_ws.status()}


async def stop_engine() -> Dict[str, Any]:
    global engine_task, engine_stop
    engine_stop.set()
    settings = db.get_settings()
    symbols = list(dict.fromkeys([str(x).strip() for x in (settings.scan_symbols or [settings.symbol]) if str(x).strip()]))
    exchange = await get_exchange()
    results: Dict[str, Any] = {}
    try:
        for sym in symbols:
            try:
                await cancel_all_safely(exchange, sym)
                results[sym] = {"orders_cancelled": True, "flattened": False}
                if settings.kill_switch_closes_positions:
                    await flatten_symbol_positions(exchange, sym, settings.hedge_mode)
                    results[sym]["flattened"] = True
                    fire_log("warning", "kill_switch_flatten", "Kill switch attempted to flatten open symbol positions", {"symbol": sym})
            except Exception as e:
                results[sym] = {"error": str(e)}
                fire_log("critical", "kill_switch_symbol_failed", f"Kill switch failed for {sym}: {e}", {"symbol": sym})
    finally:
        await exchange.close()
    if engine_task:
        engine_task.cancel()
    await bitget_ws.stop()
    fire_state(BotMode.OFF.value, False, "manual stop; scan-symbol orders cancellation attempted")
    return {"status": "stopped", "message": "Engine stopped and scan-symbol order cancellation attempted.", "symbols": results}


async def manual_arm(provider_id: str) -> Dict[str, Any]:
    rows = db.list_upcoming_events(limit=50)
    event = None
    for row in rows:
        if row.get("provider_id") == provider_id:
            event = event_from_row(row)
            break
    if not event:
        raise RuntimeError("Event not found")
    settings = db.get_settings()
    live_allowed, live_reason = live_trading_allowed(settings)
    if not live_allowed:
        raise RuntimeError(f"LIVE blocked: {live_reason}")
    asyncio.create_task(prepare_and_trade_event(settings, event))
    return {"status": "manual_armed", "event": event.model_dump(mode="json")}


async def manual_arm_now(payload: ManualArmNowPayload) -> Dict[str, Any]:
    settings = db.get_settings()
    if not settings.manual_arm_enabled:
        raise RuntimeError("manual arm disabled")
    live_allowed, live_reason = live_trading_allowed(settings)
    if not live_allowed:
        raise RuntimeError(f"LIVE blocked: {live_reason}")
    market = await analyze_market(settings.symbol, settings)
    score = float(market.get("volatility_score") or 0)
    if score < settings.notify_score:
        raise RuntimeError(f"Market score too low for manual arm: {score:.1f}. You can lower notify_score, but it increases fakeout risk.")
    event = synthetic_event(
        "manual_volatility",
        payload.arm_delay_seconds,
        f"MANUAL VOLATILITY ARM score={score:.1f}: {payload.note}",
        raw={"market": market, "note": payload.note},
    )
    asyncio.create_task(prepare_and_trade_event(settings, event))
    return {"status": "manual_volatility_armed", "event": event.model_dump(mode="json"), "market": market}
