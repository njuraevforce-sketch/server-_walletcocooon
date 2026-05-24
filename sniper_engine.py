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
)
from indicators import pre_event_metrics, volatility_metrics, score_volatility
from models import BotMode, BotSettings, NewsEvent, EventImpact, ManualArmNowPayload
from risk import compute_order_size, compute_stop_distance, live_trading_allowed, validate_market_for_event, daily_limits_ok
import bitget_ws

engine_task: Optional[asyncio.Task] = None
engine_stop = asyncio.Event()
_last_vol_arm_at: Optional[datetime] = None
_last_hot_log_at: Optional[datetime] = None


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


async def sync_calendar(settings: BotSettings) -> List[NewsEvent]:
    events = await fetch_calendar(days_ahead=5)
    filtered = filter_events_for_crypto(events, settings)
    for ev in filtered:
        db.upsert_news_event(ev)
    db.log_event("info", "calendar_sync", f"Calendar synced: {len(filtered)} relevant events", {"count": len(filtered)})
    return filtered


async def analyze_market(symbol: str, settings: BotSettings) -> Dict[str, Any]:
    exchange = await get_exchange()
    try:
        ohlcv = await fetch_ohlcv(exchange, symbol, settings.timeframe, limit=160)
        spread_bps = await get_spread_bps(exchange, symbol)
        metrics = volatility_metrics(ohlcv, settings.volatility_lookback_minutes, settings.compression_lookback_minutes)
        event_valid, event_reason = validate_market_for_event(settings, metrics, spread_bps)
        score = score_volatility(metrics, spread_bps, settings)
        return {
            "valid_for_sniper": bool(event_valid),
            "valid_for_news_sniper": bool(event_valid),
            "event_reason": event_reason,
            "reason": score["reason"],
            "spread_bps": spread_bps,
            **metrics,
            **score,
        }
    finally:
        await exchange.close()




async def analyze_markets(settings: BotSettings) -> Dict[str, Any]:
    """Scan several symbols and return the best current volatility candidate.
    This keeps the same entry filters; it only chooses the best pair in the moment.
    """
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
    best = markets_sorted[0] if markets_sorted else {}
    return {"symbols": symbols, "best": best, "markets": markets_sorted}

async def build_armed_plan(exchange, settings: BotSettings, event: NewsEvent) -> ArmedPlan:
    await configure_symbol(exchange, settings.symbol, settings.leverage, settings.isolated_margin)
    ohlcv = await fetch_ohlcv(exchange, settings.symbol, settings.timeframe, limit=160)
    spread_bps = await get_spread_bps(exchange, settings.symbol)
    lookback = settings.volatility_lookback_minutes if event.provider in {"volatility_scanner", "manual_volatility"} else settings.range_lookback_minutes
    metrics = volatility_metrics(ohlcv, lookback, settings.compression_lookback_minutes)
    score = score_volatility(metrics, spread_bps, settings)
    metrics = {**metrics, **{k: v for k, v in score.items() if isinstance(v, (int, float))}, "spread_bps": spread_bps}

    ok, reason = validate_market_for_event(settings, metrics, spread_bps)
    if not ok:
        raise RuntimeError(reason)
    if event.provider in {"volatility_scanner", "manual_volatility"} and float(score["volatility_score"]) < settings.notify_score:
        raise RuntimeError(f"volatility score too low: {score['volatility_score']}")

    buffer = max(settings.min_entry_buffer_usd, metrics["atr14"] * settings.entry_buffer_atr)
    stop_distance = compute_stop_distance(settings, metrics["atr14"], metrics["range"])
    live_balance = await fetch_balance_usdt(exchange)
    risk = compute_order_size(settings, metrics["last"], stop_distance, live_balance)
    if not risk.allowed:
        raise RuntimeError(risk.reason)

    amount = order_amount_precision(exchange, settings.symbol, risk.amount)
    buy_trigger = price_precision(exchange, settings.symbol, metrics["high"] + buffer)
    sell_trigger = price_precision(exchange, settings.symbol, metrics["low"] - buffer)

    uid = uuid.uuid4().hex[:12]
    return ArmedPlan(
        event=event,
        metrics={**metrics, "entry_buffer": buffer, "volatility_score": float(score["volatility_score"])},
        buy_trigger=buy_trigger,
        sell_trigger=sell_trigger,
        stop_distance=stop_distance,
        amount=amount,
        risk_usd=risk.risk_usd,
        notional=risk.notional,
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
    )
    sell = await place_trigger_entry(
        exchange=exchange,
        symbol=settings.symbol,
        direction="short",
        amount=plan.amount,
        trigger_price=plan.sell_trigger,
        client_oid=plan.sell_client_oid,
        hedge_mode=settings.hedge_mode,
    )
    plan.buy_order_id = str(buy.get("id") or buy.get("orderId") or plan.buy_client_oid)
    plan.sell_order_id = str(sell.get("id") or sell.get("orderId") or plan.sell_client_oid)

    strategy_name = "volatility_hunter" if plan.event.provider in {"volatility_scanner", "manual_volatility"} else "news_volatility_sniper"
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
        "meta": {"plan": plan.metrics, "paired_oid": plan.sell_client_oid},
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
        "meta": {"plan": plan.metrics, "paired_oid": plan.buy_client_oid},
    })
    db.log_event("warning", "orders_armed", "LIVE trigger traps placed", {
        "event": plan.event.title,
        "provider": plan.event.provider,
        "time": plan.event.event_time_utc.isoformat(),
        "buy_trigger": plan.buy_trigger,
        "sell_trigger": plan.sell_trigger,
        "amount": plan.amount,
        "risk_usd": plan.risk_usd,
        "score": plan.metrics.get("volatility_score"),
    })
    return plan


def infer_filled(order: Dict[str, Any]) -> bool:
    status = str(order.get("status") or "").lower()
    filled = float(order.get("filled") or 0)
    return status in ("closed", "filled") or filled > 0


async def wait_for_breakout(exchange, settings: BotSettings, plan: ArmedPlan) -> Optional[str]:
    post_wait = settings.auto_post_wait_seconds if plan.event.provider in {"volatility_scanner", "manual_volatility"} else settings.post_event_wait_seconds
    deadline = plan.event.event_time_utc + timedelta(seconds=post_wait)
    mode = BotMode.VOLATILITY_ARMED.value if plan.event.provider in {"volatility_scanner", "manual_volatility"} else BotMode.CALENDAR_ARMED.value
    db.set_runtime_state(mode, True, f"armed traps: {plan.event.title}")
    symbol = ccxt_symbol(settings.symbol)
    while utc_now() <= deadline and not engine_stop.is_set():
        try:
            buy = await exchange.fetch_order(plan.buy_order_id, symbol)
            sell = await exchange.fetch_order(plan.sell_order_id, symbol)
            buy_filled = infer_filled(buy)
            sell_filled = infer_filled(sell)
            if buy_filled and sell_filled:
                db.log_event("critical", "double_fill_detected", "Both breakout traps filled; emergency flatten started", {
                    "buy_order_id": plan.buy_order_id,
                    "sell_order_id": plan.sell_order_id,
                    "buy": buy,
                    "sell": sell,
                })
                db.update_trade_by_client_oid(plan.buy_client_oid, {"status": "double_filled"})
                db.update_trade_by_client_oid(plan.sell_client_oid, {"status": "double_filled"})
                if settings.double_fill_emergency_flatten:
                    try:
                        await cancel_all_safely(exchange, settings.symbol)
                        await flatten_symbol_positions(exchange, settings.symbol, settings.hedge_mode)
                        db.log_event("critical", "double_fill_flattened", "Emergency flatten attempted after double fill", {})
                    except Exception as inner:
                        db.log_event("critical", "double_fill_flatten_failed", f"Emergency flatten failed after double fill: {inner}", {})
                return None
            if buy_filled:
                await cancel_safely(exchange, plan.sell_order_id, settings.symbol)
                db.update_trade_by_client_oid(plan.buy_client_oid, {"status": "active", "execution_price": buy.get("average") or buy.get("price")})
                db.update_trade_by_client_oid(plan.sell_client_oid, {"status": "cancelled"})
                return "long"
            if sell_filled:
                await cancel_safely(exchange, plan.buy_order_id, settings.symbol)
                db.update_trade_by_client_oid(plan.sell_client_oid, {"status": "active", "execution_price": sell.get("average") or sell.get("price")})
                db.update_trade_by_client_oid(plan.buy_client_oid, {"status": "cancelled"})
                return "short"
        except Exception as e:
            db.log_event("error", "watch_orders", f"Order watch error: {e}", {})
        await asyncio.sleep(settings.order_watch_interval_seconds)

    await cancel_safely(exchange, plan.buy_order_id, settings.symbol)
    await cancel_safely(exchange, plan.sell_order_id, settings.symbol)
    db.update_trade_by_client_oid(plan.buy_client_oid, {"status": "expired"})
    db.update_trade_by_client_oid(plan.sell_client_oid, {"status": "expired"})
    db.log_event("info", "no_breakout", "Window passed without valid breakout; traps cancelled", {"event": plan.event.title})
    return None


async def attach_exchange_protection(exchange, settings: BotSettings, direction: str, amount: float, entry: float, stop_distance: float) -> Dict[str, Any]:
    if direction == "long":
        sl = price_precision(exchange, settings.symbol, entry - stop_distance)
        tp2 = price_precision(exchange, settings.symbol, entry + stop_distance * settings.tp2_r)
    else:
        sl = price_precision(exchange, settings.symbol, entry + stop_distance)
        tp2 = price_precision(exchange, settings.symbol, entry - stop_distance * settings.tp2_r)

    out: Dict[str, Any] = {"sl": sl, "tp2": tp2, "exchange_sl": None, "exchange_tp2": None}
    try:
        out["exchange_sl"] = await place_reduce_trigger(exchange, settings.symbol, direction, amount, sl, "stop_loss", f"vhs-sl-{uuid.uuid4().hex[:10]}", settings.hedge_mode)
        out["exchange_sl_id"] = extract_order_id(out["exchange_sl"])
    except Exception as e:
        db.log_event("error", "protect_sl_failed", f"Exchange SL failed; manual guard active: {e}", {"sl": sl})
    if settings.tp2_enabled:
        try:
            out["exchange_tp2"] = await place_reduce_trigger(exchange, settings.symbol, direction, amount, tp2, "take_profit", f"vhs-tp-{uuid.uuid4().hex[:10]}", settings.hedge_mode)
        except Exception as e:
            db.log_event("error", "protect_tp_failed", f"Exchange TP failed; manual guard active: {e}", {"tp2": tp2})

    # V7: TP without SL is dangerous. If SL failed, remove TP so the bot does not leave
    # a one-sided orphan protection order after emergency flatten.
    if not out.get("exchange_sl") and out.get("exchange_tp2") and settings.cancel_tp_if_sl_fails:
        try:
            await cancel_safely(exchange, str(out["exchange_tp2"].get("id") or out["exchange_tp2"].get("orderId")), settings.symbol)
            db.log_event("warning", "orphan_tp_cancelled", "TP cancelled because exchange SL was not confirmed", {"tp2": tp2})
        except Exception as e:
            db.log_event("error", "orphan_tp_cancel_failed", f"Could not cancel TP after SL failure: {e}", {})
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
        db.log_event("info", "trailing_sl_updated", "Exchange SL moved by trailing manager", {"new_stop": new_stop, "old_order_id": old_order_id, "new_order_id": new_id})
        return new_id
    except Exception as e:
        db.log_event("error", "trailing_sl_update_failed", f"Could not update exchange trailing SL: {e}", {"new_stop": new_stop})
        return old_order_id

async def manage_position(exchange, settings: BotSettings, plan: ArmedPlan, direction: str) -> None:
    db.set_runtime_state(BotMode.IN_TRADE.value, True, f"{direction} active")
    entry = await get_last_price(exchange, settings.symbol)
    side_close = "sell" if direction == "long" else "buy"
    remaining = plan.amount
    tp1_done = False
    trailing_active = False
    last_stop_sent = stop_price
    last_stop_update_at = 0.0
    current_sl_order_id: Optional[str] = None
    stop_price = entry - plan.stop_distance if direction == "long" else entry + plan.stop_distance
    tp1_price = entry + plan.stop_distance * settings.tp1_r if direction == "long" else entry - plan.stop_distance * settings.tp1_r
    tp2_price = entry + plan.stop_distance * settings.tp2_r if direction == "long" else entry - plan.stop_distance * settings.tp2_r
    best_price = entry
    started = time.time()
    last_protection = await attach_exchange_protection(exchange, settings, direction, remaining, entry, plan.stop_distance)
    current_sl_order_id = last_protection.get("exchange_sl_id")

    # V7 live guard: if the exchange did not confirm a real SL, do not keep the position alive.
    # Manual polling is a backup, not a substitute for an exchange-side stop during news volatility.
    if settings.hard_exchange_sl_required and not last_protection.get("exchange_sl"):
        active_oid = plan.buy_client_oid if direction == "long" else plan.sell_client_oid
        msg = "Exchange-side SL was not confirmed; position will be flattened immediately"
        db.log_event("critical", "no_exchange_sl_flatten", msg, {
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
                db.update_trade_by_client_oid(active_oid, {
                    "status": "closed",
                    "pnl": pnl - fee_est,
                    "close_price": px,
                    "close_reason": "no_exchange_sl",
                    "meta": {"entry": entry, "last": px, "gross_pnl": pnl, "fee_est": fee_est, "score": plan.metrics.get("volatility_score")},
                })
                db.set_runtime_state(BotMode.PAUSED.value, False, "paused: exchange SL failed and position was flattened")
                return
            except Exception as e:
                db.log_event("critical", "no_exchange_sl_flatten_failed", f"Could not flatten after SL failure: {e}", {})
                if settings.emergency_flatten_on_error:
                    try:
                        await flatten_symbol_positions(exchange, settings.symbol, settings.hedge_mode)
                    except Exception as inner:
                        db.log_event("critical", "symbol_flatten_failed", f"Symbol flatten failed: {inner}", {})
                return

    db.log_event("warning", "position_active", "Breakout position is active", {
        "direction": direction,
        "entry": entry,
        "amount": remaining,
        "sl": stop_price,
        "tp1": tp1_price,
        "tp2": tp2_price,
        "exchange_protection": bool(last_protection.get("exchange_sl")),
        "score": plan.metrics.get("volatility_score"),
    })

    realized = 0.0
    try:
        while not engine_stop.is_set():
            price = await get_last_price(exchange, settings.symbol)
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

            elapsed = time.time() - started
            stale_exit = elapsed >= settings.stale_trade_exit_seconds and r < settings.stale_trade_min_r
            timeout_exit = elapsed >= settings.hard_timeout_seconds

            # Pro V8: move real exchange-side stop, not only local stop.
            stop_step = max(float(plan.metrics.get("atr14", 0) or 0) * settings.trailing_min_step_atr, settings.trailing_min_step_usd)
            stop_moved_enough = abs(stop_price - last_stop_sent) >= stop_step
            if (trailing_active or r >= settings.breakeven_after_r) and stop_moved_enough and (time.time() - last_stop_update_at) >= settings.trailing_update_interval_seconds:
                current_sl_order_id = await update_exchange_stop(exchange, settings, direction, remaining, stop_price, current_sl_order_id)
                last_stop_sent = stop_price
                last_stop_update_at = time.time()

            if settings.tp1_enabled and settings.tp1_close_pct > 0 and hit_tp1 and not tp1_done:
                close_amount = order_amount_precision(exchange, settings.symbol, remaining * settings.tp1_close_pct)
                if close_amount > 0:
                    await close_position_market(exchange, settings.symbol, side_close, close_amount, direction if settings.hedge_mode else None)
                    pnl = (price - entry) * close_amount if direction == "long" else (entry - price) * close_amount
                    realized += pnl
                    remaining = order_amount_precision(exchange, settings.symbol, remaining - close_amount)
                    tp1_done = True
                    stop_price = entry
                    db.log_event("info", "tp1", "TP1 partial profit taken; stop moved to breakeven", {"price": price, "pnl": pnl, "remaining": remaining})

            exit_by_tp2 = bool(settings.tp2_enabled and hit_tp2)
            if hit_stop or exit_by_tp2 or stale_exit or timeout_exit:
                reason = "trailing_stop" if hit_stop and trailing_active else "stop" if hit_stop else "tp2" if exit_by_tp2 else "stale_exit" if stale_exit else "timeout"
                if remaining > 0:
                    await close_position_market(exchange, settings.symbol, side_close, remaining, direction if settings.hedge_mode else None)
                    pnl = (price - entry) * remaining if direction == "long" else (entry - price) * remaining
                    realized += pnl
                fee_est = plan.notional * 0.0008 * 2
                net_pnl = realized - fee_est
                active_oid = plan.buy_client_oid if direction == "long" else plan.sell_client_oid
                db.update_trade_by_client_oid(active_oid, {
                    "status": "closed",
                    "pnl": net_pnl,
                    "close_price": price,
                    "close_reason": reason,
                    "meta": {"entry": entry, "last": price, "gross_pnl": realized, "fee_est": fee_est, "tp1_done": tp1_done, "trailing_active": trailing_active, "last_stop": stop_price, "score": plan.metrics.get("volatility_score")},
                })
                db.log_event("warning" if net_pnl < 0 else "info", "position_closed", f"Position closed: {reason}", {"net_pnl": net_pnl, "price": price})
                break

            await asyncio.sleep(settings.poll_interval_seconds)
    except Exception as e:
        db.log_event("error", "manage_position_error", f"Position manager error: {e}", {})
        if settings.emergency_flatten_on_error and remaining > 0:
            try:
                await close_position_market(exchange, settings.symbol, side_close, remaining, direction if settings.hedge_mode else None)
                db.log_event("warning", "emergency_flatten", "Emergency flatten executed after manager error", {"direction": direction, "amount": remaining})
            except Exception as inner:
                db.log_event("critical", "emergency_flatten_failed", f"Emergency flatten failed: {inner}", {})


async def prepare_and_trade_event(settings: BotSettings, event: NewsEvent) -> None:
    mode = BotMode.VOLATILITY_ARMED.value if event.provider in {"volatility_scanner", "manual_volatility"} else BotMode.CALENDAR_ARMED.value
    db.upsert_news_event(event)
    db.mark_event_status(event.provider_id, "arming", "Preparing trigger traps")
    exchange = await get_exchange()
    try:
        ok, reason = daily_limits_ok(settings, db.todays_pnl(), db.todays_trade_count(), db.consecutive_losses())
        if not ok:
            raise RuntimeError(reason)

        plan = await build_armed_plan(exchange, settings, event)
        await place_armed_orders(exchange, settings, plan)
        direction = await wait_for_breakout(exchange, settings, plan)
        if direction:
            await manage_position(exchange, settings, plan, direction)
            db.mark_event_status(event.provider_id, "traded", f"Breakout direction: {direction}")
        else:
            db.mark_event_status(event.provider_id, "no_trade", "No breakout in allowed window")
    except Exception as e:
        db.log_event("error", "trade_failed", f"Trade failed: {e}", {"event": event.model_dump(mode="json")})
        db.mark_event_status(event.provider_id, "failed", str(e))
        try:
            await cancel_all_safely(exchange, settings.symbol)
        except Exception:
            pass
    finally:
        await exchange.close()
        if not engine_stop.is_set():
            db.set_runtime_state(mode, True, "waiting for next opportunity")


async def maybe_auto_arm_volatility(settings: BotSettings) -> bool:
    global _last_vol_arm_at, _last_hot_log_at
    if not settings.volatility_auto_enabled:
        return False
    if _last_vol_arm_at and (utc_now() - _last_vol_arm_at).total_seconds() < settings.volatility_cooldown_minutes * 60:
        return False

    scan = await analyze_markets(settings) if settings.scan_symbols else {"best": await analyze_market(settings.symbol, settings), "markets": []}
    market = scan.get("best") or {}
    selected_symbol = market.get("symbol") or settings.symbol
    score = float(market.get("volatility_score") or 0)
    state = str(market.get("state") or "COLD")
    db.set_runtime_state(BotMode.VOLATILITY_SCAN.value, True, f"best {selected_symbol} score {score:.1f} / {state}")

    if score >= settings.notify_score:
        if not _last_hot_log_at or (utc_now() - _last_hot_log_at).total_seconds() > 60:
            db.log_event("info", "multi_symbol_volatility_watch", f"Best market {selected_symbol} score {score:.1f}: {state}", {"best": market, "top": (scan.get("markets") or [])[:5]})
            _last_hot_log_at = utc_now()

    if score >= settings.auto_arm_score and market.get("valid_for_sniper"):
        ok, reason = daily_limits_ok(settings, db.todays_pnl(), db.todays_trade_count(), db.consecutive_losses())
        if not ok:
            db.log_event("warning", "auto_arm_blocked", reason, market)
            return False
        if settings.trade_selected_symbol and selected_symbol != settings.symbol:
            settings = settings.model_copy(update={"symbol": selected_symbol})
        event = synthetic_event(
            "volatility_scanner",
            settings.auto_arm_delay_seconds,
            f"AUTO VOLATILITY HUNT {selected_symbol} score={score:.1f}",
            raw={"market": market, "scan": scan},
        )
        _last_vol_arm_at = utc_now()
        await prepare_and_trade_event(settings, event)
        return True
    return False


async def engine_loop() -> None:
    db.set_runtime_state(BotMode.HYBRID_SCAN.value, True, "hybrid engine started")
    last_sync = datetime.fromtimestamp(0, tz=timezone.utc)
    while not engine_stop.is_set():
        try:
            settings = db.get_settings()
            if not settings.calendar_enabled and not settings.volatility_auto_enabled:
                db.set_runtime_state(BotMode.PAUSED.value, False, "calendar and volatility scanner disabled")
                await asyncio.sleep(10)
                continue

            ok, reason = daily_limits_ok(settings, db.todays_pnl(), db.todays_trade_count(), db.consecutive_losses())
            if not ok:
                db.set_runtime_state(BotMode.PAUSED.value, False, reason)
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
                    db.set_runtime_state(BotMode.CALENDAR_ARMED.value, True, f"arming news soon: {selected.title} in {seconds_to_event:.0f}s")
                    await asyncio.sleep(max(0.2, min(5, seconds_to_event - 1)))
                await prepare_and_trade_event(settings, selected)
                await asyncio.sleep(settings.event_cooldown_minutes * 60)
                continue

            armed = await maybe_auto_arm_volatility(settings)
            if not armed:
                db.set_runtime_state(BotMode.HYBRID_SCAN.value, True, "no news; scanning volatility")
                await asyncio.sleep(settings.scan_interval_seconds)
        except asyncio.CancelledError:
            break
        except Exception as e:
            db.log_event("error", "engine_loop", f"Engine loop error: {e}", {})
            await asyncio.sleep(5)
    db.set_runtime_state(BotMode.OFF.value, False, "engine stopped")


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
    exchange = await get_exchange()
    try:
        await cancel_all_safely(exchange, settings.symbol)
        if settings.kill_switch_closes_positions:
            try:
                await flatten_symbol_positions(exchange, settings.symbol, settings.hedge_mode)
                db.log_event("warning", "kill_switch_flatten", "Kill switch attempted to flatten open symbol positions", {"symbol": settings.symbol})
            except Exception as e:
                db.log_event("critical", "kill_switch_flatten_failed", f"Kill switch flatten failed: {e}", {"symbol": settings.symbol})
    finally:
        await exchange.close()
    if engine_task:
        engine_task.cancel()
    await bitget_ws.stop()
    db.set_runtime_state(BotMode.OFF.value, False, "manual stop; all orders cancellation attempted")
    return {"status": "stopped", "message": "Engine stopped and open orders cancellation attempted."}


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
    # Temporarily use payload wait by patching event raw; wait_for_breakout uses settings auto wait for manual.
    asyncio.create_task(prepare_and_trade_event(settings, event))
    return {"status": "manual_volatility_armed", "event": event.model_dump(mode="json"), "market": market}
