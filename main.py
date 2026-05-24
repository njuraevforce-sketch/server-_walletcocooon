from __future__ import annotations

import os
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware

import database as db
import bitget_ws
from calendar_client import fetch_calendar, filter_events_for_crypto
from models import BotSettings, SettingsPayload, ManualEventPayload, ManualArmNowPayload
from risk import live_trading_allowed
from exchange_client import get_exchange, fetch_balance_usdt, get_spread_bps
from sniper_engine import (
    analyze_market,
    analyze_markets,
    manual_arm,
    manual_arm_now,
    start_engine,
    stop_engine,
    sync_calendar,
)

app = FastAPI(title="Volatility Hunter Sniper Bot", version="8.3.1-model-consistency-fix")

cors_origins = [x.strip() for x in os.environ.get("CORS_ORIGINS", "*").split(",") if x.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

API_AUTH_TOKEN = os.environ.get("API_AUTH_TOKEN", "")


async def require_auth(x_bot_token: Optional[str] = Header(default=None)):
    if API_AUTH_TOKEN and x_bot_token != API_AUTH_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Bot-Token")


@app.get("/")
async def health_check():
    return {"status": "online", "system": "Volatility Hunter Sniper Live Guard", "version": "8.3.1-model-consistency-fix"}


@app.get("/api/status", dependencies=[Depends(require_auth)])
async def status():
    settings = db.get_settings()
    live_allowed, live_reason = live_trading_allowed(settings)
    return {
        "status": "success",
        "runtime": db.get_runtime_state(),
        "settings": settings.model_dump(),
        "live_allowed": live_allowed,
        "live_reason": live_reason,
        "today_pnl": db.todays_pnl(),
        "today_trade_count": db.todays_trade_count(),
        "consecutive_losses": db.consecutive_losses(),
        "ws": bitget_ws.status(),
    }


@app.get("/api/preflight", dependencies=[Depends(require_auth)])
async def preflight():
    checks = {
        "database": False,
        "settings": False,
        "exchange": False,
        "balance": False,
        "spread": False,
    }
    errors = []

    try:
        settings = db.get_settings()
        checks["database"] = True
        checks["settings"] = True
    except Exception as e:
        return {
            "status": "error",
            "version": "8.3.1-model-consistency-fix",
            "stage": "database/settings",
            "checks": checks,
            "errors": [str(e)],
        }

    live_allowed, live_reason = live_trading_allowed(settings)
    exchange = None
    balance = 0.0
    spread_bps = 999.0

    try:
        exchange = await get_exchange()
        checks["exchange"] = True
    except Exception as e:
        return {
            "status": "error",
            "version": "8.3.1-model-consistency-fix",
            "stage": "exchange/load_markets",
            "checks": checks,
            "errors": [str(e)],
            "live_allowed": live_allowed,
            "live_reason": live_reason,
            "symbol": settings.symbol,
        }

    try:
        balance = await fetch_balance_usdt(exchange)
        checks["balance"] = True
    except Exception as e:
        errors.append(f"balance: {e}")

    try:
        spread_bps = await get_spread_bps(exchange, settings.symbol)
        checks["spread"] = True
    except Exception as e:
        errors.append(f"spread/orderbook: {e}")

    try:
        await exchange.close()
    except Exception:
        pass

    market_ok = spread_bps <= settings.max_spread_bps
    return {
        "status": "success" if not errors else "warning",
        "version": "8.3.1-model-consistency-fix",
        "checks": checks,
        "errors": errors,
        "live_allowed": live_allowed,
        "live_reason": live_reason,
        "symbol": settings.symbol,
        "balance_usdt": balance,
        "spread_bps": spread_bps,
        "market_ok": market_ok,
        "hard_exchange_sl_required": settings.hard_exchange_sl_required,
        "flatten_if_exchange_sl_fails": settings.flatten_if_exchange_sl_fails,
        "kill_switch_closes_positions": settings.kill_switch_closes_positions,
    }


@app.get("/api/settings", dependencies=[Depends(require_auth)])
async def get_settings_endpoint():
    return {"status": "success", "settings": db.get_settings().model_dump()}


@app.post("/api/settings", dependencies=[Depends(require_auth)])
async def update_settings(payload: SettingsPayload):
    try:
        current = db.get_settings().model_dump()
        current.update(payload.settings or {})
        validated = BotSettings(**current)
        row = db.update_settings(validated.model_dump())
        return {"status": "success", "settings": row}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/calendar/sync", dependencies=[Depends(require_auth)])
async def calendar_sync():
    settings = db.get_settings()
    try:
        events = await sync_calendar(settings)
        return {"status": "success", "count": len(events), "events": [e.model_dump(mode="json") for e in events[:25]]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/calendar/upcoming", dependencies=[Depends(require_auth)])
async def calendar_upcoming():
    return {"status": "success", "events": db.list_upcoming_events(limit=50)}


@app.get("/api/analyze", dependencies=[Depends(require_auth)])
async def analyze():
    settings = db.get_settings()
    try:
        market = await analyze_market(settings.symbol, settings)
        events = []
        if settings.calendar_enabled:
            raw_events = await fetch_calendar(days_ahead=3)
            events = filter_events_for_crypto(raw_events, settings)
        return {
            "status": "success",
            "market": market,
            "upcoming_events": [e.model_dump(mode="json") for e in events[:10]],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/volatility", dependencies=[Depends(require_auth)])
async def volatility():
    settings = db.get_settings()
    try:
        market = await analyze_market(settings.symbol, settings)
        return {"status": "success", "market": market}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/markets/scan", dependencies=[Depends(require_auth)])
async def markets_scan():
    settings = db.get_settings()
    try:
        return {"status": "success", **await analyze_markets(settings)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




@app.get("/api/volume-shock", dependencies=[Depends(require_auth)])
async def volume_shock():
    settings = db.get_settings()
    try:
        scan = await analyze_markets(settings)
        return {"status": "success", "best_shock": scan.get("best_shock"), "markets": scan.get("markets", []), "symbols": scan.get("symbols", [])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ws/status", dependencies=[Depends(require_auth)])
async def ws_status():
    return {"status": "success", "ws": bitget_ws.status()}


@app.post("/api/start", dependencies=[Depends(require_auth)])
async def start():
    try:
        return await start_engine()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/stop", dependencies=[Depends(require_auth)])
async def stop():
    try:
        return await stop_engine()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/manual-arm-event", dependencies=[Depends(require_auth)])
async def manual_arm_event_endpoint(payload: ManualEventPayload):
    try:
        return await manual_arm(payload.provider_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/arm-now", dependencies=[Depends(require_auth)])
async def manual_arm_now_endpoint(payload: ManualArmNowPayload):
    try:
        return await manual_arm_now(payload)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
