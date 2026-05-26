from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

import websockets

from exchange_client import plain_symbol

PUBLIC_URL = "wss://ws.bitget.com/v2/ws/public"
PRIVATE_URL = "wss://ws.bitget.com/v2/ws/private"
INST_TYPE = os.environ.get("BITGET_WS_INST_TYPE", "USDT-FUTURES")


@dataclass
class WSState:
    enabled: bool = False
    public_connected: bool = False
    private_connected: bool = False
    public_last_message_ts: float = 0.0
    private_last_message_ts: float = 0.0
    last_error: str = ""
    reconnects: int = 0
    subscribed_symbols: List[str] = field(default_factory=list)
    ticker: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # НОВЫЕ ПОЛЯ ДЛЯ ORDER FLOW (СТАКАН И ДЕЛЬТА)
    orderbook: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    cvd: Dict[str, float] = field(default_factory=dict) 
    
    orders: List[Dict[str, Any]] = field(default_factory=list)
    positions: List[Dict[str, Any]] = field(default_factory=list)


_state = WSState()
_public_task: Optional[asyncio.Task] = None
_private_task: Optional[asyncio.Task] = None
_stop = asyncio.Event()
_lock = asyncio.Lock()


def status() -> Dict[str, Any]:
    return {
        "enabled": _state.enabled,
        "public_connected": _state.public_connected,
        "private_connected": _state.private_connected,
        "public_last_message_age": round(time.time() - _state.public_last_message_ts, 3) if _state.public_last_message_ts else None,
        "private_last_message_age": round(time.time() - _state.private_last_message_ts, 3) if _state.private_last_message_ts else None,
        "last_error": _state.last_error,
        "reconnects": _state.reconnects,
        "subscribed_symbols": _state.subscribed_symbols,
        "ticker_count": len(_state.ticker),
        "orderbook_cached": len(_state.orderbook),
        "cvd_tracked": len(_state.cvd),
        "orders_cached": len(_state.orders),
        "positions_cached": len(_state.positions),
    }


def get_ticker(symbol: str) -> Optional[Dict[str, Any]]:
    sym = plain_symbol(symbol)
    return _state.ticker.get(sym)


def get_orderbook(symbol: str) -> Optional[Dict[str, Any]]:
    """Возвращает кэшированный стакан (bids и asks)."""
    sym = plain_symbol(symbol)
    return _state.orderbook.get(sym)


def get_cvd(symbol: str) -> float:
    """Возвращает кумулятивную дельту объемов (CVD) с момента запуска."""
    sym = plain_symbol(symbol)
    return _state.cvd.get(sym, 0.0)


async def _public_loop(symbols: List[str], reconnect_seconds: float):
    _state.subscribed_symbols = [plain_symbol(s) for s in symbols]
    subs = []
    for sym in _state.subscribed_symbols:
        subs.append({"instType": INST_TYPE, "channel": "ticker", "instId": sym})
        subs.append({"instType": INST_TYPE, "channel": "books15", "instId": sym}) # Стакан 15 уровней
        subs.append({"instType": INST_TYPE, "channel": "trade", "instId": sym})   # Лента сделок

    while not _stop.is_set():
        try:
            async with websockets.connect(PUBLIC_URL, ping_interval=20, ping_timeout=10) as ws:
                _state.public_connected = True
                _state.last_error = ""
                await ws.send(json.dumps({"op": "subscribe", "args": subs}))
                
                async def keepalive():
                    while not _stop.is_set():
                        await asyncio.sleep(20)
                        if ws.open:
                            await ws.send("ping")
                            
                hb = asyncio.create_task(keepalive())
                try:
                    async for msg in ws:
                        if _stop.is_set():
                            break
                        _state.public_last_message_ts = time.time()
                        if msg == "pong":
                            continue
                        data = json.loads(msg)
                        arg = data.get("arg") or {}
                        ch = arg.get("channel")
                        inst_id = arg.get("instId")
                        payload = data.get("data")
                        
                        if ch == "ticker" and payload and isinstance(payload, list):
                            _state.ticker[inst_id] = payload[0]
                            
                        elif ch == "books15" and payload and isinstance(payload, list):
                            # Сохраняем стакан
                            _state.orderbook[inst_id] = payload[0]
                            
                        elif ch == "trade" and payload and isinstance(payload, list):
                            # Высчитываем кумулятивную дельту (CVD)
                            if inst_id not in _state.cvd:
                                _state.cvd[inst_id] = 0.0
                            for t in payload:
                                side = str(t.get("side", "")).lower()
                                sz = float(t.get("size") or t.get("sz") or 0)
                                if side == "buy":
                                    _state.cvd[inst_id] += sz
                                elif side == "sell":
                                    _state.cvd[inst_id] -= sz
                finally:
                    hb.cancel()
        except Exception as e:
            _state.last_error = f"public_ws: {e}"
            _state.reconnects += 1
        finally:
            _state.public_connected = False
        await asyncio.sleep(reconnect_seconds)


def _api_creds():
    key = os.environ.get("BITGET_API_KEY") or ""
    secret = os.environ.get("BITGET_API_SECRET") or ""
    passphrase = os.environ.get("BITGET_API_PASSPHRASE") or os.environ.get("BITGET_API_PASSWORD") or ""
    return key, secret, passphrase


async def _private_loop(symbols: List[str], reconnect_seconds: float):
    key, secret, passphrase = _api_creds()
    if not key or not secret:
        return

    while not _stop.is_set():
        try:
            timestamp = str(int(time.time() * 1000))
            payload = f"{timestamp}GET/user/verify"
            sign = base64.b64encode(hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest()).decode()
            
            async with websockets.connect(PRIVATE_URL, ping_interval=20, ping_timeout=10) as ws:
                login_msg = {
                    "op": "login",
                    "args": [{"apiKey": key, "passphrase": passphrase, "timestamp": timestamp, "sign": sign}],
                }
                await ws.send(json.dumps(login_msg))
                
                resp = await ws.recv()
                if "login" not in resp:
                    raise RuntimeError(f"WS login failed: {resp}")

                _state.private_connected = True
                _state.last_error = ""
                
                subs = [
                    {"instType": INST_TYPE, "channel": "orders", "instId": "default"},
                    {"instType": INST_TYPE, "channel": "orders-algo", "instId": "default"},
                    {"instType": INST_TYPE, "channel": "positions", "instId": "default"},
                ]
                await ws.send(json.dumps({"op": "subscribe", "args": subs}))

                async def keepalive():
                    while not _stop.is_set():
                        await asyncio.sleep(20)
                        if ws.open:
                            await ws.send("ping")
                            
                hb = asyncio.create_task(keepalive())
                try:
                    async for msg in ws:
                        if _stop.is_set():
                            break
                        _state.private_last_message_ts = time.time()
                        if msg == "pong":
                            continue
                        data = json.loads(msg)
                        arg = data.get("arg") or {}
                        ch = arg.get("channel")
                        payload = data.get("data") or []
                        if ch in ("orders", "orders-algo") and isinstance(payload, list):
                            _state.orders = (payload + _state.orders)[:100]
                        elif ch == "positions" and isinstance(payload, list):
                            _state.positions = payload
                finally:
                    hb.cancel()
        except Exception as e:
            _state.last_error = f"private_ws: {e}"
            _state.reconnects += 1
        finally:
            _state.private_connected = False
        await asyncio.sleep(reconnect_seconds)


async def start(symbols: Iterable[str], public_enabled: bool = True, private_enabled: bool = True, reconnect_seconds: float = 3.0):
    global _public_task, _private_task, _stop
    await stop()
    _stop = asyncio.Event()
    _state.enabled = True
    syms = list(dict.fromkeys([s for s in symbols if s]))
    if public_enabled:
        _public_task = asyncio.create_task(_public_loop(syms, reconnect_seconds))
    if private_enabled:
        _private_task = asyncio.create_task(_private_loop(syms, reconnect_seconds))


async def stop():
    global _public_task, _private_task
    _state.enabled = False
    _stop.set()
    for t in (_public_task, _private_task):
        if t and not t.done():
            t.cancel()
    _public_task = None
    _private_task = None
    _state.public_connected = False
    _state.private_connected = False
