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
        "subscribed_symbols": list(_state.subscribed_symbols),
        "ticker_count": len(_state.ticker),
        "orders_cached": len(_state.orders),
        "positions_cached": len(_state.positions),
    }


def get_ticker(symbol: str) -> Optional[Dict[str, Any]]:
    return _state.ticker.get(plain_symbol(symbol))


def _sign(timestamp: str, secret: str) -> str:
    payload = f"{timestamp}GET/user/verify".encode()
    digest = hmac.new(secret.encode(), payload, hashlib.sha256).digest()
    return base64.b64encode(digest).decode()


async def _heartbeat(ws):
    while not _stop.is_set():
        try:
            await asyncio.sleep(30)
            await ws.send("ping")
        except Exception:
            return


async def _public_loop(symbols: List[str], reconnect_seconds: float):
    args = []
    for sym in symbols:
        inst = plain_symbol(sym)
        args.append({"instType": INST_TYPE, "channel": "ticker", "instId": inst})
        args.append({"instType": INST_TYPE, "channel": "books1", "instId": inst})
    while not _stop.is_set():
        try:
            async with websockets.connect(PUBLIC_URL, ping_interval=None, close_timeout=3) as ws:
                _state.public_connected = True
                _state.subscribed_symbols = [plain_symbol(s) for s in symbols]
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                hb = asyncio.create_task(_heartbeat(ws))
                try:
                    async for msg in ws:
                        if msg == "pong":
                            _state.public_last_message_ts = time.time()
                            continue
                        _state.public_last_message_ts = time.time()
                        try:
                            data = json.loads(msg)
                        except Exception:
                            continue
                        arg = data.get("arg") or {}
                        inst = plain_symbol(arg.get("instId") or "")
                        ch = arg.get("channel")
                        payload = (data.get("data") or [{}])[0] if isinstance(data.get("data"), list) else {}
                        if not inst or not payload:
                            continue
                        cached = _state.ticker.setdefault(inst, {})
                        cached["ts"] = time.time()
                        cached["channel"] = ch
                        if ch == "ticker":
                            for k in ("lastPr", "last", "markPrice", "bidPr", "askPr", "bidPx", "askPx"):
                                if k in payload:
                                    cached[k] = payload[k]
                        elif ch == "books1":
                            bids = payload.get("bids") or []
                            asks = payload.get("asks") or []
                            if bids: cached["bidPr"] = bids[0][0]
                            if asks: cached["askPr"] = asks[0][0]
                finally:
                    hb.cancel()
        except Exception as e:
            _state.last_error = f"public_ws: {e}"
            _state.reconnects += 1
        finally:
            _state.public_connected = False
        await asyncio.sleep(reconnect_seconds)


async def _private_loop(symbols: List[str], reconnect_seconds: float):
    key = os.environ.get("BITGET_API_KEY") or ""
    secret = os.environ.get("BITGET_API_SECRET") or ""
    passphrase = os.environ.get("BITGET_API_PASSPHRASE") or os.environ.get("BITGET_API_PASSWORD") or ""
    if not key or not secret or not passphrase:
        _state.last_error = "private_ws: missing API key/secret/passphrase"
        return
    args = [
        {"instType": INST_TYPE, "channel": "orders", "instId": "default"},
        {"instType": INST_TYPE, "channel": "positions", "instId": "default"},
        {"instType": INST_TYPE, "channel": "account", "instId": "default"},
    ]
    while not _stop.is_set():
        try:
            async with websockets.connect(PRIVATE_URL, ping_interval=None, close_timeout=3) as ws:
                ts = str(int(time.time()))
                await ws.send(json.dumps({"op": "login", "args": [{"apiKey": key, "passphrase": passphrase, "timestamp": ts, "sign": _sign(ts, secret)}]}))
                login_msg = await asyncio.wait_for(ws.recv(), timeout=10)
                if '"event":"login"' not in login_msg or '"code":"0"' not in login_msg:
                    raise RuntimeError(f"login failed: {login_msg}")
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                _state.private_connected = True
                hb = asyncio.create_task(_heartbeat(ws))
                try:
                    async for msg in ws:
                        if msg == "pong":
                            _state.private_last_message_ts = time.time()
                            continue
                        _state.private_last_message_ts = time.time()
                        try:
                            data = json.loads(msg)
                        except Exception:
                            continue
                        arg = data.get("arg") or {}
                        ch = arg.get("channel")
                        payload = data.get("data") or []
                        if ch == "orders" and isinstance(payload, list):
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
