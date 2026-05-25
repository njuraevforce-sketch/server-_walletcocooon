from __future__ import annotations

import os
import time
import json
import base64
import hashlib
import hmac
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import httpx
import ccxt.async_support as ccxt


def ccxt_symbol(symbol: str) -> str:
    """Return CCXT unified Bitget USDT-margined swap symbol.

    Bitget swap symbols in CCXT are usually formatted like BTC/USDT:USDT.
    The previous v7 formatter produced BTCUSDT:USDT, which can trigger
    BadSymbol / market not found errors on preflight and analyze.
    """
    s = str(symbol or "BTCUSDT").strip().upper()
    if "/" in s and ":" in s:
        return s
    if "/" in s and s.endswith("/USDT"):
        return f"{s}:USDT"
    s = s.replace("/", "").replace(":USDT", "")
    if s.endswith("USDT"):
        base = s[:-4]
        return f"{base}/USDT:USDT"
    return s


def plain_symbol(symbol: str) -> str:
    return symbol.replace("/", "").replace(":USDT", "").upper()


BITGET_REST_URL = os.environ.get("BITGET_REST_URL", "https://api.bitget.com")
PRODUCT_TYPE = os.environ.get("BITGET_PRODUCT_TYPE", "USDT-FUTURES")
MARGIN_COIN = os.environ.get("BITGET_MARGIN_COIN", "USDT")


def _fmt_num(value: float) -> str:
    """Format numbers for Bitget without Python float noise.

    Bitget validates price scale strictly. A value that is logically 77173.7
    can become 77173.699999999997 after converting CCXT precision strings to
    float. Sending that raw value causes Bitget 40808 checkBDScale errors.
    We round to a safe 8-decimal string and trim trailing zeros. BTC/ETH/SOL
    USDT futures use fewer decimals than this, while order sizes still keep
    enough precision.
    """
    try:
        d = Decimal(str(value))
        if not d.is_finite():
            return "0"
        d = d.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
        text = format(d, "f").rstrip("0").rstrip(".")
        if text in ("", "-0"):
            return "0"
        return text
    except (InvalidOperation, ValueError, TypeError):
        text = f"{float(value):.8f}".rstrip("0").rstrip(".")
        return text if text and text != "-0" else "0"


def _api_creds() -> Tuple[str, str, str]:
    key = os.environ.get("BITGET_API_KEY") or ""
    secret = os.environ.get("BITGET_API_SECRET") or ""
    passphrase = os.environ.get("BITGET_API_PASSPHRASE") or os.environ.get("BITGET_API_PASSWORD") or ""
    if not key or not secret or not passphrase:
        raise RuntimeError("Missing BITGET_API_KEY / BITGET_API_SECRET / BITGET_API_PASSPHRASE")
    return key, secret, passphrase


def _sign_rest(timestamp: str, method: str, path: str, body: str, secret: str) -> str:
    payload = f"{timestamp}{method.upper()}{path}{body}".encode()
    digest = hmac.new(secret.encode(), payload, hashlib.sha256).digest()
    return base64.b64encode(digest).decode()


async def bitget_private_request(method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Direct Bitget V2 REST request.

    We use this for futures trigger/TP-SL plan orders because CCXT's generic
    triggerPrice mapping can send legacy/deprecated parameters to Bitget and
    Bitget rejects them with 43011 delegateType errors.
    """
    key, secret, passphrase = _api_creds()
    method_u = method.upper()
    body = json.dumps(payload or {}, separators=(",", ":")) if method_u != "GET" else ""
    timestamp = str(int(time.time() * 1000))
    headers = {
        "ACCESS-KEY": key,
        "ACCESS-SIGN": _sign_rest(timestamp, method_u, path, body, secret),
        "ACCESS-TIMESTAMP": timestamp,
        "ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
        "locale": "en-US",
    }
    async with httpx.AsyncClient(timeout=12) as client:
        if method_u == "GET":
            resp = await client.get(f"{BITGET_REST_URL}{path}", headers=headers)
        else:
            resp = await client.request(method_u, f"{BITGET_REST_URL}{path}", headers=headers, content=body)
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text, "status_code": resp.status_code}
    if resp.status_code >= 400 or str(data.get("code")) not in ("00000", "0"):
        raise RuntimeError(f"bitget {data}")
    return data


async def bitget_place_plan_order(
    symbol: str,
    side: str,
    trade_side: str,
    amount: float,
    trigger_price: float,
    client_oid: str,
    isolated: bool = True,
    reduce_only: bool = False,
    stop_loss_price: Optional[float] = None,
) -> Dict[str, Any]:
    stop_loss = _fmt_num(stop_loss_price) if stop_loss_price is not None and float(stop_loss_price) > 0 else ""
    payload = {
        "planType": "normal_plan",
        "symbol": plain_symbol(symbol),
        "productType": PRODUCT_TYPE,
        "marginMode": "isolated" if isolated else "crossed",
        "marginCoin": MARGIN_COIN,
        "size": _fmt_num(amount),
        "price": "",
        "callbackRatio": "",
        "triggerPrice": _fmt_num(trigger_price),
        "triggerType": "fill_price",
        "side": side,
        "tradeSide": trade_side,
        "orderType": "market",
        "clientOid": client_oid,
        "reduceOnly": "YES" if reduce_only else "NO",
        "presetStopSurplusPrice": "",
        "stopSurplusTriggerPrice": "",
        "stopSurplusExecutePrice": "",
        "stopSurplusTriggerType": "",
        "presetStopLossPrice": "",
        "stopLossTriggerPrice": stop_loss,
        "stopLossExecutePrice": "",
        "stopLossTriggerType": "fill_price" if stop_loss else "",
    }
    data = await bitget_private_request("POST", "/api/v2/mix/order/place-plan-order", payload)
    out = data.get("data") or {}
    return {
        "id": str(out.get("orderId") or client_oid),
        "orderId": str(out.get("orderId") or ""),
        "clientOid": str(out.get("clientOid") or client_oid),
        "status": "open",
        "info": data,
        "triggerPrice": trigger_price,
        "presetStopLossPrice": stop_loss_price,
        "side": side,
        "amount": amount,
        "type": "bitget_v2_plan_market",
    }


async def bitget_place_tpsl_order(symbol: str, direction: str, amount: float, trigger_price: float, kind: str, client_oid: str) -> Dict[str, Any]:
    plan_type = "loss_plan" if kind == "stop_loss" else "profit_plan"
    # One-way mode holdSide: buy = long position, sell = short position.
    hold_side = "buy" if direction == "long" else "sell"
    payload = {
        "marginCoin": MARGIN_COIN,
        "productType": PRODUCT_TYPE,
        "symbol": plain_symbol(symbol),
        "planType": plan_type,
        "triggerPrice": _fmt_num(trigger_price),
        "triggerType": "fill_price",
        "executePrice": "0",
        "holdSide": hold_side,
        "size": _fmt_num(amount),
        "rangeRate": "",
        "clientOid": client_oid,
    }
    data = await bitget_private_request("POST", "/api/v2/mix/order/place-tpsl-order", payload)
    out = data.get("data") or {}
    return {
        "id": str(out.get("orderId") or client_oid),
        "orderId": str(out.get("orderId") or ""),
        "clientOid": str(out.get("clientOid") or client_oid),
        "status": "open",
        "info": data,
        "triggerPrice": trigger_price,
        "side": "sell" if direction == "long" else "buy",
        "amount": amount,
        "type": f"bitget_v2_{plan_type}",
    }


async def cancel_plan_safely(order_id_or_client_oid: Optional[str], symbol: str) -> None:
    if not order_id_or_client_oid:
        return
    item = {"orderId": "", "clientOid": ""}
    oid = str(order_id_or_client_oid)
    # If it starts with our client prefix, cancel by clientOid. Otherwise try orderId.
    if oid.startswith("vhs-"):
        item["clientOid"] = oid
    else:
        item["orderId"] = oid
    payload = {
        "orderIdList": [item],
        "symbol": plain_symbol(symbol),
        "productType": PRODUCT_TYPE,
        "marginCoin": MARGIN_COIN,
    }
    try:
        await bitget_private_request("POST", "/api/v2/mix/order/cancel-plan-order", payload)
    except Exception:
        # Try by the other key as fallback.
        alt = {"orderId": "", "clientOid": ""}
        if item["orderId"]:
            alt["clientOid"] = oid
        else:
            alt["orderId"] = oid
        payload["orderIdList"] = [alt]
        try:
            await bitget_private_request("POST", "/api/v2/mix/order/cancel-plan-order", payload)
        except Exception:
            pass


async def cancel_all_plan_safely(symbol: str) -> None:
    payload = {
        "symbol": plain_symbol(symbol),
        "productType": PRODUCT_TYPE,
        "marginCoin": MARGIN_COIN,
    }
    try:
        await bitget_private_request("POST", "/api/v2/mix/order/cancel-plan-order", payload)
    except Exception:
        pass


async def fetch_symbol_position(exchange, symbol: str) -> Dict[str, Any]:
    sym = ccxt_symbol(symbol)
    try:
        positions = await exchange.fetch_positions([sym])
    except Exception:
        positions = []
    for pos in positions or []:
        raw = pos.get("info") or {}
        contracts = (
            pos.get("contracts")
            or raw.get("total")
            or raw.get("available")
            or raw.get("holdSideSize")
            or raw.get("openDelegateSize")
            or 0
        )
        try:
            amount = abs(float(contracts or 0))
        except Exception:
            amount = 0.0
        if amount <= 0:
            continue
        side = str(pos.get("side") or raw.get("holdSide") or raw.get("posSide") or "").lower()
        if side in ("buy", "long"):
            direction = "long"
        elif side in ("sell", "short"):
            direction = "short"
        else:
            signed = float(pos.get("contracts") or pos.get("amount") or 0)
            direction = "long" if signed > 0 else "short"
        entry = float(pos.get("entryPrice") or raw.get("openPriceAvg") or raw.get("averageOpenPrice") or 0)
        return {"amount": amount, "direction": direction, "entry": entry, "raw": pos}
    return {"amount": 0.0, "direction": None, "entry": 0.0, "raw": None}


async def get_exchange():
    ex = ccxt.bitget({
        "apiKey": os.environ.get("BITGET_API_KEY"),
        "secret": os.environ.get("BITGET_API_SECRET"),
        "password": os.environ.get("BITGET_API_PASSPHRASE"),
        "enableRateLimit": True,
        "options": {
            "defaultType": "swap",
            "defaultSubType": "linear",
        },
    })
    if os.environ.get("EXCHANGE_SANDBOX", "false").lower() == "true":
        ex.set_sandbox_mode(True)
    await ex.load_markets()
    return ex


async def fetch_balance_usdt(exchange) -> float:
    try:
        bal = await exchange.fetch_balance({"type": "swap"})
        total = bal.get("USDT", {}).get("total") or bal.get("total", {}).get("USDT")
        free = bal.get("USDT", {}).get("free") or bal.get("free", {}).get("USDT")
        return float(total or free or 0)
    except Exception:
        return 0.0


async def configure_symbol(exchange, symbol: str, leverage: float, isolated: bool) -> None:
    sym = ccxt_symbol(symbol)
    try:
        await exchange.set_margin_mode("isolated" if isolated else "cross", sym)
    except Exception:
        pass
    try:
        await exchange.set_leverage(int(leverage), sym)
    except Exception:
        pass


def order_amount_precision(exchange, symbol: str, amount: float) -> float:
    try:
        return float(exchange.amount_to_precision(ccxt_symbol(symbol), amount))
    except Exception:
        return round(float(amount), 6)


def price_precision(exchange, symbol: str, price: float) -> float:
    try:
        return float(exchange.price_to_precision(ccxt_symbol(symbol), price))
    except Exception:
        return round(float(price), 2)


async def cancel_safely(exchange, order_id: Optional[str], symbol: str) -> None:
    if not order_id:
        return
    try:
        await exchange.cancel_order(order_id, ccxt_symbol(symbol))
        return
    except Exception:
        pass
    await cancel_plan_safely(order_id, symbol)


async def cancel_all_safely(exchange, symbol: str) -> None:
    try:
        await exchange.cancel_all_orders(ccxt_symbol(symbol))
    except Exception:
        pass
    await cancel_all_plan_safely(symbol)


async def close_position_market(exchange, symbol: str, side_to_close: str, amount: float, pos_side: Optional[str] = None) -> Dict[str, Any]:
    # side_to_close: 'sell' to close long, 'buy' to close short.
    params: Dict[str, Any] = {"reduceOnly": True}
    if pos_side:
        params["positionSide"] = pos_side
        params["posSide"] = pos_side
    return await exchange.create_order(ccxt_symbol(symbol), "market", side_to_close, amount, None, params)


async def place_trigger_entry(exchange, symbol: str, direction: str, amount: float, trigger_price: float, client_oid: str, hedge_mode: bool, isolated: bool = True, stop_loss_price: Optional[float] = None) -> Dict[str, Any]:
    # Use Bitget V2 plan order directly. CCXT generic trigger orders can send
    # legacy Bitget parameters and cause 43011 delegateType errors.
    side = "buy" if direction == "long" else "sell"
    trade_side = "open"
    return await bitget_place_plan_order(symbol, side, trade_side, amount, trigger_price, client_oid, isolated=isolated, reduce_only=False, stop_loss_price=stop_loss_price)


async def place_reduce_trigger(exchange, symbol: str, direction: str, amount: float, trigger_price: float, kind: str, client_oid: str, hedge_mode: bool) -> Dict[str, Any]:
    # Use Bitget V2 TPSL plan orders directly for exchange-side protection.
    return await bitget_place_tpsl_order(symbol, direction, amount, trigger_price, kind, client_oid)


async def get_last_price(exchange, symbol: str) -> float:
    try:
        from bitget_ws import get_ticker
        cached = get_ticker(symbol)
        if cached:
            raw = cached.get("lastPr") or cached.get("last") or cached.get("markPrice")
            if raw:
                return float(raw)
    except Exception:
        pass
    ticker = await exchange.fetch_ticker(ccxt_symbol(symbol))
    return float(ticker.get("last") or ticker.get("close") or 0)


async def get_spread_bps(exchange, symbol: str) -> float:
    try:
        from bitget_ws import get_ticker
        cached = get_ticker(symbol)
        if cached:
            bid = float(cached.get("bidPr") or cached.get("bidPx") or 0)
            ask = float(cached.get("askPr") or cached.get("askPx") or 0)
            if bid > 0 and ask > 0:
                mid = (bid + ask) / 2
                return ((ask - bid) / mid) * 10000
    except Exception:
        pass
    ob = await exchange.fetch_order_book(ccxt_symbol(symbol), limit=5)
    bid = float(ob["bids"][0][0]) if ob.get("bids") else 0.0
    ask = float(ob["asks"][0][0]) if ob.get("asks") else 0.0
    if bid <= 0 or ask <= 0:
        return 999.0
    mid = (bid + ask) / 2
    return ((ask - bid) / mid) * 10000


async def fetch_ohlcv(exchange, symbol: str, timeframe: str = "1m", limit: int = 120) -> List[List[float]]:
    return await exchange.fetch_ohlcv(ccxt_symbol(symbol), timeframe=timeframe, limit=limit)


async def flatten_symbol_positions(exchange, symbol: str, hedge_mode: bool = False) -> List[Dict[str, Any]]:
    """Best-effort emergency flatten for the configured symbol only.
    This is intentionally conservative: it only acts on non-zero positions returned by the exchange.
    """
    results: List[Dict[str, Any]] = []
    sym = ccxt_symbol(symbol)
    try:
        positions = await exchange.fetch_positions([sym])
    except Exception:
        positions = []

    for pos in positions or []:
        try:
            raw = pos.get("info") or {}
            contracts = (
                pos.get("contracts")
                or pos.get("contractSize")
                or raw.get("total")
                or raw.get("available")
                or raw.get("holdSideSize")
                or 0
            )
            amount = abs(float(contracts or 0))
            if amount <= 0:
                continue
            side = str(pos.get("side") or raw.get("holdSide") or raw.get("posSide") or "").lower()
            if not side:
                # If CCXT gives signed contracts/amount, infer direction.
                signed = float(pos.get("contracts") or pos.get("amount") or 0)
                side = "long" if signed > 0 else "short"
            close_side = "sell" if side == "long" else "buy"
            pos_side = side if hedge_mode else None
            results.append(await close_position_market(exchange, symbol, close_side, amount, pos_side))
        except Exception as e:
            results.append({"error": str(e), "position": pos})
    return results


async def fetch_order_safely(exchange, order_id: str, symbol: str) -> Dict[str, Any]:
    return await exchange.fetch_order(order_id, ccxt_symbol(symbol))


def extract_order_id(order: Optional[Dict[str, Any]]) -> Optional[str]:
    if not order:
        return None
    return str(order.get("id") or order.get("orderId") or order.get("clientOid") or "") or None

