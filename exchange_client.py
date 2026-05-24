from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

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
    except Exception:
        pass


async def cancel_all_safely(exchange, symbol: str) -> None:
    try:
        await exchange.cancel_all_orders(ccxt_symbol(symbol))
    except Exception:
        pass


async def close_position_market(exchange, symbol: str, side_to_close: str, amount: float, pos_side: Optional[str] = None) -> Dict[str, Any]:
    # side_to_close: 'sell' to close long, 'buy' to close short.
    params: Dict[str, Any] = {"reduceOnly": True}
    if pos_side:
        params["positionSide"] = pos_side
        params["posSide"] = pos_side
    return await exchange.create_order(ccxt_symbol(symbol), "market", side_to_close, amount, None, params)


async def place_trigger_entry(exchange, symbol: str, direction: str, amount: float, trigger_price: float, client_oid: str, hedge_mode: bool) -> Dict[str, Any]:
    # direction: 'long' or 'short'
    side = "buy" if direction == "long" else "sell"
    params: Dict[str, Any] = {
        "triggerPrice": trigger_price,
        "clientOid": client_oid,
        "reduceOnly": False,
    }
    if hedge_mode:
        params["positionSide"] = direction
        params["posSide"] = direction
    # CCXT maps triggerPrice to Bitget strategy/plan order for supported versions.
    return await exchange.create_order(ccxt_symbol(symbol), "market", side, amount, None, params)


async def place_reduce_trigger(exchange, symbol: str, direction: str, amount: float, trigger_price: float, kind: str, client_oid: str, hedge_mode: bool) -> Dict[str, Any]:
    # direction is the open position direction. For long, reducing side is sell. For short, reducing side is buy.
    side = "sell" if direction == "long" else "buy"
    params: Dict[str, Any] = {
        "clientOid": client_oid,
        "reduceOnly": True,
    }
    if kind == "stop_loss":
        params["stopLossPrice"] = trigger_price
    elif kind == "take_profit":
        params["takeProfitPrice"] = trigger_price
    else:
        params["triggerPrice"] = trigger_price
    if hedge_mode:
        params["positionSide"] = direction
        params["posSide"] = direction
    return await exchange.create_order(ccxt_symbol(symbol), "market", side, amount, None, params)


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

