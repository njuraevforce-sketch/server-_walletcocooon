from __future__ import annotations

from typing import Dict, List, Any


def sma(values: List[float], period: int) -> float:
    if not values:
        return 0.0
    if len(values) < period:
        return sum(values) / len(values)
    return sum(values[-period:]) / period


def ema(values: List[float], period: int) -> float:
    if not values:
        return 0.0
    k = 2 / (period + 1)
    out = values[0]
    for v in values[1:]:
        out = v * k + out * (1 - k)
    return out


def rma(values: List[float], period: int) -> float:
    """Сглаженная скользящая средняя (Wilder's Smoothing) для ADX."""
    if not values:
        return 0.0
    alpha = 1.0 / period
    res = sum(values[:period]) / period
    for v in values[period:]:
        res = alpha * v + (1 - alpha) * res
    return res


def atr(ohlcv: List[List[float]], period: int = 14) -> float:
    if len(ohlcv) < 2:
        return 0.0
    trs: List[float] = []
    for i in range(1, len(ohlcv)):
        _, _, high, low, close, _ = ohlcv[i]
        prev_close = ohlcv[i - 1][4]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(float(tr))
    return sma(trs, period)


def adx(ohlcv: List[List[float]], period: int = 14) -> float:
    """Расчет Average Directional Index (ADX) для фильтрации флэта."""
    if len(ohlcv) <= period * 2:
        return 0.0
    
    trs, pDMs, mDMs = [], [], []
    for i in range(1, len(ohlcv)):
        high, low = ohlcv[i][2], ohlcv[i][3]
        prev_high, prev_low, prev_close = ohlcv[i-1][2], ohlcv[i-1][3], ohlcv[i-1][4]
        
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        up_move = high - prev_high
        down_move = prev_low - low
        
        pDM = up_move if up_move > down_move and up_move > 0 else 0.0
        mDM = down_move if down_move > up_move and down_move > 0 else 0.0
        
        trs.append(tr)
        pDMs.append(pDM)
        mDMs.append(mDM)
    
    smoothed_tr = sum(trs[:period])
    smoothed_pDM = sum(pDMs[:period])
    smoothed_mDM = sum(mDMs[:period])
    
    if smoothed_tr == 0:
        return 0.0

    dxs = []
    for i in range(period, len(trs)):
        smoothed_tr = smoothed_tr - (smoothed_tr / period) + trs[i]
        smoothed_pDM = smoothed_pDM - (smoothed_pDM / period) + pDMs[i]
        smoothed_mDM = smoothed_mDM - (smoothed_mDM / period) + mDMs[i]
        
        di_plus = 100 * (smoothed_pDM / smoothed_tr) if smoothed_tr > 0 else 0
        di_minus = 100 * (smoothed_mDM / smoothed_tr) if smoothed_tr > 0 else 0
        
        dx_den = di_plus + di_minus
        dx = 100 * abs(di_plus - di_minus) / dx_den if dx_den > 0 else 0
        dxs.append(dx)
        
    if not dxs:
        return 0.0
    
    return rma(dxs, period)


def orderbook_imbalance(ob: Dict[str, Any], depth: int = 15) -> float:
    """
    Вычисляет дисбаланс стакана.
    Возвращает число от -1.0 (давление продавцов/стена Ask) до 1.0 (давление покупателей/стена Bid).
    """
    if not ob:
        return 0.0
    
    bids = ob.get("bids", [])
    asks = ob.get("asks", [])
    
    bid_vol = sum(float(b[1]) for b in bids[:depth]) if bids else 0.0
    ask_vol = sum(float(a[1]) for a in asks[:depth]) if asks else 0.0
    
    if bid_vol + ask_vol == 0:
        return 0.0
        
    return (bid_vol - ask_vol) / (bid_vol + ask_vol)


def vwap(ohlcv: List[List[float]], lookback: int = 30) -> float:
    rows = ohlcv[-lookback:] if len(ohlcv) >= lookback else ohlcv
    num = 0.0
    den = 0.0
    for _, open_, high, low, close, volume in rows:
        typ = (high + low + close) / 3
        num += typ * volume
        den += volume
    return num / den if den > 0 else 0.0


def pre_event_metrics(ohlcv: List[List[float]], lookback: int) -> Dict[str, float]:
    rows = ohlcv[-lookback:] if len(ohlcv) >= lookback else ohlcv
    if not rows:
        return {"high": 0.0, "low": 0.0, "range": 0.0, "last": 0.0}
    high = max(r[2] for r in rows)
    low = min(r[3] for r in rows)
    return {"high": high, "low": low, "range": high - low, "last": rows[-1][4]}


def volatility_metrics(ohlcv: List[List[float]], lookback: int, compression_lookback: int) -> Dict[str, float]:
    if not ohlcv:
        return {}
        
    last_candle = ohlcv[-1]
    last = last_candle[4]
    closes = [r[4] for r in ohlcv]
    ema20 = ema(closes, 20)
    ema50 = ema(closes, 50)
    vwap30 = vwap(ohlcv, 30)
    atr14 = atr(ohlcv, 14)
    atr60 = atr(ohlcv, 60)
    
    # НОВОЕ: Считаем силу тренда
    adx14 = adx(ohlcv, 14)

    vol_base = sma([r[5] for r in ohlcv[:-1]], 20)
    volume_spike = last_candle[5] / vol_base if vol_base > 0 else 1.0

    body = abs(last_candle[4] - last_candle[1])
    wick_tot = (last_candle[2] - last_candle[3]) - body
    candle_tot = last_candle[2] - last_candle[3]

    last_body_atr = body / atr14 if atr14 > 0 else 0.0
    last_wick_ratio = wick_tot / candle_tot if candle_tot > 0 else 0.0
    last_direction = 1 if last_candle[4] >= last_candle[1] else -1

    base_range = 0.0
    range_expansion = 0.0
    if len(ohlcv) >= lookback + 1:
        base_rows = ohlcv[-(lookback + 1) : -1]
        base_high = max(r[2] for r in base_rows)
        base_low = min(r[3] for r in base_rows)
        base_range = base_high - base_low
        range_expansion = candle_tot / base_range if base_range > 0 else 1.0

    comp_rows = ohlcv[-compression_lookback:] if len(ohlcv) >= compression_lookback else ohlcv
    comp_high = max(r[2] for r in comp_rows) if comp_rows else last
    comp_low = min(r[3] for r in comp_rows) if comp_rows else last
    comp_range = comp_high - comp_low
    compression_ratio = atr14 / (comp_range / compression_lookback) if comp_range > 0 else 1.0

    atr_expansion = atr14 / atr60 if atr60 > 0 else 1.0
    pre = pre_event_metrics(ohlcv, lookback)

    return {
        "high": pre["high"],
        "low": pre["low"],
        "range": pre["range"],
        "last": last,
        "ema20": ema20,
        "ema50": ema50,
        "vwap30": vwap30,
        "atr14": atr14,
        "atr60": atr60,
        "adx14": adx14,
        "atr_expansion": atr_expansion,
        "volume_spike": volume_spike,
        "last_body_atr": last_body_atr,
        "last_wick_ratio": last_wick_ratio,
        "last_direction": last_direction,
        "base_range": base_range,
        "range_expansion": range_expansion,
        "compression_ratio": compression_ratio,
    }


def clamp(val: float, min_val: float, max_val: float) -> float:
    return max(min_val, min(val, max_val))


def score_volatility(metrics: Dict[str, float], spread_bps: float, settings: Any) -> Dict[str, Any]:
    atr_ratio = float(metrics.get("atr_expansion") or 0.0)
    vol_ratio = float(metrics.get("volume_spike") or 0.0)
    range_exp = float(metrics.get("range_expansion") or 0.0)
    body_atr = float(metrics.get("last_body_atr") or 0.0)
    wick = float(metrics.get("last_wick_ratio") or 0.0)
    pre_range = float(metrics.get("range") or 0.0)

    atr_score = clamp((atr_ratio - 1.0) * 50, 0, 25)
    vol_score = clamp((vol_ratio - 1.0) * 20, 0, 30)
    exp_score = clamp((range_exp - 1.0) * 40, 0, 25)
    spread_score = clamp((settings.max_spread_bps - spread_bps) / max(0.01, settings.max_spread_bps) * 10, 0, 20)

    opt_range = (settings.min_pre_range_usd + settings.max_pre_range_usd) / 2
    range_score = clamp(15 - abs(pre_range - opt_range) / opt_range * 15, 0, 15)
    candle_score = clamp((1.0 - wick) * 10, 0, 10)

    total = atr_score + vol_score + exp_score + spread_score + range_score + candle_score
    reasons = []
    if spread_bps > settings.max_spread_bps:
        reasons.append(f"spread too wide {spread_bps:.2f}bps")
    if atr_ratio < settings.min_atr_expansion_ratio:
        reasons.append(f"ATR expansion weak {atr_ratio:.2f}")
    if vol_ratio < settings.min_volume_spike_ratio:
        reasons.append(f"volume weak {vol_ratio:.2f}")
    if range_exp < settings.min_range_expansion_ratio:
        reasons.append(f"range expansion weak {range_exp:.2f}")
    if body_atr > settings.max_chase_candle_atr:
        reasons.append(f"last candle too extended {body_atr:.2f} ATR")
    if wick > settings.max_wick_ratio:
        reasons.append(f"wicks too large {wick:.2f}")
    if pre_range < settings.min_pre_range_usd or pre_range > settings.max_pre_range_usd:
        reasons.append(f"range not ideal {pre_range:.2f}")

    state = "HOT_ARM" if total >= settings.auto_arm_score else "WATCH" if total >= settings.notify_score else "COLD"
    return {
        "volatility_score": round(total, 2),
        "state": state,
        "should_arm": total >= settings.auto_arm_score and not reasons[:1],
        "reason": "; ".join(reasons) if reasons else "volatility accepted",
        "atr_score": round(atr_score, 2),
        "volume_score": round(vol_score, 2),
        "expansion_score": round(exp_score, 2),
        "spread_score": round(spread_score, 2),
        "range_score": round(range_score, 2),
        "candle_score": round(candle_score, 2),
    }
