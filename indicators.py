from __future__ import annotations

from typing import Dict, List


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


def vwap(ohlcv: List[List[float]], lookback: int = 30) -> float:
    rows = ohlcv[-lookback:] if len(ohlcv) >= lookback else ohlcv
    num = 0.0
    den = 0.0
    for _, open_, high, low, close, volume in rows:
        typical = (high + low + close) / 3
        num += typical * volume
        den += volume
    return num / den if den else 0.0


def _range(rows: List[List[float]]) -> float:
    if not rows:
        return 0.0
    return float(max(r[2] for r in rows) - min(r[3] for r in rows))


def candle_quality(ohlcv: List[List[float]], atr14: float) -> Dict[str, float]:
    if not ohlcv or atr14 <= 0:
        return {"last_body_atr": 0.0, "last_wick_ratio": 1.0, "last_direction": 0.0}
    _, open_, high, low, close, _ = ohlcv[-1]
    full = max(high - low, 1e-9)
    body = abs(close - open_)
    upper = high - max(open_, close)
    lower = min(open_, close) - low
    wick_ratio = (upper + lower) / full
    direction = 1.0 if close > open_ else -1.0 if close < open_ else 0.0
    return {
        "last_body_atr": float(body / atr14),
        "last_wick_ratio": float(wick_ratio),
        "last_direction": direction,
    }


def pre_event_metrics(ohlcv: List[List[float]], lookback_minutes: int) -> Dict[str, float]:
    rows = ohlcv[-lookback_minutes:] if len(ohlcv) >= lookback_minutes else ohlcv
    highs = [r[2] for r in rows]
    lows = [r[3] for r in rows]
    closes = [r[4] for r in ohlcv]
    vols = [r[5] for r in ohlcv]
    recent_vol = sma(vols[-lookback_minutes:], max(1, min(lookback_minutes, len(vols))))
    base_vol = sma(vols[:-lookback_minutes] or vols, max(1, min(30, len(vols))))
    a14 = atr(ohlcv, 14)
    a60 = atr(ohlcv, 60)
    q = candle_quality(ohlcv, a14)
    return {
        "high": float(max(highs)) if highs else 0.0,
        "low": float(min(lows)) if lows else 0.0,
        "range": float(max(highs) - min(lows)) if highs and lows else 0.0,
        "last": float(closes[-1]) if closes else 0.0,
        "ema20": ema(closes, 20),
        "ema50": ema(closes, 50),
        "vwap30": vwap(ohlcv, 30),
        "atr14": a14,
        "atr60": a60,
        "atr_expansion": (a14 / a60) if a60 > 0 else 1.0,
        "volume_spike": (recent_vol / base_vol) if base_vol > 0 else 1.0,
        **q,
    }


def volatility_metrics(ohlcv: List[List[float]], lookback_minutes: int, compression_lookback_minutes: int) -> Dict[str, float]:
    m = pre_event_metrics(ohlcv, lookback_minutes)
    recent = ohlcv[-lookback_minutes:] if len(ohlcv) >= lookback_minutes else ohlcv
    base_start = max(0, len(ohlcv) - lookback_minutes - compression_lookback_minutes)
    base_end = max(0, len(ohlcv) - lookback_minutes)
    base = ohlcv[base_start:base_end] or ohlcv[:-lookback_minutes] or ohlcv
    base_range = _range(base)
    recent_range = _range(recent)
    m["base_range"] = base_range
    m["range_expansion"] = (recent_range / base_range) if base_range > 0 else 1.0
    m["compression_ratio"] = (base_range / recent_range) if recent_range > 0 else 1.0
    return m


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def score_volatility(metrics: Dict[str, float], spread_bps: float, settings) -> Dict[str, float | str | bool]:
    atr_ratio = metrics.get("atr_expansion", 1.0)
    vol_ratio = metrics.get("volume_spike", 1.0)
    range_exp = metrics.get("range_expansion", 1.0)
    wick = metrics.get("last_wick_ratio", 1.0)
    body_atr = metrics.get("last_body_atr", 0.0)
    pre_range = metrics.get("range", 0.0)

    atr_score = clamp((atr_ratio - 1.0) / max(0.01, settings.min_atr_expansion_ratio - 1.0) * 22.0, 0, 24)
    vol_score = clamp((vol_ratio - 1.0) / max(0.01, settings.min_volume_spike_ratio - 1.0) * 22.0, 0, 24)
    expansion_score = clamp((range_exp - 1.0) / max(0.01, settings.min_range_expansion_ratio - 1.0) * 16.0, 0, 16)
    spread_score = clamp((settings.max_spread_bps - spread_bps) / max(0.01, settings.max_spread_bps) * 16.0, 0, 16)
    range_score = 0.0
    if settings.min_pre_range_usd <= pre_range <= settings.max_pre_range_usd:
        range_score = 12.0
    elif pre_range < settings.min_pre_range_usd:
        range_score = clamp(pre_range / max(1, settings.min_pre_range_usd) * 12.0, 0, 12)
    else:
        range_score = clamp(settings.max_pre_range_usd / max(1, pre_range) * 12.0, 0, 12)
    candle_score = 8.0
    if wick > settings.max_wick_ratio:
        candle_score -= clamp((wick - settings.max_wick_ratio) / max(0.01, 1 - settings.max_wick_ratio) * 5.0, 0, 5)
    if body_atr > settings.max_chase_candle_atr:
        candle_score -= clamp((body_atr - settings.max_chase_candle_atr) * 3.0, 0, 8)
    candle_score = clamp(candle_score, 0, 8)

    total = atr_score + vol_score + expansion_score + spread_score + range_score + candle_score
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
        "expansion_score": round(expansion_score, 2),
        "spread_score": round(spread_score, 2),
        "range_score": round(range_score, 2),
        "candle_score": round(candle_score, 2),
    }
