from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

Candle = List[float]  # timestamp, open, high, low, close, volume


def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    k = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def sma(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def atr(candles: List[Candle], period: int = 14) -> Optional[float]:
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        prev_close = candles[i - 1][4]
        high = candles[i][2]
        low = candles[i][3]
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sum(trs[-period:]) / period


def session_vwap(candles: List[Candle]) -> Optional[float]:
    pv = 0.0
    vol = 0.0
    for _, _, high, low, close, volume in candles[-96:]:
        typical = (high + low + close) / 3
        pv += typical * volume
        vol += volume
    return pv / vol if vol else None


def normalize_symbol(symbol: str) -> str:
    if '/' in symbol:
        return symbol
    if symbol.endswith('USDT'):
        return symbol.replace('USDT', '/USDT') + ':USDT'
    return symbol


@dataclass
class Signal:
    action: str  # buy, sell, wait
    reason: str
    entry: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    qty: Optional[float] = None
    rr: Optional[float] = None
    indicators: Optional[Dict] = None


def build_signal(
    candles: List[Candle],
    ticker: Dict,
    settings: Dict,
) -> Signal:
    need = max(int(settings['ema_slow']), int(settings['volume_sma_period'])) + 5
    if len(candles) < need:
        return Signal('wait', f'Недостаточно свечей: нужно минимум {need}', indicators={})

    closes = [float(c[4]) for c in candles]
    volumes = [float(c[5]) for c in candles]
    last = candles[-1]
    prev = candles[-2]
    last_close = float(last[4])
    last_high = float(last[2])
    last_low = float(last[3])
    prev_high = float(prev[2])
    prev_low = float(prev[3])

    fast = ema(closes, int(settings['ema_fast']))[-1]
    slow = ema(closes, int(settings['ema_slow']))[-1]
    a = atr(candles, int(settings['atr_period']))
    vwap = session_vwap(candles)
    vol_sma = sma(volumes, int(settings['volume_sma_period']))

    bid = float(ticker.get('bid') or last_close)
    ask = float(ticker.get('ask') or last_close)
    spread_pct = (ask - bid) / last_close if last_close else 1
    vol_ratio = volumes[-1] / vol_sma if vol_sma else 0

    indicators = {
        'price': last_close,
        'ema_fast': fast,
        'ema_slow': slow,
        'atr': a,
        'vwap': vwap,
        'volume_ratio': vol_ratio,
        'spread_pct': spread_pct,
    }

    if a is None or vwap is None or vol_sma is None:
        return Signal('wait', 'Недостаточно данных для ATR/VWAP/Volume', indicators=indicators)

    if spread_pct > float(settings['max_spread_pct']):
        return Signal('wait', f'Спред слишком высокий: {spread_pct:.4%}', indicators=indicators)

    if vol_ratio < float(settings['min_volume_ratio']):
        return Signal('wait', f'Нет подтверждения объемом: {vol_ratio:.2f}x', indicators=indicators)

    trend_up = fast > slow and last_close > vwap
    trend_down = fast < slow and last_close < vwap
    broke_up = last_close > prev_high
    broke_down = last_close < prev_low

    require_retest = bool(settings.get('require_retest', True))
    retest_long = last_low <= prev_high <= last_close
    retest_short = last_high >= prev_low >= last_close

    risk_usd = float(settings['account_equity']) * float(settings['risk_per_trade'])
    sl_dist = max(a * float(settings['sl_atr_mult']), last_close * 0.001)
    tp_dist = max(a * float(settings['tp_atr_mult']), sl_dist * float(settings['min_rr']))
    qty = risk_usd / sl_dist if sl_dist > 0 else 0

    if trend_up and broke_up and bool(settings.get('allow_long', True)):
        if require_retest and not retest_long:
            return Signal('wait', 'LONG тренд есть, но нет ретеста уровня', indicators=indicators)
        entry = ask
        sl = entry - sl_dist
        tp = entry + tp_dist
        rr = (tp - entry) / (entry - sl)
        if rr < float(settings['min_rr']):
            return Signal('wait', f'RR слабый для LONG: {rr:.2f}', indicators=indicators)
        return Signal('buy', 'LONG: EMA тренд + VWAP + пробой/ретест + объем', entry, sl, tp, qty, rr, indicators)

    if trend_down and broke_down and bool(settings.get('allow_short', True)):
        if require_retest and not retest_short:
            return Signal('wait', 'SHORT тренд есть, но нет ретеста уровня', indicators=indicators)
        entry = bid
        sl = entry + sl_dist
        tp = entry - tp_dist
        rr = (entry - tp) / (sl - entry)
        if rr < float(settings['min_rr']):
            return Signal('wait', f'RR слабый для SHORT: {rr:.2f}', indicators=indicators)
        return Signal('sell', 'SHORT: EMA тренд + VWAP + пробой/ретест + объем', entry, sl, tp, qty, rr, indicators)

    return Signal('wait', 'Нет качественного сетапа: бот пропускает рынок', indicators=indicators)
