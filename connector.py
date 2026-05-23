import os
import asyncio
from typing import Any, Dict, Optional
import ccxt.async_support as ccxt
import database as db
from strategy import build_signal, normalize_symbol

API_KEY = os.environ.get('BITGET_API_KEY')
API_SECRET = os.environ.get('BITGET_API_SECRET')
API_PASSPHRASE = os.environ.get('BITGET_API_PASSPHRASE')
EXCHANGE_SANDBOX = os.environ.get('EXCHANGE_SANDBOX', 'true').lower() == 'true'

runner_task: Optional[asyncio.Task] = None
runner_enabled = False


async def get_client():
    exchange = ccxt.bitget({
        'apiKey': API_KEY,
        'secret': API_SECRET,
        'password': API_PASSPHRASE,
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'},
    })
    if EXCHANGE_SANDBOX:
        exchange.set_sandbox_mode(True)
    return exchange


async def safe_close(exchange):
    try:
        await exchange.close()
    except Exception:
        pass


def trading_blocked(settings: Dict[str, Any]) -> Optional[str]:
    if db.get_open_trades(settings['symbol']):
        return 'Уже есть открытая/активная сделка по этому символу'
    daily_pnl = db.get_daily_pnl()
    equity = float(settings['account_equity'])
    max_loss = -abs(equity * float(settings['max_daily_loss_pct']))
    if daily_pnl <= max_loss:
        return f'Дневной лимит убытка достигнут: {daily_pnl:.2f}$'
    if db.get_daily_trade_count() >= int(settings['max_trades_per_day']):
        return 'Достигнут лимит сделок на сегодня'
    if not db.cooldown_ok(int(settings['cooldown_minutes'])):
        return 'Cooldown после прошлой сделки еще не прошел'
    return None


async def analyze_once() -> Dict[str, Any]:
    settings = db.get_settings()
    if not settings:
        return {'status': 'error', 'message': 'Настройки не найдены'}

    symbol = normalize_symbol(settings['symbol'])
    exchange = await get_client()
    try:
        ticker = await exchange.fetch_ticker(symbol)
        candles = await exchange.fetch_ohlcv(symbol, timeframe=settings['timeframe'], limit=260)
        signal = build_signal(candles, ticker, settings)
        return {
            'status': 'success',
            'symbol': symbol,
            'signal': signal.__dict__,
        }
    except Exception as e:
        db.log_event('Ошибка анализа', 'error', {'error': str(e)})
        return {'status': 'error', 'message': str(e)}
    finally:
        await safe_close(exchange)


async def place_paper_trade(settings: Dict[str, Any], signal) -> Dict[str, Any]:
    trade = db.log_trade({
        'symbol': settings['symbol'],
        'order_side': signal.action,
        'strategy': 'trend_retest_v1',
        'mode': 'paper',
        'trigger_price': signal.entry,
        'execution_price': signal.entry,
        'qty': signal.qty,
        'stop_loss_price': signal.stop_loss,
        'take_profit_price': signal.take_profit,
        'status': 'active',
        'reason': signal.reason,
        'meta': {'rr': signal.rr, 'indicators': signal.indicators},
    })
    db.log_event('Paper trade открыт', 'success', {'trade_id': trade['id'], 'side': signal.action})
    return {'status': 'success', 'message': 'Paper trade открыт', 'trade': trade}


async def place_live_trade(settings: Dict[str, Any], signal) -> Dict[str, Any]:
    symbol = normalize_symbol(settings['symbol'])
    exchange = await get_client()
    try:
        side = signal.action
        amount = float(signal.qty)
        if amount <= 0:
            return {'status': 'error', 'message': 'Размер позиции <= 0'}

        order = await exchange.create_order(symbol, 'market', side, amount, None, {'reduceOnly': False})
        trade = db.log_trade({
            'exchange_order_id': order.get('id'),
            'symbol': settings['symbol'],
            'order_side': side,
            'strategy': 'trend_retest_v1',
            'mode': 'live',
            'trigger_price': signal.entry,
            'execution_price': float(order.get('average') or signal.entry),
            'qty': amount,
            'stop_loss_price': signal.stop_loss,
            'take_profit_price': signal.take_profit,
            'status': 'active',
            'reason': signal.reason,
            'meta': {'rr': signal.rr, 'raw_order': order, 'indicators': signal.indicators},
        })

        # ВАЖНО: у Bitget/ccxt параметры SL/TP могут отличаться по версии API.
        # Поэтому live-режим по умолчанию должен сначала пройти на sandbox.
        db.log_event('Live order открыт. Проверь, что SL/TP созданы на бирже.', 'warning', {'trade_id': trade['id']})
        return {'status': 'success', 'message': 'Live order открыт; SL/TP нужно проверить в Bitget sandbox', 'trade': trade}
    except Exception as e:
        db.log_event('Ошибка live order', 'error', {'error': str(e)})
        return {'status': 'error', 'message': str(e)}
    finally:
        await safe_close(exchange)


async def try_enter_trade() -> Dict[str, Any]:
    settings = db.get_settings()
    if not settings:
        return {'status': 'error', 'message': 'Настройки не найдены'}

    block = trading_blocked(settings)
    if block:
        db.log_event(block, 'info')
        return {'status': 'wait', 'message': block}

    analysis = await analyze_once()
    if analysis['status'] != 'success':
        return analysis

    signal_dict = analysis['signal']
    action = signal_dict['action']
    if action == 'wait':
        db.log_event(signal_dict['reason'], 'info', signal_dict.get('indicators') or {})
        return {'status': 'wait', 'message': signal_dict['reason'], 'signal': signal_dict}

    # rebuild lightweight signal object
    class S: pass
    s = S()
    for k, v in signal_dict.items():
        setattr(s, k, v)

    if settings.get('paper_mode', True):
        return await place_paper_trade(settings, s)
    return await place_live_trade(settings, s)


async def bot_loop(interval_seconds: int = 30):
    global runner_enabled
    db.log_event('Bot loop запущен', 'success')
    while runner_enabled:
        try:
            settings = db.get_settings()
            if not settings or not settings.get('is_active'):
                await asyncio.sleep(interval_seconds)
                continue
            await try_enter_trade()
        except Exception as e:
            db.log_event('Критическая ошибка bot_loop', 'error', {'error': str(e)})
        await asyncio.sleep(interval_seconds)
    db.log_event('Bot loop остановлен', 'warning')


async def start_bot() -> Dict[str, Any]:
    global runner_task, runner_enabled
    if runner_task and not runner_task.done():
        return {'status': 'success', 'message': 'Бот уже запущен'}
    db.update_status(True)
    runner_enabled = True
    runner_task = asyncio.create_task(bot_loop())
    return {'status': 'success', 'message': 'Бот включен. Он будет входить только при качественном сетапе.'}


async def stop_bot() -> Dict[str, Any]:
    global runner_task, runner_enabled
    runner_enabled = False
    db.update_status(False)
    if runner_task:
        runner_task.cancel()
    db.log_event('Kill switch: бот остановлен', 'warning')
    return {'status': 'success', 'message': 'Бот остановлен. Новые входы запрещены.'}
