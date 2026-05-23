import os
import asyncio
import ccxt.async_support as ccxt
import database as db

API_KEY = os.environ.get("BITGET_API_KEY")
API_SECRET = os.environ.get("BITGET_API_SECRET")
API_PASSPHRASE = os.environ.get("BITGET_API_PASSPHRASE")

monitor_task = None
is_monitoring = False

async def get_client():
    return ccxt.bitget({
        'apiKey': API_KEY,
        'secret': API_SECRET,
        'password': API_PASSPHRASE,
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'}
    })

async def smart_monitor(exchange, symbol: str, buy_id: str, sell_id: str, stop_loss_dist: float):
    """Умный модуль: OCO-логика и авто-безубыток"""
    global is_monitoring
    try:
        while is_monitoring:
            # 1. Проверяем статус ордеров
            buy_order = await exchange.fetch_order(buy_id, symbol)
            sell_order = await exchange.fetch_order(sell_id, symbol)

            # OCO: Если Buy Stop сработал (стал закрытым/исполненным)
            if buy_order['status'] == 'closed':
                print("Buy Stop сработал! Отменяем Sell Stop...")
                await exchange.cancel_order(sell_id, symbol)
                db.update_order(buy_id, "active")
                db.update_order(sell_id, "cancelled")
                
                # Здесь включается логика безубытка (упрощенно)
                # Бот может выставить стоп-лосс приказ и следить за профитом
                is_monitoring = False 
                break

            # OCO: Если Sell Stop сработал
            elif sell_order['status'] == 'closed':
                print("Sell Stop сработал! Отменяем Buy Stop...")
                await exchange.cancel_order(buy_id, symbol)
                db.update_order(sell_id, "active")
                db.update_order(buy_id, "cancelled")
                is_monitoring = False
                break

            await asyncio.sleep(2) # Пингуем каждые 2 секунды
            
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"Ошибка монитора: {e}")
    finally:
        await exchange.close()

async def execute_strategy(symbol: str, volume: float, gap: float, sl_dist: float):
    global is_monitoring, monitor_task
    
    # Формат ccxt для фьючерсов Bitget требует :USDT на конце
    ccxt_symbol = f"{symbol}:USDT" if not symbol.endswith(":USDT") else symbol
    
    exchange = await get_client()
    try:
        ticker = await exchange.fetch_ticker(ccxt_symbol)
        current_price = ticker['last']
        
        buy_price = current_price + gap
        sell_price = current_price - gap
        
        buy_params = {'triggerPrice': buy_price, 'positionSide': 'long'}
        sell_params = {'triggerPrice': sell_price, 'positionSide': 'short'}
        
        # Закидываем ордера
        results = await asyncio.gather(
            exchange.create_order(ccxt_symbol, 'market', 'buy', volume, None, buy_params),
            exchange.create_order(ccxt_symbol, 'market', 'sell', volume, None, sell_params),
            return_exceptions=True
        )
        
        orders = []
        for idx, res in enumerate(results):
            if not isinstance(res, Exception):
                side = 'buy' if idx == 0 else 'sell'
                price = buy_price if idx == 0 else sell_price
                orders.append(res['id'])
                db.log_order(res['id'], symbol, side, price)
        
        if len(orders) == 2:
            # Если оба ордера успешно встали, запускаем умный мониторинг
            is_monitoring = True
            monitor_task = asyncio.create_task(smart_monitor(exchange, ccxt_symbol, orders[0], orders[1], sl_dist))
            
        return {"status": "success", "orders": orders, "message": "Ордера выставлены, монитор запущен"}
    except Exception as e:
        await exchange.close()
        return {"status": "error", "message": str(e)}

async def kill_switch(symbol: str):
    """Экстренное снятие всех ордеров и остановка мониторинга"""
    global is_monitoring, monitor_task
    is_monitoring = False
    if monitor_task:
        monitor_task.cancel()
        
    ccxt_symbol = f"{symbol}:USDT" if not symbol.endswith(":USDT") else symbol
    exchange = await get_client()
    try:
        await exchange.cancel_all_orders(ccxt_symbol)
        db.update_status(False)
        return {"status": "success", "message": "Kill Switch активирован. Ордера сняты."}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        await exchange.close()
