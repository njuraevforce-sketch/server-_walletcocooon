import os
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") # Здесь будет service_role ключ

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_settings():
    """Берет настройки из панели управления"""
    res = supabase.table('bot_settings').select('*').limit(1).execute()
    return res.data[0] if res.data else None

def update_status(is_active: bool):
    """Обновляет статус кнопки Вкл/Выкл"""
    settings = get_settings()
    if settings:
        supabase.table('bot_settings').update({"is_active": is_active}).eq("id", settings['id']).execute()

def log_order(order_id: str, symbol: str, side: str, price: float):
    """Записывает выставленный ордер в базу"""
    supabase.table('trades_log').insert({
        "exchange_order_id": order_id,
        "symbol": symbol,
        "order_side": side,
        "trigger_price": price,
        "status": "pending"
    }).execute()

def update_order(order_id: str, status: str, pnl: float = 0):
    """Обновляет статус ордера (сработал/безубыток/закрыт)"""
    supabase.table('trades_log').update({
        "status": status,
        "pnl": pnl
    }).eq("exchange_order_id", order_id).execute()
