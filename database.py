import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from supabase import create_client, Client

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or os.environ.get('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError('SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required')

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_settings() -> Optional[Dict[str, Any]]:
    res = supabase.table('bot_settings').select('*').limit(1).execute()
    return res.data[0] if res.data else None


def update_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    settings = get_settings()
    payload['updated_at'] = now_iso()
    if not settings:
        res = supabase.table('bot_settings').insert(payload).execute()
    else:
        res = supabase.table('bot_settings').update(payload).eq('id', settings['id']).execute()
    return res.data[0]


def update_status(is_active: bool) -> None:
    settings = get_settings()
    if settings:
        supabase.table('bot_settings').update({'is_active': is_active, 'updated_at': now_iso()}).eq('id', settings['id']).execute()


def log_event(message: str, level: str = 'info', meta: Optional[Dict[str, Any]] = None) -> None:
    supabase.table('bot_events').insert({'message': message, 'level': level, 'meta': meta or {}}).execute()


def get_events(limit: int = 50) -> List[Dict[str, Any]]:
    res = supabase.table('bot_events').select('*').order('created_at', desc=True).limit(limit).execute()
    return res.data or []


def log_trade(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload.setdefault('created_at', now_iso())
    payload.setdefault('updated_at', now_iso())
    res = supabase.table('trades_log').insert(payload).execute()
    return res.data[0]


def update_trade(trade_id: str, payload: Dict[str, Any]) -> None:
    payload['updated_at'] = now_iso()
    supabase.table('trades_log').update(payload).eq('id', trade_id).execute()


def get_open_trades(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    q = supabase.table('trades_log').select('*').in_('status', ['pending', 'active', 'breakeven'])
    if symbol:
        q = q.eq('symbol', symbol)
    res = q.order('created_at', desc=True).execute()
    return res.data or []


def get_recent_trades(limit: int = 50) -> List[Dict[str, Any]]:
    res = supabase.table('trades_log').select('*').order('created_at', desc=True).limit(limit).execute()
    return res.data or []


def get_daily_pnl() -> float:
    start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    res = supabase.table('trades_log').select('pnl').gte('created_at', start.isoformat()).execute()
    return float(sum(float(row.get('pnl') or 0) for row in (res.data or [])))


def get_daily_trade_count() -> int:
    start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    res = supabase.table('trades_log').select('id', count='exact').gte('created_at', start.isoformat()).execute()
    return int(res.count or 0)


def get_last_trade_time() -> Optional[datetime]:
    res = supabase.table('trades_log').select('created_at').order('created_at', desc=True).limit(1).execute()
    if not res.data:
        return None
    return datetime.fromisoformat(res.data[0]['created_at'].replace('Z', '+00:00'))


def cooldown_ok(minutes: int) -> bool:
    last = get_last_trade_time()
    if not last:
        return True
    return datetime.now(timezone.utc) - last >= timedelta(minutes=minutes)
