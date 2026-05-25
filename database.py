from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from supabase import create_client, Client

from models import BotSettings, NewsEvent

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_KEY are required")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_settings() -> BotSettings:
    res = supabase.table("bot_settings").select("*").limit(1).execute()
    if not res.data:
        settings = BotSettings()
        supabase.table("bot_settings").insert(settings.model_dump()).execute()
        return settings
    data = res.data[0]
    # Ignore id/updated_at fields if present
    return BotSettings(**{k: v for k, v in data.items() if k in BotSettings.model_fields})


def update_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    current = supabase.table("bot_settings").select("id").limit(1).execute()
    update_payload = {k: v for k, v in payload.items() if k in BotSettings.model_fields}
    update_payload["updated_at"] = utc_now_iso()
    if current.data:
        row_id = current.data[0]["id"]
        res = supabase.table("bot_settings").update(update_payload).eq("id", row_id).execute()
    else:
        seed = BotSettings(**update_payload).model_dump()
        res = supabase.table("bot_settings").insert(seed).execute()
    return res.data[0] if res.data else update_payload


def set_runtime_state(mode: str, is_active: bool, reason: str = "") -> None:
    current = supabase.table("bot_runtime").select("id").limit(1).execute()
    payload = {
        "mode": mode,
        "is_active": is_active,
        "reason": reason,
        "updated_at": utc_now_iso(),
    }
    if current.data:
        supabase.table("bot_runtime").update(payload).eq("id", current.data[0]["id"]).execute()
    else:
        supabase.table("bot_runtime").insert(payload).execute()


def get_runtime_state() -> Dict[str, Any]:
    res = supabase.table("bot_runtime").select("*").limit(1).execute()
    return res.data[0] if res.data else {"mode": "off", "is_active": False, "reason": "not initialized"}


def log_event(level: str, event_type: str, message: str, data: Optional[Dict[str, Any]] = None) -> None:
    supabase.table("bot_events").insert({
        "level": level,
        "event_type": event_type,
        "message": message,
        "data": data or {},
        "created_at": utc_now_iso(),
    }).execute()


def upsert_news_event(event: NewsEvent) -> None:
    payload = event.model_dump(mode="json")
    payload["event_time_utc"] = event.event_time_utc.isoformat()
    payload["updated_at"] = utc_now_iso()
    supabase.table("news_events").upsert(payload, on_conflict="provider_id").execute()


def list_upcoming_events(limit: int = 25) -> List[Dict[str, Any]]:
    now = utc_now_iso()
    res = (
        supabase.table("news_events")
        .select("*")
        .gte("event_time_utc", now)
        .order("event_time_utc")
        .limit(limit)
        .execute()
    )
    return res.data or []


def mark_event_status(provider_id: str, status: str, note: str = "") -> None:
    supabase.table("news_events").update({
        "bot_status": status,
        "bot_note": note,
        "updated_at": utc_now_iso(),
    }).eq("provider_id", provider_id).execute()


def create_trade(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload.setdefault("created_at", utc_now_iso())
    res = supabase.table("trades_log").insert(payload).execute()
    return res.data[0] if res.data else payload


def update_trade(trade_id: str, payload: Dict[str, Any]) -> None:
    payload["updated_at"] = utc_now_iso()
    supabase.table("trades_log").update(payload).eq("id", trade_id).execute()


def update_trade_by_client_oid(client_oid: str, payload: Dict[str, Any]) -> None:
    payload["updated_at"] = utc_now_iso()
    supabase.table("trades_log").update(payload).eq("client_oid", client_oid).execute()


def todays_pnl() -> float:
    start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    res = supabase.table("trades_log").select("pnl").gte("created_at", start).execute()
    return float(sum(float(row.get("pnl") or 0) for row in (res.data or [])))


def todays_trade_count() -> int:
    """Count only real executed entries for daily trade limits.

    Trigger traps create two rows in trades_log before any position exists.
    Expired/cancelled traps must not consume max_trades_per_day.
    A real trade is counted only when the entry was actually filled and
    execution_price is present.
    """
    start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    res = (
        supabase.table("trades_log")
        .select("id,execution_price,status")
        .gte("created_at", start)
        .execute()
    )
    count = 0
    for row in res.data or []:
        status = str(row.get("status") or "").lower()
        if status in {"armed", "expired", "cancelled"}:
            continue
        if row.get("execution_price") is not None:
            count += 1
    return count


def consecutive_losses(limit: int = 10) -> int:
    res = supabase.table("trades_log").select("pnl,status").eq("status", "closed").order("created_at", desc=True).limit(limit).execute()
    losses = 0
    for row in res.data or []:
        if float(row.get("pnl") or 0) < 0:
            losses += 1
        else:
            break
    return losses
