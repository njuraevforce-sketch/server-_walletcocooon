from __future__ import annotations

import os
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import httpx

from models import EventImpact, NewsEvent, BotSettings

FMP_API_KEY = os.environ.get("FMP_API_KEY")
TRADING_ECONOMICS_KEY = os.environ.get("TRADING_ECONOMICS_KEY")
TRADING_ECONOMICS_SECRET = os.environ.get("TRADING_ECONOMICS_SECRET")
CALENDAR_PROVIDER = os.environ.get("CALENDAR_PROVIDER", "fmp").lower()

CRITICAL_KEYWORDS = [
    "fomc", "fed interest rate", "federal funds", "interest rate decision",
    "powell", "cpi", "core cpi", "pce", "core pce", "non farm payroll", "nonfarm payroll", "nfp",
]
HIGH_KEYWORDS = [
    "ppi", "core ppi", "unemployment", "gdp", "retail sales", "ism", "pmi",
    "initial jobless", "jobless claims", "adp employment", "fed speech",
]


def _parse_dt(value: Any) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    s = str(value).replace("Z", "+00:00")
    try:
        # FMP often returns '2026-05-23 12:30:00'
        if "T" not in s and "+" not in s:
            return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                pass
    return datetime.now(timezone.utc)


def classify_impact(title: str, raw_impact: Any = None) -> EventImpact:
    raw = str(raw_impact or "").lower()
    t = title.lower()
    if "critical" in raw or any(k in t for k in CRITICAL_KEYWORDS):
        return EventImpact.CRITICAL
    if "high" in raw or any(k in t for k in HIGH_KEYWORDS):
        return EventImpact.HIGH
    if "medium" in raw or "moderate" in raw:
        return EventImpact.MEDIUM
    return EventImpact.LOW


def _provider_id(provider: str, title: str, event_time: datetime, country: str) -> str:
    base = f"{provider}|{country}|{event_time.isoformat()}|{title}"
    return hashlib.sha256(base.encode()).hexdigest()[:32]


async def fetch_fmp_calendar(days_ahead: int = 7, days_back: int = 0) -> List[NewsEvent]:
    if not FMP_API_KEY:
        return []
    now = datetime.now(timezone.utc)
    frm = (now - timedelta(days=days_back)).date().isoformat()
    to = (now + timedelta(days=days_ahead)).date().isoformat()
    url = "https://financialmodelingprep.com/stable/economic-calendar"
    params = {"from": frm, "to": to, "apikey": FMP_API_KEY}
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
    events: List[NewsEvent] = []
    if not isinstance(data, list):
        return events
    for row in data:
        title = str(row.get("event") or row.get("title") or row.get("name") or "Economic Event")
        country = str(row.get("country") or row.get("countryCode") or "").upper() or "US"
        currency = str(row.get("currency") or ("USD" if country == "US" else "")).upper() or "USD"
        event_time = _parse_dt(row.get("date") or row.get("datetime") or row.get("event_time"))
        impact = classify_impact(title, row.get("impact") or row.get("importance"))
        events.append(NewsEvent(
            provider_id=_provider_id("fmp", title, event_time, country),
            provider="fmp",
            title=title,
            country=country,
            currency=currency,
            impact=impact,
            event_time_utc=event_time,
            previous=None if row.get("previous") is None else str(row.get("previous")),
            estimate=None if row.get("estimate") is None else str(row.get("estimate")),
            actual=None if row.get("actual") is None else str(row.get("actual")),
            raw=row,
        ))
    return events


async def fetch_trading_economics_calendar(days_ahead: int = 7) -> List[NewsEvent]:
    # TE has several auth modes. This keeps integration simple while still usable with key/secret.
    if not TRADING_ECONOMICS_KEY:
        return []
    now = datetime.now(timezone.utc)
    frm = now.date().isoformat()
    to = (now + timedelta(days=days_ahead)).date().isoformat()
    credentials = TRADING_ECONOMICS_KEY
    if TRADING_ECONOMICS_SECRET:
        credentials = f"{TRADING_ECONOMICS_KEY}:{TRADING_ECONOMICS_SECRET}"
    url = f"https://api.tradingeconomics.com/calendar/country/all/{frm}/{to}"
    params = {"c": credentials, "format": "json"}
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
    events: List[NewsEvent] = []
    if not isinstance(data, list):
        return events
    for row in data:
        title = str(row.get("Event") or row.get("event") or "Economic Event")
        country = str(row.get("Country") or row.get("country") or "").upper() or "US"
        currency = str(row.get("Currency") or row.get("currency") or ("USD" if country == "US" else "")).upper() or "USD"
        event_time = _parse_dt(row.get("Date") or row.get("date"))
        impact = classify_impact(title, row.get("Importance") or row.get("importance"))
        events.append(NewsEvent(
            provider_id=_provider_id("tradingeconomics", title, event_time, country),
            provider="tradingeconomics",
            title=title,
            country=country,
            currency=currency,
            impact=impact,
            event_time_utc=event_time,
            previous=None if row.get("Previous") is None else str(row.get("Previous")),
            estimate=None if row.get("Forecast") is None else str(row.get("Forecast")),
            actual=None if row.get("Actual") is None else str(row.get("Actual")),
            raw=row,
        ))
    return events


async def fetch_calendar(days_ahead: int = 7) -> List[NewsEvent]:
    if CALENDAR_PROVIDER == "tradingeconomics":
        primary = await fetch_trading_economics_calendar(days_ahead)
        if primary:
            return primary
    return await fetch_fmp_calendar(days_ahead)


def filter_events_for_crypto(events: List[NewsEvent], settings: BotSettings) -> List[NewsEvent]:
    countries = {c.upper() for c in settings.allowed_countries}
    keywords = [k.lower() for k in settings.allowed_keywords]
    result: List[NewsEvent] = []
    for e in events:
        if e.country.upper() not in countries and e.currency.upper() != "USD":
            continue
        if settings.high_impact_only and e.impact not in (EventImpact.HIGH, EventImpact.CRITICAL):
            continue
        title = e.title.lower()
        if keywords and not any(k in title for k in keywords):
            # Still allow FOMC/Fed/CPI family because they often have provider-specific titles.
            if not any(k in title for k in CRITICAL_KEYWORDS + HIGH_KEYWORDS):
                continue
        result.append(e)
    return sorted(result, key=lambda x: x.event_time_utc)
