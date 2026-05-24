# V8 Pro WebSocket + Multi-Pair + Trailing Stop

What changed:

- Public Bitget WebSocket ticker/books1 cache for faster price/spread updates.
- Private Bitget WebSocket scaffold for order/position/account events with auto reconnect.
- REST fallback remains active for safety.
- Multi-symbol scanner: scans `scan_symbols`, picks highest valid volatility candidate, and can trade selected symbol.
- Real `/api/settings` GET/POST endpoints for panel-managed settings.
- Pro trailing mode: TP2 disabled by default, real exchange-side SL can be moved behind price.
- New panel in `frontend/App.vue` with no fake settings fields: Save goes to backend `/api/settings`.

Before deploy to existing Supabase, run `supabase_migration_v8_pro.sql`.

Recommended first live test:

```sql
update bot_settings
set live_mode=true, max_notional_usd=50, leverage=1, risk_per_event_pct=0.001, max_trades_per_day=10,
    auto_arm_score=88, tp1_enabled=true, tp1_close_pct=0.25, tp2_enabled=false, trailing_mode=true,
    exchange_trailing_sl_enabled=true, scan_symbols='["BTC/USDT","ETH/USDT","SOL/USDT"]'::jsonb
where id=(select id from bot_settings limit 1);
```

Railway env must include:

```env
BITGET_API_KEY=
BITGET_API_SECRET=
BITGET_API_PASSPHRASE=
SUPABASE_URL=
SUPABASE_KEY=
API_AUTH_TOKEN=
LIVE_TRADING_UNLOCK=true
EXCHANGE_STOPS_VERIFIED=true
```
