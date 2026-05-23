# Volatility Hunter Sniper v7 Live Guard

Это версия после v6, где добавлена главная защита для реальных денег:

> **Если после входа биржевой Stop-Loss не подтвердился — бот сразу закрывает позицию market reduce-only.**

Manual polling/ручной контроль backend больше не считается достаточной защитой для новостной волатильности. Биржевой SL обязателен.

## Главная стратегия

Бот не открывает long + short одновременно market-ордерами.

Он ставит две ловушки:

- `BUY STOP` выше локального диапазона
- `SELL STOP` ниже локального диапазона

Когда одна ловушка срабатывает:

1. Вторая ловушка отменяется.
2. Бот ставит биржевой SL и TP2.
3. Если SL не подтвердился — позиция сразу закрывается.
4. Если SL подтвердился — бот ведет сделку: TP1, breakeven, trailing, timeout exit.

## 3 режима

1. **Calendar News Mode** — бот торгует high-impact новости из календаря.
2. **Auto Volatility Scanner** — бот сам сканирует волатильность без новостей.
3. **Manual ARM NOW** — ты сам видишь рынок и нажимаешь ARM NOW.

## Что нового в v7

- `hard_exchange_sl_required=true` — реальный SL обязателен.
- `flatten_if_exchange_sl_fails=true` — если SL не поставился, позиция закрывается сразу.
- `cancel_tp_if_sl_fails=true` — если TP поставился, а SL нет, TP отменяется как orphan-order.
- `double_fill_emergency_flatten=true` — если обе ловушки внезапно исполнились, бот пытается закрыть всё по символу.
- `kill_switch_closes_positions=true` — STOP теперь не только отменяет ордера, но и пытается закрыть позицию по символу.
- `/api/preflight` — проверка live-гейтов, баланса, спреда и live guard настроек перед запуском.

## Установка

1. Выполни `supabase_migration.sql` в Supabase SQL Editor.
2. Замени backend-файлы на Railway.
3. Замени фронт `frontend/App.vue`.
4. Поставь env из `.env.example`.
5. Проверь `/api/preflight`.
6. Только потом запускай `/api/start` или `ARM NOW`.

## Live-гейты

Реальные ордера не будут отправляться, пока одновременно не включены:

```bash
settings.live_mode=true
LIVE_TRADING_UNLOCK=true
EXCHANGE_STOPS_VERIFIED=true
EXCHANGE_SANDBOX=false
```

## Рекомендованные первые настройки для реальных денег

Для первого боевого запуска:

- `risk_per_event_pct`: 0.001
- `max_daily_loss_pct`: 0.003–0.005
- `max_trades_per_day`: 1
- `max_consecutive_losses`: 1
- `leverage`: 1
- `max_notional_usd`: 50–150
- `auto_arm_score`: 85+
- `hard_exchange_sl_required`: true
- `flatten_if_exchange_sl_fails`: true

## Важное

Это не гарантия прибыли. Цель v7 — сделать live-запуск менее опасным: не держать позицию без биржевого stop-loss, не оставлять orphan-ордера и не продолжать торговлю после аварийных условий.
