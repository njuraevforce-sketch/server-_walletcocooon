import os
from fastapi import FastAPI, Header, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import database as db
import connector
from schemas import SettingsUpdate

BOT_API_TOKEN = os.environ.get('BOT_API_TOKEN', '')
origins = [x.strip() for x in os.environ.get('FRONTEND_ORIGINS', 'http://localhost:5173').split(',') if x.strip()]

app = FastAPI(title='Quant Breakout Engine Pro', version='2.0.0')
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=['GET', 'POST'],
    allow_headers=['Authorization', 'Content-Type'],
)


def require_auth(authorization: str = Header(default='')):
    if not BOT_API_TOKEN:
        raise HTTPException(status_code=500, detail='BOT_API_TOKEN не задан на сервере')
    if authorization != f'Bearer {BOT_API_TOKEN}':
        raise HTTPException(status_code=401, detail='Unauthorized')


@app.get('/')
async def health_check():
    return {'status': 'online', 'system': 'Quant Breakout Engine Pro'}


@app.get('/api/status', dependencies=[Depends(require_auth)])
async def get_status():
    return {
        'status': 'success',
        'settings': db.get_settings(),
        'open_trades': db.get_open_trades(),
        'recent_trades': db.get_recent_trades(20),
        'events': db.get_events(30),
        'daily_pnl': db.get_daily_pnl(),
        'daily_trade_count': db.get_daily_trade_count(),
    }


@app.post('/api/settings', dependencies=[Depends(require_auth)])
async def update_settings(payload: SettingsUpdate):
    clean = {k: v for k, v in payload.model_dump().items() if v is not None}
    return {'status': 'success', 'settings': db.update_settings(clean)}


@app.post('/api/analyze', dependencies=[Depends(require_auth)])
async def analyze():
    return await connector.analyze_once()


@app.post('/api/try-entry', dependencies=[Depends(require_auth)])
async def try_entry():
    return await connector.try_enter_trade()


@app.post('/api/start', dependencies=[Depends(require_auth)])
async def start_bot():
    return await connector.start_bot()


@app.post('/api/stop', dependencies=[Depends(require_auth)])
async def stop_bot():
    return await connector.stop_bot()
