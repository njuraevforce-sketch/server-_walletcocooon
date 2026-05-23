from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import database as db
import connector

app = FastAPI(title="Breakout Bot Core")

app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"], 
    allow_credentials=True, 
    allow_methods=["*"], 
    allow_headers=["*"],
)

@app.get("/")
async def health_check():
    return {"status": "online", "system": "HFT Breakout Engine"}

@app.get("/api/status")
async def get_bot_status():
    """Отдает данные для дашборда Vue.js"""
    settings = db.get_settings()
    return {"status": "success", "settings": settings}

@app.post("/api/start")
async def start_bot():
    """Кнопка 'СТАРТ' из панели"""
    settings = db.get_settings()
    if not settings:
        raise HTTPException(status_code=400, detail="Настройки не найдены в базе")
    
    db.update_status(True)
    
    result = await connector.execute_strategy(
        settings['symbol'], 
        float(settings['trade_volume']), 
        float(settings['gap_distance']),
        float(settings['stop_loss'])
    )
    return result

@app.post("/api/stop")
async def stop_bot():
    """Кнопка 'СТОП' / Kill Switch из панели"""
    settings = db.get_settings()
    symbol = settings['symbol'] if settings else 'BTC/USDT'
    
    result = await connector.kill_switch(symbol)
    return result
