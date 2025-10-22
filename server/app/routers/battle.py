# app/routers/battle.py
from __future__ import annotations
from typing import List
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.services.dao_battle import start_battle_db, get_battle_state_db
from app.main import broadcast_event  # уже есть в проекте

router = APIRouter(prefix="/battle", tags=["battle"])

class StartBattleIn(BaseModel):
    node_id: str
    actor_ids: List[str]  # игрок + враги, которых хотим «схлопнуть» в сцену

@router.post("/start")
async def start_battle(body: StartBattleIn, session: AsyncSession = Depends(get_session)):
    out = await start_battle_db(session, body.node_id, body.actor_ids)
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error", "start_failed"))
    # стримим событие для клиента/логгера
    await broadcast_event({"type": "BATTLE_START", "payload": {"session_id": out["session_id"], "node_id": body.node_id, "actors": out.get("participants", [])}})
    return out

@router.get("/state/{session_id}")
async def get_battle_state(session_id: str, session: AsyncSession = Depends(get_session)):
    out = await get_battle_state_db(session, session_id)
    if not out.get("ok"):
        raise HTTPException(status_code=404, detail=out.get("error", "not_found"))
    return out
