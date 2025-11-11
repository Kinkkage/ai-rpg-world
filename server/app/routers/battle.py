# app/routers/battle.py
from __future__ import annotations
from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.services.dao_battle import (
    start_battle_db,
    get_battle_state_db,
    next_turn_db,
    end_battle_db,
)
from app.main import broadcast_event  # уже есть в проекте

router = APIRouter(prefix="/battle", tags=["battle"])

# ====================
#        MODELS
# ====================

class StartBattleIn(BaseModel):
    node_id: str
    actor_ids: List[str]

class NextTurnIn(BaseModel):
    session_id: str = Field(..., description="ID боевой сессии")

class EndBattleIn(BaseModel):
    session_id: str = Field(..., description="ID боевой сессии")

# ====================
#       ENDPOINTS
# ====================

@router.post("/start")
async def start_battle(body: StartBattleIn, session: AsyncSession = Depends(get_session)) -> Dict[str, Any]:
    out = await start_battle_db(session, body.node_id, body.actor_ids)
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error", "start_failed"))
    await broadcast_event({
        "type": "BATTLE_START",
        "payload": {
            "session_id": out["session_id"],
            "node_id": body.node_id,
            "actors": out.get("participants", [])
        }
    })
    return out


@router.get("/state/{session_id}")
async def get_battle_state(session_id: str, session: AsyncSession = Depends(get_session)) -> Dict[str, Any]:
    out = await get_battle_state_db(session, session_id)
    if not out.get("ok"):
        raise HTTPException(status_code=404, detail=out.get("error", "not_found"))
    return out


@router.post("/next_turn")
async def next_turn(body: NextTurnIn, session: AsyncSession = Depends(get_session)) -> Dict[str, Any]:
    out = await next_turn_db(session, body.session_id)
    if not out.get("ok"):
        raise HTTPException(status_code=404, detail="not_found")
    await broadcast_event({
        "type": "BATTLE_NEXT_TURN",
        "payload": {"session_id": body.session_id, "turn_index": out["session"]["turn_index"]}
    })
    return out


@router.post("/end")
async def end_battle(body: EndBattleIn, session: AsyncSession = Depends(get_session)) -> Dict[str, Any]:
    await end_battle_db(session, body.session_id)
    await broadcast_event({"type": "BATTLE_END", "payload": {"session_id": body.session_id}})
    return {"ok": True}


# ====================
#   DEPRECATED: ATTACK
# ====================

@router.post("/action/attack")
async def deprecated_attack(*_args, **_kwargs):
    # Временная минимальная атака отключена.
    # Используй полноценный бой из /combat (дистанция, ЛоС, точность, патроны, криты и т.п.).
    raise HTTPException(status_code=410, detail="use /combat endpoints")
