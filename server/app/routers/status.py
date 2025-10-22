# app/routers/status.py
from __future__ import annotations

from typing import Optional, Dict, Any, List, DefaultDict
from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.dao_status import (
    get_statuses_db,
    apply_status_db,
    remove_status_db,
    advance_statuses_db,
)

router = APIRouter(prefix="/world/status", tags=["status"])

class ApplyStatusIn(BaseModel):
    actor_id: str
    status_id: str
    turns_left: int = Field(1, ge=0)
    intensity: float = 1.0
    stacks: int = 1
    source_id: Optional[str] = None

class RemoveStatusIn(BaseModel):
    actor_id: str
    status_id: str

@router.get("/{actor_id}")
async def list_statuses(actor_id: str, session: AsyncSession = Depends(get_session)):
    rows = await get_statuses_db(session, actor_id)
    # показываем только активные статусы
    return [r for r in rows if (r.get("turns_left") or 0) > 0]

@router.post("/apply")
async def apply_status(payload: ApplyStatusIn, session: AsyncSession = Depends(get_session)):
    out = await apply_status_db(
        session=session,
        actor_id=payload.actor_id,
        status_id=payload.status_id,
        turns_left=payload.turns_left,
        intensity=payload.intensity,
        stacks=payload.stacks,
        source_id=payload.source_id,
    )
    if not out.get("ok"):
        code = 404 if out.get("error") == "actor_not_found" else 400
        raise HTTPException(status_code=code, detail=out.get("error", "apply_failed"))

    try:
        from app.main import broadcast_event, compose_status_narrative
        evt = {
            "type": "STATUS_APPLY",
            "payload": {
                "actor_id": payload.actor_id,
                "status": payload.status_id,
                "turns_left": payload.turns_left,
                "stacks": payload.stacks,
                "intensity": payload.intensity,
                "source_id": payload.source_id,
            }
        }
        await broadcast_event(evt)
        await compose_status_narrative(session, payload.actor_id, [evt])
    except Exception:
        pass

    return out

@router.post("/remove")
async def remove_status(payload: RemoveStatusIn, session: AsyncSession = Depends(get_session)):
    out = await remove_status_db(session, payload.actor_id, payload.status_id)
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error", "remove_failed"))

    try:
        from app.main import broadcast_event, compose_status_narrative
        evt = {"type": "STATUS_EXPIRE", "payload": {"actor_id": payload.actor_id, "status": payload.status_id, "manual": True}}
        await broadcast_event(evt)
        await compose_status_narrative(session, payload.actor_id, [evt])
    except Exception:
        pass

    return out

@router.post("/advance_turn")
async def advance_turn(session: AsyncSession = Depends(get_session)):
    out = await advance_statuses_db(session)
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail="advance_failed")

    try:
        from app.main import broadcast_event, compose_status_narrative
        by_actor: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
        for evt in out.get("events", []):
            await broadcast_event(evt)
            aid = (evt.get("payload") or {}).get("actor_id")
            if aid and evt.get("type") in ("STATUS_TICK", "STATUS_EXPIRE", "STATUS_APPLY"):
                by_actor[aid].append(evt)
        for aid, evts in by_actor.items():
            await compose_status_narrative(session, aid, evts)
    except Exception:
        pass

    return out
