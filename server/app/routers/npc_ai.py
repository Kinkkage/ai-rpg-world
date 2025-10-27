from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.db import get_session

router = APIRouter(prefix="/npc", tags=["npc-ai"])

class ModeReq(BaseModel):
    ai_mode: str  # aggressive|neutral|friendly|passive

@router.post("/{actor_id}/set_mode")
async def set_mode(actor_id: str, data: ModeReq, session: AsyncSession = Depends(get_session)):
    if data.ai_mode not in ("aggressive","neutral","friendly","passive"):
        raise HTTPException(400, "bad ai_mode")
    row = (await session.execute(text("UPDATE actors SET ai_mode=:m WHERE id=:id RETURNING id, ai_mode"),
                                 {"m": data.ai_mode, "id": actor_id})).mappings().first()
    if not row: raise HTTPException(404, "actor not found")
    await session.commit()
    return {"ok": True, "id": row["id"], "ai_mode": row["ai_mode"]}

@router.get("/{actor_id}/ai_state")
async def ai_state(actor_id: str, session: AsyncSession = Depends(get_session)):
    row = (await session.execute(text("""
        SELECT id, ai_mode, target_id, meta FROM actors WHERE id=:id
    """), {"id": actor_id})).mappings().first()
    if not row: raise HTTPException(404, "actor not found")
    return {"ok": True, "id": row["id"], "ai_mode": row["ai_mode"], "target_id": row["target_id"], "meta": row["meta"]}
