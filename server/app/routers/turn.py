# app/routers/turn.py
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.dao_turn import advance_turn_db   # <- используем наш DAO, это важно

router = APIRouter(prefix="/world/turn", tags=["turn"])

@router.post("/advance")
async def advance_turn(session: AsyncSession = Depends(get_session)):
    """
    Продвигаем один ход (детерминированный сет-тик):
      1) считаем, что истечёт (turns_left <= 1),
      2) уменьшаем счётчик всем,
      3) удаляем всё, что <= 0,
      4) коммитим,
      5) возвращаем events + expired.
    """
    res = await advance_turn_db(session)
    return {"ok": True, **res}
