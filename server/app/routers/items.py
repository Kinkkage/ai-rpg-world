# app/routers/items.py
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.dao import use_consumable_db

router = APIRouter(prefix="/items", tags=["items"])


class UseRequest(BaseModel):
    actor_id: str
    item_id: str  # uuid предмета (из инвентаря/рук/контейнера не важно)
    target: str | None = None  # можно использовать на другого актёра


@router.post("/use")
async def use_item(req: UseRequest, session: AsyncSession = Depends(get_session)):
    """
    Использование предмета:
      - Для расходников: лечит, наносит эффект, исчезает.
      - Для предметов с зарядами: уменьшает charges.
      - При 0 charges — предмет удаляется.
    """
    res = await use_consumable_db(session, req.actor_id, req.item_id)
    return {"ok": True, "events": res}
