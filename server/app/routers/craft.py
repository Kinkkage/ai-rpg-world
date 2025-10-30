# app/routers/craft.py
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, Any, Dict

from app.db import get_session
from sqlalchemy.ext.asyncio import AsyncSession

# импорт из DAO (важно!)
from app.dao import craft_plan_db, craft_execute_db


router = APIRouter(prefix="/craft", tags=["craft"])


# ─────────────────────────────────────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────────────────────────────────────

class CraftPlanRequest(BaseModel):
    actor_id: str
    station_object_id: Optional[str] = None  # null → крафт "на коленке"
    text: str                                 # Например: "хочу сделать электрошокер"


class CraftExecuteRequest(BaseModel):
    actor_id: str
    plan: Dict[str, Any]                      # Передаём план, полученный от /plan
    confirm: bool = True                      # true → выполнить


# ─────────────────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/plan")
async def craft_plan(req: CraftPlanRequest, session: AsyncSession = Depends(get_session)):
    """
    Планирование крафта: анализируем запрос игрока, навыки, содержимое инвентаря.
    Возвращаем детализированный план (что нужно, что есть, какие аналоги возможны).
    """
    result = await craft_plan_db(
        session=session,
        actor_id=req.actor_id,
        station_object_id=req.station_object_id,
        text=req.text,
    )

    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result)

    return result


@router.post("/execute")
async def craft_execute(req: CraftExecuteRequest, session: AsyncSession = Depends(get_session)):
    """
    Выполнение крафта: списываем ресурсы, создаём итоговый предмет.
    """
    result = await craft_execute_db(
        session=session,
        actor_id=req.actor_id,
        plan=req.plan,
        confirm=req.confirm,
    )

    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result)

    return result
