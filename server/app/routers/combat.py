# app/routers/combat.py
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.db import get_session
from app.dao import preview_attack_geometry_db, perform_attack_db

router = APIRouter(prefix="/combat", tags=["combat"])


@router.get("/preview/{attacker_id}/{target_id}")
async def preview_attack(
    attacker_id: str,
    target_id: str,
    session: AsyncSession = Depends(get_session),
):
    """
    Предпросмотр атаки:
      - distance, aligned, los
      - сведения об оружии (class, damage_type, opt/max_range, crit_chance, hit_bonus)
      - projected_accuracy
    """
    return await preview_attack_geometry_db(session, attacker_id, target_id)


@router.post("/attack/{attacker_id}/{target_id}")
async def do_attack(
    attacker_id: str,
    target_id: str,
    session: AsyncSession = Depends(get_session),
):
    """
    Фактическая атака:
      - проверка LOS/дистанции/max_range
      - расход боезапаса (charges или ammo из рюкзака)
      - бросок попадания/крит
      - применение урона + резисты
      - подробные события (events)
    """
    return await perform_attack_db(session, attacker_id, target_id)


# --- опционально: удобно быстро сбрасывать HP манекену при тестировании ---
@router.post("/debug/heal/{actor_id}")
async def debug_heal(
    actor_id: str,
    hp: int = 100,
    session: AsyncSession = Depends(get_session),
):
    await session.execute(text("UPDATE actors SET hp = :hp WHERE id = :aid"), {"hp": hp, "aid": actor_id})
    await session.commit()
    return {"ok": True, "actor": actor_id, "hp": hp}
