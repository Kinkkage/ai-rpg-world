# app/services/armor.py
from __future__ import annotations
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

async def effective_armor_level(session: AsyncSession, actor_id: str) -> int:
    """
    Возвращает итоговый уровень брони 0..5 для актёра, исходя из надетого предмета.
    """
    row = (await session.execute(text("""
        select k.armor_level
          from inventories inv
          join items i on i.id = inv.equipped_armor
          join item_kinds k on k.id = i.kind_id
         where inv.actor_id = :aid
         limit 1
    """), {"aid": actor_id})).mappings().first()
    return int(row["armor_level"]) if row else 0

def apply_armor_reduction(base_damage: int, armor_level: int) -> int:
    """
    Снижает урон по формуле A: -10% за уровень брони, минимум 1.
    """
    if base_damage < 1:
        base_damage = 1
    reduced = base_damage - int(base_damage * 0.1 * max(0, min(5, armor_level)))
    return max(1, reduced)
