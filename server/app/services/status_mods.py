# app/services/status_mods.py
from __future__ import annotations
from typing import Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

async def get_status_combat_mods(
    session: AsyncSession,
    attacker_id: str,
    target_id: str,
) -> Dict[str, Any]:
    """
    Агрегация простых модификаторов из actor_statuses для атакующего/цели.
    Поддержка meta-ключей:
      - accuracy_mod_attacker: int
      - damage_bonus_attacker: int
      - damage_mult_attacker: float
      - armor_bonus_target: int
    """
    mods: Dict[str, Any] = {
        "accuracy_mod_attacker": 0,
        "damage_bonus_attacker": 0,
        "damage_mult_attacker": 1.0,
        "armor_bonus_target": 0,
    }

    rows = (await session.execute(
        text("""
            select actor_id, label, turns_left, coalesce(meta,'{}'::jsonb) as meta
              from actor_statuses
             where actor_id in (:atk, :tgt)
               and turns_left > 0
        """),
        {"atk": attacker_id, "tgt": target_id}
    )).mappings().all()

    for r in rows:
        raw_meta = r["meta"] or {}
        meta: Dict[str, Any] = dict(raw_meta) if isinstance(raw_meta, dict) else {}

        if r["actor_id"] == attacker_id:
            # безопасные приведения типов
            try: mods["accuracy_mod_attacker"] += int(meta.get("accuracy_mod_attacker", 0))
            except Exception: pass
            try: mods["damage_bonus_attacker"] += int(meta.get("damage_bonus_attacker", 0))
            except Exception: pass
            try: mods["damage_mult_attacker"] *= float(meta.get("damage_mult_attacker", 1.0))
            except Exception: pass
        else:
            try: mods["armor_bonus_target"] += int(meta.get("armor_bonus_target", 0))
            except Exception: pass

    # sanity: не даём отрицательных/некорректных значений «сломать» бой
    if not (0.1 <= float(mods["damage_mult_attacker"]) <= 5.0):
        mods["damage_mult_attacker"] = 1.0

    return mods
