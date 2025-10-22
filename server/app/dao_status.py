from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

# --------------------------- эффектные статусы ---------------------------
# Никаких внешних зависимостей (таблица statuses не требуется)

def _effect_burn(intensity: float, stacks: int) -> Dict[str, Any]:
    # урон каждый ход: round(intensity * 2)
    dmg = max(0, int(round((intensity or 1.0) * 2)))
    return {"dmg": dmg}

def _effect_bleed(intensity: float, stacks: int) -> Dict[str, Any]:
    # урон от кровотечения: stacks * 1
    dmg = max(0, int(stacks or 1) * 1)
    return {"dmg": dmg}

# неуронные — просто флаги; оставил для совместимости
def _effect_guard(intensity: float, stacks: int) -> Dict[str, Any]:
    return {"incoming_mult": 0.5}

def _effect_rage(intensity: float, stacks: int) -> Dict[str, Any]:
    return {"outgoing_mult": 1.5}

_EFFECT_MAP = {
    "burn": _effect_burn,
    "bleed": _effect_bleed,
    "guard": _effect_guard,
    "rage": _effect_rage,
}

# --------------------------- CRUD статусов ---------------------------

async def get_statuses_db(session: AsyncSession, actor_id: str) -> List[Dict[str, Any]]:
    rows = (
        await session.execute(
            text(
                """
                SELECT status_id, turns_left, stacks, intensity, source_id, created_at
                  FROM actor_statuses
                 WHERE actor_id = :aid
                   AND COALESCE(turns_left, 0) > 0
                 ORDER BY created_at
                """
            ),
            {"aid": actor_id},
        )
    ).mappings().all()
    return [dict(r) for r in rows]


async def apply_status_db(
    session: AsyncSession,
    actor_id: str,
    status_id: str,
    turns_left: int = 1,
    intensity: float = 1.0,
    stacks: int = 1,
    source_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Апсёрт: перезаписываем длительность на присланную (важно для тестов и ожидаемого UX).
    """
    exists_row = (
        await session.execute(
            text("SELECT 1 FROM actors WHERE id = :aid LIMIT 1"),
            {"aid": actor_id},
        )
    ).first()
    if not exists_row:
        return {"ok": False, "error": "actor_not_found", "actor_id": actor_id}

    try:
        await session.execute(
            text(
                """
                INSERT INTO actor_statuses (actor_id, status_id, turns_left, intensity, stacks, source_id)
                VALUES (:aid, :sid, :t, :i, :st, :src)
                ON CONFLICT (actor_id, status_id) DO UPDATE
                   SET turns_left = EXCLUDED.turns_left,
                       stacks     = EXCLUDED.stacks,
                       intensity  = EXCLUDED.intensity,
                       source_id  = COALESCE(EXCLUDED.source_id, actor_statuses.source_id)
                """
            ),
            {"aid": actor_id, "sid": status_id, "t": turns_left, "i": intensity, "st": stacks, "src": source_id},
        )
    except IntegrityError as e:
        return {"ok": False, "error": "integrity_error", "detail": str(e)}

    return {"ok": True, "applied": status_id, "turns_left": turns_left, "stacks": stacks, "intensity": intensity}


async def remove_status_db(session: AsyncSession, actor_id: str, status_id: str) -> Dict[str, Any]:
    await session.execute(
        text("DELETE FROM actor_statuses WHERE actor_id=:aid AND status_id=:sid"),
        {"aid": actor_id, "sid": status_id},
    )
    return {"ok": True, "removed": status_id}

# --------------------------- тик хода с эффектами ---------------------------

async def advance_statuses_db(session: AsyncSession) -> Dict[str, Any]:
    """
    Тикаем ВСЕ статусы:
      - считаем и применяем урон (если есть) по actors.hp
      - уменьшаем turns_left
      - удаляем истёкшие
      - собираем события STATUS_TICK / STATUS_EXPIRE
    """
    rows = (
        await session.execute(
            text(
                """
                SELECT actor_id, status_id, turns_left, stacks, intensity
                  FROM actor_statuses
                 ORDER BY created_at
                """
            )
        )
    ).mappings().all()

    events: List[Dict[str, Any]] = []

    for r in rows:
        actor_id = r["actor_id"]
        status_id = r["status_id"]
        stacks = int(r.get("stacks") or 1)
        intensity = float(r.get("intensity") or 1.0)

        effect_fn = _EFFECT_MAP.get(status_id)
        effect: Dict[str, Any] = effect_fn(intensity, stacks) if effect_fn else {}

        # 1) Применим урон (если есть)
        dmg = int(effect.get("dmg") or 0)
        new_hp = None
        if dmg > 0:
            hp_row = (
                await session.execute(
                    text(
                        """
                        UPDATE actors
                           SET hp = GREATEST(0, COALESCE(hp,0) - :dmg)
                         WHERE id = :aid
                     RETURNING hp
                        """
                    ),
                    {"aid": actor_id, "dmg": dmg},
                )
            ).mappings().first()
            new_hp = (hp_row and int(hp_row["hp"])) if hp_row else None
            events.append({"type": "STATUS_TICK", "payload": {"actor_id": actor_id, "status": status_id, "dmg": dmg, "hp": new_hp}})
        elif effect:
            # неуронные — просто тик-событие
            events.append({"type": "STATUS_TICK", "payload": {"actor_id": actor_id, "status": status_id, **effect}})

        # 2) Уменьшим длительность конкретного статуса
        left_row = (
            await session.execute(
                text(
                    """
                    UPDATE actor_statuses
                       SET turns_left = turns_left - 1
                     WHERE actor_id = :aid AND status_id = :sid
                 RETURNING turns_left
                    """
                ),
                {"aid": actor_id, "sid": status_id},
            )
        ).mappings().first()

        # 3) Если истёк — удалим и сообщим
        if left_row and int(left_row["turns_left"]) <= 0:
            await session.execute(
                text("DELETE FROM actor_statuses WHERE actor_id=:aid AND status_id=:sid"),
                {"aid": actor_id, "sid": status_id},
            )
            events.append({"type": "STATUS_EXPIRE", "payload": {"actor_id": actor_id, "status": status_id}})

    # Финальная уборка на всякий случай
    await session.execute(text("DELETE FROM actor_statuses WHERE COALESCE(turns_left, 0) <= 0"))

    await session.commit()
    expired = sum(1 for e in events if e.get("type") == "STATUS_EXPIRE")
    return {"ok": True, "events": events, "expired": expired}
