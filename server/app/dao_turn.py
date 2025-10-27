# app/dao_turn.py
from __future__ import annotations
from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# ВАЖНО: импорт из твоей текущей структуры
from app.dao_status import advance_statuses_db      # ← лежит в app/
from app.services.dao_ai import run_ai_turn_db      # ← лежит в app/services/

async def _get_active_nodes(session: AsyncSession) -> List[str]:
    """
    Узлы, где есть игроки (минимальная, но надёжная эвристика активности).
    """
    rows = (
        await session.execute(
            text("""
                SELECT DISTINCT node_id
                  FROM actors
                 WHERE kind = 'player'
                   AND node_id IS NOT NULL
            """)
        )
    ).mappings().all()
    return [r["node_id"] for r in rows if r["node_id"]]


async def _get_npc_ids_in_nodes(session: AsyncSession, nodes: List[str]) -> List[str]:
    """
    Живые NPC в указанных узлах.
    """
    if not nodes:
        return []
    rows = (
        await session.execute(
            text("""
                SELECT id
                  FROM actors
                 WHERE kind = 'npc'
                   AND hp > 0
                   AND node_id = ANY(:nodes)
            """),
            {"nodes": nodes},
        )
    ).mappings().all()
    return [r["id"] for r in rows]


async def advance_turn_db(session: AsyncSession) -> Dict[str, Any]:
    """
    Тик мира:
      1) статусы (урон/истечение),
      2) активные узлы (есть игрок),
      3) AI-ход для всех NPC в активных узлах,
      4) события + count истёкших статусов.
    """
    events: List[Dict[str, Any]] = []
    expired_total = 0

    # 1) тик статусов
    try:
        st = await advance_statuses_db(session)
        events.extend(st.get("events", []))
        expired_total += int(st.get("expired", 0))
    except Exception as e:
        events.append({"type": "ERROR_STATUS_TICK", "payload": {"detail": str(e)}})

    # 2) активные узлы (с игроком)
    nodes = await _get_active_nodes(session)

    # 3) все NPC в этих узлах
    npc_ids = await _get_npc_ids_in_nodes(session, nodes)

    # 4) AI-ход
    for npc_id in npc_ids:
        try:
            ev = await run_ai_turn_db(session, npc_id)
            if ev:
                events.append(ev)
        except Exception as e:
            events.append({
                "type": "ERROR_NPC_AI",
                "payload": {"actor_id": npc_id, "detail": str(e)},
            })

    return {"events": events, "expired": expired_total}
