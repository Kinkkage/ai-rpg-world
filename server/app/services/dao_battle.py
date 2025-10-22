# app/services/dao_battle.py
from __future__ import annotations
import uuid
from typing import Dict, Any, List, Optional
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

async def start_battle_db(session: AsyncSession, node_id: str, actor_ids: List[str]) -> Dict[str, Any]:
    """
    Создаёт сессию боя и регистрирует участников.
    Пока без инициативы/очереди — просто фиксируем alive и join_order.
    """
    # защитимся от второй активной сессии на том же узле
    row = (await session.execute(
        text("select id from battle_sessions where node_id=:nid and state='running' limit 1"),
        {"nid": node_id},
    )).mappings().first()
    if row:
        return {"ok": True, "session_id": row["id"], "created": False}

    sid = str(uuid.uuid4())
    await session.execute(
        text("""insert into battle_sessions(id, node_id, state, turn_index) values(:id, :nid, 'running', 0)"""),
        {"id": sid, "nid": node_id},
    )

    # подтянем существующих акторов чтобы не получить «висячих» айди
    rows = (await session.execute(
        text("select id from actors where id = any(:ids)"),
        {"ids": actor_ids},
    )).mappings().all()
    present = [r["id"] for r in rows]

    for aid in present:
        await session.execute(
            text("""insert into battle_participants(session_id, actor_id, team, initiative, alive)
                    values(:sid, :aid, 'neutral', 0, true)
                    on conflict (session_id, actor_id) do nothing"""),
            {"sid": sid, "aid": aid},
        )

    await session.commit()
    return {"ok": True, "session_id": sid, "created": True, "participants": present}

async def get_battle_state_db(session: AsyncSession, session_id: str) -> Dict[str, Any]:
    sess = (await session.execute(
        text("""select id, node_id, turn_index, active_actor_id, state, created_at, finished_at
                from battle_sessions where id=:id"""),
        {"id": session_id},
    )).mappings().first()
    if not sess:
        return {"ok": False, "error": "not_found"}

    parts = (await session.execute(
        text("""select actor_id, team, initiative, alive, join_order
                from battle_participants where session_id=:sid order by join_order"""),
        {"sid": session_id},
    )).mappings().all()

    return {"ok": True, "session": dict(sess), "participants": [dict(p) for p in parts]}
