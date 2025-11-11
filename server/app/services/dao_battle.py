# app/services/dao_battle.py
from __future__ import annotations
import uuid
from typing import Dict, Any, List, Optional
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

async def start_battle_db(session: AsyncSession, node_id: str, actor_ids: List[str]) -> Dict[str, Any]:
    """
    Создаём/переиспользуем running-сессию на узле и гарантируем добавление участников.
    """
    row = (await session.execute(
        text("select id from battle_sessions where node_id=:nid and state='running' limit 1"),
        {"nid": node_id},
    )).mappings().first()

    created = False
    if row:
        sid = row["id"]
    else:
        sid = str(uuid.uuid4())
        await session.execute(text("""
            insert into battle_sessions(id, node_id, state, turn_index, active_actor_id)
            values(:id, :nid, 'running', 0, null)
        """), {"id": sid, "nid": node_id})
        created = True

    # добавим только реально существующих актёров
    rows = (await session.execute(
        text("select id from actors where id = any(:ids)"),
        {"ids": actor_ids},
    )).mappings().all()
    present = [r["id"] for r in rows]

    # ВАЖНО: НЕ передавать join_order! Он identity.
    for aid in present:
        await session.execute(text("""
            insert into battle_participants(session_id, actor_id, team, initiative, alive)
            values(:sid, :aid, 'neutral', 0, true)
            on conflict (session_id, actor_id) do nothing
        """), {"sid": sid, "aid": aid})

    await session.commit()

    # вернуть актуальный список участников (по авто-инкрементному join_order)
    parts = (await session.execute(text("""
        select actor_id, team, initiative, alive, join_order
          from battle_participants
         where session_id=:sid
         order by join_order
    """), {"sid": sid})).mappings().all()
    participants = [dict(p) for p in parts]

    return {"ok": True, "session_id": sid, "created": created, "participants": participants}


async def get_battle_state_db(session: AsyncSession, session_id: str) -> Dict[str, Any]:
    sess = (await session.execute(text("""
        select id, node_id, turn_index, active_actor_id, state, created_at, finished_at
          from battle_sessions where id=:id
    """), {"id": session_id})).mappings().first()
    if not sess:
        return {"ok": False, "error": "not_found"}

    parts = (await session.execute(text("""
        select actor_id, team, initiative, alive, join_order
          from battle_participants
         where session_id=:sid
         order by join_order
    """), {"sid": session_id})).mappings().all()

    return {"ok": True, "session": dict(sess), "participants": [dict(p) for p in parts]}


async def set_active_actor_db(session: AsyncSession, session_id: str, actor_id: Optional[str]) -> None:
    await session.execute(text("""
        update battle_sessions set active_actor_id=:aid where id=:sid
    """), {"sid": session_id, "aid": actor_id})
    await session.commit()


async def next_turn_db(session: AsyncSession, session_id: str) -> Dict[str, Any]:
    """
    +1 ход и авто-очистка навыков, чьё время истекло (actor_skills как «текстовые бусты»).
    """
    await session.execute(text("""
        update battle_sessions
           set turn_index = turn_index + 1
         where id=:sid
    """), {"sid": session_id})

    # удаляем навыки, которые истекли к новому индексу хода
    await session.execute(text("""
        delete from actor_skills
         where session_id=:sid
           and applied_at_turn + duration_turns <= (
               select turn_index from battle_sessions where id=:sid
           )
    """), {"sid": session_id})

    await session.commit()
    return await get_battle_state_db(session, session_id)


async def end_battle_db(session: AsyncSession, session_id: str) -> None:
    """
    Завершает бой и чистит «текстовые» навыки для этой сессии.
    (Механические статусы живут в actor_statuses — их чистит твой общий тик/логика статусов.)
    """
    await session.execute(text("""
        update battle_sessions
           set state='finished', finished_at = now()
         where id=:sid
    """), {"sid": session_id})

    await session.execute(text("""
        delete from actor_skills
         where session_id=:sid
    """), {"sid": session_id})

    await session.commit()
