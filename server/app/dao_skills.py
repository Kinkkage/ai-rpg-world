# app/dao_skills.py
from __future__ import annotations
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

async def has_skill_active(session: AsyncSession, actor_id: str, session_id: str, label: str) -> bool:
    row = (await session.execute(
        text("select turn_index from battle_sessions where id=:sid"),
        {"sid": session_id}
    )).mappings().first()
    if not row:
        return False
    cur_turn = int(row["turn_index"])
    return bool((await session.execute(text("""
        select 1
          from actor_skills
         where actor_id=:aid and session_id=:sid and label=:lbl
           and applied_at_turn + duration_turns > :cur
         limit 1
    """), {"aid": actor_id, "sid": session_id, "lbl": label, "cur": cur_turn})).scalar())
