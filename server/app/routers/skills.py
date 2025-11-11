# app/routers/skills.py
from __future__ import annotations
from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.db import get_session

router = APIRouter(prefix="/world/skills", tags=["skills"])

class SkillUpsertIn(BaseModel):
    actor_id: str
    session_id: str                 # id из battle_sessions
    label: str                      # "гипноз", "burn", "poison", ...
    note: str = ""
    tags: List[str] = []
    duration_turns: int = Field(1, ge=1, le=10)

class SkillRemoveIn(BaseModel):
    actor_id: str
    session_id: str
    label: str

def _is_skill_active_row(row: Dict[str, Any], current_turn: int) -> bool:
    return int(row["applied_at_turn"]) + int(row["duration_turns"]) > int(current_turn)

@router.get("/{actor_id}/{session_id}")
async def list_skills(actor_id: str, session_id: str, session: AsyncSession = Depends(get_session)):
    sess = (await session.execute(
        text("select turn_index from battle_sessions where id=:sid"),
        {"sid": session_id}
    )).mappings().first()
    if not sess:
        raise HTTPException(status_code=404, detail="battle_session_not_found")
    cur_turn = int(sess["turn_index"])

    rows = (await session.execute(text("""
        select id, actor_id, session_id, label, note, tags, applied_at_turn, duration_turns
          from actor_skills
         where actor_id=:aid and session_id=:sid
         order by id desc
    """), {"aid": actor_id, "sid": session_id})).mappings().all()

    return [dict(r) for r in rows if _is_skill_active_row(r, cur_turn)]

@router.post("/apply")
async def apply_skill(body: SkillUpsertIn, session: AsyncSession = Depends(get_session)):
    # базовая валидация
    ok_actor = (await session.execute(text("select 1 from actors where id=:id"), {"id": body.actor_id})).scalar()
    ok_battle = (await session.execute(text("select turn_index from battle_sessions where id=:id"), {"id": body.session_id})).mappings().first()
    if not ok_actor:
        raise HTTPException(status_code=404, detail="actor_not_found")
    if not ok_battle:
        raise HTTPException(status_code=404, detail="battle_session_not_found")

    cur_turn = int(ok_battle["turn_index"])

    # 1) пишем текстовый навык — для UI/LLM
    await session.execute(text("""
        insert into actor_skills(actor_id, session_id, label, note, tags, applied_at_turn, duration_turns)
        values(:aid, :sid, :lbl, :note, :tags, :turn, :dur)
        on conflict (actor_id, session_id, label, applied_at_turn) do nothing
    """), {
        "aid": body.actor_id, "sid": body.session_id, "lbl": body.label,
        "note": body.note, "tags": body.tags,
        "turn": cur_turn, "dur": body.duration_turns
    })

    # 2) БРИДЖ: если это известный «механический» эффект — дублируем в actor_statuses
    #    (чтобы /combat и тик статусов видели числовой эффект)
    lbl = body.label.lower().strip()
    if lbl in ("burn", "poison", "slow"):
        if lbl == "burn":
            # 1 ход, дот 25% от «базовой силы» — пока кладём intensity=1 и дот в meta по желанию
            await session.execute(text("""
                insert into actor_statuses(actor_id, session_id, label, note, turns_left, intensity, meta)
                values(:aid, :sid, 'burn', :note, CAST(:turns AS int), CAST(:intensity AS int),
                       jsonb_build_object('source','skills_bridge'))
            """), {"aid": body.actor_id, "sid": body.session_id, "note": body.note, "turns": 1, "intensity": 1})

        elif lbl == "poison":
            # 2 хода, лёгкий яд; initiative_mod в meta при желании можно учесть в /combat
            await session.execute(text("""
                insert into actor_statuses(actor_id, session_id, label, note, turns_left, intensity, meta)
                values(:aid, :sid, 'poison', :note, CAST(:turns AS int), CAST(:intensity AS int),
                       jsonb_build_object('source','skills_bridge','initiative_mod', CAST(:init AS int)))
            """), {"aid": body.actor_id, "sid": body.session_id, "note": body.note, "turns": 2, "intensity": 1, "init": -1})

        elif lbl == "slow":
            # 1 ход, замедление; speed_mod можно читать из meta
            await session.execute(text("""
                insert into actor_statuses(actor_id, session_id, label, note, turns_left, intensity, meta)
                values(:aid, :sid, 'slow', :note, CAST(:turns AS int), CAST(:intensity AS int),
                       jsonb_build_object('source','skills_bridge','speed_mod', CAST(:spd AS int)))
            """), {"aid": body.actor_id, "sid": body.session_id, "note": body.note, "turns": 1, "intensity": 1, "spd": -1})

    await session.commit()
    return {"ok": True, "applied_at_turn": cur_turn, "bridged": lbl in ("burn","poison","slow")}

@router.post("/remove")
async def remove_skill(body: SkillRemoveIn, session: AsyncSession = Depends(get_session)):
    await session.execute(text("""
        delete from actor_skills
         where actor_id=:aid and session_id=:sid and label=:lbl
    """), {"aid": body.actor_id, "sid": body.session_id, "lbl": body.label})
    await session.commit()
    return {"ok": True}
