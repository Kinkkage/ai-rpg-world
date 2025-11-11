# app/routers/do.py
from __future__ import annotations
from typing import Dict, Any, Optional, List
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB

from app.db import get_session
from app.services.llm_bus import llm_decide_hero, llm_decide_npc

router = APIRouter(prefix="/do", tags=["do"])

# ---------- MODELS ----------
class DoIn(BaseModel):
    actor_id: str
    session_id: str
    say: Optional[str] = ""
    act: Optional[str] = ""
    target_id: Optional[str] = None  # может быть пустым (социалька/манёвр)

class ResolveChoiceIn(BaseModel):
    session_id: str
    actor_id: str          # кто выбирает
    choice_id: int         # id из pending_choices

class NpcTurnIn(BaseModel):
    session_id: str
    npc_id: str
    target_id: str
    last_damage_taken: int = 0

# ---------- UTILS ----------
async def _actor_brief(session: AsyncSession, aid: str) -> Optional[Dict[str, Any]]:
    row = (await session.execute(text("""
        select a.id, a.node_id, a.x, a.y, a.stats, coalesce(a.meta,'{}'::jsonb) as meta
          from actors a where a.id=:aid
    """), {"aid": aid})).mappings().first()
    return dict(row) if row else None

async def _distance(a: Dict[str, Any], b: Optional[Dict[str, Any]]) -> int:
    if not a or not b:
        return 0
    dx = abs(int(a["x"]) - int(b["x"]))
    dy = abs(int(a["y"]) - int(b["y"]))
    return max(dx, dy)

async def _apply_damage_jsonb(session: AsyncSession, target_id: str, dmg: int) -> None:
    await session.execute(text("""
        update actors
           set stats = jsonb_set(
                coalesce(stats,'{}'::jsonb),
                '{hp}',
                to_jsonb( GREATEST(0, (coalesce((stats->>'hp')::int, 0)) - CAST(:dmg AS int)) ),
                true
           )
         where id = :tid
    """), {"tid": target_id, "dmg": int(max(0, dmg))})

async def _insert_log(
    session: AsyncSession,
    session_id: str,
    who: str,
    role: str,
    text_out: str,
    meta: dict,
    phase: str,  # turn | reaction | system
) -> int:
    # текущий ход сессии
    turn_row = (
        await session.execute(
            text("select turn_index from battle_sessions where id=:sid"),
            {"sid": session_id},
        )
    ).mappings().first()
    turn = int(turn_row["turn_index"]) if turn_row and turn_row["turn_index"] is not None else 0

    stmt = text("""
        insert into combat_log(session_id, turn_index, actor_id, role, text, meta, phase)
        values(:sid, :turn, :aid, :role, :txt, :meta, :phase)
        returning id
    """).bindparams(
        bindparam("sid"),
        bindparam("turn"),
        bindparam("aid"),
        bindparam("role"),
        bindparam("txt"),
        bindparam("meta", type_=JSONB),
        bindparam("phase"),
    )

    row = (
        await session.execute(
            stmt,
            {"sid": session_id, "turn": turn, "aid": who, "role": role, "txt": text_out, "meta": meta, "phase": phase},
        )
    ).mappings().first()
    return int(row["id"])

async def _write_choices(session: AsyncSession, session_id: str, actor_id: str, choices: List[Dict[str, Any]]) -> List[int]:
    ids: List[int] = []
    for ch in choices or []:
        row = (await session.execute(text("""
            insert into pending_choices(session_id, actor_id, label, value)
            values(:sid, :aid, :lbl, :val)
            returning id
        """), {"sid": session_id, "aid": actor_id, "lbl": ch["label"], "val": ch["value"]})).mappings().first()
        ids.append(int(row["id"]))
    return ids

    # ДОБАВЬТЕ эти утилиты в верхнюю часть файла рядом с остальными утилками:

# ---- helper: safe JSONB status write ----
from sqlalchemy import bindparam
from sqlalchemy.dialects.postgresql import JSONB

async def _give_status(
    session: AsyncSession,
    actor_id: str,
    label: str,
    turns: int = 1,
    intensity: int = 1,
    session_id: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    stmt = text("""
        insert into actor_statuses(actor_id, session_id, label, turns_left, intensity, meta)
        values(:aid, :sid, :label, :t, :i, :meta)
    """).bindparams(
        bindparam("aid"),
        bindparam("sid"),
        bindparam("label"),
        bindparam("t"),
        bindparam("i"),
        bindparam("meta", type_=JSONB),  # ВАЖНО: явный JSONB
    )
    await session.execute(stmt, {
        "aid": actor_id,
        "sid": session_id,
        "label": label,
        "t": int(turns),
        "i": int(intensity),
        "meta": meta or {}
    })


async def _apply_extra_damage(session: AsyncSession, target_id: str, dmg: int):
    await _apply_damage_jsonb(session, target_id, max(0, int(dmg)))

# ---------- RESOLVE CHOICE (один шаг) ----------
@router.post("/resolve_choice")
async def resolve_choice(body: ResolveChoiceIn, session: AsyncSession = Depends(get_session)):
    row = (await session.execute(text("""
        select id, actor_id, label, value
          from pending_choices
         where id=:id and session_id=:sid
    """), {"id": body.choice_id, "sid": body.session_id})).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="choice_not_found")

    val = (row["value"] or "").lower().strip()

    # По соглашению:
    # "sidestep"/"rush" — УДАЧНЫЕ (даём окно контратаки)
    # "cover"/"kite"    — НЕУДАЧНЫЕ (сразу последствия)
    if val in ("sidestep", "rush"):
        narration = f"{row['actor_id']} ловко уходит с линии атаки — есть окно для контратаки."
        await _insert_log(
            session,
            body.session_id,
            body.actor_id,
            "system",
            narration,
            {"resolved_choice": dict(row), "result": "counter_window"},
            phase="system",
        )
        await session.execute(text("delete from pending_choices where id=:id"), {"id": body.choice_id})
        await session.commit()
        return {"ok": True, "narration": narration, "counter_open": True}

    else:
        # Неприятные последствия (можно варьировать по типу)
        penalty = 5  # лёгкий урон за плохой выбор
        await _apply_damage_jsonb(session, body.actor_id, penalty)
        narration = f"{row['actor_id']} пытается прикрыться, но не успевает — получает урон ({penalty})."
        await _insert_log(
            session,
            body.session_id,
            body.actor_id,
            "system",
            narration,
            {"resolved_choice": dict(row), "result": "punished"},
            phase="system",
        )
        await session.execute(text("delete from pending_choices where id=:id"), {"id": body.choice_id})
        await session.commit()
        return {"ok": True, "narration": narration, "counter_open": False}



# ---------- HERO TURN ----------
@router.post("")
async def hero_do(body: DoIn, session: AsyncSession = Depends(get_session)) -> Dict[str, Any]:
    sess = (await session.execute(
        text("select 1 from battle_sessions where id=:sid and state='running'"),
        {"sid": body.session_id}
    )).scalar()
    if not sess:
        raise HTTPException(status_code=404, detail="battle_not_running")

    actor = await _actor_brief(session, body.actor_id)
    if not actor:
        raise HTTPException(status_code=404, detail="actor_not_found")

    target = await _actor_brief(session, body.target_id) if body.target_id else None
    dist = await _distance(actor, target)

    payload = {
        "actor": {"id": actor["id"], "stats": dict(actor.get("stats") or {}), "meta": dict(actor.get("meta") or {})},
        "target": {"id": target["id"], "stats": dict(target.get("stats") or {})} if target else None,
        "distance": dist,
        "say": body.say or "",
        "act": body.act or "",
    }
    decision = llm_decide_hero(payload)

    mech = decision.get("mechanics") or {}
    if mech.get("type") == "hit" and body.target_id:
        await _apply_damage_jsonb(session, body.target_id, int(mech.get("damage", 0)))

    log_id = await _insert_log(
        session,
        body.session_id,
        body.actor_id,
        "hero",
        decision.get("narration", ""),
        {"input": body.dict(), "decision": decision},
        phase="turn",
    )

    choice_ids: List[int] = []
    if decision.get("choices"):
        choice_ids = await _write_choices(session, body.session_id, body.actor_id, decision["choices"])

    await session.commit()
    return {
        "ok": True,
        "log_id": log_id,
        "applied": mech,
        "choices_ids": choice_ids,
        "narration": decision.get("narration", "")
    }

# ---------- NPC TURN ----------
@router.post("/npc_turn")
async def npc_turn(body: NpcTurnIn, session: AsyncSession = Depends(get_session)):
    npc = await _actor_brief(session, body.npc_id)
    target = await _actor_brief(session, body.target_id)
    if not npc or not target:
        raise HTTPException(status_code=404, detail="actor_not_found")

    dist = await _distance(npc, target)

    decision = llm_decide_npc({
        "actor": {"id": npc["id"], "stats": dict(npc.get("stats") or {}), "meta": dict(npc.get("meta") or {})},
        "target": {"id": target["id"], "stats": dict(target.get("stats") or {})},
        "distance": dist,
        "last_damage_taken": int(body.last_damage_taken),
    })

    mech = decision.get("mechanics") or {}
    if mech.get("type") == "hit":
        await _apply_damage_jsonb(session, body.target_id, int(mech.get("damage", 0)))

    log_id = await _insert_log(
        session,
        body.session_id,
        body.npc_id,
        "npc",
        decision.get("narration", ""),
        {"decision": decision},
        phase="reaction",
    )

    choice_ids: List[int] = []
    if decision.get("choices"):
        choice_ids = await _write_choices(session, body.session_id, body.npc_id, decision["choices"])

    await session.commit()
    return {
        "ok": True,
        "log_id": log_id,
        "applied": mech,
        "choices_ids": choice_ids,
        "narration": decision.get("narration", "")
    }

# ---------- RESOLVE CHOICE ----------
@router.post("/resolve_choice")
async def resolve_choice(body: ResolveChoiceIn, session: AsyncSession = Depends(get_session)):
    row = (await session.execute(text("""
        select id, actor_id, label, value
          from pending_choices
         where id=:id and session_id=:sid
    """), {"id": body.choice_id, "sid": body.session_id})).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="choice_not_found")

    narration = f"{row['actor_id']} выбирает: {row['label']}."
    await _insert_log(
        session,
        body.session_id,
        body.actor_id,
        "system",
        narration,
        {"resolved_choice": dict(row)},
        phase="system",
    )

    await session.execute(text("delete from pending_choices where id=:id"), {"id": body.choice_id})
    await session.commit()
    return {"ok": True, "narration": narration}
