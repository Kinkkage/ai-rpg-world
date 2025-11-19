from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB

from app.db import get_session
from app.services.llm_bus import decide_hero, decide_npc
from app.services.llm_models import LLMDecision
from app.dao import fetch_inventory

router = APIRouter(prefix="/do", tags=["do"])


# ---------- Pydantic-модели запросов ----------

class DoIn(BaseModel):
    actor_id: str
    session_id: str
    say: Optional[str] = ""
    act: Optional[str] = ""
    target_id: Optional[str] = None  # может быть пустым (социалька/манёвр)


class ResolveChoiceIn(BaseModel):
    session_id: str
    actor_id: str          # кто выбирает (обычно hero)
    choice_id: int         # id из pending_choices


class NpcTurnIn(BaseModel):
    session_id: str
    npc_id: str
    target_id: str
    last_damage_taken: int = 0
    last_hero_say: Optional[str] = ""   # чтобы NPC слышал героя
    last_hero_act: Optional[str] = ""   # и видел действие героя


# ---------- Утилиты: актёр / дистанция / урон / лог ----------

async def _actor_brief(session: AsyncSession, aid: str) -> Optional[Dict[str, Any]]:
    row = (
        await session.execute(
            text("""
                select a.id, a.node_id, a.x, a.y, a.stats, coalesce(a.meta, '{}'::jsonb) as meta
                  from actors a
                 where a.id = :aid
            """),
            {"aid": aid},
        )
    ).mappings().first()
    return dict(row) if row else None


def _distance(a: Optional[Dict[str, Any]], b: Optional[Dict[str, Any]]) -> int:
    if not a or not b:
        return 0
    dx = abs(int(a["x"]) - int(b["x"]))
    dy = abs(int(a["y"]) - int(b["y"]))
    return max(dx, dy)


async def _apply_damage_jsonb(session: AsyncSession, target_id: str, dmg: int) -> None:
    """
    Аккуратно вычитаем урон из stats->hp у цели.
    """
    await session.execute(
        text("""
            update actors
               set stats = jsonb_set(
                    coalesce(stats,'{}'::jsonb),
                    '{hp}',
                    to_jsonb(
                        GREATEST(
                            0,
                            (coalesce((stats->>'hp')::int, 0)) - CAST(:dmg AS int)
                        )
                    ),
                    true
               )
             where id = :tid
        """),
        {"tid": target_id, "dmg": int(max(0, dmg))},
    )


async def _insert_log(
    session: AsyncSession,
    session_id: str,
    who: str,
    role: str,
    text_out: str,
    meta: dict,
    phase: str,  # "turn" | "reaction" | "system"
) -> int:
    """
    Пишем запись в combat_log, автоматически подставляя текущий turn_index.
    """
    turn_row = (
        await session.execute(
            text("select turn_index from battle_sessions where id = :sid"),
            {"sid": session_id},
        )
    ).mappings().first()
    turn = int(turn_row["turn_index"]) if turn_row and turn_row["turn_index"] is not None else 0

    stmt = text("""
        insert into combat_log(session_id, turn_index, actor_id, role, text, meta, phase)
        values (:sid, :turn, :aid, :role, :txt, :meta, :phase)
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
            {
                "sid": session_id,
                "turn": turn,
                "aid": who,
                "role": role,
                "txt": text_out,
                "meta": meta,
                "phase": phase,
            },
        )
    ).mappings().first()
    return int(row["id"])


async def _write_choices(
    session: AsyncSession,
    session_id: str,
    actor_id: str,
    choices: List[Dict[str, Any]],
) -> List[int]:
    """
    Сохраняем варианты выбора (увороты/манёвры) в pending_choices.
    """
    ids: List[int] = []
    for ch in choices or []:
        row = (
            await session.execute(
                text("""
                    insert into pending_choices(session_id, actor_id, label, value)
                    values (:sid, :aid, :lbl, :val)
                    returning id
                """),
                {
                    "sid": session_id,
                    "aid": actor_id,
                    "lbl": ch["label"],
                    "val": ch["value"],
                },
            )
        ).mappings().first()
        ids.append(int(row["id"]))
    return ids


# ---------- Память боя для LLM ----------

async def _get_battle_history(
    session: AsyncSession,
    session_id: str,
    limit: int = 8,
) -> List[Dict[str, Any]]:
    """
    Последние N записей из combat_log по этой сессии
    (в лёгком JSON-формате для LLM).
    """
    rows = (
        await session.execute(
            text("""
                select turn_index, actor_id, role, text, phase
                  from combat_log
                 where session_id = :sid
                 order by id desc
                 limit :lim
            """),
            {"sid": session_id, "lim": limit},
        )
    ).mappings().all()

    # Разворачиваем в хронологический порядок (от старых к новым)
    return [dict(r) for r in reversed(rows)]


# ---------- Навыки / статусы / инвентарь для LLM ----------

async def _get_actor_active_skills(
    session: AsyncSession,
    actor_id: str,
    session_id: str,
) -> List[Dict[str, Any]]:
    """
    Активные навыки актёра в рамках боевой сессии.
    Берём те, у которых ещё не вышел срок действия.
    """
    sess = (
        await session.execute(
            text("select turn_index from battle_sessions where id = :sid"),
            {"sid": session_id},
        )
    ).mappings().first()
    if not sess:
        return []
    cur_turn = int(sess["turn_index"])

    rows = (
        await session.execute(
            text("""
                select label, note, tags, applied_at_turn, duration_turns
                  from actor_skills
                 where actor_id = :aid
                   and session_id = :sid
                 order by id desc
            """),
            {"aid": actor_id, "sid": session_id},
        )
    ).mappings().all()

    out: List[Dict[str, Any]] = []
    for r in rows:
        if int(r["applied_at_turn"]) + int(r["duration_turns"]) > cur_turn:
            out.append(
                {
                    "label": r["label"],
                    "note": r["note"],
                    "tags": r["tags"],
                    "applied_at_turn": r["applied_at_turn"],
                    "duration_turns": r["duration_turns"],
                }
            )
    return out


async def _get_actor_statuses(
    session: AsyncSession,
    actor_id: str,
    session_id: str,
) -> List[Dict[str, Any]]:
    """
    !!! ВРЕМЕННЫЙ СТАБ !!!

    Мы не трогаем таблицу actor_statuses, т.к. схема БД может отличаться.
    Чтобы не ловить ошибок по несуществующим колонкам, просто возвращаем
    пустой список статусов.

    LLM это не сломает — он просто не увидит активные статусы.
    Позже можно аккуратно подключить реальные статусы, когда будет понятна
    точная структура actor_statuses (из DDL/dao_status.py).
    """
    return []


async def _build_actor_context(
    session: AsyncSession,
    actor_id: str,
    session_id: str,
) -> Dict[str, Any]:
    """
    Полный контекст актёра (stats/meta + inventory + skills + statuses)
    для передачи в LLM.
    """
    row = await _actor_brief(session, actor_id)
    if not row:
        raise HTTPException(status_code=404, detail="actor_not_found")

    inventory = await fetch_inventory(session, actor_id)
    skills = await _get_actor_active_skills(session, actor_id, session_id)
    statuses = await _get_actor_statuses(session, actor_id, session_id)

    return {
        "id": row["id"],
        "stats": dict(row.get("stats") or {}),
        "meta": dict(row.get("meta") or {}),
        "inventory": inventory,
        "skills": skills,
        "statuses": statuses,
    }


# ---------- ХОД ГЕРОЯ ----------

@router.post("")
async def hero_do(body: DoIn, session: AsyncSession = Depends(get_session)) -> Dict[str, Any]:
    # проверяем, что бой идёт
    sess = (
        await session.execute(
            text("select 1 from battle_sessions where id = :sid and state = 'running'"),
            {"sid": body.session_id},
        )
    ).scalar()
    if not sess:
        raise HTTPException(status_code=404, detail="battle_not_running")

    # Полный контекст героя и цели
    actor_ctx = await _build_actor_context(session, body.actor_id, body.session_id)
    target_ctx: Optional[Dict[str, Any]] = None
    if body.target_id:
        target_ctx = await _build_actor_context(session, body.target_id, body.session_id)

    # Для дистанции нужны только координаты
    actor_brief = await _actor_brief(session, body.actor_id)
    target_brief = await _actor_brief(session, body.target_id) if body.target_id else None
    dist = _distance(actor_brief, target_brief)

    # История боя для памяти сцены
    battle_history = await _get_battle_history(session, body.session_id, limit=8)

    payload = {
        "actor": actor_ctx,
        "target": target_ctx,
        "distance": dist,
        "say": body.say or "",
        "act": body.act or "",
        "battle_history": battle_history,
    }

    decision: LLMDecision = await decide_hero(payload)
    mech = decision.mechanics

    # Применяем урон по цели, если ход — успешная атака
    if mech.type == "hit" and body.target_id:
        await _apply_damage_jsonb(session, body.target_id, int(mech.damage))

    log_id = await _insert_log(
        session,
        body.session_id,
        body.actor_id,
        "hero",
        decision.narration,
        {
            "input": body.dict(),
            "decision": decision.dict(),
        },
        phase="turn",
    )

    choice_ids: List[int] = []
    if decision.choices:
        choices_payload = [{"label": c.label, "value": c.value} for c in decision.choices]
        choice_ids = await _write_choices(session, body.session_id, body.actor_id, choices_payload)

    await session.commit()

    return {
        "ok": True,
        "log_id": log_id,
        "applied": {
            "type": mech.type,
            "damage": mech.damage,
            "status": mech.status,
        },
        "choices_ids": choice_ids,
        "narration": decision.narration,
    }


# ---------- ХОД NPC ----------

@router.post("/npc_turn")
async def npc_turn(body: NpcTurnIn, session: AsyncSession = Depends(get_session)) -> Dict[str, Any]:
    # проверяем, что бой идёт
    sess = (
        await session.execute(
            text("select 1 from battle_sessions where id = :sid and state = 'running'"),
            {"sid": body.session_id},
        )
    ).scalar()
    if not sess:
        raise HTTPException(status_code=404, detail="battle_not_running")

    npc_ctx = await _build_actor_context(session, body.npc_id, body.session_id)
    target_ctx = await _build_actor_context(session, body.target_id, body.session_id)

    npc_brief = await _actor_brief(session, body.npc_id)
    target_brief = await _actor_brief(session, body.target_id)
    if not npc_brief or not target_brief:
        raise HTTPException(status_code=404, detail="actor_not_found")

    dist = _distance(npc_brief, target_brief)
    battle_history = await _get_battle_history(session, body.session_id, limit=8)

    payload = {
        "actor": npc_ctx,
        "target": target_ctx,
        "distance": dist,
        "last_damage_taken": int(body.last_damage_taken),
        "hero_say": body.last_hero_say or "",
        "hero_act": body.last_hero_act or "",
        "battle_history": battle_history,
    }

    decision: LLMDecision = await decide_npc(payload)
    mech = decision.mechanics

    if mech.type == "hit":
        await _apply_damage_jsonb(session, body.target_id, int(mech.damage))

    log_id = await _insert_log(
        session,
        body.session_id,
        body.npc_id,
        "npc",
        decision.narration,
        {"decision": decision.dict()},
        phase="reaction",
    )

    choice_ids: List[int] = []
    if decision.choices:
        choices_payload = [{"label": c.label, "value": c.value} for c in decision.choices]
        choice_ids = await _write_choices(session, body.session_id, body.npc_id, choices_payload)

    await session.commit()

    return {
        "ok": True,
        "log_id": log_id,
        "applied": {
            "type": mech.type,
            "damage": mech.damage,
            "status": mech.status,
        },
        "choices_ids": choice_ids,
        "narration": decision.narration,
    }


# ---------- RESOLVE CHOICE (контратака / наказание) ----------

@router.post("/resolve_choice")
async def resolve_choice(body: ResolveChoiceIn, session: AsyncSession = Depends(get_session)) -> Dict[str, Any]:
    row = (
        await session.execute(
            text("""
                select id, actor_id, label, value
                  from pending_choices
                 where id = :id
                   and session_id = :sid
            """),
            {"id": body.choice_id, "sid": body.session_id},
        )
    ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="choice_not_found")

    val = (row["value"] or "").lower().strip()

    # "sidestep"/"rush" — удачные (даём окно контратаки)
    # "cover"/"kite"    — неудачные (сразу урон/наказание)
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
        await session.execute(text("delete from pending_choices where id = :id"), {"id": body.choice_id})
        await session.commit()
        return {"ok": True, "narration": narration, "counter_open": True}
    else:
        penalty = 5
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
        await session.execute(text("delete from pending_choices where id = :id"), {"id": body.choice_id})
        await session.commit()
        return {"ok": True, "narration": narration, "counter_open": False}
