from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import random
import re  # для разрезания narration на речь и действие

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB

from app.db import get_session
from app.services.llm_bus import decide_hero, decide_npc
from app.services.llm_models import LLMDecision
from app.dao import fetch_inventory, _spend_one_charge, handle_actor_death


router = APIRouter(prefix="/do", tags=["do"])

# --------- КОНСТАНТЫ ДЛЯ "АДА" (герой не умирает, а телепортируется) ---------

# TODO: здесь потом подставишь реальные id узла/локации ада и спавн-координаты
HELL_NODE_ID = "hell_node"   # id узла/локации для "ада"
HELL_SPAWN_X = 0
HELL_SPAWN_Y = 0


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
async def _apply_wound_status(
    session: AsyncSession,
    target_id: str,
    status: Optional[Dict[str, Any]],
) -> None:
    """
    Проставляем/обновляем рану в stats->wounds у цели.

    Ожидаем, что status ~ {
      "location": "head" | "torso" | "left_arm" | "right_arm" | "left_leg" | "right_leg",
      "severity": "light" | "heavy",
      "bleeding": true/false
    }
    """
    if not status:
        return

    loc = status.get("location")
    if not loc:
        return

    severity = status.get("severity", "light")
    bleeding = bool(status.get("bleeding", False))

    await session.execute(
        text(
            """
            update actors
               set stats = jsonb_set(
                    coalesce(stats, '{}'::jsonb),
                    '{wounds}',
                    coalesce(stats->'wounds', '{}'::jsonb) ||
                      jsonb_build_object(
                        CAST(:loc AS text),
                        jsonb_build_object(
                            'severity', CAST(:severity AS text),
                            'bleeding', CAST(:bleeding AS boolean)
                        )
                      ),
                    true
               )
             where id = :tid
            """
        ),
        {
            "loc": loc,
            "severity": severity,
            "bleeding": bleeding,
            "tid": target_id,
        },
    )


def _split_narration_fields(narration: str) -> Dict[str, str]:
    """
    Делим narration на две части:
    - say_out: то, что персонаж сказал (первая найденная прямая речь в кавычках)
    - act_out: остальное (действия, описание).
    """
    narration = (narration or "").strip()
    if not narration:
        return {"say_out": "", "act_out": ""}

    # Ищем первую пару кавычек с текстом внутри
    m = re.search(r'"([^"]+)"', narration)
    if not m:
        # нет прямой речи — всё считаем действием
        return {"say_out": "", "act_out": narration}

    say_text = m.group(1).strip()

    before = narration[:m.start()]
    after = narration[m.end():]

    # Склеиваем всё, что не речь
    glue = (before + " " + after).strip()

    # Удаляем типичные "глагол речи + двоеточие" около кавычек из действия
    # Примеры: "NPC рычит:", "он говорит:", "он кричит:"
    glue = re.sub(
        r"\b(говорит|рычит|кричит|шипит|шепчет|усмехается|произносит)\s*:",
        "",
        glue,
        flags=re.IGNORECASE,
    )

    # Чистим двойные пробелы и лишние пробелы перед запятыми
    glue = re.sub(r"\s+", " ", glue)
    glue = glue.replace(" ,", ",").strip()

    return {
        "say_out": say_text,
        "act_out": glue,
    }

    


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
    Пишем запись в combat_log, автоматически подставляя текущий turn_index
    И дописываем в meta информацию о локации (node_id), если можем.
    """
    # базовый turn_index из battle_sessions
    turn_row = (
        await session.execute(
            text("select turn_index from battle_sessions where id = :sid"),
            {"sid": session_id},
        )
    ).mappings().first()
    turn = int(turn_row["turn_index"]) if turn_row and turn_row["turn_index"] is not None else 0

    # копия meta, чтобы не мутировать исходный dict снаружи
    meta_out: Dict[str, Any] = dict(meta or {})

    # попробуем узнать локацию актёра (node_id)
    actor_row = await _actor_brief(session, who)
    if actor_row:
        node_id = actor_row.get("node_id")
        if node_id is not None:
            loc = dict(meta_out.get("location") or {})
            loc["node_id"] = node_id
            meta_out["location"] = loc

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
                "meta": meta_out,
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
    в лёгком JSON-формате для LLM.

    Теперь, помимо текста, прокидываем ещё и раздельные поля:
    - say_out: что сказал актёр (если было)
    - act_out: что сделал актёр (если было)
    """
    rows = (
        await session.execute(
            text("""
                select turn_index, actor_id, role, text, phase, meta
                  from combat_log
                 where session_id = :sid
                 order by id desc
                 limit :lim
            """),
            {"sid": session_id, "lim": limit},
        )
    ).mappings().all()

    history: List[Dict[str, Any]] = []

    for r in reversed(rows):
        item: Dict[str, Any] = {
            "turn_index": r["turn_index"],
            "actor_id": r["actor_id"],
            "role": r["role"],
            "text": r["text"],
            "phase": r["phase"],
        }

        meta = r.get("meta") or {}
        if isinstance(meta, dict):
            if "say_out" in meta:
                item["say_out"] = meta["say_out"]
            if "act_out" in meta:
                item["act_out"] = meta["act_out"]

        history.append(item)

    return history


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


# ---------- Перемещение актёра к цели / от цели ----------

async def _move_actor_towards(
    session: AsyncSession,
    mover_id: str,
    target_brief: Dict[str, Any],
    max_steps: int = 1,
) -> Dict[str, Any]:
    """
    Простейшее передвижение актёра НАВСТРЕЧУ цели.
    Двигаем не более чем на max_steps клеток за ход.
    """
    mover = await _actor_brief(session, mover_id)
    if not mover or not target_brief:
        return mover or {}

    x = int(mover["x"])
    y = int(mover["y"])
    tx = int(target_brief["x"])
    ty = int(target_brief["y"])

    steps_left = max_steps

    while steps_left > 0:
        dx = tx - x
        dy = ty - y
        if dx == 0 and dy == 0:
            break  # уже стоим на той же клетке

        step_x = 0
        step_y = 0
        if dx != 0:
            step_x = 1 if dx > 0 else -1
        if dy != 0:
            step_y = 1 if dy > 0 else -1

        x += step_x
        y += step_y
        steps_left -= 1

    # Обновляем координаты в БД
    await session.execute(
        text("""
            update actors
               set x = :x, y = :y
             where id = :aid
        """),
        {"x": x, "y": y, "aid": mover_id},
    )

    mover["x"] = x
    mover["y"] = y
    return mover


async def _move_actor_away(
    session: AsyncSession,
    mover_id: str,
    target_brief: Dict[str, Any],
    max_steps: int = 1,
) -> Dict[str, Any]:
    """
    Простейшее передвижение актёра ОТ цели (наоборот).
    """
    mover = await _actor_brief(session, mover_id)
    if not mover or not target_brief:
        return mover or {}

    x = int(mover["x"])
    y = int(mover["y"])
    tx = int(target_brief["x"])
    ty = int(target_brief["y"])

    steps_left = max_steps

    while steps_left > 0:
        dx = x - tx
        dy = y - ty
        if dx == 0 and dy == 0:
            # стоим на одной клетке — сделаем шаг "вправо-вниз", чтобы уйти
            dx, dy = 1, 1

        step_x = 0
        step_y = 0
        if dx != 0:
            step_x = 1 if dx > 0 else -1
        if dy != 0:
            step_y = 1 if dy > 0 else -1

        x += step_x
        y += step_y
        steps_left -= 1

    await session.execute(
        text("""
            update actors
               set x = :x, y = :y
             where id = :aid
        """),
        {"x": x, "y": y, "aid": mover_id},
    )

    mover["x"] = x
    mover["y"] = y
    return mover


async def _move_hero_by_act(
    session: AsyncSession,
    hero_id: str,
    target_brief: Optional[Dict[str, Any]],
    act_text: str,
    max_steps: int = 3,
) -> None:
    """
    Простая логика движения героя по тексту act.

    МУЛЬТИЯЗЫЧНЫЕ ТРИГГЕРЫ (RU / EN / NL / UK):
    - если в act есть одно из "towards_triggers" — двигаем ГЕРОЯ к цели (если она есть);
    - если одно из "away_triggers" — двигаем ОТ цели;
    - максимум max_steps клеток за ход.
    """
    text_lower = (act_text or "").lower()
    if not text_lower.strip():
        return

    if not target_brief:
        # Пока без цели не заморачиваемся
        return

    # --- ДВИЖЕНИЕ К ЦЕЛИ ---
    towards_triggers = (
        # RU
        "подхож",      # подхожу, подхожу ближе
        "подбег",      # подбегаю
        "ближе",       # становлюсь ближе
        "сближаюсь",   # сближаюсь с ним

        # EN
        "approach",        # I approach him
        "move closer",     # move closer to him
        "step closer",     # step closer
        "come closer",     # come closer
        "run toward",      # run toward him / towards him
        "run towards",

        # NL
        "loop naar",       # loop naar hem
        "ga naar",         # ga naar hem
        "ren naar",        # ren naar hem
        "dichterbij",      # kom dichterbij

        # UK
        "підход",          # підходжу
        "наближ",          # наближаюсь
        "йду до",          # йду до нього
        "біжу до",         # біжу до нього
    )

    # --- ДВИЖЕНИЕ ОТ ЦЕЛИ ---
    away_triggers = (
        # RU
        "отхож",       # отхожу
        "отступ",      # отступаю
        "назад",       # шаг назад
        "отпрыг",      # отпрыгиваю
        "отскаки",     # отскакиваю
        "держусь подальше",

        # EN
        "step back",       # step back
        "move back",       # move back
        "back off",        # back off
        "fall back",       # fall back
        "run away",        # run away from him
        "retreat",         # retreat

        # NL
        "ga weg",          # ga weg
        "loop weg",        # loop weg
        "ren weg",         # ren weg
        "achteruit",       # stap achteruit
        "terug",           # een stap terug

        # UK
        "відход",          # відходжу
        "відступ",         # відступаю
        "назад",           # крок назад
        "відбіг",          # відбігаю
    )

    if any(t in text_lower for t in towards_triggers):
        await _move_actor_towards(session, hero_id, target_brief, max_steps=max_steps)
    elif any(t in text_lower for t in away_triggers):
        await _move_actor_away(session, hero_id, target_brief, max_steps=max_steps)
    else:
        return



# ---------- "Смерть" героя как телепорт в ад ----------

async def _teleport_actor_to_node(
    session: AsyncSession,
    actor_id: str,
    node_id: str,
    x: int,
    y: int,
) -> None:
    """
    Перемещаем актёра на указанный узел/локацию и координаты.
    """
    await session.execute(
        text("""
            update actors
               set node_id = :node_id,
                   x = :x,
                   y = :y
             where id = :aid
        """),
        {"node_id": node_id, "x": x, "y": y, "aid": actor_id},
    )


async def _handle_hero_zero_hp(
    session: AsyncSession,
    hero_id: str,
    session_id: str,
) -> None:
    """
    Если у героя hp <= 0 — никакой смерти,
    а телепорт в "ад" (HELL_NODE_ID, HELL_SPAWN_X/Y) + запись в лог.
    """
    brief = await _actor_brief(session, hero_id)
    if not brief:
        return

    stats = dict(brief.get("stats") or {})
    hp = int((stats.get("hp") or 0))

    if hp > 0:
        return  # герой ещё жив, ничего не делаем

    # Телепортируем героя в "ад"
    await _teleport_actor_to_node(session, hero_id, HELL_NODE_ID, HELL_SPAWN_X, HELL_SPAWN_Y)

    narration = (
        "Герой теряет последние силы, мир вокруг рвётся на куски, "
        "и сознание проваливается во тьму. "
        "Когда он приходит в себя, вокруг уже другое место."
    )

    await _insert_log(
        session,
        session_id,
        hero_id,
        "system",
        narration,
        {
            "event": "hero_teleport_on_zero_hp",
            "to_node_id": HELL_NODE_ID,
        },
        phase="system",
    )

async def _maybe_throw_item_by_act(
    session: AsyncSession,
    actor_id: str,
    act_text: str,
) -> None:
    """
    Простая логика 'броска' предмета из руки по тексту act.

    Если в тексте есть триггеры типа 'бросаю / кидаю / throw / gooi',
    мы пытаемся понять:
    - какой предмет в руке бросить,
    - и убираем его из inventories.left_item / right_item.

    Пока предмет просто "теряется": owner_actor = NULL.
    Позже можно будет класть его в мир (node_objects + object_inventories).
    """
    text_lower = (act_text or "").lower().strip()
    if not text_lower:
        return

    # Триггеры "броска" на разных языках
    throw_triggers = (
        # RU
        "бросаю", "бросить", "кидаю", "кинуть", "швыряю", "швырнуть",
        # EN
        "throw ", "throwing", "toss ", "tossing", "chuck ",
        # NL
        "gooi ", "gooien", "werp ", "werpen",
        # UK
        "кидаю", "кинути", "жбурляю", "жбурнути",
    )

    if not any(t in text_lower for t in throw_triggers):
        # нет явного броска — ничего не делаем
        return

    # Смотрим, что сейчас в руках у актёра
    inv_row = (
        await session.execute(
            text("""
                select left_item, right_item
                  from inventories
                 where actor_id = :aid
            """),
            {"aid": actor_id},
        )
    ).mappings().first()

    if not inv_row:
        return

    left_id = inv_row["left_item"]
    right_id = inv_row["right_item"]

    if not left_id and not right_id:
        # в руках пусто — нечего бросать
        return

    # Подтягиваем названия предметов, чтобы прикинуть, что именно бросают
    items_info = {}
    if left_id or right_id:
        rows = (
            await session.execute(
                text("""
                    select i.id,
                           i.kind_id,
                           coalesce(ik.title, '') as kind_title,
                           coalesce(i.meta->>'title', '') as meta_title
                      from items i
                      left join item_kinds ik on ik.id = i.kind_id
                     where i.id = any(:ids)
                """),
                {"ids": [x for x in (left_id, right_id) if x]},
            )
        ).mappings().all()
        for r in rows:
            items_info[str(r["id"])] = {
                "kind_id": r["kind_id"],
                "kind_title": (r["kind_title"] or "").lower(),
                "meta_title": (r["meta_title"] or "").lower(),
            }

    def _match_item(item_id: Optional[str]) -> bool:
        if not item_id:
            return False
        info = items_info.get(str(item_id))
        if not info:
            return False

        names = []
        for name in (info["kind_title"], info["meta_title"]):
            if name:
                names.extend(name.split())

        for name in names:
            name = name.strip()
            if len(name) < 3:
                continue
            if name in text_lower:
                return True

        return False


    item_to_throw = None

    # 1) если в act упоминается название предмета (хотя бы одно слово) — бросаем именно его
    if left_id and _match_item(left_id):
        item_to_throw = left_id
    elif right_id and _match_item(right_id):
        item_to_throw = right_id
    else:
        # 2) если ничего явно не упомянуто — бросаем то, что в правой руке,
        #    если она не пустая, иначе — левую
        if right_id:
            item_to_throw = right_id
        else:
            item_to_throw = left_id

    if not item_to_throw:
        return

    # Убираем предмет из рук
    await session.execute(
        text("""
            update inventories
               set left_item = case when left_item = :it then null else left_item end,
                   right_item = case when right_item = :it then null else right_item end
             where actor_id = :aid
        """),
        {"aid": actor_id, "it": item_to_throw},
    )

    # И "отвязываем" предмет от владельца
    await session.execute(
        text("""
            update items
               set owner_actor = null
             where id = :it
        """),
        {"it": item_to_throw},
    )




async def _maybe_spend_ammo_for_hero(
    session: AsyncSession,
    actor_ctx: Dict[str, Any],
    act_text: str,
) -> None:
    """
    Если герой по тексту реально стреляет (стреляю/shoot/fire) и в руке есть
    дальнобойное оружие (tags содержит ranged/gun/bow) — списываем 1 charge
    у этого оружия (через общий DAO-хелпер _spend_one_charge).
    """
    text_lower = (act_text or "").lower().strip()
    if not text_lower:
        return

    shoot_triggers = (
        "стреляю",
        "стрелять",
        "выстрел",
        "выстрелить",
        "делаю выстрел",
        "стреляю из",
        "shoot",
        "firing",
        "fire ",
        "shot ",
        "open fire",
        "открываю огонь",
    )
    if not any(tr in text_lower for tr in shoot_triggers):
        return

    inv = (actor_ctx.get("inventory") or {})

    # сначала правая рука, потом левая
    for hand_key in ("right_hand", "left_hand"):
        hand = inv.get(hand_key) or {}
        item = hand.get("item")
        if not item:
            continue

        tags = [str(t).lower() for t in (item.get("tags") or [])]
        if not any(t in tags for t in ("ranged", "gun", "bow")):
            continue

        item_id = item.get("id")
        if not item_id:
            continue

        # списываем 1 заряд (патрон/выстрел) из этого оружия
        await _spend_one_charge(session, item_id)
        break  # один выстрел — один ствол

    


def _estimate_shots_from_text(act_text: str, max_shots: int = 6) -> int:
    """
    Грубая оценка количества выстрелов по тексту act.
    - Ищем первую цифру: 'стреляю 3 раза' -> 3.
    - Ограничиваем 1..max_shots.
    - Если цифры нет — считаем, что 1 выстрел.
    """
    if not act_text:
        return 1

    s = act_text.lower()

    # 1) Явное число
    m = re.search(r"\d+", s)
    if m:
        try:
            n = int(m.group(0))
        except ValueError:
            n = 1
        if n <= 0:
            return 1
        return max(1, min(n, max_shots))

    # 2) Пара словесных паттернов
    patterns = {
        "дважды": 2,
        "два раза": 2,
        "трижды": 3,
        "три раза": 3,
        "twice": 2,
        "two times": 2,
        "three times": 3,
        "twee keer": 2,
        "drie keer": 3,
    }
    for phrase, n in patterns.items():
        if phrase in s:
            return n

    # 3) "несколько раз" / "many times"
    if "несколько раз" in s or "many times" in s:
        return 3

    return 1

    from typing import Any, Dict  # если уже есть в файле – не дублируй

async def _spend_ranged_charges_from_act(
    session: AsyncSession,
    actor_ctx: Dict[str, Any],
    act_text: str,
) -> None:
    """
    Списываем charges с дальнобойного оружия в руках героя,
    исходя из текста act (кол-во выстрелов).

    Работает для любых предметов с тегами 'gun' или 'ranged'.
    НЕ трогаем charges, если в act нет явных глаголов стрельбы.
    """
    if not act_text:
        return

    s = act_text.lower()

    # триггеры стрельбы, чтобы не жрать патроны при "бью рукоятью"
    shoot_triggers = (
        # RU
        "стреляю", "выстрел", "очередь", "делаю выстрел",
        # EN
        "shoot", "firing", "fire a shot", "fire at",
        # NL
        "schiet", "schieten", "vuur", "vuren",
        # UK
        "стріляю", "постріл", "роблю постріл",
    )

    if not any(t in s for t in shoot_triggers):
        # нет явной стрельбы — ничего не списываем
        return

    inv = (actor_ctx.get("inventory") or {})
    candidates = []

    # приоритет: правая рука, потом левая
    for hand_key in ("right_hand", "left_hand"):
        hand = inv.get(hand_key) or {}
        item = hand.get("item")
        if not item:
            continue
        tags = item.get("tags") or []
        if "gun" in tags or "ranged" in tags:
            candidates.append(item)

    if not candidates:
        return

    weapon = candidates[0]
    weapon_id = weapon.get("id")
    if not weapon_id:
        return

    shots = _estimate_shots_from_text(act_text, max_shots=6)
    if shots <= 0:
        return

    # используем уже существующий _spend_one_charge
    from app.dao import _spend_one_charge  # если у тебя импорт уже есть наверху файла — эту строку можно убрать

    for _ in range(shots):
        await _spend_one_charge(session, weapon_id)




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

    # Полный контекст героя и цели (для LLM и для логики ран/патронов)
    actor_ctx = await _build_actor_context(session, body.actor_id, body.session_id)
    target_ctx: Optional[Dict[str, Any]] = None
    if body.target_id:
        target_ctx = await _build_actor_context(session, body.target_id, body.session_id)

    # Сначала пробуем сдвинуть героя по тексту act (до расчёта дистанции)
    target_brief_for_move = await _actor_brief(session, body.target_id) if body.target_id else None
    if body.act:
        await _move_hero_by_act(session, body.actor_id, target_brief_for_move, body.act, max_steps=3)

    # После возможного движения берём актуальные координаты
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

    # Решение LLM (нарратив + mechanics)
    decision: LLMDecision = await decide_hero(payload)
    mech = decision.mechanics

    # 1) Пытаемся выбросить предмет по ключевым словам ("бросаю"/"кидаю"/"throw" и т.п.)
    await _maybe_throw_item_by_act(session, body.actor_id, body.act or "")

    # 2) Списываем патроны / charges с дальнобойного оружия,
    #    если в act явно описана стрельба.
    await _spend_ranged_charges_from_act(session, actor_ctx, body.act or "")

    # Разделяем narration на "что сказал" / "что сделал"
    split = _split_narration_fields(decision.narration)
    say_out = split["say_out"]
    act_out = split["act_out"]

    # Применяем урон по цели, если ход — успешная атака
    if mech.type == "hit" and body.target_id:
        dmg_int = int(mech.damage)
        await _apply_damage_jsonb(session, body.target_id, dmg_int)

        # Если цель — герой, проверяем "смерть" как телепорт в ад
        if body.target_id == "hero":
            await _handle_hero_zero_hp(session, body.target_id, body.session_id)
        else:
            # Если это NPC/другой актёр — проверяем, не упал ли он до 0 hp,
            # и если да — вызываем handle_actor_death (труп + лут)
            brief = await _actor_brief(session, body.target_id)
            if brief:
                stats = dict(brief.get("stats") or {})
                cur_hp = int(stats.get("hp") or 0)
                if cur_hp <= 0:
                    await handle_actor_death(session, body.target_id)

    log_id = await _insert_log(
        session,
        body.session_id,
        body.actor_id,
        "hero",
        decision.narration,
        {
            "input": body.dict(),
            "decision": decision.dict(),
            "say_out": say_out,
            "act_out": act_out,
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
        "say_out": say_out,
        "act_out": act_out,
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

    # Полный контекст NPC и цели
    npc_ctx = await _build_actor_context(session, body.npc_id, body.session_id)
    target_ctx = await _build_actor_context(session, body.target_id, body.session_id)

    # ------ ПРОВЕРКА: NPC МЁРТВ/ВЫВЕДЕН ИЗ БОЯ ------
    npc_hp = int((npc_ctx.get("stats") or {}).get("hp", 0) or 0)
    if npc_hp <= 0:
        narration = "Тело NPC остаётся неподвижным — он больше не способен действовать в бою."
        split = _split_narration_fields(narration)
        log_id = await _insert_log(
            session,
            body.session_id,
            body.npc_id,
            "npc",
            narration,
            {
                "reason": "npc_hp_zero_or_below",
                "say_out": split["say_out"],
                "act_out": split["act_out"],
            },
            phase="reaction",
        )
        await session.commit()
        return {
            "ok": True,
            "log_id": log_id,
            "applied": {
                "type": "none",
                "damage": 0,
                "status": None,
            },
            "choices_ids": [],
            "narration": narration,
            "say_out": split["say_out"],
            "act_out": split["act_out"],
        }

    # Краткие данные для дистанции
    npc_brief = await _actor_brief(session, body.npc_id)
    target_brief = await _actor_brief(session, body.target_id)
    if not npc_brief or not target_brief:
        raise HTTPException(status_code=404, detail="actor_not_found")

    dist_before = _distance(npc_brief, target_brief)

    # ----- ВАРИАНТ 1: NPC ДАЛЕКО (distance > 2) — ЧИСТОЕ ПЕРЕМЕЩЕНИЕ (до 3 клеток) -----
    if dist_before > 2:
        new_npc_brief = await _move_actor_towards(session, body.npc_id, target_brief, max_steps=3)
        dist_after = _distance(new_npc_brief, target_brief)

        narration = (
            f"NPC стремительно сокращает дистанцию, смещаясь к герою "
            f"(было {dist_before}, стало {dist_after})."
        )
        split = _split_narration_fields(narration)
        log_id = await _insert_log(
            session,
            body.session_id,
            body.npc_id,
            "npc",
            narration,
            {
                "action": "move_towards_target",
                "dist_before": dist_before,
                "dist_after": dist_after,
                "say_out": split["say_out"],
                "act_out": split["act_out"],
            },
            phase="reaction",
        )
        await session.commit()
        return {
            "ok": True,
            "log_id": log_id,
            "applied": {
                "type": "none",
                "damage": 0,
                "status": None,
            },
            "choices_ids": [],
            "narration": narration,
            "say_out": split["say_out"],
            "act_out": split["act_out"],
        }

    # ----- ВАРИАНТ 2: distance == 2 — ОДИН ШАГ + ДЕЙСТВИЕ LLM -----
    if dist_before == 2:
        new_npc_brief = await _move_actor_towards(session, body.npc_id, target_brief, max_steps=1)
        dist = _distance(new_npc_brief, target_brief)  # должно стать 1
    else:
        # distance 0–1 — уже вплотную, не двигаемся
        dist = dist_before

    # Здесь NPC уже относительно близко
    battle_history = await _get_battle_history(session, body.session_id, limit=8)

    # ---------- ДИНАМИЧЕСКАЯ ВРАЖДЕБНОСТЬ К ГЕРОЮ ----------
    ai_meta = ((npc_ctx.get("meta") or {}).get("ai") or {}).copy()

    base_hostility_raw = ai_meta.get("hostility_to_player", 0.0) or 0.0
    try:
        base_hostility = float(base_hostility_raw)
    except (TypeError, ValueError):
        base_hostility = 0.0

    hero_say_text = (body.last_hero_say or "").lower()
    hero_act_text = (body.last_hero_act or "").lower()

    # Минимальный набор оскорбительных кусочков на разных языках.
    # Это не идеально и не покрывает всё, но даёт базовую реакцию на токсичные реплики.
    insult_keywords = [
        # RU (без жёсткого мата, только грубые/оскорбительные формы)
        "идиот", "тупой", "дур", "тварь", "урод", "слаба", "ничтож", "жалкий",
        "козел", "козёл", "скотина", "мраз",
        # EN
        "idiot", "stupid", "fool", "loser", "pathetic", "worthless", "trash",
        # NL (просто несколько грубостей)
        "sukkel", "idioot", "waardeloos",
    ]

    insult_hits = 0
    if hero_say_text or hero_act_text:
        for kw in insult_keywords:
            if kw in hero_say_text or kw in hero_act_text:
                insult_hits += 1

    hostility = base_hostility

    # 1) Реальный урон от героя на предыдущем ходу — сразу резкий рост злобы.
    if body.last_damage_taken and body.last_damage_taken > 0:
        # гарантированно поднимаем хотя бы до "боевого" уровня
        hostility = max(hostility + 0.6, 0.7)
    else:
        # 2) Нет урона, но есть словесная агрессия/оскорбления
        if insult_hits > 0:
            # каждое совпадение даёт +0.15, но без выхода за 1.0
            hostility += 0.15 * insult_hits
        else:
            # 3) Никакой агрессии и урона — злость понемногу остывает
            hostility -= 0.05

    # Клипуем в [0.0, 1.0]
    hostility = max(0.0, min(1.0, hostility))

    # Обновляем контекст NPC для LLM
    ai_meta["hostility_to_player"] = hostility
    npc_meta = (npc_ctx.get("meta") or {}).copy()
    npc_meta["ai"] = ai_meta
    npc_ctx["meta"] = npc_meta

    # И сохраняем в БД, чтобы отношение запоминалось между ходами/сессиями
    await session.execute(
    text("""
        update actors
           set meta = jsonb_set(
                coalesce(meta, '{}'::jsonb),
                '{ai,hostility_to_player}',
                to_jsonb(CAST(:h AS numeric)),
                true
           )
         where id = :nid
    """),
    {"h": hostility, "nid": body.npc_id},
)

    # ---------- КОНЕЦ БЛОКА ДИНАМИЧЕСКОЙ ВРАЖДЕБНОСТИ ----------

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

    # Разделяем narration на "что сказал" / "что сделал"
    split = _split_narration_fields(decision.narration)
    say_out = split["say_out"]
    act_out = split["act_out"]

    # Применяем урон по герою, если NPC реально попал
    if mech.type == "hit":
        await _apply_damage_jsonb(session, body.target_id, int(mech.damage))

        # Если модель описала рану — сохраняем её на цели (обычно герой)
        if mech.status:
            await _apply_wound_status(session, body.target_id, mech.status)

        # если цель — герой, проверяем "смерть" как телепорт
        if body.target_id == "hero":
            await _handle_hero_zero_hp(session, body.target_id, body.session_id)
    else:
        # На будущее: если вдруг статус есть без урона — тоже сохраняем рану
        if mech.status:
            await _apply_wound_status(session, body.target_id, mech.status)

    log_id = await _insert_log(
        session,
        body.session_id,
        body.npc_id,
        "npc",
        decision.narration,
        {
            "decision": decision.dict(),
            "say_out": say_out,
            "act_out": act_out,
        },
        phase="reaction",
    )

    # --------- ВАЖНАЯ ЧАСТЬ: choices только при редких ударах ---------
    choice_ids: List[int] = []

    is_damaging_hit = (mech.type == "hit" and int(mech.damage) > 0)

    allow_reaction = (
        is_damaging_hit
        and bool(decision.choices)
        and random.random() < 0.2  # 0.2 = 20%
    )

    if allow_reaction:
        choices_payload = [{"label": c.label, "value": c.value} for c in decision.choices]
        choice_ids = await _write_choices(session, body.session_id, body.npc_id, choices_payload)
    else:
        choice_ids = []

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
        "say_out": say_out,
        "act_out": act_out,
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
        split = _split_narration_fields(narration)
        await _insert_log(
            session,
            body.session_id,
            body.actor_id,
            "system",
            narration,
            {
                "resolved_choice": dict(row),
                "result": "counter_window",
                "say_out": split["say_out"],
                "act_out": split["act_out"],
            },
            phase="system",
        )
        await session.execute(text("delete from pending_choices where id = :id"), {"id": body.choice_id})
        await session.commit()
        return {
            "ok": True,
            "narration": narration,
            "counter_open": True,
            "say_out": split["say_out"],
            "act_out": split["act_out"],
        }
    else:
        penalty = 5
        await _apply_damage_jsonb(session, body.actor_id, penalty)
        narration = f"{row['actor_id']} пытается прикрыться, но не успевает — получает урон ({penalty})."
        split = _split_narration_fields(narration)
        await _insert_log(
            session,
            body.session_id,
            body.actor_id,
            "system",
            narration,
            {
                "resolved_choice": dict(row),
                "result": "punished",
                "say_out": split["say_out"],
                "act_out": split["act_out"],
            },
            phase="system",
        )
        await session.execute(text("delete from pending_choices where id = :id"), {"id": body.choice_id})
        await session.commit()
        return {
            "ok": True,
            "narration": narration,
            "counter_open": False,
            "say_out": split["say_out"],
            "act_out": split["act_out"],
        }
