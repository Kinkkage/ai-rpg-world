# app/main.py — DROP-IN версия (без циклических импортов)
import os
import sys
import json
import asyncio
from datetime import datetime
from typing import List, Literal, Optional, Dict, Any, Tuple

from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from sqlalchemy import text, bindparam
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import JSONB  # UUID/ARRAY можно вернуть при необходимости

# DB / DAO
import random  # для вероятности наложения статуса
from app.db import get_session
from app.dao import (
    fetch_node,
    fetch_inventory,
    learn_skill,
    actor_knows_skill,  # может использоваться дальше; оставим
    list_skills,
    # Backpack / Bag:
    equip_backpack_db,
    unequip_backpack_db,
    hold_bag_db,
    # Drop to ground:
    drop_to_ground_db,
    # Transfers / grid:
    transfer_item_db,
    grid_put_item_db,
    grid_take_item_db,
    # Drop from hidden:
    drop_hidden_to_ground_db,
)

# ────────────────────────────────────────────────────────────────────────────────
# ЛЕНИВЫЕ ПРОКСИ ДЛЯ dao_status (у тебя файл app/dao_status.py)
# ────────────────────────────────────────────────────────────────────────────────
import importlib

def _load_ds():
    try:
        return importlib.import_module("app.dao_status")
    except ModuleNotFoundError:
        # fallback, если когда-то перенесёшь в services/
        return importlib.import_module("app.services.dao_status")

async def get_statuses_db_status(session, actor_id: str):
    ds = _load_ds()
    return await ds.get_statuses_db(session, actor_id)

async def apply_status_db_status(
    session,
    actor_id: str,
    status_id: str,
    turns_left: int = 1,
    intensity: float = 1.0,
    stacks: int = 1,
    source_id: Optional[str] = None,
):
    ds = _load_ds()
    return await ds.apply_status_db(
        session=session,
        actor_id=actor_id,
        status_id=status_id,
        turns_left=turns_left,
        intensity=intensity,
        stacks=stacks,
        source_id=source_id,
    )

async def advance_statuses_db_status(session):
    ds = _load_ds()
    return await ds.advance_statuses_db(session)

# ────────────────────────────────────────────────────────────────────────────────
# Приложение + WS менеджер
# ────────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="AI RPG World")
MAIN_BUILD_TS = datetime.utcnow().isoformat()

class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast_json(self, data: Dict[str, Any]):
        bad: List[WebSocket] = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                bad.append(ws)
        for ws in bad:
            self.disconnect(ws)

manager = ConnectionManager()

async def broadcast_event(evt: Dict[str, Any]):
    """доступно как: from app.main import broadcast_event"""
    await manager.broadcast_json({"event": evt})

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)

# ────────────────────────────────────────────────────────────────────────────────
# Только сейчас импортируем роутеры (broadcast_event уже объявлен)
# ────────────────────────────────────────────────────────────────────────────────
from app.routers import world, narrative, assets
from app.routers import status as status_router
from app.routers import turn as turn_router
from app.routers import battle as battle_router  # у тебя он есть по тестам
from app.routers.narrative import NarrateIn, narrate as narrate_endpoint
from app.routers.inventory import router as inventory_router
from app.routers.items import router as items_router
# модуль intents — опционально
try:
    from app.routers import intents as intents_router  # noqa: F401
    HAS_INTENTS = True
except Exception:
    intents_router = None
    HAS_INTENTS = False

# ────────────────────────────────────────────────────────────────────────────────
# SCHEMAS
# ────────────────────────────────────────────────────────────────────────────────
class Intent(BaseModel):
    type: Literal["MOVE", "INSPECT", "TALK", "EQUIP", "UNEQUIP", "USE_ITEM", "COMBINE_USE", "ATTACK"]
    payload: Dict[str, Any]

class Event(BaseModel):
    type: str
    payload: Dict[str, Any]

# ────────────────────────────────────────────────────────────────────────────────
# NARRATIVE HELPERS
# ────────────────────────────────────────────────────────────────────────────────
def _split_chunks(s: str, maxlen: int = 48):
    s = s.strip()
    if not s:
        return []
    out, buf = [], ""
    for word in s.split():
        if len(buf) + 1 + len(word) <= maxlen:
            buf = (buf + " " + word).strip()
        else:
            out.append(buf)
            buf = word
    if buf:
        out.append(buf)
    return out

async def stream_text_rich(text: str, style: str = "default", delay_ms: int = 35):
    chunks = _split_chunks(text, maxlen=48)
    if not chunks:
        return
    for ch in chunks:
        await broadcast_event({"type": "NARRATE_PART", "payload": {"chunk": ch, "style": style}})
        await asyncio.sleep(delay_ms / 1000)
    await broadcast_event({"type": "NARRATE_DONE", "payload": {"style": style}})

async def _get_node_biome(session: AsyncSession, node_id: str) -> str:
    row = (await session.execute(text("select biome from nodes where id=:id"), {"id": node_id})).mappings().first()
    return (row and row["biome"]) or "default"

def _style_for_biome(biome: str, is_battle: bool = False) -> str:
    if is_battle:
        return "battle"
    if biome in ("castle", "forest", "desert", "battle"):
        return biome if biome != "desert" else "default"
    return "default"

async def compose_narrative(
    session: AsyncSession,
    node_id: str,
    events: List[Dict[str, Any]],
    context_extra: Optional[Dict[str, Any]] = None,
    is_battle: bool = False,
) -> Optional[Dict[str, Any]]:
    biome = await _get_node_biome(session, node_id)
    style_id = _style_for_biome(biome, is_battle=is_battle)
    body = NarrateIn(
        node_id=node_id,
        style_id=style_id,
        events=events,
        context={"biome": biome, **(context_extra or {})}
    )
    out = await narrate_endpoint(body, session)
    if out and out.text:
        return {"type": "TEXT_RICH", "payload": {"text": out.text, "style": style_id}}
    return None

# ────────────────────────────────────────────────────────────────────────────────
# STATUS NARRATIVE (стиль "status")
# ────────────────────────────────────────────────────────────────────────────────
async def _get_actor_node(session: AsyncSession, actor_id: str) -> Optional[str]:
    row = (await session.execute(text("select node_id from actors where id=:id"), {"id": actor_id})).mappings().first()
    return row and row.get("node_id")

async def compose_status_narrative(
    session: AsyncSession,
    actor_id: str,
    events: List[Dict[str, Any]],
) -> None:
    if not events:
        return
    node_id = await _get_actor_node(session, actor_id)
    if not node_id:
        return
    body = NarrateIn(node_id=node_id, style_id="status", events=events, context={"mode": "status", "actor_id": actor_id})
    out = await narrate_endpoint(body, session)
    if out and out.text:
        rich = {"type": "TEXT_RICH", "payload": {"text": out.text, "style": "status"}}
        await broadcast_event(rich)

# ────────────────────────────────────────────────────────────────────────────────
# HELPERS
# ────────────────────────────────────────────────────────────────────────────────
OPPOSITE_DIR = {"north": "south", "south": "north", "east": "west", "west": "east"}

async def _ensure_reverse_exit(session: AsyncSession, from_node_id: str, to_node_id: str, direction: str):
    opposite = OPPOSITE_DIR.get(direction)
    if not opposite:
        return
    row = (await session.execute(text("select exits from nodes where id=:id"), {"id": to_node_id})).mappings().first()
    if not row:
        return
    exits_b = _normalize_exits(row.get("exits"))
    if exits_b.get(opposite) == from_node_id:
        return
    if exits_b.get(opposite) is None:
        exits_b[opposite] = from_node_id
        await session.execute(text("update nodes set exits=:exits where id=:id"), {"id": to_node_id, "exits": json.dumps(exits_b)})

def _emit_text(msg: str) -> Event:
    return {"type": "TEXT", "payload": {"text": msg}}

def _normalize_exits(value: Any) -> Dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}

# ────────────────────────────────────────────────────────────────────────────────
# SYSTEM HEALTH
# ────────────────────────────────────────────────────────────────────────────────
@app.get("/ping")
def ping():
    return {"ok": True}

@app.get("/health/db")
async def health_db(session: AsyncSession = Depends(get_session)):
    try:
        await session.execute(text("select 1"))
        return {"db": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health/env")
def health_env():
    db_url = os.getenv("DATABASE_URL", "")
    return {
        "PGSSLMODE": os.getenv("PGSSLMODE"),
        "DATABASE_URL_has_sslmode": ("sslmode=" in db_url),
        "DATABASE_URL_has_ssl": ("ssl=" in db_url),
    }

# полезно понять, какой main реально импортирован
@app.get("/debug/source")
def debug_source():
    return {
        "main_file": __file__,
        "cwd": os.getcwd(),
        "build_ts": MAIN_BUILD_TS,
        "python_exe": sys.executable,
        "sys_path_head": sys.path[:5],
    }

# ────────────────────────────────────────────────────────────────────────────────
# NODE / INVENTORY VIEW
# ────────────────────────────────────────────────────────────────────────────────
@app.get("/node/{node_id}")
async def get_node(node_id: str, session: AsyncSession = Depends(get_session)):
    node = await fetch_node(session, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node

@app.get("/inventory/{actor_id}")
async def get_inventory(actor_id: str, session: AsyncSession = Depends(get_session)):
    return await fetch_inventory(session, actor_id)

# ────────────────────────────────────────────────────────────────────────────────
# INVENTORY ACTIONS (Backpack / Bag)
# ────────────────────────────────────────────────────────────────────────────────
class EquipBackpackIn(BaseModel):
    actor_id: str = "player"
    item_id: str   # UUID рюкзака (экземпляр items.id)

class HoldBagIn(BaseModel):
    actor_id: str = "player"
    item_id: str   # UUID мешка (экземпляр items.id)
    hand: Literal["left", "right"] = "left"

class ActorOnlyIn(BaseModel):
    actor_id: str = "player"

@app.post("/inventory/equip_backpack")
async def equip_backpack(body: EquipBackpackIn, session: AsyncSession = Depends(get_session)):
    res = await equip_backpack_db(session, body.actor_id, body.item_id)
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "fail"))
    return res

@app.post("/inventory/unequip_backpack")
async def unequip_backpack(body: ActorOnlyIn, session: AsyncSession = Depends(get_session)):
    res = await unequip_backpack_db(session, body.actor_id)
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "fail"))
    return res

@app.post("/inventory/hold_bag")
async def hold_bag(body: HoldBagIn, session: AsyncSession = Depends(get_session)):
    res = await hold_bag_db(session, body.actor_id, body.item_id, body.hand)
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "fail"))
    return res

# ────────────────────────────────────────────────────────────────────────────────
# NPC STATE
# ────────────────────────────────────────────────────────────────────────────────
@app.get("/npc/{actor_id}")
async def get_npc(actor_id: str, session: AsyncSession = Depends(get_session)):
    row = (
        await session.execute(
            text("""
                select id, archtype, mood, trust, aggression, node_id, kind
                from actors
                where id=:id
            """),
            {"id": actor_id},
        )
    ).mappings().first()

    if not row or (row.get("kind") != "npc"):
        raise HTTPException(status_code=404, detail="NPC not found")

    memories = (
        await session.execute(
            text("""
                select category, event, description, ts
                from npc_memories
                where actor_id=:id
                order by ts desc
                limit 5
            """),
            {"id": actor_id},
        )
    ).mappings().all()

    return {
        "id": row["id"],
        "archtype": row["archtype"],
        "mood": row["mood"],
        "trust": row["trust"],
        "aggression": row["aggression"],
        "memories": list(memories),
    }

# ────────────────────────────────────────────────────────────────────────────────
# SIMPLE PLAYER MOCK (локальные предметы для /intent)
# ────────────────────────────────────────────────────────────────────────────────
ITEMS: Dict[str, Dict[str, Any]] = {
    "lighter": {"id":"lighter","title":"Зажигалка","tags":["tool","fire"],"props":{"ignite":True,"consumes_per_use":1},"charges":50},
    "deodorant":{"id":"deodorant","title":"Дезодорант","tags":["spray","flammable"],"props":{"flammable":True,"consumes_per_use":1},"charges":20},
    "water_bottle":{"id":"water_bottle","title":"Бутылка воды","tags":["liquid","water"],"props":{"water":True,"consumes_per_use":1},"charges":3},
    "greatsword":{"id":"greatsword","title":"Тяжёлый меч","tags":["melee","sword"],"props":{"two_hands":True},"charges":None},
}
PLAYER = {"pos":{"x":5,"y":5}, "hp":100, "hands":{"left":None,"right":None}, "backpack":["lighter","deodorant","water_bottle","greatsword"]}

def _equip(hand: str, item_id: str) -> List[Event]:
    if item_id not in PLAYER["backpack"]:
        return [_emit_text("Этого предмета нет в рюкзаке.")]
    if PLAYER["hands"][hand]:
        return [_emit_text(f"Рука {hand} занята.")]
    if ITEMS[item_id].get("props",{}).get("two_hands"):
        if PLAYER["hands"]["left"] or PLAYER["hands"]["right"]:
            return [_emit_text("Это двуручный предмет — освободите обе руки.")]
        PLAYER["hands"]["left"] = item_id
        PLAYER["hands"]["right"] = item_id
        PLAYER["backpack"].remove(item_id)
        return [
            {"type":"EQUIP_CHANGE","payload":{"hand":"both","item":ITEMS[item_id]["title"]}},
            _emit_text(f"Вы взяли {ITEMS[item_id]['title']} двумя руками.")
        ]
    PLAYER["backpack"].remove(item_id)
    PLAYER["hands"][hand] = item_id
    return [
        {"type":"EQUIP_CHANGE","payload":{"hand":hand,"item":ITEMS[item_id]["title"]}},
        _emit_text(f"Вы взяли в {hand} {ITEMS[item_id]['title']}.")
    ]

def _unequip(hand: str) -> List[Event]:
    item_id = PLAYER["hands"][hand]
    if not item_id:
        return [_emit_text(f"В {hand} руке пусто.")]
    if ITEMS[item_id].get("props",{}).get("two_hands"):
        PLAYER["hands"]["left"] = None
        PLAYER["hands"]["right"] = None
        PLAYER["backpack"].append(item_id)
        return [
            {"type":"EQUIP_CHANGE","payload":{"hand":"both","item":None}},
            _emit_text(f"Вы убрали {ITEMS[item_id]['title']} в рюкзак.")
        ]
    PLAYER["hands"][hand] = None
    PLAYER["backpack"].append(item_id)
    return [
        {"type":"EQUIP_CHANGE","payload":{"hand":hand,"item":None}},
        _emit_text(f"Вы убрали {ITEMS[item_id]['title']} в рюкзак.")
    ]

def _consume(item_id: str, amount: int = 1) -> List[Event]:
    item = ITEMS[item_id]
    if item.get("charges") is None:
        return []
    if item.get("charges", 0) < amount:
        return [_emit_text(f"{item['title']} пуст.")]
    item["charges"] -= amount
    return [{"type":"CONSUME","payload":{"item":item["title"],"delta":-amount,"left":item["charges"]}}]

def _use_item_single(item_id: str, target: Optional[str] = None) -> List[Event]:
    item = ITEMS[item_id]; p = item["props"]; ev: List[Event] = []
    if p.get("water"):
        ev += _consume(item_id, p.get("consumes_per_use", 1))
        ev.append({"type":"FX","payload":{"kind":"splash","on":target or "ground"}})
        ev.append(_emit_text("Вы плеснули воду."))
    elif p.get("ignite"):
        ev += _consume(item_id, p.get("consumes_per_use", 1))
        ev.append({"type":"FX","payload":{"kind":"spark","on":target or "front"}})
        ev.append(_emit_text("Щёлк! Искра вспыхнула."))
    else:
        ev.append(_emit_text("Ничего не произошло."))
    return ev

def _combine_use(left_id: str, right_id: str, target: Optional[str] = None) -> List[Event]:
    if {"lighter","deodorant"} == {left_id,right_id}:
        return [
            *_consume("lighter",1),
            *_consume("deodorant",1),
            {"type":"FX","payload":{"kind":"flame_cone","dir":"front","range":3,"width":2}},
            _emit_text("Вы пускаете струю огня!")
        ]
    return [_emit_text("Эти предметы не комбинируются.")]

# ────────────────────────────────────────────────────────────────────────────────
# SKILLS
# ────────────────────────────────────────────────────────────────────────────────
class LearnSkillIn(BaseModel):
    actor_id: str = "player"
    skill_id: str

@app.post("/skills/learn")
async def post_learn_skill(data: LearnSkillIn, session: AsyncSession = Depends(get_session)):
    return await learn_skill(session, data.actor_id, data.skill_id)

POS_WORDS = ["спасибо","благодарю","признателен","молодец","добр","уважаю","восхищаюсь","выручил"]
NEG_WORDS = ["плох","ненавижу","дурак","идиот","туп","предам","обманул","врёшь","угроза","напасть","убью","убить","жалкий","никчемный","дурной","скотина"]

def classify_tone(s: str) -> str:
    s = (s or "").lower()
    if any(w in s for w in NEG_WORDS):
        return "neg"
    if any(w in s for w in POS_WORDS):
        return "pos"
    return "neutral"

WEAPON_KEYWORDS = ["меч","клинок","сабля","удар","рублю","рубаю","режу","sword","blade","slash","strike","cut","heavy slash"]

def _text_has_any(t: str, words: List[str]) -> bool:
    low = (t or "").lower()
    return any(w in low for w in words)

async def detect_skill_from_text(session: AsyncSession, text_value: str) -> Optional[Tuple[str, Dict[str, Any]]]:
    low = (text_value or "").lower()
    for sk in await list_skills(session):
        title = (sk.get("title") or "").lower()
        props = sk.get("props") or {}
        aliases = [a.lower() for a in (props.get("aliases") or [])]
        if title and title in low:
            return sk["id"], sk
        if any(a and a in low for a in aliases):
            return sk["id"], sk
    return None

def _skill_events(skill_id: str, skill_title: str) -> List[Event]:
    return [
        {"type":"SKILL_USE","payload":{"skill_id":skill_id,"title":skill_title}},
        _emit_text(f"Вы применяете навык: {skill_title}.")
    ]

def _has_item_with_tag(tag: str) -> bool:
    hands = PLAYER["hands"]
    for hid in [hands["left"], hands["right"]]:
        if hid and tag in (ITEMS[hid].get("tags") or []):
            return True
    return False

# ────────────────────────────────────────────────────────────────────────────────
# INTENT: базовый ATTACK + автотик статусов + лут при смерти
# ────────────────────────────────────────────────────────────────────────────────
async def _status_mods_for_actor(session: AsyncSession, actor_id: str) -> Dict[str, float]:
    mods = {"outgoing_mult": 1.0, "incoming_mult": 1.0}
    try:
        rows = await get_statuses_db_status(session, actor_id)
    except Exception:
        return mods
    for s in rows:
        sid = s.get("status_id")
        if sid == "rage":
            mods["outgoing_mult"] *= 1.5
        elif sid == "guard":
            mods["incoming_mult"] *= 0.5
    return mods

# ─────────────────────────────────────────────
# Loot helpers: создание лута при смерти
# ─────────────────────────────────────────────
async def _get_actor_pos(session: AsyncSession, actor_id: str):
    row = (
        await session.execute(
            text("select node_id, x, y from actors where id=:id"),
            {"id": actor_id},
        )
    ).mappings().first()
    return (row["node_id"], int(row["x"]), int(row["y"])) if row else None

async def _ensure_loot_object(session: AsyncSession, node_id: str, x: int, y: int) -> int:
    # создаём контейнер-объект "труп" на слое 3
    obj = (
        await session.execute(
            text("""
                insert into node_objects(node_id, x, y, layer, asset_id, props)
                values (:nid, :x, :y, 3, 'corpse', '{"title":"Труп","state":"open"}'::jsonb)
                returning id
            """),
            {"nid": node_id, "x": x, "y": y},
        )
    ).mappings().first()
    oid = obj["id"]
    await session.execute(
        text("""
            insert into object_inventories(object_id, items)
            values (:oid, '{}'::uuid[])
            on conflict (object_id) do nothing
        """),
        {"oid": oid},
    )
    return oid

async def _append_items_to_object(session: AsyncSession, oid: int, item_ids: List[str]):
    for iid in item_ids:
        await session.execute(
            text("""
                update object_inventories
                   set items = array_append(coalesce(items,'{}'::uuid[]), CAST(:iid AS uuid))
                 where object_id = :oid
            """),
            {"oid": oid, "iid": iid},
        )

async def handle_actor_death(session: AsyncSession, actor_id: str):
    """Сбрасывает всё, что было на актёре, в лут-объект 'Труп' на той же клетке."""
    pos = await _get_actor_pos(session, actor_id)
    if not pos:
        return
    nid, x, y = pos
    oid = await _ensure_loot_object(session, nid, x, y)

    inv = (
        await session.execute(
            text("""
                select left_item, right_item, hidden_slot, equipped_bag, backpack
                  from inventories
                 where actor_id = :aid
            """),
            {"aid": actor_id},
        )
    ).mappings().first() or {}

    item_ids: List[str] = []
    for k in ("left_item", "right_item", "hidden_slot", "equipped_bag"):
        if inv.get(k):
            item_ids.append(str(inv[k]))
    if inv.get("backpack"):
        item_ids.extend([str(i) for i in inv["backpack"]])

    # Добавляем в контейнер
    if item_ids:
        await _append_items_to_object(session, oid, item_ids)

    # Очищаем инвентарь умершего
    await session.execute(
        text("""
            update inventories
               set left_item=null, right_item=null, hidden_slot=null,
                   equipped_bag=null, backpack='{}'::uuid[]
             where actor_id=:aid
        """),
        {"aid": actor_id},
    )

    # Помечаем актёра как мёртвого (meta.dead=true)
    await session.execute(
        text("""
            update actors
               set meta = coalesce(meta,'{}'::jsonb) || jsonb_build_object('dead', true)
             where id = :aid
        """),
        {"aid": actor_id},
    )

async def _apply_attack_and_optionally_status(
    session: AsyncSession,
    attacker_id: str,
    target_id: str,
    base_damage: int,
    status_apply: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    atk_mods = await _status_mods_for_actor(session, attacker_id)
    tgt_mods = await _status_mods_for_actor(session, target_id)
    dmg = max(0, int(round(base_damage * atk_mods["outgoing_mult"] * tgt_mods["incoming_mult"])))

    new_hp_row = (await session.execute(
        text("""
            update actors
               set hp = greatest(0, coalesce(hp,0) - :dmg)
             where id=:tid
         returning hp
        """),
        {"tid": target_id, "dmg": dmg},
    )).mappings().first()
    new_hp = (new_hp_row and int(new_hp_row["hp"])) if new_hp_row else None

    ev_resolve = {
        "type": "ATTACK_RESOLVE",
        "payload": {
            "attacker_id": attacker_id,
            "target_id": target_id,
            "base": base_damage,
            "out_mult": atk_mods["outgoing_mult"],
            "in_mult": tgt_mods["incoming_mult"],
            "dmg": dmg,
            "hp": new_hp,
        }
    }
    events.append(ev_resolve)
    try:
        await broadcast_event(ev_resolve)
    except Exception:
        pass

    # если цель умерла — сбрасываем её лут на землю
    if new_hp == 0:
        await handle_actor_death(session, target_id)

    if status_apply and status_apply.get("id"):
        chance = float(status_apply.get("chance", 1.0))
        if chance >= 1.0 or random.random() < max(0.0, min(chance, 1.0)):
            st_id = status_apply["id"]
            turns = int(status_apply.get("turns", 1))
            intensity = float(status_apply.get("intensity", 1.0))
            stacks = int(status_apply.get("stacks", 1))

            out = await apply_status_db_status(
                session=session,
                actor_id=target_id,
                status_id=st_id,
                turns_left=turns,
                intensity=intensity,
                stacks=stacks,
                source_id=attacker_id,
            )
            if out.get("ok"):
                ev_apply = {
                    "type": "STATUS_APPLY",
                    "payload": {
                        "actor_id": target_id,
                        "status": st_id,
                        "turns_left": turns,
                        "stacks": stacks,
                        "intensity": intensity,
                        "source_id": attacker_id,
                    }
                }
                events.append(ev_apply)
                try:
                    await broadcast_event(ev_apply)
                except Exception:
                    pass

    return events

@app.post("/intent")
async def post_intent(intent: Intent, session: AsyncSession = Depends(get_session)) -> List[Event]:
    t = intent.type
    p = intent.payload or {}
    out_events: List[Event] = []

    if t == "ATTACK":
        attacker_id = p.get("attacker_id") or "player"
        target_id = p.get("target_id")
        base_damage = int(p.get("base_damage") or 0)
        status_apply = p.get("status_apply")

        if not target_id:
            return [{"type": "TEXT", "payload": {"text": "Нет цели для атаки."}}]

        # ── новая проверка: цель существует и жива ──────────────────────────────
        tgt = (await session.execute(
            text("select hp, coalesce((meta->>'dead')::bool, false) as dead from actors where id=:id"),
            {"id": target_id}
        )).mappings().first()
        if not tgt:
            return [{"type": "TEXT", "payload": {"text": "Цель не найдена."}}]
        if int(tgt["hp"]) <= 0 or bool(tgt["dead"]):
            return [{"type": "TEXT", "payload": {"text": "Цель уже мертва."}}]
        # ───────────────────────────────────────────────────────────────────────

        try:
            evs = await _apply_attack_and_optionally_status(
                session, attacker_id, target_id, base_damage, status_apply
            )
            out_events.extend(evs)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"attack_failed: {e}")

        try:
            res = await advance_statuses_db_status(session)
            if res.get("ok"):
                for evt in res.get("events", []):
                    out_events.append(evt)
                    try:
                        await broadcast_event(evt)
                    except Exception:
                        pass
        except Exception:
            pass

        await session.commit()
        return out_events

    return [{"type": "TEXT", "payload": {"text": "Ничего не произошло."}}]

# ────────────────────────────────────────────────────────────────────────────────
# TALK
# ────────────────────────────────────────────────────────────────────────────────
@app.post("/talk")
async def talk_to_npc(data: Dict[str, Any], session: AsyncSession = Depends(get_session)):
    npc_id = data.get("npc_id")
    text_in = (data.get("text") or "")

    npc = (await session.execute(
        text("""
            select id, archtype, mood, trust, aggression, node_id, kind
              from actors
             where id=:id
        """),
        {"id": npc_id},
    )).mappings().first()
    if not npc or (npc.get("kind") != "npc"):
        raise HTTPException(status_code=404, detail="NPC not found")

    mood = npc["mood"]
    trust = int(npc["trust"])
    reply = ""
    category = "talk_neutral"

    tone = classify_tone(text_in)
    low = text_in.lower()

    if tone == "pos":
        trust = min(trust + 5, 100)
        mood = "friendly"
        reply = "Он кивает и благодарно улыбается."
        category = "talk_positive"
    elif tone == "neg":
        trust = max(trust - 10, 0)
        mood = "angry"
        reply = "Он зарычал и напрягся, готовясь к атаке."
        category = "talk_negative"
    else:
        reply = "Он выслушал вас, не выражая эмоций."
        category = "talk_neutral"

    await session.execute(text("update actors set mood=:mood, trust=:trust where id=:id"),
                          {"mood": mood, "trust": trust, "id": npc_id})

    insert_mem = text("""
        insert into npc_memories(actor_id, category, event, description, payload)
        values(:aid, :cat, :evt, :desc, :payload)
    """)
    await session.execute(insert_mem, {
        "aid": npc_id,
        "cat": category,
        "evt": category,
        "desc": low[:100],
        "payload": json.dumps({"player": "player", "reply": reply, "ts": datetime.utcnow().isoformat()}),
    })
    await session.commit()

    ev_text = {"type": "TEXT", "payload": {"text": reply}}
    ev_state = {"type": "NPC_STATE", "payload": {"npc_id": npc_id, "mood": mood, "trust": trust}}

    npc_node = (await session.execute(text("select node_id from actors where id=:id"), {"id": npc_id})).scalar()
    player_node = (await session.execute(text("select node_id from actors where id='player'"))).scalar()
    current_node = npc_node or player_node

    if current_node:
        rich = await compose_narrative(
            session,
            node_id=current_node,
            events=[ev_text, ev_state],
            context_extra={"actor_id": "player", "npc_id": npc_id, "mode": "talk"},
            is_battle=False
        )
        if rich:
            await broadcast_event(rich)

    await broadcast_event(ev_text)
    await broadcast_event(ev_state)

    return {"events": [ev_text, ev_state]}

# ────────────────────────────────────────────────────────────────────────────────
# CONTAINERS & STATE (open/closed/locked)
# ────────────────────────────────────────────────────────────────────────────────
async def _patch_object_props(session: AsyncSession, object_id: int, patch: Dict[str, Any]):
    stmt = text("""
        update node_objects
           set props = coalesce(props, '{}'::jsonb) || :patch
         where id = :oid
    """).bindparams(bindparam("patch", type_=JSONB), bindparam("oid"))
    await session.execute(stmt, {"patch": patch, "oid": object_id})

class PickupFromContainerIn(BaseModel):
    object_id: int
    item_id: str
    actor_id: str

class DropToContainerIn(BaseModel):
    object_id: int
    item_id: str
    actor_id: str

class OpenContainerIn(BaseModel):
    object_id: int
    state: Literal["open", "closed"] = "open"

class UnlockContainerIn(BaseModel):
    object_id: int
    actor_id: str
    key_kind_id: Optional[str] = None

@app.get("/world/container/{object_id}")
async def get_container(object_id: int, session: AsyncSession = Depends(get_session)):
    obj = (await session.execute(text("select id, asset_id, props from node_objects where id=:id"), {"id": object_id})).mappings().first()
    if not obj:
        raise HTTPException(status_code=404, detail="object_not_found")

    inv = (await session.execute(text("select items from object_inventories where object_id=:id"), {"id": object_id})).mappings().first()
    ids = (inv and inv.get("items")) or []

    items = []
    if ids:
        rows = (await session.execute(text("""
            select i.id, k.id as kind_id, k.title, k.tags, k.handedness, k.props
              from items i
              join item_kinds k on k.id = i.kind_id
             where i.id = any(:ids)
        """), {"ids": ids})).mappings().all()
        items = [dict(r) for r in rows]

    return {"ok": True, "object": {"id": obj["id"], "asset_id": obj["asset_id"], "props": obj["props"]}, "items": items}

@app.get("/world/container/{object_id}/state")
async def get_container_state(object_id: int, session: AsyncSession = Depends(get_session)):
    row = (await session.execute(text("select id, asset_id, props from node_objects where id=:id"), {"id": object_id})).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="object_not_found")
    props = (row.get("props") or {}) if isinstance(row.get("props"), dict) else {}
    state = props.get("state", "open")
    key_kind_id = props.get("key_kind_id")
    return {"ok": True, "object": {"id": row["id"], "asset_id": row["asset_id"]}, "state": state, "key_kind_id": key_kind_id}

@app.post("/world/container/open")
async def open_container(body: OpenContainerIn, session: AsyncSession = Depends(get_session)):
    obj = (await session.execute(text("select id from node_objects where id=:id"), {"id": body.object_id})).mappings().first()
    if not obj:
        raise HTTPException(status_code=404, detail="object_not_found")
    await _patch_object_props(session, body.object_id, {"state": body.state})
    await session.commit()
    return {"ok": True, "object_id": body.object_id, "state": body.state}

@app.post("/world/container/unlock")
async def unlock_container(body: UnlockContainerIn, session: AsyncSession = Depends(get_session)):
    row = (await session.execute(text("select id, props from node_objects where id=:id"), {"id": body.object_id})).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="object_not_found")
    props = (row.get("props") or {}) if isinstance(row.get("props"), dict) else {}
    state = props.get("state", "open")
    if state != "locked":
        raise HTTPException(status_code=400, detail="not_locked")
    required_key = props.get("key_kind_id")
    if not required_key:
        raise HTTPException(status_code=400, detail="no_key_required")
    if body.key_kind_id and body.key_kind_id != required_key:
        raise HTTPException(status_code=400, detail="wrong_key_kind")
    has_key = (await session.execute(text("""
        select 1
          from inventories inv
          join items i on i.id = any(coalesce(inv.backpack, '{}'::uuid[]))
         where inv.actor_id = :aid
           and i.kind_id = :kkid
         limit 1
    """), {"aid": body.actor_id, "kkid": required_key})).scalar()
    if not has_key:
        raise HTTPException(status_code=403, detail="key_not_found")
    await _patch_object_props(session, body.object_id, {"state": "open"})
    await session.commit()
    return {"ok": True, "unlocked": True, "object_id": body.object_id}

@app.post("/world/pickup_from_container")
async def pickup_from_container(body: PickupFromContainerIn, session: AsyncSession = Depends(get_session)):
    obj = (await session.execute(text("select id, props from node_objects where id=:id"), {"id": body.object_id})).mappings().first()
    if not obj:
        raise HTTPException(status_code=404, detail="object_not_found")
    props = (obj.get("props") or {}) if isinstance(obj.get("props"), dict) else {}
    state = props.get("state", "open")
    if state != "open":
        raise HTTPException(status_code=423, detail=state)

    await session.execute(text("""
        insert into inventories(actor_id, left_item, right_item, backpack)
        values (:aid, null, null, '{}'::uuid[])
        on conflict (actor_id) do nothing
    """), {"aid": body.actor_id})

    removed = (await session.execute(text("""
        update object_inventories
           set items = array_remove(coalesce(items,'{}'::uuid[]), CAST(:iid AS uuid))
         where object_id = :oid
           and CAST(:iid AS uuid) = any(coalesce(items,'{}'::uuid[]))
        returning items
    """), {"iid": body.item_id, "oid": body.object_id})).mappings().first()
    if not removed:
        raise HTTPException(status_code=404, detail="item_not_in_container")

    bp = (await session.execute(text("""
        update inventories
           set backpack = array_append(coalesce(backpack,'{}'::uuid[]), CAST(:iid AS uuid))
         where actor_id = :aid
        returning backpack
    """), {"iid": body.item_id, "aid": body.actor_id})).mappings().first()
    await session.commit()
    return {"ok": True, "moved": body.item_id, "to_actor": body.actor_id, "backpack": bp and bp["backpack"]}

@app.post("/world/drop_to_container")
async def drop_to_container(body: DropToContainerIn, session: AsyncSession = Depends(get_session)):
    obj = (await session.execute(text("select id, props from node_objects where id=:id"), {"id": body.object_id})).mappings().first()
    if not obj:
        raise HTTPException(status_code=404, detail="object_not_found")
    props = (obj.get("props") or {}) if isinstance(obj.get("props"), dict) else {}
    state = props.get("state", "open")
    if state != "open":
        raise HTTPException(status_code=423, detail=state)

    await session.execute(text("""
        insert into object_inventories(object_id, items)
        values (:oid, '{}'::uuid[])
        on conflict (object_id) do nothing
    """), {"oid": body.object_id})

    removed = (await session.execute(text("""
        update inventories
           set backpack = array_remove(coalesce(backpack,'{}'::uuid[]), CAST(:iid AS uuid))
         where actor_id = :aid
           and CAST(:iid AS uuid) = any(coalesce(backpack,'{}'::uuid[]))
        returning backpack
    """), {"iid": body.item_id, "aid": body.actor_id})).mappings().first()
    if not removed:
        raise HTTPException(status_code=404, detail="item_not_in_backpack")

    items_now = (await session.execute(text("""
        update object_inventories
           set items = array_append(coalesce(items,'{}'::uuid[]), CAST(:iid AS uuid))
         where object_id = :oid
        returning items
    """), {"iid": body.item_id, "oid": body.object_id})).mappings().first()

    await session.commit()
    return {"ok": True, "moved": body.item_id, "from_actor": body.actor_id, "to_object": body.object_id, "object_items": items_now and items_now["items"]}

# ────────────────────────────────────────────────────────────────────────────────
# TRANSFER / GRID / DROP
# ────────────────────────────────────────────────────────────────────────────────
class TransferIn(BaseModel):
    actor_id: str = "player"
    source: Literal["left", "right", "hidden", "backpack"]
    target: Literal["left", "right", "hidden", "backpack"]
    item_id: Optional[str] = None  # обязателен, если source='backpack'

@app.post("/inventory/transfer")
async def inventory_transfer(body: TransferIn, session: AsyncSession = Depends(get_session)):
    res = await transfer_item_db(session, body.actor_id, body.source, body.target, body.item_id)
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "fail"))
    return res

class GridPutIn(BaseModel):
    actor_id: str = "player"
    container_item_id: str
    slot_x: int
    slot_y: int
    source_place: Literal["left", "right", "hidden", "backpack"]
    item_id: str

class GridTakeIn(BaseModel):
    actor_id: str = "player"
    container_item_id: str
    slot_x: int
    slot_y: int
    target_place: Literal["left", "right", "hidden", "backpack"]

@app.post("/inventory/grid/put")
async def grid_put(body: GridPutIn, session: AsyncSession = Depends(get_session)):
    res = await grid_put_item_db(session, body.actor_id, body.container_item_id, body.slot_x, body.slot_y, body.source_place, body.item_id)
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "fail"))
    return res

@app.post("/inventory/grid/take")
async def grid_take(body: GridTakeIn, session: AsyncSession = Depends(get_session)):
    res = await grid_take_item_db(session, body.actor_id, body.container_item_id, body.slot_x, body.slot_y, body.target_place)
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "fail"))
    return res

class DropIn(BaseModel):
    actor_id: str = "player"
    source: Literal["left", "right", "hidden", "backpack", "equipped_bag"]
    item_id: Optional[str] = None

@app.post("/inventory/drop")
async def inventory_drop(body: DropIn, session: AsyncSession = Depends(get_session)):
    res = await drop_to_ground_db(session, body.actor_id, body.source, body.item_id)
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "fail"))
    return res

@app.post("/inventory/drop_hidden")
async def inventory_drop_hidden(body: ActorOnlyIn, session: AsyncSession = Depends(get_session)):
    res = await drop_hidden_to_ground_db(session, body.actor_id)
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "fail"))
    return res

# ────────────────────────────────────────────────────────────────────────────────
# DEBUG SNAPSHOT / SEEDERS
# ────────────────────────────────────────────────────────────────────────────────
class DebugStateOut(BaseModel):
    ok: bool
    actor: Dict[str, Any]
    inventory: Dict[str, Any]
    ground: Dict[str, Any]
    known_ids: Dict[str, str]

@app.get("/debug/state", response_model=DebugStateOut)
async def debug_state(session: AsyncSession = Depends(get_session), actor_id: str = "player"):
    pos = (await session.execute(text("""
        SELECT id, node_id, COALESCE(x,0) AS x, COALESCE(y,0) AS y
          FROM actors WHERE id=:aid
    """), {"aid": actor_id})).mappings().first()
    if not pos or not pos["node_id"]:
        raise HTTPException(status_code=404, detail="player_not_found_or_no_position")

    node_id, x, y = pos["node_id"], int(pos["x"]), int(pos["y"])
    inv = await fetch_inventory(session, actor_id)

    loot_rows = (await session.execute(text("""
        SELECT id, asset_id, props FROM node_objects
         WHERE node_id=:nid AND x=:x AND y=:y AND layer=3
         ORDER BY id
    """), {"nid": node_id, "x": x, "y": y})).mappings().all()

    ground = {"cell": {"node_id": node_id, "x": x, "y": y}, "objects": []}

    for obj in loot_rows:
        inv_row = (await session.execute(text("SELECT items FROM object_inventories WHERE object_id=:oid"), {"oid": obj["id"]})).mappings().first()
        ids = (inv_row and inv_row.get("items")) or []
        items = []
        if ids:
            rows = (await session.execute(text("""
                SELECT i.id, k.id AS kind_id, k.title
                  FROM items i JOIN item_kinds k ON k.id = i.kind_id
                 WHERE i.id = ANY(:ids)
            """), {"ids": ids})).mappings().all()
            items = [dict(r) for r in rows]
        ground["objects"].append({"object_id": obj["id"], "asset_id": obj["asset_id"], "props": obj["props"], "items": items})

    known_ids = {
        "sack_id": "00000000-0000-0000-0000-00000000a001",
        "backpack_id": "00000000-0000-0000-0000-00000000b001",
        "food_id": "00000000-0000-0000-0000-00000000c004",
        "example_node": "forest_path_9596da"
    }

    return {"ok": True, "actor": {"id": actor_id, "node_id": node_id, "x": x, "y": y}, "inventory": inv, "ground": ground, "known_ids": known_ids}

class DebugSeedIn(BaseModel):
    node_id: str = "forest_path_9596da"
    x: int = 5
    y: int = 5
    actor_id: str = "player"

@app.post("/debug/seed_state")
async def debug_seed_state(body: DebugSeedIn, session: AsyncSession = Depends(get_session)):
    """
    Идемпотентный сидер стартовой сцены:
      - создаёт (если нет) ноду body.node_id и ставит актёра в (x,y)
      - гарантирует kind'ы: cloth_sack, basic_backpack, canned_food
      - для игрока (actor_id='player') — фикс-UID предметы (a001/b001/c004) и их раскладка
      - для прочих актёров — пустой инвентарь (без конфликтов уникальных индексов)
    """
    aid = body.actor_id
    nid = body.node_id
    x, y = int(body.x), int(body.y)

    # 1) узел (твоя схема поддерживает расширенные поля — оставляем как было)
    await session.execute(text("""
        insert into nodes(id,title,biome,width,height,size_w,size_h,exits,content,description)
        values(:id,'Forest Path','forest',16,16,16,16,'{}'::jsonb,'{}'::jsonb,'')
        on conflict (id) do nothing
    """), {"id": nid})

    # 2) актёр
    await session.execute(text("""
        insert into actors(id, kind, node_id, x, y, hp, mood, trust, aggression)
        values(:aid,'player', :nid, :x, :y, 100, 'neutral', 50, 0)
        on conflict (id) do update set node_id=:nid, x=:x, y=:y
    """), {"aid": aid, "nid": nid, "x": x, "y": y})

    # 3) виды предметов
    await session.execute(
        text("""
            insert into item_kinds(
                id,title,tags,handedness,
                props,grid_w,grid_h,hands_required,
                size_w,size_h,is_container
            )
            values
              ('cloth_sack','Мешок',      ARRAY['container'],'one_hand',
                :sack_props, 2, 2, 1,
                2, 2, true),
              ('basic_backpack','Рюкзак',  ARRAY['container'],'back',
                :bp_props,   4, 4, 0,
                4, 4, true),
              ('canned_food','Консерва',   ARRAY['food'],'one_hand',
                :food_props, 0, 0, 1,
                0, 0, false)
            on conflict (id) do update set
                props          = excluded.props,
                grid_w         = excluded.grid_w,
                grid_h         = excluded.grid_h,
                hands_required = excluded.hands_required,
                size_w         = excluded.size_w,
                size_h         = excluded.size_h,
                is_container   = excluded.is_container
        """).bindparams(
            bindparam("sack_props", {"ui": "sack"}, type_=JSONB),
            bindparam("bp_props",   {"ui": "backpack"}, type_=JSONB),
            bindparam("food_props", {}, type_=JSONB),
        )
    )

    # 4) фикс-UID предметы (только для игрока)
    sack_id_fixed = "00000000-0000-0000-0000-00000000a001"
    backpack_id_fixed = "00000000-0000-0000-0000-00000000b001"
    food_id_fixed = "00000000-0000-0000-0000-00000000c004"

    if aid == "player":
        await session.execute(text("""
            insert into items(id, kind_id, charges, durability)
            values
              (CAST(:sack AS uuid),     'cloth_sack',     null, null),
              (CAST(:bp AS uuid),       'basic_backpack', null, null),
              (CAST(:food AS uuid),     'canned_food',    null, null)
            on conflict (id) do nothing
        """), {"sack": sack_id_fixed, "bp": backpack_id_fixed, "food": food_id_fixed})

        # 5) инвентарь игрока
        await session.execute(text("""
            insert into inventories(actor_id, left_item, right_item, hidden_slot, equipped_bag, backpack)
            values (:aid, null, CAST(:sack AS uuid), null, null, ARRAY[CAST(:bp AS uuid)])
            on conflict (actor_id) do update
               set left_item=null,
                   right_item=CAST(:sack AS uuid),
                   hidden_slot=null,
                   equipped_bag=null,
                   backpack=ARRAY[CAST(:bp AS uuid)]
        """), {"aid": aid, "sack": sack_id_fixed, "bp": backpack_id_fixed})

        # 6) мешок: очистить слоты и положить консерву внутрь
        await session.execute(
            text("delete from carried_container_slots where container_item_id = CAST(:sack AS uuid)"),
            {"sack": sack_id_fixed}
        )
        await session.execute(
            text("delete from carried_container_slots where item_id = CAST(:food AS uuid)"),
            {"food": food_id_fixed}
        )
        await session.execute(text("""
            insert into carried_container_slots(container_item_id, slot_x, slot_y, item_id)
            values (CAST(:sack AS uuid), 0, 0, CAST(:food AS uuid))
            on conflict (container_item_id, slot_x, slot_y) do update
                set item_id = excluded.item_id
        """), {"sack": sack_id_fixed, "food": food_id_fixed})

        seeded_items = {"sack_id": sack_id_fixed, "backpack_id": backpack_id_fixed, "food_id": food_id_fixed}
    else:
        # для не-игроков — пустой инвентарь, чтобы не конфликтовать по уникальным индексам
        await session.execute(text("""
            insert into inventories(actor_id, left_item, right_item, hidden_slot, equipped_bag, backpack)
            values (:aid, null, null, null, null, '{}'::uuid[])
            on conflict (actor_id) do update
               set left_item=null,
                   right_item=null,
                   hidden_slot=null,
                   equipped_bag=null,
                   backpack='{}'::uuid[]
        """), {"aid": aid})
        seeded_items = {}

    # 7) подчистить возможные лут-объекты на клетке (разнесено на два вызова)
    params = {"nid": nid, "x": x, "y": y}
    await session.execute(
        text("""
            delete from object_inventories
             where object_id in (
                select id
                  from node_objects
                 where node_id = :nid and x = :x and y = :y and layer = 3
             )
        """),
        params,
    )
    await session.execute(
        text("""
            delete from node_objects
             where node_id = :nid and x = :x and y = :y and layer = 3
        """),
        params,
    )

    await session.commit()
    return {
        "ok": True,
        "seeded": True,
        "actor_id": aid,
        "node_id": nid,
        "x": x, "y": y,
        "items": seeded_items,
    }

# минимальный сидап (без узлов)
class SeedMiniIn(BaseModel):
    actor_id: str = "player"
    add_food: bool = True
    ensure_sack: bool = True
    ensure_backpack: bool = True

@app.post("/debug/seed_mini")
async def debug_seed_mini(body: SeedMiniIn, session: AsyncSession = Depends(get_session)):
    aid = body.actor_id

    await session.execute(text("""
        insert into inventories(actor_id, left_item, right_item, hidden_slot, equipped_bag, backpack)
        values (:aid, null, null, null, null, '{}'::uuid[])
        on conflict (actor_id) do nothing
    """), {"aid": aid})

    # виды предметов
    await session.execute(
        text("""
            insert into item_kinds(
                id,title,tags,handedness,
                props,grid_w,grid_h,hands_required,
                size_w,size_h,is_container
            )
            values
              ('basic_backpack','Рюкзак','{container}','back',
                :bp_props,   4, 3, 0,
                4, 3, true),
              ('cloth_sack','Мешок','{container}','one_hand',
                :sack_props,  2, 2, 1,
                2, 2, true),
              ('food_apple','Яблоко','{food}','one_hand',
                :apple_props, 0, 0, 0,
                0, 0, false)
            on conflict (id) do update set
                props          = excluded.props,
                grid_w         = excluded.grid_w,
                grid_h         = excluded.grid_h,
                hands_required = excluded.hands_required,
                size_w         = excluded.size_w,
                size_h         = excluded.size_h,
                is_container   = excluded.is_container
        """).bindparams(
            bindparam("bp_props",    {"ui": "backpack"}, type_=JSONB),
            bindparam("sack_props",  {"ui": "sack"}, type_=JSONB),
            bindparam("apple_props", {}, type_=JSONB),
        )
    )

    await session.execute(text("""
        insert into items(id,kind_id,charges,durability) values
          ('00000000-0000-0000-0000-00000000b001','basic_backpack',null,null),
          ('00000000-0000-0000-0000-00000000a001','cloth_sack',null,null),
          ('00000000-0000-0000-0000-00000000c004','food_apple',null,null)
        on conflict (id) do nothing
    """))

    for iid, enabled in [
        ("00000000-0000-0000-0000-00000000b001", body.ensure_backpack),
        ("00000000-0000-0000-0000-00000000a001", body.ensure_sack),
        ("00000000-0000-0000-0000-00000000c004", body.add_food),
    ]:
        if not enabled:
            continue
        await session.execute(text("""
            update inventories
               set backpack = case
                   when not (CAST(:iid as uuid) = any(coalesce(backpack,'{}'::uuid[])))
                   then array_append(coalesce(backpack,'{}'::uuid[]), CAST(:iid as uuid))
                   else backpack end
             where actor_id=:aid
        """), {"aid": aid, "iid": iid})

    await session.commit()
    return {"ok": True}

# ────────────────────────────────────────────────────────────────────────────────
# СИДЕР ПРОСТОГО NPC (нож в руке, яблоко в рюкзаке)
# ────────────────────────────────────────────────────────────────────────────────
class SeedNpcIn(BaseModel):
    npc_id: str = "wolf_1"
    kind: str = "npc"
    archtype: str = "wolf"
    hp: int = 30
    mood: str = "aggressive"
    trust: int = 0
    aggression: int = 80
    # если пусто — поставим к игроку
    node_id: Optional[str] = None
    x: Optional[int] = None
    y: Optional[int] = None

@app.post("/debug/seed_npc_simple")
async def debug_seed_npc_simple(body: SeedNpcIn, session: AsyncSession = Depends(get_session)):
    # координаты: если не заданы — берём позицию игрока
    pos = (await session.execute(text("""
        select node_id, x, y from actors where id='player'
    """))).mappings().first()
    if not pos and (body.node_id is None):
        raise HTTPException(status_code=400, detail="player_position_unknown_and_node_not_provided")
    nid = body.node_id or pos["node_id"]
    x = body.x if body.x is not None else int(pos["x"])
    y = body.y if body.y is not None else int(pos["y"])

    # Базовые item_kinds (нож, яблоко) — идемпотентно
    await session.execute(
        text("""
            insert into item_kinds(id,title,tags,handedness,props,grid_w,grid_h,hands_required,size_w,size_h,is_container)
            values
              ('knife_basic','Нож','{melee,blade}','one_hand', '{}'::jsonb, 0,0,1, 0,0, false),
              ('food_apple','Яблоко','{food}','one_hand','{}'::jsonb, 0,0,0, 0,0, false)
            on conflict (id) do nothing
        """)
    )

    # Создаём/обновляем NPC
    await session.execute(text("""
        insert into actors(id, kind, archtype, node_id, x, y, hp, mood, trust, aggression, meta)
        values(:id, :kind, :arch, :nid, :x, :y, :hp, :mood, :trust, :aggr, '{}'::jsonb)
        on conflict (id) do update set
            kind=excluded.kind, archtype=excluded.archtype,
            node_id=excluded.node_id, x=excluded.x, y=excluded.y,
            hp=excluded.hp, mood=excluded.mood, trust=excluded.trust, aggression=excluded.aggression
    """), {
        "id": body.npc_id, "kind": body.kind, "arch": body.archtype,
        "nid": nid, "x": x, "y": y, "hp": body.hp,
        "mood": body.mood, "trust": body.trust, "aggr": body.aggression
    })

    # Два предмета: нож и яблоко (новые uuid)
    knife_id_row = (await session.execute(
        text("insert into items(kind_id) values ('knife_basic') returning id")
    )).mappings().first()
    apple_id_row = (await session.execute(
        text("insert into items(kind_id) values ('food_apple') returning id")
    )).mappings().first()
    knife_id = str(knife_id_row["id"])
    apple_id = str(apple_id_row["id"])

    # Инвентарь NPC: нож в правую руку, яблоко в рюкзак
    await session.execute(text("""
        insert into inventories(actor_id, left_item, right_item, hidden_slot, equipped_bag, backpack)
        values (:aid, null, CAST(:knife as uuid), null, null, ARRAY[CAST(:apple as uuid)])
        on conflict (actor_id) do update set
            left_item=null,
            right_item=CAST(:knife as uuid),
            hidden_slot=null,
            equipped_bag=null,
            backpack=ARRAY[CAST(:apple as uuid)]
    """), {"aid": body.npc_id, "knife": knife_id, "apple": apple_id})

    await session.commit()
    return {"ok": True, "npc_id": body.npc_id, "node_id": nid, "x": x, "y": y, "knife": knife_id, "apple": apple_id}

# ────────────────────────────────────────────────────────────────────────────────
# РЕГИСТРАЦИЯ РОУТЕРОВ
# ────────────────────────────────────────────────────────────────────────────────
app.include_router(world.router)
app.include_router(narrative.router)
app.include_router(assets.router)
app.include_router(status_router.router)
app.include_router(turn_router.router)     # важно для /world/turn/advance
app.include_router(battle_router.router)   # боевой роутер

app.include_router(inventory_router)
app.include_router(items_router)
# Health-check корень
@app.get("/")
async def root():
    return {"ok": True}
