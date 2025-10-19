# server/app/main.py
import os
import json
import asyncio
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import List, Literal, Optional, Dict, Any, Tuple

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB

# DB / DAO
from app.db import get_session
from app.dao import (
    fetch_node,
    fetch_inventory,
    learn_skill,
    actor_knows_skill,
    list_skills,
)

# Роутеры
from app.routers import assets
from app.routers.world import spawn_route as world_spawn_route, SpawnRouteRequest
from app.routers import world, narrative
from app.routers.narrative import NarrateIn, narrate as narrate_endpoint

app = FastAPI(title="AI RPG World")

# ---------- WebSocket manager ----------
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
    # evt — уже в формате {"type": "...", "payload": {...}}
    await manager.broadcast_json({"event": evt})

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()  # игнорируем любые входящие сообщения
    except WebSocketDisconnect:
        manager.disconnect(ws)

# ---------- SCHEMAS ----------
class Intent(BaseModel):
    type: Literal["MOVE", "INSPECT", "TALK", "EQUIP", "UNEQUIP", "USE_ITEM", "COMBINE_USE", "ATTACK"]
    payload: Dict[str, Any]

# терпим новые типы событий (например, NODE_CHANGE, TEXT_RICH)
class Event(BaseModel):
    type: str
    payload: Dict[str, Any]

# ---------- NARRATIVE HELPERS ----------
def _split_chunks(s: str, maxlen: int = 48):
    """Дробим текст на читаемые кусочки, не рвём слова."""
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
    """Псевдо-стрим: шлём кусочки в сокет как NARRATE_PART, затем NARRATE_DONE."""
    chunks = _split_chunks(text, maxlen=48)
    if not chunks:
        return
    for ch in chunks:
        await broadcast_event({"type": "NARRATE_PART", "payload": {"chunk": ch, "style": style}})
        await asyncio.sleep(delay_ms / 1000)
    await broadcast_event({"type": "NARRATE_DONE", "payload": {"style": style}})

async def _get_node_biome(session: AsyncSession, node_id: str) -> str:
    row = (await session.execute(
        text("select biome from nodes where id=:id"),
        {"id": node_id}
    )).mappings().first()
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
    """Внутренний вызов /narrate, собирает абзац. Возвращает TEXT_RICH или None."""
    biome = await _get_node_biome(session, node_id)
    style_id = _style_for_biome(biome, is_battle=is_battle)
    body = NarrateIn(
        node_id=node_id,
        style_id=style_id,
        events=events,
        context={"biome": biome, **(context_extra or {})}
    )
    out = await narrate_endpoint(body, session)  # прямой вызов обработчика
    if out and out.text:
        return {"type": "TEXT_RICH", "payload": {"text": out.text, "style": style_id}}
    return None

# ---------- HELPERS ----------
# Направления и их противоположности
OPPOSITE_DIR = {
    "north": "south",
    "south": "north",
    "east":  "west",
    "west":  "east",
}

async def _ensure_reverse_exit(
    session: AsyncSession,
    from_node_id: str,      # узел, из которого пришли (A)
    to_node_id: str,        # узел, в который пришли (B)
    direction: str          # направление, по которому шли (например, "north")
):
    """
    Гарантируем, что в узле B есть обратный выход в A.
    Если шли на north A->B, то в B появится south -> A.
    Не перезаписывает существующие связи.
    """
    opposite = OPPOSITE_DIR.get(direction)
    if not opposite:
        return

    row = (
        await session.execute(
            text("select exits from nodes where id=:id"),
            {"id": to_node_id},
        )
    ).mappings().first()
    if not row:
        return

    exits_b = _normalize_exits(row.get("exits"))
    if exits_b.get(opposite) == from_node_id:
        return  # уже есть корректная обратная ссылка

    if exits_b.get(opposite) is None:
        exits_b[opposite] = from_node_id
        await session.execute(
            text("update nodes set exits=:exits where id=:id"),
            {"id": to_node_id, "exits": json.dumps(exits_b)},
        )
        # commit делаем выше по стеку

def _emit_text(msg: str) -> Event:
    return {"type":"TEXT","payload":{"text":msg}}

def _normalize_exits(value: Any) -> Dict[str, Any]:
    """
    Приводит exits из БД к dict:
    - {} -> {}
    - None -> {}
    - строка JSON -> dict/{} (если это не объект)
    - list/другие типы -> {}
    """
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

# ---------- SYSTEM HEALTH ----------
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

# ---------- NODE ----------
@app.get("/node/{node_id}")
async def get_node(node_id: str, session: AsyncSession = Depends(get_session)):
    node = await fetch_node(session, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node

# ---------- INVENTORY ----------
@app.get("/inventory/{actor_id}")
async def get_inventory(actor_id: str, session: AsyncSession = Depends(get_session)):
    return await fetch_inventory(session, actor_id)

# ---------- NPC STATE ----------
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

# ---------- SIMPLE PLAYER MOCK ----------
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

# ---------- SKILLS ----------
class LearnSkillIn(BaseModel):
    actor_id: str = "player"
    skill_id: str

@app.post("/skills/learn")
async def post_learn_skill(data: LearnSkillIn, session: AsyncSession = Depends(get_session)):
    return await learn_skill(session, data.actor_id, data.skill_id)

# --- простой «тон» текста ---
POS_WORDS = ["спасибо","благодарю","признателен","молодец","добр","уважаю","восхищаюсь","выручил"]
NEG_WORDS = [
    "плох","ненавижу","дурак","идиот","туп","предам","обманул","врёшь",
    "угроза","напасть","убью","убить","жалкий","никчемный","дурной","скотина"
]

def classify_tone(s: str) -> str:
    s = (s or "").lower()
    if any(w in s for w in NEG_WORDS):
        return "neg"
    if any(w in s for w in POS_WORDS):
        return "pos"
    return "neutral"

# --- Навыки из текста ---
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

# ---------- INTENT ----------
@app.post("/intent")
async def post_intent(intent: Intent, session: AsyncSession = Depends(get_session)) -> List[Event]:
    t = intent.type
    p = intent.payload

    # Креативный текст и навыки
    if t in ["USE_ITEM","COMBINE_USE"] and "text" in p:
        found = await detect_skill_from_text(session, p["text"])
        if found:
            skill_id, sk = found
            if not await actor_knows_skill(session, "player", skill_id):
                return [{"type":"TEXT","payload":{"text":"Вы это пока не умеете."}}]
            text_needs_weapon = _text_has_any(p["text"], WEAPON_KEYWORDS)
            if not text_needs_weapon:
                return [
                    {"type":"SKILL_USE","payload":{"skill_id":skill_id,"title":sk.get("title") or skill_id}},
                    {"type":"TEXT","payload":{"text":f"Вы применяете навык: {sk.get('title') or skill_id}."}}
                ]
            if not _has_item_with_tag("sword"):
                return [{"type":"TEXT","payload":{"text":"Нужен меч в руке, чтобы выполнить атаку с этим приёмом."}}]
            return [
                {"type":"SKILL_USE","payload":{"skill_id":skill_id,"title":sk.get("title") or skill_id}},
                {"type":"TEXT","payload":{"text":f"Вы применяете навык: {sk.get('title') or skill_id}."}},
                {"type":"FX","payload":{"kind":"power_slash","on":"front","bonus":True}},
                {"type":"TEXT","payload":{"text":"Вы совмещаете приём с атакой мечом!"}}
            ]

    # ---------- MOVE: переход между узлами по направлению или по координатам
    if t == "MOVE":
        direction = p.get("direction")
        if direction:
            # 1) текущий узел игрока
            current_node = (
                await session.execute(text("select node_id from actors where id='player'"))
            ).scalar()
            if not current_node:
                return [{"type":"TEXT","payload":{"text":"Игрок не привязан к узлу."}}]

            # 2) exits текущего узла
            row = (
                await session.execute(
                    text("select exits from nodes where id=:id"),
                    {"id": current_node},
                )
            ).mappings().first()
            if not row:
                return [{"type":"TEXT","payload":{"text":f"Текущий узел {current_node} не найден."}}]

            exits = _normalize_exits(row.get("exits"))

            # 3) куда идём
            next_node = exits.get(direction)

            # 4) если выхода нет — создаём новый узел и сохраняем связь
            if not next_node:
                req = SpawnRouteRequest(theme="forest_path", size=[16, 16])
                res = await world_spawn_route(req, session)   # внутренний вызов роутера
                next_node = res.node_id

                exits[direction] = next_node
                await session.execute(
                    text("update nodes set exits=:exits where id=:id"),
                    {"id": current_node, "exits": json.dumps(exits)},
                )
                await session.commit()

            # 5) переносим игрока
            await session.execute(
                text("update actors set node_id=:nid where id='player'"),
                {"nid": next_node},
            )

            # 6) обратный выход
            await _ensure_reverse_exit(
                session,
                from_node_id=current_node,
                to_node_id=next_node,
                direction=direction,
            )

            # фиксируем обе операции
            await session.commit()

            # 7) базовые события + нарратив
            ev: List[Event] = [
                {"type": "TEXT", "payload": {"text": f"Вы переместились в {next_node}."}},
                {"type": "NODE_CHANGE", "payload": {"node_id": next_node}},
            ]
            rich = await compose_narrative(
                session,
                node_id=next_node,
                events=list(ev),
                context_extra={"actor_id": "player"},
                is_battle=False
            )

            # либо стримим, либо добавляем сразу в ответ
            if rich:
                if os.getenv("NARRATE_STREAM_FAKE", "0") == "1":
                    await stream_text_rich(rich["payload"]["text"], rich["payload"]["style"])
                else:
                    ev.append(rich)

            return ev

        # ---- перемещение по координатам в пределах узла
        x, y = p.get("x"), p.get("y")
        PLAYER["pos"] = {"x": x, "y": y}
        return [
            {"type":"MOVE_ANIM","payload":{"actor_id":"player","to":{"x":x,"y":y},"ms":180}},
            {"type":"TEXT","payload":{"text":f"Вы переместились на ({x},{y})."}},
        ]

    if t == "INSPECT":
        return [{"type":"TEXT","payload":{"text":f"Осматриваете {p.get('target_id','...')}..."}}]
    if t == "TALK":
        return [{"type":"TEXT","payload":{"text":f"{p.get('npc_id','king').title()}: Приветствую."}}]
    if t == "EQUIP":
        return _equip(p.get("hand","right"), p.get("item_id"))
    if t == "UNEQUIP":
        return _unequip(p.get("hand","right"))
    if t == "USE_ITEM":
        item_id = p.get("item_id")
        if item_id not in PLAYER["hands"].values():
            return [{"type":"TEXT","payload":{"text":"Нужно держать предмет в руке."}}]
        return _use_item_single(item_id, p.get("target_id"))
    if t == "COMBINE_USE":
        left, right = PLAYER["hands"]["left"], PLAYER["hands"]["right"]
        if not left or not right:
            return [{"type":"TEXT","payload":{"text":"Нужно держать предметы в обеих руках."}}]
        return _combine_use(left, right, p.get("target_id"))

    # Реакция NPC в бою
    if t == "ATTACK" and "target_id" in p:
        npc_id = p["target_id"]
        await session.execute(text("""
            update actors
               set trust = greatest(trust - 15, 0),
                   aggression = least(aggression + 25, 100)
             where id = :id and kind = 'npc'
        """), {"id": npc_id})
        await session.execute(text("""
            insert into npc_memories(actor_id, category, event, description)
            values(:id, 'combat', 'combat', 'Был атакован игроком')
        """), {"id": npc_id})
        await session.commit()

        ev: List[Event] = [{"type":"TEXT","payload":{"text":f"Вы напали на {npc_id}!"}}]
        for e in ev:
            await broadcast_event(e)

        npc = (await session.execute(text("select aggression from actors where id=:id"), {"id": npc_id})).mappings().first()
        if npc and int(npc["aggression"]) > 70:
            counter = {"type":"NPC_ATTACK","payload":{"npc_id": npc_id}}
            ev.append(counter)
            await broadcast_event(counter)
        return ev

    return [{"type":"TEXT","payload":{"text":"Ничего не произошло."}}]

# ---------- TALK: память + эмоции NPC ----------
@app.post("/talk")
async def talk_to_npc(data: Dict[str, Any], session: AsyncSession = Depends(get_session)):
    npc_id = data.get("npc_id")
    text_in = (data.get("text") or "")

    npc = (
        await session.execute(
            text("""
                select id, archtype, mood, trust, aggression, node_id, kind
                from actors
                where id=:id
            """),
            {"id": npc_id},
        )
    ).mappings().first()
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

    await session.execute(
        text("update actors set mood=:mood, trust=:trust where id=:id"),
        {"mood": mood, "trust": trust, "id": npc_id},
    )

    insert_mem = text("""
        insert into npc_memories(actor_id, category, event, description, payload)
        values(:aid, :cat, :evt, :desc, :payload)
    """).bindparams(
        bindparam("aid"),
        bindparam("cat"),
        bindparam("evt"),
        bindparam("desc"),
        bindparam("payload", type_=JSONB),
    )

    await session.execute(
        insert_mem,
        {
            "aid": npc_id,
            "cat": category,
            "evt": category,
            "desc": low[:100],
            "payload": {"player": "player", "reply": reply, "ts": datetime.utcnow().isoformat()},
        },
    )

    await session.commit()

    ev_text = {"type": "TEXT", "payload": {"text": reply}}
    ev_state = {"type": "NPC_STATE", "payload": {"npc_id": npc_id, "mood": mood, "trust": trust}}

    # Узел для стиля нарратива: узел NPC или игрока
    npc_node = (await session.execute(
        text("select node_id from actors where id=:id"),
        {"id": npc_id}
    )).scalar()
    player_node = (await session.execute(
        text("select node_id from actors where id='player'")
    )).scalar()
    current_node = npc_node or player_node

    rich = None
    if current_node:
        rich = await compose_narrative(
            session,
            node_id=current_node,
            events=[ev_text, ev_state],
            context_extra={"actor_id": "player", "npc_id": npc_id, "mode": "talk"},
            is_battle=False
        )

    # live-апдейт клиентам
    await broadcast_event(ev_text)
    await broadcast_event(ev_state)

    out_events = [ev_text, ev_state]

    if rich:
        if os.getenv("NARRATE_STREAM_FAKE", "0") == "1":
            await stream_text_rich(rich["payload"]["text"], rich["payload"]["style"])
        else:
            await broadcast_event(rich)
            out_events.append(rich)

    return {"events": out_events}

# ✅ РЕГИСТРИРУЕМ РОУТЕРЫ
app.include_router(world.router)
app.include_router(narrative.router)
app.include_router(assets.router)