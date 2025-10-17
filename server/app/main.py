# server/app/main.py
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Literal, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

# DB/DAO
from app.db import get_session
from app.dao import fetch_node, fetch_inventory

app = FastAPI(title="AI RPG World")

# ---------- SCHEMAS ----------
class Intent(BaseModel):
    type: Literal["MOVE","INSPECT","TALK","EQUIP","UNEQUIP","USE_ITEM","COMBINE_USE"]
    payload: Dict[str, Any]

class Event(BaseModel):
    type: Literal[
        "MOVE_ANIM","TEXT","HIGHLIGHT","FX","STATUS_APPLY","INVENTORY",
        "DAMAGE","HEAL","CONSUME","EQUIP_CHANGE"
    ]
    payload: Dict[str, Any]

# ---------- SYSTEM HEALTH ----------
@app.get("/ping")
def ping():
    return {"ok": True}

@app.get("/health/db")
async def health_db(session: AsyncSession = Depends(get_session)):
    """
    Проверка подключения к базе данных.
    Вернёт {"db": "ok"}, если всё работает.
    """
    try:
        await session.execute(text("select 1"))
        return {"db": "ok"}
    except Exception as e:
        # чтобы точно увидеть ошибку в логах Render
        raise HTTPException(status_code=500, detail=str(e))

# ---------- NODE from DB ----------
@app.get("/node/{node_id}")
async def get_node(node_id: str, session: AsyncSession = Depends(get_session)):
    node = await fetch_node(session, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node

# ---------- INVENTORY view from DB ----------
@app.get("/inventory/{actor_id}")
async def get_inventory(actor_id: str, session: AsyncSession = Depends(get_session)):
    return await fetch_inventory(session, actor_id)

# ---------- SIMPLE REGISTRIES (in-memory for MVP) ----------
ITEMS: Dict[str, Dict[str, Any]] = {
    "lighter": {
        "id": "lighter",
        "title": "Зажигалка",
        "description": "Карманная зажигалка. Даёт источник огня.",
        "tags": ["tool", "fire"],
        "handedness": "one_hand",
        "charges": 50,
        "props": {"ignite": True, "consumes_per_use": 1},
    },
    "deodorant": {
        "id": "deodorant",
        "title": "Дезодорант",
        "description": "Аэрозоль, легковоспламеним.",
        "tags": ["spray", "flammable"],
        "handedness": "one_hand",
        "charges": 20,
        "props": {"spray": True, "flammable": True, "consumes_per_use": 1},
    },
    "water_bottle": {
        "id": "water_bottle",
        "title": "Бутылка воды",
        "description": "Питьё и тушение огня.",
        "tags": ["liquid", "water"],
        "handedness": "one_hand",
        "charges": 3,
        "props": {"water": True, "consumes_per_use": 1},
    },
}

# Состояние игрока (упрощенно; пока в памяти процесса)
PLAYER: Dict[str, Any] = {
    "pos": {"x": 5, "y": 5},
    "hp": 100,
    "hands": {"left": None, "right": None},
    "backpack": ["lighter", "deodorant", "water_bottle"],
}

def _emit_text(msg: str) -> Event:
    return {"type": "TEXT", "payload": {"text": msg}}

def _equip(hand: str, item_id: str) -> List[Event]:
    ev = []
    if item_id not in PLAYER["backpack"]:
        return [_emit_text("Этого предмета нет в рюкзаке.")]
    if PLAYER["hands"][hand] is not None:
        ev.append(_emit_text(f"Рука {hand} занята. Сначала уберите предмет."))
        return ev
    PLAYER["backpack"].remove(item_id)
    PLAYER["hands"][hand] = item_id
    ev.append({"type": "EQUIP_CHANGE", "payload": {"hand": hand, "item": ITEMS[item_id]["title"]}})
    ev.append(_emit_text(f"Вы взяли в {hand} {ITEMS[item_id]['title']}."))
    return ev

def _unequip(hand: str) -> List[Event]:
    ev = []
    item_id = PLAYER["hands"][hand]
    if not item_id:
        return [_emit_text(f"В {hand} руке пусто.")]
    PLAYER["hands"][hand] = None
    PLAYER["backpack"].append(item_id)
    ev.append({"type": "EQUIP_CHANGE", "payload": {"hand": hand, "item": None}})
    ev.append(_emit_text(f"Вы убрали {ITEMS[item_id]['title']} в рюкзак."))
    return ev

def _consume(item_id: str, amount: int = 1) -> List[Event]:
    ev = []
    item = ITEMS[item_id]
    if item.get("charges", 0) < amount:
        ev.append(_emit_text(f"{item['title']} пуст."))
        return ev
    item["charges"] -= amount
    ev.append({"type": "CONSUME", "payload": {"item": item["title"], "delta": -amount, "left": item["charges"]}})
    return ev

def _use_item_single(item_id: str, target: Optional[str] = None) -> List[Event]:
    item = ITEMS[item_id]
    p = item["props"]
    ev = []
    if p.get("water"):
        ev += _consume(item_id, p.get("consumes_per_use", 1))
        ev.append({"type": "FX", "payload": {"kind": "splash", "on": target or "ground"}})
        ev.append(_emit_text("Вы плеснули воду. Огонь на цели погашен."))
    elif p.get("ignite"):
        ev += _consume(item_id, p.get("consumes_per_use", 1))
        ev.append({"type": "FX", "payload": {"kind": "spark", "on": target or "front"}})
        ev.append(_emit_text("Щёлк! Искра вспыхнула."))
    else:
        ev.append(_emit_text("Ничего не произошло."))
    return ev

def _combine_use(left_id: str, right_id: str, target: Optional[str] = None) -> List[Event]:
    ev = []
    pair = set([left_id, right_id])
    if {"lighter", "deodorant"} == pair:
        ev += _consume("lighter", 1)
        ev += _consume("deodorant", 1)
        ev.append({"type": "FX", "payload": {"kind": "flame_cone", "dir": "front", "range": 3, "width": 2}})
        ev.append({"type": "STATUS_APPLY", "payload": {"status": "Burn", "targets": "in_cone", "duration": 2}})
        ev.append(_emit_text("Вы пускаете струю огня! Враги впереди охвачены пламенем."))
    else:
        ev.append(_emit_text("Эти предметы не комбинируются."))
    return ev

@app.post("/intent")
def post_intent(intent: Intent) -> List[Event]:
    t = intent.type
    p = intent.payload
    if t == "MOVE":
        x, y = p.get("x"), p.get("y")
        PLAYER["pos"] = {"x": x, "y": y}
        return [
            {"type": "MOVE_ANIM", "payload": {"actor_id": "player", "to": {"x": x, "y": y}, "ms": 180}},
            _emit_text(f"Вы переместились на ({x},{y})."),
        ]
    if t == "INSPECT":
        target = p.get("target_id", "")
        return [_emit_text(f"Осматриваете {target}...")]
    if t == "TALK":
        npc = p.get("npc_id", "king")
        return [_emit_text(f"{npc.title()}: Приветствую, странник.")]
    if t == "EQUIP":
        hand = p.get("hand", "right")
        item_id = p.get("item_id")
        return _equip(hand, item_id)
    if t == "UNEQUIP":
        hand = p.get("hand", "right")
        return _unequip(hand)
    if t == "USE_ITEM":
        item_id = p.get("item_id")
        target = p.get("target_id")
        if item_id not in PLAYER["hands"].values():
            return [_emit_text("Нужно держать предмет в руке.")]
        return _use_item_single(item_id, target)
    if t == "COMBINE_USE":
        left = PLAYER["hands"]["left"]
        right = PLAYER["hands"]["right"]
        if not left or not right:
            return [_emit_text("Нужно держать предметы в обеих руках.")]
        return _combine_use(left, right, p.get("target_id"))
    return [_emit_text("Ничего не произошло.")]

# -------- /talk endpoint ----------
class TalkIn(BaseModel):
    npc_id: str
    text: str

class TalkOut(BaseModel):
    say: str
    intents: List[Dict[str, Any]] = []

@app.post("/talk", response_model=TalkOut)
def talk(input: TalkIn):
    if input.npc_id == "king":
        return {"say": "Я слушаю. Что тебе нужно?", "intents": []}
    return {"say": "...", "intents": []}
