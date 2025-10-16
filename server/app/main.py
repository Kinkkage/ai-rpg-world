from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Literal

app = FastAPI(title="AI RPG World")

class Intent(BaseModel):
    type: Literal["MOVE","INSPECT","TALK"]
    payload: dict

class Event(BaseModel):
    type: Literal["MOVE_ANIM","TEXT","HIGHLIGHT"]
    payload: dict

class Node(BaseModel):
    id: str
    title: str
    size: dict
    props: list
    actors: list
    exits: list

DEMO_NODE = Node(
    id="castle_hall",
    title="Зал замка",
    size={"w":16,"h":16},
    props=[{"id":"chest_1","x":8,"y":6,"type":"chest"}],
    actors=[{"id":"player","x":5,"y":5,"type":"player"},
            {"id":"king","x":10,"y":6,"type":"npc","arch":"king"}],
    exits=[{"id":"to_courtyard","x":0,"y":8,"to":"castle_courtyard"}]
)

@app.get("/ping")
def ping():
    return {"ok": True}

@app.get("/node/{node_id}")
def get_node(node_id: str):
    return DEMO_NODE

@app.post("/intent")
def post_intent(intent: Intent) -> List[Event]:
    if intent.type == "MOVE":
        x, y = intent.payload.get("x"), intent.payload.get("y")
        return [
            {"type":"MOVE_ANIM","payload":{"actor_id":"player","to":{"x":x,"y":y},"ms":180}},
            {"type":"TEXT","payload":{"text":f"Вы переместились на ({x},{y})."}}
        ]
    if intent.type == "INSPECT":
        target = intent.payload.get("target_id")
        return [{"type":"TEXT","payload":{"text":f"Осматриваете {target}..."}}]
    if intent.type == "TALK":
        npc = intent.payload.get("npc_id","king")
        return [{"type":"TEXT","payload":{"text":f"{npc.title()}: Приветствую, странник."}}]
    return [{"type":"TEXT","payload":{"text":"Ничего не произошло."}}]
