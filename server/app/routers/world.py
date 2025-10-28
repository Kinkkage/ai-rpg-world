# server/app/routers/world.py
import json
import uuid
import random
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.db import get_session
# Новые DAO для генерации мира (глава 14)
from app.services.dao_worldgen import spawn_node_db, spawn_route_db, get_world_map_db

router = APIRouter(prefix="/world", tags=["world"])


# ─────────────────────────────────────────────────────────
# СТАРЫЕ МОДЕЛИ/ЭНДПОИНТЫ (оставляем для совместимости)
# ─────────────────────────────────────────────────────────
class SpawnRouteRequest(BaseModel):
    name: str = "new_zone"
    theme: str = "forest_path"
    size: List[int] = [16, 16]
    npc_spawn_prob: float = 0.15  # 0.0..1.0


class SpawnRouteResponse(BaseModel):
    ok: bool
    node_id: str
    size: List[int]
    npcs: List[str] = []


@router.post("/spawn_route", response_model=SpawnRouteResponse)
async def spawn_route(data: SpawnRouteRequest, session: AsyncSession = Depends(get_session)):
    """
    УСТАРЕВШИЙ вариант генерации (оставлен для совместимости клиента).
    Новый — см. POST /world/spawn_node и /world/spawn_route2.
    """
    name = data.name
    theme = data.theme
    width, height = (data.size[0], data.size[1]) if len(data.size) == 2 else (16, 16)

    node_id = f"{theme}_{uuid.uuid4().hex[:6]}"
    exits: Dict[str, Optional[str]] = {}

    # простая генерация тайлов
    terrain: List[List[str]] = []
    for y in range(height):
        row = []
        for x in range(width):
            r = random.random()
            if r < 0.10:
                row.append("tree")
            elif r < 0.12:
                row.append("rock")
            else:
                row.append("grass")
        row and terrain.append(row)

    content: Dict[str, Any] = {"name": name, "theme": theme, "terrain": terrain}

    # ВСТАВКА УЗЛА (под текущую схему)
    await session.execute(
        text("""
            INSERT INTO nodes (id, title, biome, exits, width, height, content, size_w, size_h, description)
            VALUES (:id, :title, :biome, :exits, :w, :h, :content, :sw, :sh, :desc)
        """),
        {
            "id": node_id,
            "title": f"{theme.capitalize()} Area",
            "biome": theme,
            "exits": json.dumps(exits),
            "w": width,
            "h": height,
            "content": json.dumps(content),
            "sw": width,
            "sh": height,
            "desc": f"Рандомно сгенерированная зона: {theme}",
        },
    )

    spawned_npcs: List[str] = []

    # спавн одного NPC с вероятностью
    if random.random() < max(0.0, min(1.0, data.npc_spawn_prob)):
        npc_id = f"npc_{uuid.uuid4().hex[:4]}"
        await session.execute(
            text("""
                INSERT INTO actors (id, kind, archtype, node_id, mood, trust, aggression)
                VALUES (:id, 'npc', 'villager', :node, 'neutral', 50, 10)
            """),
            {"id": npc_id, "node": node_id},
        )
        await session.execute(
            text("""
                INSERT INTO npc_memories (actor_id, category, event, description, payload)
                VALUES (:aid, 'world', 'spawn', 'Появился в новой зоне', :payload)
            """),
            {"aid": npc_id, "payload": json.dumps({"node_id": node_id})},
        )
        spawned_npcs.append(npc_id)

    await session.commit()

    return SpawnRouteResponse(ok=True, node_id=node_id, size=[width, height], npcs=spawned_npcs)


# ---------- ОТЛАДОЧНЫЕ ЭНДПОИНТЫ (как были) ----------

@router.get("/nodes")
async def list_nodes(
    limit: int = Query(20, ge=1, le=300),
    session: AsyncSession = Depends(get_session),
):
    rows = (
        await session.execute(
            text("""
                SELECT id, title, biome, exits, width, height
                FROM nodes
                ORDER BY created_at DESC NULLS LAST, id DESC
                LIMIT :limit
            """),
            {"limit": limit},
        )
    ).mappings().all()

    def norm(v):
        if v is None:
            return {}
        if isinstance(v, dict):
            return v
        if isinstance(v, str):
            try:
                p = json.loads(v)
                return p if isinstance(p, dict) else {}
            except Exception:
                return {}
        return {}

    return [
        {
            "id": r["id"],
            "title": r["title"],
            "biome": r["biome"],
            "exits": norm(r["exits"]),
            "size": [r["width"], r["height"]],
        }
        for r in rows
    ]


@router.get("/node_raw/{node_id}")
async def node_raw(node_id: str, session: AsyncSession = Depends(get_session)):
    row = (
        await session.execute(
            text("""
                SELECT id, title, biome, exits, width, height, content, description
                FROM nodes
                WHERE id = :id
            """),
            {"id": node_id},
        )
    ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="node not found (raw)")

    # нормализуем exits для наглядности
    exits = row["exits"]
    if isinstance(exits, str):
        try:
            exits = json.loads(exits)
        except Exception:
            exits = {}

    return {
        "id": row["id"],
        "title": row["title"],
        "biome": row["biome"],
        "exits": exits or {},
        "width": row["width"],
        "height": row["height"],
        "content_keys": list((row["content"] or {}).keys()) if isinstance(row["content"], dict) else ["terrain?"],
        "description": row["description"],
    }


# ─────────────────────────────────────────────────────────
# НОВЫЕ ЭНДПОИНТЫ ГЛАВЫ 14
# ─────────────────────────────────────────────────────────

class SpawnNodeReq(BaseModel):
    node_id: str
    biome: Optional[str] = None
    difficulty: Optional[int] = 1
    seed: Optional[str] = None
    size: Optional[List[int]] = [16, 16]


@router.post("/spawn_node")
async def spawn_node_api(data: SpawnNodeReq, session: AsyncSession = Depends(get_session)):
    """
    Генерирует содержимое указанного узла (terrain/loot/NPC/POI) по правилам биома
    детерминированно от seed. Узел должен существовать в nodes.
    """
    size = tuple(data.size or [16, 16])
    res = await spawn_node_db(
        session,
        data.node_id,
        biome=(data.biome or "forest"),
        difficulty=int(data.difficulty or 1),
        seed=data.seed,
        size=size,
    )
    return {"ok": True, **res}


class SpawnRouteV2Req(BaseModel):
    from_node: str
    dir: str
    target_biome: Optional[str] = None
    target_difficulty: Optional[int] = None
    seed: Optional[str] = None
    size: Optional[List[int]] = [16, 16]


@router.post("/spawn_route2")
async def spawn_route_v2(data: SpawnRouteV2Req, session: AsyncSession = Depends(get_session)):
    """
    Создаёт (если не существует) узел по направлению от from_node, связывает двусторонне,
    генерирует его содержимое и возвращает JSON узла. Если выход уже есть — просто возвращает его.
    """
    size = tuple(data.size or [16, 16])
    res = await spawn_route_db(
        session,
        from_node=data.from_node,
        dir=data.dir,
        target_biome=data.target_biome,
        target_difficulty=data.target_difficulty,
        seed=data.seed,
        size=size,
    )
    return {"ok": True, **res}


@router.get("/map")
async def world_map(session: AsyncSession = Depends(get_session)):
    """
    Мини-карта мира: {nodes: [...], edges: [...]}
    """
    res = await get_world_map_db(session)
    return {"ok": True, **res}
