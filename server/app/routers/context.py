# app/routers/context.py
from __future__ import annotations
from typing import Dict, Any, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.db import get_session

router = APIRouter(prefix="/combat", tags=["combat"])

# -----------------------------
# 1) Полный боевой контекст
# -----------------------------
@router.get("/context/{session_id}")
async def combat_context(session_id: str, session: AsyncSession = Depends(get_session)) -> Dict[str, Any]:
    sess = (await session.execute(text("""
        select id, node_id, turn_index, state
          from battle_sessions
         where id=:sid
    """), {"sid": session_id})).mappings().first()
    if not sess:
        raise HTTPException(status_code=404, detail="battle_session_not_found")

    parts = (await session.execute(text("""
        select bp.actor_id, bp.team, bp.initiative, bp.alive, bp.join_order,
               a.stats,
               coalesce((a.meta->'ai'->>'hostility_to_player')::int, 0) as hostility_to_player,
               ik.title as armor_title,
               ik.armor_level as armor_level
          from battle_participants bp
          join actors a on a.id = bp.actor_id
     left join inventories inv on inv.actor_id = bp.actor_id
     left join items it on it.id = inv.equipped_armor
     left join item_kinds ik on ik.id = it.kind_id
         where bp.session_id=:sid
         order by bp.join_order
    """), {"sid": session_id})).mappings().all()

    skills = (await session.execute(text("""
        select actor_id, label, note, tags, applied_at_turn, duration_turns
          from actor_skills
         where session_id=:sid
    """), {"sid": session_id})).mappings().all()

    statuses = (await session.execute(text("""
        select actor_id, label, turns_left, intensity, coalesce(meta,'{}'::jsonb) as meta
          from actor_statuses
         where session_id=:sid
    """), {"sid": session_id})).mappings().all()

    skills_by_actor: Dict[str, List[Dict[str, Any]]] = {}
    for r in skills:
        skills_by_actor.setdefault(r["actor_id"], []).append({
            "label": r["label"],
            "note": r["note"],
            "tags": r["tags"],
            "applied_at_turn": int(r["applied_at_turn"]),
            "duration_turns": int(r["duration_turns"]),
        })

    statuses_by_actor: Dict[str, List[Dict[str, Any]]] = {}
    for r in statuses:
        statuses_by_actor.setdefault(r["actor_id"], []).append({
            "label": r["label"],
            "turns_left": int(r["turns_left"]),
            "intensity": int(r["intensity"]),
            "meta": dict(r["meta"]) if r["meta"] else {},
        })

    roster = []
    for p in parts:
        aid = p["actor_id"]
        roster.append({
            "actor_id": aid,
            "team": p["team"],
            "alive": bool(p["alive"]),
            "initiative": int(p["initiative"]),
            "stats": dict(p["stats"] or {}),
            "armor": {
                "title": p["armor_title"],
                "level": int(p["armor_level"]) if p["armor_level"] is not None else 0
            },
            "skills": skills_by_actor.get(aid, []),
            "statuses": statuses_by_actor.get(aid, []),
            # NEW: отношение NPC к герою (0..100; 0 нейтрально). Это просто данные для ЛЛМ.
            "attitude": {
                "to_hero": int(p["hostility_to_player"] or 0),
                "scale": "0-100"
            },
        })

    return {
        "ok": True,
        "session": {
            "id": sess["id"],
            "node_id": sess["node_id"],
            "turn_index": int(sess["turn_index"]),
            "state": sess["state"],
        },
        "actors": roster
    }

# -----------------------------------------
# 2) Локальный контекст 3×3 вокруг актёра
# -----------------------------------------
@router.get("/context/grid/{actor_id}")
async def grid_3x3(
    actor_id: str,
    session_id: str = Query(..., description="ID боевой сессии (для списка участников)"),
    session: AsyncSession = Depends(get_session)
):
    center = (await session.execute(text("""
        select id, node_id, x, y
          from actors
         where id=:aid
    """), {"aid": actor_id})).mappings().first()
    if not center:
        raise HTTPException(status_code=404, detail="actor_not_found")

    # Актёры в окне 3×3
    rows = (await session.execute(text("""
        select a.id as actor_id, a.x, a.y, a.stats,
               coalesce((a.meta->'ai'->>'hostility_to_player')::int, 0) as hostility_to_player
          from actors a
          join battle_participants bp on bp.actor_id = a.id
         where bp.session_id = :sid
           and a.node_id = :nid
           and a.x between :xmin and :xmax
           and a.y between :ymin and :ymax
    """), {
        "sid": session_id,
        "nid": center["node_id"],
        "xmin": center["x"] - 1, "xmax": center["x"] + 1,
        "ymin": center["y"] - 1, "ymax": center["y"] + 1,
    })).mappings().all()

    # тайлы (низкий слой) — как раньше
    tiles = []
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            tiles.append({
                "x": int(center["x"] + dx),
                "y": int(center["y"] + dy),
                "kind": "ground",
                "blocks_los": False,
                "blocks_move": False,
            })

    # объекты на слоях
    obj_rows = (await session.execute(text("""
        select o.id, o.asset_id, o.x, o.y, o.layer, coalesce(o.props,'{}'::jsonb) as props
          from node_objects o
         where o.node_id=:nid
           and o.x between :xmin and :xmax
           and o.y between :ymin and :ymax
    """), {
        "nid": center["node_id"],
        "xmin": center["x"] - 1, "xmax": center["x"] + 1,
        "ymin": center["y"] - 1, "ymax": center["y"] + 1,
    })).mappings().all()

    objects = []
    for r in obj_rows:
        props = dict(r["props"] or {})
        kind = props.get("kind", "prop")
        blocks_los = bool(props.get("blocks_los", kind in ("tree", "wall")))
        blocks_move = bool(props.get("blocks_move", kind in ("tree", "wall")))
        pickupable = bool(props.get("pickupable", kind in ("loot",)))
        is_container = bool(props.get("is_container", False))
        objects.append({
            "id": int(r["id"]),
            "asset_id": r["asset_id"],
            "x": int(r["x"]),
            "y": int(r["y"]),
            "layer": int(r["layer"]),
            "kind": kind,
            "blocks_los": blocks_los,
            "blocks_move": blocks_move,
            "pickupable": pickupable,
            "is_container": is_container,
        })

    entities = []
    for r in rows:
        entities.append({
            "actor_id": r["actor_id"],
            "x": int(r["x"]),
            "y": int(r["y"]),
            "stats": dict(r["stats"] or {}),
            "is_center": (r["actor_id"] == actor_id),
            # NEW: соседям тоже отдаём отношение (удобно ЛЛМ для «кто агрится на игрока рядом»)
            "attitude": {
                "to_hero": int(r["hostility_to_player"] or 0),
                "scale": "0-100"
            },
        })

    return {
        "ok": True,
        "center": {"actor_id": center["id"], "node_id": center["node_id"], "x": int(center["x"]), "y": int(center["y"])},
        "area": {"w": 3, "h": 3},
        "entities": entities,
        "tiles": tiles,
        "objects": objects,
    }
