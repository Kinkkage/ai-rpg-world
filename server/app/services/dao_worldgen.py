# server/app/services/dao_worldgen.py
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
import random, uuid

from sqlalchemy import text, bindparam
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import JSONB

# _____ CONSTANTS / DIRECTIONS __________________________________________________

DIRS = ("N", "E", "S", "W")
REV = {"N": "S", "S": "N", "E": "W", "W": "E"}

# _____ RNG / HELPERS ___________________________________________________________

def _rng_from_seed(seed_str: str) -> random.Random:
    """Детерминированный RNG без внешних пакетов (FNV-подобный хеш)."""
    h = 2166136261
    for ch in seed_str:
        h ^= ord(ch)
        h *= 16777619
        h &= 0xFFFFFFFF
    return random.Random(h)

def _pick_weighted(rng: random.Random, weights: Dict[str, float]) -> str:
    """Выбор ключа словаря по весам."""
    total = sum(max(0.0, float(v)) for v in weights.values()) or 1.0
    r = rng.random() * total
    acc = 0.0
    for k, v in weights.items():
        acc += max(0.0, float(v))
        if r <= acc:
            return k
    # fallback
    return next(iter(weights.keys())) if weights else "grass"

# _____ RULES ___________________________________________________________________

async def _get_rule(session: AsyncSession, biome: str) -> Dict[str, Any]:
    """Читает jsonb-конфиг правил из gen_rules по биому."""
    row = (
        await session.execute(
            text("SELECT config FROM gen_rules WHERE biome = :b"),
            {"b": biome},
        )
    ).mappings().first()
    return (row and row["config"]) or {}

# _____ GRAPH / WORLD EDGES _____________________________________________________

async def _ensure_reverse_exit(session: AsyncSession, a: str, dir_: str, b: str) -> None:
    """Создаёт A --dir--> B и обратную связь B --rev(dir)--> A (upsert)."""
    if dir_ not in DIRS:
        raise ValueError("bad dir")
    # прямая
    await session.execute(
        text(
            """
            INSERT INTO world_edges(from_node, dir, to_node)
            VALUES (:a, :d, :b)
            ON CONFLICT (from_node, dir) DO UPDATE SET to_node = EXCLUDED.to_node
            """
        ),
        {"a": a, "d": dir_, "b": b},
    )
    # обратная
    rd = REV[dir_]
    await session.execute(
        text(
            """
            INSERT INTO world_edges(from_node, dir, to_node)
            VALUES (:b, :rd, :a)
            ON CONFLICT (from_node, dir) DO UPDATE SET to_node = EXCLUDED.to_node
            """
        ),
        {"a": a, "b": b, "rd": rd},
    )

# _____ CORE: SPAWN NODE ________________________________________________________

async def spawn_node_db(
    session: AsyncSession,
    node_id: str,
    *,
    biome: str = "forest",
    difficulty: int = 1,   # сейчас — просто маркер, без влияния на вероятности
    seed: Optional[str] = None,
    size: Tuple[int, int] = (16, 16),
) -> Dict[str, Any]:
    """
    Генерит содержимое узла детерминированно от seed:
      - L1 фон (terrain) по весам,
      - L2 блокеры (tree/rock),
      - L3 лут (списком координат),
      - NPC по вероятностям (min..max),
      - POI (редко), пишет в meta.poi,
      - сохраняет всё в nodes.content/meta и пишет лог.
    """
    width, height = size
    rule = await _get_rule(session, biome) or {}
    rng = _rng_from_seed(f"{node_id}|{biome}|{difficulty}|{seed or ''}")

    # --- гарантируем существование узла (скелет, если вдруг его нет) ---
    await session.execute(
        text("""
            INSERT INTO nodes (id, title, biome, exits, width, height, content, size_w, size_h, description, meta)
            VALUES (:id, :title, :biome, '{}'::jsonb, :w, :h, '{}'::jsonb, :w, :h, :desc, '{}'::jsonb)
            ON CONFLICT (id) DO NOTHING
        """),
        {
            "id": node_id,
            "title": f"{biome.capitalize()} Area",
            "biome": biome,
            "w": width,
            "h": height,
            "desc": f"Generated node: {biome}",
        },
    )

    # --- L1: фон ---
    l1 = rule.get("l1") or {"grass": 1.0}
    terrain: List[List[str]] = []
    for y in range(height):
        row = []
        for x in range(width):
            row.append(_pick_weighted(rng, l1))
        terrain.append(row)

    # --- L2: блокеры ---
    l2_blocks = rule.get("l2_blockers") or []
    for y in range(height):
        for x in range(width):
            for it in l2_blocks:
                p = float(it.get("p", 0.0) or 0.0)
                asset = it.get("asset")
                if asset and rng.random() < p:
                    if asset in ("tree", "rock"):  # в боёвке блокируют только эти
                        terrain[y][x] = asset

    # --- L3: лут ---
    loot_placements: List[Dict[str, Any]] = []
    for y in range(height):
        for x in range(width):
            for it in (rule.get("l3_loot") or []):
                if rng.random() < float(it.get("p", 0.0) or 0.0):
                    loot_placements.append({"x": x, "y": y, "kind": it.get("kind", "unknown")})

    # --- NPC ---
    npcs_spawned: List[str] = []
    for spec in (rule.get("npc") or []):
        p = float(spec.get("p", 0.0) or 0.0)
        if rng.random() < p:
            cnt = rng.randint(int(spec.get("min", 1)), int(spec.get("max", 1)))
            for _ in range(cnt):
                for _try in range(25):
                    x = rng.randrange(0, width)
                    y = rng.randrange(0, height)
                    if terrain[y][x] not in ("tree", "rock"):
                        npc_id = f"npc_{uuid.uuid4().hex[:4]}"
                        await session.execute(
                            text("""
                                INSERT INTO actors
                                    (id, kind, archtype, node_id, x, y, hp, mood, trust, aggression, meta)
                                VALUES
                                    (:id, 'npc', :arch, :node, :x, :y, 100, 'neutral', 50, 10, '{}'::jsonb)
                            """),
                            {"id": npc_id, "arch": (spec.get("arch") or "villager"), "node": node_id, "x": x, "y": y},
                        )
                        npcs_spawned.append(npc_id)
                        break

    # --- POI ---
    poi = None
    for it in (rule.get("poi") or []):
        if rng.random() < float(it.get("p", 0.0) or 0.0):
            poi = it.get("id")
            break

    # --- контент и мета как dict ---
    content: Dict[str, Any] = {"terrain": terrain, "loot": loot_placements}
    meta: Dict[str, Any] = {"poi": poi} if poi else {}

    # --- UPDATE nodes (JSONB bindparam) ---
    stmt = (
        text("""
            UPDATE nodes
               SET biome = :biome,
                   difficulty = :diff,
                   width = :w, height = :h,
                   size_w = :w, size_h = :h,
                   grid_base_w = :gbw, grid_base_h = :gbh,
                   grid_stride_combat = :gsc,
                   content = :content,
                   meta = COALESCE(meta, '{}'::jsonb) || :meta
             WHERE id = :id
        """).bindparams(bindparam("content", type_=JSONB), bindparam("meta", type_=JSONB))
    )

    await session.execute(
        stmt,
        {
            "id": node_id,
            "biome": biome,
            "diff": difficulty,
            "w": width,
            "h": height,
            "gbw": 32,
            "gbh": 32,
            "gsc": 2,
            "content": content,
            "meta": meta,
        },
    )

    # --- лог генерации (JSONB bindparam) ---
    log_stmt = text("""
        INSERT INTO world_gen_log(node_id, biome, difficulty, counts)
        VALUES (:id, :b, :d, :counts)
    """).bindparams(bindparam("counts", type_=JSONB))

    await session.execute(
        log_stmt,
        {"id": node_id, "b": biome, "d": difficulty, "counts": {"loot": len(loot_placements), "npcs": len(npcs_spawned)}},
    )

    await session.commit()

    return {
        "node_id": node_id,
        "biome": biome,
        "difficulty": difficulty,
        "size": [width, height],
        "npcs": npcs_spawned,
        "poi": poi,
    }

# _____ CORE: SPAWN ROUTE (V2) _________________________________________________

async def spawn_route_db(
    session: AsyncSession,
    *,
    from_node: str,
    dir: str,
    target_biome: Optional[str] = None,
    target_difficulty: Optional[int] = None,
    seed: Optional[str] = None,
    size: Tuple[int, int] = (16, 16),
) -> Dict[str, Any]:
    """
    Если выхода нет — создаёт новый узел по направлению dir от from_node,
    связывает двусторонне, генерирует содержимое и возвращает JSON узла.
    Если выход уже есть — просто возвращает существующий.
    """
    dir = dir.upper()
    if dir not in DIRS:
        raise ValueError("dir must be one of N/E/S/W")

    # Уже есть выход?
    row = (
        await session.execute(
            text(
                """
                SELECT to_node FROM world_edges WHERE from_node = :n AND dir = :d
                """
            ),
            {"n": from_node, "d": dir},
        )
    ).mappings().first()

    if row and row["to_node"]:
        to_node = row["to_node"]
        node_row = (
            await session.execute(
                text(
                    """
                    SELECT id, biome, difficulty, width, height, content, meta
                      FROM nodes
                     WHERE id = :id
                    """
                ),
                {"id": to_node},
            )
        ).mappings().first()
        return {"existed": True, "node": dict(node_row)} if node_row else {"existed": True, "node": {"id": to_node}}

    # Создаём новый узел и связываем
    biome = target_biome or "forest"
    difficulty = int(target_difficulty or 1)
    to_node = f"{biome}_{uuid.uuid4().hex[:6]}"

    await session.execute(
        text(
            """
            INSERT INTO nodes (id, title, biome, exits, width, height, content, size_w, size_h, description, meta)
            VALUES (:id, :title, :biome, '{}'::jsonb, :w, :h, '{}'::jsonb, :w, :h, :desc, '{}'::jsonb)
            """
        ),
        {
            "id": to_node,
            "title": f"{biome.capitalize()} Area",
            "biome": biome,
            "w": size[0],
            "h": size[1],
            "desc": f"Generated route: {biome}",
        },
    )

    await _ensure_reverse_exit(session, from_node, dir, to_node)

    # Генерация содержимого узла
    gen = await spawn_node_db(
        session,
        to_node,
        biome=biome,
        difficulty=difficulty,
        seed=seed,
        size=size,
    )

    event = {
        "type": "NODE_SPAWN",
        "payload": {"node_id": to_node, "biome": biome, "difficulty": difficulty},
    }

    return {
        "existed": False,
        "node": {
            "id": gen["node_id"],
            "biome": biome,
            "difficulty": difficulty,
            "size": gen["size"],
            "meta": {"poi": gen.get("poi")},
        },
        "event": event,
    }

# _____ MAP API ________________________________________________________________

async def get_world_map_db(session: AsyncSession) -> Dict[str, Any]:
    """Возвращает список узлов и рёбра графа для мини-карты."""
    nodes = (
        await session.execute(
            text(
                """
                SELECT id, biome, difficulty, meta
                  FROM nodes
                 ORDER BY created_at DESC NULLS LAST, id DESC
                """
            )
        )
    ).mappings().all()

    edges = (
        await session.execute(
            text(
                """
                SELECT from_node AS "from", dir, to_node AS "to"
                  FROM world_edges
                """
            )
        )
    ).mappings().all()

    return {"nodes": [dict(n) for n in nodes], "edges": [dict(e) for e in edges]}
