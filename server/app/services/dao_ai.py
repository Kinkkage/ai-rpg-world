# server/app/services/dao_ai.py
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple

import json
import random

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


# ─────────────────────────────
# Утилиты геометрии / видимости
# ─────────────────────────────
def manhattan(a: Tuple[int, int], b: Tuple[int, int]) -> int:
    return abs(a[0] - b[0]) + abs(a[1] - b[1])


def bresenham_line(x0: int, y0: int, x1: int, y1: int):
    # Классический алгоритм Брезенхема (для LoS)
    dx = abs(x1 - x0)
    dy = -abs(y1 - y0)
    sx = 1 if x0 < x1 else -1
    sy = 1 if y0 < y1 else -1
    err = dx + dy
    x, y = x0, y0
    while True:
        yield (x, y)
        if x == x1 and y == y1:
            break
        e2 = 2 * err
        if e2 >= dy:
            err += dy
            x += sx
        if e2 <= dx:
            err += dx
            y += sy


async def load_node_terrain(session: AsyncSession, node_id: str) -> Optional[List[List[str]]]:
    row = (
        await session.execute(
            text("""SELECT content FROM nodes WHERE id = :nid"""),
            {"nid": node_id},
        )
    ).mappings().first()
    if not row:
        return None
    content = row["content"]
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except Exception:
            content = {}
    return content.get("terrain")


def is_blocking(tile: str) -> bool:
    return tile in ("tree", "rock")


async def has_los(session: AsyncSession, node_id: str, a: Tuple[int, int], b: Tuple[int, int]) -> bool:
    terrain = await load_node_terrain(session, node_id)
    if not terrain:
        return True
    for (x, y) in bresenham_line(a[0], a[1], b[0], b[1]):
        if (x, y) == a or (x, y) == b:
            continue
        if 0 <= y < len(terrain) and 0 <= x < len(terrain[0]):
            if is_blocking(terrain[y][x]):
                return False
    return True


def in_bounds(terrain: Optional[List[List[str]]], x: int, y: int) -> bool:
    if not terrain:
        return True
    return 0 <= y < len(terrain) and 0 <= x < len(terrain[0])


async def step_towards(
    session: AsyncSession,
    node_id: str,
    from_xy: Tuple[int, int],
    to_xy: Tuple[int, int],
) -> Tuple[int, int]:
    """
    Делаем один шаг по манхэттену к цели.
    Пытаемся не заходить на блокирующие тайлы (tree/rock) и не выходить за границы.
    Если оба направления блокируются — остаёмся на месте.
    """
    fx, fy = from_xy
    tx, ty = to_xy

    terrain = await load_node_terrain(session, node_id)

    # приоритет осей: сначала та, где больше дельта
    dx = 1 if tx > fx else -1 if tx < fx else 0
    dy = 1 if ty > fy else -1 if ty < fy else 0

    cand: List[Tuple[int, int]] = []
    # сначала пробуем ось с большей разницей
    if abs(tx - fx) >= abs(ty - fy):
        if dx != 0:
            cand.append((fx + dx, fy))
        if dy != 0:
            cand.append((fx, fy + dy))
    else:
        if dy != 0:
            cand.append((fx, fy + dy))
        if dx != 0:
            cand.append((fx + dx, fy))

    for nx, ny in cand:
        if in_bounds(terrain, nx, ny):
            if not terrain or not is_blocking(terrain[ny][nx]):
                return nx, ny

    # если оба направления плохие — остаёмся
    return fx, fy


# ─────────────────────────────
# Ядро AI
# ─────────────────────────────
async def run_ai_turn_db(session: AsyncSession, actor_id: str) -> Optional[Dict[str, Any]]:
    """
    Делает один AI-ход для NPC:
      - видимость и поиск цели,
      - ATTACK при расстоянии <=1,
      - MOVE в сторону цели,
      - FOLLOW игрока в friendly,
      - иначе IDLE/небольшое смещение.
    Возвращает одно событие (dict) или None.
    """
    # 1) сам NPC
    me = (
        await session.execute(
            text(
                """
                SELECT id, kind, node_id, x, y, hp, ai_mode, target_id, mood, trust, aggression, meta
                  FROM actors
                 WHERE id = :id
                """
            ),
            {"id": actor_id},
        )
    ).mappings().first()

    if not me or me["kind"] != "npc":
        return None
    if int(me["hp"] or 0) <= 0:
        return None
    if not me["node_id"]:
        return {"type": "NPC_IDLE", "payload": {"actor_id": actor_id}}

    node_id = me["node_id"]
    my_pos = (int(me["x"]), int(me["y"]))
    ai_mode = (me["ai_mode"] or "neutral").lower()
    trust = int(me["trust"] or 50)
    aggression = int(me["aggression"] or 0)

    # 2) окружение
    others = (
        await session.execute(
            text(
                """
                SELECT id, kind, x, y, hp, ai_mode, mood, trust, aggression
                  FROM actors
                 WHERE node_id = :nid
                   AND id <> :me
                   AND hp > 0
                """
            ),
            {"nid": node_id, "me": actor_id},
        )
    ).mappings().all()

    # критерий врага
    def is_enemy(row) -> bool:
        # Игрок — враг сразу, если мы в режиме aggressive
        if row["kind"] == "player":
            if ai_mode == "aggressive":
                return True
            # запасной социальный критерий
            return (aggression - trust) >= 10
        # npc vs npc — пока не враждуют
        return False

    # 3) поиск ближайшей видимой цели
    target = None
    target_dist = 10**9
    SEARCH_RADIUS = 6

    for r in others:
        if not is_enemy(r):
            continue
        pos = (int(r["x"]), int(r["y"]))
        d = manhattan(my_pos, pos)
        if d <= SEARCH_RADIUS and await has_los(session, node_id, my_pos, pos):
            if d < target_dist:
                target, target_dist = r, d

    # 4) выбор действия
    # ATTACK при дистанции <=1
    if ai_mode in ("aggressive", "friendly", "neutral") and target and target_dist <= 1:
        # placeholder: реальную атаку потом заменим на боевой DAO
        new_hp_row = (
            await session.execute(
                text("""UPDATE actors SET hp = GREATEST(0, hp - 1) WHERE id = :tid RETURNING hp"""),
                {"tid": target["id"]},
            )
        ).mappings().first()
        await session.commit()
        return {
            "type": "NPC_ATTACK",
            "payload": {
                "actor_id": actor_id,
                "target_id": target["id"],
                "hp": int(new_hp_row["hp"]) if new_hp_row else None,
            },
        }

    # MOVE_TO к цели
    if ai_mode in ("aggressive", "friendly") and target and target_dist > 1:
        tx, ty = int(target["x"]), int(target["y"])
        nx, ny = await step_towards(session, node_id, my_pos, (tx, ty))
        if (nx, ny) != my_pos:
            await session.execute(
                text("""UPDATE actors SET x = :x, y = :y WHERE id = :id"""),
                {"x": nx, "y": ny, "id": actor_id},
            )
            await session.commit()
            return {"type": "NPC_MOVE", "payload": {"actor_id": actor_id, "to": [nx, ny]}}

    # FOLLOW игрока (держим 1..2 клетки)
    if ai_mode == "friendly":
        player = next((r for r in others if r["kind"] == "player"), None)
        if player:
            px, py = int(player["x"]), int(player["y"])
            d = manhattan(my_pos, (px, py))
            if d > 2:
                nx, ny = await step_towards(session, node_id, my_pos, (px, py))
                if (nx, ny) != my_pos:
                    await session.execute(
                        text("""UPDATE actors SET x = :x, y = :y WHERE id = :id"""),
                        {"x": nx, "y": ny, "id": actor_id},
                    )
                    await session.commit()
                    return {
                        "type": "NPC_FOLLOW",
                        "payload": {"actor_id": actor_id, "to": [nx, ny], "target_id": player["id"]},
                    }

    # IDLE / небольшое блуждание
    if random.random() < 0.33:
        terrain = await load_node_terrain(session, node_id)
        x, y = my_pos
        dx, dy = random.choice([(1, 0), (-1, 0), (0, 1), (0, -1), (0, 0)])
        nx, ny = x + dx, y + dy
        if in_bounds(terrain, nx, ny) and (not terrain or not is_blocking(terrain[ny][nx])):
            await session.execute(
                text("""UPDATE actors SET x = :x, y = :y WHERE id = :id"""),
                {"x": nx, "y": ny, "id": actor_id},
            )
            await session.commit()
            return {"type": "NPC_IDLE_MOVE", "payload": {"actor_id": actor_id, "to": [nx, ny]}}

    return {"type": "NPC_IDLE", "payload": {"actor_id": actor_id}}
