# server/app/dao.py
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import UUID, ARRAY, JSONB
import json
from app.services.armor import effective_armor_level, apply_armor_reduction
from app.services.status_mods import get_status_combat_mods


# ===================== NODE =====================
async def fetch_node(session: AsyncSession, node_id: str):
    # Берём размеры гибко: width/height или size_w/size_h (что есть в схеме)
    node = (
        await session.execute(
            text(
                """
                SELECT
                    id,
                    title,
                    biome,
                    COALESCE(width, size_w, 16)  AS w,
                    COALESCE(height, size_h, 16) AS h,
                    exits,
                    content,
                    description
                FROM nodes
                WHERE id = :id
                """
            ),
            {"id": node_id},
        )
    ).mappings().first()

    if not node:
        return None

    # exits может быть json/jsonb или текстом — нормализуем к dict
    exits_raw = node.get("exits")
    if exits_raw is None:
        exits: Dict[str, Any] = {}
    elif isinstance(exits_raw, dict):
        exits = exits_raw
    elif isinstance(exits_raw, str):
        try:
            parsed = json.loads(exits_raw)
            exits = parsed if isinstance(parsed, dict) else {}
        except Exception:
            exits = {}
    else:
        exits = {}

    # актёры с координатами (x, y)
    actors = (
        await session.execute(
            text(
                """
                SELECT id, kind, archtype, node_id, x, y, hp, mood, trust, aggression
                FROM actors
                WHERE node_id = :id
                """
            ),
            {"id": node_id},
        )
    ).mappings().all()

    # объекты (props/decoration) с координатами и слоем
    objects = (
        await session.execute(
            text(
                """
                SELECT id, asset_id, x, y, rotation, props, layer
                FROM node_objects
                WHERE node_id = :id
                ORDER BY y, x, layer, id
                """
            ),
            {"id": node_id},
        )
    ).mappings().all()

    # факты
    facts = (
        await session.execute(
            text(
                """
                SELECT k, v FROM facts WHERE node_id = :id
                """
            ),
            {"id": node_id},
        )
    ).mappings().all()

    return {
        "id": node["id"],
        "title": node["title"],
        "biome": node["biome"],
        "size": {"w": int(node["w"]), "h": int(node["h"])},
        "actors": [dict(a) for a in actors],
        "objects": [dict(o) for o in objects],
        "exits": exits,
        "facts": {f["k"]: f["v"] for f in facts},
        "content": node.get("content"),
        "description": node.get("description"),
    }


# ===================== INVENTORY (VIEW) =====================
async def _brief_item(session: AsyncSession, item_id):
    """Короткое описание предмета с параметрами kind, включая контейнерные поля."""
    if not item_id:
        return None
    row = (
        await session.execute(
            text(
                """
                select i.id, i.kind_id, i.charges, i.durability,
                       k.title, k.tags, k.handedness, k.props,
                       k.grid_w, k.grid_h, k.hands_required
                  from items i
                  join item_kinds k on k.id = i.kind_id
                 where i.id = :iid
                """
            ),
            {"iid": item_id},
        )
    ).mappings().first()
    return dict(row) if row else None


async def _grid_view(session: AsyncSession, container_item_id):
    """
    Возвращает описание грида переносимого контейнера (рюкзак или мешок):
    { item_id, grid_w, grid_h, slots:[{x,y,item_id}] }
    """
    cont = await _brief_item(session, container_item_id)
    if not cont:
        return None

    gw = int(cont.get("grid_w") or 0)
    gh = int(cont.get("grid_h") or 0)
    if gw <= 0 or gh <= 0:
        return None  # не контейнер

    rows = (
        await session.execute(
            text(
                """
                select slot_x as x, slot_y as y, item_id
                  from carried_container_slots
                 where container_item_id = :cid
                 order by y, x
                """
            ),
            {"cid": container_item_id},
        )
    ).mappings().all()
    filled = {(r["x"], r["y"]): r["item_id"] for r in rows}

    slots: List[Dict[str, Any]] = []
    for y in range(gh):
        for x in range(gw):
            iid = filled.get((x, y))
            slots.append({"x": x, "y": y, "item_id": iid})
    return {
        "item_id": str(container_item_id),
        "grid_w": gw,
        "grid_h": gh,
        "slots": slots,
    }


async def fetch_inventory(session: AsyncSession, actor_id: str):
    """
    Расширенная выдача инвентаря:
    - руки (и если в руке мешок — отдадим его грид),
    - скрытая ячейка hidden_slot,
    - активный рюкзак equipped_bag (грид),
    - legacy-массив backpack (как было раньше — в поле backpack_legacy).
    """
    inv = (
        await session.execute(
            text(
                """
                select actor_id, left_item, right_item, hidden_slot, equipped_bag, backpack
                  from inventories
                 where actor_id = :id
                """
            ),
            {"id": actor_id},
        )
    ).mappings().first()

    if not inv:
        return {
            "left_hand": None,
            "right_hand": None,
            "hidden_slot": None,
            "backpack": None,
            "backpack_legacy": [],
        }

    # --- руки
    left_brief = await _brief_item(session, inv["left_item"])
    right_brief = await _brief_item(session, inv["right_item"])

    # если в руке переносимый контейнер (мешок/пакет) — отрисуем грид
    left_grid = None
    if (
        left_brief
        and (left_brief.get("grid_w") and left_brief.get("grid_h"))
        and (int(left_brief.get("hands_required") or 0) == 1)
    ):
        left_grid = await _grid_view(session, left_brief["id"])

    right_grid = None
    if (
        right_brief
        and (right_brief.get("grid_w") and right_brief.get("grid_h"))
        and (int(right_brief.get("hands_required") or 0) == 1)
    ):
        right_grid = await _grid_view(session, right_brief["id"])

    # --- скрытая ячейка
    hidden_brief = await _brief_item(session, inv.get("hidden_slot"))

    # --- активный рюкзак
    backpack_grid = None
    if inv.get("equipped_bag"):
        backpack_grid = await _grid_view(session, inv["equipped_bag"])

    # --- legacy массив (старое поле) — не ломаем
    backpack_ids = inv.get("backpack") or []
    backpack_legacy: List[Dict[str, Any]] = []
    if backpack_ids:
        stmt = (
            text(
                """
            select i.id, k.id as kind_id, k.title, i.charges
              from items i
              join item_kinds k on k.id = i.kind_id
             where i.id = any(:ids)
            """
            ).bindparams(bindparam("ids", value=backpack_ids, type_=ARRAY(UUID(as_uuid=True))))
        )
        rows = (await session.execute(stmt)).mappings().all()
        backpack_legacy = [dict(r) for r in rows]

    return {
        "left_hand": {"item": left_brief, "grid": left_grid},
        "right_hand": {"item": right_brief, "grid": right_grid},
        "hidden_slot": {"item": hidden_brief},
        "backpack": backpack_grid,  # новый рюкзак-грид (если надет)
        "backpack_legacy": backpack_legacy,  # старый массив для совместимости
    }


# ===================== SKILLS =====================
async def learn_skill(session: AsyncSession, actor_id: str, skill_id: str):
    sk = (
        await session.execute(
            text(
                """
              select id, min_level from skills where id=:sid
            """
            ),
            {"sid": skill_id},
        )
    ).mappings().first()
    if not sk:
        return {"ok": False, "reason": "skill_not_found"}

    actor = (
        await session.execute(
            text(
                """
              select level, skill_tokens from actors where id=:aid
            """
            ),
            {"aid": actor_id},
        )
    ).mappings().first()
    if not actor:
        return {"ok": False, "reason": "actor_not_found"}

    if (actor["level"] or 0) < (sk["min_level"] or 1):
        return {"ok": False, "reason": "level_too_low"}

    if (actor["skill_tokens"] or 0) < 1:
        return {"ok": False, "reason": "no_tokens"}

    await session.execute(
        text(
            """
          insert into actor_skills(actor_id,skill_id) values(:aid,:sid)
          on conflict do nothing
        """
        ),
        {"aid": actor_id, "sid": skill_id},
    )

    await session.execute(
        text(
            """
          update actors set skill_tokens = skill_tokens - 1 where id=:aid
        """
        ),
        {"aid": actor_id},
    )

    await session.commit()
    return {"ok": True}


async def actor_knows_skill(session: AsyncSession, actor_id: str, skill_id: str) -> bool:
    row = await session.execute(
        text(
            """
            select 1 from actor_skills where actor_id=:aid and skill_id=:sid
        """
        ),
        {"aid": actor_id, "sid": skill_id},
    )
    return row.first() is not None


async def list_skills(session: AsyncSession):
    rows = (
        await session.execute(
            text(
                """
                select id, title, props
                from skills
            """
            )
        )
    ).mappings().all()
    return [dict(r) for r in rows]


# ===================== INVENTORY (DB ACTIONS) =====================
async def _get_inventory_row(session: AsyncSession, actor_id: str):
    return (
        await session.execute(
            text(
                """
                select actor_id, left_item, right_item, backpack
                from inventories where actor_id=:aid
                """
            ),
            {"aid": actor_id},
        )
    ).mappings().first()


async def _item_view_full(session: AsyncSession, item_id) -> Optional[Dict[str, Any]]:
    if not item_id:
        return None
    row = (
        await session.execute(
            text(
                """
                select i.id, i.kind_id, i.charges, i.durability,
                       k.title, k.tags, k.handedness, k.props
                from items i
                join item_kinds k on k.id = i.kind_id
                where i.id = :iid
                """
            ),
            {"iid": item_id},
        )
    ).mappings().first()
    return dict(row) if row else None


async def _handedness(session: AsyncSession, item_id) -> str:
    r = (
        await session.execute(
            text(
                """
                select k.handedness
                from items i join item_kinds k on k.id=i.kind_id
                where i.id=:iid
                """
            ),
            {"iid": item_id},
        )
    ).mappings().first()
    return (r and r["handedness"]) or "one_hand"


async def equip_item_db(session: AsyncSession, actor_id: str, hand: str, item_id) -> List[Dict[str, Any]]:
    inv = await _get_inventory_row(session, actor_id)
    if not inv:
        raise ValueError("Inventory not found")

    in_backpack = await session.execute(
        text(
            """
            select CAST(:iid AS uuid) = any(coalesce(backpack,'{}'::uuid[])) as ok
            from inventories where actor_id=:aid
        """
        ),
        {"iid": item_id, "aid": actor_id},
    )
    if not in_backpack.scalar():
        return [{"type": "TEXT", "payload": {"text": "Этого предмета нет в рюкзаке."}}]

    hd = await _handedness(session, item_id)

    if hd in ("one_hands", "one_hand"):
        current = inv[f"{hand}_item"]
        if current:
            return [{"type": "TEXT", "payload": {"text": f"Рука {hand} занята."}}]

        await session.execute(
            text(
                f"""
                update inventories
                set backpack = array_remove(coalesce(backpack,'{{}}'::uuid[]), CAST(:iid AS uuid)),
                    { 'left_item' if hand=='left' else 'right_item' } = CAST(:iid AS uuid)
                where actor_id=:aid
                """
            ),
            {"iid": item_id, "aid": actor_id},
        )
        await session.commit()

        iv = await _item_view_full(session, item_id)
        return [
            {"type": "EQUIP_CHANGE", "payload": {"hand": hand, "item": iv["title"]}},
            {"type": "TEXT", "payload": {"text": f"Вы взяли в {hand} {iv['title']}."}},
        ]

    if inv["left_item"] or inv["right_item"]:
        return [{"type": "TEXT", "payload": {"text": "Это двуручный предмет — освободите обе руки."}}]

    await session.execute(
        text(
            """
            update inventories
            set backpack = array_remove(coalesce(backpack,'{}'::uuid[]), CAST(:iid AS uuid)),
                left_item = CAST(:iid AS uuid),
                right_item = CAST(:iid AS uuid)
            where actor_id=:aid
            """
        ),
        {"iid": item_id, "aid": actor_id},
    )
    await session.commit()

    iv = await _item_view_full(session, item_id)
    return [
        {"type": "EQUIP_CHANGE", "payload": {"hand": "both", "item": iv["title"]}},
        {"type": "TEXT", "payload": {"text": f"Вы взяли {iv['title']} двумя руками."}},
    ]


async def unequip_item_db(session: AsyncSession, actor_id: str, hand: str) -> List[Dict[str, Any]]:
    inv = await _get_inventory_row(session, actor_id)
    if not inv:
        raise ValueError("Inventory not found")

    cur = inv[f"{hand}_item"]
    if not cur:
        return [{"type": "TEXT", "payload": {"text": f"В {hand} руке пусто."}}]

    hd = await _handedness(session, cur)
    if hd == "two_hands":
        await session.execute(
            text(
                """
                update inventories
                set left_item = null, right_item = null,
                    backpack = array_append(coalesce(backpack,'{}'::uuid[]), CAST(:iid AS uuid))
                where actor_id=:aid
                """
            ),
            {"iid": cur, "aid": actor_id},
        )
        await session.commit()

        iv = await _item_view_full(session, cur)
        return [
            {"type": "EQUIP_CHANGE", "payload": {"hand": "both", "item": None}},
            {"type": "TEXT", "payload": {"text": f"Вы убрали {iv['title']} в рюкзак."}},
        ]

    await session.execute(
        text(
            f"""
            update inventories
            set { 'left_item' if hand=='left' else 'right_item' } = null,
                backpack = array_append(coalesce(backpack,'{{}}'::uuid[]), CAST(:iid AS uuid))
            where actor_id=:aid
            """
        ),
        {"iid": cur, "aid": actor_id},
    )
    await session.commit()

    iv = await _item_view_full(session, cur)
    return [
        {"type": "EQUIP_CHANGE", "payload": {"hand": hand, "item": None}},
        {"type": "TEXT", "payload": {"text": f"Вы убрали {iv['title']} в рюкзак."}},
    ]


# ===================== USE / COMBINE (DB) =====================
async def use_item_db(session: AsyncSession, actor_id: str, item_id, target: Optional[str]) -> List[Dict[str, Any]]:
    iv = await _item_view_full(session, item_id)
    if not iv:
        return [{"type": "TEXT", "payload": {"text": "Предмет не найден."}}]

    props = iv.get("props") or {}
    ev: List[Dict[str, Any]] = []

    async def _consume(amount: int):
        if iv["charges"] is None:
            return
        if (iv["charges"] or 0) < amount:
            ev.append({"type": "TEXT", "payload": {"text": f"{iv['title']} пуст."}})
            return
        row = (
            await session.execute(
                text(
                    """
                    update items set charges = charges - :amt
                    where id=:iid
                    returning charges
                    """
                ),
                {"amt": amount, "iid": item_id},
            )
        ).mappings().first()
        left = row and row["charges"]
        ev.append({"type": "CONSUME", "payload": {"item": iv["title"], "delta": -amount, "left": left}})

    if props.get("water"):
        await _consume(props.get("consumes_per_use", 1))
        ev.append({"type": "FX", "payload": {"kind": "splash", "on": target or "ground"}})
        ev.append({"type": "TEXT", "payload": {"text": "Вы плеснули воду."}})
    elif props.get("ignite"):
        await _consume(props.get("consumes_per_use", 1))
        ev.append({"type": "FX", "payload": {"kind": "spark", "on": target or "front"}})
        ev.append({"type": "TEXT", "payload": {"text": "Щёлк! Искра вспыхнула."}})
    else:
        ev.append({"type": "TEXT", "payload": {"text": "Ничего не произошло."}})

    await session.commit()
    return ev


async def combine_use_db(session: AsyncSession, actor_id: str) -> List[Dict[str, Any]]:
    inv = await _get_inventory_row(session, actor_id)
    left = inv["left_item"]
    right = inv["right_item"]
    if not left or not right:
        return [{"type": "TEXT", "payload": {"text": "Нужно держать предметы в обеих руках."}}]

    lv = await _item_view_full(session, left)
    rv = await _item_view_full(session, right)
    pair = {lv["kind_id"], rv["kind_id"]}

    ev: List[Dict[str, Any]] = []

    async def _consume(iid, title, amount: int = 1):
        row = (
            await session.execute(
                text(
                    """
                    update items set charges = charges - :amt
                    where id=:iid
                    returning charges
                    """
                ),
                {"amt": amount, "iid": iid},
            )
        ).mappings().first()
        left = row and row["charges"]
        ev.append({"type": "CONSUME", "payload": {"item": title, "delta": -amount, "left": left}})

    if pair == {"lighter", "deodorant"}:
        if (lv["charges"] or 0) < 1 or (rv["charges"] or 0) < 1:
            return [{"type": "TEXT", "payload": {"text": "Не хватает зарядов."}}]
        await _consume(left, lv["title"], 1)
        await _consume(right, rv["title"], 1)
        ev.append({"type": "FX", "payload": {"kind": "flame_cone", "dir": "front", "range": 3, "width": 2}})
        ev.append({"type": "STATUS_APPLY", "payload": {"status": "Burn", "targets": "in_cone", "duration": 2}})
        ev.append({"type": "TEXT", "payload": {"text": "Вы пускаете струю огня!"}})
        await session.commit()
        return ev

    return [{"type": "TEXT", "payload": {"text": "Эти предметы не комбинируются."}}]


# ===================== BACKPACK / BAG EQUIP (FIXED) =====================
async def equip_backpack_db(session: AsyncSession, actor_id: str, item_id: str):
    """
    Надеть рюкзак: если предмет - контейнер (grid_w>0) и нет уже надетого,
    устанавливаем equipped_bag = item_id, убираем его из рюкзака-списка
    и (важно) освобождаем руку, если этот предмет был в одной из рук.
    """
    row = (
        await session.execute(
            text(
                """
                SELECT i.id, i.kind_id, k.grid_w, k.grid_h, k.hands_required, k.title
                  FROM items i
                  JOIN item_kinds k ON k.id = i.kind_id
                 WHERE i.id = :iid
                """
            ),
            {"iid": item_id},
        )
    ).mappings().first()
    if not row:
        return {"ok": False, "error": "item_not_found"}

    if not row["grid_w"]:
        return {"ok": False, "error": "not_a_container"}

    inv = (
        await session.execute(
            text(
                """
                SELECT backpack, equipped_bag, left_item, right_item
                  FROM inventories WHERE actor_id=:aid
                """
            ),
            {"aid": actor_id},
        )
    ).mappings().first()
    if not inv:
        return {"ok": False, "error": "no_inventory"}

    if inv["equipped_bag"]:
        return {"ok": False, "error": "already_has_backpack"}

    # Нормализуем к строкам для корректного сравнения UUID <-> str
    bp_ids = [str(x) for x in (inv["backpack"] or [])]
    in_backpack = str(item_id) in bp_ids
    in_left = (inv["left_item"] is not None) and (str(inv["left_item"]) == str(item_id))
    in_right = (inv["right_item"] is not None) and (str(inv["right_item"]) == str(item_id))

    if not (in_backpack or in_left or in_right):
        return {"ok": False, "error": "item_not_owned"}

    # Надеваем: снимаем из массива/backpack, и если был в руке — освобождаем её
    await session.execute(
        text(
            """
            UPDATE inventories
               SET equipped_bag = CAST(:iid AS uuid),
                   backpack     = array_remove(coalesce(backpack,'{}'::uuid[]), CAST(:iid AS uuid)),
                   left_item    = CASE WHEN left_item  = CAST(:iid AS uuid) THEN NULL ELSE left_item END,
                   right_item   = CASE WHEN right_item = CAST(:iid AS uuid) THEN NULL ELSE right_item END
             WHERE actor_id = :aid
            """
        ),
        {"iid": item_id, "aid": actor_id},
    )
    await session.commit()
    return {"ok": True, "title": row["title"]}


async def unequip_backpack_db(session: AsyncSession, actor_id: str):
    """
    Снять рюкзак: перенести его из equipped_bag обратно в массив backpack (uuid[]).
    Используем array_append(..., CAST(:iid AS uuid)), а не '|| :iid'.
    """
    inv = (
        await session.execute(
            text("""SELECT equipped_bag FROM inventories WHERE actor_id=:aid"""),
            {"aid": actor_id},
        )
    ).mappings().first()
    if not inv or not inv["equipped_bag"]:
        return {"ok": False, "error": "no_backpack"}

    item_id = inv["equipped_bag"]

    await session.execute(
        text(
            """
            UPDATE inventories
               SET equipped_bag = NULL,
                   backpack     = array_append(coalesce(backpack,'{}'::uuid[]), CAST(:iid AS uuid))
             WHERE actor_id = :aid
            """
        ),
        {"iid": item_id, "aid": actor_id},
    )
    await session.commit()
    return {"ok": True, "item_id": str(item_id)}


async def hold_bag_db(session: AsyncSession, actor_id: str, item_id: str, hand: str = "left"):
    """
    Взять мешок в руку (если она свободна). Проверяем, что это контейнер с hands_required=1.
    """
    row = (
        await session.execute(
            text(
                """
                SELECT i.id, i.kind_id, k.grid_w, k.grid_h, k.hands_required, k.title
                  FROM items i
                  JOIN item_kinds k ON k.id = i.kind_id
                 WHERE i.id = :iid
                """
            ),
            {"iid": item_id},
        )
    ).mappings().first()
    if not row:
        return {"ok": False, "error": "item_not_found"}

    if (not row["grid_w"]) or row["hands_required"] != 1:
        return {"ok": False, "error": "not_a_handheld_bag"}

    inv = (
        await session.execute(
            text("""SELECT left_item, right_item, backpack FROM inventories WHERE actor_id=:aid"""),
            {"aid": actor_id},
        )
    ).mappings().first()
    if not inv:
        return {"ok": False, "error": "no_inventory"}

    current = inv[f"{hand}_item"]
    if current:
        return {"ok": False, "error": "hand_occupied"}

    # Перемещаем из массива backpack в руку
    await session.execute(
        text(
            f"""
            UPDATE inventories
               SET {hand}_item = CAST(:iid AS uuid),
                   backpack    = array_remove(coalesce(backpack,'{{}}'::uuid[]), CAST(:iid AS uuid))
             WHERE actor_id=:aid
            """
        ),
        {"iid": item_id, "aid": actor_id},
    )
    await session.commit()
    return {"ok": True, "title": row["title"], "hand": hand}


# ===================== UNIVERSAL TRANSFER (no grid) =====================
async def transfer_item_db(
    session: AsyncSession,
    actor_id: str,
    source: str,
    target: str,
    item_id: Optional[str] = None,
):
    """
    Перемещает один предмет между: left/right/hidden/backpack (без работы с grid).
    Если item_id не указан:
      - при source in {left,right,hidden} берём текущий предмет оттуда,
      - при source=backpack вернём ошибку (нужен item_id).
    """
    if source == target:
        return {"ok": False, "error": "same_place"}

    # защищённая ячейка: из hidden можно вынести только отдельной операцией drop_hidden_to_ground_db
    if source == "hidden":
        return {"ok": False, "error": "hidden_protected"}

    # заберем текущие значения
    inv = (
        await session.execute(
            text(
                """SELECT left_item, right_item, hidden_slot, backpack
                   FROM inventories WHERE actor_id=:aid"""
            ),
            {"aid": actor_id},
        )
    ).mappings().first()
    if not inv:
        return {"ok": False, "error": "no_inventory"}

    def _get_from_place(place: str) -> Optional[str]:
        if place == "left":
            return inv["left_item"]
        if place == "right":
            return inv["right_item"]
        if place == "hidden":
            return inv["hidden_slot"]
        return None

    # 1) Определяем item_id
    if source in ("left", "right", "hidden"):
        item_id = item_id or _get_from_place(source)
        if not item_id:
            return {"ok": False, "error": "source_empty"}
    elif source == "backpack":
        if not item_id:
            return {"ok": False, "error": "item_id_required"}
        # убедимся, что он в backpack
        in_bp = (
            await session.execute(
                text(
                    """
                    SELECT CAST(:iid AS uuid) = ANY(coalesce(backpack,'{}'::uuid[])) AS ok
                      FROM inventories WHERE actor_id=:aid
                    """
                ),
                {"iid": item_id, "aid": actor_id},
            )
        ).scalar()
        if not in_bp:
            return {"ok": False, "error": "not_in_backpack"}
    else:
        return {"ok": False, "error": "bad_source"}

    # 2) Проверка целевого места
    if target in ("left", "right"):
        # рука должна быть свободна
        cur = inv[f"{target}_item"]
        if cur:
            return {"ok": False, "error": "hand_occupied"}
        # если предмет двуручный — обе руки должны быть свободны
        hd = await _handedness(session, item_id)
        if hd == "two_hands":
            if inv["left_item"] or inv["right_item"]:
                return {"ok": False, "error": "need_both_hands_free"}

    if target == "hidden":
        if inv["hidden_slot"]:
            return {"ok": False, "error": "hidden_busy"}

    if target not in ("left", "right", "hidden", "backpack"):
        return {"ok": False, "error": "bad_target"}

    # 3) Удаляем из source
    if source == "left":
        await session.execute(
            text("""UPDATE inventories SET left_item = NULL WHERE actor_id=:aid"""),
            {"aid": actor_id},
        )
    elif source == "right":
        await session.execute(
            text("""UPDATE inventories SET right_item = NULL WHERE actor_id=:aid"""),
            {"aid": actor_id},
        )
    elif source == "hidden":
        # сюда не дойдём из-за защиты; оставлено для полноты.
        await session.execute(
            text("""UPDATE inventories SET hidden_slot = NULL WHERE actor_id=:aid"""),
            {"aid": actor_id},
        )
    elif source == "backpack":
        await session.execute(
            text(
                """UPDATE inventories
                       SET backpack = array_remove(coalesce(backpack,'{}'::uuid[]), CAST(:iid AS uuid))
                     WHERE actor_id=:aid"""
            ),
            {"aid": actor_id, "iid": item_id},
        )

    # 4) Кладём в target
    if target == "left":
        # если двуручный — занимаем обе руки
        if await _handedness(session, item_id) == "two_hands":
            await session.execute(
                text(
                    """
                UPDATE inventories
                   SET left_item = CAST(:iid AS uuid),
                       right_item = CAST(:iid AS uuid)
                 WHERE actor_id=:aid
                """
                ),
                {"aid": actor_id, "iid": item_id},
            )
        else:
            await session.execute(
                text(
                    """
                UPDATE inventories SET left_item = CAST(:iid AS uuid)
                 WHERE actor_id=:aid
                """
                ),
                {"aid": actor_id, "iid": item_id},
            )

    elif target == "right":
        if await _handedness(session, item_id) == "two_hands":
            await session.execute(
                text(
                    """
                UPDATE inventories
                   SET left_item = CAST(:iid AS uuid),
                       right_item = CAST(:iid AS uuid)
                 WHERE actor_id=:aid
                """
                ),
                {"aid": actor_id, "iid": item_id},
            )
        else:
            await session.execute(
                text(
                    """
                UPDATE inventories SET right_item = CAST(:iid AS uuid)
                 WHERE actor_id=:aid
                """
                ),
                {"aid": actor_id, "iid": item_id},
            )

    elif target == "hidden":
        await session.execute(
            text(
                """
            UPDATE inventories SET hidden_slot = CAST(:iid AS uuid)
             WHERE actor_id=:aid
            """
            ),
            {"aid": actor_id, "iid": item_id},
        )

    elif target == "backpack":
        await session.execute(
            text(
                """
            UPDATE inventories
               SET backpack = array_append(coalesce(backpack,'{}'::uuid[]), CAST(:iid AS uuid))
             WHERE actor_id=:aid
            """
            ),
            {"aid": actor_id, "iid": item_id},
        )

    await session.commit()
    return {"ok": True, "moved": str(item_id), "from": source, "to": target}


# ===================== GRID PUT/TAKE (equipped bag or hand-held sack) =====================
async def _owns_container(session: AsyncSession, actor_id: str, container_item_id: str) -> Tuple[bool, str]:
    """Проверяем, что контейнер принадлежит актёру: либо надет (equipped_bag), либо в руке left/right."""
    row = (
        await session.execute(
            text(
                """
        SELECT equipped_bag, left_item, right_item
          FROM inventories WHERE actor_id=:aid
    """
            ),
            {"aid": actor_id},
        )
    ).mappings().first()
    if not row:
        return False, "no_inventory"
    cid = str(container_item_id)
    if row["equipped_bag"] and str(row["equipped_bag"]) == cid:
        return True, "equipped"
    if row["left_item"] and str(row["left_item"]) == cid:
        return True, "left"
    if row["right_item"] and str(row["right_item"]) == cid:
        return True, "right"
    return False, "not_owner"


async def _is_container(session: AsyncSession, item_id: str) -> bool:
    r = (
        await session.execute(
            text(
                """
        SELECT COALESCE(k.grid_w,0) AS gw, COALESCE(k.grid_h,0) AS gh
          FROM items i JOIN item_kinds k ON k.id = i.kind_id
         WHERE i.id = :iid
    """
            ),
            {"iid": item_id},
        )
    ).mappings().first()
    if not r:
        return False
    return int(r["gw"]) > 0 and int(r["gh"]) > 0


async def grid_put_item_db(
    session: AsyncSession,
    actor_id: str,
    container_item_id: str,
    slot_x: int,
    slot_y: int,
    source_place: str,  # 'left'|'right'|'hidden'|'backpack'
    item_id: str,
):
    # запрет контейнер-в-контейнер (пока)
    if await _is_container(session, item_id):
        return {"ok": False, "error": "container_in_container_forbidden"}

    # нельзя класть предмет в самого себя
    if str(container_item_id) == str(item_id):
        return {"ok": False, "error": "self_reference"}

    # защищённая ячейка: из hidden нельзя класть в контейнер слота
    if source_place == "hidden":
        return {"ok": False, "error": "hidden_protected"}

    # контейнер должен принадлежать актёру
    ok, why = await _owns_container(session, actor_id, container_item_id)
    if not ok:
        return {"ok": False, "error": why}

    # контейнер реально имеет grid?
    cont = await _brief_item(session, container_item_id)
    gw, gh = int(cont.get("grid_w") or 0), int(cont.get("grid_h") or 0)
    if gw <= 0 or gh <= 0:
        return {"ok": False, "error": "not_a_container"}
    if not (0 <= slot_x < gw and 0 <= slot_y < gh):
        return {"ok": False, "error": "out_of_bounds"}

    # слот свободен?
    exists = (
        await session.execute(
            text(
                """
        SELECT 1 FROM carried_container_slots
         WHERE container_item_id = :cid AND slot_x=:x AND slot_y=:y
         LIMIT 1
    """
            ),
            {"cid": container_item_id, "x": slot_x, "y": slot_y},
        )
    ).scalar()
    if exists:
        return {"ok": False, "error": "slot_busy"}

    # предмет действительно у игрока в source_place?
    if source_place == "backpack":
        in_src = (
            await session.execute(
                text(
                    """
            SELECT CAST(:iid AS uuid) = ANY(coalesce(backpack,'{}'::uuid[])) AS ok
              FROM inventories WHERE actor_id=:aid
        """
                ),
                {"iid": item_id, "aid": actor_id},
            )
        ).scalar()
    elif source_place in ("left", "right", "hidden"):
        col = "left_item" if source_place == "left" else "right_item" if source_place == "right" else "hidden_slot"
        in_src = (
            await session.execute(
                text(
                    f"""
            SELECT ({col} = CAST(:iid AS uuid)) AS ok
              FROM inventories WHERE actor_id=:aid
        """
                ),
                {"iid": item_id, "aid": actor_id},
            )
        ).scalar()
    else:
        return {"ok": False, "error": "bad_source"}

    if not in_src:
        return {"ok": False, "error": "item_not_in_source"}

    # 1) удаляем из source_place
    if source_place == "backpack":
        await session.execute(
            text(
                """
            UPDATE inventories
               SET backpack = array_remove(coalesce(backpack,'{}'::uuid[]), CAST(:iid AS uuid))
             WHERE actor_id=:aid
        """
            ),
            {"iid": item_id, "aid": actor_id},
        )
    elif source_place == "left":
        await session.execute(text("""UPDATE inventories SET left_item=NULL WHERE actor_id=:aid"""), {"aid": actor_id})
    elif source_place == "right":
        await session.execute(text("""UPDATE inventories SET right_item=NULL WHERE actor_id=:aid"""), {"aid": actor_id})
    elif source_place == "hidden":
        # сюда не дойдём (protected), оставлено для симметрии
        await session.execute(text("""UPDATE inventories SET hidden_slot=NULL WHERE actor_id=:aid"""), {"aid": actor_id})

    # 2) кладём в слот
    await session.execute(
        text(
            """
        INSERT INTO carried_container_slots(container_item_id, slot_x, slot_y, item_id)
        VALUES (CAST(:cid AS uuid), :x, :y, CAST(:iid AS uuid))
        ON CONFLICT (container_item_id, slot_x, slot_y) DO NOTHING
    """
        ),
        {"cid": container_item_id, "x": slot_x, "y": slot_y, "iid": item_id},
    )

    await session.commit()
    return {"ok": True}


async def grid_take_item_db(
    session: AsyncSession,
    actor_id: str,
    container_item_id: str,
    slot_x: int,
    slot_y: int,
    target_place: str,  # 'left'|'right'|'hidden'|'backpack'
):
    ok, why = await _owns_container(session, actor_id, container_item_id)
    if not ok:
        return {"ok": False, "error": why}

    # берём предмет из слота
    row = (
        await session.execute(
            text(
                """
        SELECT item_id
          FROM carried_container_slots
         WHERE container_item_id=:cid AND slot_x=:x AND slot_y=:y
    """
            ),
            {"cid": container_item_id, "x": slot_x, "y": slot_y},
        )
    ).mappings().first()
    if not row or not row["item_id"]:
        return {"ok": False, "error": "slot_empty"}

    iid = row["item_id"]

    # проверка таргета
    inv = (
        await session.execute(
            text("""SELECT left_item, right_item, hidden_slot FROM inventories WHERE actor_id=:aid"""),
            {"aid": actor_id},
        )
    ).mappings().first()

    if target_place in ("left", "right"):
        if inv[f"{target_place}_item"]:
            return {"ok": False, "error": "hand_occupied"}
        # двуручный нельзя класть в одну руку
        if await _handedness(session, iid) == "two_hands":
            if inv["left_item"] or inv["right_item"]:
                return {"ok": False, "error": "need_both_hands_free"}

    if target_place == "hidden" and inv["hidden_slot"]:
        return {"ok": False, "error": "hidden_busy"}

    if target_place not in ("left", "right", "hidden", "backpack"):
        return {"ok": False, "error": "bad_target"}

    # 1) очищаем слот
    await session.execute(
        text(
            """
        DELETE FROM carried_container_slots
         WHERE container_item_id=:cid AND slot_x=:x AND slot_y=:y
    """
        ),
        {"cid": container_item_id, "x": slot_x, "y": slot_y},
    )

    # 2) кладём в target
    if target_place == "left":
        # если двуручный — занимаем обе руки
        if await _handedness(session, iid) == "two_hands":
            await session.execute(
                text(
                    """
                UPDATE inventories SET left_item=CAST(:iid AS uuid), right_item=CAST(:iid AS uuid)
                 WHERE actor_id=:aid
            """
                ),
                {"iid": iid, "aid": actor_id},
            )
        else:
            await session.execute(
                text(
                    """
                UPDATE inventories SET left_item=CAST(:iid AS uuid)
                 WHERE actor_id=:aid
            """
                ),
                {"iid": iid, "aid": actor_id},
            )
    elif target_place == "right":
        if await _handedness(session, iid) == "two_hands":
            await session.execute(
                text(
                    """
                UPDATE inventories SET left_item=CAST(:iid AS uuid), right_item=CAST(:iid AS uuid)
                 WHERE actor_id=:aid
            """
                ),
                {"iid": iid, "aid": actor_id},
            )
        else:
            await session.execute(
                text(
                    """
                UPDATE inventories SET right_item=CAST(:iid AS uuid)
                 WHERE actor_id=:aid
            """
                ),
                {"iid": iid, "aid": actor_id},
            )
    elif target_place == "hidden":
        await session.execute(
            text(
                """
            UPDATE inventories SET hidden_slot=CAST(:iid AS uuid)
             WHERE actor_id=:aid
        """
            ),
            {"iid": iid, "aid": actor_id},
        )
    elif target_place == "backpack":
        await session.execute(
            text(
                """
            UPDATE inventories SET backpack = array_append(coalesce(backpack,'{}'::uuid[]), CAST(:iid AS uuid))
             WHERE actor_id=:aid
        """
            ),
            {"iid": iid, "aid": actor_id},
        )

    await session.commit()
    return {"ok": True, "moved": str(iid)}


# ===================== NEAREST FREE CELL FOR DROP =====================
async def _is_cell_free(session: AsyncSession, node_id: str, x: int, y: int, layer: int = 3) -> bool:
    row = (
        await session.execute(
            text(
                """select 1 from node_objects where node_id=:nid and x=:x and y=:y and layer=:layer limit 1"""
            ),
            {"nid": node_id, "x": x, "y": y, "layer": layer},
        )
    ).first()
    return row is None


async def _find_nearest_free_cell(
    session: AsyncSession, node_id: str, x: int, y: int, layer: int = 3, max_radius: int = 5
) -> Optional[Tuple[int, int]]:
    # сначала пробуем там же
    if await _is_cell_free(session, node_id, x, y, layer):
        return x, y
    # по кольцам вокруг
    for r in range(1, max_radius + 1):
        # верх/низ
        for cx in range(x - r, x + r + 1):
            for cy in (y - r, y + r):
                if await _is_cell_free(session, node_id, cx, cy, layer):
                    return cx, cy
        # боковые
        for cy in range(y - r + 1, y + r):
            for cx in (x - r, x + r):
                if await _is_cell_free(session, node_id, cx, cy, layer):
                    return cx, cy
    return None


# ===================== HIDDEN & GENERIC DROP TO GROUND =====================
# --- helper: выбираем asset_id для "лежит на полу"
async def _drop_asset_id(session: AsyncSession, item_id: str) -> str:
    """
    Возвращает asset_id для лут-объекта на полу на основе kind.props.ui или kind_id.
    Если в props.ui есть строка (например 'sack'|'backpack'), вернём 'drop_<ui>'.
    Иначе вернём 'dropped_loot' по умолчанию.
    """
    row = (
        await session.execute(
            text(
                """
        SELECT k.id AS kind_id, k.props
          FROM items i
          JOIN item_kinds k ON k.id = i.kind_id
         WHERE i.id = :iid
    """
            ),
            {"iid": item_id},
        )
    ).mappings().first()
    if not row:
        return "dropped_loot"
    props = row.get("props") or {}
    ui = None
    if isinstance(props, dict):
        ui = props.get("ui")
    if isinstance(ui, str) and ui:
        return f"drop_{ui}"
    # fallback по виду предмета
    kid = (row.get("kind_id") or "").lower()
    if "sack" in kid or "bag" in kid or "backpack" in kid:
        return "drop_bag"
    return "dropped_loot"


async def drop_to_ground_db(
    session: AsyncSession,
    actor_id: str,
    source: str,         # 'left'|'right'|'hidden'|'backpack'|'equipped_bag'
    item_id: Optional[str] = None
):
    """
    Универсальный дроп из указанного источника на клетку актёра (слой L3).
    Для source='backpack' item_id обязателен и должен быть в массиве.
    Для остальных источников item_id можно опустить — возьмём текущий.
    Контейнеры (мешок/рюкзак) падают НА ПОЛ СО СВОИМ СОДЕРЖИМЫМ (слоты не чистим).
    """
    # 0) инвентарь и позиция
    inv = (
        await session.execute(
            text(
                """
        SELECT left_item, right_item, hidden_slot, equipped_bag, backpack
          FROM inventories WHERE actor_id=:aid
    """
            ),
            {"aid": actor_id},
        )
    ).mappings().first()
    if not inv:
        return {"ok": False, "error": "no_inventory"}

    pos = (
        await session.execute(
            text(
                """
        SELECT node_id, COALESCE(x,0) AS x, COALESCE(y,0) AS y
          FROM actors WHERE id=:aid
    """
            ),
            {"aid": actor_id},
        )
    ).mappings().first()
    if not pos or not pos["node_id"]:
        return {"ok": False, "error": "no_actor_position"}

    node_id, x, y = pos["node_id"], int(pos["x"]), int(pos["y"])

    # 1) определяем item_id и валидируем источник
    src = source
    if src not in ("left", "right", "hidden", "backpack", "equipped_bag"):
        return {"ok": False, "error": "bad_source"}

    if src == "left":
        item_id = item_id or inv["left_item"]
    elif src == "right":
        item_id = item_id or inv["right_item"]
    elif src == "hidden":
        item_id = item_id or inv["hidden_slot"]
    elif src == "equipped_bag":
        item_id = item_id or inv["equipped_bag"]
    elif src == "backpack":
        if not item_id:
            return {"ok": False, "error": "item_id_required"}
        in_bp = (
            await session.execute(
                text(
                    """
            SELECT CAST(:iid AS uuid) = ANY(coalesce(backpack,'{}'::uuid[])) AS ok
              FROM inventories WHERE actor_id=:aid
        """
                ),
                {"iid": item_id, "aid": actor_id},
            )
        ).scalar()
        if not in_bp:
            return {"ok": False, "error": "not_in_backpack"}

    if not item_id:
        return {"ok": False, "error": "source_empty"}

    # 2) освобождаем источник
    if src == "left":
        await session.execute(text("""UPDATE inventories SET left_item=NULL WHERE actor_id=:aid"""), {"aid": actor_id})
    elif src == "right":
        await session.execute(text("""UPDATE inventories SET right_item=NULL WHERE actor_id=:aid"""), {"aid": actor_id})
    elif src == "hidden":
        # единственный разрешённый способ вынести из защищённой hidden
        await session.execute(text("""UPDATE inventories SET hidden_slot=NULL WHERE actor_id=:aid"""), {"aid": actor_id})
    elif src == "equipped_bag":
        await session.execute(text("""UPDATE inventories SET equipped_bag=NULL WHERE actor_id=:aid"""), {"aid": actor_id})
    elif src == "backpack":
        await session.execute(
            text(
                """
            UPDATE inventories
               SET backpack = array_remove(coalesce(backpack,'{}'::uuid[]), CAST(:iid AS uuid))
             WHERE actor_id=:aid
        """
            ),
            {"aid": actor_id, "iid": item_id},
        )

    # 3) ищем ближайшую свободную клетку на слое L3
    pos_free = await _find_nearest_free_cell(session, node_id, x, y, layer=3, max_radius=5)
    if not pos_free:
        await session.rollback()
        return {"ok": False, "error": "no_free_cell_nearby"}
    drop_x, drop_y = pos_free

    # 4) создаём лут-ассет
    asset_id = await _drop_asset_id(session, item_id)
    obj = (
        await session.execute(
            text(
                """
        INSERT INTO node_objects(node_id, asset_id, x, y, rotation, layer, props)
        VALUES (:nid, :asset, :x, :y, 0, 3, '{"state":"open"}'::jsonb)
        RETURNING id
    """
            ),
            {"nid": node_id, "asset": asset_id, "x": drop_x, "y": drop_y},
        )
    ).mappings().first()
    object_id = obj["id"]

    # 5) создаём/обновляем инвентарь объекта: кладём предмет внутрь
    await session.execute(
        text(
            """
        INSERT INTO object_inventories(object_id, items)
        VALUES (:oid, ARRAY[CAST(:iid AS uuid)])
        ON CONFLICT (object_id) DO UPDATE
          SET items = object_inventories.items || ARRAY[CAST(:iid AS uuid)]
    """
        ),
        {"oid": object_id, "iid": item_id},
    )

    await session.commit()
    return {
        "ok": True,
        "object_id": object_id,
        "dropped": str(item_id),
        "node_id": node_id,
        "x": drop_x,
        "y": drop_y,
    }


async def drop_hidden_to_ground_db(session: AsyncSession, actor_id: str):
    """
    Выбросить предмет из защищённой ячейки hidden_slot на землю.
    Делает маленький лут-объект (layer=3) и кладёт туда предмет через object_inventories.
    """
    # 1) есть ли предмет в hidden?
    inv = (
        await session.execute(
            text("""SELECT hidden_slot FROM inventories WHERE actor_id=:aid"""),
            {"aid": actor_id},
        )
    ).mappings().first()
    if not inv or not inv["hidden_slot"]:
        return {"ok": False, "error": "hidden_empty"}

    item_id = inv["hidden_slot"]

    # 2) позиция актёра
    pos = (
        await session.execute(
            text(
                """
        SELECT node_id, COALESCE(x,0) AS x, COALESCE(y,0) AS y
          FROM actors WHERE id=:aid
    """
            ),
            {"aid": actor_id},
        )
    ).mappings().first()

    if not pos or not pos["node_id"]:
        return {"ok": False, "error": "no_actor_position"}

    node_id, x, y = pos["node_id"], int(pos["x"]), int(pos["y"])

    # 3) найдём ближайшую свободную L3 клетку
    pos_free = await _find_nearest_free_cell(session, node_id, x, y, layer=3, max_radius=5)
    if not pos_free:
        return {"ok": False, "error": "no_free_cell_nearby"}
    drop_x, drop_y = pos_free

    # 4) создаём объект лута (layer=3, открытый)
    obj = (
        await session.execute(
            text(
                """
        INSERT INTO node_objects(node_id, asset_id, x, y, rotation, layer, props)
        VALUES (:nid, :asset, :x, :y, 0, 3, '{"state":"open"}'::jsonb)
        RETURNING id
    """
            ),
            {"nid": node_id, "asset": "dropped_loot", "x": drop_x, "y": drop_y},
        )
    ).mappings().first()
    obj_id = obj["id"]

    # 5) создаём инвентарь объекта и положим туда предмет
    await session.execute(
        text(
            """
        INSERT INTO object_inventories(object_id, items)
        VALUES (:oid, ARRAY[CAST(:iid AS uuid)])
        ON CONFLICT (object_id) DO UPDATE
          SET items = object_inventories.items || ARRAY[CAST(:iid AS uuid)]
    """
        ),
        {"oid": obj_id, "iid": item_id},
    )

    # 6) очищаем hidden_slot
    await session.execute(text("""UPDATE inventories SET hidden_slot = NULL WHERE actor_id=:aid"""), {"aid": actor_id})

    await session.commit()
    return {"ok": True, "object_id": obj_id, "dropped": str(item_id), "node_id": node_id, "x": drop_x, "y": drop_y}
# ===================== AMMO / CONSUMABLES (DAO) =====================

# ВНИМАНИЕ: предполагается, что миграции уже добавили поля:
#   item_kinds.ammo_type, item_kinds.max_charges, item_kinds.range_cells, item_kinds.use_effect
#   items.charges (у тебя есть)
# И есть справочник ammo_types(id, ...). Если FK не хочешь — можно без неё.

async def _get_item_with_kind(session: AsyncSession, item_id: str):
    """Тянем предмет + поля его kind, нужные для логики зарядов/расходников."""
    row = (
        await session.execute(
            text("""
                SELECT i.id, i.kind_id, i.charges, i.durability,
                       k.title, k.tags, k.handedness, k.props,
                       k.ammo_type, k.max_charges, k.range_cells, k.use_effect
                  FROM items i
                  JOIN item_kinds k ON k.id = i.kind_id
                 WHERE i.id = :iid
            """),
            {"iid": item_id},
        )
    ).mappings().first()
    return dict(row) if row else None


async def _delete_item_everywhere(session: AsyncSession, item_id: str):
    """
    Полное удаление предмета с очисткой всех ссылок.
    Используется при выработке расходника или расходе патронов-предметов.
    """
    # очистка из инвентарей актёров
    await session.execute(text("""
        UPDATE inventories
           SET left_item    = CASE WHEN left_item    = CAST(:iid AS uuid) THEN NULL ELSE left_item END,
               right_item   = CASE WHEN right_item   = CAST(:iid AS uuid) THEN NULL ELSE right_item END,
               hidden_slot  = CASE WHEN hidden_slot  = CAST(:iid AS uuid) THEN NULL ELSE hidden_slot END,
               equipped_bag = CASE WHEN equipped_bag = CAST(:iid AS uuid) THEN NULL ELSE equipped_bag END,
               backpack     = array_remove(coalesce(backpack,'{}'::uuid[]), CAST(:iid AS uuid))
    """), {"iid": item_id})

    # очистка из переносимых контейнеров
    await session.execute(text("""
        DELETE FROM carried_container_slots WHERE item_id = CAST(:iid AS uuid)
    """), {"iid": item_id})

    # очистка из контейнеров на земле
    await session.execute(text("""
        UPDATE object_inventories
           SET items = array_remove(items, CAST(:iid AS uuid))
    """), {"iid": item_id})

    # сам предмет
    await session.execute(text("DELETE FROM items WHERE id = :iid"), {"iid": item_id})


async def consume_charge_db(session: AsyncSession, item_id: str, amount: int = 1):
    """
    Списывает charges у предмета. Возвращает {"ok", "left"}.
    Если charges == NULL -> предмет не имеет счётчика (ничего не делаем).
    Если не хватает — вернёт {"ok": False, "error": "empty"}.
    """
    row = await _get_item_with_kind(session, item_id)
    if not row:
        return {"ok": False, "error": "item_not_found"}

    charges = row.get("charges")
    if charges is None:
        # нет счётчика — например, меч
        return {"ok": True, "left": None}

    if int(charges or 0) < amount:
        return {"ok": False, "error": "empty", "left": int(charges or 0)}

    new_row = (
        await session.execute(
            text("""UPDATE items SET charges = charges - :a WHERE id=:iid RETURNING charges"""),
            {"a": amount, "iid": item_id},
        )
    ).mappings().first()
    left = int(new_row["charges"] if new_row and new_row["charges"] is not None else 0)
    return {"ok": True, "left": left}


async def reload_weapon_db(session: AsyncSession, actor_id: str, weapon_item_id: str):
    """
    Перезаряжает оружие из рюкзака патронами нужного типа.
    - Оружие: item_kinds.ammo_type = 'small'|'gas'|..., max_charges > 0
    - В магазин оружия записывается items.charges
    - Патроны — это отдельные items, у которых kind.ammo_type совпадает, их items.charges расходуем.
      При нуле — предмет патронов удаляется.
    Возвращает dict с событием RELOAD (для WS), либо ошибку.
    """
    w = await _get_item_with_kind(session, weapon_item_id)
    if not w:
        return {"ok": False, "error": "weapon_not_found"}

    cap = int(w.get("max_charges") or 0)
    if cap <= 0:
        return {"ok": False, "error": "not_reloadable"}

    ammo_type = w.get("ammo_type")
    if not ammo_type:
        # Оружие без внешних патронов (внутренние заряды) — перезарядка не через боеприпасы
        return {"ok": False, "error": "no_external_ammo"}

    cur = int(w.get("charges") or 0)
    if cur >= cap:
        return {"ok": False, "error": "already_full", "left": cur}

    # Забираем список id из рюкзака
    inv = (
        await session.execute(
            text("""SELECT backpack FROM inventories WHERE actor_id=:aid"""),
            {"aid": actor_id},
        )
    ).mappings().first()
    backpack_ids = [str(x) for x in (inv and inv["backpack"] or [])]
    if not backpack_ids:
        return {"ok": False, "error": "no_ammo_in_backpack"}

    # Подтянем предметы из рюкзака
    stmt = text("""
        SELECT i.id, i.charges, k.ammo_type, k.title
          FROM items i JOIN item_kinds k ON k.id = i.kind_id
         WHERE i.id = ANY(:ids)
    """).bindparams(bindparam("ids", value=backpack_ids, type_=ARRAY(UUID(as_uuid=True))))
    rows = (await session.execute(stmt)).mappings().all()

    need = cap - cur
    loaded = 0
    used_list = []

    for r in rows:
        if r["ammo_type"] != ammo_type:
            continue
        ammo_left = int(r.get("charges") or 0)
        if ammo_left <= 0 or need <= 0:
            continue

        take = min(ammo_left, need)

        # Списываем у пачки патронов
        new_left = (
            await session.execute(
                text("""UPDATE items SET charges = charges - :t WHERE id=:iid RETURNING charges"""),
                {"t": take, "iid": r["id"]},
            )
        ).mappings().first()["charges"]

        # Если пачка опустела — удаляем предмет полностью
        if int(new_left or 0) <= 0:
            await _delete_item_everywhere(session, str(r["id"]))

        used_list.append({"ammo_item_id": str(r["id"]), "taken": int(take)})
        loaded += take
        need -= take
        if need <= 0:
            break

    if loaded == 0:
        return {"ok": False, "error": "no_usable_ammo"}

    # Кладём в магазин оружия
    new_weapon_charges = (
        await session.execute(
            text("""UPDATE items SET charges = COALESCE(charges,0) + :add WHERE id=:iid RETURNING charges"""),
            {"add": loaded, "iid": weapon_item_id},
        )
    ).mappings().first()["charges"]

    await session.commit()
    return {
        "ok": True,
        "loaded": int(loaded),
        "weapon_charges": int(new_weapon_charges or 0),
        "used": used_list,
        "event": {
            "type": "RELOAD",
            "payload": {"item_id": str(weapon_item_id), "delta": int(loaded), "left": int(new_weapon_charges or 0)}
        },
    }


async def use_consumable_db(session: AsyncSession, actor_id: str, item_id: str):
    """
    Использование расходника/аптечки/еды.
    Логика:
      - читаем item_kinds.use_effect;
      - применяем простой эффект (например, HEAL_50);
      - если max_charges > 0 — списываем 1 charge, при 0 удаляем;
        иначе (одноразовый) — удаляем сразу.
    Возвращает {"ok":True, "events":[...]}.
    """
        # просто делегируем универсальной функции
    return await use_item_db(session, actor_id, item_id)

# универсальная функция использования предмета
async def use_item_db(session: AsyncSession, actor_id: str, item_id: str, target_id: str | None = None):
    """
    Универсальное использование предмета.
    Если предмет имеет use_effect — применяет его.
    Если charges > 0 — тратит 1 заряд.
    Если charges <= 0 — удаляет предмет.
    """
    from sqlalchemy import text

    # достаём предмет и его kind
    q = await session.execute(text("""
        SELECT i.id, i.charges, k.title, k.use_effect
        FROM items i
        JOIN item_kinds k ON i.kind_id = k.id
        WHERE i.id = :iid
    """), {"iid": item_id})
    item = q.mappings().first()
    if not item:
        return [{"type": "TEXT", "payload": {"text": "Предмет не найден."}}]

    events = []
    use_effect = item["use_effect"] or ""

    # --- обработка эффектов ---
    if use_effect.startswith("HEAL_"):
        heal_amount = int(use_effect.split("_")[1])
        await session.execute(text("""
            UPDATE actors SET hp = LEAST(hp + :heal, 100) WHERE id = :aid
        """), {"aid": actor_id, "heal": heal_amount})
        events.append({"type": "ITEM_USE", "payload": {"effect": "heal", "amount": heal_amount}})

    elif use_effect.startswith("BURN_"):
        dmg = int(use_effect.split("_")[1])
        target = target_id or actor_id
        await session.execute(text("""
            UPDATE actors SET hp = GREATEST(hp - :dmg, 0) WHERE id = :tid
        """), {"tid": target, "dmg": dmg})
        events.append({"type": "ITEM_USE", "payload": {"effect": "burn", "amount": dmg}})

    elif use_effect:
        events.append({"type": "ITEM_USE", "payload": {"effect": use_effect}})
    else:
        events.append({"type": "TEXT", "payload": {"text": "Ничего не произошло."}})

    # --- расход зарядов ---
    if item["charges"] is not None:
        if item["charges"] > 1:
            await session.execute(text("UPDATE items SET charges = charges - 1 WHERE id = :iid"), {"iid": item_id})
            events.append({"type": "CONSUME", "payload": {"item": item["title"], "delta": -1, "left": item["charges"] - 1}})
        else:
            await session.execute(text("DELETE FROM items WHERE id = :iid"), {"iid": item_id})
            events.append({"type": "ITEM_DESTROYED", "payload": {"item": item["title"]}})

    await session.commit()
    return events


    it = await _get_item_with_kind(session, item_id)
    if not it:
        return {"ok": False, "error": "item_not_found"}

    effect = it.get("use_effect")
    if not effect:
        return {"ok": False, "error": "not_consumable"}

    events: List[Dict[str, Any]] = []

    # Примитивные эффекты на старте главы: HEAL_XX
    if effect.startswith("HEAL_"):
        try:
            heal = int(effect.split("_")[1])
        except Exception:
            heal = 0
        if heal > 0:
            row = (
                await session.execute(
                    text("""UPDATE actors SET hp = LEAST(100, COALESCE(hp,0) + :h) WHERE id=:aid RETURNING hp"""),
                    {"h": heal, "aid": actor_id},
                )
            ).mappings().first()
            events.append({"type": "ITEM_USE", "payload": {"actor_id": actor_id, "item_id": str(item_id), "effect": effect, "hp": int(row["hp"])}})

    # Списание/удаление
    max_ch = int(it.get("max_charges") or 0)
    if max_ch > 0:
        # многоразовый расходник
        res = await consume_charge_db(session, item_id, 1)
        if not res["ok"]:
            return {"ok": False, "error": "empty"}
        left = int(res.get("left") or 0)
        events.append({"type": "CONSUME", "payload": {"item_id": str(item_id), "delta": -1, "left": left}})
        if left <= 0:
            await _delete_item_everywhere(session, item_id)
    else:
        # одноразовый
        await _delete_item_everywhere(session, item_id)

    await session.commit()
    return {"ok": True, "events": events}


async def spend_shot_if_needed(session: AsyncSession, weapon_item_id: str):
    """
    Хелпер для /intent ATTACK:
      - ближнее оружие (нет ammo_type и нет max_charges) -> не тратим, ok=True
      - оружие с внутренними/внешними зарядами: списываем 1 из items.charges
      - при нуле -> ok=False, error='empty', событие AMMO_EMPTY
    Возвращает dict с полями ok, event (если было списание/пусто).
    """
    w = await _get_item_with_kind(session, weapon_item_id)
    if not w:
        return {"ok": False, "error": "weapon_not_found"}

    if not w.get("max_charges") and not w.get("ammo_type"):
        # ближний бой — ничего не списываем
        return {"ok": True, "melee": True}

    cur = int(w.get("charges") or 0)
    if cur <= 0:
        return {"ok": False, "error": "empty",
                "event": {"type": "AMMO_EMPTY", "payload": {"item_id": str(weapon_item_id)}}}

    res = await consume_charge_db(session, weapon_item_id, 1)
    if not res["ok"]:
        return {"ok": False, "error": "empty",
                "event": {"type": "AMMO_EMPTY", "payload": {"item_id": str(weapon_item_id)}}}

    left = int(res.get("left") or 0)
    return {"ok": True, "event": {"type": "CONSUME", "payload": {"item_id": str(weapon_item_id), "delta": -1, "left": left}}}

# ===================== COMBAT GEOMETRY (LoS, distance, accuracy) =====================
from typing import Iterable

def _chebyshev_distance(ax: int, ay: int, bx: int, by: int) -> int:
    return max(abs(bx - ax), abs(by - ay))

def _aligned(ax: int, ay: int, bx: int, by: int) -> bool:
    dx, dy = abs(bx - ax), abs(by - ay)
    return dx == 0 or dy == 0 or dx == dy  # по прямой или диагонали

def _bresenham_line(ax: int, ay: int, bx: int, by: int) -> Iterable[tuple[int, int]]:
    """Клетки по линии между A и B, включая конечную, исключая стартовую."""
    x0, y0, x1, y1 = ax, ay, bx, by
    dx = abs(x1 - x0)
    dy = -abs(y1 - y0)
    sx = 1 if x0 < x1 else -1
    sy = 1 if y0 < y1 else -1
    err = dx + dy
    x, y = x0, y0
    first = True
    while True:
        if not first:
            yield (x, y)
        first = False
        if x == x1 and y == y1:
            break
        e2 = 2 * err
        if e2 >= dy:
            err += dy
            x += sx
        if e2 <= dx:
            err += dx
            y += sy

async def _cell_blocks_los(session: AsyncSession, node_id: str, x: int, y: int) -> bool:
    """
    Блокирует обзор объект, у которого в node_objects.props стоит:
      {"block_los": true}
    """
    row = (
        await session.execute(
            text(
                """
                SELECT 1
                FROM node_objects o
                WHERE o.node_id = :nid AND o.x = :x AND o.y = :y
                  AND (o.props ? 'block_los')
                  AND (o.props->>'block_los')::boolean = true
                LIMIT 1
                """
            ),
            {"nid": node_id, "x": x, "y": y},
        )
    ).first()
    return row is not None


async def check_los(session: AsyncSession, node_id: str, ax: int, ay: int, bx: int, by: int) -> bool:
    """True, если между A и B нет клеток, блокирующих обзор."""
    for cx, cy in _bresenham_line(ax, ay, bx, by):
        # конечную клетку (цель) тоже считаем видимой; блокируем только промежуточные
        if (cx, cy) == (bx, by):
            return True
        if await _cell_blocks_los(session, node_id, cx, cy):
            return False
    return True

async def _get_actor_pos(session: AsyncSession, actor_id: str):
    row = (
        await session.execute(
            text("SELECT node_id, COALESCE(x,0) AS x, COALESCE(y,0) AS y FROM actors WHERE id=:id"),
            {"id": actor_id},
        )
    ).mappings().first()
    if not row or not row["node_id"]:
        return None
    return row["node_id"], int(row["x"]), int(row["y"])

async def _weapon_in_hand(session: AsyncSession, actor_id: str):
    """
    Берём предмет из правой руки (если пусто — из левой). Возвращаем (item_id, kind_row).
    kind_row включает нужные поля для дальности и точности.
    """
    inv = (
        await session.execute(
            text("""
                SELECT left_item, right_item FROM inventories WHERE actor_id=:aid
            """),
            {"aid": actor_id},
        )
    ).mappings().first()
    if not inv:
        return None, None

    hand_item = inv["right_item"] or inv["left_item"]
    if not hand_item:
        return None, None

    kind = (
        await session.execute(
            text("""
                SELECT k.id, k.title, k.weapon_class, k.damage_type,
                       COALESCE(k.opt_range,1) AS opt_range,
                       COALESCE(k.max_range,1) AS max_range,
                       COALESCE(k.crit_chance,5.0) AS crit_chance,
                       COALESCE(k.hit_bonus,0) AS hit_bonus
                FROM items i
                JOIN item_kinds k ON k.id = i.kind_id
                WHERE i.id = :iid
            """),
            {"iid": hand_item},
        )
    ).mappings().first()
    return hand_item, (dict(kind) if kind else None)

def _estimate_accuracy(dist: int, aligned: bool, opt_range: int, hit_bonus: int) -> int:
    """
    Простая модель точности:
      базово 100
      если не по прямой/диагонали → -30
      штраф -5 за каждую клетку сверх opt_range
      + hit_bonus
      итог ограничиваем 5..95 (чтобы не было 0 и 100)
    """
    acc = 100
    if not aligned:
        acc -= 30
    if dist > opt_range:
        acc -= (dist - opt_range) * 5
    acc += hit_bonus
    return max(5, min(95, acc))

async def preview_attack_geometry_db(session: AsyncSession, attacker_id: str, target_id: str):
    """
    Возвращает геометрию и предварительную точность выстрела:
      - distance, aligned, los
      - weapon (title, opt/max, hit_bonus)
      - projected accuracy
    Ошибки возвращает через {"ok": False, "error": "..."}.
    """
    apos = await _get_actor_pos(session, attacker_id)
    tpos = await _get_actor_pos(session, target_id)
    if not apos:
        return {"ok": False, "error": "attacker_not_found_or_no_position"}
    if not tpos:
        return {"ok": False, "error": "target_not_found_or_no_position"}

    node_a, ax, ay = apos
    node_t, tx, ty = tpos
    if node_a != node_t:
        return {"ok": False, "error": "different_nodes"}

    dist = _chebyshev_distance(ax, ay, tx, ty)
    aligned = _aligned(ax, ay, tx, ty)
    los = await check_los(session, node_a, ax, ay, tx, ty)

    item_id, kind = await _weapon_in_hand(session, attacker_id)
    if not kind:
        return {
            "ok": True,
            "distance": dist,
            "aligned": aligned,
            "los": los,
            "weapon": None,
            "projected_accuracy": None,
        }

    acc = _estimate_accuracy(dist, aligned, int(kind["opt_range"]), int(kind["hit_bonus"]))

    return {
        "ok": True,
        "distance": dist,
        "aligned": aligned,
        "los": los,
        "weapon": {
            "item_id": str(item_id),
            "title": kind["title"],
            "weapon_class": kind["weapon_class"],
            "damage_type": kind["damage_type"],
            "opt_range": int(kind["opt_range"]),
            "max_range": int(kind["max_range"]),
            "crit_chance": float(kind["crit_chance"]),
            "hit_bonus": int(kind["hit_bonus"]),
        },
        "projected_accuracy": acc,
    }
# ===================== COMBAT ATTACK (range/los/hit/crit/damage) =====================
import random

async def _get_resist_mod(session: AsyncSession, actor_id: str, damage_type: str) -> float:
    row = (
        await session.execute(
            text("SELECT resistances FROM actors WHERE id=:id"),
            {"id": actor_id},
        )
    ).mappings().first()
    if not row:
        return 1.0
    res = row.get("resistances") or {}
    try:
        # jsonb может приехать как dict/str — нормализуем
        if isinstance(res, str):
            import json as _json
            res = _json.loads(res)
    except Exception:
        res = {}
    return float(res.get(damage_type, 1.0))

async def _base_damage_for(kind: dict) -> int:
    """
    Откуда взять базовый урон:
    1) если в props есть damage -> берём его,
    2) иначе условно по классу: melee=5, ranged=6, magic=7 (чтобы сразу работало).
    Ты потом сможешь задать точные цифры в item_kinds.props -> {"damage": N}.
    """
    props = kind.get("props") or {}
    dmg = None
    if isinstance(props, dict):
        dmg = props.get("damage")
    if isinstance(dmg, (int, float)) and dmg > 0:
        return int(dmg)
    wc = (kind.get("weapon_class") or "melee").lower()
    if wc == "ranged":
        return 6
    if wc == "magic":
        return 7
    return 5  # melee по умолчанию

async def _weapon_kind_for_item(session: AsyncSession, item_id: str) -> dict | None:
    row = (
        await session.execute(
            text("""
                SELECT k.id, k.title, k.weapon_class, k.damage_type, k.props,
                       COALESCE(k.opt_range,1) AS opt_range,
                       COALESCE(k.max_range,1) AS max_range,
                       COALESCE(k.crit_chance,5.0) AS crit_chance,
                       COALESCE(k.hit_bonus,0) AS hit_bonus
                FROM items i
                JOIN item_kinds k ON k.id = i.kind_id
                WHERE i.id = :iid
            """),
            {"iid": item_id},
        )
    ).mappings().first()
    return dict(row) if row else None

async def _get_item_charges(session, item_id: str) -> int | None:
    row = (await session.execute(
        text("SELECT charges FROM items WHERE id=:iid"),
        {"iid": item_id}
    )).mappings().first()
    if not row:
        return None
    return row["charges"]


async def _spend_one_charge(session, item_id: str) -> int | None:
    row = (await session.execute(
        text("""
            UPDATE items
               SET charges = CASE
                               WHEN charges IS NULL THEN NULL
                               WHEN charges > 0 THEN charges - 1
                               ELSE charges
                             END
             WHERE id=:iid
         RETURNING charges
        """),
        {"iid": item_id}
    )).mappings().first()
    return row and row["charges"]

    

# ---------- Боевая логика: реальная атака ----------
from sqlalchemy import text
import random

async def handle_actor_death(session: AsyncSession, actor_id: str) -> None:
    """
    Общий хук для смерти актёра.
    Сейчас делает минимум:
    - гарантированно ставит hp = 0 в stats.
    Дальше можно расширить: телепорт героя, дроп лута, отметка "труп" и т.п.
    """
    await session.execute(
        text(
            """
            update actors
               set stats = jsonb_set(
                    coalesce(stats, '{}'::jsonb),
                    '{hp}',
                    to_jsonb(0),
                    true
               )
             where id = :aid
            """
        ),
        {"aid": actor_id},
    )

# ----------------- ammo helpers -----------------
async def _weapon_ammo_type_for_item(session, item_id: str) -> str | None:
    row = (await session.execute(
        text("""
            SELECT k.ammo_type
            FROM items i
            JOIN item_kinds k ON k.id = i.kind_id
            WHERE i.id = :iid
        """),
        {"iid": item_id}
    )).mappings().first()
    return row and row["ammo_type"]


async def _find_ammo_in_backpack(session, actor_id: str, ammo_type: str):
    """
    Ищем первый подходящий патрон в рюкзаке:
    - inventories.backpack (uuid[])
    - items.id = any(backpack) and item_kinds.ammo_type = :ammo_type
    Возвращаем dict(id, title, charges) или None.
    """
    row = (await session.execute(
        text("""
            SELECT i.id, k.title, i.charges
            FROM inventories inv
            JOIN items i ON i.id = ANY(COALESCE(inv.backpack,'{}'::uuid[]))
            JOIN item_kinds k ON k.id = i.kind_id
            WHERE inv.actor_id = :aid
              AND COALESCE(k.ammo_type, '') = :ammo
            LIMIT 1
        """),
        {"aid": actor_id, "ammo": ammo_type}
    )).mappings().first()
    return dict(row) if row else None


async def _consume_one_ammo_from_backpack(session, actor_id: str, ammo_item_id: str):
    """
    Тратим 1 заряд из ammo-предмета:
    - если charges > 1: charges -= 1
    - если charges <= 1 или NULL: удалить предмет и убрать из inventories.backpack
    Возвращаем {"left": int|None, "deleted": bool}
    """
    # current charges
    r = (await session.execute(
        text("SELECT charges FROM items WHERE id=:iid"),
        {"iid": ammo_item_id}
    )).mappings().first()
    if not r:
        return {"left": None, "deleted": True}

    ch = r["charges"]
    if ch is None or ch <= 1:
        # удаляем сам предмет
        await session.execute(text("DELETE FROM items WHERE id=:iid"), {"iid": ammo_item_id})
        # и убираем из backpack
        await session.execute(
            text("""
                UPDATE inventories
                   SET backpack = array_remove(COALESCE(backpack,'{}'::uuid[]), CAST(:iid AS uuid))
                 WHERE actor_id = :aid
            """),
            {"aid": actor_id, "iid": ammo_item_id}
        )
        return {"left": 0, "deleted": True}

    # иначе просто минус 1
    row2 = (await session.execute(
        text("""
            UPDATE items SET charges = charges - 1
             WHERE id=:iid
         RETURNING charges
        """),
        {"iid": ammo_item_id}
    )).mappings().first()
    return {"left": row2 and row2["charges"], "deleted": False}

async def _actor_stat_from_meta(session, actor_id: str, key: str, default: int = 0) -> int:
    row = (await session.execute(
        text("SELECT meta FROM actors WHERE id=:aid"),
        {"aid": actor_id}
    )).mappings().first()
    if not row:
        return default
    meta = row["meta"] or {}
    try:
        v = meta.get(key, default)
        return int(v) if v is not None else default
    except Exception:
        return default




async def perform_attack_db(session, attacker_id: str, target_id: str):
    """
    Выполняет фактическую атаку (старая логика) + статусы/броня:
      - LOS/дистанция/max_range
      - боезапас
      - бросок попадания/крит
      - сопротивления цели
      - броня цели (-10% за уровень 0..5) (+временный бонус от guard)
      - статусные модификаторы: slow/guard/rage (простые и прозрачные)
    """
    import random
    from sqlalchemy import text

    events = []

    # --- атакующий + оружие (правая рука) ---
    q = await session.execute(text("""
        SELECT a.id AS aid, a.node_id, a.x, a.y,
               i.id AS item_id,
               k.title AS weapon_title, k.weapon_class, k.damage_type,
               k.opt_range, k.max_range, k.crit_chance, k.hit_bonus,
               k.ammo_type, k.tags,
               (k.props->>'damage')::int AS base_damage
        FROM actors a
        LEFT JOIN inventories inv ON inv.actor_id = a.id
        LEFT JOIN items i         ON i.id = inv.right_item
        LEFT JOIN item_kinds k    ON k.id = i.kind_id
        WHERE a.id = :aid
    """), {"aid": attacker_id})
    atk = q.mappings().first()
    if not atk or not atk["item_id"]:
        return {"ok": True, "events": [{"type": "NO_WEAPON", "payload": {}}]}

    weapon  = atk
    item_id = weapon["item_id"]

    # --- цель ---
    tq = await session.execute(
        text("SELECT id, node_id, x, y, resistances FROM actors WHERE id=:tid"),
        {"tid": target_id}
    )
    tgt = tq.mappings().first()
    if not tgt:
        return {"ok": False, "error": "target_not_found"}

    # --- геометрия ---
    dx = abs(atk["x"] - tgt["x"])
    dy = abs(atk["y"] - tgt["y"])
    dist = max(dx, dy)
    aligned = (dx == 0 or dy == 0 or dx == dy)

    # --- линия обзора ---
    los = await check_los(session, atk["node_id"], atk["x"], atk["y"], tgt["x"], tgt["y"])
    if not los:
        return {"ok": True, "events": [
            {"type": "ATTACK_START", "payload": {
                "attacker": attacker_id, "target": target_id,
                "weapon": {"title": weapon["weapon_title"], "class": weapon["weapon_class"], "damage_type": weapon["damage_type"]},
                "distance": dist, "aligned": aligned, "los": False
            }},
            {"type": "LOS_BLOCKED", "payload": {}}
        ]}

    # --- проверка максимальной дальности ---
    max_r = int(weapon["max_range"] or 0)
    if max_r > 0 and dist > max_r:
        return {"ok": True, "events": [
            {"type": "ATTACK_START", "payload": {
                "attacker": attacker_id, "target": target_id,
                "weapon": {"title": weapon["weapon_title"], "class": weapon["weapon_class"], "damage_type": weapon["damage_type"]},
                "distance": dist, "aligned": aligned, "los": True
            }},
            {"type": "ATTACK_OUT_OF_RANGE", "payload": {"max_range": max_r}}
        ]}

    # --- правило ближнего боя: строго соседняя клетка ---
    if (weapon["weapon_class"] or "").lower() == "melee" and dist != 1:
        return {"ok": True, "events": [
            {"type": "ATTACK_START", "payload": {
                "attacker": attacker_id, "target": target_id,
                "weapon": {"title": weapon["weapon_title"], "class": weapon["weapon_class"], "damage_type": weapon["damage_type"]},
                "distance": dist, "aligned": aligned, "los": True
            }},
            {"type": "ATTACK_OUT_OF_RANGE", "payload": {"reason": "melee_requires_adjacent", "required": 1}}
        ]}

    # --- боезапас/заряды ---
    spent_ev = empty_ev = hint_ev = None
    cur_ch = await _get_item_charges(session, item_id)
    if cur_ch is not None:
        if (cur_ch or 0) <= 0:
            await session.rollback()
            return {"ok": True, "events": [
                {"type": "ATTACK_START", "payload": {
                    "attacker": attacker_id, "target": target_id,
                    "weapon": {"title": weapon["weapon_title"], "class": weapon["weapon_class"], "damage_type": weapon["damage_type"]},
                    "distance": dist, "aligned": aligned, "los": True
                }},
                {"type": "NO_AMMO", "payload": {}}
            ]}
        left = await _spend_one_charge(session, item_id)
        spent_ev = {"type": "CONSUME", "payload": {"item": weapon["weapon_title"], "delta": -1, "left": left}}
        if left == 0:
            empty_ev = {"type": "AMMO_EMPTY", "payload": {}}
            hint_ev  = {"type": "RELOAD_HINT", "payload": {"endpoint": "/inventory/reload"}}
    else:
        weapon_ammo = await _weapon_ammo_type_for_item(session, item_id)
        if weapon_ammo:
            ammo = await _find_ammo_in_backpack(session, attacker_id, weapon_ammo)
            if not ammo:
                await session.rollback()
                return {"ok": True, "events": [
                    {"type": "ATTACK_START", "payload": {
                        "attacker": attacker_id, "target": target_id,
                        "weapon": {"title": weapon["weapon_title"], "class": weapon["weapon_class"], "damage_type": weapon["damage_type"]},
                        "distance": dist, "aligned": aligned, "los": True
                    }},
                    {"type": "NO_AMMO", "payload": {"ammo_type": weapon_ammo}}
                ]}
            result  = await _consume_one_ammo_from_backpack(session, attacker_id, ammo["id"])
            spent_ev = {"type": "AMMO_CONSUME", "payload": {"ammo_title": ammo["title"], "delta": -1, "left": result["left"]}}
            if result["deleted"]:
                empty_ev = {"type": "AMMO_DEPLETED", "payload": {"ammo_title": ammo["title"]}}

    # --- старт атаки ---
    events.append({
        "type": "ATTACK_START",
        "payload": {
            "attacker": attacker_id, "target": target_id,
            "weapon": {"title": weapon["weapon_title"], "class": weapon["weapon_class"], "damage_type": weapon["damage_type"]},
            "distance": dist, "aligned": aligned, "los": True
        }
    })
    if spent_ev: events.append(spent_ev)
    if empty_ev: events.append(empty_ev)
    if hint_ev:  events.append(hint_ev)

    # --- ТОЧНОСТЬ + статус-модификаторы ---
    accuracy = 100
    mods = {}

    if not aligned:
        mods["angle_penalty"] = -30
        accuracy -= 30

    opt_r = int(weapon["opt_range"] or 0)
    range_penalty = max(0, dist - opt_r) * 5
    if range_penalty:
        mods["range_penalty"] = -range_penalty
        accuracy -= range_penalty

    if weapon["hit_bonus"]:
        mods["weapon_hit_bonus"] = int(weapon["hit_bonus"])
        accuracy += int(weapon["hit_bonus"])

    # штраф для луков в упоре
    tags = weapon.get("tags") or []
    is_bow = (str(weapon.get("ammo_type") or "").lower() == "arrow") or ("bow" in [t.lower() for t in (tags or [])])
    min_r = int(weapon.get("min_range") or 0)
    if is_bow and min_r > 0 and (weapon.get("weapon_class") or "").lower() == "ranged" and dist < min_r:
        close_pen = (min_r - dist) * int(weapon.get("near_penalty") or 10)
        if close_pen > 0:
            mods["near_penalty"] = -close_pen
            accuracy -= close_pen

    # из meta
    atk_acc = await _actor_stat_from_meta(session, attacker_id, "acc_bonus", 0)
    tgt_eva = await _actor_stat_from_meta(session, target_id,   "evasion",   0)
    if atk_acc:
        mods["acc_bonus"] = int(atk_acc); accuracy += int(atk_acc)
    if tgt_eva:
        mods["evasion"] = -int(tgt_eva);  accuracy -= int(tgt_eva)

    # --- статусные модификаторы (slow/guard/rage) ---
    smods = await get_status_combat_mods(session, attacker_id, target_id)
    acc_delta = smods.get("accuracy_mod_attacker", 0)
    if acc_delta:
        mods["status_acc"] = acc_delta
        accuracy += acc_delta

    accuracy = max(5, min(95, accuracy))
    roll = random.randint(1, 100)
    events.append({"type": "HIT_ROLL", "payload": {"accuracy": accuracy, "roll": roll, "mods": mods}})
    if roll > accuracy:
        events.append({"type": "ATTACK_MISS", "payload": {}})
        await session.commit()
        return {"ok": True, "events": events}

    # --- базовый урон + крит ---
    base = (weapon["base_damage"] or 5) + int(smods.get("damage_bonus_attacker", 0))
    base = int(round(base * float(smods.get("damage_mult_attacker", 1.0))))
    crit_mult = 2.0
    try:
        row_props = (await session.execute(
            text("SELECT k.props FROM item_kinds k JOIN items i ON i.kind_id = k.id WHERE i.id = :iid"),
            {"iid": item_id}
        )).mappings().first()
        if row_props:
            props = row_props.get("props") or {}
            if isinstance(props, dict):
                cm = props.get("crit_mult")
                if cm is not None:
                    crit_mult = float(cm)
    except Exception:
        pass

    crit = roll <= (weapon["crit_chance"] or 0)
    if crit:
        base = int(round(base * crit_mult))
        events.append({"type": "ATTACK_CRIT", "payload": {
            "crit_chance": weapon["crit_chance"],
            "crit_mult": crit_mult
        }})

    # --- сопротивление цели ---
    dmg_type = weapon["damage_type"] or "physical"
    resist_mod = 1.0
    if tgt["resistances"]:
        mod = tgt["resistances"].get(dmg_type)
        if mod is not None:
            resist_mod = float(mod)
    events.append({"type": "RESIST_APPLY", "payload": {"damage_type": dmg_type, "resist_mod": resist_mod}})

    # --- броня цели (0..5 уровней) + бонус от guard ---
    final_dmg = max(1, int(base * resist_mod))
    armor_lvl = await effective_armor_level(session, target_id) + int(smods.get("armor_bonus_target", 0))
    if armor_lvl < 0:
        armor_lvl = 0
    if armor_lvl > 5:
        armor_lvl = 5

    armored = apply_armor_reduction(final_dmg, armor_lvl)
    if armor_lvl > 0:
        events.append({"type": "ARMOR_APPLY", "payload": {"level": armor_lvl, "before": final_dmg, "after": armored}})
    final_dmg = armored

    # --- применяем урон: stats.hp (JSONB) ---
    await session.execute(text("""
        update actors
           set stats = jsonb_set(
                coalesce(stats,'{}'::jsonb),
                '{hp}',
                to_jsonb( GREATEST(0, (coalesce((stats->>'hp')::int, 0)) - CAST(:dmg AS int)) ),
                true
           )
         where id = :tid
    """), {"tid": target_id, "dmg": int(final_dmg)})

    events.append({"type": "DAMAGE_APPLY", "payload": {"final": final_dmg}})

    # --- смерть цели ---
    nhp = (await session.execute(text("""
        select coalesce((stats->>'hp')::int, 0) as hp from actors where id=:tid
    """), {"tid": target_id})).mappings().first()["hp"]
    if nhp <= 0:
        events.append({"type": "DEATH", "payload": {"target": target_id}})

    await session.commit()
    return {"ok": True, "events": events}

# --- REACTIVE COUNTER HELPERS ---

async def _apply_tmp_status(
    session: AsyncSession,
    actor_id: str,
    label: str,
    turns: int,
    meta: dict,
    note: str = "",
    tags: list[str] | None = None,
) -> None:
    """Запишем 1-ходовой статус в actor_statuses (session_id допускается NULL)."""
    await session.execute(
        text("""
            insert into actor_statuses(actor_id, session_id, label, note, tags, turns_left, intensity, meta)
            values(:aid, null, :lbl, :note, :tags, :ttl, 1, :meta)
        """),
        {
            "aid": actor_id,
            "lbl": label,
            "note": note,
            "tags": tags or [],
            "ttl": int(turns),
            "meta": meta or {},
        }
    )

async def npc_reactive_counter_db(
    session: AsyncSession,
    npc_id: str,
    target_id: str,
    received_damage: int,
) -> dict:
    """
    Реактивная ответка NPC:
      - слабый удар героя (<=5)  -> press (+5 acc, +2 dmg)
      - средний (6..11)         -> без модификаторов
      - сильный (>=12)          -> по умолчанию stagger (-15 acc), но 20% шанс rage (+10 acc, +30% dmg)
    Затем запускаем обычную атаку perform_attack_db(npc -> hero).
    """
    import random

    received_damage = max(0, int(received_damage))

    applied = []
    if received_damage <= 5:
        # «прижал» (чуть точнее и больнее)
        await _apply_tmp_status(session, npc_id, "press", 1, {"accuracy_mod_attacker": 5, "damage_bonus_attacker": 2})
        applied.append({"label": "press", "mods": {"+acc": 5, "+dmg": 2}})
    elif 6 <= received_damage <= 11:
        # нейтрально — без статуса
        pass
    else:
        # сильный урон → обычно шатание, но редко ярость
        if random.random() < 0.20:
            await _apply_tmp_status(session, npc_id, "rage", 1, {"accuracy_mod_attacker": 10, "damage_mult_attacker": 1.3})
            applied.append({"label": "rage", "mods": {"+acc": 10, "x dmg": 1.3}})
        else:
            await _apply_tmp_status(session, npc_id, "stagger", 1, {"accuracy_mod_attacker": -15})
            applied.append({"label": "stagger", "mods": {"-acc": 15}})

    await session.commit()

    # запускаем обычную атаку (старый боевой движок)
    # ВАЖНО: perform_attack_db уже учитывает статусные моды через get_status_combat_mods.
    result = await perform_attack_db(session, attacker_id=npc_id, target_id=target_id)
    return {"ok": True, "applied_statuses": applied, "attack": result}
