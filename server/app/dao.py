# server/app/dao.py
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import UUID, ARRAY, JSONB
import json


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
