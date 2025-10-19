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
            text("""
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
            """),
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
            text("""
                SELECT id, kind, archtype, node_id, x, y, hp, mood, trust, aggression
                FROM actors
                WHERE node_id = :id
            """),
            {"id": node_id},
        )
    ).mappings().all()

    # объекты (props/decoration) с координатами и слоем
    objects = (
        await session.execute(
            text("""
                SELECT id, asset_id, x, y, rotation, props, layer
                FROM node_objects
                WHERE node_id = :id
                ORDER BY y, x, layer, id
            """),
            {"id": node_id},
        )
    ).mappings().all()

    # факты
    facts = (
        await session.execute(
            text("""
                SELECT k, v FROM facts WHERE node_id = :id
            """),
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
async def fetch_inventory(session: AsyncSession, actor_id: str):
    inv = (
        await session.execute(
            text("""
                select left_item, right_item, backpack
                from inventories where actor_id = :id
            """),
            {"id": actor_id},
        )
    ).mappings().first()

    if not inv:
        return {"left": None, "right": None, "backpack": []}

    async def item_view(item_id):
        if not item_id:
            return None
        row = (
            await session.execute(
                text("""
                    select i.id, k.id as kind_id, k.title, i.charges
                    from items i
                    join item_kinds k on k.id = i.kind_id
                    where i.id = :iid
                """),
                {"iid": item_id},
            )
        ).mappings().first()
        return row

    left = await item_view(inv["left_item"])
    right = await item_view(inv["right_item"])

    backpack: List[Dict[str, Any]] = []
    bp_ids = inv["backpack"] or []
    if bp_ids:
        stmt = text("""
            select i.id, k.id as kind_id, k.title, i.charges
            from items i
            join item_kinds k on k.id = i.kind_id
            where i.id = any(:ids)
        """).bindparams(
            bindparam("ids", value=bp_ids, type_=ARRAY(UUID(as_uuid=True)))
        )
        rows = (await session.execute(stmt)).mappings().all()
        backpack = list(rows)

    return {"left": left, "right": right, "backpack": backpack}

# ===================== SKILLS =====================
async def learn_skill(session: AsyncSession, actor_id: str, skill_id: str):
    sk = (
        await session.execute(
            text("""
              select id, min_level from skills where id=:sid
            """),
            {"sid": skill_id},
        )
    ).mappings().first()
    if not sk:
        return {"ok": False, "reason": "skill_not_found"}

    actor = (
        await session.execute(
            text("""
              select level, skill_tokens from actors where id=:aid
            """),
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
        text("""
          insert into actor_skills(actor_id,skill_id) values(:aid,:sid)
          on conflict do nothing
        """),
        {"aid": actor_id, "sid": skill_id},
    )

    await session.execute(
        text("""
          update actors set skill_tokens = skill_tokens - 1 where id=:aid
        """),
        {"aid": actor_id},
    )

    await session.commit()
    return {"ok": True}

async def actor_knows_skill(session: AsyncSession, actor_id: str, skill_id: str) -> bool:
    row = await session.execute(
        text("""
            select 1 from actor_skills where actor_id=:aid and skill_id=:sid
        """),
        {"aid": actor_id, "sid": skill_id},
    )
    return row.first() is not None

async def list_skills(session: AsyncSession):
    rows = (
        await session.execute(
            text("""
                select id, title, props
                from skills
            """)
        )
    ).mappings().all()
    return [dict(r) for r in rows]

# ===================== INVENTORY (DB ACTIONS) =====================
async def _get_inventory_row(session: AsyncSession, actor_id: str):
    return (
        await session.execute(
            text("""
                select actor_id, left_item, right_item, backpack
                from inventories where actor_id=:aid
            """),
            {"aid": actor_id},
        )
    ).mappings().first()

async def _item_view_full(session: AsyncSession, item_id) -> Optional[Dict[str, Any]]:
    if not item_id:
        return None
    row = (
        await session.execute(
            text("""
                select i.id, i.kind_id, i.charges, i.durability,
                       k.title, k.tags, k.handedness, k.props
                from items i
                join item_kinds k on k.id = i.kind_id
                where i.id = :iid
            """),
            {"iid": item_id},
        )
    ).mappings().first()
    return dict(row) if row else None

async def _handedness(session: AsyncSession, item_id) -> str:
    r = (
        await session.execute(
            text("""
                select k.handedness
                from items i join item_kinds k on k.id=i.kind_id
                where i.id=:iid
            """),
            {"iid": item_id},
        )
    ).mappings().first()
    return (r and r["handedness"]) or "one_hand"

async def equip_item_db(session: AsyncSession, actor_id: str, hand: str, item_id) -> List[Dict[str, Any]]:
    inv = await _get_inventory_row(session, actor_id)
    if not inv:
        raise ValueError("Inventory not found")

    in_backpack = await session.execute(
        text("""
            select :iid = any(coalesce(backpack,'{}'::uuid[])) as ok
            from inventories where actor_id=:aid
        """),
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
                set backpack = array_remove(coalesce(backpack,'{{}}'::uuid[]), :iid),
                    { 'left_item' if hand=='left' else 'right_item' } = :iid
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
        text("""
            update inventories
            set backpack = array_remove(coalesce(backpack,'{}'::uuid[]), :iid),
                left_item = :iid,
                right_item = :iid
            where actor_id=:aid
        """),
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
            text("""
                update inventories
                set left_item = null, right_item = null,
                    backpack = coalesce(backpack,'{}'::uuid[]) || :iid
                where actor_id=:aid
            """),
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
                backpack = coalesce(backpack,'{{}}'::uuid[]) || :iid
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
                text("""
                    update items set charges = charges - :amt
                    where id=:iid
                    returning charges
                """),
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
                text("""
                    update items set charges = charges - :amt
                    where id=:iid
                    returning charges
                """),
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
