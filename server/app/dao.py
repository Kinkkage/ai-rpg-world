# server/app/dao.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

async def fetch_node(session: AsyncSession, node_id: str):
    node = (await session.execute(text("""
        select id, title, biome, size_w, size_h, exits::jsonb as exits
        from nodes where id = :id
    """), {"id": node_id})).mappings().first()
    if not node:
        return None

    actors = (await session.execute(text("""
        select id, kind, archtype, x, y, hp, mood, trust
        from actors where node_id = :id
    """), {"id": node_id})).mappings().all()

    facts = (await session.execute(text("""
        select k, v from facts where node_id = :id
    """), {"id": node_id})).mappings().all()

    return {
        "id": node["id"],
        "title": node["title"],
        "biome": node["biome"],
        "size": {"w": node["size_w"], "h": node["size_h"]},
        "actors": list(actors),
        "exits": node["exits"] or [],
        "facts": {f["k"]: f["v"] for f in facts}
    }

async def fetch_inventory(session: AsyncSession, actor_id: str):
    inv = (await session.execute(text("""
        select left_item, right_item, backpack
        from inventories where actor_id = :id
    """), {"id": actor_id})).mappings().first()

    if not inv:
        return {"left": None, "right": None, "backpack": []}

    async def item_view(item_id):
        if not item_id:
            return None
        row = (await session.execute(text("""
            select i.id, k.title, i.charges
            from items i
            join item_kinds k on k.id = i.kind_id
            where i.id = :iid
        """), {"iid": item_id})).mappings().first()
        return row

    left = await item_view(inv["left_item"])
    right = await item_view(inv["right_item"])

    backpack = []
    if inv["backpack"]:
        rows = (await session.execute(text("""
            select i.id, k.title, i.charges
            from items i
            join item_kinds k on k.id = i.kind_id
            where i.id = any(:ids)
        """), {"ids": inv["backpack"]})).mappings().all()
        backpack = list(rows)

    return {"left": left, "right": right, "backpack": backpack}
