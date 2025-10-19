# server/app/routers/assets.py
from typing import Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from pydantic import BaseModel, Field
from sqlalchemy import text, bindparam
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import JSONB  # важно

from app.db import get_session
from app.dao import fetch_node

router = APIRouter()

# ---------- MODELS ----------
class PlaceAssetIn(BaseModel):
    node_id: str
    asset_id: str  # ожидаем запись из таблицы assets (kind: room/deco/prop/...)


class PlaceObjectIn(BaseModel):
    node_id: str
    asset_id: str               # должен быть kind='prop' или 'deco'
    x: int = Field(ge=0)
    y: int = Field(ge=0)
    rotation: int = 0
    layer: int = Field(1, ge=1, le=3)  # 1-низ, 2-средний, 3-верх
    props: Dict[str, Any] = Field(default_factory=dict)  # безопасный дефолт


# ---------- SEARCH ----------
@router.get("/assets/search")
async def search_assets(
    q: Optional[str] = Query(None, description="поиск по id/title/description"),
    biome: Optional[str] = Query(None, description="фильтр по biome_hint"),
    tag: Optional[str] = Query(None, description="фильтр по одному тегу"),
    session: AsyncSession = Depends(get_session),
):
    sql = """
        SELECT id, title, kind, biome_hint, tags, description
        FROM assets
        WHERE 1=1
    """
    params: Dict[str, Any] = {}

    if q:
        sql += " AND (id ILIKE :q OR title ILIKE :q OR description ILIKE :q)"
        params["q"] = f"%{q}%"

    if biome:
        sql += " AND biome_hint = :biome"
        params["biome"] = biome

    if tag:
        sql += " AND :tag = ANY(tags)"
        params["tag"] = tag

    rows = (await session.execute(text(sql), params)).mappings().all()
    return [dict(r) for r in rows]


# ---------- PLACE (применение ассета к узлу на уровне фактов/контента) ----------
@router.post("/world/place")
async def place_asset(body: PlaceAssetIn, session: AsyncSession = Depends(get_session)):
    # 1) Проверим, что узел существует
    node_exists = (
        await session.execute(text("SELECT 1 FROM nodes WHERE id = :id"), {"id": body.node_id})
    ).scalar()
    if not node_exists:
        raise HTTPException(status_code=404, detail="node_not_found")

    # 2) Проверим ассет
    asset = (
        await session.execute(
            text("SELECT id, title, prefab FROM assets WHERE id = :id"),
            {"id": body.asset_id},
        )
    ).mappings().first()
    if not asset:
        raise HTTPException(status_code=404, detail="asset_not_found")

    prefab = asset.get("prefab") or {}
    facts_patch: Dict[str, Any] = prefab.get("facts") or {}
    content_patch: Dict[str, Any] = prefab.get("content_patch") or {}

    # 3) Применим facts (upsert по (node_id, k)) — передаём JSONB корректно
    if facts_patch:
        stmt = text(
            """
            INSERT INTO facts(node_id, k, v)
            VALUES (:nid, :k, :v)
            ON CONFLICT (node_id, k) DO UPDATE SET v = EXCLUDED.v
            """
        ).bindparams(
            bindparam("nid"),
            bindparam("k"),
            bindparam("v", type_=JSONB),  # ключевой момент
        )
        for k, v in facts_patch.items():
            await session.execute(stmt, {"nid": body.node_id, "k": k, "v": v})

    # 4) Применим content_patch (jsonb merge) — тоже как JSONB
    if content_patch:
        stmt2 = text(
            """
            UPDATE nodes
               SET content = COALESCE(content, '{}'::jsonb) || :patch
             WHERE id = :nid
            """
        ).bindparams(
            bindparam("nid"),
            bindparam("patch", type_=JSONB),  # и здесь JSONB
        )
        await session.execute(stmt2, {"nid": body.node_id, "patch": content_patch})

    # 5) Лог размещения ассета
    await session.execute(
        text("INSERT INTO node_assets(node_id, asset_id) VALUES (:nid, :aid)"),
        {"nid": body.node_id, "aid": body.asset_id},
    )

    await session.commit()

    # 6) Вернём обновлённый узел
    node = await fetch_node(session, body.node_id)
    return {"ok": True, "node": node}


# ---------- PLACE OBJECT (поставить физический объект на клетку) ----------
@router.post("/world/place_object")
async def place_object(body: PlaceObjectIn, session: AsyncSession = Depends(get_session)):
    # 1) Проверка узла
    exists = (await session.execute(text("SELECT 1 FROM nodes WHERE id=:id"), {"id": body.node_id})).scalar()
    if not exists:
        raise HTTPException(status_code=404, detail="node_not_found")

    # 2) Проверка ассета (prop/deco)
    a = (
        await session.execute(text("SELECT id, kind FROM assets WHERE id=:id"), {"id": body.asset_id})
    ).mappings().first()
    if not a:
        raise HTTPException(status_code=404, detail="asset_not_found")
    if a["kind"] not in ("prop", "deco"):
        raise HTTPException(status_code=400, detail="asset_kind_not_placeable")

    # 3) Границы узла
    node = (
        await session.execute(
            text(
                "SELECT COALESCE(width, size_w, 16) AS w, COALESCE(height, size_h, 16) AS h FROM nodes WHERE id=:id"
            ),
            {"id": body.node_id},
        )
    ).mappings().first()
    W, H = int(node["w"]), int(node["h"])
    if not (0 <= body.x < W and 0 <= body.y < H):
        raise HTTPException(status_code=400, detail="out_of_bounds")

    # 4) Запретим два объекта с одинаковым layer в одной клетке
    conflict = (await session.execute(text("""
        SELECT 1 FROM node_objects
         WHERE node_id=:nid AND x=:x AND y=:y AND layer=:layer
         LIMIT 1
    """), {"nid": body.node_id, "x": body.x, "y": body.y, "layer": body.layer})).scalar()
    if conflict:
        raise HTTPException(status_code=409, detail="cell_layer_occupied")

    # 5) Вставка объекта с корректной типизацией props -> JSONB
    insert_stmt = text(
        """
        INSERT INTO node_objects(node_id, asset_id, x, y, rotation, props, layer)
        VALUES (:nid, :aid, :x, :y, :rot, :props, :layer)
        """
    ).bindparams(
        bindparam("nid"),
        bindparam("aid"),
        bindparam("x"),
        bindparam("y"),
        bindparam("rot"),
        bindparam("props", type_=JSONB),  # ключевой момент
        bindparam("layer"),
    )

    await session.execute(
        insert_stmt,
        {
            "nid": body.node_id,
            "aid": body.asset_id,
            "x": body.x,
            "y": body.y,
            "rot": body.rotation or 0,
            "props": body.props or {},
            "layer": body.layer,
        },
    )
    await session.commit()

    # 6) Вернём обновлённый узел (если в fetch_node добавлен objects — он отобразится)
    full_node = await fetch_node(session, body.node_id)
    return {"ok": True, "node": full_node}


# --- управление объектами (перемещение/правка/удаление) ---
class UpdateObjectIn(BaseModel):
    # Любое поле опционально — меняем только то, что пришло
    x: Optional[int] = Field(default=None, ge=0)
    y: Optional[int] = Field(default=None, ge=0)
    rotation: Optional[int] = None
    layer: Optional[int] = Field(default=None, ge=1, le=3)
    props_patch: Optional[Dict[str, Any]] = Field(default=None)  # jsonb merge
    replace_props: bool = False  # если True — полностью заменить props на props_patch


@router.put("/world/object/{obj_id}")
async def update_object(
    obj_id: int = Path(..., ge=1),
    body: UpdateObjectIn = ...,
    session: AsyncSession = Depends(get_session),
):
    # 0) достанем текущий объект и размер узла
    row = (await session.execute(text("""
        SELECT o.id, o.node_id, o.asset_id, o.x, o.y, o.rotation, o.layer, o.props,
               COALESCE(n.width, n.size_w, 16) AS w,
               COALESCE(n.height, n.size_h, 16) AS h
          FROM node_objects o
          JOIN nodes n ON n.id = o.node_id
         WHERE o.id = :id
    """), {"id": obj_id})).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="object_not_found")

    node_id = row["node_id"]
    W, H = int(row["w"]), int(row["h"])

    new_x = body.x if body.x is not None else row["x"]
    new_y = body.y if body.y is not None else row["y"]
    new_rot = body.rotation if body.rotation is not None else row["rotation"]
    new_layer = body.layer if body.layer is not None else row["layer"]

    # 1) проверка границ
    if not (0 <= new_x < W and 0 <= new_y < H):
        raise HTTPException(status_code=400, detail="out_of_bounds")

    # 2) проверка конфликта по слою/клетке, если координаты или слой меняются
    if (new_x != row["x"]) or (new_y != row["y"]) or (new_layer != row["layer"]):
        conflict = (await session.execute(text("""
            SELECT 1 FROM node_objects
             WHERE node_id=:nid AND x=:x AND y=:y AND layer=:layer AND id<>:id
             LIMIT 1
        """), {"nid": node_id, "x": new_x, "y": new_y, "layer": new_layer, "id": obj_id})).scalar()
        if conflict:
            raise HTTPException(status_code=409, detail="cell_layer_occupied")

    # 3) апдейт props
    if body.replace_props and body.props_patch is not None:
        # полная замена props
        stmt = text("""
            UPDATE node_objects
               SET x = :x, y = :y, rotation = :rot, layer = :layer, props = :p
             WHERE id = :id
        """).bindparams(
            bindparam("x"), bindparam("y"), bindparam("rot"),
            bindparam("layer"),
            bindparam("p", type_=JSONB),
            bindparam("id"),
        )
        params = {"x": new_x, "y": new_y, "rot": new_rot, "layer": new_layer, "p": body.props_patch, "id": obj_id}
        await session.execute(stmt, params)

    elif (body.props_patch is not None) and (not body.replace_props):
        # merge props (jsonb ||)
        stmt = text("""
            UPDATE node_objects
               SET x = :x, y = :y, rotation = :rot, layer = :layer,
                   props = COALESCE(props, '{}'::jsonb) || :patch
             WHERE id = :id
        """).bindparams(
            bindparam("x"), bindparam("y"), bindparam("rot"),
            bindparam("layer"),
            bindparam("patch", type_=JSONB),
            bindparam("id"),
        )
        params = {"x": new_x, "y": new_y, "rot": new_rot, "layer": new_layer, "patch": body.props_patch, "id": obj_id}
        await session.execute(stmt, params)

    else:
        # меняем только координаты/rotation/layer
        await session.execute(text("""
            UPDATE node_objects
               SET x = :x, y = :y, rotation = :rot, layer = :layer
             WHERE id = :id
        """), {"x": new_x, "y": new_y, "rot": new_rot, "layer": new_layer, "id": obj_id})

    await session.commit()

    # вернём обновлённый узел
    node = await fetch_node(session, node_id)
    return {"ok": True, "node": node}


@router.delete("/world/object/{obj_id}")
async def delete_object(
    obj_id: int = Path(..., ge=1),
    session: AsyncSession = Depends(get_session),
):
    row = (await session.execute(text("""
        SELECT id, node_id FROM node_objects WHERE id=:id
    """), {"id": obj_id})).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="object_not_found")

    node_id = row["node_id"]

    await session.execute(text("DELETE FROM node_objects WHERE id=:id"), {"id": obj_id})
    await session.commit()

    node = await fetch_node(session, node_id)
    return {"ok": True, "node": node}

# --- ДОБАВКИ ДЛЯ КОНТЕЙНЕРОВ И ПОДБОРА ---
from fastapi import Path
from pydantic import BaseModel
from sqlalchemy.dialects.postgresql import UUID as PGUUID

# утилка: короткий вью предмета (как в fetch_inventory)
async def _item_brief(session: AsyncSession, item_id):
    row = (await session.execute(text("""
        select i.id, k.id as kind_id, k.title, i.charges
        from items i
        join item_kinds k on k.id = i.kind_id
        where i.id = :iid
    """), {"iid": item_id})).mappings().first()
    return dict(row) if row else None

# --- 2.1: ПРОСМОТР КОНТЕЙНЕРА ---
@router.get("/world/container/{obj_id}")
async def get_container(
    obj_id: int = Path(..., ge=1),
    session: AsyncSession = Depends(get_session),
):
    # проверим, что объект- контейнер
    obj = (await session.execute(text("""
        select id, node_id, is_container, container_capacity
        from node_objects where id=:id
    """), {"id": obj_id})).mappings().first()
    if not obj or not obj["is_container"]:
        raise HTTPException(status_code=404, detail="container_not_found")

    inv = (await session.execute(text("""
        select items from object_inventories where object_id=:id
    """), {"id": obj_id})).mappings().first()

    item_ids = (inv and inv["items"]) or []
    items = []
    for iid in item_ids:
        brief = await _item_brief(session, iid)
        if brief:
            items.append(brief)

    return {
        "object_id": obj_id,
        "capacity": obj["container_capacity"],
        "items": items
    }

class TakePutIn(BaseModel):
    item_id: str  # UUID предмета

# --- 2.2: ВЗЯТЬ ИЗ КОНТЕЙНЕРА ---
@router.post("/world/container/{obj_id}/take")
async def take_from_container(
    obj_id: int,
    body: TakePutIn,
    session: AsyncSession = Depends(get_session),
):
    # объект- контейнер?
    obj = (await session.execute(text("""
        select id, node_id, is_container from node_objects where id=:id
    """), {"id": obj_id})).mappings().first()
    if not obj or not obj["is_container"]:
        raise HTTPException(status_code=404, detail="container_not_found")

    # есть ли такой item в контейнере
    in_container = (await session.execute(text("""
        select :iid = any(coalesce(items,'{}'::uuid[])) as ok
        from object_inventories where object_id=:oid
    """), {"iid": body.item_id, "oid": obj_id})).scalar()
    if not in_container:
        raise HTTPException(status_code=404, detail="item_not_in_container")

    # убираем из контейнера
    await session.execute(text("""
        update object_inventories
        set items = array_remove(coalesce(items,'{}'::uuid[]), :iid)
        where object_id=:oid
    """), {"iid": body.item_id, "oid": obj_id})

    # кладём игроку в рюкзак
    await session.execute(text("""
        update inventories
        set backpack = coalesce(backpack,'{}'::uuid[]) || :iid
        where actor_id='player'
    """), {"iid": body.item_id})

    await session.commit()

    # вернём обновлённый контейнер
    return await get_container(obj_id, session)

# --- 2.3: ПОЛОЖИТЬ В КОНТЕЙНЕР ---
@router.post("/world/container/{obj_id}/put")
async def put_to_container(
    obj_id: int,
    body: TakePutIn,
    session: AsyncSession = Depends(get_session),
):
    # объект- контейнер?
    obj = (await session.execute(text("""
        select id, node_id, is_container, container_capacity from node_objects where id=:id
    """), {"id": obj_id})).mappings().first()
    if not obj or not obj["is_container"]:
        raise HTTPException(status_code=404, detail="container_not_found")

    # предмет у игрока?
    in_player = (await session.execute(text("""
        select :iid = any(coalesce(backpack,'{}'::uuid[])) as ok
        from inventories where actor_id='player'
    """), {"iid": body.item_id})).scalar()
    if not in_player:
        raise HTTPException(status_code=404, detail="item_not_in_player")

    # проверим вместимость
    cur = (await session.execute(text("""
        select coalesce(array_length(items,1),0) as cnt
        from object_inventories where object_id=:oid
    """), {"oid": obj_id})).mappings().first()
    cnt = (cur and cur["cnt"]) or 0
    if cnt >= int(obj["container_capacity"] or 0):
        raise HTTPException(status_code=400, detail="container_full")

    # убираем у игрока
    await session.execute(text("""
        update inventories
        set backpack = array_remove(coalesce(backpack,'{}'::uuid[]), :iid)
        where actor_id='player'
    """), {"iid": body.item_id})

    # кладём в контейнер
    await session.execute(text("""
        insert into object_inventories(object_id, items)
        values(:oid, ARRAY[:iid::uuid])
        on conflict (object_id) do update
        set items = object_inventories.items || excluded.items
    """), {"oid": obj_id, "iid": body.item_id})

    await session.commit()
    return await get_container(obj_id, session)

# --- 2.4: ПОДОБРАТЬ ОБЪЕКТ ВЕРХНЕГО СЛОЯ (layer=3) ---
class PickupIn(BaseModel):
    object_id: int  # id из node_objects

@router.post("/world/pickup")
async def pickup_object(
    body: PickupIn,
    session: AsyncSession = Depends(get_session),
):
    obj = (await session.execute(text("""
        select id, node_id, layer, pickupable, pickup_kind_id, pickup_charges
        from node_objects where id=:id
    """), {"id": body.object_id})).mappings().first()
    if not obj:
        raise HTTPException(status_code=404, detail="object_not_found")

    if obj["layer"] != 3 or not obj["pickupable"]:
        raise HTTPException(status_code=400, detail="not_pickupable")

    kind_id = obj["pickup_kind_id"]
    if not kind_id:
        raise HTTPException(status_code=400, detail="pickup_kind_missing")

    # создаём экземпляр item
    new_item = (await session.execute(text("""
        insert into items(id, kind_id, charges)
        values (gen_random_uuid(), :kid, :chg)
        returning id
    """), {"kid": kind_id, "chg": obj["pickup_charges"]})).mappings().first()
    iid = new_item["id"]

    # кладём в рюкзак игрока
    await session.execute(text("""
        update inventories
        set backpack = coalesce(backpack,'{}'::uuid[]) || :iid
        where actor_id='player'
    """), {"iid": iid})

    # удаляем объект с карты
    await session.execute(text("delete from node_objects where id=:id"), {"id": body.object_id})
    await session.commit()

    # отдаём обновлённый узел
    node = await fetch_node(session, obj["node_id"])
    return {"ok": True, "node": node}
