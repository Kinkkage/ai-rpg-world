# server/app/services/dao_trade.py
from __future__ import annotations
from typing import Any, Dict, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB as PG_JSONB

# ===== helpers: economy, contexts, price ======================================

async def _get_biome_rate(session: AsyncSession, biome: str) -> float:
    """
    Мультипликатор биома из gen_rules.config.economy.biome_rate.
    Если не задано — 1.0.
    """
    row = (await session.execute(
        text("SELECT (config->'economy'->>'biome_rate')::numeric AS r FROM gen_rules WHERE biome=:b"),
        {"b": biome}
    )).mappings().first()
    return float(row["r"]) if row and row["r"] is not None else 1.0

async def _detect_vendor_context(session: AsyncSession, vendor_id: str) -> Dict[str, Any]:
    """
    Возвращает контекст продавца:
      {type:'market'|'npc', biome, node_id?, trust?, aggression?}
    market = узел с meta.poi='market'
    npc    = запись в actors с таким id
    """
    # market node?
    n = (await session.execute(text("""
        SELECT id, biome, meta FROM nodes WHERE id=:id
    """), {"id": vendor_id})).mappings().first()
    if n:
        meta = n.get("meta") or {}
        if isinstance(meta, str):
            import json
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        if isinstance(meta, dict) and meta.get("poi") == "market":
            return {"type": "market", "biome": n["biome"], "node_id": n["id"]}

    # npc vendor?
    a = (await session.execute(text("""
        SELECT a.id, a.node_id, a.trust, a.aggression, n.biome
          FROM actors a
          LEFT JOIN nodes n ON n.id = a.node_id
         WHERE a.id=:id
    """), {"id": vendor_id})).mappings().first()
    if a:
        return {
            "type": "npc",
            "biome": a["biome"],
            "node_id": a["node_id"],
            "trust": int(a["trust"] or 50),
            "aggression": int(a["aggression"] or 0),
        }

    raise ValueError("vendor_not_found_or_not_market")

def _trust_price_factor(trust: int, aggression: int) -> float:
    """
    Лёгкая скидка/наценка от отношений: в пределах 0.9..1.1.
    trust выше -> дешевле, aggression выше -> дороже.
    """
    base = 1.0 - (trust - 50) * 0.002 + (aggression) * 0.002
    return max(0.90, min(1.10, base))

def _cap_price_silver(s: int) -> int:
    """Кэп цены 1..10 серебра по требованию главы."""
    return max(1, min(10, int(s)))

# ===== public DAO ==============================================================

async def get_market_db(session: AsyncSession, vendor_id: str, viewer_actor_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Возвращает список товаров продавца с конечной ценой в СЕРЕБРЕ (инт).
    Формула: price_silver = round(base_price * biome_rate * price_mod * trust_factor) -> cap 1..10
    ВАЖНО: rarity НЕ влияет на цену (по твоему требованию).
    """
    ctx = await _detect_vendor_context(session, vendor_id)
    biome_rate = await _get_biome_rate(session, ctx["biome"])

    # доверие/агрессия только для NPC; рынку нейтрал (1.0)
    trust_factor = 1.0
    if ctx["type"] == "npc":
        trust_factor = _trust_price_factor(int(ctx.get("trust", 50)), int(ctx.get("aggression", 0)))

    rows = (await session.execute(text("""
        SELECT vi.item_kind_id, vi.stock, vi.price_mod,
               ik.title, ik.base_price::numeric AS base_price, ik.rarity
          FROM vendor_inventory vi
          JOIN item_kinds ik ON ik.id = vi.item_kind_id
         WHERE vi.vendor_id = :vid
         ORDER BY ik.title
    """), {"vid": vendor_id})).mappings().all()

    items = []
    for r in rows:
        base = float(r["base_price"] or 0.0)  # трактуем base_price как "база в серебре"
        raw_silver = round(base * float(biome_rate) * float(r["price_mod"]) * trust_factor)
        unit_price_silver = _cap_price_silver(raw_silver)

        items.append({
            "item_kind_id": r["item_kind_id"],
            "title": r["title"],
            "rarity": int(r["rarity"] or 3),
            "stock": int(r["stock"] or 0),
            "unit_price_silver": unit_price_silver
        })

    return {
        "vendor_id": vendor_id,
        "vendor_type": ctx["type"],
        "biome": ctx["biome"],
        "items": items
    }

async def transfer_money_db(session: AsyncSession, from_actor: str, to_actor: str, amount_silver: int) -> Dict[str, Any]:
    """
    Перевод денег в серебре (целые).
    actors.wallet трактуем как "серебро" (numeric, но пишем/читаем целые).
    """
    if amount_silver <= 0:
        return {"ok": False, "error": "amount_must_be_positive"}

    # баланс отправителя
    bal = (await session.execute(text("SELECT wallet FROM actors WHERE id=:id"), {"id": from_actor})).mappings().first()
    if not bal:
        return {"ok": False, "error": "from_actor_not_found"}
    from_balance = int(round(float(bal["wallet"] or 0)))
    if from_balance < amount_silver:
        return {"ok": False, "error": "insufficient_funds", "have_silver": from_balance, "need_silver": amount_silver}

    # получатель должен существовать
    dest = (await session.execute(text("SELECT 1 FROM actors WHERE id=:id"), {"id": to_actor})).first()
    if not dest:
        return {"ok": False, "error": "to_actor_not_found"}

    # перевод
    await session.execute(text("UPDATE actors SET wallet = wallet - :a WHERE id=:id"), {"a": amount_silver, "id": from_actor})
    await session.execute(text("UPDATE actors SET wallet = wallet + :a WHERE id=:id"), {"a": amount_silver, "id": to_actor})

    # лог
    stmt = text("""
        INSERT INTO world_trade_log(type, actor_id, vendor_id, amount, payload)
        VALUES ('MONEY_TRANSFER', :from, :to, :amt, :payload)
    """).bindparams(bindparam("payload", type_=PG_JSONB))

    await session.execute(stmt, {
        "from": from_actor,
        "to": to_actor,
        "amt": amount_silver,  # сумма в серебре
        "payload": {"currency": "silver", "amount_silver": amount_silver}
    })

    await session.commit()
    return {"ok": True, "from": from_actor, "to": to_actor, "amount_silver": amount_silver}

async def buy_item_db(session: AsyncSession, buyer_id: str, vendor_id: str, item_kind_id: str, qty: int = 1) -> Dict[str, Any]:
    """
    Покупка в серебре.
    - списываем серебро у buyer
    - уменьшаем stock у vendor_inventory
    - если vendor — NPC, начисляем ему серебро
    - логируем
    """
    if qty <= 0:
        return {"ok": False, "error": "qty_must_be_positive"}

    # контекст и цена
    market = await get_market_db(session, vendor_id)
    line = next((i for i in market["items"] if i["item_kind_id"] == item_kind_id), None)
    if not line:
        return {"ok": False, "error": "item_not_in_vendor"}

    if line["stock"] < qty:
        return {"ok": False, "error": "not_enough_stock", "stock": line["stock"]}

    unit_silver = int(line["unit_price_silver"])
    total_silver = unit_silver * qty

    # баланс покупателя (серебро)
    bal = (await session.execute(text("SELECT wallet FROM actors WHERE id=:id"), {"id": buyer_id})).mappings().first()
    if not bal:
        return {"ok": False, "error": "buyer_not_found"}
    buyer_silver = int(round(float(bal["wallet"] or 0)))
    if buyer_silver < total_silver:
        return {"ok": False, "error": "insufficient_funds", "have_silver": buyer_silver, "need_silver": total_silver}

    # списываем деньги и склад
    await session.execute(text("UPDATE actors SET wallet = wallet - :a WHERE id=:id"), {"a": total_silver, "id": buyer_id})
    await session.execute(text("""
        UPDATE vendor_inventory SET stock = stock - :q
         WHERE vendor_id=:v AND item_kind_id=:k
    """), {"q": qty, "v": vendor_id, "k": item_kind_id})

    # если NPC-вендор — зачислим ему выручку; рынок-узел кошелька не имеет
    ctx = await _detect_vendor_context(session, vendor_id)
    if ctx["type"] == "npc":
        await session.execute(text("UPDATE actors SET wallet = wallet + :a WHERE id=:id"), {"a": total_silver, "id": vendor_id})

    # лог (JSONB bind)
    import uuid
    receipt = f"rcpt_{uuid.uuid4().hex[:8]}"
    stmt = text("""
        INSERT INTO world_trade_log(type, actor_id, vendor_id, item_kind_id, qty, amount, payload)
        VALUES ('TRADE_BUY', :actor, :vendor, :kind, :qty, :amt, :payload)
    """).bindparams(bindparam("payload", type_=PG_JSONB))

    await session.execute(stmt, {
        "actor": buyer_id,
        "vendor": vendor_id,
        "kind": item_kind_id,
        "qty": qty,
        "amt": total_silver,  # сумма в серебре
        "payload": {"receipt": receipt, "currency": "silver", "unit_price_silver": unit_silver, "total_silver": total_silver}
    })

    await session.commit()

    # пока предметы фактически не кладём в инвентарь (ждём схему items)
    return {"ok": True, "receipt": receipt, "total_silver": total_silver}

async def sell_item_db(session: AsyncSession, seller_id: str, vendor_id: str, item_kind_id: str, qty: int = 1) -> Dict[str, Any]:
    """
    Продажа игроком предметов рынку/NPC.
    - Цена выкупа = 50% от рыночной (в серебре), минимум 1
    - Начисляем серебро игроку
    - Увеличиваем stock у вендора
    """
    if qty <= 0:
        return {"ok": False, "error": "qty_must_be_positive"}

    market = await get_market_db(session, vendor_id)
    line = next((i for i in market["items"] if i["item_kind_id"] == item_kind_id), None)
    if not line:
        return {"ok": False, "error": "vendor_does_not_buy_this"}

    unit_market_silver = int(line["unit_price_silver"])
    unit_buyback_silver = max(1, int(round(unit_market_silver * 0.5)))
    total_silver = unit_buyback_silver * qty

    # начисляем деньги продавцу (игроку)
    await session.execute(text("UPDATE actors SET wallet = wallet + :a WHERE id=:id"), {"a": total_silver, "id": seller_id})

    # склад продавца (рынка/NPC) растёт
    await session.execute(text("""
        INSERT INTO vendor_inventory(vendor_id, item_kind_id, stock, price_mod)
        VALUES (:v, :k, :q, 1.0)
        ON CONFLICT (vendor_id, item_kind_id)
          DO UPDATE SET stock = vendor_inventory.stock + EXCLUDED.stock
    """), {"v": vendor_id, "k": item_kind_id, "q": qty})

    # лог
    stmt = text("""
        INSERT INTO world_trade_log(type, actor_id, vendor_id, item_kind_id, qty, amount, payload)
        VALUES ('TRADE_SELL', :actor, :vendor, :kind, :qty, :amt, :payload)
    """).bindparams(bindparam("payload", type_=PG_JSONB))

    await session.execute(stmt, {
        "actor": seller_id,
        "vendor": vendor_id,
        "kind": item_kind_id,
        "qty": qty,
        "amt": total_silver,  # сумма в серебре
        "payload": {"currency": "silver", "unit_buyback_silver": unit_buyback_silver, "total_silver": total_silver}
    })

    await session.commit()

    # пока физически предметы у игрока не списываем (подождём схему items)
    return {"ok": True, "total_silver": total_silver}
