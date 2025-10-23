# app/routers/inventory.py
from fastapi import APIRouter, Depends
from pydantic import BaseModel, validator
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.dao import (
    fetch_inventory,
    reload_weapon_db,
    equip_item_db,
    unequip_item_db,
    hold_bag_db,
    equip_backpack_db,
    unequip_backpack_db,
)

router = APIRouter(prefix="/inventory", tags=["inventory"])


# ==================== MODELS ====================

class ReloadRequest(BaseModel):
    actor_id: str
    hand: str = "right"

    @validator("hand")
    def _check_hand(cls, v: str):
        v = v.lower()
        if v not in ("left", "right"):
            raise ValueError("hand must be 'left' or 'right'")
        return v


# ==================== FETCH ====================

@router.get("/{actor_id}")
async def api_get_inventory(actor_id: str, session: AsyncSession = Depends(get_session)):
    """
    Возвращает структуру инвентаря актёра.
    """
    inv = await fetch_inventory(session, actor_id)
    return {"ok": True, "inventory": inv}


# ==================== RELOAD ====================

@router.post("/reload")
async def reload_weapon(req: ReloadRequest, session: AsyncSession = Depends(get_session)):
    """
    Перезарядка оружия из руки актёра (left/right) за счёт патронов в рюкзаке.
    """
    inv = await fetch_inventory(session, req.actor_id)
    hand_info = inv.get(f"{req.hand}_hand") or {}
    item = hand_info.get("item")
    if not item or not item.get("id"):
        return {"ok": False, "error": "no_weapon_in_hand"}

    res = await reload_weapon_db(session, req.actor_id, item["id"])
    return res


# ==================== EQUIP / UNEQUIP ====================

@router.post("/{actor_id}/equip/{hand}/{item_id}")
async def api_equip_item(actor_id: str, hand: str, item_id: str, session: AsyncSession = Depends(get_session)):
    """
    Взять предмет в руку (left/right).
    """
    result = await equip_item_db(session, actor_id, hand, item_id)
    return {"ok": True, "events": result}


@router.post("/{actor_id}/unequip/{hand}")
async def api_unequip_item(actor_id: str, hand: str, session: AsyncSession = Depends(get_session)):
    """
    Убрать предмет из руки в рюкзак.
    """
    result = await unequip_item_db(session, actor_id, hand)
    return {"ok": True, "events": result}


# ==================== BAG / BACKPACK ====================

@router.post("/{actor_id}/hold_bag/{hand}/{item_id}")
async def api_hold_bag(actor_id: str, hand: str, item_id: str, session: AsyncSession = Depends(get_session)):
    """
    Взять мешок (контейнер) в руку.
    """
    result = await hold_bag_db(session, actor_id, item_id, hand)
    return result


@router.post("/{actor_id}/equip_backpack/{item_id}")
async def api_equip_backpack(actor_id: str, item_id: str, session: AsyncSession = Depends(get_session)):
    """
    Надеть рюкзак (контейнер) на спину.
    """
    result = await equip_backpack_db(session, actor_id, item_id)
    return result


@router.post("/{actor_id}/unequip_backpack")
async def api_unequip_backpack(actor_id: str, session: AsyncSession = Depends(get_session)):
    """
    Снять рюкзак.
    """
    result = await unequip_backpack_db(session, actor_id)
    return result
