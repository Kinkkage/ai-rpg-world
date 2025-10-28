# server/app/routers/trade.py
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.services.dao_trade import (
    get_market_db, buy_item_db, sell_item_db, transfer_money_db
)

router = APIRouter(prefix="/world", tags=["trade"])

# ===== Schemas =================================================================

class BuyReq(BaseModel):
    buyer_id: str
    vendor_id: str
    item_kind_id: str
    qty: int = 1

class SellReq(BaseModel):
    seller_id: str
    vendor_id: str
    item_kind_id: str
    qty: int = 1

class TransferReq(BaseModel):
    from_actor: str
    to_actor: str
    amount_silver: int  # <-- теперь серебро (целое число)

# ===== Endpoints ================================================================

@router.get("/market/{vendor_id}")
async def market_view(vendor_id: str, session: AsyncSession = Depends(get_session)):
    try:
        data = await get_market_db(session, vendor_id)
        return {"ok": True, **data}
    except ValueError as e:
        raise HTTPException(404, detail=str(e))

@router.post("/trade/buy")
async def trade_buy(req: BuyReq, session: AsyncSession = Depends(get_session)):
    res = await buy_item_db(session, req.buyer_id, req.vendor_id, req.item_kind_id, req.qty)
    if not res.get("ok"):
        raise HTTPException(400, detail=res)
    return res

@router.post("/trade/sell")
async def trade_sell(req: SellReq, session: AsyncSession = Depends(get_session)):
    res = await sell_item_db(session, req.seller_id, req.vendor_id, req.item_kind_id, req.qty)
    if not res.get("ok"):
        raise HTTPException(400, detail=res)
    return res

@router.post("/trade/transfer_money")
async def trade_transfer(req: TransferReq, session: AsyncSession = Depends(get_session)):
    # передаём серебро в DAO
    res = await transfer_money_db(session, req.from_actor, req.to_actor, req.amount_silver)
    if not res.get("ok"):
        raise HTTPException(400, detail=res)
    return res
