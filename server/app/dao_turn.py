from typing import Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from app.dao_status import advance_statuses_db  # используем эффектный тик

async def advance_turn_db(session: AsyncSession) -> Dict[str, Any]:
    """
    Продвигаем ход через эффектный тик статусов (burn/bleed и т.д.).
    """
    res = await advance_statuses_db(session)
    # res уже включает commit и события
    return {"ok": True, **res}
