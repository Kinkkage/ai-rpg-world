from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.routers.do import _build_actor_context  # переиспользуем уже готовую функцию


router = APIRouter(prefix="/debug", tags=["debug"])  # общий префикс /debug


@router.get("/actor/{actor_id}")
async def debug_actor_state(
    actor_id: str,
    session_id: str,
    session: AsyncSession = Depends(get_session),
):
    """
    Вернуть полный контекст актёра (такой же, как получает LLM):
    stats + meta + inventory + skills + statuses.

    session_id нужен, потому что навыки/статусы зависят от боевой сессии.
    """
    try:
        ctx = await _build_actor_context(session, actor_id, session_id)
    except HTTPException as e:
        # просто пробрасываем 404, если актёр не найден
        raise e

    return {
        "actor_id": actor_id,
        "session_id": session_id,
        "context": ctx,
    }
