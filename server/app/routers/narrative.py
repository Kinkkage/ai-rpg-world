# server/app/routers/narrative.py
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import os
import math
import json

from app.db import get_session

router = APIRouter()

# ---------- Models ----------
class NarrateIn(BaseModel):
    node_id: str
    style_id: Optional[str] = None   # если не передали — выберем "default"
    events: List[Dict[str, Any]]
    context: Dict[str, Any] = {}

class NarrateOut(BaseModel):
    text: str
    source: str  # "llm" | "template"

# ---------- Helpers ----------
def _estimate_tokens_from_events(evs: List[Dict[str, Any]]) -> int:
    """
    Грубая оценка токенов на вход: сериализуем в JSON и считаем символы.
    1 токен ~= 4 символам.
    """
    try:
        s = json.dumps(evs, ensure_ascii=False)
    except Exception:
        s = str(evs)
    return max(1, math.ceil(len(s) / 4))

def _estimate_tokens_from_text(s: str) -> int:
    s = s or ""
    return max(1, math.ceil(len(s) / 4))

async def _get_style_cfg(session: AsyncSession, style_id: str) -> Dict[str, Any]:
    row = (await session.execute(
        text("select config from narrative_styles where id=:sid"),
        {"sid": style_id}
    )).mappings().first()
    return (row and row["config"]) or {"tone": "neutral", "max_chars": 240, "persona": "narrator"}

async def _choose_style(session: AsyncSession, requested: Optional[str]) -> str:
    if requested:
        # проверим, что стиль есть
        ok = (await session.execute(
            text("select 1 from narrative_styles where id=:sid"),
            {"sid": requested}
        )).first()
        if ok:
            return requested
    return "default"

async def _spent_last_hour(session: AsyncSession) -> Dict[str, int]:
    row = (await session.execute(text("""
        select
          coalesce(sum(tokens_in), 0)  as tin,
          coalesce(sum(tokens_out), 0) as tout
        from narrative_logs
        where ts > now() - interval '1 hour'
          and source = 'llm'
    """))).mappings().first()
    return {"in": int(row["tin"]), "out": int(row["tout"])}

def _pricing() -> Dict[str, float]:
    # цены за 1k токенов
    pin = float(os.getenv("NARRATIVE_PRICE_IN_PER_1K", "0.50"))
    pout = float(os.getenv("NARRATIVE_PRICE_OUT_PER_1K", "1.50"))
    return {"in": pin, "out": pout}

def _limits() -> Dict[str, float]:
    max_t = int(os.getenv("NARRATIVE_MAX_TOKENS_PER_HOUR", "50000"))
    max_cents = float(os.getenv("NARRATIVE_MAX_CENTS_PER_HOUR", "100"))
    return {"tokens_per_hour": max_t, "cents_per_hour": max_cents}

def _can_use_llm() -> bool:
    return os.getenv("NARRATIVE_ENABLE_LLM", "true").lower() == "true" and bool(os.getenv("OPENAI_API_KEY"))

def _template_fallback(events, style_cfg) -> str:
    # Простая эвристика на случай фолбэка (заменим позже на Jinja2/словарь)
    for e in events:
        if e.get("type") == "ATTACK" and (e.get("payload") or {}).get("result") == "hit":
            return "Вы наносите точный удар — противник пошатнулся."
    return "События развиваются размеренно. Ничего необычного."

def _cents_for_tokens(tokens_in: int, tokens_out: int) -> float:
    price = _pricing()
    return (tokens_in / 1000.0) * price["in"] * 100.0 + (tokens_out / 1000.0) * price["out"] * 100.0
    # возвращаем центы

# ---------- Endpoints ----------
@router.post("/narrate", response_model=NarrateOut)
async def narrate(body: NarrateIn, session: AsyncSession = Depends(get_session)):
    style_id = await _choose_style(session, body.style_id)
    style_cfg = await _get_style_cfg(session, style_id)

    # оценки токенов
    est_in = _estimate_tokens_from_events(body.events)
    # пока текст не знаем; после генерации оценим est_out

    use_llm = _can_use_llm()
    src = "template"
    text_out: Optional[str] = None

    if use_llm:
        # проверим лимиты по прошлому часу
        spent = await _spent_last_hour(session)
        limits = _limits()
        # токенный лимит
        if (spent["in"] + est_in) > limits["tokens_per_hour"]:
            use_llm = False
        else:
            # денежный лимит — грубо оценим будущие затраты (берём небольшой буфер)
            # допустим, ожидаем, что ответ будет не длиннее style_cfg.max_chars
            max_chars = int(style_cfg.get("max_chars", 240))
            est_out = _estimate_tokens_from_text("x" * max_chars)
            projected_cents = _cents_for_tokens(spent["in"] + est_in, spent["out"] + est_out)
            if projected_cents > limits["cents_per_hour"]:
                use_llm = False

    if use_llm:
        try:
            # Здесь должен быть реальный вызов твоей LLM-обёртки.
            # Мы оставляем заглушку, чтобы не делать внешних вызовов прямо сейчас.
            # Примерно:
            # text_out = await call_llm(events=body.events, style=style_cfg, context=body.context)
            # Для следа: пометить как llm
            pass
        except Exception:
            text_out = None

    if not text_out:
        text_out = _template_fallback(body.events, style_cfg)

    # оценим «выходные» токены по длине текста
    est_out_final = _estimate_tokens_from_text(text_out)

    # лог
    await session.execute(text("""
      insert into narrative_logs(node_id, raw_events, style_id, output_text, source, tokens_in, tokens_out)
      values(:nid, :evs, :sid, :txt, :src, :tin, :tout)
    """), {
        "nid": body.node_id,
        "evs": body.events,
        "sid": style_id,
        "txt": text_out,
        "src": "llm" if _can_use_llm() and text_out and use_llm else "template",
        "tin": est_in,
        "tout": est_out_final
    })
    await session.commit()

    return {"text": text_out, "source": "llm" if use_llm else "template"}

# ---- Admin: стили ----
@router.get("/narrative/styles")
async def list_styles(session: AsyncSession = Depends(get_session)):
    rows = (await session.execute(text("""
        select id, title, config from narrative_styles order by id
    """))).mappings().all()
    return [dict(r) for r in rows]

# ---- Admin: последние логи ----
@router.get("/narrative/logs")
async def recent_logs(
    limit: int = Query(20, ge=1, le=200),
    session: AsyncSession = Depends(get_session)
):
    rows = (await session.execute(text(f"""
        select ts, node_id, style_id, source, tokens_in, tokens_out, left(output_text, 240) as preview
        from narrative_logs
        order by ts desc
        limit :lim
    """)), {"lim": limit}).mappings().all()
    return [dict(r) for r in rows]
