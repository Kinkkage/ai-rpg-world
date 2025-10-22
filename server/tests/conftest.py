# tests/conftest.py
import os
import asyncio
import pytest_asyncio
from typing import AsyncGenerator

# На Windows — стабильная политика лупа для asyncpg
if os.name == "nt":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://ai_rpg_world_db_user:b6YEB7op5ppUw6uIT7QBLV8ZAMUWxlMa@dpg-d3p8nhbipnbc739o20f0-a.frankfurt-postgres.render.com:5432/ai_rpg_world_db",
)

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy import text

# ВАЖНО: NullPool чтобы не переиспользовать коннекты между разными event loop
test_engine = create_async_engine(
    DATABASE_URL,
    future=True,
    echo=False,
    poolclass=NullPool,
    pool_pre_ping=True,
)
TestSessionLocal = async_sessionmaker(test_engine, expire_on_commit=False, class_=AsyncSession)

# Импортируем приложение и зависимость — будем оверрайдить get_session
from app.main import app
from app.db import get_session as app_get_session

# Выдаём ручкам FastAPI сессию из нашего тестового движка
async def override_get_session() -> AsyncGenerator[AsyncSession, None]:
    async with TestSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except:  # noqa
            await session.rollback()
            raise

@pytest_asyncio.fixture(autouse=True, scope="function")
async def _override_deps() -> AsyncGenerator[None, None]:
    app.dependency_overrides[app_get_session] = override_get_session
    try:
        yield
    finally:
        app.dependency_overrides.pop(app_get_session, None)

# HTTP-клиент (создаётся и живёт в том же event loop, что и тест)
from httpx import AsyncClient

@pytest_asyncio.fixture(scope="function")
async def client() -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(app=app, base_url="http://testserver") as ac:
        yield ac

# Подготовка данных — ОТКРЫВАЕМ СЕССИЮ ЗДЕСЬ, а не через отдельную фикстуру
@pytest_asyncio.fixture(scope="function")
async def ensure_player():
    async with TestSessionLocal() as session:
        # статусы
        await session.execute(text("""
            insert into statuses (id, title, tick_damage, is_positive)
            values
              ('burn',  'Горение', 2, false),
              ('guard', 'Защита',  0, true)
            on conflict (id) do nothing;
        """))
        # стиль
        await session.execute(text("""
            insert into narrative_styles (id, title, config)
            values (
              'status',
              'Статус-эффекты',
              jsonb_build_object(
                'tone','urgent',
                'max_chars', 180,
                'persona','battle_observer'
              )
            )
            on conflict (id) do nothing;
        """))
        # игрок
        await session.execute(text("""
            insert into actors (id, kind, node_id, x, y, hp, mood, trust, aggression)
            values ('player','player', NULL, 5, 5, 100, 'neutral', 50, 0)
            on conflict (id) do nothing;
        """))
        await session.commit()
        # ничего не возвращаем — просто гарантируем наличие данных
