# server/app/db.py
import os
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL", "")

# 1) Гарантируем драйвер asyncpg
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# 2) Насильно УДАЛЯЕМ sslmode И включаем ssl=true
if DATABASE_URL:
    u = urlparse(DATABASE_URL)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    q.pop("sslmode", None)      # вырезаем всё, что могло прийти из ENV
    q["ssl"] = "true"           # asyncpg-понятная форма
    DATABASE_URL = urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q), u.fragment))

print("📡 DB URL used by app =", DATABASE_URL)  # проверь в логах, тут НЕ должно быть sslmode

engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def get_session() -> AsyncSession:
    async with async_session() as s:
        yield s
