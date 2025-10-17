# server/app/db.py
import os
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

DATABASE_URL = os.getenv("DATABASE_URL", "")

# Преобразуем postgresql:// → postgresql+asyncpg://
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# Удаляем любой sslmode и включаем ssl=true
if DATABASE_URL:
    u = urlparse(DATABASE_URL)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    q.pop("sslmode", None)          # <- вырезаем sslmode всегда
    q.setdefault("ssl", "true")     # <- гарантируем ssl
    DATABASE_URL = urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q), u.fragment))

print("📡 DB URL used by app =", DATABASE_URL)

engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def get_session() -> AsyncSession:
    async with async_session() as s:
        yield s
