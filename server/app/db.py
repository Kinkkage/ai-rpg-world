# server/app/db.py
import os
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import make_url

DATABASE_URL = os.getenv("DATABASE_URL", "")

# Авто-нормализация для asyncpg:
# если кто-то случайно оставил ?sslmode=require — заменим на ?ssl=true
if DATABASE_URL:
    try:
        url = make_url(DATABASE_URL)
        if "+asyncpg" in url.drivername:
            q = dict(url.query)
            if "sslmode" in q:      # psycopg-параметр → ломает asyncpg
                q.pop("sslmode", None)
            # если ssl не задан — включим
            q.setdefault("ssl", "true")
            url = url.set(query=q)
            DATABASE_URL = str(url)
    except Exception:
        # В крайнем случае грубой заменой
        DATABASE_URL = DATABASE_URL.replace("sslmode=require", "ssl=true")

print("📡 DB URL used by app =", DATABASE_URL)

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def get_session() -> AsyncSession:
    async with async_session() as s:
        yield s
