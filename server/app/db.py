# server/app/db.py
import os
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import make_url

DATABASE_URL = os.getenv("DATABASE_URL", "")

# Автофикс: если кто-то снова поставит sslmode=require при +asyncpg
if DATABASE_URL and "+asyncpg" in DATABASE_URL and "sslmode=" in DATABASE_URL:
    # аккуратно перепакуем query часть
    url = make_url(DATABASE_URL)
    q = dict(url.query)
    q.pop("sslmode", None)
    q["ssl"] = "true"
    url = url.set(query=q)
    DATABASE_URL = str(url)

# (необязательно) отладочный print — увидишь в Render Logs текущий URL
print("📡 DB URL =", DATABASE_URL)

engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def get_session() -> AsyncSession:
    async with async_session() as s:
        yield s
