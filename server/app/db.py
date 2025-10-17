# server/app/db.py
import os
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import make_url

DATABASE_URL = os.getenv("DATABASE_URL", "")

# --- автофикс для asyncpg ---
if "sslmode" in DATABASE_URL and "+asyncpg" in DATABASE_URL:
    # заменим sslmode=require на ssl=true
    DATABASE_URL = DATABASE_URL.replace("sslmode=require", "ssl=true")

# --- отладка: можно напечатать ---
print("📡 Using DB URL:", DATABASE_URL)

engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def get_session() -> AsyncSession:
    async with async_session() as s:
        yield s
