# server/app/db.py
import os
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from urllib.parse import urlencode

DATABASE_URL = os.getenv("DATABASE_URL", "")

# --- Правка SSL для Supabase ---
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

# Добавляем SSL, если его нет
if "?sslmode=" not in DATABASE_URL and "?ssl=" not in DATABASE_URL:
    sep = "&" if "?" in DATABASE_URL else "?"
    DATABASE_URL = f"{DATABASE_URL}{sep}{urlencode({'ssl': 'true'})}"

print("📡 DB URL used by app =", DATABASE_URL)

# --- SQLAlchemy async engine ---
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True
)

async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def get_session() -> AsyncSession:
    async with async_session() as s:
        yield s
