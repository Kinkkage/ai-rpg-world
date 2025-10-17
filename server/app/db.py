# server/app/db.py
import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# локально читаем .env; на Render переменные уже в окружении
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")  # postgresql+asyncpg://...

engine = create_async_engine(
    DATABASE_URL, echo=False, pool_pre_ping=True
)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def get_session() -> AsyncSession:
    async with async_session() as s:
        yield s
