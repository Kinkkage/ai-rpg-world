# server/app/db.py
import os
import ssl
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ArgumentError

# ────────────────────────────────────────────────
# 1️⃣ Загружаем переменные окружения
# ────────────────────────────────────────────────
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    # Безопасная проверка — чтобы сразу видно было, что .env не найден
    raise RuntimeError(
        "❌ Не задана переменная окружения DATABASE_URL.\n"
        "Создай файл `.env` в корне проекта (рядом с папкой app) и добавь строку, например:\n"
        "DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/ai_rpg_world"
    )

# ────────────────────────────────────────────────
# 2️⃣ Настройка SSL (если проект работает на Render или в облаке)
# ────────────────────────────────────────────────
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# ────────────────────────────────────────────────
# 3️⃣ Создаём движок и фабрику сессий
# ────────────────────────────────────────────────
try:
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
        connect_args={"ssl": ssl_context},
    )
except ArgumentError as e:
    raise RuntimeError(f"Ошибка в строке подключения DATABASE_URL: {e}")

async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


# ────────────────────────────────────────────────
# 4️⃣ Функция получения асинхронной сессии
# ────────────────────────────────────────────────
async def get_session() -> AsyncSession:
    async with async_session() as s:
        yield s
