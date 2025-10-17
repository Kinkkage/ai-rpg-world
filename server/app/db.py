# server/app/db.py
import os
import socket
import ssl
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# 1) Нормализуем окружение
os.environ["PGSSLMODE"] = "require"  # перекроет любые странные значения
RAW_URL = os.getenv("DATABASE_URL", "").strip()

def _mask(url: str) -> str:
    try:
        u = urlparse(url)
        if "@" in u.netloc and ":" in u.netloc.split("@")[0]:
            creds, host = u.netloc.split("@", 1)
            user = creds.split(":", 1)[0]
            netloc = f"{user}:********@{host}"
            return urlunparse((u.scheme, netloc, u.path, u.params, u.query, u.fragment))
    except Exception:
        pass
    return url

# 2) Переключаем драйвер и чистим query-параметры
def normalize_dsn(url: str) -> str:
    if not url:
        return url
    # обязательно asyncpg
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)

    u = urlparse(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))

    # убираем любые sslmode/pgbouncer — они ломали asyncpg
    q.pop("sslmode", None)
    q.pop("pgbouncer", None)

    # добавляем флаг, который понимает asyncpg
    # (альтернатива — передать SSLContext через connect_args)
    q["ssl"] = "true"

    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q), u.fragment))

DATABASE_URL = normalize_dsn(RAW_URL)

print("📡 DB URL used by app   =", _mask(DATABASE_URL))
print("🔐 PGSSLMODE            =", os.getenv("PGSSLMODE"))

# 3) Дополнительно готовим строгий SSLContext (чтобы не зависеть от query)
ssl_ctx = ssl.create_default_context()               # проверка корневого CA включена
ssl_ctx.check_hostname = True                        # проверяем hostname
# ssl_ctx.verify_mode = ssl.CERT_REQUIRED            # по умолчанию REQUIRED в create_default_context

# 4) Создаём движок
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    connect_args={"ssl": ssl_ctx},   # «ремень и подтяжки»
)

async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def get_session() -> AsyncSession:
    async with async_session() as s:
        yield s

# 5) Вспомогательная диагностика DNS (для health-эндпоинта)
def resolve_db_host():
    try:
        u = urlparse(DATABASE_URL)
        host = u.hostname or ""
        ip = socket.gethostbyname(host) if host else ""
        return {"host": host, "ip": ip}
    except Exception as e:
        return {"error": str(e)}
