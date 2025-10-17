import os
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# Перекрываем всё окружение валидным значением
os.environ["PGSSLMODE"] = "require"

DATABASE_URL = os.getenv("DATABASE_URL", "")

# Переключаем драйвер на asyncpg
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# Чистим sslmode и добавляем ssl=true
if DATABASE_URL:
    u = urlparse(DATABASE_URL)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    q.pop("sslmode", None)  # убрать всегда
    q["ssl"] = "true"       # формат, который понимает asyncpg
    DATABASE_URL = urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q), u.fragment))

def _mask(url: str) -> str:
    try:
        before_at, after_at = url.split("@", 1)
        if ":" in before_at:
            u, _p = before_at.rsplit(":", 1)
            before_at = u + ":********"
        return before_at + "@" + after_at
    except Exception:
        return url

print("📡 DB URL used by app =", _mask(DATABASE_URL))
print("🔐 PGSSLMODE =", os.getenv("PGSSLMODE"))

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    connect_args={"ssl": "require"},  # доп. гарантия
)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def get_session() -> AsyncSession:
    async with async_session() as s:
        yield s
