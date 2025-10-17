# server/app/db.py
import os
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import make_url

DATABASE_URL = os.getenv("DATABASE_URL", "")

if DATABASE_URL:
    try:
        url = make_url(DATABASE_URL)
        if "+asyncpg" in url.drivername:
            q = dict(url.query)
            # если sslmode не из допустимых — удалим
            valid = {"disable","allow","prefer","require","verify-ca","verify-full"}
            val = q.get("sslmode")
            if val is not None and val not in valid:
                q.pop("sslmode", None)
            # если ни ssl, ни корректного sslmode — включим ssl=true
            if "ssl" not in q and ("sslmode" not in q or q.get("sslmode") not in valid):
                q["ssl"] = "true"
            url = url.set(query=q)
            DATABASE_URL = str(url)
    except Exception:
        # запасной путь: убираем кривой sslmode
        DATABASE_URL = DATABASE_URL.replace("sslmode=true", "").replace("sslmode=False", "")

print("📡 DB URL used by app =", DATABASE_URL)

engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def get_session() -> AsyncSession:
    async with async_session() as s:
        yield s

