import json
import os

import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

_pool: asyncpg.Pool | None = None

TABLE = "instapost_state"


async def init_pool():
    global _pool
    if _pool is not None:
        return _pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")

    _pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=1,
        max_size=2,
        ssl="require",
        command_timeout=30,
        max_inactive_connection_lifetime=60,
    )

    async with _pool.acquire() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id          INTEGER PRIMARY KEY,
                data        JSONB NOT NULL,
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await conn.execute(f"""
            INSERT INTO {TABLE} (id, data)
            VALUES (1, '{{}}'::jsonb)
            ON CONFLICT (id) DO NOTHING
        """)

    return _pool


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialized")
    return _pool


async def close_pool():
    global _pool
    if _pool is not None:
        try:
            await _pool.close()
        except Exception:
            pass
        _pool = None


async def read_state() -> dict:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f"SELECT data FROM {TABLE} WHERE id = 1")
    if not row:
        return {}
    data = row["data"]
    return json.loads(data) if isinstance(data, str) else dict(data)


async def write_state(state: dict) -> None:
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            UPDATE {TABLE}
            SET data = $1::jsonb, updated_at = NOW()
            WHERE id = 1
            """,
            json.dumps(state),
        )