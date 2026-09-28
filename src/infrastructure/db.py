import os
import json
from contextlib import asynccontextmanager
import asyncpg

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5434"))
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgrespassword")
PG_DATABASE = os.getenv("PG_DATABASE", "auction_db")

pool: asyncpg.Pool | None = None


async def _init_connection(conn: asyncpg.Connection):
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def get_db_pool() -> asyncpg.Pool:
    global pool
    if pool is None:
        pool = await asyncpg.create_pool(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            database=PG_DATABASE,
            min_size=2,
            max_size=10,
            init=_init_connection,
        )
    return pool


async def close_db_pool():
    global pool
    if pool is not None:
        await pool.close()
        pool = None


@asynccontextmanager
async def transaction():
    db = await get_db_pool()
    async with db.acquire() as conn:
        async with conn.transaction() as tr:
            yield conn, tr
