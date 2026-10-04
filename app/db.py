"""Postgres access via asyncpg."""

from pathlib import Path

import asyncpg

_pool: asyncpg.Pool | None = None

_MIGRATION = Path(__file__).resolve().parent.parent / "migrations" / "001_init.sql"


async def connect(database_url: str) -> None:
    global _pool
    _pool = await asyncpg.create_pool(database_url, min_size=1, max_size=10)
    async with _pool.acquire() as connection:
        await _apply_migration(connection)


async def disconnect() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("database is not connected")
    return _pool


async def _apply_migration(connection: asyncpg.Connection) -> None:
    exists = await connection.fetchval("SELECT to_regclass('public.shows')")
    if exists is not None:
        return
    script = _MIGRATION.read_text(encoding="utf-8")
    async with connection.transaction():
        for statement in script.split(";"):
            sql = statement.strip()
            if sql:
                await connection.execute(sql)
