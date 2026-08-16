import logging
import os
import re
from typing import Any

import asyncpg
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger("vex_monitor.database")

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class Database:
    def __init__(self, database_url: str, schema: str = "vex_monitor") -> None:
        if not database_url:
            raise RuntimeError("Falta DATABASE_URL.")
        if not _SAFE_IDENTIFIER.fullmatch(schema):
            raise ValueError("DB_SCHEMA contiene caracteres no válidos.")

        self.database_url = database_url
        self.schema = schema
        self.pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(
            dsn=self.database_url,
            min_size=1,
            max_size=5,
            command_timeout=30,
        )
        await self.create_schema()
        await self.migrate_legacy_routes()
        log.info("PostgreSQL conectado. Schema=%s", self.schema)

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def create_schema(self) -> None:
        if self.pool is None:
            raise RuntimeError("Base de datos no conectada.")

        s = self.schema
        sql = f"""
        CREATE SCHEMA IF NOT EXISTS {s};

        CREATE TABLE IF NOT EXISTS {s}.monitor_routes (
            id BIGSERIAL PRIMARY KEY,
            source_guild_id BIGINT NOT NULL,
            source_channel_id BIGINT NOT NULL,
            target_guild_id BIGINT NOT NULL,
            target_channel_id BIGINT NOT NULL,
            route_label TEXT,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (
                source_guild_id,
                source_channel_id,
                target_guild_id,
                target_channel_id
            )
        );

        CREATE TABLE IF NOT EXISTS {s}.mirror_log (
            id BIGSERIAL PRIMARY KEY,
            route_id BIGINT NOT NULL
                REFERENCES {s}.monitor_routes(id)
                ON DELETE CASCADE,
            source_message_id BIGINT NOT NULL,
            source_channel_id BIGINT NOT NULL,
            target_message_id BIGINT,
            status TEXT NOT NULL DEFAULT 'processing'
                CHECK (status IN ('processing', 'sent', 'failed')),
            error_text TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (route_id, source_message_id)
        );

        CREATE INDEX IF NOT EXISTS idx_monitor_routes_source
            ON {s}.monitor_routes(source_guild_id, source_channel_id)
            WHERE enabled = TRUE;

        CREATE INDEX IF NOT EXISTS idx_monitor_routes_target
            ON {s}.monitor_routes(target_guild_id, target_channel_id)
            WHERE enabled = TRUE;

        CREATE INDEX IF NOT EXISTS idx_mirror_log_created
            ON {s}.mirror_log(created_at DESC);
        """
        async with self.pool.acquire() as conn:
            await conn.execute(sql)

    async def migrate_legacy_routes(self) -> None:
        """
        Si existe la versión anterior:
          monitor_guilds + monitored_channels
        copia sus rutas al nuevo monitor_routes sin borrar nada.
        """
        if self.pool is None:
            return

        s = self.schema
        async with self.pool.acquire() as conn:
            old_guilds = await conn.fetchval(
                "SELECT to_regclass($1)",
                f"{s}.monitor_guilds",
            )
            old_channels = await conn.fetchval(
                "SELECT to_regclass($1)",
                f"{s}.monitored_channels",
            )

            if not old_guilds or not old_channels:
                return

            sql = f"""
            INSERT INTO {s}.monitor_routes (
                source_guild_id,
                source_channel_id,
                target_guild_id,
                target_channel_id,
                route_label,
                enabled
            )
            SELECT
                mg.source_guild_id,
                mc.source_channel_id,
                mg.target_guild_id,
                mg.target_channel_id,
                mc.channel_label,
                (mg.enabled AND mc.enabled)
            FROM {s}.monitor_guilds AS mg
            JOIN {s}.monitored_channels AS mc
              ON mc.monitor_guild_id = mg.id
            ON CONFLICT (
                source_guild_id,
                source_channel_id,
                target_guild_id,
                target_channel_id
            ) DO NOTHING;
            """
            result = await conn.execute(sql)
            log.info("Migración compatible con schema anterior: %s", result)

    async def get_active_routes(self) -> list[dict[str, Any]]:
        if self.pool is None:
            raise RuntimeError("Base de datos no conectada.")

        s = self.schema
        sql = f"""
        SELECT
            id,
            source_guild_id,
            source_channel_id,
            target_guild_id,
            target_channel_id,
            route_label
        FROM {s}.monitor_routes
        WHERE enabled = TRUE
        ORDER BY id;
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql)
        return [dict(r) for r in rows]

    async def claim_message(
        self,
        route_id: int,
        source_message_id: int,
        source_channel_id: int,
    ) -> bool:
        if self.pool is None:
            raise RuntimeError("Base de datos no conectada.")

        s = self.schema
        sql = f"""
        INSERT INTO {s}.mirror_log (
            route_id,
            source_message_id,
            source_channel_id,
            status
        )
        VALUES ($1, $2, $3, 'processing')
        ON CONFLICT (route_id, source_message_id) DO NOTHING
        RETURNING id;
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                sql, route_id, source_message_id, source_channel_id
            )
        return row is not None

    async def mark_sent(
        self,
        route_id: int,
        source_message_id: int,
        target_message_id: int,
    ) -> None:
        if self.pool is None:
            return

        s = self.schema
        sql = f"""
        UPDATE {s}.mirror_log
        SET status='sent',
            target_message_id=$3,
            error_text=NULL,
            updated_at=NOW()
        WHERE route_id=$1 AND source_message_id=$2;
        """
        async with self.pool.acquire() as conn:
            await conn.execute(
                sql, route_id, source_message_id, target_message_id
            )

    async def mark_failed(
        self,
        route_id: int,
        source_message_id: int,
        error_text: str,
    ) -> None:
        if self.pool is None:
            return

        s = self.schema
        sql = f"""
        UPDATE {s}.mirror_log
        SET status='failed',
            error_text=$3,
            updated_at=NOW()
        WHERE route_id=$1 AND source_message_id=$2;
        """
        async with self.pool.acquire() as conn:
            await conn.execute(
                sql, route_id, source_message_id, error_text[:4000]
            )
