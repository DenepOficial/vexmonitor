import asyncio
import io
import logging
import mimetypes
import os
import re
from collections import defaultdict
from typing import Any

import discord
from dotenv import load_dotenv

from database import Database

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_SCHEMA = os.getenv("DB_SCHEMA", "vex_monitor").strip()
ROUTE_REFRESH_SECONDS = max(10, int(os.getenv("ROUTE_REFRESH_SECONDS", "20")))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("vex_monitor")

if not DISCORD_TOKEN:
    raise RuntimeError("Falta la variable DISCORD_TOKEN.")

if not DATABASE_URL:
    raise RuntimeError("Falta la variable DATABASE_URL.")


def truncate(text: str | None, limit: int, fallback: str = "") -> str:
    value = (text or "").strip() or fallback
    return value if len(value) <= limit else value[: limit - 1] + "…"


def safe_filename(filename: str, index: int) -> str:
    value = filename.strip() or f"archivo_{index}"
    value = re.sub(r"[^A-Za-z0-9._()\- ]+", "_", value)
    return f"{index:02d}_{value}"[:180]


def attachment_kind(attachment: discord.Attachment) -> str:
    content_type = (attachment.content_type or "").lower()

    if content_type.startswith("image/"):
        return "image"
    if content_type.startswith("audio/"):
        return "audio"
    if content_type.startswith("video/"):
        return "video"

    guessed, _ = mimetypes.guess_type(attachment.filename)
    guessed = (guessed or "").lower()

    if guessed.startswith("image/"):
        return "image"
    if guessed.startswith("audio/"):
        return "audio"
    if guessed.startswith("video/"):
        return "video"

    return "file"


class MonitorClient(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.messages = True
        intents.message_content = True

        super().__init__(
            intents=intents,
            status=discord.Status.invisible,
            allowed_mentions=discord.AllowedMentions.none(),
            max_messages=1000,
        )

        self.db = Database(DATABASE_URL, DB_SCHEMA)

        # (source_guild_id, source_channel_id) -> lista de rutas
        self.routes: dict[tuple[int, int], list[dict[str, Any]]] = {}
        self.route_task: asyncio.Task | None = None

    async def setup_hook(self) -> None:
        await self.db.connect()
        await self.refresh_routes()

        self.route_task = asyncio.create_task(
            self.route_refresh_loop(),
            name="route_refresh_loop",
        )

    async def close(self) -> None:
        if self.route_task:
            self.route_task.cancel()

        await self.db.close()
        await super().close()

    async def on_ready(self) -> None:
        log.info(
            "Conectado como %s (%s) | invisible=SI | rutas=%s",
            self.user,
            self.user.id if self.user else "?",
            sum(len(v) for v in self.routes.values()),
        )

    async def route_refresh_loop(self) -> None:
        while not self.is_closed():
            try:
                await asyncio.sleep(ROUTE_REFRESH_SECONDS)
                await self.refresh_routes()

            except asyncio.CancelledError:
                return

            except Exception:
                log.exception(
                    "No se pudieron actualizar las rutas. "
                    "Se mantienen las últimas rutas válidas."
                )

    async def refresh_routes(self) -> None:
        rows = await self.db.get_active_routes()

        new_routes: dict[
            tuple[int, int],
            list[dict[str, Any]]
        ] = defaultdict(list)

        for route in rows:
            key = (
                int(route["source_guild_id"]),
                int(route["source_channel_id"]),
            )
            new_routes[key].append(route)

        self.routes = dict(new_routes)

        log.info(
            "Rutas actualizadas: %s canales origen / %s rutas",
            len(self.routes),
            sum(len(v) for v in self.routes.values()),
        )

    async def on_message(self, message: discord.Message) -> None:
        if message.guild is None:
            return

        # Evita que el propio bot se replique a sí mismo.
        if self.user and message.author.id == self.user.id:
            return

        routes = self.routes.get(
            (message.guild.id, message.channel.id)
        )

        if not routes:
            return

        for route in routes:
            target_guild_id = int(route["target_guild_id"])
            target_channel_id = int(route["target_channel_id"])

            # Protección contra rutas circulares exactas.
            if (
                message.guild.id == target_guild_id
                and message.channel.id == target_channel_id
            ):
                continue

            route_id = int(route["id"])

            claimed = await self.db.claim_message(
                route_id=route_id,
                source_message_id=message.id,
                source_channel_id=message.channel.id,
            )

            if not claimed:
                continue

            try:
                sent = await self.forward_message(
                    message=message,
                    route=route,
                )

                await self.db.mark_sent(
                    route_id=route_id,
                    source_message_id=message.id,
                    target_message_id=sent.id,
                )

            except Exception as exc:
                log.exception(
                    "Error procesando ruta %s / mensaje %s",
                    route_id,
                    message.id,
                )

                await self.db.mark_failed(
                    route_id=route_id,
                    source_message_id=message.id,
                    error_text=f"{type(exc).__name__}: {exc}",
                )

    async def get_target_channel(
        self,
        guild_id: int,
        channel_id: int,
    ) -> discord.abc.Messageable:
        guild = self.get_guild(guild_id)

        if guild is None:
            raise RuntimeError(
                f"El bot no está en el servidor destino {guild_id}."
            )

        channel = guild.get_channel_or_thread(channel_id)

        if channel is None:
            channel = await self.fetch_channel(channel_id)

        if not isinstance(channel, discord.abc.Messageable):
            raise RuntimeError(
                f"El canal destino {channel_id} no admite mensajes."
            )

        return channel

    async def resolve_reference(
        self,
        message: discord.Message,
    ) -> discord.Message | None:
        reference = message.reference

        if reference is None or reference.message_id is None:
            return None

        if isinstance(reference.resolved, discord.Message):
            return reference.resolved

        channel_id = reference.channel_id or message.channel.id
        channel = self.get_channel(channel_id)

        if channel is None:
            try:
                channel = await self.fetch_channel(channel_id)
            except discord.DiscordException:
                return None

        if not isinstance(channel, discord.abc.Messageable):
            return None

        try:
            return await channel.fetch_message(reference.message_id)

        except (
            discord.NotFound,
            discord.Forbidden,
            discord.HTTPException,
        ):
            return None

    async def build_payload(
        self,
        message: discord.Message,
        target_guild: discord.Guild,
        route: dict[str, Any],
    ) -> tuple[list[discord.Embed], list[discord.File]]:
        guild_name = message.guild.name
        channel_name = getattr(
            message.channel,
            "name",
            str(message.channel.id),
        )

        content = message.content.strip()

        if not content:
            if message.attachments:
                content = "*Mensaje sin texto; contiene adjuntos.*"
            elif message.stickers:
                content = "*Mensaje con sticker.*"
            else:
                content = "*Sin contenido de texto visible.*"

        embed = discord.Embed(
            title="Mensaje monitoreado",
            description=truncate(content, 3500),
            timestamp=message.created_at,
        )

        display_name = getattr(
            message.author,
            "display_name",
            message.author.name,
        )

        embed.set_author(
            name=truncate(
                f"{display_name} (@{message.author.name})",
                256,
            ),
            icon_url=message.author.display_avatar.url,
        )

        embed.add_field(
            name="Servidor",
            value=f"{guild_name}\n`{message.guild.id}`",
            inline=True,
        )

        embed.add_field(
            name="Canal",
            value=f"#{channel_name}\n`{message.channel.id}`",
            inline=True,
        )

        embed.add_field(
            name="Usuario",
            value=f"{message.author.mention}\n`{message.author.id}`",
            inline=True,
        )

        referenced = await self.resolve_reference(message)

        if message.reference:
            if referenced:
                ref_author = getattr(
                    referenced.author,
                    "display_name",
                    referenced.author.name,
                )

                ref_text = truncate(
                    referenced.content,
                    500,
                    "*Sin texto visible*",
                )

                ref_value = (
                    f"**{ref_author}** "
                    f"(`{referenced.author.id}`)\n"
                    f"{ref_text}\n"
                    f"[Abrir mensaje respondido]"
                    f"({referenced.jump_url})"
                )

            else:
                ref_value = (
                    "No pude recuperar el mensaje respondido.\n"
                    f"ID: `{message.reference.message_id}`"
                )

            embed.add_field(
                name="↩️ En respuesta a",
                value=truncate(ref_value, 700),
                inline=False,
            )

        if message.stickers:
            sticker_text = "\n".join(
                f"• {sticker.name} — {sticker.url}"
                for sticker in message.stickers[:5]
            )

            embed.add_field(
                name="Stickers",
                value=truncate(sticker_text, 700),
                inline=False,
            )

        embed.add_field(
            name="Mensaje original",
            value=f"[Abrir en Discord]({message.jump_url})",
            inline=False,
        )

        footer = f"Mensaje ID: {message.id}"

        if route.get("route_label"):
            footer += f" • {route['route_label']}"

        embed.set_footer(
            text=truncate(footer, 2048)
        )

        files: list[discord.File] = []
        image_urls: list[str] = []
        attachment_lines: list[str] = []

        upload_limit = target_guild.filesize_limit

        for index, attachment in enumerate(
            message.attachments[:10],
            start=1,
        ):
            kind = attachment_kind(attachment)

            icon = {
                "image": "🖼️",
                "audio": "🎵",
                "video": "🎬",
                "file": "📎",
            }[kind]

            size_mb = attachment.size / 1024 / 1024
            content_type = (
                attachment.content_type or "desconocido"
            )

            if attachment.size > upload_limit:
                attachment_lines.append(
                    f"{icon} **{attachment.filename}** "
                    f"({size_mb:.2f} MB) — "
                    f"[abrir original]({attachment.url})"
                )
                continue

            try:
                raw = await attachment.read(use_cached=True)

                discord_file = discord.File(
                    io.BytesIO(raw),
                    filename=safe_filename(
                        attachment.filename,
                        index,
                    ),
                    spoiler=attachment.is_spoiler(),
                )

                files.append(discord_file)

                attachment_lines.append(
                    f"{icon} **{attachment.filename}** "
                    f"({size_mb:.2f} MB, {content_type})"
                )

                if kind == "image":
                    image_urls.append(
                        f"attachment://{discord_file.filename}"
                    )

            except (
                discord.HTTPException,
                OSError,
            ):
                attachment_lines.append(
                    f"{icon} **{attachment.filename}** — "
                    f"[abrir original]({attachment.url})"
                )

        if attachment_lines:
            embed.add_field(
                name="Adjuntos",
                value=truncate(
                    "\n".join(attachment_lines),
                    900,
                ),
                inline=False,
            )

        embeds = [embed]

        if image_urls:
            embed.set_image(url=image_urls[0])

            for image_url in image_urls[1:10]:
                extra_embed = discord.Embed()
                extra_embed.set_image(url=image_url)
                embeds.append(extra_embed)

        return embeds[:10], files[:10]

    async def forward_message(
        self,
        message: discord.Message,
        route: dict[str, Any],
    ) -> discord.Message:
        target_guild_id = int(route["target_guild_id"])
        target_channel_id = int(route["target_channel_id"])

        target_guild = self.get_guild(target_guild_id)

        if target_guild is None:
            raise RuntimeError(
                f"No encuentro el servidor destino "
                f"{target_guild_id}."
            )

        target_channel = await self.get_target_channel(
            guild_id=target_guild_id,
            channel_id=target_channel_id,
        )

        embeds, files = await self.build_payload(
            message=message,
            target_guild=target_guild,
            route=route,
        )

        return await target_channel.send(
            embeds=embeds,
            files=files,
            allowed_mentions=discord.AllowedMentions.none(),
            silent=True,
        )


client = MonitorClient()

if __name__ == "__main__":
    client.run(
        DISCORD_TOKEN,
        log_handler=None,
    )
