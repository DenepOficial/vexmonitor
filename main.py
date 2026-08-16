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
    raise RuntimeError("Falta DISCORD_TOKEN.")
if not DATABASE_URL:
    raise RuntimeError("Falta DATABASE_URL.")


def truncate(text: str | None, limit: int, fallback: str = "") -> str:
    value = (text or "").strip() or fallback
    return value if len(value) <= limit else value[: limit - 1] + "…"


def safe_filename(filename: str, index: int) -> str:
    value = filename.strip() or f"archivo_{index}"
    value = re.sub(r"[^A-Za-z0-9._()\- ]+", "_", value)
    return f"{index:02d}_{value}"[:180]


def attachment_kind(attachment: discord.Attachment) -> str:
    ctype = (attachment.content_type or "").lower()
    if ctype.startswith("image/"):
        return "image"
    if ctype.startswith("audio/"):
        return "audio"
    if ctype.startswith("video/"):
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
        self.routes: dict[tuple[int, int], list[dict[str, Any]]] = {}
        self.route_task: asyncio.Task | None = None

    async def setup_hook(self) -> None:
        await self.db.connect()
        await self.refresh_routes()
        self.route_task = asyncio.create_task(self.route_refresh_loop())

    async def close(self) -> None:
        if self.route_task:
            self.route_task.cancel()
        await self.db.close()
        await super().close()

    async def on_ready(self) -> None:
        log.info(
            "ONLINE como %s (%s) | presencia=invisible | rutas=%s",
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
                log.exception("Fallo actualizando rutas; mantengo las anteriores.")

    async def refresh_routes(self) -> None:
        rows = await self.db.get_active_routes()
        new: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)

        for row in rows:
            key = (
                int(row["source_guild_id"]),
                int(row["source_channel_id"]),
            )
            new[key].append(row)

        self.routes = dict(new)
        log.info(
            "Rutas recargadas: %s canales origen / %s rutas",
            len(self.routes),
            sum(len(v) for v in self.routes.values()),
        )

    async def on_message(self, message: discord.Message) -> None:
        if message.guild is None:
            return

        # Nunca reflejar mensajes enviados por ESTE MISMO bot.
        if self.user and message.author.id == self.user.id:
            return

        routes = self.routes.get((message.guild.id, message.channel.id))
        if not routes:
            return

        for route in routes:
            # Evita una ruta absurda origen == destino.
            if (
                message.guild.id == int(route["target_guild_id"])
                and message.channel.id == int(route["target_channel_id"])
            ):
                continue

            route_id = int(route["id"])
            claimed = await self.db.claim_message(
                route_id,
                message.id,
                message.channel.id,
            )
            if not claimed:
                continue

            try:
                sent = await self.forward_message(message, route)
                await self.db.mark_sent(route_id, message.id, sent.id)
            except Exception as exc:
                log.exception(
                    "Error en ruta %s, mensaje %s", route_id, message.id
                )
                await self.db.mark_failed(
                    route_id,
                    message.id,
                    f"{type(exc).__name__}: {exc}",
                )

    async def get_target_channel(
        self, guild_id: int, channel_id: int
    ) -> discord.abc.Messageable:
        guild = self.get_guild(guild_id)
        if guild is None:
            raise RuntimeError(f"Bot no está en servidor destino {guild_id}.")

        channel = guild.get_channel_or_thread(channel_id)
        if channel is None:
            channel = await self.fetch_channel(channel_id)

        if not isinstance(channel, discord.abc.Messageable):
            raise RuntimeError(f"Canal destino {channel_id} no admite mensajes.")
        return channel

    async def resolve_reference(
        self, message: discord.Message
    ) -> discord.Message | None:
        ref = message.reference
        if ref is None or ref.message_id is None:
            return None

        if isinstance(ref.resolved, discord.Message):
            return ref.resolved

        channel_id = ref.channel_id or message.channel.id
        channel = self.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(channel_id)
            except discord.DiscordException:
                return None

        if not isinstance(channel, discord.abc.Messageable):
            return None

        try:
            return await channel.fetch_message(ref.message_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None

    async def build_payload(
        self,
        message: discord.Message,
        target_guild: discord.Guild,
        route: dict[str, Any],
    ) -> tuple[list[discord.Embed], list[discord.File]]:
        guild_name = message.guild.name
        channel_name = getattr(message.channel, "name", str(message.channel.id))

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

        display_name = getattr(message.author, "display_name", message.author.name)
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
                    f"**{ref_author}** (`{referenced.author.id}`)\n"
                    f"{ref_text}\n"
                    f"[Abrir mensaje respondido]({referenced.jump_url})"
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
            stickers = "\n".join(
                f"• {s.name} — {s.url}" for s in message.stickers[:5]
            )
            embed.add_field(
                name="Stickers",
                value=truncate(stickers, 700),
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
        embed.set_footer(text=truncate(footer, 2048))

        files: list[discord.File] = []
        image_urls: list[str] = []
        attachment_lines: list[str] = []
        upload_limit = target_guild.filesize_limit

        for index, attachment in enumerate(message.attachments[:10], start=1):
            kind = attachment_kind(attachment)
            icon = {
                "image": "🖼️",
                "audio": "🎵",
                "video": "🎬",
                "file": "📎",
            }[kind]

            mb = attachment.size / 1024 / 1024
            ctype = attachment.content_type or "desconocido"

            if attachment.size > upload_limit:
                attachment_lines.append(
                    f"{icon} **{attachment.filename}** "
                    f"({mb:.2f} MB) — [abrir original]({attachment.url})"
                )
                continue

            try:
                raw = await attachment.read(use_cached=True)
                f = discord.File(
                    io.BytesIO(raw),
                    filename=safe_filename(attachment.filename, index),
                    spoiler=attachment.is_spoiler(),
                )
                files.append(f)
                attachment_lines.append(
                    f"{icon} **{attachment.filename}** "
                    f"({mb:.2f} MB, {ctype})"
                )
                if kind == "image":
                    image_urls.append(f"attachment://{f.filename}")
            except (discord.HTTPException, OSError):
                attachment_lines.append(
                    f"{icon} **{attachment.filename}** — "
                    f"[abrir original]({attachment.url})"
                )

        if attachment_lines:
            embed.add_field(
                name="Adjuntos",
                value=truncate("\n".join(attachment_lines), 900),
                inline=False,
            )

        embeds = [embed]
        if image_urls:
            embed.set_image(url=image_urls[0])
            for image_url in image_urls[1:10]:
                e = discord.Embed()
                e.set_image(url=image_url)
                embeds.append(e)

        return embeds[:10], files[:10]

    async def forward_message(
        self,
        message: discord.Message,
        route: dict[str, Any],
    ) -> discord.Message:
        target_guild_id = int(route["target_guild_id"])
        target_channel_id = int(route["target_channel_id"])

        guild = self.get_guild(target_guild_id)
        if guild is None:
            raise RuntimeError(
                f"No encuentro servidor destino {target_guild_id}."
            )

        channel = await self.get_target_channel(
            target_guild_id,
            target_channel_id,
        )
        embeds, files = await self.build_payload(message, guild, route)

        return await channel.send(
            embeds=embeds,
            files=files,
            allowed_mentions=discord.AllowedMentions.none(),
            silent=True,
        )


client = MonitorClient()

if __name__ == "__main__":
    client.run(DISCORD_TOKEN, log_handler=None)
