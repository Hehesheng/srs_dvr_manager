"""Media hall lifecycle: rooms, members, cache budget and cleanup."""

import asyncio
import logging
import os
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import yaml

from . import protocol
from .auth import HallAuth
from .cache import BlockCache
from .jellyfin import (
    HlsSession,
    JellyfinAuthError,
    JellyfinClient,
    JellyfinError,
    JellyfinSession,
    JellyfinSource,
    clamp_bitrate,
    DEFAULT_TRANSCODE_BITRATE,
    direct_mime,
    duration_seconds,
    item_kind,
    item_name,
    normalize_jellyfin_url,
    playback_mode,
    subtitle_streams,
    text_subtitle_streams,
)
from .rooms import MediaInfo, Member, Room
from .subtitles import (
    Subtitle,
    SubtitleFormatError,
    convert_subtitle,
    sanitize_lang,
    sanitize_name,
)
from .upstream import UpstreamError, UpstreamSource, probe_url

logger = logging.getLogger(__name__)

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CONFIG: Dict[str, Any] = {
    "enabled": True,
    "max_rooms": 10,
    "max_members_per_room": 20,
    "block_size": 262144,
    "cache_bytes_per_room": 268435456,
    "cache_bytes_total": 1073741824,
    "host_read_concurrency": 4,
    "room_idle_timeout": 1800,
    "member_idle_timeout": 600,
    "host_offline_timeout": 30,
    "media_url_ttl": 86400,
    "max_media_bytes": 64 * 1024 * 1024 * 1024,
    "max_subtitle_bytes": 10 * 1024 * 1024,
}
ENDED_ROOM_GRACE = 60.0
# Embedded tracks are uploaded per playlist item, so a long playlist needs room
# for several extra entries on top of the subtitles the host uploads by hand.
MAX_SUBTITLES_PER_ROOM = 32
MAX_PLAYLIST_ITEMS = 100
# Re-send the projected host timeline once a second.  Members otherwise only
# hear about changes, so any silence (seek, pause, stale tab) would leave them
# extrapolating from an old anchor.
HOST_PULSE_INTERVAL = 1.0
# The name the host shows up under in the member list.  Same length as a member
# name, so the two read the same in the list.
HOST_NAME_MAX = 32


def clean_host_name(raw: Any) -> str:
    """Tidy a host's own name: one line, no stray spaces, inside the cap.

    A name is drawn next to a member's in one narrow list, so a newline or a
    run of spaces is a layout accident, not something to preserve.  Empty is a
    legitimate value: it means "call me 房主".
    """
    text = " ".join(str(raw or "").split())
    if len(text) > HOST_NAME_MAX:
        raise HallError(
            f"名字最多 {HOST_NAME_MAX} 个字", protocol.ERR_HOST_NAME_INVALID, 400
        )
    return text


class HallError(Exception):
    status_code = 400
    code = protocol.ERR_BAD_MESSAGE

    def __init__(self, message: str, code: Optional[str] = None, status_code: Optional[int] = None):
        super().__init__(message)
        self.message = message
        if code:
            self.code = code
        if status_code:
            self.status_code = status_code


JELLYFIN_DEFAULTS: Dict[str, Any] = {
    "enabled": False,
    "max_bitrate": 8_000_000,
    "seek_debounce_ms": 800,
}

# How many distinct Jellyfin servers keep a client.  A client is only a bitrate
# ceiling on top of the hall's one aiohttp session (nothing to close, nothing to
# leak), so this is a memo-size bound, not a resource limit.
JELLYFIN_CLIENT_LIMIT = 16


@dataclass
class BuiltMedia:
    """A media item plus whatever source object it needs to be served."""

    media: MediaInfo
    upstream: Optional[UpstreamSource] = None
    jellyfin_source: Optional[JellyfinSource] = None
    hls: Optional[HlsSession] = None
    # The Jellyfin MediaSource behind this item; the room only adopts its
    # subtitle state once this item is the one actually playing.
    jellyfin_item: Optional[Dict[str, Any]] = None


@dataclass
class JellyfinConfig:
    """Whether the hall offers Jellyfin at all, and how hard it may pull.

    The server address is not here any more: each room's host types it in the
    browser when connecting, so the same deployment can host rooms that point
    at different Jellyfin servers (and nobody has to edit config.yaml and
    restart to move to another one).
    """

    enabled: bool = False
    max_bitrate: int = 8_000_000
    seek_debounce_ms: int = 800

    @classmethod
    def from_payload(cls, payload: Optional[Dict[str, Any]]) -> "JellyfinConfig":
        source = payload if isinstance(payload, dict) else {}
        merged = dict(JELLYFIN_DEFAULTS)
        merged.update({k: v for k, v in source.items() if v is not None})
        if source.get("enabled") is None:
            # A config written before addresses moved to the browser had no
            # ``enabled`` switch, only a url.  Reading that as "on" is what
            # keeps an upgrade from silently switching Jellyfin off.
            enabled = bool(str(source.get("url") or "").strip())
        else:
            enabled = bool(source.get("enabled"))
        try:
            bitrate = int(merged.get("max_bitrate"))
        except (TypeError, ValueError):
            bitrate = JELLYFIN_DEFAULTS["max_bitrate"]
        try:
            debounce = int(merged.get("seek_debounce_ms"))
        except (TypeError, ValueError):
            debounce = JELLYFIN_DEFAULTS["seek_debounce_ms"]
        return cls(
            enabled=enabled,
            max_bitrate=max(500_000, min(40_000_000, bitrate)),
            seek_debounce_ms=max(0, min(5000, debounce)),
        )

    def public(self) -> Dict[str, Any]:
        """What the frontend may know: no credentials live here."""
        return {"enabled": self.enabled, "max_bitrate": self.max_bitrate}


@dataclass
class HallConfig:
    enabled: bool = True
    max_rooms: int = 10
    max_members_per_room: int = 20
    block_size: int = 262144
    cache_bytes_per_room: int = 268435456
    cache_bytes_total: int = 1073741824
    host_read_concurrency: int = 4
    room_idle_timeout: float = 1800
    member_idle_timeout: float = 600
    # The host cannot come back once its page is gone, so a room whose host
    # stays offline is ended after this grace period.
    host_offline_timeout: float = 30.0
    media_url_ttl: int = 86400
    max_media_bytes: int = 64 * 1024 * 1024 * 1024
    max_subtitle_bytes: int = 10 * 1024 * 1024
    signing_secret: str = ""
    jellyfin: JellyfinConfig = field(default_factory=JellyfinConfig)

    @classmethod
    def load(cls, config_path: str = "config.yaml") -> "HallConfig":
        raw: Dict[str, Any] = {}
        jellyfin_raw: Optional[Dict[str, Any]] = None
        if os.path.exists(config_path):
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    payload = yaml.safe_load(f) or {}
                raw = payload.get("hall", {}) or {}
                jellyfin_raw = payload.get("jellyfin")
            except Exception:
                logger.exception("Failed to read hall config; using defaults")
        merged = dict(DEFAULT_CONFIG)
        merged.update({k: v for k, v in raw.items() if v is not None})
        return cls(
            enabled=bool(merged["enabled"]),
            max_rooms=max(1, int(merged["max_rooms"])),
            max_members_per_room=max(1, int(merged["max_members_per_room"])),
            block_size=max(64 * 1024, int(merged["block_size"])),
            cache_bytes_per_room=max(8 * 1024 * 1024, int(merged["cache_bytes_per_room"])),
            cache_bytes_total=max(8 * 1024 * 1024, int(merged["cache_bytes_total"])),
            host_read_concurrency=max(1, int(merged["host_read_concurrency"])),
            room_idle_timeout=max(60.0, float(merged["room_idle_timeout"])),
            member_idle_timeout=max(60.0, float(merged["member_idle_timeout"])),
            host_offline_timeout=max(1.0, float(merged["host_offline_timeout"])),
            media_url_ttl=max(60, int(merged["media_url_ttl"])),
            max_media_bytes=max(1024 * 1024, int(merged["max_media_bytes"])),
            max_subtitle_bytes=max(1024, int(merged["max_subtitle_bytes"])),
            signing_secret=str(merged.get("signing_secret") or ""),
            jellyfin=JellyfinConfig.from_payload(jellyfin_raw),
        )


class HallManager:
    def __init__(self, config: HallConfig) -> None:
        self.config = config
        self.auth = HallAuth.from_config(
            {"signing_secret": config.signing_secret, "media_url_ttl": config.media_url_ttl},
            PROJECT_DIR,
        )
        self.rooms: Dict[str, Room] = {}
        self._codes: Dict[str, str] = {}
        self._ended_at: Dict[str, float] = {}
        self._host_offline_tasks: Dict[str, asyncio.Task] = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._pulse_task: Optional[asyncio.Task] = None
        self._started = False
        # One client per Jellyfin address the hosts have used, because the
        # address is per room now.  A client is just an aiohttp session plus a
        # bitrate ceiling, so keeping a handful around costs nothing.
        self._jellyfin_clients: "OrderedDict[str, JellyfinClient]" = OrderedDict()
        # One seek restart per room is enough; dragging must not spawn a
        # transcoder per pointer move.
        self._seek_tasks: Dict[str, asyncio.Task] = {}
        self._subtitle_tasks: set = set()

    @classmethod
    def from_config(cls, config_path: str = "config.yaml") -> "HallManager":
        return cls(HallConfig.load(config_path))

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    async def start(self) -> None:
        if self._started:
            return
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=25, connect=8, sock_read=20)
        )
        self._started = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        self._pulse_task = asyncio.create_task(self._pulse_loop())
        logger.info(
            "Media hall started: max_rooms=%d block=%d cache_room=%d cache_total=%d jellyfin=%s",
            self.config.max_rooms,
            self.config.block_size,
            self.config.cache_bytes_per_room,
            self.config.cache_bytes_total,
            "on" if self.config.jellyfin.enabled else "off",
        )

    async def stop(self) -> None:
        self._started = False
        for task in (self._cleanup_task, self._pulse_task):
            if task is not None:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._cleanup_task = None
        self._pulse_task = None
        for task in list(self._host_offline_tasks.values()):
            task.cancel()
        self._host_offline_tasks.clear()
        for room in list(self.rooms.values()):
            self._stop_room_transcode(room)
            await self._close_room(room, code=1001, reason="server shutting down")
        self.rooms.clear()
        self._codes.clear()
        if self._session is not None:
            await self._session.close()
            self._session = None
        logger.info("Media hall stopped")

    # -- room lookup ---------------------------------------------------
    def get_room(self, ref: str) -> Optional[Room]:
        room = self.rooms.get(ref)
        if room is not None:
            return room
        room_id = self._codes.get(ref.upper())
        if room_id is None:
            return None
        return self.rooms.get(room_id)

    def require_room(self, ref: str) -> Room:
        room = self.get_room(ref)
        if room is None:
            raise HallError("room not found", protocol.ERR_ROOM_NOT_FOUND, 404)
        if room.ended:
            raise HallError("room has ended", protocol.ERR_ROOM_ENDED, 410)
        return room

    def list_rooms(self) -> List[Dict[str, Any]]:
        """Active rooms, newest first, as public summaries."""
        active = [room for room in self.rooms.values() if not room.ended]
        active.sort(key=lambda room: room.created_at, reverse=True)
        return [room.public_summary() for room in active]

    # -- creation / join ----------------------------------------------
    async def create_room(
        self,
        name: str,
        media_payload: Optional[Dict[str, Any]] = None,
        host_name: str = "",
    ) -> Room:
        if not self.enabled:
            raise HallError("hall is disabled")
        active = [r for r in self.rooms.values() if not r.ended]
        if len(active) >= self.config.max_rooms:
            raise HallError("too many rooms", "TOO_MANY_ROOMS", 429)

        room_name = (name or "Media Room").strip()[:64] or "Media Room"
        media: Optional[MediaInfo] = None
        built: Optional[BuiltMedia] = None
        if media_payload:
            built = await self._build_media(media_payload)
            media = built.media

        room = Room(
            hall=self,
            name=room_name,
            media=media,
            block_size=self.config.block_size,
            cache_bytes=self.config.cache_bytes_per_room,
            host_read_concurrency=self.config.host_read_concurrency,
            upstream=built.upstream if built else None,
            jellyfin_source=built.jellyfin_source if built else None,
            hls=built.hls if built else None,
        )
        # A name the host set before is carried in by the browser, so the
        # member list says the same thing without the host having to re-enter
        # it in every room.
        room.host_name = clean_host_name(host_name)
        while room.room_code in self._codes:
            room.room_code = self.auth.new_room_code()
        # Quality starts at the room default, never above what this server was
        # configured to pull; the host can lower or raise it inside that range.
        room.jellyfin_bitrate_max = clamp_bitrate(self.config.jellyfin.max_bitrate)
        room.jellyfin_bitrate = min(DEFAULT_TRANSCODE_BITRATE, room.jellyfin_bitrate_max)
        self.rooms[room.room_id] = room
        self._codes[room.room_code] = room.room_id
        logger.info(
            "Room created id=%s code=%s media=%s playlist=%d",
            room.room_id,
            room.room_code,
            room.media.name if room.media else "-",
            len(room.playlist_order),
        )
        return room

    async def _build_media(
        self, media_payload: Dict[str, Any], room: Optional[Room] = None
    ) -> "BuiltMedia":
        kind = str(media_payload.get("kind") or "host_file")
        media_name = str(media_payload.get("name") or "").strip()[:256]
        if kind not in ("host_file", "url", "jellyfin"):
            raise HallError("media.kind must be host_file, url or jellyfin")

        media_id = "m_" + os.urandom(8).hex()
        upstream: Optional[UpstreamSource] = None

        if kind == "jellyfin":
            return await self._build_jellyfin_media(media_payload, room, media_id)

        if kind == "host_file":
            try:
                size = int(media_payload.get("size") or 0)
            except (TypeError, ValueError):
                raise HallError("media.size must be an integer")
            if size <= 0 or size > self.config.max_media_bytes:
                raise HallError("invalid media.size")
            mime = str(media_payload.get("mime") or "application/octet-stream")[:128]
            media = MediaInfo(
                media_id=media_id,
                kind=kind,
                name=media_name or "media",
                size=size,
                mime=mime,
                range_supported=True,
            )
        else:
            if self._session is None:
                raise HallError("hall is not started", "HALL_UNAVAILABLE", 503)
            source_url = str(media_payload.get("url") or "").strip()
            if not source_url:
                raise HallError("media.url is required")
            try:
                probed = await probe_url(self._session, source_url)
            except UpstreamError as exc:
                raise HallError(str(exc), getattr(exc, "code", "UPSTREAM_ERROR"), 400)
            size = int(probed["size"])
            if size <= 0 or size > self.config.max_media_bytes:
                raise HallError("invalid upstream media size")
            media = MediaInfo(
                media_id=media_id,
                kind="url",
                name=media_name or str(probed["url"]).rsplit("/", 1)[-1] or "media",
                size=size,
                mime=str(probed["mime"]),
                url=str(probed["url"]),
                range_supported=bool(probed["range_supported"]),
            )
            upstream = UpstreamSource(
                self._session,
                media.url or source_url,
                media.size,
                media.mime,
                media.range_supported,
            )
        return BuiltMedia(media=media, upstream=upstream)

    async def _build_jellyfin_media(
        self, media_payload: Dict[str, Any], room: Optional[Room], media_id: str
    ) -> "BuiltMedia":
        self._require_jellyfin_enabled()
        if room is None or room.jellyfin is None:
            raise HallError(
                "请先填写 Jellyfin 网址和密钥", protocol.ERR_JELLYFIN_AUTH_REQUIRED, 401
            )
        client = self.jellyfin_for(room.jellyfin)
        item_id = str(media_payload.get("item_id") or "").strip()
        if not item_id or not item_id.replace("-", "").isalnum():
            raise HallError("缺少有效的 Jellyfin 视频 ID")

        try:
            item = await client.item(room.jellyfin, item_id)
        except JellyfinAuthError:
            room.jellyfin = None
            raise HallError(
                "Jellyfin 连接已失效，请重新填写网址和密钥",
                protocol.ERR_JELLYFIN_AUTH_FAILED,
                401,
            )
        except JellyfinError as exc:
            raise HallError(exc.message, exc.code, exc.status_code)

        if item_kind(item) in ("series", "season", "folder"):
            # A container has no file of its own; say so instead of letting it
            # fall through to "这个条目没有可播放的文件".
            raise HallError(
                "请选择具体某一集或影片，而不是整个剧集/文件夹",
                protocol.ERR_BAD_MESSAGE,
                400,
            )
        sources = item.get("MediaSources") or []
        if not sources:
            raise HallError("这个条目没有可播放的文件", "MEDIA_NOT_FOUND", 404)
        source = sources[0]
        mode = playback_mode(source)
        duration = duration_seconds(item)
        name = item_name(item)

        if mode == "direct":
            # Direct play gets real WebVTT tracks; there is no transcode to burn
            # subtitles into, so the picker has nothing to choose.
            try:
                size = await client.probe_size(room.jellyfin, item_id)
            except JellyfinError as exc:
                raise HallError(exc.message, exc.code, exc.status_code)
            if size <= 0 or size > self.config.max_media_bytes:
                raise HallError("这个视频的大小不可用")
            mime = direct_mime(source)
            media = MediaInfo(
                media_id=media_id,
                kind="jellyfin",
                name=name,
                size=size,
                mime=mime,
                playback_mode="direct",
                duration=duration,
                item_id=item_id,
            )
            return BuiltMedia(
                media=media,
                jellyfin_source=JellyfinSource(
                    client, room.jellyfin, item_id, size, mime
                ),
                jellyfin_item=source,
            )

        try:
            hls = await client.open_transcode(
                room.jellyfin,
                item_id,
                0.0,
                1,
                max_bitrate=room.jellyfin_bitrate,
            )
        except JellyfinError as exc:
            raise HallError(exc.message, exc.code, exc.status_code)
        media = MediaInfo(
            media_id=media_id,
            kind="jellyfin",
            name=name,
            size=0,
            mime="application/vnd.apple.mpegurl",
            playback_mode="hls",
            duration=duration,
            item_id=item_id,
        )
        return BuiltMedia(media=media, hls=hls, jellyfin_item=source)

    def _apply_jellyfin_item(self, room: Room, built: "BuiltMedia") -> None:
        """Adopt a Jellyfin item's subtitle state, once it is the active media.

        Every item carries its own tracks, so this has to run on each switch —
        leaving the previous item's (empty) list in place is how the CC menu
        went blank after visiting a direct-play film.
        """
        if built.media.kind != "jellyfin" or built.jellyfin_item is None:
            return
        source = built.jellyfin_item
        room.jellyfin_item = source
        # Direct play has no burn-in, so it has no tracks to offer.
        room.jellyfin_subtitle_tracks = (
            subtitle_streams(source) if built.media.playback_mode == "hls" else []
        )
        room.jellyfin_subtitle = None
        room.jellyfin_subtitles_off = False

    def _require_host(self, room: Room, host_token: str) -> None:
        if host_token != room.host_token:
            raise HallError("invalid host token", protocol.ERR_FORBIDDEN, 403)
        if room.ended:
            raise HallError("room has ended", protocol.ERR_ROOM_ENDED, 410)

    def _ensure_playlist_capacity(self, room: Room) -> None:
        if len(room.playlist_order) >= MAX_PLAYLIST_ITEMS:
            raise HallError(
                f"at most {MAX_PLAYLIST_ITEMS} items per playlist",
                "TOO_MANY_MEDIA",
                409,
            )

    async def _source_for(self, room: Room, media: MediaInfo) -> BuiltMedia:
        """Rebuild the source objects for a playlist item that is re-activated."""
        if media.kind == "url" and media.url:
            if self._session is None:
                raise HallError("hall is not started", "HALL_UNAVAILABLE", 503)
            return BuiltMedia(
                media=media,
                upstream=UpstreamSource(
                    self._session,
                    media.url,
                    media.size,
                    media.mime,
                    media.range_supported,
                ),
            )
        if media.kind == "jellyfin":
            if room.jellyfin is None:
                raise HallError(
                    "Jellyfin 连接已失效，请重新填写网址和密钥",
                    protocol.ERR_JELLYFIN_AUTH_REQUIRED,
                    401,
                )
            client = self.jellyfin_for(room.jellyfin)
            item_id = media.item_id or ""
            # Re-read the item: the subtitle list belongs to this title, and
            # the room may have been playing something else in between.
            try:
                item = await client.item(room.jellyfin, item_id)
            except JellyfinAuthError:
                room.jellyfin = None
                raise HallError(
                    "Jellyfin 连接已失效，请重新填写网址和密钥",
                    protocol.ERR_JELLYFIN_AUTH_FAILED,
                    401,
                )
            except JellyfinError as exc:
                raise HallError(exc.message, exc.code, exc.status_code)
            sources = item.get("MediaSources") or [{}]
            source = sources[0]
            if media.playback_mode == "hls":
                try:
                    hls = await client.open_transcode(
                        room.jellyfin,
                        item_id,
                        0.0,
                        room.media_epoch + 1,
                        max_bitrate=room.jellyfin_bitrate,
                    )
                except JellyfinError as exc:
                    raise HallError(exc.message, exc.code, exc.status_code)
                return BuiltMedia(media=media, hls=hls, jellyfin_item=source)
            return BuiltMedia(
                media=media,
                jellyfin_source=JellyfinSource(
                    client, room.jellyfin, item_id, media.size, media.mime
                ),
                jellyfin_item=source,
            )
        return BuiltMedia(media=media)

    # -- jellyfin ------------------------------------------------------
    def _require_jellyfin_enabled(self) -> None:
        if not self.config.jellyfin.enabled:
            raise HallError(
                "Jellyfin 源未启用", protocol.ERR_JELLYFIN_UNAVAILABLE, 503
            )

    def jellyfin_status(self, room: Room, host_token: str) -> Dict[str, Any]:
        """Everything the host view needs before it opens the picker."""
        self._require_host(room, host_token)
        return {
            # `enabled` is the deployment switch only: whether this room has a
            # server connected is the separate `authenticated` answer.
            "enabled": bool(self.config.jellyfin.enabled),
            "authenticated": room.jellyfin is not None,
            "jellyfin": room.jellyfin.public() if room.jellyfin else None,
            "max_bitrate": self.config.jellyfin.max_bitrate,
        }

    def jellyfin_for(self, session: Optional[JellyfinSession]) -> JellyfinClient:
        """The client that talks to the server behind ``session``.

        The address travels with the room now, so this is looked up (and built
        on first use) rather than held on the manager.  Rooms that point at the
        same address share one client; the LRU only bounds how many different
        servers a long-lived process remembers.
        """
        base_url = (session.base_url if session else "").rstrip("/")
        if not base_url:
            raise HallError(
                "请先填写 Jellyfin 网址和密钥", protocol.ERR_JELLYFIN_AUTH_REQUIRED, 401
            )
        client = self._jellyfin_clients.get(base_url)
        if client is not None:
            self._jellyfin_clients.move_to_end(base_url)
            return client
        if self._session is None:
            raise HallError("hall is not started", "HALL_UNAVAILABLE", 503)
        client = JellyfinClient(
            self._session, base_url, self.config.jellyfin.max_bitrate
        )
        self._jellyfin_clients[base_url] = client
        while len(self._jellyfin_clients) > JELLYFIN_CLIENT_LIMIT:
            self._jellyfin_clients.popitem(last=False)
        return client

    async def jellyfin_connect(
        self,
        room: Room,
        host_token: str,
        url: str,
        token: str,
        user_id: Optional[str] = None,
    ) -> JellyfinSession:
        """Adopt the server and API key the host typed into the browser.

        The address is checked here rather than on the first library call, so
        a typo comes back as a message about the address.  Connecting again
        replaces whatever this room had before, which is what "换一个服务器"
        does.
        """
        self._require_host(room, host_token)
        self._require_jellyfin_enabled()
        try:
            base_url = normalize_jellyfin_url(url)
        except JellyfinError as exc:
            raise HallError(exc.message, exc.code, exc.status_code)
        token = str(token or "").strip()
        if not token:
            raise HallError(
                "请填写 Jellyfin 密钥", protocol.ERR_JELLYFIN_CONFIG_REQUIRED, 400
            )
        if len(token) > 512:
            raise HallError(
                "Jellyfin 密钥太长了", protocol.ERR_JELLYFIN_CONFIG_REQUIRED, 400
            )
        probe = JellyfinSession(
            token=token, user_id="", user_name="", device_id="", base_url=base_url
        )
        client = self.jellyfin_for(probe)
        try:
            session = await client.verify(
                token, f"srs-hall-{room.room_id}", user_id
            )
        except JellyfinAuthError:
            # "认证已失效" would be the wrong story here: this is the first
            # use of a key the host just typed.
            raise HallError(
                "这枚密钥不能用，请检查网址和 API 密钥",
                protocol.ERR_JELLYFIN_AUTH_FAILED,
                401,
            )
        except JellyfinError as exc:
            raise HallError(exc.message, exc.code, exc.status_code)
        room.jellyfin = session
        room.touch()
        logger.info(
            "Jellyfin connected room=%s server=%s user=%s",
            room.room_id,
            base_url,
            session.user_name,
        )
        return session

    async def jellyfin_libraries(self, room: Room) -> List[Dict[str, Any]]:
        self._require_jellyfin_enabled()
        if room.jellyfin is None:
            raise HallError(
                "请先填写 Jellyfin 网址和密钥", protocol.ERR_JELLYFIN_AUTH_REQUIRED, 401
            )
        client = self.jellyfin_for(room.jellyfin)
        try:
            return await client.libraries(room.jellyfin)
        except JellyfinAuthError:
            room.jellyfin = None
            raise HallError(
                "Jellyfin 登录已失效，请重新登录",
                protocol.ERR_JELLYFIN_AUTH_FAILED,
                401,
            )
        except JellyfinError as exc:
            raise HallError(exc.message, exc.code, exc.status_code)

    async def jellyfin_items(
        self,
        room: Room,
        host_token: str,
        parent_id: Optional[str],
        query: str,
        start: int,
        limit: int,
        mode: str = "children",
    ) -> Dict[str, Any]:
        # The library listing belongs to the host, not to whoever joined the
        # room; only the host may ask what is in it.
        self._require_host(room, host_token)
        self._require_jellyfin_enabled()
        if room.jellyfin is None:
            raise HallError(
                "请先填写 Jellyfin 网址和密钥", protocol.ERR_JELLYFIN_AUTH_REQUIRED, 401
            )
        client = self.jellyfin_for(room.jellyfin)
        mode = mode if mode in ("children", "episodes") else "children"
        try:
            return await client.items(
                room.jellyfin, parent_id, query, start, limit, mode
            )
        except JellyfinAuthError:
            room.jellyfin = None
            raise HallError(
                "Jellyfin 登录已失效，请重新登录",
                protocol.ERR_JELLYFIN_AUTH_FAILED,
                401,
            )
        except JellyfinError as exc:
            raise HallError(exc.message, exc.code, exc.status_code)

    async def seek_transcode(self, room: Room, host_token: str, position: float) -> None:
        self._require_host(room, host_token)
        if room.media is None or room.media.playback_mode != "hls" or room.hls is None:
            raise HallError("当前播放项不需要转码", protocol.ERR_BAD_MESSAGE, 400)
        if room.jellyfin is None:
            raise HallError(
                "Jellyfin 登录已失效，请重新登录",
                protocol.ERR_JELLYFIN_AUTH_REQUIRED,
                401,
            )
        room.pending_seek = max(0.0, float(position))
        task = self._seek_tasks.get(room.room_id)
        if task is None or task.done():
            task = asyncio.create_task(self._run_seek(room))
            self._seek_tasks[room.room_id] = task

    async def set_transcode_quality(
        self, room: Room, host_token: str, bitrate: int, position: float = 0.0
    ) -> int:
        """Set the room's transcode bitrate, restarting the transcode if one runs.

        The host owns this: it changes what every member receives, so the
        change travels through the same restart as a seek and publishes a new
        epoch for members to follow.  Rooms with nothing to transcode just
        remember the setting for the next item.
        """
        self._require_host(room, host_token)
        room.jellyfin_bitrate = clamp_bitrate(bitrate, ceiling=room.jellyfin_bitrate_max)
        if room.media is not None and room.media.playback_mode == "hls" and room.hls is not None:
            if room.jellyfin is None:
                raise HallError(
                    "Jellyfin 登录已失效，请重新登录",
                    protocol.ERR_JELLYFIN_AUTH_REQUIRED,
                    401,
                )
            await self._restart_transcode(room, max(0.0, float(position)))
        else:
            await self.broadcast_room(room)
        return room.jellyfin_bitrate

    async def set_transcode_subtitle(
        self,
        room: Room,
        host_token: str,
        choice: Optional[Any],
        position: float,
    ) -> None:
        """Pick the subtitle burned into the room's transcode.

        ``None`` follows Jellyfin's account default, ``"off"`` asks for no
        subtitle at all, and an ``int`` picks that stream index.  Every change
        restarts the transcode, so it goes through the same debounced path as
        a seek and publishes a new epoch.
        """
        self._require_host(room, host_token)
        if room.media is None or room.media.playback_mode != "hls" or room.hls is None:
            raise HallError("当前播放项不需要转码", protocol.ERR_BAD_MESSAGE, 400)
        if room.jellyfin is None:
            raise HallError(
                "Jellyfin 登录已失效，请重新登录",
                protocol.ERR_JELLYFIN_AUTH_REQUIRED,
                401,
            )
        if choice is None:
            index: Optional[int] = None
            off = False
        elif isinstance(choice, str) and choice == "off":
            index = None
            off = True
        elif isinstance(choice, int) and not isinstance(choice, bool) and choice >= 0:
            index = int(choice)
            off = False
        else:
            raise HallError("字幕选择不正确", protocol.ERR_BAD_MESSAGE, 400)
        if index is not None:
            known = {
                int(track["index"])
                for track in room.jellyfin_subtitle_tracks
                if isinstance(track.get("index"), int)
            }
            if known and index not in known:
                raise HallError("没有这条字幕轨", protocol.ERR_BAD_MESSAGE, 400)
        if (
            index == room.jellyfin_subtitle
            and off == room.jellyfin_subtitles_off
            and room.hls is not None
        ):
            return
        room.jellyfin_subtitle = index
        room.jellyfin_subtitles_off = off
        room.pending_seek = max(0.0, float(position))
        task = self._seek_tasks.get(room.room_id)
        if task is None or task.done():
            task = asyncio.create_task(self._run_seek(room))
            self._seek_tasks[room.room_id] = task

    async def _run_seek(self, room: Room) -> None:
        """Restart the transcode at the host's latest position.

        A drag arrives as a burst of seeks and a restart takes seconds, so this
        waits for the target to hold still before spending one.  A seek that
        lands while the restart runs is not lost either: the loop comes round
        again, so the room ends up on the position the host let go of rather
        than on whichever one happened to reach the transcoder first.
        """
        delay = self.config.jellyfin.seek_debounce_ms / 1000.0
        try:
            while True:
                while True:
                    target = room.pending_seek
                    if delay > 0:
                        await asyncio.sleep(delay)
                    if room.pending_seek != target:
                        continue
                    break
                await self._restart_transcode(room, target)
                if room.pending_seek == target:
                    break
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Jellyfin seek restart failed room=%s", room.room_id)
        finally:
            if self._seek_tasks.get(room.room_id) is asyncio.current_task():
                self._seek_tasks.pop(room.room_id, None)

    async def _restart_transcode(self, room: Room, position: float) -> None:
        session = room.jellyfin
        old = room.hls
        if session is None or old is None or room.ended:
            return
        try:
            client = self.jellyfin_for(session)
        except HallError:
            return
        try:
            fresh = await client.open_transcode(
                session,
                old.item_id,
                position,
                old.epoch + 1,
                subtitle_index=room.jellyfin_subtitle,
                subtitles_off=room.jellyfin_subtitles_off,
                max_bitrate=room.jellyfin_bitrate,
            )
        except JellyfinAuthError:
            room.jellyfin = None
            logger.info("Jellyfin seek needs a reconnect room=%s", room.room_id)
            return
        except JellyfinError as exc:
            logger.warning(
                "Jellyfin seek failed room=%s position=%s: %s",
                room.room_id,
                position,
                exc.message,
            )
            return
        await client.stop_transcode(session, old)
        room.hls = fresh
        # Cached segments belong to the stream that is now gone.
        room.cache = BlockCache(room.cache.block_size, room.cache.max_bytes)
        await self.broadcast_room(room)
        logger.info(
            "Jellyfin transcode restarted room=%s position=%.1f epoch=%d",
            room.room_id,
            position,
            fresh.epoch,
        )

    async def hls_playlist(self, room: Room) -> str:
        session = room.jellyfin
        if session is None or room.hls is None or room.media is None:
            raise HallError(
                "转码会话不存在", protocol.ERR_JELLYFIN_TRANSCODE_FAILED, 404
            )
        client = self.jellyfin_for(session)
        media = room.media
        epoch = room.media_epoch
        try:
            return await client.render_playlist(
                session,
                room.hls,
                lambda index: self.auth.hls_segment_url(
                    room.room_id, media.media_id, epoch, index
                ),
            )
        except JellyfinError as exc:
            raise HallError(exc.message, exc.code, exc.status_code)

    async def hls_segment(self, room: Room, index: int) -> bytes:
        session = room.jellyfin
        if session is None or room.hls is None:
            raise HallError(
                "转码会话不存在", protocol.ERR_JELLYFIN_TRANSCODE_FAILED, 404
            )
        client = self.jellyfin_for(session)
        try:
            return await room.cache.get_or_fetch(
                index,
                lambda: client.read_segment(session, room.hls, index),
            )
        except JellyfinError as exc:
            raise HallError(exc.message, exc.code, exc.status_code)

    def _stop_room_transcode(self, room: Room) -> None:
        task = self._seek_tasks.pop(room.room_id, None)
        if task is not None and not task.done():
            task.cancel()
        hls = room.hls
        session = room.jellyfin
        room.hls = None
        if hls is not None and session is not None:
            try:
                client = self.jellyfin_for(session)
            except HallError:
                return
            asyncio.create_task(client.stop_transcode(session, hls))

    def _spawn_jellyfin_subtitles(self, room: Room, media: MediaInfo) -> None:
        """Pull the item's text subtitle tracks in the background."""
        if media.kind != "jellyfin" or room.jellyfin is None:
            return
        if media.playback_mode != "direct":
            # A transcoded item has its subtitle burned into the picture by
            # Jellyfin; loading a WebVTT track as well would double it up.
            return
        source = room.jellyfin_item or {}
        tracks = text_subtitle_streams(source)
        if not tracks:
            return
        task = asyncio.create_task(self._load_jellyfin_subtitles(room, media, tracks))
        self._subtitle_tasks.add(task)
        task.add_done_callback(self._subtitle_tasks.discard)

    async def _load_jellyfin_subtitles(
        self, room: Room, media: MediaInfo, tracks: List[Dict[str, Any]]
    ) -> None:
        session = room.jellyfin
        if session is None:
            return
        try:
            client = self.jellyfin_for(session)
        except HallError:
            return
        source_id = str((room.jellyfin_item or {}).get("Id") or media.item_id or "")
        for track in tracks:
            if room.ended or room.media is None or room.media.media_id != media.media_id:
                return
            try:
                index = int(track.get("Index"))
            except (TypeError, ValueError):
                continue
            text = await client.subtitle_vtt(session, media.item_id or "", source_id, index)
            if not text:
                continue
            if len(room.subtitles) >= MAX_SUBTITLES_PER_ROOM:
                return
            language = str(track.get("Language") or "und")[:16]
            label = str(track.get("DisplayTitle") or track.get("Title") or language)
            subtitle = Subtitle(
                subtitle_id="sub_" + os.urandom(8).hex(),
                name=sanitize_name(f"{label}.vtt"),
                lang=sanitize_lang(language),
                data=text.encode("utf-8"),
                size=len(text.encode("utf-8")),
                media_id=media.media_id,
            )
            room.subtitles[subtitle.subtitle_id] = subtitle
            if room.active_subtitle_id is None and room.media and room.media.media_id == media.media_id:
                room.active_subtitle_id = subtitle.subtitle_id
                room.active_subtitle_visible = True
            await self.broadcast_room(room)

    async def change_media(
        self, room: Room, host_token: str, media_payload: Dict[str, Any]
    ) -> Room:
        """Swap the playing file; it becomes the newest playlist entry."""
        self._require_host(room, host_token)
        self._ensure_playlist_capacity(room)
        built = await self._build_media(media_payload, room)
        media = built.media
        room.add_playlist_item(media)
        self._apply_jellyfin_item(room, built)
        room.replace_media(media, built.upstream, built.jellyfin_source, built.hls)
        self._spawn_jellyfin_subtitles(room, media)
        await self.broadcast_room(room)
        logger.info(
            "Room media changed room=%s media=%s size=%d kind=%s playlist=%d",
            room.room_id,
            room.media.name,
            room.media.size,
            room.media.kind,
            len(room.playlist_order),
        )
        return room

    async def add_playlist_media(
        self, room: Room, host_token: str, media_payload: Dict[str, Any]
    ) -> Tuple[MediaInfo, bool]:
        """Append an item; an empty room starts playing the first one."""
        self._require_host(room, host_token)
        self._ensure_playlist_capacity(room)
        built = await self._build_media(media_payload, room)
        media = built.media
        room.add_playlist_item(media)
        activated = room.media is None
        if activated:
            # Only the item that actually starts playing owns the subtitle state.
            self._apply_jellyfin_item(room, built)
            room.replace_media(media, built.upstream, built.jellyfin_source, built.hls)
            self._spawn_jellyfin_subtitles(room, media)
        await self.broadcast_room(room)
        logger.info(
            "Playlist item added room=%s media=%s size=%d kind=%s active=%s",
            room.room_id,
            media.name,
            media.size,
            media.kind,
            activated,
        )
        return media, activated

    async def select_playlist_media(self, room: Room, host_token: str, media_id: str) -> Room:
        self._require_host(room, host_token)
        media = room.playlist.get(media_id)
        if media is None:
            raise HallError("media not found", protocol.ERR_MEDIA_NOT_FOUND, 404)
        built = await self._source_for(room, media)
        self._apply_jellyfin_item(room, built)
        room.replace_media(media, built.upstream, built.jellyfin_source, built.hls)
        self._spawn_jellyfin_subtitles(room, media)
        await self.broadcast_room(room)
        logger.info(
            "Playlist item selected room=%s media=%s", room.room_id, media.media_id
        )
        return room

    async def remove_playlist_media(self, room: Room, host_token: str, media_id: str) -> Room:
        self._require_host(room, host_token)
        was_active = room.media is not None and room.media.media_id == media_id
        following = room.following_item_id(media_id) if was_active else None
        if not room.remove_playlist_item(media_id):
            raise HallError("media not found", protocol.ERR_MEDIA_NOT_FOUND, 404)
        # Subtitles extracted from the removed item have nothing left to play on.
        room.drop_subtitles_for(media_id)
        if was_active:
            next_media = room.playlist.get(following) if following else None
            if next_media is not None:
                built = await self._source_for(room, next_media)
                room.replace_media(
                    next_media, built.upstream, built.jellyfin_source, built.hls
                )
                self._spawn_jellyfin_subtitles(room, next_media)
            else:
                room.clear_media()
        await self.broadcast_room(room)
        logger.info(
            "Playlist item removed room=%s media=%s remaining=%d",
            room.room_id,
            media_id,
            len(room.playlist_order),
        )
        return room

    async def set_host_name(self, room: Room, host_token: str, name: str) -> str:
        """Set (or clear) the name the host shows up under.

        Members name themselves when they join; this is the host's own, and it
        belongs to the room rather than to a connection, so everyone sees the
        same thing and it survives the host's worker reconnecting.
        """
        self._require_host(room, host_token)
        room.host_name = clean_host_name(name)
        room.touch()
        await self.broadcast_room(room)
        return room.host_name

    async def join_room(self, ref: str, member_name: str) -> Member:
        room = self.require_room(ref)
        if room.member_count() >= self.config.max_members_per_room:
            raise HallError("room is full", protocol.ERR_ROOM_FULL, 409)
        member = room.add_member((member_name or "Guest").strip()[:32])
        await self.broadcast_room(room)
        return member

    @staticmethod
    def _room_has_media(room: Room, media_id: str) -> bool:
        """A room can hold media outside its playlist: the one it opened with."""
        if media_id in room.playlist:
            return True
        return room.media is not None and room.media.media_id == media_id

    async def add_subtitle(
        self,
        room: Room,
        host_token: str,
        name: str,
        lang: str,
        data: bytes,
        media_id: Optional[str] = None,
    ) -> Subtitle:
        if host_token != room.host_token:
            raise HallError("invalid host token", protocol.ERR_FORBIDDEN, 403)
        if room.ended:
            raise HallError("room has ended", protocol.ERR_ROOM_ENDED, 410)
        if len(data) > self.config.max_subtitle_bytes:
            raise HallError(
                f"subtitle exceeds {self.config.max_subtitle_bytes} bytes",
                protocol.ERR_SUBTITLE_TOO_LARGE,
                413,
            )
        if not data:
            raise HallError("subtitle is empty", protocol.ERR_BAD_MESSAGE, 400)
        if len(room.subtitles) >= MAX_SUBTITLES_PER_ROOM:
            raise HallError(
                f"at most {MAX_SUBTITLES_PER_ROOM} subtitles per room",
                protocol.ERR_TOO_MANY_SUBTITLES,
                409,
            )

        # Tracks extracted from a playlist item belong to that item; a bare
        # upload is room level and applies to whatever is playing.
        if media_id is not None and not self._room_has_media(room, media_id):
            raise HallError("media not found", protocol.ERR_MEDIA_NOT_FOUND, 404)

        clean_name = sanitize_name(name)
        try:
            converted = convert_subtitle(clean_name, data)
        except SubtitleFormatError as exc:
            raise HallError(str(exc), protocol.ERR_SUBTITLE_FORMAT, 415)

        subtitle = Subtitle(
            subtitle_id="sub_" + os.urandom(8).hex(),
            name=clean_name,
            lang=sanitize_lang(lang),
            data=converted,
            size=len(converted),
            media_id=media_id,
        )
        room.subtitles[subtitle.subtitle_id] = subtitle
        # Show the first subtitle of whatever is playing right now, so tracks
        # pulled out of a freshly added file appear without extra clicks.
        current = room.media.media_id if room.media else None
        if room.active_subtitle_id is None and media_id in (None, current):
            room.active_subtitle_id = subtitle.subtitle_id
            room.active_subtitle_visible = True
        await self.broadcast_room(room)
        logger.info(
            "Subtitle added room=%s id=%s name=%s size=%d",
            room.room_id,
            subtitle.subtitle_id,
            subtitle.name,
            subtitle.size,
        )
        return subtitle

    async def delete_subtitle(self, room: Room, subtitle_id: str, host_token: str) -> None:
        if host_token != room.host_token:
            raise HallError("invalid host token", protocol.ERR_FORBIDDEN, 403)
        if room.ended:
            raise HallError("room has ended", protocol.ERR_ROOM_ENDED, 410)
        if room.subtitles.pop(subtitle_id, None) is None:
            raise HallError("subtitle not found", protocol.ERR_SUBTITLE_NOT_FOUND, 404)
        if room.active_subtitle_id == subtitle_id:
            room.active_subtitle_id = None
            room.active_subtitle_visible = True
        await self.broadcast_room(room)
        logger.info("Subtitle removed room=%s id=%s", room.room_id, subtitle_id)

    async def select_subtitle(
        self, room: Room, subtitle_id: Optional[str], visible: bool
    ) -> None:
        if subtitle_id is not None and not room.subtitle_available(subtitle_id):
            raise HallError("subtitle not found", protocol.ERR_SUBTITLE_NOT_FOUND, 404)
        room.active_subtitle_id = subtitle_id
        room.active_subtitle_visible = bool(visible) if subtitle_id else True
        await self.broadcast_room(room)
        logger.debug(
            "Subtitle selected room=%s id=%s visible=%s",
            room.room_id,
            subtitle_id or "-",
            room.active_subtitle_visible,
        )

    async def end_room(self, room: Room, host_token: str) -> None:
        if host_token != room.host_token:
            raise HallError("invalid host token", protocol.ERR_FORBIDDEN, 403)
        await self._end_room(room, code=4000, reason="room ended")

    async def _end_room(self, room: Room, code: int = 4000, reason: str = "room ended") -> None:
        self._cancel_host_offline_timer(room)
        if room.ended:
            return
        room.ended = True
        room.timeline.mark_ended()
        room.host_source.fail_all()
        room.clear_subtitles()
        self._ended_at[room.room_id] = time.time()
        await self.broadcast_room(room)
        await self._close_room(room, code=code, reason=reason)
        logger.info("Room ended id=%s", room.room_id)

    # -- host presence -------------------------------------------------
    def host_connected(self, room: Room) -> None:
        """Cancel a pending offline end when the host comes back."""
        self._cancel_host_offline_timer(room)

    def host_disconnected(self, room: Room) -> None:
        """End the room when its host does not come back in time.

        A closed host page drops the only copy of the media, so there is
        nothing left to serve; a short grace period still lets a reload or a
        network blip reconnect.
        """
        if room.ended or room.host_online:
            return
        self._cancel_host_offline_timer(room)
        self._host_offline_tasks[room.room_id] = asyncio.create_task(
            self._end_after_host_offline(room)
        )

    def _cancel_host_offline_timer(self, room: Room) -> None:
        task = self._host_offline_tasks.pop(room.room_id, None)
        if task is not None and not task.done():
            task.cancel()

    async def _end_after_host_offline(self, room: Room) -> None:
        try:
            await asyncio.sleep(self.config.host_offline_timeout)
        except asyncio.CancelledError:
            raise
        self._host_offline_tasks.pop(room.room_id, None)
        if room.ended or room.host_online:
            return
        if self.rooms.get(room.room_id) is not room:
            return
        logger.info(
            "Host stayed offline for %.0fs; ending room id=%s",
            self.config.host_offline_timeout,
            room.room_id,
        )
        await self._end_room(room, code=4000, reason="host offline")

    # -- broadcasting --------------------------------------------------
    def _connections(self, room: Room) -> List[Any]:
        connections: List[Any] = []
        if room.host_ws is not None:
            connections.append(room.host_ws)
        for member in room.members.values():
            if member.ws is not None:
                connections.append(member.ws)
        return connections

    async def send(self, ws: Any, payload: Dict[str, Any]) -> bool:
        try:
            await ws.send_json(payload)
            return True
        except Exception:
            return False

    async def broadcast(self, room: Room, payload: Dict[str, Any]) -> None:
        for ws in self._connections(room):
            await self.send(ws, payload)

    async def broadcast_room(self, room: Room) -> None:
        await self.broadcast(room, room.room_state())

    async def broadcast_host_event(self, room: Room, event: str) -> None:
        host = room.host_snapshot()
        payload_type = protocol.MSG_RESYNC if event == "resync" else event
        await self.broadcast(
            room,
            {
                "type": payload_type,
                "server_ts": round(time.time() * 1000.0, 3),
                "host": host,
                "host_name": room.host_name,
                "members": [m.public() for m in room.members.values()],
            },
        )

    def _notify_member_readonly(self, ws: Any) -> None:
        asyncio.create_task(
            self.send(
                ws,
                {
                    "type": protocol.MSG_ERROR,
                    "code": protocol.ERR_MEMBER_READONLY,
                    "message": "播放由房主控制",
                },
            )
        )

    # -- cache budget --------------------------------------------------
    def cache_total_bytes(self) -> int:
        return sum(room.cache.bytes_used for room in self.rooms.values())

    def enforce_global_cache(self) -> None:
        total = self.cache_total_bytes()
        guard = 0
        while total > self.config.cache_bytes_total and guard < 10000:
            guard += 1
            candidates = [room for room in self.rooms.values() if room.cache.oldest_tick() is not None]
            if not candidates:
                return
            victim = min(candidates, key=lambda r: r.cache.oldest_tick() or 0)
            freed = victim.cache.evict_one()
            if freed <= 0:
                return
            total -= freed

    # -- cleanup -------------------------------------------------------
    async def _close_room(self, room: Room, code: int = 1000, reason: str = "") -> None:
        for ws in self._connections(room):
            try:
                await ws.close(code=code, reason=reason)
            except Exception:
                pass
        room.clear_host()
        for member in room.members.values():
            member.ws = None

    def _remove_room(self, room: Room) -> None:
        self._cancel_host_offline_timer(room)
        self.rooms.pop(room.room_id, None)
        if self._codes.get(room.room_code) == room.room_id:
            self._codes.pop(room.room_code, None)
        self._ended_at.pop(room.room_id, None)
        self._stop_room_transcode(room)
        room.cache.clear()

    async def _pulse_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(HOST_PULSE_INTERVAL)
                for room in list(self.rooms.values()):
                    if room.ended or not room.host_online or not room.members:
                        continue
                    await self.broadcast_host_event(room, "state")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Media hall host pulse failed")

    async def _cleanup_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(30)
                await self.cleanup_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Media hall cleanup failed")

    async def cleanup_once(self) -> None:
        now = time.time()
        for room in list(self.rooms.values()):
            if room.ended:
                ended_at = self._ended_at.get(room.room_id, now)
                if now - ended_at > ENDED_ROOM_GRACE:
                    self._remove_room(room)
                continue

            stale_members = [
                member
                for member in room.members.values()
                if not member.connected and now - member.last_seen > self.config.member_idle_timeout
            ]
            for member in stale_members:
                room.members.pop(member.member_id, None)
                room.member_tokens.pop(member.token, None)
            if stale_members:
                await self.broadcast_room(room)

            if not room.has_audience() and now - room.last_active > self.config.room_idle_timeout:
                logger.info("Removing idle room id=%s", room.room_id)
                await self._close_room(room, code=4001, reason="idle timeout")
                self._remove_room(room)
