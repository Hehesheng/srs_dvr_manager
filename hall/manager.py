"""Media hall lifecycle: rooms, members, cache budget and cleanup."""

import asyncio
import logging
import os
import time
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
    direct_mime,
    duration_seconds,
    item_name,
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
    "url": "",
    "max_bitrate": 8_000_000,
    "seek_debounce_ms": 800,
}


@dataclass
class BuiltMedia:
    """A media item plus whatever source object it needs to be served."""

    media: MediaInfo
    upstream: Optional[UpstreamSource] = None
    jellyfin_source: Optional[JellyfinSource] = None
    hls: Optional[HlsSession] = None


@dataclass
class JellyfinConfig:
    """Where the Jellyfin server lives, and how hard we are allowed to pull."""

    enabled: bool = False
    url: str = ""
    max_bitrate: int = 8_000_000
    seek_debounce_ms: int = 800

    @classmethod
    def from_payload(cls, payload: Optional[Dict[str, Any]]) -> "JellyfinConfig":
        merged = dict(JELLYFIN_DEFAULTS)
        if isinstance(payload, dict):
            merged.update({k: v for k, v in payload.items() if v is not None})
        url = str(merged.get("url") or "").strip().rstrip("/")
        enabled = bool(merged.get("enabled")) and url.startswith(("http://", "https://"))
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
            url=url,
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
        self.jellyfin: Optional[JellyfinClient] = None
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
        if self.config.jellyfin.enabled:
            self.jellyfin = JellyfinClient(
                self._session,
                self.config.jellyfin.url,
                self.config.jellyfin.max_bitrate,
            )
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        self._pulse_task = asyncio.create_task(self._pulse_loop())
        logger.info(
            "Media hall started: max_rooms=%d block=%d cache_room=%d cache_total=%d jellyfin=%s",
            self.config.max_rooms,
            self.config.block_size,
            self.config.cache_bytes_per_room,
            self.config.cache_bytes_total,
            "on" if self.jellyfin else "off",
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
        self, name: str, media_payload: Optional[Dict[str, Any]] = None
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
        while room.room_code in self._codes:
            room.room_code = self.auth.new_room_code()
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
        if self.jellyfin is None:
            raise HallError(
                "Jellyfin 源未启用", protocol.ERR_JELLYFIN_UNAVAILABLE, 503
            )
        if room is None or room.jellyfin is None:
            raise HallError(
                "请先登录 Jellyfin", protocol.ERR_JELLYFIN_AUTH_REQUIRED, 401
            )
        item_id = str(media_payload.get("item_id") or "").strip()
        if not item_id or not item_id.replace("-", "").isalnum():
            raise HallError("缺少有效的 Jellyfin 视频 ID")

        try:
            item = await self.jellyfin.item(room.jellyfin, item_id)
        except JellyfinAuthError:
            room.jellyfin = None
            raise HallError(
                "Jellyfin 登录已失效，请重新登录",
                protocol.ERR_JELLYFIN_AUTH_FAILED,
                401,
            )
        except JellyfinError as exc:
            raise HallError(exc.message, exc.code, exc.status_code)

        sources = item.get("MediaSources") or []
        if not sources:
            raise HallError("这个条目没有可播放的文件", "MEDIA_NOT_FOUND", 404)
        source = sources[0]
        mode = playback_mode(source)
        duration = duration_seconds(item)
        name = item_name(item)
        room.jellyfin_item = source
        room.jellyfin_subtitle_tracks = subtitle_streams(source)
        room.jellyfin_subtitle = None
        room.jellyfin_subtitles_off = False

        if mode == "direct":
            # Direct play gets real WebVTT tracks; there is no transcode to burn
            # subtitles into, so the picker has nothing to choose.
            room.jellyfin_subtitle_tracks = []
            try:
                size = await self.jellyfin.probe_size(room.jellyfin, item_id)
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
                    self.jellyfin, room.jellyfin, item_id, size, mime
                ),
            )

        try:
            hls = await self.jellyfin.open_transcode(room.jellyfin, item_id, 0.0, 1)
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
        return BuiltMedia(media=media, hls=hls)

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
            if self.jellyfin is None or room.jellyfin is None:
                raise HallError(
                    "Jellyfin 登录已失效，请重新登录",
                    protocol.ERR_JELLYFIN_AUTH_REQUIRED,
                    401,
                )
            item_id = media.item_id or ""
            if media.playback_mode == "hls":
                try:
                    hls = await self.jellyfin.open_transcode(
                        room.jellyfin, item_id, 0.0, room.media_epoch + 1
                    )
                except JellyfinError as exc:
                    raise HallError(exc.message, exc.code, exc.status_code)
                return BuiltMedia(media=media, hls=hls)
            return BuiltMedia(
                media=media,
                jellyfin_source=JellyfinSource(
                    self.jellyfin, room.jellyfin, item_id, media.size, media.mime
                ),
            )
        return BuiltMedia(media=media)

    # -- jellyfin ------------------------------------------------------
    def _require_jellyfin(self) -> JellyfinClient:
        if self.jellyfin is None:
            raise HallError(
                "Jellyfin 源未启用", protocol.ERR_JELLYFIN_UNAVAILABLE, 503
            )
        return self.jellyfin

    async def jellyfin_login(
        self, room: Room, host_token: str, username: str, password: str
    ) -> JellyfinSession:
        self._require_host(room, host_token)
        client = self._require_jellyfin()
        device_id = f"srs-hall-{room.room_id}"
        try:
            session = await client.authenticate(username, password, device_id)
        except JellyfinError as exc:
            raise HallError(exc.message, exc.code, exc.status_code)
        room.jellyfin = session
        room.touch()
        logger.info(
            "Jellyfin login room=%s user=%s", room.room_id, session.user_name
        )
        return session

    async def jellyfin_attach_token(
        self, room: Room, host_token: str, token: str, user_id: Optional[str] = None
    ) -> JellyfinSession:
        """Adopt a token the browser remembered from an earlier login."""
        self._require_host(room, host_token)
        client = self._require_jellyfin()
        device_id = f"srs-hall-{room.room_id}"
        try:
            session = await client.verify(token, device_id, user_id)
        except JellyfinError as exc:
            raise HallError(exc.message, exc.code, exc.status_code)
        room.jellyfin = session
        room.touch()
        return session

    async def jellyfin_libraries(self, room: Room) -> List[Dict[str, Any]]:
        client = self._require_jellyfin()
        if room.jellyfin is None:
            raise HallError(
                "请先登录 Jellyfin", protocol.ERR_JELLYFIN_AUTH_REQUIRED, 401
            )
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
    ) -> Dict[str, Any]:
        # The library listing belongs to the host, not to whoever joined the
        # room; only the host may ask what is in it.
        self._require_host(room, host_token)
        client = self._require_jellyfin()
        if room.jellyfin is None:
            raise HallError(
                "请先登录 Jellyfin", protocol.ERR_JELLYFIN_AUTH_REQUIRED, 401
            )
        try:
            return await client.items(room.jellyfin, parent_id, query, start, limit)
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
        delay = self.config.jellyfin.seek_debounce_ms / 1000.0
        try:
            while True:
                target = room.pending_seek
                if delay > 0:
                    await asyncio.sleep(delay)
                if room.pending_seek != target:
                    continue
                break
            await self._restart_transcode(room, target)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Jellyfin seek restart failed room=%s", room.room_id)
        finally:
            if self._seek_tasks.get(room.room_id) is asyncio.current_task():
                self._seek_tasks.pop(room.room_id, None)

    async def _restart_transcode(self, room: Room, position: float) -> None:
        client = self.jellyfin
        session = room.jellyfin
        old = room.hls
        if client is None or session is None or old is None or room.ended:
            return
        try:
            fresh = await client.open_transcode(
                session,
                old.item_id,
                position,
                old.epoch + 1,
                subtitle_index=room.jellyfin_subtitle,
                subtitles_off=room.jellyfin_subtitles_off,
            )
        except JellyfinAuthError:
            room.jellyfin = None
            logger.info("Jellyfin seek needs a new login room=%s", room.room_id)
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
        client = self.jellyfin
        session = room.jellyfin
        if client is None or session is None or room.hls is None or room.media is None:
            raise HallError(
                "转码会话不存在", protocol.ERR_JELLYFIN_TRANSCODE_FAILED, 404
            )
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
        client = self.jellyfin
        session = room.jellyfin
        if client is None or session is None or room.hls is None:
            raise HallError(
                "转码会话不存在", protocol.ERR_JELLYFIN_TRANSCODE_FAILED, 404
            )
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
        if hls is not None and session is not None and self.jellyfin is not None:
            asyncio.create_task(self.jellyfin.stop_transcode(session, hls))

    def _spawn_jellyfin_subtitles(self, room: Room, media: MediaInfo) -> None:
        """Pull the item's text subtitle tracks in the background."""
        if media.kind != "jellyfin" or self.jellyfin is None or room.jellyfin is None:
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
        if session is None or self.jellyfin is None:
            return
        source_id = str((room.jellyfin_item or {}).get("Id") or media.item_id or "")
        for track in tracks:
            if room.ended or room.media is None or room.media.media_id != media.media_id:
                return
            try:
                index = int(track.get("Index"))
            except (TypeError, ValueError):
                continue
            text = await self.jellyfin.subtitle_vtt(session, media.item_id or "", source_id, index)
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
