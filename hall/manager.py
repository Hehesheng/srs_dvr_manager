"""Media hall lifecycle: rooms, members, cache budget and cleanup."""

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import aiohttp
import yaml

from . import protocol
from .auth import HallAuth
from .rooms import MediaInfo, Member, Room
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
    "media_url_ttl": 86400,
    "max_media_bytes": 64 * 1024 * 1024 * 1024,
}
ENDED_ROOM_GRACE = 60.0
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
    media_url_ttl: int = 86400
    max_media_bytes: int = 64 * 1024 * 1024 * 1024
    signing_secret: str = ""

    @classmethod
    def load(cls, config_path: str = "config.yaml") -> "HallConfig":
        raw: Dict[str, Any] = {}
        if os.path.exists(config_path):
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    payload = yaml.safe_load(f) or {}
                raw = payload.get("hall", {}) or {}
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
            media_url_ttl=max(60, int(merged["media_url_ttl"])),
            max_media_bytes=max(1024 * 1024, int(merged["max_media_bytes"])),
            signing_secret=str(merged.get("signing_secret") or ""),
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
        self._session: Optional[aiohttp.ClientSession] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._pulse_task: Optional[asyncio.Task] = None
        self._started = False

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
            "Media hall started: max_rooms=%d block=%d cache_room=%d cache_total=%d",
            self.config.max_rooms,
            self.config.block_size,
            self.config.cache_bytes_per_room,
            self.config.cache_bytes_total,
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
        for room in list(self.rooms.values()):
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

    # -- creation / join ----------------------------------------------
    async def create_room(self, name: str, media_payload: Dict[str, Any]) -> Room:
        if not self.enabled:
            raise HallError("hall is disabled")
        active = [r for r in self.rooms.values() if not r.ended]
        if len(active) >= self.config.max_rooms:
            raise HallError("too many rooms", "TOO_MANY_ROOMS", 429)

        room_name = (name or "Media Room").strip()[:64] or "Media Room"
        kind = str(media_payload.get("kind") or "host_file")
        media_name = str(media_payload.get("name") or "").strip()[:256]
        if kind not in ("host_file", "url"):
            raise HallError("media.kind must be host_file or url")

        media_id = "m_" + os.urandom(8).hex()
        upstream: Optional[UpstreamSource] = None

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

        room = Room(
            hall=self,
            name=room_name,
            media=media,
            block_size=self.config.block_size,
            cache_bytes=self.config.cache_bytes_per_room,
            host_read_concurrency=self.config.host_read_concurrency,
            upstream=upstream,
        )
        while room.room_code in self._codes:
            room.room_code = self.auth.new_room_code()
        self.rooms[room.room_id] = room
        self._codes[room.room_code] = room.room_id
        logger.info(
            "Room created id=%s code=%s media=%s size=%d kind=%s",
            room.room_id,
            room.room_code,
            room.media.name,
            room.media.size,
            room.media.kind,
        )
        return room

    async def join_room(self, ref: str, member_name: str) -> Member:
        room = self.require_room(ref)
        if room.member_count() >= self.config.max_members_per_room:
            raise HallError("room is full", protocol.ERR_ROOM_FULL, 409)
        member = room.add_member((member_name or "Guest").strip()[:32])
        await self.broadcast_room(room)
        return member

    async def end_room(self, room: Room, host_token: str) -> None:
        if host_token != room.host_token:
            raise HallError("invalid host token", protocol.ERR_FORBIDDEN, 403)
        if room.ended:
            return
        room.ended = True
        room.timeline.mark_ended()
        room.host_source.fail_all()
        self._ended_at[room.room_id] = time.time()
        await self.broadcast_room(room)
        await self._close_room(room, code=4000, reason="room ended")
        logger.info("Room ended id=%s", room.room_id)

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
        self.rooms.pop(room.room_id, None)
        if self._codes.get(room.room_code) == room.room_id:
            self._codes.pop(room.room_code, None)
        self._ended_at.pop(room.room_id, None)
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
