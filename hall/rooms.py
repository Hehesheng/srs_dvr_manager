"""Room, member and media data structures."""

import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .auth import HallAuth
from .cache import BlockCache
from .host_source import HostSource, SourceOffline
from .subtitles import Subtitle
from .timeline import HostTimeline
from .upstream import UpstreamSource


def _wall_ms() -> float:
    return time.time() * 1000.0


@dataclass
class MediaInfo:
    media_id: str
    kind: str  # host_file | url
    name: str
    size: int
    mime: str
    url: Optional[str] = None
    range_supported: bool = True

    def public(self) -> Dict[str, Any]:
        return {
            "media_id": self.media_id,
            "kind": self.kind,
            "name": self.name,
            "size": self.size,
            "mime": self.mime,
            "range_supported": self.range_supported,
        }


@dataclass
class Member:
    member_id: str
    name: str
    token: str
    joined_at: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    ws: Optional[Any] = None
    connected_at: float = 0.0
    status: Dict[str, Any] = field(default_factory=dict)

    @property
    def connected(self) -> bool:
        return self.ws is not None

    def public(self) -> Dict[str, Any]:
        return {
            "id": self.member_id,
            "name": self.name,
            "connected": self.connected,
        }


class Room:
    def __init__(
        self,
        hall: Any,
        name: str,
        media: MediaInfo,
        block_size: int,
        cache_bytes: int,
        host_read_concurrency: int,
        upstream: Optional[UpstreamSource] = None,
    ) -> None:
        self.hall = hall
        self.room_id = "r_" + secrets.token_hex(8)
        self.room_code = hall.auth.new_room_code()
        self.name = name
        self.media = media
        self.host_token = HallAuth.new_token()
        self.created_at = time.time()
        self.last_active = self.created_at
        self.ended = False

        self.host_ws: Optional[Any] = None
        self.host_online = False
        self.host_connected_at = 0.0
        self.gen = 0

        self.timeline = HostTimeline()
        self.cache = BlockCache(block_size, cache_bytes)
        self.host_source = HostSource(
            self.room_id,
            block_size=block_size,
            concurrency=host_read_concurrency,
        )
        self.upstream = upstream
        self.members: Dict[str, Member] = {}
        self.member_tokens: Dict[str, str] = {}
        self.subtitles: Dict[str, Subtitle] = {}
        self.active_subtitle_id: Optional[str] = None
        self.active_subtitle_visible: bool = True

    # -- lifecycle -----------------------------------------------------
    def touch(self) -> None:
        self.last_active = time.time()

    def has_audience(self) -> bool:
        return self.host_online or any(m.connected for m in self.members.values())

    def replace_media(self, media: MediaInfo, upstream: Optional[UpstreamSource] = None) -> None:
        """Swap the room's media, dropping every block or read tied to the old file."""
        block_size = self.cache.block_size
        cache_bytes = self.cache.max_bytes
        self.media = media
        self.upstream = upstream
        self.cache = BlockCache(block_size, cache_bytes)
        # In-flight host reads belong to the old media; failing them also makes
        # any response still streaming from the old file stop quietly.
        self.new_generation()
        self.timeline = HostTimeline()
        self.touch()

    # -- members -------------------------------------------------------
    def add_member(self, name: str) -> Member:
        member_id = "m_" + secrets.token_hex(8)
        token = HallAuth.new_token()
        member = Member(member_id=member_id, name=name or "Guest", token=token)
        self.members[member_id] = member
        self.member_tokens[token] = member_id
        self.touch()
        return member

    def member_by_token(self, token: str) -> Optional[Member]:
        member_id = self.member_tokens.get(token)
        if member_id is None:
            return None
        return self.members.get(member_id)

    def member_count(self) -> int:
        return len(self.members)

    # -- host ----------------------------------------------------------
    def new_generation(self) -> int:
        self.gen = (self.gen + 1) & 0xFFFFFFFF
        self.host_source.supersede()
        return self.gen

    def set_host(self, ws: Any) -> None:
        self.host_ws = ws
        self.host_online = True
        self.host_connected_at = time.time()
        self.host_source.attach(ws)
        self.touch()

    def clear_host(self, ws: Optional[Any] = None) -> None:
        if ws is not None and self.host_ws is not ws:
            return
        self.host_ws = None
        self.host_online = False
        self.host_source.detach()
        self.touch()

    # -- blocks --------------------------------------------------------
    async def load_block(self, index: int) -> bytes:
        offset = index * self.host_source.block_size
        length = min(self.host_source.block_size, self.media.size - offset)
        if length <= 0:
            return b""
        if self.ended:
            raise SourceOffline("room has ended")
        if self.media.kind == "url":
            if self.upstream is None:
                raise SourceOffline("upstream is not configured")
            return await self.upstream.read(offset, length)
        return await self.host_source.read(offset, length, self.gen)

    async def get_block(self, index: int) -> bytes:
        data = await self.cache.get_or_fetch(index, lambda: self.load_block(index))
        self.hall.enforce_global_cache()
        return data

    # -- subtitles -----------------------------------------------------
    def subtitle_list(self) -> List[Dict[str, Any]]:
        return [
            subtitle.public(self.hall.auth.subtitle_url(self.room_id, subtitle.subtitle_id))
            for subtitle in self.subtitles.values()
        ]

    def clear_subtitles(self) -> None:
        self.subtitles.clear()
        self.active_subtitle_id = None
        self.active_subtitle_visible = True

    # -- snapshots -----------------------------------------------------
    def host_snapshot(self) -> Dict[str, Any]:
        snapshot = self.timeline.snapshot()
        snapshot["online"] = self.host_online
        return snapshot

    def room_state(self) -> Dict[str, Any]:
        return {
            "type": "room_state",
            "room_id": self.room_id,
            "room_code": self.room_code,
            "name": self.name,
            "server_ts": round(_wall_ms(), 3),
            "media": self.media.public(),
            "media_url": self.hall.auth.media_url(self.room_id, self.media.media_id),
            "host": self.host_snapshot(),
            "members": [m.public() for m in self.members.values()],
            "subtitles": self.subtitle_list(),
            "active_subtitle_id": self.active_subtitle_id,
            "active_subtitle_visible": self.active_subtitle_visible,
            "ended": self.ended,
        }

    def public_summary(self) -> Dict[str, Any]:
        """Small, token-free view of a room for the public room list."""
        return {
            "room_code": self.room_code,
            "name": self.name,
            "media_name": self.media.name,
            "media_size": self.media.size,
            "member_count": self.member_count(),
            "host_online": self.host_online,
            "created_at": round(self.created_at, 3),
        }

    def public_info(self) -> Dict[str, Any]:
        return {
            "room_id": self.room_id,
            "room_code": self.room_code,
            "name": self.name,
            "ended": self.ended,
            "media": self.media.public(),
            "host": self.host_snapshot(),
            "member_count": self.member_count(),
            "members": [m.public() for m in self.members.values()],
            "subtitles": self.subtitle_list(),
            "active_subtitle_id": self.active_subtitle_id,
            "active_subtitle_visible": self.active_subtitle_visible,
            "cache": {
                "bytes": self.cache.bytes_used,
                "hits": self.cache.hits,
                "misses": self.cache.misses,
            },
        }

    def media_list(self) -> List[Dict[str, Any]]:
        return [self.media.public()]
