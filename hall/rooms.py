"""Room, member and media data structures."""

import asyncio
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


# How long a block read may wait for the host plane to load the media the room
# just switched to.  The host announces ``media_ready`` once its worker holds
# the new file; reads issued before that would otherwise be answered with bytes
# from the previous file under the new media id.
HOST_MEDIA_READY_TIMEOUT = 5.0


def _wall_ms() -> float:
    return time.time() * 1000.0


@dataclass
class MediaInfo:
    media_id: str
    kind: str  # host_file | url | jellyfin
    name: str
    size: int
    mime: str
    url: Optional[str] = None
    range_supported: bool = True
    # "direct" plays the bytes as they are; "hls" streams a Jellyfin transcode.
    playback_mode: str = "direct"
    # The item's real length.  For "hls" this is the only trustworthy duration,
    # because the transcoded playlist grows as it is encoded.
    duration: float = 0.0
    item_id: Optional[str] = None

    def public(self) -> Dict[str, Any]:
        return {
            "media_id": self.media_id,
            "kind": self.kind,
            "name": self.name,
            "size": self.size,
            "mime": self.mime,
            "range_supported": self.range_supported,
            "playback_mode": self.playback_mode,
            "duration": round(self.duration, 3),
            "item_id": self.item_id,
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
        media: Optional[MediaInfo],
        block_size: int,
        cache_bytes: int,
        host_read_concurrency: int,
        upstream: Optional[UpstreamSource] = None,
        jellyfin_source: Optional[Any] = None,
        hls: Optional[Any] = None,
    ) -> None:
        self.hall = hall
        self.room_id = "r_" + secrets.token_hex(8)
        self.room_code = hall.auth.new_room_code()
        self.name = name
        # A room may be created before the host picks anything to play.
        self.media = media
        self.playlist: Dict[str, MediaInfo] = {}
        self.playlist_order: List[str] = []
        if media is not None:
            self.playlist[media.media_id] = media
            self.playlist_order.append(media.media_id)
        self.host_token = HallAuth.new_token()
        self.created_at = time.time()
        self.last_active = self.created_at
        self.ended = False

        self.host_ws: Optional[Any] = None
        self.host_online = False
        self.host_connected_at = 0.0
        self.gen = 0
        # Media the host worker currently holds, and whether this host knows to
        # announce it (older clients do not, so their reads are not gated).
        self.host_media_id: Optional[str] = None
        self.host_media_capable = False

        self.timeline = HostTimeline()
        self.cache = BlockCache(block_size, cache_bytes)
        self.host_source = HostSource(
            self.room_id,
            block_size=block_size,
            concurrency=host_read_concurrency,
        )
        self.upstream = upstream
        # Jellyfin state.  The token lives here and nowhere a client can read.
        self.jellyfin: Optional[Any] = None
        self.jellyfin_source: Optional[Any] = jellyfin_source
        self.hls: Optional[Any] = hls
        # The Jellyfin MediaSource of the current item: its MediaStreams hold
        # the subtitle tracks this room can offer.
        self.jellyfin_item: Optional[Dict[str, Any]] = None
        self.jellyfin_subtitle_tracks: List[Dict[str, Any]] = []
        # Which track the transcode burns in: an index, or None with
        # ``jellyfin_subtitles_off`` set to ask Jellyfin for no subtitles.
        self.jellyfin_subtitle: Optional[int] = None
        self.jellyfin_subtitles_off: bool = False
        # Latest seek the host asked for while a restart was already running.
        self.pending_seek: float = 0.0
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

    def replace_media(
        self,
        media: MediaInfo,
        upstream: Optional[UpstreamSource] = None,
        jellyfin_source: Optional[Any] = None,
        hls: Optional[Any] = None,
    ) -> None:
        """Swap the room's media, dropping every block or read tied to the old file."""
        block_size = self.cache.block_size
        cache_bytes = self.cache.max_bytes
        self.media = media
        self.upstream = upstream
        self.jellyfin_source = jellyfin_source
        self.hls = hls
        # Subtitle state describes one Jellyfin item; anything else drops it.
        # (A Jellyfin build has just filled these in for the item it chose.)
        if media.kind != "jellyfin":
            self.jellyfin_item = None
            self.jellyfin_subtitle_tracks = []
            self.jellyfin_subtitle = None
            self.jellyfin_subtitles_off = False
        self.host_media_id = None
        self.cache = BlockCache(block_size, cache_bytes)
        # In-flight host reads belong to the old media; failing them also makes
        # any response still streaming from the old file stop quietly.
        self.new_generation()
        self.timeline = HostTimeline()
        self.reset_inactive_subtitle()
        self.auto_activate_scoped_subtitle()
        self.touch()

    def clear_media(self) -> None:
        """Drop the current media, e.g. when the last playlist entry is removed."""
        block_size = self.cache.block_size
        cache_bytes = self.cache.max_bytes
        self.media = None
        self.upstream = None
        self.jellyfin_source = None
        self.hls = None
        self.jellyfin_item = None
        self.jellyfin_subtitle_tracks = []
        self.jellyfin_subtitle = None
        self.jellyfin_subtitles_off = False
        self.host_media_id = None
        self.cache = BlockCache(block_size, cache_bytes)
        self.new_generation()
        self.timeline = HostTimeline()
        self.reset_inactive_subtitle()
        self.touch()

    # -- playlist ------------------------------------------------------
    def add_playlist_item(self, media: MediaInfo) -> None:
        self.playlist[media.media_id] = media
        self.playlist_order.append(media.media_id)
        self.touch()

    def remove_playlist_item(self, media_id: str) -> bool:
        if media_id not in self.playlist:
            return False
        self.playlist.pop(media_id, None)
        self.playlist_order = [item for item in self.playlist_order if item != media_id]
        return True

    def following_item_id(self, media_id: str) -> Optional[str]:
        """Next entry after ``media_id``, falling back to the one before it."""
        if media_id not in self.playlist_order:
            return None
        index = self.playlist_order.index(media_id)
        if index + 1 < len(self.playlist_order):
            return self.playlist_order[index + 1]
        if index > 0:
            return self.playlist_order[index - 1]
        return None

    def playlist_public(self) -> List[Dict[str, Any]]:
        active_id = self.media.media_id if self.media else None
        items = []
        for media_id in self.playlist_order:
            media = self.playlist.get(media_id)
            if media is None:
                continue
            item = media.public()
            item["active"] = media_id == active_id
            items.append(item)
        return items

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
        # A (re)connected worker has to tell us which media it holds.
        self.host_media_id = None
        self.host_source.attach(ws)
        self.touch()

    def clear_host(self, ws: Optional[Any] = None) -> None:
        if ws is not None and self.host_ws is not ws:
            return
        self.host_ws = None
        self.host_online = False
        self.host_media_id = None
        self.host_source.detach()
        self.touch()

    # -- host media plane ---------------------------------------------
    def mark_host_media(self, media_id: Optional[str]) -> None:
        """Remember which media the host worker now serves."""
        self.host_media_id = str(media_id) if media_id else None
        self.touch()

    def _host_media_ready(self) -> bool:
        if self.media is None or self.media.kind != "host_file":
            return True
        if not self.host_media_capable:
            # Legacy host: it never announces, so gate nothing.
            return True
        return self.host_media_id == self.media.media_id

    async def wait_host_media(self, timeout: Optional[float] = None) -> bool:
        """Wait until the host worker serves the media the room is pointing at.

        Without this the first reads after a playlist switch are answered from
        the previous file (under the new media id) and get cached, which makes
        the member's player fail permanently instead of recovering on retry.
        """
        if self._host_media_ready():
            return True
        if timeout is None:
            timeout = HOST_MEDIA_READY_TIMEOUT
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            await asyncio.sleep(0.02)
            if self._host_media_ready():
                return True
        return self._host_media_ready()

    # -- blocks --------------------------------------------------------
    async def load_block(self, index: int) -> bytes:
        if self.ended:
            raise SourceOffline("room has ended")
        if self.media is None:
            raise SourceOffline("no media selected")
        offset = index * self.host_source.block_size
        length = min(self.host_source.block_size, self.media.size - offset)
        if length <= 0:
            return b""
        if self.media.kind == "url":
            if self.upstream is None:
                raise SourceOffline("upstream is not configured")
            return await self.upstream.read(offset, length)
        if self.media.kind == "jellyfin":
            # A transcoded item has no byte range to serve: it is streamed as
            # HLS through the playlist endpoints instead.
            if self.jellyfin_source is None:
                raise SourceOffline("jellyfin source is not configured")
            return await self.jellyfin_source.read(offset, length)
        # After a playlist switch the host needs a moment to hand the new file
        # to its worker; reading before that returns the previous file's bytes.
        if not await self.wait_host_media():
            raise SourceOffline("host has not switched to the requested media")
        return await self.host_source.read(offset, length, self.gen)

    async def get_block(self, index: int) -> bytes:
        data = await self.cache.get_or_fetch(index, lambda: self.load_block(index))
        self.hall.enforce_global_cache()
        return data

    # -- subtitles -----------------------------------------------------
    def subtitle_list(self) -> List[Dict[str, Any]]:
        """Subtitles that apply to the current media.

        Room-level uploads (``media_id`` empty) are always listed; tracks pulled
        out of a playlist item only show up while that item is playing.
        """
        current = self.media.media_id if self.media else None
        return [
            subtitle.public(self.hall.auth.subtitle_url(self.room_id, subtitle.subtitle_id))
            for subtitle in self.subtitles.values()
            if subtitle.media_id is None or subtitle.media_id == current
        ]

    def subtitle_available(self, subtitle_id: str) -> bool:
        subtitle = self.subtitles.get(subtitle_id)
        if subtitle is None:
            return False
        current = self.media.media_id if self.media else None
        return subtitle.media_id is None or subtitle.media_id == current

    def drop_subtitles_for(self, media_id: str) -> None:
        for subtitle_id, subtitle in list(self.subtitles.items()):
            if subtitle.media_id == media_id:
                self.subtitles.pop(subtitle_id, None)
        self.reset_inactive_subtitle()

    def auto_activate_scoped_subtitle(self) -> None:
        """Show a track that belongs to the media now playing, if any."""
        if self.active_subtitle_id is not None or self.media is None:
            return
        for subtitle in self.subtitles.values():
            if subtitle.media_id == self.media.media_id:
                self.active_subtitle_id = subtitle.subtitle_id
                self.active_subtitle_visible = True
                return

    def reset_inactive_subtitle(self) -> None:
        """Forget the active subtitle once it no longer fits the current media."""
        if self.active_subtitle_id is None:
            return
        if not self.subtitle_available(self.active_subtitle_id):
            self.active_subtitle_id = None
            self.active_subtitle_visible = True

    def clear_subtitles(self) -> None:
        self.subtitles.clear()
        self.active_subtitle_id = None
        self.active_subtitle_visible = True

    # -- snapshots -----------------------------------------------------
    def host_snapshot(self) -> Dict[str, Any]:
        snapshot = self.timeline.snapshot()
        snapshot["online"] = self.host_online
        return snapshot

    @property
    def media_epoch(self) -> int:
        """Bumped every time the transcoder is restarted for a seek."""
        return int(self.hls.epoch) if self.hls is not None else 0

    @property
    def media_start(self) -> float:
        """Where in the item the current transcode begins, in seconds."""
        return float(self.hls.start_seconds) if self.hls is not None else 0.0

    def media_url(self) -> Optional[str]:
        if self.media is None:
            return None
        if self.media.playback_mode == "hls":
            return self.hall.auth.hls_playlist_url(
                self.room_id, self.media.media_id, self.media_epoch
            )
        return self.hall.auth.media_url(self.room_id, self.media.media_id)

    def room_state(self) -> Dict[str, Any]:
        return {
            "type": "room_state",
            "room_id": self.room_id,
            "room_code": self.room_code,
            "name": self.name,
            "server_ts": round(_wall_ms(), 3),
            "media": self.media.public() if self.media else None,
            "media_url": self.media_url(),
            "media_epoch": self.media_epoch,
            "media_start": round(self.media_start, 3),
            "playlist": self.playlist_public(),
            "host": self.host_snapshot(),
            "members": [m.public() for m in self.members.values()],
            "subtitles": self.subtitle_list(),
            "active_subtitle_id": self.active_subtitle_id,
            "active_subtitle_visible": self.active_subtitle_visible,
            "jellyfin": self.jellyfin.public() if self.jellyfin else None,
            "jellyfin_subtitles": self.jellyfin_subtitle_tracks,
            "jellyfin_subtitle": self.jellyfin_subtitle,
            "jellyfin_subtitles_off": self.jellyfin_subtitles_off,
            "ended": self.ended,
        }

    def public_summary(self) -> Dict[str, Any]:
        """Small, token-free view of a room for the public room list."""
        return {
            "room_code": self.room_code,
            "name": self.name,
            "media_name": self.media.name if self.media else None,
            "media_size": self.media.size if self.media else 0,
            "playlist_count": len(self.playlist_order),
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
            "media": self.media.public() if self.media else None,
            "media_url": self.media_url(),
            "media_epoch": self.media_epoch,
            "media_start": round(self.media_start, 3),
            "playlist": self.playlist_public(),
            "host": self.host_snapshot(),
            "member_count": self.member_count(),
            "members": [m.public() for m in self.members.values()],
            "subtitles": self.subtitle_list(),
            "active_subtitle_id": self.active_subtitle_id,
            "active_subtitle_visible": self.active_subtitle_visible,
            "jellyfin": self.jellyfin.public() if self.jellyfin else None,
            "jellyfin_subtitles": self.jellyfin_subtitle_tracks,
            "jellyfin_subtitle": self.jellyfin_subtitle,
            "jellyfin_subtitles_off": self.jellyfin_subtitles_off,
            "cache": {
                "bytes": self.cache.bytes_used,
                "hits": self.cache.hits,
                "misses": self.cache.misses,
            },
        }

    def media_list(self) -> List[Dict[str, Any]]:
        return self.playlist_public()
