"""Jellyfin as a hall media source: login, browsing, direct play and HLS.

Everything here talks to exactly one server: the base URL from the hall
config.  Requests are never built from URLs supplied by a browser, so this
module deliberately does not go through ``upstream.validate_public_url`` --
that guard exists for arbitrary public URLs, and it would also reject a NAS
sitting on a private address.

The caller's Jellyfin token is the only credential involved.  It is kept in
memory on the room and must never reach a member: members only ever see the
hall's own signed URLs.
"""

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urljoin, urlparse

import aiohttp

logger = logging.getLogger(__name__)

CLIENT_NAME = "SRS Media Hall"
DEVICE_NAME = "SRS Media Hall"
CLIENT_VERSION = "1.0.0"

# Containers and codecs every browser we support can decode.  Anything else is
# handed to Jellyfin for transcoding instead of being played byte for byte.
DIRECT_CONTAINERS = {"mp4", "m4v", "webm", "mov"}
DIRECT_VIDEO = {"h264", "avc1", "av1", "vp8", "vp9"}
DIRECT_AUDIO = {"aac", "mp3", "opus", "vorbis"}
# Text tracks Jellyfin can convert to WebVTT; PGS/VOBSUB are images and cannot.
TEXT_SUBTITLE_CODECS = {"ass", "ssa", "srt", "subrip", "vtt", "webvtt", "mov_text"}

MIME_BY_CONTAINER = {
    "mp4": "video/mp4",
    "m4v": "video/mp4",
    "mov": "video/quicktime",
    "webm": "video/webm",
    "mkv": "video/x-matroska",
}

PLAYLIST_TIMEOUT = 20.0
SEGMENT_TIMEOUT = 45.0
SEGMENT_WAIT = 20.0


class JellyfinError(Exception):
    status_code = 502
    code = "JELLYFIN_UNAVAILABLE"

    def __init__(self, message: str, code: Optional[str] = None, status_code: Optional[int] = None):
        super().__init__(message)
        self.message = message
        if code:
            self.code = code
        if status_code:
            self.status_code = status_code


class JellyfinAuthError(JellyfinError):
    status_code = 401
    code = "JELLYFIN_AUTH_FAILED"


@dataclass
class JellyfinSession:
    """One host's authenticated view of the Jellyfin server."""

    token: str
    user_id: str
    user_name: str
    device_id: str

    def public(self) -> Dict[str, Any]:
        # Never includes the token: this dict is safe to broadcast.
        return {"user_name": self.user_name, "user_id": self.user_id}


@dataclass
class HlsSession:
    """A single transcoding session shared by everybody in one room."""

    item_id: str
    media_source_id: str
    play_session_id: str
    live_url: str
    start_seconds: float
    epoch: int = 0
    target_duration: float = 3.0
    segments: Dict[int, str] = field(default_factory=dict)
    durations: Dict[int, float] = field(default_factory=dict)
    complete: bool = False
    refreshed_at: float = 0.0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    def public(self) -> Dict[str, Any]:
        return {"epoch": self.epoch, "start_seconds": round(self.start_seconds, 3)}


def _strip_override_tags(text: str) -> str:
    text = re.sub(r"\{[^{}]*\\[^{}]*\}", "", text)
    return re.sub(r"<[^>]{1,40}>", "", text)


def clean_vtt(raw: str) -> str:
    """Turn Jellyfin's ASS-derived WebVTT into something a <track> can show.

    The conversion keeps raw ASS override tags (``{\\fad(200,200)}{\\an8}``),
    a ``Region:`` header and cue-local layout hints; browsers render all of
    that as literal text.
    """
    text = raw.replace("\r\n", "\n").replace("\r", "\n").lstrip("﻿")
    out: List[str] = ["WEBVTT", ""]
    seen_cue = False
    for line in text.split("\n"):
        stripped = line.strip()
        if not stripped:
            out.append("")
            continue
        upper = stripped.upper()
        if not seen_cue and (
            upper.startswith("REGION:")
            or upper.startswith("WEBVTT")
            or upper.startswith("NOTE")
            or upper.startswith("STYLE")
        ):
            continue
        if "-->" in stripped:
            seen_cue = True
            left = stripped.split("-->", 1)[0].strip()
            right = stripped.split("-->", 1)[1].strip().split(" ")[0].strip()
            out.append(f"{left} --> {right}")
            continue
        cleaned = _strip_override_tags(line).strip()
        if cleaned:
            out.append(cleaned)
    # Collapse the blank runs the stripping leaves behind.
    result: List[str] = []
    for line in out:
        if line == "" and result and result[-1] == "":
            continue
        result.append(line)
    return "\n".join(result).strip() + "\n"


def media_streams(media_source: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [s for s in (media_source.get("MediaStreams") or []) if isinstance(s, dict)]


def playback_mode(media_source: Dict[str, Any]) -> str:
    """``direct`` when the browser can decode this file on its own."""
    container = str(media_source.get("Container") or "").lower()
    streams = media_streams(media_source)
    videos = [s for s in streams if s.get("Type") == "Video"]
    audios = [s for s in streams if s.get("Type") == "Audio"]
    if container not in DIRECT_CONTAINERS:
        return "hls"
    if not videos:
        return "hls"
    if str(videos[0].get("Codec") or "").lower() not in DIRECT_VIDEO:
        return "hls"
    if audios and str(audios[0].get("Codec") or "").lower() not in DIRECT_AUDIO:
        return "hls"
    return "direct"


def direct_mime(media_source: Dict[str, Any]) -> str:
    container = str(media_source.get("Container") or "").lower()
    for candidate in container.split(","):
        mime = MIME_BY_CONTAINER.get(candidate.strip())
        if mime:
            return mime
    return "video/mp4"


def text_subtitle_streams(media_source: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        stream
        for stream in media_streams(media_source)
        if stream.get("Type") == "Subtitle"
        and str(stream.get("Codec") or "").lower() in TEXT_SUBTITLE_CODECS
        and stream.get("Index") is not None
    ]


def duration_seconds(item: Dict[str, Any]) -> float:
    ticks = item.get("RunTimeTicks")
    try:
        return max(0.0, float(ticks) / 10_000_000.0)
    except (TypeError, ValueError):
        return 0.0


def item_name(item: Dict[str, Any]) -> str:
    name = str(item.get("Name") or "").strip()
    year = item.get("ProductionYear")
    if year and name:
        return f"{name} ({year})"
    return name or "Jellyfin 视频"


class JellyfinClient:
    def __init__(self, session: aiohttp.ClientSession, base_url: str, max_bitrate: int) -> None:
        self.session = session
        self.base_url = (base_url or "").rstrip("/")
        self.max_bitrate = max(1_000_000, int(max_bitrate))

    # -- plumbing ------------------------------------------------------
    def _headers(self, token: Optional[str], device_id: str) -> Dict[str, str]:
        authorization = (
            f'MediaBrowser Client="{CLIENT_NAME}", Device="{DEVICE_NAME}", '
            f'DeviceId="{device_id or "srs-hall"}", Version="{CLIENT_VERSION}"'
        )
        headers = {"Authorization": authorization, "Accept": "application/json"}
        if token:
            headers["X-Emby-Token"] = token
        return headers

    def absolute(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return f"{self.base_url}/{path.lstrip('/')}"

    async def _json(
        self,
        method: str,
        path: str,
        session: Optional[JellyfinSession] = None,
        *,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        timeout: float = 20.0,
    ) -> Any:
        url = self.absolute(path)
        if params:
            url = f"{url}{'&' if '?' in url else '?'}{urlencode(params)}"
        token = session.token if session else None
        device_id = session.device_id if session else "srs-hall"
        try:
            async with self.session.request(
                method,
                url,
                headers=self._headers(token, device_id),
                json=body,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as response:
                if response.status == 401:
                    raise JellyfinAuthError("Jellyfin 认证已失效，请重新登录")
                if response.status >= 400:
                    detail = (await response.text())[:200]
                    raise JellyfinError(
                        f"Jellyfin 返回 HTTP {response.status}: {detail}", "JELLYFIN_UNAVAILABLE"
                    )
                if response.content_type == "application/json":
                    return await response.json(content_type=None)
                return await response.text()
        except JellyfinError:
            raise
        except asyncio.TimeoutError:
            raise JellyfinError("连接 Jellyfin 超时", "JELLYFIN_UNAVAILABLE", 504)
        except aiohttp.ClientError as exc:
            raise JellyfinError(f"连接 Jellyfin 失败: {exc}", "JELLYFIN_UNAVAILABLE", 502)

    # -- login ---------------------------------------------------------
    async def authenticate(
        self, username: str, password: str, device_id: str
    ) -> JellyfinSession:
        url = self.absolute("/Users/AuthenticateByName")
        try:
            async with self.session.post(
                url,
                headers={**self._headers(None, device_id), "Content-Type": "application/json"},
                json={"Username": username, "Pw": password},
                timeout=aiohttp.ClientTimeout(total=20),
            ) as response:
                if response.status in (401, 403):
                    raise JellyfinAuthError("Jellyfin 用户名或密码不正确")
                if response.status >= 400:
                    raise JellyfinError(
                        f"Jellyfin 登录失败（HTTP {response.status}）", "JELLYFIN_UNAVAILABLE"
                    )
                payload = await response.json(content_type=None)
        except JellyfinError:
            raise
        except asyncio.TimeoutError:
            raise JellyfinError("连接 Jellyfin 超时", "JELLYFIN_UNAVAILABLE", 504)
        except aiohttp.ClientError as exc:
            raise JellyfinError(f"连接 Jellyfin 失败: {exc}", "JELLYFIN_UNAVAILABLE", 502)

        token = str(payload.get("AccessToken") or "")
        user = payload.get("User") or {}
        if not token or not user.get("Id"):
            raise JellyfinAuthError("Jellyfin 没有返回有效的访问令牌")
        return JellyfinSession(
            token=token,
            user_id=str(user.get("Id")),
            user_name=str(user.get("Name") or username),
            device_id=device_id,
        )

    async def verify(
        self, token: str, device_id: str, user_id: Optional[str] = None
    ) -> JellyfinSession:
        """Check a token the browser remembered, and read back who it belongs to.

        ``/Users/Me`` is not reliable across server builds (10.10 answers 400
        when the token belongs to an API key rather than a signed-in user), so
        the browser hands back the user id it was given at login and we look
        that up directly.
        """
        probe = JellyfinSession(token=token, user_id=user_id or "", user_name="", device_id=device_id)
        if user_id:
            payload = await self._json("GET", f"/Users/{user_id}", probe)
            if not isinstance(payload, dict) or str(payload.get("Id") or "") != user_id:
                raise JellyfinAuthError("Jellyfin 认证已失效，请重新登录")
        else:
            users = await self._json("GET", "/Users", probe)
            candidates = [
                user for user in (users or []) if isinstance(user, dict) and user.get("Id")
            ]
            if len(candidates) != 1:
                # More than one account: only the host knows which one it is.
                raise JellyfinAuthError("记住的登录已失效，请重新登录", "JELLYFIN_AUTH_FAILED", 401)
            payload = candidates[0]
            user_id = str(payload.get("Id"))
        return JellyfinSession(
            token=token,
            user_id=user_id,
            user_name=str(payload.get("Name") or ""),
            device_id=device_id,
        )

    # -- browsing ------------------------------------------------------
    async def libraries(self, session: JellyfinSession) -> List[Dict[str, Any]]:
        payload = await self._json("GET", "/Library/VirtualFolders", session)
        result = []
        for folder in payload or []:
            collection = str(folder.get("CollectionType") or "")
            item_id = str(folder.get("ItemId") or "")
            if not item_id:
                continue
            result.append(
                {
                    "id": item_id,
                    "name": str(folder.get("Name") or "媒体库"),
                    "collection_type": collection,
                }
            )
        return result

    async def items(
        self,
        session: JellyfinSession,
        parent_id: Optional[str] = None,
        query: str = "",
        start: int = 0,
        limit: int = 60,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "Recursive": "true",
            "IncludeItemTypes": "Movie,Episode,Video,MusicVideo",
            "Fields": "MediaSources,RunTimeTicks",
            "SortBy": "SortName",
            "SortOrder": "Ascending",
            "StartIndex": max(0, int(start)),
            "Limit": min(200, max(1, int(limit))),
            "ImageTypeLimit": 0,
            "EnableImages": "false",
        }
        if parent_id:
            params["ParentId"] = parent_id
        if query.strip():
            params["SearchTerm"] = query.strip()[:64]
            params["Recursive"] = "true"
        payload = await self._json(
            "GET", f"/Users/{session.user_id}/Items", session, params=params
        )
        return self._project_items(payload or {})

    def _project_items(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        items = []
        for item in payload.get("Items") or []:
            sources = item.get("MediaSources") or []
            source = sources[0] if sources else {}
            mode = playback_mode(source) if source else "hls"
            items.append(
                {
                    "id": str(item.get("Id") or ""),
                    "name": item_name(item),
                    "type": str(item.get("Type") or ""),
                    "mode": mode,
                    "duration": round(duration_seconds(item), 3),
                    "bitrate": int(source.get("Bitrate") or 0),
                    "container": str(source.get("Container") or ""),
                }
            )
        return {"items": items, "total": int(payload.get("TotalRecordCount") or len(items))}

    async def item(self, session: JellyfinSession, item_id: str) -> Dict[str, Any]:
        payload = await self._json(
            "GET",
            f"/Users/{session.user_id}/Items",
            session,
            params={"Ids": item_id, "Fields": "MediaSources"},
        )
        items = (payload or {}).get("Items") or []
        if not items:
            raise JellyfinError("Jellyfin 里找不到这个视频", "MEDIA_NOT_FOUND", 404)
        return items[0]

    async def probe_size(self, session: JellyfinSession, item_id: str) -> int:
        """MediaSources.Size is often null, so ask the stream itself."""
        url = self.absolute(f"/Videos/{item_id}/stream?static=true")
        headers = self._headers(session.token, session.device_id)
        headers["Range"] = "bytes=0-0"
        try:
            async with self.session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)
            ) as response:
                if response.status == 401:
                    raise JellyfinAuthError("Jellyfin 认证已失效，请重新登录")
                content_range = response.headers.get("Content-Range") or ""
                if "/" in content_range:
                    total = content_range.rsplit("/", 1)[-1].strip()
                    if total.isdigit() and int(total) > 0:
                        return int(total)
                length = response.headers.get("Content-Length")
                if response.status == 206 and length and length.isdigit():
                    return int(length)
                raise JellyfinError("无法确定视频大小", "JELLYFIN_UNAVAILABLE")
        except JellyfinError:
            raise
        except asyncio.TimeoutError:
            raise JellyfinError("连接 Jellyfin 超时", "JELLYFIN_UNAVAILABLE", 504)
        except aiohttp.ClientError as exc:
            raise JellyfinError(f"连接 Jellyfin 失败: {exc}", "JELLYFIN_UNAVAILABLE", 502)

    async def read_range(
        self, session: JellyfinSession, item_id: str, offset: int, length: int
    ) -> bytes:
        url = self.absolute(f"/Videos/{item_id}/stream?static=true")
        headers = self._headers(session.token, session.device_id)
        headers["Range"] = f"bytes={offset}-{offset + length - 1}"
        try:
            async with self.session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status == 401:
                    raise JellyfinAuthError("Jellyfin 认证已失效，请重新登录")
                if response.status not in (200, 206):
                    raise JellyfinError(
                        f"Jellyfin 读取失败（HTTP {response.status}）", "JELLYFIN_UNAVAILABLE"
                    )
                return await response.content.readexactly(length)
        except JellyfinError:
            raise
        except asyncio.IncompleteReadError:
            raise JellyfinError("Jellyfin 返回的数据不完整", "JELLYFIN_UNAVAILABLE")
        except asyncio.TimeoutError:
            raise JellyfinError("连接 Jellyfin 超时", "JELLYFIN_UNAVAILABLE", 504)
        except aiohttp.ClientError as exc:
            raise JellyfinError(f"连接 Jellyfin 失败: {exc}", "JELLYFIN_UNAVAILABLE", 502)

    # -- transcoding ---------------------------------------------------
    def _device_profile(self) -> Dict[str, Any]:
        return {
            "MaxStreamingBitrate": self.max_bitrate,
            "DirectPlayProfiles": [
                {
                    "Container": "mp4,m4v,webm,mov",
                    "Type": "Video",
                    "VideoCodec": "h264,av1,vp9,vp8",
                    "AudioCodec": "aac,mp3,opus,vorbis",
                }
            ],
            "TranscodingProfiles": [
                {
                    "Container": "ts",
                    "Type": "Video",
                    "VideoCodec": "h264",
                    "AudioCodec": "aac",
                    "Protocol": "hls",
                    "Context": "Streaming",
                    "MaxAudioChannels": "2",
                    "MinSegments": 1,
                    "BreakOnNonKeyFrames": True,
                }
            ],
            "CodecProfiles": [],
            "SubtitleProfiles": [],
        }

    async def open_transcode(
        self,
        session: JellyfinSession,
        item_id: str,
        start_seconds: float,
        epoch: int,
    ) -> HlsSession:
        """Start (or restart) a transcoding session at ``start_seconds``."""
        body = {
            "UserId": session.user_id,
            "StartTimeTicks": int(max(0.0, start_seconds) * 10_000_000),
            "IsPlayback": True,
            "AutoOpenLiveStream": True,
            "MaxStreamingBitrate": self.max_bitrate,
            "DeviceProfile": self._device_profile(),
        }
        payload = await self._json(
            "POST",
            f"/Items/{item_id}/PlaybackInfo",
            session,
            params={"UserId": session.user_id},
            body=body,
            timeout=25.0,
        )
        sources = (payload or {}).get("MediaSources") or []
        if not sources:
            raise JellyfinError("Jellyfin 没有给出可用的播放地址", "JELLYFIN_TRANSCODE_FAILED")
        source = sources[0]
        transcoding_url = str(source.get("TranscodingUrl") or "")
        if not transcoding_url:
            raise JellyfinError("Jellyfin 没有给出转码地址", "JELLYFIN_TRANSCODE_FAILED")
        master_url = self.absolute(transcoding_url)
        live_url = await self._resolve_live_url(session, master_url)

        play_session_id = str(
            source.get("PlaySessionId") or (payload or {}).get("PlaySessionId") or ""
        )
        return HlsSession(
            item_id=item_id,
            media_source_id=str(source.get("Id") or item_id),
            play_session_id=play_session_id,
            live_url=live_url,
            start_seconds=max(0.0, float(start_seconds)),
            epoch=epoch,
        )

    async def _resolve_live_url(self, session: JellyfinSession, master_url: str) -> str:
        text = await self._json("GET", self._with_token(master_url, session), session, timeout=25.0)
        if not isinstance(text, str):
            raise JellyfinError("Jellyfin 播放列表格式异常", "JELLYFIN_TRANSCODE_FAILED")
        # Only a URI that follows #EXT-X-STREAM-INF is a variant; a plain media
        # playlist has no variants and can be used directly.
        expect_variant = False
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("#EXT-X-STREAM-INF"):
                expect_variant = True
                continue
            if stripped.startswith("#"):
                continue
            if expect_variant:
                return urljoin(master_url, stripped)
        return master_url

    @staticmethod
    def _with_token(url: str, session: JellyfinSession) -> str:
        parsed = urlparse(url)
        if "api_key=" in (parsed.query or ""):
            return url
        joiner = "&" if parsed.query else "?"
        return f"{url}{joiner}api_key={session.token}"

    async def refresh_playlist(self, session: JellyfinSession, hls: HlsSession) -> float:
        """Read Jellyfin's playlist and remember every segment it lists.

        Jellyfin hands out the *whole* item as a VOD playlist straight away,
        while the segments themselves are produced on demand, so this list is
        complete from the first call and never grows.  Nothing is pruned: a
        member who joins an hour in, or a seek backwards, still has to be able
        to ask for segment 3.

        Returns the playlist's media sequence, which is the segment number the
        first listed entry maps to.
        """
        async with hls._lock:
            url = self._with_token(hls.live_url, session)
            text = await self._json("GET", url, session, timeout=PLAYLIST_TIMEOUT)
            if not isinstance(text, str):
                raise JellyfinError("Jellyfin 播放列表格式异常", "JELLYFIN_TRANSCODE_FAILED")
            sequence = 0
            target = hls.target_duration
            duration = hls.target_duration
            index: Optional[int] = None
            for line in text.splitlines():
                stripped = line.strip()
                if not stripped:
                    continue
                upper = stripped.upper()
                if upper.startswith("#EXT-X-MEDIA-SEQUENCE:"):
                    try:
                        sequence = int(stripped.split(":", 1)[1].strip())
                    except ValueError:
                        sequence = 0
                    index = sequence
                    continue
                if upper.startswith("#EXT-X-TARGETDURATION:"):
                    try:
                        target = float(stripped.split(":", 1)[1].strip())
                    except ValueError:
                        pass
                    continue
                if upper.startswith("#EXT-X-ENDLIST"):
                    hls.complete = True
                    continue
                if upper.startswith("#EXT-X-PLAYLIST-TYPE:VOD"):
                    # Jellyfin labels the item VOD as soon as it hands out the
                    # full list, before the segments themselves exist.
                    hls.complete = True
                    continue
                if upper.startswith("#EXTINF:"):
                    try:
                        duration = float(stripped.split(":", 1)[1].split(",")[0])
                    except ValueError:
                        duration = target
                    continue
                if stripped.startswith("#"):
                    continue
                if index is None:
                    index = sequence
                hls.segments[index] = urljoin(hls.live_url, stripped)
                hls.durations[index] = max(0.1, duration)
                index += 1
            hls.target_duration = max(1.0, float(target))
            hls.refreshed_at = time.time()
            return float(sequence)

    async def render_playlist(self, session: JellyfinSession, hls: HlsSession, rewrite) -> str:
        """Republish Jellyfin's playlist with our own (signed) segment URLs.

        The list is published as VOD: every segment of this transcode already
        has its final number, so the browser can jump anywhere inside it and a
        member can be seeked straight to where the host is.  Segment 0 is
        ``start_seconds`` into the item, which is exactly the offset the
        players keep in ``media_start``.
        """
        await self.refresh_playlist(session, hls)
        indices = sorted(hls.segments)
        lines = ["#EXTM3U"]
        if hls.complete:
            lines.append("#EXT-X-PLAYLIST-TYPE:VOD")
        lines.append("#EXT-X-VERSION:3")
        lines.append(f"#EXT-X-TARGETDURATION:{int(round(hls.target_duration))}")
        lines.append(f"#EXT-X-MEDIA-SEQUENCE:{indices[0] if indices else 0}")
        for index in indices:
            lines.append(f"#EXTINF:{hls.durations.get(index, hls.target_duration):.3f},")
            lines.append(rewrite(index))
        if hls.complete:
            lines.append("#EXT-X-ENDLIST")
        return "\n".join(lines) + "\n"

    async def read_segment(
        self, session: JellyfinSession, hls: HlsSession, index: int
    ) -> bytes:
        deadline = time.monotonic() + SEGMENT_WAIT
        if index not in hls.segments:
            await self.refresh_playlist(session, hls)
        if index not in hls.segments:
            raise JellyfinError("Jellyfin 播放列表里没有这个分片", "JELLYFIN_TRANSCODE_FAILED", 404)
        url = self._with_token(hls.segments[index], session)
        while True:
            try:
                async with self.session.get(
                    url,
                    headers={"Accept": "*/*"},
                    timeout=aiohttp.ClientTimeout(total=SEGMENT_TIMEOUT),
                ) as response:
                    if response.status == 404 and time.monotonic() < deadline:
                        # Listed, but the encoder has not written it yet.  It
                        # produces segments on demand, so this is a wait, not
                        # an error.
                        await asyncio.sleep(0.4)
                        continue
                    if response.status == 404:
                        raise JellyfinError(
                            "转码分片还没准备好", "JELLYFIN_TRANSCODE_FAILED", 404
                        )
                    if response.status >= 400:
                        raise JellyfinError(
                            f"Jellyfin 分片读取失败（HTTP {response.status}）",
                            "JELLYFIN_TRANSCODE_FAILED",
                        )
                    return await response.read()
            except JellyfinError:
                raise
            except asyncio.TimeoutError:
                raise JellyfinError("Jellyfin 分片读取超时", "JELLYFIN_TRANSCODE_FAILED", 504)
            except aiohttp.ClientError as exc:
                raise JellyfinError(f"连接 Jellyfin 失败: {exc}", "JELLYFIN_UNAVAILABLE", 502)

    async def stop_transcode(self, session: JellyfinSession, hls: HlsSession) -> None:
        if not hls.play_session_id:
            return
        try:
            await self._json(
                "DELETE",
                f"/Videos/{hls.item_id}/ActiveEncodings",
                session,
                params={
                    "DeviceId": session.device_id,
                    "PlaySessionId": hls.play_session_id,
                },
                timeout=10.0,
            )
        except Exception:
            # A leftover encoder is a nuisance, not a failure worth surfacing.
            logger.info(
                "Could not stop Jellyfin transcode item=%s session=%s",
                hls.item_id,
                hls.play_session_id,
            )

    # -- subtitles -----------------------------------------------------
    async def subtitle_vtt(
        self, session: JellyfinSession, item_id: str, media_source_id: str, index: int
    ) -> Optional[str]:
        path = f"/Videos/{item_id}/{media_source_id}/Subtitles/{index}/Stream.vtt"
        try:
            text = await self._json("GET", self._with_token(path, session), session, timeout=45.0)
        except JellyfinError as exc:
            logger.info(
                "Jellyfin subtitle %s/%s unavailable: %s", item_id, index, exc.message
            )
            return None
        if not isinstance(text, str) or "-->" not in text:
            return None
        cleaned = clean_vtt(text)
        return cleaned if "-->" in cleaned else None


class JellyfinSource:
    """Byte-level reads for a direct-play item, mirroring UpstreamSource."""

    def __init__(
        self,
        client: JellyfinClient,
        session: JellyfinSession,
        item_id: str,
        size: int,
        mime: str,
    ) -> None:
        self.client = client
        self.session = session
        self.item_id = item_id
        self.size = int(size)
        self.mime = mime

    async def read(self, offset: int, length: int) -> bytes:
        if length <= 0:
            return b""
        return await self.client.read_range(self.session, self.item_id, offset, length)
