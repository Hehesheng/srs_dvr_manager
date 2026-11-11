"""Jellyfin as a hall media source: browsing, direct play and HLS.

Which server this module talks to is decided per room: the host's browser
hands the hall a base URL plus an API key when it connects the room, and the
hall keeps that pair on the room for as long as it lives.  Nothing about
Jellyfin -- neither the address nor the credential -- comes from the server
configuration any more.

That makes the base URL attacker-influenced in principle, so it is limited to
a plain ``http(s)`` origin (plus an optional path prefix for servers mounted
under a subdirectory) with no credentials, query or fragment in it, and only
the holder of the room's host token can set it.  We still do not run the
address through ``upstream.validate_public_url``: that guard exists for
arbitrary public URLs, and a Jellyfin (or a NAS) sitting on a private address
is a perfectly normal thing to point this at.

The caller's API key is the only credential involved.  It is kept in memory
on the room and must never reach a member: members only ever see the hall's
own signed URLs.
"""

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse
from uuid import uuid4

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

# Transcode quality.  A burned-in subtitle is re-encoded, so the room starts
# below the raw source bitrate and the host can move it from there.
DEFAULT_TRANSCODE_BITRATE = 6_000_000
MIN_TRANSCODE_BITRATE = 1_000_000
MAX_TRANSCODE_BITRATE = 40_000_000


def clamp_bitrate(value: Any, ceiling: Optional[int] = None) -> int:
    """Keep a requested bitrate inside what Jellyfin can be asked for."""
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = DEFAULT_TRANSCODE_BITRATE
    top = MAX_TRANSCODE_BITRATE
    if ceiling is not None:
        try:
            top = min(top, max(MIN_TRANSCODE_BITRATE, int(ceiling)))
        except (TypeError, ValueError):
            pass
    return max(MIN_TRANSCODE_BITRATE, min(top, number))

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
# A restart kills the responses the old encoder was still streaming, so a
# handful of readers lose the segment they were part-way through.  How many
# times a segment is asked for before the player is told it failed.
SEGMENT_TRY_LIMIT = 5


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


# A host: letters, digits, dots, dashes and underscores (a NAS on the LAN has
# no dot), or a bracketed IPv6 literal, optionally with a port.
_HOST_PORT_RE = re.compile(r"^(?:\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9._-]+)(?::[0-9]{1,5})?$")


def normalize_jellyfin_url(raw: Any) -> str:
    """Turn what the host typed into the base URL we will call.

    Accepts ``host`` (https assumed), ``https://host`` and ``https://host/jf``
    for servers mounted under a subdirectory.  Refuses anything that is not
    plain http(s), carries credentials in front of the host, or hides a query
    or fragment -- a base URL is a place, not a request.
    """
    text = str(raw or "").strip()
    if not text:
        raise JellyfinError("请填写 Jellyfin 网址", "JELLYFIN_CONFIG_REQUIRED", 400)
    if len(text) > 255:
        raise JellyfinError("Jellyfin 网址太长了", "JELLYFIN_URL_INVALID", 400)
    if any(char.isspace() for char in text):
        # This string ends up glued in front of API paths; whitespace there is
        # always a paste accident, never an address.
        raise JellyfinError("Jellyfin 网址里不要有空格", "JELLYFIN_URL_INVALID", 400)
    if "://" not in text:
        text = f"https://{text}"
    try:
        parsed = urlparse(text)
    except ValueError:
        raise JellyfinError("Jellyfin 网址格式不对", "JELLYFIN_URL_INVALID", 400)
    if parsed.scheme not in ("http", "https"):
        raise JellyfinError("Jellyfin 网址要以 http:// 或 https:// 开头", "JELLYFIN_URL_INVALID", 400)
    if "@" in parsed.netloc:
        raise JellyfinError("Jellyfin 网址里不要带用户名和密码", "JELLYFIN_URL_INVALID", 400)
    if not _HOST_PORT_RE.match(parsed.netloc):
        raise JellyfinError("Jellyfin 网址格式不对", "JELLYFIN_URL_INVALID", 400)
    if parsed.query or parsed.fragment:
        raise JellyfinError("Jellyfin 网址只填到服务器地址，不要带参数", "JELLYFIN_URL_INVALID", 400)
    # A server under /jellyfin is real, so keep the path; let the client join
    # API paths onto it exactly like it did with the old configured URL.
    return text.rstrip("/")


@dataclass
class JellyfinSession:
    """One host's authenticated view of one Jellyfin server.

    ``base_url`` rides along because the address is per room now: every call
    this session makes has to go back to the same server the host typed in,
    and rooms in the same process may well point at different ones.
    """

    token: str
    user_id: str
    user_name: str
    device_id: str
    base_url: str = ""

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
    # Which subtitle track Jellyfin is burning into this stream; None means
    # "whatever the account default picks" and ``subtitles_off`` means none.
    subtitle_index: Optional[int] = None
    subtitles_off: bool = False
    target_duration: float = 3.0
    segments: Dict[int, str] = field(default_factory=dict)
    durations: Dict[int, float] = field(default_factory=dict)
    # Item time of each listed segment, read from its ``runtimeTicks``.
    offsets: Dict[int, float] = field(default_factory=dict)
    complete: bool = False
    refreshed_at: float = 0.0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    @property
    def offset_seconds(self) -> float:
        """Where the first segment we publish sits in the item.

        Jellyfin numbers a transcode playlist from the *item's* start even
        when the transcode was asked to begin at 300s: its segment URLs carry
        the ``runtimeTicks`` each one was cut from, and the first of them is
        still the item's own 0.  So this is what the players keep as their
        clock offset -- ``start_seconds`` is where the room wants playback to
        begin, which is a different thing and only equals this one at 0.
        """
        if not self.segments:
            return 0.0
        return self.offsets.get(min(self.segments), 0.0)

    def public(self) -> Dict[str, Any]:
        return {
            "epoch": self.epoch,
            "start_seconds": round(self.start_seconds, 3),
            "offset_seconds": round(self.offset_seconds, 3),
        }


def segment_item_offset(url: str) -> Optional[float]:
    """The item time a Jellyfin transcode segment was cut from, in seconds.

    ``None`` when the URL carries no ``runtimeTicks`` (a plain media playlist
    or an unexpected shape), which the caller reads as "no better answer than
    0.0", the item start.
    """
    try:
        query = dict(parse_qsl(urlparse(url).query))
    except ValueError:
        return None
    raw = query.get("runtimeTicks")
    if raw is None:
        return None
    try:
        return max(0.0, float(raw) / 10_000_000)
    except ValueError:
        return None


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


LANGUAGE_NAMES = {
    "zh": "简体中文",
    "chi": "简体中文",
    "zho": "中文",
    "zh-cn": "简体中文",
    "zh-hans": "简体中文",
    "zh-tw": "繁體中文",
    "zh-hant": "繁體中文",
    "eng": "英语",
    "en": "英语",
    "jpn": "日语",
    "ja": "日语",
    "kor": "韩语",
    "ko": "韩语",
    "und": "字幕",
}


def subtitle_label(stream: Dict[str, Any], language: str = "") -> str:
    """``简体中文 - Chi - 默认 - ASS`` becomes ``简体中文（默认）``."""
    raw = str(stream.get("DisplayTitle") or stream.get("Title") or "").strip()
    parts = [part.strip() for part in re.split(r"\s+-\s+", raw) if part.strip()] if raw else []
    lang = (language or str(stream.get("Language") or "")).strip().lower()
    base = parts[0] if parts else ""
    if not base:
        base = LANGUAGE_NAMES.get(lang, language or "字幕")
    flags = {part.lower() for part in parts[1:]}
    marks: List[str] = []
    default = bool(stream.get("IsDefault")) or bool(flags & {"默认", "default"})
    if default:
        marks.append("默认")
    if flags & {"强制", "forced"}:
        marks.append("强制")
    label = f"{base}（{'、'.join(marks)}）" if marks else base
    return label[:40]


def subtitle_streams(media_source: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Every subtitle track, including image-based ones.

    Burning subtitles into the transcode works for PGS/VOBSUB too, so unlike
    ``text_subtitle_streams`` this list is not limited to text codecs.
    """
    tracks: List[Dict[str, Any]] = []
    for stream in media_streams(media_source):
        if stream.get("Type") != "Subtitle":
            continue
        try:
            index = int(stream.get("Index"))
        except (TypeError, ValueError):
            continue
        language = str(stream.get("Language") or "").strip()
        tracks.append(
            {
                "index": index,
                "label": subtitle_label(stream, language),
                "language": language,
                "codec": str(stream.get("Codec") or "").lower(),
                "is_default": bool(stream.get("IsDefault")),
            }
        )
    return tracks


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


# Everything the browser sends back: enough to render a folder row, label an
# episode (S01E05) and decide whether it can be played directly.
BROWSE_FIELDS = (
    "MediaSources,RunTimeTicks,ChildCount,RecursiveItemCount,"
    "SeriesId,SeriesName,IndexNumber,ParentIndexNumber"
)

# Anything shorter is an episode or a clip, not a film: 40 minutes.
FEATURE_SECONDS = 2400.0

KIND_BY_TYPE = {
    "Series": "series",
    "Season": "season",
    "Folder": "folder",
    "BoxSet": "folder",
    "CollectionFolder": "folder",
    "Movie": "movie",
    "Episode": "episode",
    "Video": "video",
    "MusicVideo": "video",
}


def item_kind(item: Dict[str, Any]) -> str:
    """A small, stable vocabulary for the browser, whatever Jellyfin calls it."""
    return KIND_BY_TYPE.get(str(item.get("Type") or ""), "item")


def browse_name(item: Dict[str, Any], kind: str) -> str:
    """The title to show in the picker.

    ``ProductionYear`` helps tell two films apart, but Jellyfin also stamps a
    guessed year on every episode file, and libraries that store episodes as
    loose movies then read "第 3 集 (1991) (2013)…".  Only keep the year for
    things long enough to be an actual film.
    """
    if kind in ("episode", "video") or duration_seconds(item) < FEATURE_SECONDS:
        return str(item.get("Name") or "").strip() or "Jellyfin 视频"
    return item_name(item)


def item_child_count(item: Dict[str, Any], kind: str) -> int:
    """How many things live under a folder row.

    A series reports one child (its season) but 23 recursive items, and the
    host cares about the 23 episodes, so prefer the recursive count there.
    """
    if kind in ("series", "season"):
        keys = ("RecursiveItemCount", "ChildCount")
    else:
        keys = ("ChildCount", "RecursiveItemCount")
    for key in keys:
        value = item.get(key)
        try:
            number = int(value)
        except (TypeError, ValueError):
            continue
        if number > 0:
            return number
    return 0


class JellyfinClient:
    def __init__(self, session: aiohttp.ClientSession, base_url: str, max_bitrate: int) -> None:
        self.session = session
        self.base_url = (base_url or "").rstrip("/")
        # The ceiling this server was configured for; rooms pick below it.
        self.max_bitrate = clamp_bitrate(max_bitrate, ceiling=None)

    # -- plumbing ------------------------------------------------------
    def _headers(self, token: Optional[str], device_id: str) -> Dict[str, str]:
        # The key has to ride in the MediaBrowser header: Jellyfin 12 dropped
        # X-Emby-Token and answers 401 on every authenticated call without it,
        # which is what made a perfectly good key look wrong on a 12.1 server.
        # Older servers (10.x) read the same header, so one form covers both;
        # X-Emby-Token stays for anything older still that only knows that one.
        fields = [
            f'Client="{CLIENT_NAME}"',
            f'Device="{DEVICE_NAME}"',
            f'DeviceId="{device_id or "srs-hall"}"',
            f'Version="{CLIENT_VERSION}"',
        ]
        if token:
            fields.append(f'Token="{token}"')
        headers = {
            "Authorization": "MediaBrowser " + ", ".join(fields),
            "Accept": "application/json",
        }
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

    # -- connecting ----------------------------------------------------
    async def verify(
        self, token: str, device_id: str, user_id: Optional[str] = None
    ) -> JellyfinSession:
        """Check an API key and work out which account our library calls run as.

        Jellyfin needs a user id for browsing and playback, but an API key --
        the thing a host actually pastes -- is owned by the server rather than
        by one account.  So: trust a remembered user id if the browser has one,
        otherwise ask ``/Users/Me`` (right for a user token), and otherwise
        read the account list and take the only user, or the first
        administrator when there are several.
        """
        probe = JellyfinSession(
            token=token, user_id=user_id or "", user_name="", device_id=device_id,
            base_url=self.base_url,
        )
        payload: Optional[Dict[str, Any]] = None
        if user_id:
            found = await self._json("GET", f"/Users/{user_id}", probe)
            if isinstance(found, dict) and str(found.get("Id") or "") == user_id:
                payload = found
            else:
                raise JellyfinAuthError("Jellyfin 密钥无效或账号已不存在")
        if payload is None:
            payload = await self._resolve_user(probe)
        return JellyfinSession(
            token=token,
            user_id=str(payload.get("Id") or ""),
            user_name=str(payload.get("Name") or ""),
            device_id=device_id,
            base_url=self.base_url,
        )

    async def _resolve_user(self, probe: JellyfinSession) -> Dict[str, Any]:
        """Ask Jellyfin who we are, falling back to picking from its user list."""
        try:
            me = await self._json("GET", "/Users/Me", probe)
            if isinstance(me, dict) and me.get("Id"):
                return me
        except JellyfinAuthError:
            raise
        except JellyfinError:
            # 10.10 answers 400 here for an API key: the key is not a signed-in
            # user, so there is no "me".  The account list works either way.
            pass
        users = await self._json("GET", "/Users", probe)
        candidates = [
            user for user in (users or []) if isinstance(user, dict) and user.get("Id")
        ]
        if not candidates:
            raise JellyfinAuthError("这枚密钥看不到任何 Jellyfin 账号")
        if len(candidates) == 1:
            return candidates[0]
        admins = [
            user
            for user in candidates
            if isinstance(user.get("Policy"), dict) and user["Policy"].get("IsAdministrator")
        ]
        return (admins or candidates)[0]

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
        mode: str = "children",
    ) -> Dict[str, Any]:
        """List one level of the library, or the episodes of a series.

        ``children`` walks down the folder tree one step at a time, which is
        what keeps a folder's episodes together instead of interleaving them
        with everything else in the library.  ``episodes`` flattens a series
        so the host does not have to click through "Season 1" first.
        """
        wanted = mode if mode in ("children", "episodes") else "children"
        params: Dict[str, Any] = {
            "Fields": BROWSE_FIELDS,
            "SortOrder": "Ascending",
            "StartIndex": max(0, int(start)),
            "Limit": min(200, max(1, int(limit))),
            "ImageTypeLimit": 0,
            "EnableImages": "false",
        }
        if wanted == "episodes":
            params["Recursive"] = "true"
            # Episode covers a real series; Movie covers the many anime
            # folders whose "episodes" Jellyfin labelled as loose movies.
            params["IncludeItemTypes"] = "Episode,Movie,Video,MusicVideo"
            params["SortBy"] = "ParentIndexNumber,IndexNumber,SortName"
        else:
            params["Recursive"] = "false"
            # Folders sort first so the tree stays readable: series and
            # folders on top, loose movies underneath.
            params["SortBy"] = "IsFolder,SortName"
        if parent_id:
            params["ParentId"] = parent_id
        text = query.strip()[:64]
        if text:
            # A search reaches into the current folder, and offers the folders
            # themselves so a hit can be opened instead of flattened.
            params["SearchTerm"] = text
            params["Recursive"] = "true"
            params["SortBy"] = "IsFolder,SortName"
            params["IncludeItemTypes"] = "Movie,Series,Folder,BoxSet,Episode,Video,MusicVideo"
        payload = await self._json(
            "GET", f"/Users/{session.user_id}/Items", session, params=params
        ) or {}
        if text:
            # Jellyfin ignores IsFolder in a search, but the thing the host
            # searched for is usually the series — put it above its episodes.
            items = payload.get("Items") or []
            payload = {
                **payload,
                "Items": sorted(
                    items,
                    key=lambda entry: (
                        0 if entry.get("IsFolder") else 1,
                        str(entry.get("SortName") or entry.get("Name") or ""),
                    ),
                ),
            }
        return self._project_items(payload)

    def _project_items(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        items = []
        for item in payload.get("Items") or []:
            sources = item.get("MediaSources") or []
            source = sources[0] if sources else {}
            folder = bool(item.get("IsFolder"))
            kind = item_kind(item)
            # Containers have no file of their own; leaving mode as "hls"
            # would make the browser offer to play a series.
            mode = None if folder or not source else playback_mode(source)
            items.append(
                {
                    "id": str(item.get("Id") or ""),
                    "name": browse_name(item, kind),
                    "type": str(item.get("Type") or ""),
                    "kind": kind,
                    "is_folder": folder,
                    "child_count": item_child_count(item, kind),
                    "series_id": str(item.get("SeriesId") or ""),
                    "series_name": str(item.get("SeriesName") or ""),
                    "season": item.get("ParentIndexNumber"),
                    "episode": item.get("IndexNumber"),
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
    def _device_profile(
        self, subtitles_off: bool = False, max_bitrate: Optional[int] = None
    ) -> Dict[str, Any]:
        # An empty SubtitleProfiles list says "this client cannot show any
        # subtitle", which makes Jellyfin burn the account's default track into
        # the picture.  Declaring text formats as external instead means no
        # burn-in at all.
        subtitle_profiles: List[Dict[str, Any]] = []
        if subtitles_off:
            subtitle_profiles = [
                {"Format": codec, "Method": "External"}
                for codec in ("srt", "subrip", "ass", "ssa", "vtt", "webvtt")
            ]
        return {
            "MaxStreamingBitrate": clamp_bitrate(
                self.max_bitrate if max_bitrate is None else max_bitrate,
                ceiling=self.max_bitrate,
            ),
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
            "SubtitleProfiles": subtitle_profiles,
        }

    async def open_transcode(
        self,
        session: JellyfinSession,
        item_id: str,
        start_seconds: float,
        epoch: int,
        subtitle_index: Optional[int] = None,
        subtitles_off: bool = False,
        max_bitrate: Optional[int] = None,
    ) -> HlsSession:
        """Start (or restart) a transcoding session at ``start_seconds``.

        ``PlaybackInfo``'s own ``SubtitleStreamIndex`` is ignored by Jellyfin
        (it always answers with the account default), so the track is chosen
        by rewriting the query of the returned ``TranscodingUrl`` instead --
        together with a fresh ``PlaySessionId``, without which Jellyfin keeps
        serving the previous session's segments.
        """
        bitrate = clamp_bitrate(
            self.max_bitrate if max_bitrate is None else max_bitrate,
            ceiling=self.max_bitrate,
        )
        body = {
            "UserId": session.user_id,
            "StartTimeTicks": int(max(0.0, start_seconds) * 10_000_000),
            "IsPlayback": True,
            "AutoOpenLiveStream": True,
            "MaxStreamingBitrate": bitrate,
            "DeviceProfile": self._device_profile(subtitles_off, bitrate),
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
        transcoding_url, rewritten_session_id = self._rewrite_transcode_subtitles(
            transcoding_url, subtitle_index, subtitles_off
        )
        master_url = self.absolute(transcoding_url)
        live_url = await self._resolve_live_url(session, master_url)

        play_session_id = rewritten_session_id or str(
            source.get("PlaySessionId") or (payload or {}).get("PlaySessionId") or ""
        )
        return HlsSession(
            item_id=item_id,
            media_source_id=str(source.get("Id") or item_id),
            play_session_id=play_session_id,
            live_url=live_url,
            start_seconds=max(0.0, float(start_seconds)),
            epoch=epoch,
            subtitle_index=subtitle_index,
            subtitles_off=subtitles_off,
        )

    @staticmethod
    def _rewrite_transcode_subtitles(
        transcoding_url: str, subtitle_index: Optional[int], subtitles_off: bool
    ) -> Tuple[str, Optional[str]]:
        """Point the transcode at ``subtitle_index`` (or at no subtitle at all).

        Returns the rewritten URL and the new ``PlaySessionId``, or the URL
        unchanged and ``None`` when nothing needs to change.
        """
        # Jellyfin spells these SubtitleStreamIndex / SubtitleMethod /
        # PlaySessionId; match them whatever case it sends.
        index_key = "subtitlestreamindex"
        method_key = "subtitlemethod"
        session_key = "playsessionid"

        parsed = urlparse(transcoding_url)
        pairs = parse_qsl(parsed.query, keep_blank_values=True)
        lower = {key.lower() for key, _ in pairs}
        has_subtitle = index_key in lower or method_key in lower

        if subtitle_index is not None:
            wanted = {index_key: str(int(subtitle_index)), method_key: "Encode"}
        elif subtitles_off and has_subtitle:
            wanted = {index_key: None, method_key: None}
        else:
            return transcoding_url, None

        new_session_id = uuid4().hex
        kept: List[Tuple[str, str]] = []
        for key, value in pairs:
            low = key.lower()
            if low in (index_key, method_key):
                if wanted.get(low) is not None:
                    kept.append((key, str(wanted[low])))
                continue
            if low == session_key:
                kept.append((key, new_session_id))
                continue
            kept.append((key, value))
        if wanted.get(index_key) is not None and index_key not in lower:
            kept.append(("SubtitleStreamIndex", str(wanted[index_key])))
        if wanted.get(method_key) is not None and method_key not in lower:
            kept.append(("SubtitleMethod", str(wanted[method_key])))
        if session_key not in lower:
            kept.append(("PlaySessionId", new_session_id))
        return urlunparse(parsed._replace(query=urlencode(kept, doseq=True))), new_session_id

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
                offset = segment_item_offset(hls.segments[index])
                if offset is not None:
                    hls.offsets[index] = offset
                index += 1
            hls.target_duration = max(1.0, float(target))
            hls.refreshed_at = time.time()
            return float(sequence)

    async def render_playlist(self, session: JellyfinSession, hls: HlsSession, rewrite) -> str:
        """Republish Jellyfin's playlist with our own (signed) segment URLs.

        The list is published as VOD: every segment of this transcode already
        has its final number, so the browser can jump anywhere inside it and a
        member can be seeked straight to where the host is.  Its first entry
        sits at ``offset_seconds`` into the item -- the item start, whatever
        position the transcode was asked to resume from.
        """
        await self.refresh_playlist(session, hls)
        indices = sorted(hls.segments)
        lines = ["#EXTM3U"]
        if hls.complete:
            lines.append("#EXT-X-PLAYLIST-TYPE:VOD")
        lines.append("#EXT-X-VERSION:3")
        lines.append(f"#EXT-X-TARGETDURATION:{int(round(hls.target_duration))}")
        lines.append(f"#EXT-X-MEDIA-SEQUENCE:{indices[0] if indices else 0}")
        # Where in *this playlist* the room wants playback to begin.  hls.js
        # takes its own startPosition over this, so it is the engine we do not
        # drive (native HLS) that reads it and opens at the right place.
        start_at = hls.start_seconds - hls.offset_seconds
        if start_at > 0.5:
            lines.append(f"#EXT-X-START:TIME-OFFSET:{start_at:.3f},PRECISE=YES")
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
        tries = 0
        while True:
            tries += 1
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
                    if response.status >= 500 and tries < SEGMENT_TRY_LIMIT:
                        # Replacing an encoder makes Jellyfin answer 5xx for a
                        # moment -- the same moment that drops connections.
                        await asyncio.sleep(0.4)
                        continue
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
                # Stopping an encoding kills the response the segment was
                # arriving on, so a seek leaves a few readers with an aborted
                # connection.  Retrying picks the segment up from the stream
                # that replaced it; only a connection that stays broken is
                # worth telling the player about.
                if tries < SEGMENT_TRY_LIMIT:
                    await asyncio.sleep(0.4)
                    continue
                logger.warning(
                    "Jellyfin segment read failed index=%s after %d tries: %s",
                    index,
                    tries,
                    exc,
                )
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
