"""HTTP and WebSocket routes for the media hall."""

import logging
from typing import Any, Optional, Union

from fastapi import APIRouter, Header, Query, Request, WebSocket, status
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

from . import protocol
from .host_source import HostSourceError
from .manager import HallError, HallManager
from .media import (
    RangeError,
    full_headers,
    parse_range,
    partial_headers,
    stream_full,
    stream_range,
    unsatisfiable_headers,
)
from .upstream import UpstreamError
from .ws import handle_websocket

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/hall", tags=["hall"])


class MediaPayload(BaseModel):
    kind: str = "host_file"
    name: str = ""
    size: int = 0
    mime: str = "application/octet-stream"
    url: Optional[str] = None
    # Jellyfin items are addressed by id; the server owns every credential.
    item_id: Optional[str] = None


class JellyfinLoginRequest(BaseModel):
    username: str = Field(default="", max_length=64)
    password: str = Field(default="", max_length=256)
    token: Optional[str] = None
    # Returned by the login call and echoed back with a remembered token, so
    # the server can look the account up without relying on /Users/Me.
    user_id: Optional[str] = Field(default=None, max_length=64)


class HlsSeekRequest(BaseModel):
    position: float = 0.0


class HlsSubtitleRequest(BaseModel):
    # A stream index, "off", or null to follow Jellyfin's default track.
    subtitle: Optional[Union[int, str]] = None
    position: float = 0.0


class CreateRoomRequest(BaseModel):
    name: str = Field(default="Media Room", max_length=64)
    # Optional: a host may open the room first and fill the playlist later.
    media: Optional[MediaPayload] = None


class JoinRequest(BaseModel):
    name: str = Field(default="Guest", max_length=32)


def _hall(request: Request) -> Optional[HallManager]:
    return getattr(request.app.state, "hall", None)


def _error_json(
    status_code: int, code: str, message: str, headers: Optional[dict] = None
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"code": code, "message": message},
        headers=headers,
    )


def _hall_error(exc: HallError) -> JSONResponse:
    return _error_json(exc.status_code, exc.code, exc.message)


def _media_fields(room, hall: HallManager) -> dict:
    return {
        "media_id": room.media.media_id if room.media else None,
        "media": room.media.public() if room.media else None,
        "media_url": room.media_url(),
    }


@router.get("/api/rooms")
async def list_rooms(request: Request):
    hall = _hall(request)
    if hall is None:
        return _error_json(status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable")
    return {"rooms": hall.list_rooms()}


@router.post("/api/rooms")
async def create_room(payload: CreateRoomRequest, request: Request):
    hall = _hall(request)
    if hall is None:
        return _error_json(status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable")
    media_payload = payload.media.model_dump(exclude_none=True) if payload.media else None
    try:
        room = await hall.create_room(payload.name, media_payload)
    except HallError as exc:
        return _hall_error(exc)
    return {
        "room_id": room.room_id,
        "room_code": room.room_code,
        "host_token": room.host_token,
        **_media_fields(room, hall),
        "ws_url": f"/hall/ws/{room.room_id}",
        "room": room.public_info(),
    }


@router.post("/api/rooms/{room_ref}/media")
async def change_media(
    room_ref: str,
    payload: MediaPayload,
    request: Request,
    x_host_token: Optional[str] = Header(default=None, alias="X-Host-Token"),
):
    hall = _hall(request)
    if hall is None:
        return _error_json(status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable")
    room = hall.get_room(room_ref)
    if room is None:
        return _error_json(status.HTTP_404_NOT_FOUND, protocol.ERR_ROOM_NOT_FOUND, "room not found")
    try:
        await hall.change_media(
            room, x_host_token or "", payload.model_dump(exclude_none=True)
        )
    except HallError as exc:
        return _hall_error(exc)
    return {
        **_media_fields(room, hall),
        "playlist": room.playlist_public(),
        "room": room.public_info(),
    }


@router.post("/api/rooms/{room_ref}/playlist")
async def add_playlist_item(
    room_ref: str,
    payload: MediaPayload,
    request: Request,
    x_host_token: Optional[str] = Header(default=None, alias="X-Host-Token"),
):
    hall = _hall(request)
    if hall is None:
        return _error_json(status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable")
    room = hall.get_room(room_ref)
    if room is None:
        return _error_json(status.HTTP_404_NOT_FOUND, protocol.ERR_ROOM_NOT_FOUND, "room not found")
    try:
        media, activated = await hall.add_playlist_media(
            room, x_host_token or "", payload.model_dump(exclude_none=True)
        )
    except HallError as exc:
        return _hall_error(exc)
    # ``_media_fields`` also carries the *active* media id, so the id of the
    # item that was just appended has to win.
    return {
        **_media_fields(room, hall),
        "media_id": media.media_id,
        "active": activated,
        "playlist": room.playlist_public(),
        "room": room.public_info(),
    }


@router.post("/api/rooms/{room_ref}/playlist/{media_id}/activate")
async def activate_playlist_item(
    room_ref: str,
    media_id: str,
    request: Request,
    x_host_token: Optional[str] = Header(default=None, alias="X-Host-Token"),
):
    hall = _hall(request)
    if hall is None:
        return _error_json(status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable")
    room = hall.get_room(room_ref)
    if room is None:
        return _error_json(status.HTTP_404_NOT_FOUND, protocol.ERR_ROOM_NOT_FOUND, "room not found")
    try:
        await hall.select_playlist_media(room, x_host_token or "", media_id)
    except HallError as exc:
        return _hall_error(exc)
    return {
        **_media_fields(room, hall),
        "playlist": room.playlist_public(),
        "room": room.public_info(),
    }


@router.delete("/api/rooms/{room_ref}/playlist/{media_id}")
async def remove_playlist_item(
    room_ref: str,
    media_id: str,
    request: Request,
    x_host_token: Optional[str] = Header(default=None, alias="X-Host-Token"),
):
    hall = _hall(request)
    if hall is None:
        return _error_json(status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable")
    room = hall.get_room(room_ref)
    if room is None:
        return _error_json(status.HTTP_404_NOT_FOUND, protocol.ERR_ROOM_NOT_FOUND, "room not found")
    try:
        await hall.remove_playlist_media(room, x_host_token or "", media_id)
    except HallError as exc:
        return _hall_error(exc)
    return {
        **_media_fields(room, hall),
        "playlist": room.playlist_public(),
        "room": room.public_info(),
    }


@router.post("/api/rooms/{room_ref}/join")
async def join_room(room_ref: str, payload: JoinRequest, request: Request):
    hall = _hall(request)
    if hall is None:
        return _error_json(status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable")
    try:
        member = await hall.join_room(room_ref, payload.name)
        room = hall.get_room(room_ref)
        if room is None:
            raise HallError("room not found", "ROOM_NOT_FOUND", 404)
    except HallError as exc:
        return _hall_error(exc)
    return {
        "member_token": member.token,
        "member_id": member.member_id,
        "room_id": room.room_id,
        "room_code": room.room_code,
        **_media_fields(room, hall),
        "ws_url": f"/hall/ws/{room.room_id}",
        "room": room.public_info(),
    }


@router.post("/api/rooms/{room_ref}/jellyfin/session")
async def jellyfin_session(
    room_ref: str,
    payload: JellyfinLoginRequest,
    request: Request,
    x_host_token: Optional[str] = Header(default=None, alias="X-Host-Token"),
):
    """Sign the host in to Jellyfin, or adopt a token the browser remembered.

    The password is used for this one call and never stored.  The resulting
    token stays on the room and is never returned to a client.
    """
    hall = _hall(request)
    if hall is None:
        return _error_json(status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable")
    room = hall.get_room(room_ref)
    if room is None:
        return _error_json(status.HTTP_404_NOT_FOUND, protocol.ERR_ROOM_NOT_FOUND, "room not found")
    try:
        if payload.token:
            session = await hall.jellyfin_attach_token(
                room, x_host_token or "", payload.token, payload.user_id
            )
        else:
            session = await hall.jellyfin_login(
                room,
                x_host_token or "",
                payload.username.strip(),
                payload.password,
            )
        libraries = await hall.jellyfin_libraries(room)
    except HallError as exc:
        return _hall_error(exc)
    # The host gets the token back so this browser can stay signed in for a
    # week.  It is the host's own credential — every other endpoint, and every
    # payload a member can read, stays free of it.
    return {"jellyfin": session.public(), "libraries": libraries, "token": session.token}


@router.get("/api/rooms/{room_ref}/jellyfin/items")
async def jellyfin_items(
    room_ref: str,
    request: Request,
    parent: Optional[str] = Query(default=None),
    library: Optional[str] = Query(default=None),
    q: str = Query(default=""),
    start: int = Query(default=0),
    limit: int = Query(default=60),
    mode: str = Query(default="children"),
    x_host_token: Optional[str] = Header(default=None, alias="X-Host-Token"),
):
    hall = _hall(request)
    if hall is None:
        return _error_json(status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable")
    room = hall.get_room(room_ref)
    if room is None:
        return _error_json(status.HTTP_404_NOT_FOUND, protocol.ERR_ROOM_NOT_FOUND, "room not found")
    try:
        # `parent` is the folder being browsed; `library` is the older name for
        # the same thing and still comes from cached browser pages.
        return await hall.jellyfin_items(
            room, x_host_token or "", parent or library, q, start, limit, mode
        )
    except HallError as exc:
        return _hall_error(exc)


@router.get("/api/rooms/{room_ref}/jellyfin/status")
async def jellyfin_status(room_ref: str, request: Request):
    """What the host view needs to know before it shows the Jellyfin menu."""
    hall = _hall(request)
    if hall is None:
        return _error_json(status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable")
    room = hall.get_room(room_ref)
    if room is None:
        return _error_json(status.HTTP_404_NOT_FOUND, protocol.ERR_ROOM_NOT_FOUND, "room not found")
    config = hall.config.jellyfin
    return {
        "enabled": bool(config.enabled and hall.jellyfin is not None),
        "authenticated": room.jellyfin is not None,
        "jellyfin": room.jellyfin.public() if room.jellyfin else None,
        "max_bitrate": config.max_bitrate,
    }


@router.post("/api/rooms/{room_ref}/hls/seek")
async def hls_seek(
    room_ref: str,
    payload: HlsSeekRequest,
    request: Request,
    x_host_token: Optional[str] = Header(default=None, alias="X-Host-Token"),
):
    """Restart the room's transcode at a new position.

    The response carries the room's new playlist URL; every client's
    ``media_epoch`` moves with it, which is how members know to reload.
    """
    hall = _hall(request)
    if hall is None:
        return _error_json(status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable")
    room = hall.get_room(room_ref)
    if room is None:
        return _error_json(status.HTTP_404_NOT_FOUND, protocol.ERR_ROOM_NOT_FOUND, "room not found")
    try:
        await hall.seek_transcode(room, x_host_token or "", payload.position)
    except HallError as exc:
        return _hall_error(exc)
    return {"code": 0, "position": round(payload.position, 3)}


@router.post("/api/rooms/{room_ref}/hls/subtitle")
async def hls_subtitle(
    room_ref: str,
    payload: HlsSubtitleRequest,
    request: Request,
    x_host_token: Optional[str] = Header(default=None, alias="X-Host-Token"),
):
    """Choose the subtitle burned into the room's transcode.

    The pick restarts the transcode at ``position``, so every client's
    ``media_epoch`` moves with it -- members follow without doing anything.
    """
    hall = _hall(request)
    if hall is None:
        return _error_json(status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable")
    room = hall.get_room(room_ref)
    if room is None:
        return _error_json(status.HTTP_404_NOT_FOUND, protocol.ERR_ROOM_NOT_FOUND, "room not found")
    choice: Any = payload.subtitle
    if isinstance(choice, str):
        text = choice.strip().lower()
        if text.isdigit():
            choice = int(text)
        else:
            choice = text or None
    try:
        await hall.set_transcode_subtitle(room, x_host_token or "", choice, payload.position)
    except HallError as exc:
        return _hall_error(exc)
    return {
        "code": 0,
        "subtitle": room.jellyfin_subtitle,
        "subtitles_off": room.jellyfin_subtitles_off,
    }


@router.get("/api/rooms/{room_ref}")
async def room_info(room_ref: str, request: Request):
    hall = _hall(request)
    if hall is None:
        return _error_json(status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable")
    room = hall.get_room(room_ref)
    if room is None:
        return _error_json(status.HTTP_404_NOT_FOUND, "ROOM_NOT_FOUND", "room not found")
    return room.public_info()


@router.delete("/api/rooms/{room_ref}")
async def end_room(
    room_ref: str,
    request: Request,
    x_host_token: Optional[str] = Header(default=None, alias="X-Host-Token"),
    token: Optional[str] = Query(default=None),
):
    hall = _hall(request)
    if hall is None:
        return _error_json(status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable")
    room = hall.get_room(room_ref)
    if room is None:
        return _error_json(status.HTTP_404_NOT_FOUND, "ROOM_NOT_FOUND", "room not found")
    host_token = x_host_token or token or ""
    try:
        await hall.end_room(room, host_token)
    except HallError as exc:
        return _hall_error(exc)
    return {"code": 0, "ended": True}


class _UploadTooLarge(Exception):
    pass


async def _read_upload(request: Request, limit: int) -> bytes:
    declared = request.headers.get("content-length")
    if declared:
        try:
            if int(declared) > limit:
                raise _UploadTooLarge()
        except ValueError:
            pass
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > limit:
            raise _UploadTooLarge()
        body.extend(chunk)
    return bytes(body)


@router.post("/api/rooms/{room_ref}/subtitles")
async def upload_subtitle(
    room_ref: str,
    request: Request,
    name: str = Query(default="subtitles.srt"),
    lang: str = Query(default="und"),
    media_id: Optional[str] = Query(default=None),
    x_host_token: Optional[str] = Header(default=None, alias="X-Host-Token"),
):
    hall = _hall(request)
    if hall is None:
        return _error_json(status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable")
    room = hall.get_room(room_ref)
    if room is None:
        return _error_json(status.HTTP_404_NOT_FOUND, protocol.ERR_ROOM_NOT_FOUND, "room not found")
    if room.ended:
        return _error_json(status.HTTP_410_GONE, protocol.ERR_ROOM_ENDED, "room has ended")

    token = x_host_token or ""
    if token != room.host_token:
        return _error_json(status.HTTP_403_FORBIDDEN, protocol.ERR_FORBIDDEN, "only the host can upload subtitles")

    try:
        data = await _read_upload(request, hall.config.max_subtitle_bytes)
    except _UploadTooLarge:
        return _error_json(
            413,
            protocol.ERR_SUBTITLE_TOO_LARGE,
            f"subtitle is larger than {hall.config.max_subtitle_bytes} bytes",
        )

    try:
        subtitle = await hall.add_subtitle(room, token, name, lang, data, media_id)
    except HallError as exc:
        return _hall_error(exc)
    return {
        "subtitle_id": subtitle.subtitle_id,
        "name": subtitle.name,
        "lang": subtitle.lang,
        "format": subtitle.format,
        "size": subtitle.size,
        "url": hall.auth.subtitle_url(room.room_id, subtitle.subtitle_id),
        "media_id": subtitle.media_id,
        "active_subtitle_id": room.active_subtitle_id,
    }


@router.delete("/api/rooms/{room_ref}/subtitles/{subtitle_id}")
async def delete_subtitle(
    room_ref: str,
    subtitle_id: str,
    request: Request,
    x_host_token: Optional[str] = Header(default=None, alias="X-Host-Token"),
):
    hall = _hall(request)
    if hall is None:
        return _error_json(status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable")
    room = hall.get_room(room_ref)
    if room is None:
        return _error_json(status.HTTP_404_NOT_FOUND, protocol.ERR_ROOM_NOT_FOUND, "room not found")
    try:
        await hall.delete_subtitle(room, subtitle_id, x_host_token or "")
    except HallError as exc:
        return _hall_error(exc)
    return {"code": 0, "deleted": True}


@router.get("/subtitles/{room_ref}/{subtitle_id}.vtt")
async def get_subtitle(
    room_ref: str,
    subtitle_id: str,
    request: Request,
    exp: int = Query(...),
    sig: str = Query(...),
):
    hall = _hall(request)
    if hall is None:
        return _error_json(status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable")
    room = hall.get_room(room_ref)
    if room is None:
        return _error_json(status.HTTP_404_NOT_FOUND, protocol.ERR_ROOM_NOT_FOUND, "room not found")
    if room.ended:
        return _error_json(status.HTTP_410_GONE, protocol.ERR_ROOM_ENDED, "room has ended")
    subtitle = room.subtitles.get(subtitle_id)
    if subtitle is None:
        return _error_json(status.HTTP_404_NOT_FOUND, protocol.ERR_SUBTITLE_NOT_FOUND, "subtitle not found")
    if not hall.auth.verify_subtitle(room.room_id, subtitle_id, exp, sig):
        return _error_json(
            status.HTTP_403_FORBIDDEN, protocol.ERR_SIGNATURE, "subtitle URL is invalid or expired"
        )
    return Response(
        content=subtitle.data,
        media_type="text/vtt; charset=utf-8",
        headers={
            "X-Content-Type-Options": "nosniff",
            "Cache-Control": "private, max-age=3600",
        },
    )


@router.websocket("/ws/{room_ref}")
async def hall_websocket(websocket: WebSocket, room_ref: str):
    hall = getattr(websocket.app.state, "hall", None)
    if hall is None:
        await websocket.close(code=4503, reason="media hall is unavailable")
        return
    await handle_websocket(websocket, room_ref, hall)


def _media_or_error(request: Request, room_ref: str, media_id: str, exp: int, sig: str):
    hall = _hall(request)
    if hall is None:
        return None, None, _error_json(
            status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable"
        )
    room = hall.get_room(room_ref)
    if room is None:
        return None, None, _error_json(status.HTTP_404_NOT_FOUND, "ROOM_NOT_FOUND", "room not found")
    if room.ended:
        return None, None, _error_json(status.HTTP_410_GONE, "ROOM_ENDED", "room has ended")
    if room.media is None or room.media.media_id != media_id:
        return None, None, _error_json(status.HTTP_404_NOT_FOUND, "MEDIA_NOT_FOUND", "media not found")
    if not hall.auth.verify_media(room.room_id, media_id, exp, sig):
        return None, None, _error_json(
            status.HTTP_403_FORBIDDEN, "SIGNATURE_INVALID", "media URL is invalid or expired"
        )
    return hall, room, None


@router.head("/media/{room_ref}/{media_id}")
async def media_head(
    room_ref: str,
    media_id: str,
    request: Request,
    exp: int = Query(...),
    sig: str = Query(...),
):
    _, room, error = _media_or_error(request, room_ref, media_id, exp, sig)
    if error is not None:
        return error
    headers = full_headers(room.media.mime, room.media.size)
    return Response(status_code=status.HTTP_200_OK, headers=headers)


@router.get("/media/{room_ref}/{media_id}")
async def media_get(
    room_ref: str,
    media_id: str,
    request: Request,
    exp: int = Query(...),
    sig: str = Query(...),
    range_header: Optional[str] = Header(default=None, alias="Range"),
):
    hall, room, error = _media_or_error(request, room_ref, media_id, exp, sig)
    if error is not None:
        return error

    try:
        byte_range = parse_range(range_header, room.media.size, hall.config.block_size * 32)
    except RangeError:
        return Response(
            status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            headers=unsatisfiable_headers(room.media.size),
        )

    if room.media.kind == "url" and not room.media.range_supported:
        return StreamingResponse(
            room.upstream.stream_all(),
            status_code=status.HTTP_200_OK,
            headers=full_headers(room.media.mime, room.media.size),
        )

    target = byte_range
    if target is None:
        headers = full_headers(room.media.mime, room.media.size)
        first_index = 0
        response_status = status.HTTP_200_OK
    else:
        headers = partial_headers(room.media.mime, room.media.size, target)
        first_index = target.start // hall.config.block_size
        response_status = status.HTTP_206_PARTIAL_CONTENT

    # Fetch the first block before sending headers so source failures become 503s.
    try:
        await room.get_block(first_index)
    except HostSourceError as exc:
        return _error_json(
            exc.status_code,
            exc.code,
            exc.message,
            headers={"Retry-After": str(exc.retry_after)},
        )
    except UpstreamError as exc:
        return _error_json(exc.status_code, exc.code, str(exc))
    except Exception:
        logger.exception("Media prefetch failed room=%s", room.room_id)
        return _error_json(status.HTTP_502_BAD_GATEWAY, "SOURCE_ERROR", "failed to read media source")

    if target is None:
        body = stream_full(room, request, hall.config.block_size)
    else:
        body = stream_range(room, request, target, hall.config.block_size)

    return StreamingResponse(body, status_code=response_status, headers=headers)


HLS_HEADERS = {
    "Cache-Control": "private, no-store",
    "X-Accel-Buffering": "no",
}


def _hls_or_error(
    request: Request, room_ref: str, media_id: str, epoch: int, exp: int, sig: str
):
    hall = _hall(request)
    if hall is None:
        return None, None, _error_json(
            status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable"
        )
    room = hall.get_room(room_ref)
    if room is None:
        return None, None, _error_json(status.HTTP_404_NOT_FOUND, "ROOM_NOT_FOUND", "room not found")
    if room.ended:
        return None, None, _error_json(status.HTTP_410_GONE, "ROOM_ENDED", "room has ended")
    if room.media is None or room.media.media_id != media_id:
        return None, None, _error_json(status.HTTP_404_NOT_FOUND, "MEDIA_NOT_FOUND", "media not found")
    if not hall.auth.verify_hls(room.room_id, media_id, epoch, exp, sig):
        return None, None, _error_json(
            status.HTTP_403_FORBIDDEN, "SIGNATURE_INVALID", "media URL is invalid or expired"
        )
    if epoch != room.media_epoch:
        # The transcode was restarted; the old playlist must not be followed.
        return None, None, _error_json(
            status.HTTP_409_CONFLICT, "STREAM_RESTARTED", "转码已重新开始，请重新加载播放地址"
        )
    return hall, room, None


@router.get("/media/{room_ref}/{media_id}/live.m3u8")
async def hls_playlist(
    room_ref: str,
    media_id: str,
    request: Request,
    epoch: int = Query(...),
    exp: int = Query(...),
    sig: str = Query(...),
):
    hall, room, error = _hls_or_error(request, room_ref, media_id, epoch, exp, sig)
    if error is not None:
        return error
    if room.media is None or room.media.playback_mode != "hls":
        return _error_json(
            status.HTTP_404_NOT_FOUND, "MEDIA_NOT_FOUND", "this item is not transcoded"
        )
    try:
        text = await hall.hls_playlist(room)
    except HallError as exc:
        return _hall_error(exc)
    return Response(
        content=text,
        media_type="application/vnd.apple.mpegurl",
        headers=HLS_HEADERS,
    )


@router.get("/media/{room_ref}/{media_id}/seg/{index}.ts")
async def hls_segment(
    room_ref: str,
    media_id: str,
    index: int,
    request: Request,
    epoch: int = Query(...),
    exp: int = Query(...),
    sig: str = Query(...),
):
    hall, room, error = _hls_or_error(request, room_ref, media_id, epoch, exp, sig)
    if error is not None:
        return error
    if room.media is None or room.media.playback_mode != "hls":
        return _error_json(
            status.HTTP_404_NOT_FOUND, "MEDIA_NOT_FOUND", "this item is not transcoded"
        )
    try:
        data = await hall.hls_segment(room, index)
    except HallError as exc:
        return _hall_error(exc)
    return Response(content=data, media_type="video/mp2t", headers=HLS_HEADERS)
