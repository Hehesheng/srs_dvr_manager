"""HTTP and WebSocket routes for the media hall."""

import logging
from typing import Optional

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
        subtitle = await hall.add_subtitle(room, token, name, lang, data)
    except HallError as exc:
        return _hall_error(exc)
    return {
        "subtitle_id": subtitle.subtitle_id,
        "name": subtitle.name,
        "lang": subtitle.lang,
        "format": subtitle.format,
        "size": subtitle.size,
        "url": hall.auth.subtitle_url(room.room_id, subtitle.subtitle_id),
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
