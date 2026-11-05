"""HTTP and WebSocket routes for the media hall."""

import logging
from typing import Optional

from fastapi import APIRouter, Header, Query, Request, WebSocket, status
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

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
    media: MediaPayload


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


@router.post("/api/rooms")
async def create_room(payload: CreateRoomRequest, request: Request):
    hall = _hall(request)
    if hall is None:
        return _error_json(status.HTTP_503_SERVICE_UNAVAILABLE, "HALL_UNAVAILABLE", "media hall is unavailable")
    try:
        room = await hall.create_room(payload.name, payload.media.model_dump(exclude_none=True))
    except HallError as exc:
        return _hall_error(exc)
    return {
        "room_id": room.room_id,
        "room_code": room.room_code,
        "host_token": room.host_token,
        "media_id": room.media.media_id,
        "media": room.media.public(),
        "media_url": hall.auth.media_url(room.room_id, room.media.media_id),
        "ws_url": f"/hall/ws/{room.room_id}",
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
        "media": room.media.public(),
        "media_url": hall.auth.media_url(room.room_id, room.media.media_id),
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
    if room.media.media_id != media_id:
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
