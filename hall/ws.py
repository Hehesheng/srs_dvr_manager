"""WebSocket control channel for media hall rooms."""

import asyncio
import hmac
import logging
import time
from typing import Any, Dict, Optional

from starlette.websockets import WebSocket, WebSocketDisconnect

from . import protocol
from .manager import HallError, HallManager
from .rooms import Member, Room

logger = logging.getLogger(__name__)

AUTH_TIMEOUT = 10.0
READONLY_MESSAGES = {
    "seek",
    "play",
    "pause",
    "ratechange",
    "state",
    "end",
    protocol.MSG_SUBTITLE_SELECT,
}


def _wall_ms() -> int:
    return int(time.time() * 1000)


async def _send_error(ws: WebSocket, code: str, message: str) -> None:
    try:
        await ws.send_json({"type": protocol.MSG_ERROR, "code": code, "message": message})
        logger.debug("Sent WS error %s", code)
    except Exception:
        logger.exception("Failed to send WS error %s", code)


async def _authenticate(ws: WebSocket, room: Room, payload: Dict[str, Any]) -> Optional[str]:
    role = str(payload.get("role") or "")
    token = str(payload.get("token") or "")
    if role == "host":
        if not token or not hmac.compare_digest(token, room.host_token):
            await _send_error(ws, protocol.ERR_UNAUTHORIZED, "host token is invalid")
            await ws.close(code=4401)
            return None
        if room.ended:
            await _send_error(ws, protocol.ERR_ROOM_ENDED, "room has ended")
            await ws.close(code=4410)
            return None
        previous = room.host_ws
        if previous is not None and previous is not ws:
            try:
                await previous.close(code=4001, reason="replaced by a new host connection")
            except Exception:
                pass
        caps = payload.get("capabilities")
        room.host_media_capable = isinstance(caps, list) and "media_ready" in caps
        room.new_generation()
        room.set_host(ws)
        return "host"

    if role == "member":
        member = room.member_by_token(token)
        if member is None:
            await _send_error(ws, protocol.ERR_UNAUTHORIZED, "member token is invalid")
            await ws.close(code=4403)
            return None
        if room.ended:
            await _send_error(ws, protocol.ERR_ROOM_ENDED, "room has ended")
            await ws.close(code=4410)
            return None
        member.ws = ws
        member.connected_at = time.time()
        member.last_seen = time.time()
        return "member"

    await _send_error(ws, protocol.ERR_UNAUTHORIZED, "role must be host or member")
    await ws.close(code=4401)
    return None


async def handle_websocket(ws: WebSocket, room_ref: str, hall: HallManager) -> None:
    await ws.accept()

    try:
        first = await asyncio.wait_for(ws.receive(), AUTH_TIMEOUT)
    except (asyncio.TimeoutError, WebSocketDisconnect):
        await ws.close(code=4408, reason="auth timeout")
        return

    if first.get("type") != "websocket.receive" or "text" not in first:
        await ws.close(code=4401, reason="auth message required")
        return
    try:
        payload = protocol.decode_control(first["text"])
    except Exception:
        await ws.close(code=4401, reason="invalid auth message")
        return

    room = hall.get_room(room_ref)
    if room is None:
        await _send_error(ws, protocol.ERR_ROOM_NOT_FOUND, "room not found")
        await ws.close(code=4404)
        return
    if str(payload.get("type")) != protocol.MSG_AUTH:
        await ws.close(code=4401, reason="first message must be auth")
        return

    role = await _authenticate(ws, room, payload)
    if role is None:
        return

    member: Optional[Member] = None
    if role == "member":
        member = room.member_by_token(str(payload.get("token") or ""))
        if member is not None:
            room.members[member.member_id] = member

    state = room.room_state()
    await ws.send_json(
        {
            "type": protocol.MSG_AUTH_OK,
            "role": role,
            "room_id": room.room_id,
            "room_code": room.room_code,
            "server_ts": _wall_ms(),
            "media": room.media.public() if room.media else None,
            "media_url": room.media_url(),
            "playlist": room.playlist_public(),
            "host": room.host_snapshot(),
            "members": [m.public() for m in room.members.values()],
            "room": state,
        }
    )
    await hall.broadcast_room(room)
    logger.info(
        "Hall WS connected room=%s role=%s member=%s",
        room.room_id,
        role,
        member.member_id if member else "-",
    )

    logger.debug("Hall WS entering receive loop room=%s role=%s", room.room_id, role)
    try:
        while True:
            message = await ws.receive()
            if message.get("type") == "websocket.disconnect":
                break
            if "bytes" in message and message["bytes"] is not None:
                if role == "host":
                    room.host_source.handle_binary(message["bytes"])
                else:
                    await _send_error(ws, protocol.ERR_MEMBER_READONLY, "播放由房主控制")
                continue
            text = message.get("text")
            if text is None:
                continue
            try:
                msg = protocol.decode_control(text)
            except Exception:
                await _send_error(ws, protocol.ERR_BAD_MESSAGE, "invalid JSON message")
                continue
            room.touch()
            if role == "host":
                await _handle_host_message(ws, room, hall, msg)
            else:
                await _handle_member_message(ws, room, member, msg)
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("Hall WS loop failed room=%s role=%s", room.room_id, role)
    finally:
        if role == "host":
            room.clear_host(ws)
            logger.info("Hall host disconnected room=%s", room.room_id)
        elif member is not None:
            if member.ws is ws:
                member.ws = None
            member.last_seen = time.time()
            logger.info("Hall member disconnected room=%s member=%s", room.room_id, member.member_id)
        if not room.ended:
            await hall.broadcast_room(room)


async def _handle_host_message(ws: WebSocket, room: Room, hall: HallManager, msg: Dict[str, Any]) -> None:
    kind = str(msg.get("type") or "")
    if kind in (protocol.MSG_STATE, protocol.MSG_SEEK):
        try:
            events = room.timeline.update(msg, force_seek=(kind == protocol.MSG_SEEK))
        except (TypeError, ValueError):
            await _send_error(ws, protocol.ERR_BAD_MESSAGE, "invalid state payload")
            return
        if "seek" in events:
            # In-flight host reads belong to the previous position; drop them so
            # members immediately fetch from the new target.
            room.new_generation()
        for event in events:
            await hall.broadcast_host_event(room, event)
        return
    if kind == protocol.MSG_END:
        room.timeline.mark_ended()
        await hall.broadcast_host_event(room, "state")
        return
    if kind == protocol.MSG_SUBTITLE_SELECT:
        raw_id = msg.get("subtitle_id")
        subtitle_id = str(raw_id) if raw_id else None
        visible = bool(msg.get("visible", True))
        try:
            await hall.select_subtitle(room, subtitle_id, visible)
        except HallError as exc:
            await _send_error(ws, exc.code, exc.message)
        return
    if kind == protocol.MSG_MEDIA_READY:
        room.mark_host_media(msg.get("media_id"))
        return
    if kind == protocol.MSG_FLOW:
        try:
            room.host_source.set_buffered(int(msg.get("buffered") or 0))
        except (TypeError, ValueError):
            pass
        return
    if kind in (protocol.MSG_PING,):
        await ws.send_json(
            {
                "type": protocol.MSG_PONG,
                "t0": msg.get("t0"),
                "t1": _wall_ms(),
                "t2": _wall_ms(),
            }
        )
        return
    await _send_error(ws, protocol.ERR_BAD_MESSAGE, f"unsupported host message: {kind}")


async def _handle_member_message(
    ws: WebSocket, room: Room, member: Optional[Member], msg: Dict[str, Any]
) -> None:
    kind = str(msg.get("type") or "")
    logger.debug("Member message room=%s kind=%s member=%s", room.room_id, kind, member.member_id if member else "-")
    if member is not None:
        member.last_seen = time.time()

    if kind == protocol.MSG_PING:
        t1 = _wall_ms()
        t2 = _wall_ms()
        await ws.send_json(
            {
                "type": protocol.MSG_PONG,
                "t0": msg.get("t0"),
                "t1": t1,
                "t2": t2,
            }
        )
        return

    if kind == protocol.MSG_STATUS:
        if member is not None:
            member.status = {
                "position": msg.get("position"),
                "buffering": msg.get("buffering"),
                "rtt": msg.get("rtt"),
                "jitter": msg.get("jitter"),
            }
        return

    if kind in READONLY_MESSAGES:
        await _send_error(ws, protocol.ERR_MEMBER_READONLY, "播放由房主控制")
        return

    await _send_error(ws, protocol.ERR_BAD_MESSAGE, f"unsupported member message: {kind}")
