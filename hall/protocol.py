"""Wire protocol helpers for the media hall.

Control messages are JSON objects.  Bulk media data travels as binary frames
from the host to the server:

    offset  size  field
    0       4     req_id  uint32 BE
    4       4     gen     uint32 BE
    8       2     seq     uint16 BE
    10      2     flags   uint16 BE   bit0=final, bit1=error
    12      N     payload
"""

import json
import struct
from typing import Any, Dict, Tuple

HEADER = struct.Struct(">IIHH")
HEADER_SIZE = HEADER.size

FLAG_FINAL = 0x0001
FLAG_ERROR = 0x0002

MAX_CONTROL_MESSAGE = 64 * 1024
MAX_FRAME_PAYLOAD = 4 * 1024 * 1024

# Client -> server
MSG_AUTH = "auth"
MSG_STATE = "state"
MSG_SEEK = "seek"
MSG_END = "end"
MSG_STATUS = "status"
MSG_PING = "ping"
MSG_FLOW = "flow"
MSG_SUBTITLE_SELECT = "subtitle_select"
MSG_MEDIA_READY = "media_ready"

# Server -> client
MSG_AUTH_OK = "auth_ok"
MSG_READ = "read"
MSG_CANCEL = "cancel"
MSG_ROOM_STATE = "room_state"
MSG_RESYNC = "resync"
MSG_PONG = "pong"
MSG_ERROR = "error"

# Error codes
ERR_UNAUTHORIZED = "UNAUTHORIZED"
ERR_FORBIDDEN = "FORBIDDEN"
ERR_MEMBER_READONLY = "MEMBER_READONLY"
ERR_ROOM_NOT_FOUND = "ROOM_NOT_FOUND"
ERR_ROOM_FULL = "ROOM_FULL"
ERR_ROOM_ENDED = "ROOM_ENDED"
ERR_HOST_OFFLINE = "HOST_OFFLINE"
ERR_SOURCE_BUSY = "SOURCE_BUSY"
ERR_SOURCE_OFFLINE = "SOURCE_OFFLINE"
ERR_GENERATION_CHANGED = "GENERATION_CHANGED"
ERR_BAD_MESSAGE = "BAD_MESSAGE"
ERR_MEDIA_NOT_FOUND = "MEDIA_NOT_FOUND"
ERR_SIGNATURE = "SIGNATURE_INVALID"
ERR_RANGE_UNSUPPORTED = "RANGE_UNSUPPORTED"
ERR_SUBTITLE_NOT_FOUND = "SUBTITLE_NOT_FOUND"
ERR_SUBTITLE_TOO_LARGE = "SUBTITLE_TOO_LARGE"
ERR_SUBTITLE_FORMAT = "SUBTITLE_FORMAT_UNSUPPORTED"
ERR_TOO_MANY_SUBTITLES = "TOO_MANY_SUBTITLES"
ERR_JELLYFIN_CONFIG_REQUIRED = "JELLYFIN_CONFIG_REQUIRED"
ERR_JELLYFIN_URL_INVALID = "JELLYFIN_URL_INVALID"
ERR_JELLYFIN_AUTH_REQUIRED = "JELLYFIN_AUTH_REQUIRED"
ERR_JELLYFIN_AUTH_FAILED = "JELLYFIN_AUTH_FAILED"
ERR_JELLYFIN_UNAVAILABLE = "JELLYFIN_UNAVAILABLE"
ERR_JELLYFIN_TRANSCODE_FAILED = "JELLYFIN_TRANSCODE_FAILED"


def encode_control(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def decode_control(text: str) -> Dict[str, Any]:
    if len(text) > MAX_CONTROL_MESSAGE:
        raise ValueError("control message too large")
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("control message must be an object")
    return payload


def encode_frame(req_id: int, gen: int, seq: int, flags: int, payload: bytes = b"") -> bytes:
    if len(payload) > MAX_FRAME_PAYLOAD:
        raise ValueError("frame payload too large")
    return HEADER.pack(req_id & 0xFFFFFFFF, gen & 0xFFFFFFFF, seq & 0xFFFF, flags & 0xFFFF) + payload


def decode_frame(data: bytes) -> Tuple[int, int, int, int, bytes]:
    if len(data) < HEADER_SIZE:
        raise ValueError("frame too short")
    req_id, gen, seq, flags = HEADER.unpack_from(data)
    return req_id, gen, seq, flags, data[HEADER_SIZE:]
