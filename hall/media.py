"""HTTP Range parsing and streaming helpers for hall media."""

from dataclasses import dataclass
from typing import AsyncIterator, Dict, Optional

from starlette.requests import Request

DEFAULT_MAX_OPEN_RANGE = 8 * 1024 * 1024


class RangeError(ValueError):
    pass


@dataclass
class ByteRange:
    start: int
    end: int  # inclusive

    @property
    def length(self) -> int:
        return self.end - self.start + 1


def parse_range(header: Optional[str], total: int, max_open_bytes: int = DEFAULT_MAX_OPEN_RANGE) -> Optional[ByteRange]:
    if total <= 0:
        raise RangeError("empty media")
    if header is None or not header.strip():
        return None
    value = header.strip()
    if not value.lower().startswith("bytes="):
        raise RangeError("unsupported range unit")
    spec = value[6:].strip()
    if not spec or "," in spec:
        raise RangeError("only a single range is supported")
    start_s, sep, end_s = spec.partition("-")
    if not sep:
        raise RangeError("malformed range")

    if start_s == "":
        try:
            suffix = int(end_s)
        except ValueError:
            raise RangeError("malformed suffix range")
        if suffix <= 0:
            raise RangeError("invalid suffix range")
        start = max(0, total - suffix)
        end = total - 1
        return ByteRange(start, end)

    try:
        start = int(start_s)
    except ValueError:
        raise RangeError("malformed range start")
    if start < 0 or start >= total:
        raise RangeError("range starts beyond media")

    if end_s == "":
        end = min(total - 1, start + max_open_bytes - 1)
        return ByteRange(start, end)

    try:
        end = int(end_s)
    except ValueError:
        raise RangeError("malformed range end")
    if end < start:
        raise RangeError("range end precedes start")
    end = min(end, total - 1)
    return ByteRange(start, end)


def media_headers(mime: str, total: int) -> Dict[str, str]:
    return {
        "Content-Type": mime or "application/octet-stream",
        "Accept-Ranges": "bytes",
        "Cache-Control": "private, no-store",
        "X-Accel-Buffering": "no",
    }


def partial_headers(mime: str, total: int, byte_range: ByteRange) -> Dict[str, str]:
    headers = media_headers(mime, total)
    headers["Content-Range"] = f"bytes {byte_range.start}-{byte_range.end}/{total}"
    headers["Content-Length"] = str(byte_range.length)
    return headers


def full_headers(mime: str, total: int) -> Dict[str, str]:
    headers = media_headers(mime, total)
    headers["Content-Length"] = str(total)
    return headers


def unsatisfiable_headers(total: int) -> Dict[str, str]:
    return {
        "Content-Range": f"bytes */{total}",
        "Content-Length": "0",
        "Accept-Ranges": "bytes",
    }


async def stream_range(
    room,
    request: Request,
    byte_range: ByteRange,
    block_size: int,
) -> AsyncIterator[bytes]:
    position = byte_range.start
    while position <= byte_range.end:
        index = position // block_size
        block = await room.get_block(index)
        if not block:
            return
        block_start = index * block_size
        offset = position - block_start
        available = len(block) - offset
        if available <= 0:
            return
        length = min(available, byte_range.end - position + 1)
        chunk = block[offset : offset + length]
        if chunk:
            yield chunk
        position += len(chunk)
        if await request.is_disconnected():
            return


async def stream_full(room, request: Request, block_size: int) -> AsyncIterator[bytes]:
    total = room.media.size
    index = 0
    while index * block_size < total:
        block = await room.get_block(index)
        if not block:
            return
        yield block
        index += 1
        if await request.is_disconnected():
            return
