"""HTTP Range parsing and streaming helpers for hall media."""

import asyncio
import logging
from dataclasses import dataclass
from typing import AsyncIterator, Dict, Optional, Tuple

from starlette.requests import Request

from .host_source import HostSourceError

logger = logging.getLogger(__name__)

DEFAULT_MAX_OPEN_RANGE = 8 * 1024 * 1024
# A member stream consumes blocks strictly in order, but every block is a
# round trip to the host browser.  Read a few blocks ahead so that latency is
# overlapped instead of capping throughput at one block per RTT.
PREFETCH_BLOCKS = 4


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


def _ignore_result(task: "asyncio.Task[bytes]") -> None:
    # Prefetches that are cancelled or abandoned must not log "exception was
    # never retrieved"; awaiting the task later still raises normally.
    if not task.cancelled():
        task.exception()


async def _iter_blocks(
    room,
    request: Request,
    first_index: int,
    last_index: int,
    prefetch: int = PREFETCH_BLOCKS,
) -> AsyncIterator[Tuple[int, bytes]]:
    pending: Dict[int, "asyncio.Task[bytes]"] = {}
    next_index = first_index

    def schedule() -> None:
        nonlocal next_index
        while next_index <= last_index and len(pending) < prefetch:
            task = asyncio.ensure_future(room.get_block(next_index))
            task.add_done_callback(_ignore_result)
            pending[next_index] = task
            next_index += 1

    try:
        schedule()
        index = first_index
        while index <= last_index:
            task = pending.pop(index, None)
            if task is None:
                task = asyncio.ensure_future(room.get_block(index))
                task.add_done_callback(_ignore_result)
            try:
                block = await task
            except HostSourceError:
                # The response headers are already on the wire.  Stop the body
                # quietly (the player will retry or show the room state) rather
                # than logging an ASGI traceback for an expected source loss.
                logger.debug("Media stream ended room=%s", getattr(room, "room_id", "?"))
                return
            if not block:
                return
            yield index, block
            index += 1
            schedule()
            if await request.is_disconnected():
                return
    finally:
        for task in pending.values():
            if not task.done():
                task.cancel()


async def stream_range(
    room,
    request: Request,
    byte_range: ByteRange,
    block_size: int,
) -> AsyncIterator[bytes]:
    position = byte_range.start
    first_index = byte_range.start // block_size
    last_index = byte_range.end // block_size
    async for index, block in _iter_blocks(room, request, first_index, last_index):
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


async def stream_full(room, request: Request, block_size: int) -> AsyncIterator[bytes]:
    total = room.media.size
    last_index = (total - 1) // block_size if total > 0 else -1
    async for _, block in _iter_blocks(room, request, 0, last_index):
        yield block
