"""Fetch media blocks from the host browser over the control WebSocket."""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from . import protocol

logger = logging.getLogger(__name__)

FIRST_BYTE_TIMEOUT = 2.0
BLOCK_TIMEOUT = 5.0
BUFFERED_HIGH_WATER = 1024 * 1024


class HostSourceError(Exception):
    status_code = 503
    retry_after = 1

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class SourceOffline(HostSourceError):
    def __init__(self, message: str = "host is offline") -> None:
        super().__init__(protocol.ERR_SOURCE_OFFLINE, message)


class SourceBusy(HostSourceError):
    def __init__(self, message: str = "host source is slow") -> None:
        super().__init__(protocol.ERR_SOURCE_BUSY, message)


class GenerationChanged(HostSourceError):
    status_code = 503

    def __init__(self, message: str = "host generation changed") -> None:
        super().__init__(protocol.ERR_GENERATION_CHANGED, message)


@dataclass
class PendingRead:
    req_id: int
    gen: int
    offset: int
    length: int
    buffer: bytearray = field(default_factory=bytearray)
    next_seq: int = 0
    first_byte: asyncio.Event = field(default_factory=asyncio.Event)
    future: Optional[asyncio.Future] = None


class HostSource:
    def __init__(
        self,
        room_id: str,
        block_size: int,
        concurrency: int = 4,
        first_byte_timeout: float = FIRST_BYTE_TIMEOUT,
        block_timeout: float = BLOCK_TIMEOUT,
    ) -> None:
        self.room_id = room_id
        self.block_size = block_size
        self.first_byte_timeout = first_byte_timeout
        self.block_timeout = block_timeout
        self._semaphore = asyncio.Semaphore(max(1, concurrency))
        self._ws: Optional[Any] = None
        self._pending: Dict[int, PendingRead] = {}
        self._next_req_id = 1
        self._buffered = 0
        self._capacity = asyncio.Event()
        self._capacity.set()
        self.bytes_read = 0
        self.requests = 0
        self.timeouts = 0

    # -- connection ----------------------------------------------------
    @property
    def online(self) -> bool:
        return self._ws is not None

    def attach(self, ws: Any) -> None:
        self._ws = ws
        self._buffered = 0
        self._capacity.set()

    def detach(self, ws: Optional[Any] = None) -> None:
        if ws is not None and self._ws is not ws:
            return
        self._ws = None
        self.fail_all(SourceOffline())

    # -- flow control --------------------------------------------------
    def set_buffered(self, amount: int) -> None:
        self._buffered = max(0, int(amount))
        if self._buffered <= BUFFERED_HIGH_WATER:
            self._capacity.set()
        else:
            self._capacity.clear()

    async def _wait_for_capacity(self) -> None:
        if self._buffered <= BUFFERED_HIGH_WATER:
            return
        started = time.monotonic()
        while self._buffered > BUFFERED_HIGH_WATER:
            try:
                await asyncio.wait_for(self._capacity.wait(), 1.0)
            except asyncio.TimeoutError:
                if time.monotonic() - started > 3.0:
                    logger.warning("Host %s backpressure wait timed out", self.room_id)
                    return

    # -- reads ---------------------------------------------------------
    def _new_req_id(self) -> int:
        req_id = self._next_req_id
        self._next_req_id += 1
        if self._next_req_id > 0xFFFFFFFF:
            self._next_req_id = 1
        return req_id

    async def read(self, offset: int, length: int, gen: int) -> bytes:
        if not self.online:
            raise SourceOffline()
        if length <= 0:
            return b""

        async with self._semaphore:
            await self._wait_for_capacity()
            try:
                return await self._read_once(offset, length, gen)
            except (SourceBusy, asyncio.TimeoutError):
                logger.warning(
                    "Host read timeout room=%s offset=%d length=%d; retrying in halves",
                    self.room_id,
                    offset,
                    length,
                )
                self.timeouts += 1
                if not self.online:
                    raise SourceOffline()
                if length > 1:
                    return await self._read_halves(offset, length, gen)
                raise SourceBusy()
            except HostSourceError:
                raise
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("Host read failed room=%s", self.room_id)
                raise SourceBusy(str(exc))

    async def _read_halves(self, offset: int, length: int, gen: int) -> bytes:
        first = max(1, length // 2)
        try:
            left = await self._read_once(offset, first, gen)
            right = await self._read_once(offset + len(left), length - len(left), gen)
        except HostSourceError:
            raise
        except Exception as exc:
            raise SourceBusy(str(exc))
        return left + right

    async def _read_once(self, offset: int, length: int, gen: int) -> bytes:
        ws = self._ws
        if ws is None:
            raise SourceOffline()

        req_id = self._new_req_id()
        loop = asyncio.get_running_loop()
        pending = PendingRead(req_id=req_id, gen=gen, offset=offset, length=length)
        pending.future = loop.create_future()
        pending.future.add_done_callback(lambda f: None if f.cancelled() else f.exception())
        self._pending[req_id] = pending
        self.requests += 1

        try:
            await ws.send_json(
                {
                    "type": protocol.MSG_READ,
                    "id": req_id,
                    "offset": offset,
                    "length": length,
                    "gen": gen,
                }
            )
        except Exception:
            self._pending.pop(req_id, None)
            raise SourceOffline()

        deadline = loop.time() + self.block_timeout
        try:
            first_wait = min(self.first_byte_timeout, self.block_timeout)
            await asyncio.wait_for(pending.first_byte.wait(), first_wait)
            remaining = max(0.1, deadline - loop.time())
            data = await asyncio.wait_for(asyncio.shield(pending.future), remaining)
            self.bytes_read += len(data)
            return data
        except asyncio.TimeoutError:
            await self._cancel(req_id)
            raise SourceBusy()
        except asyncio.CancelledError:
            await self._cancel(req_id)
            raise
        finally:
            self._pending.pop(req_id, None)

    async def _cancel(self, req_id: int) -> None:
        ws = self._ws
        if ws is None:
            return
        try:
            await ws.send_json({"type": protocol.MSG_CANCEL, "id": req_id})
        except Exception:
            pass

    # -- binary frames from the host ----------------------------------
    def handle_binary(self, data: bytes) -> None:
        try:
            req_id, gen, seq, flags, payload = protocol.decode_frame(data)
        except ValueError:
            return

        pending = self._pending.get(req_id)
        if pending is None:
            return
        if gen != pending.gen:
            return

        if flags & protocol.FLAG_ERROR:
            self._fail(pending, SourceOffline("host failed to read the requested block"))
            return

        if seq != pending.next_seq:
            self._fail(pending, SourceBusy("out-of-order host frame"))
            return
        pending.next_seq += 1
        pending.buffer.extend(payload)
        if not pending.first_byte.is_set():
            pending.first_byte.set()

        if flags & protocol.FLAG_FINAL:
            if len(pending.buffer) < pending.length:
                self._fail(
                    pending,
                    SourceBusy(
                        f"short host read: got {len(pending.buffer)} of {pending.length} bytes"
                    ),
                )
            else:
                self._resolve(pending, bytes(pending.buffer[: pending.length]))

    def _resolve(self, pending: PendingRead, data: bytes) -> None:
        self._pending.pop(pending.req_id, None)
        if not pending.future.done():
            pending.future.set_result(data)

    def _fail(self, pending: PendingRead, error: HostSourceError) -> None:
        self._pending.pop(pending.req_id, None)
        # Wake a reader that is still waiting for the first frame; otherwise it
        # sits out the full timeout and reports a spurious source-busy error.
        pending.first_byte.set()
        if not pending.future.done():
            pending.future.set_exception(error)

    def fail_all(self, error: Optional[HostSourceError] = None) -> None:
        if error is None:
            error = SourceOffline()
        for pending in list(self._pending.values()):
            self._fail(pending, error)
        self._pending.clear()

    def supersede(self) -> None:
        """Invalidate in-flight reads after the host seeks or reconnects."""
        for pending in list(self._pending.values()):
            self._fail(pending, GenerationChanged())
        self._pending.clear()

    def metrics(self) -> Dict[str, Any]:
        return {
            "bytes_read": self.bytes_read,
            "requests": self.requests,
            "timeouts": self.timeouts,
            "pending": len(self._pending),
            "buffered": self._buffered,
            "online": self.online,
        }
