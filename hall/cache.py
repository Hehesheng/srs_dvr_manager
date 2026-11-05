"""Per-room LRU block cache with single-flight loading."""

import asyncio
import logging
from collections import OrderedDict
from typing import Awaitable, Callable, Dict, Optional

logger = logging.getLogger(__name__)


class BlockCache:
    def __init__(self, block_size: int, max_bytes: int) -> None:
        self.block_size = block_size
        self.max_bytes = max(1, int(max_bytes))
        self._blocks: "OrderedDict[int, bytes]" = OrderedDict()
        self._ticks: Dict[int, int] = {}
        self._tick = 0
        self._inflight: Dict[int, asyncio.Task] = {}
        self.bytes_used = 0
        self.hits = 0
        self.misses = 0

    # -- introspection -------------------------------------------------
    def __contains__(self, index: int) -> bool:
        return index in self._blocks

    def get(self, index: int) -> Optional[bytes]:
        data = self._blocks.get(index)
        if data is None:
            self.misses += 1
            return None
        self.hits += 1
        self._blocks.move_to_end(index)
        self._tick += 1
        self._ticks[index] = self._tick
        return data

    def oldest_tick(self) -> Optional[int]:
        if not self._blocks:
            return None
        first = next(iter(self._blocks))
        return self._ticks.get(first, 0)

    # -- population ----------------------------------------------------
    def put(self, index: int, data: bytes) -> None:
        if not data:
            return
        previous = self._blocks.get(index)
        if previous is not None:
            self.bytes_used -= len(previous)
        self._blocks[index] = data
        self._blocks.move_to_end(index)
        self._tick += 1
        self._ticks[index] = self._tick
        self.bytes_used += len(data)
        while self.bytes_used > self.max_bytes and self._blocks:
            self.evict_one()

    def evict_one(self) -> int:
        if not self._blocks:
            return 0
        index, data = self._blocks.popitem(last=False)
        self._ticks.pop(index, None)
        self.bytes_used -= len(data)
        return len(data)

    def clear(self) -> None:
        self._blocks.clear()
        self._ticks.clear()
        self.bytes_used = 0

    # -- loading -------------------------------------------------------
    async def get_or_fetch(self, index: int, fetch: Callable[[], Awaitable[bytes]]) -> bytes:
        cached = self.get(index)
        if cached is not None:
            return cached

        task = self._inflight.get(index)
        if task is None:
            task = asyncio.ensure_future(self._load(index, fetch))
            self._inflight[index] = task
            task.add_done_callback(lambda t, i=index: self._finish(i, t))
        return await asyncio.shield(task)

    async def _load(self, index: int, fetch: Callable[[], Awaitable[bytes]]) -> bytes:
        data = await fetch()
        if data:
            self.put(index, data)
        return data

    def _finish(self, index: int, task: asyncio.Task) -> None:
        if self._inflight.get(index) is task:
            self._inflight.pop(index, None)
        if not task.cancelled():
            # Mark the exception as retrieved when every waiter was cancelled.
            task.exception()
