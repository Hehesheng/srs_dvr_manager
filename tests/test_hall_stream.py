import asyncio

from hall.host_source import HostSourceError
from hall.media import ByteRange, PREFETCH_BLOCKS, stream_full, stream_range


class FakeRequest:
    def __init__(self, disconnect_after_calls: int = 0) -> None:
        self.calls = 0
        self.disconnect_after_calls = disconnect_after_calls

    async def is_disconnected(self) -> bool:
        self.calls += 1
        return self.disconnect_after_calls > 0 and self.calls >= self.disconnect_after_calls


class FakeRoom:
    def __init__(self, size: int, block_size: int, delay: float = 0.01) -> None:
        self.media = type("Media", (), {"size": size})()
        self.block_size = block_size
        self.delay = delay
        self.calls = []
        self.active = 0
        self.max_active = 0

    def block(self, index: int) -> bytes:
        start = index * self.block_size
        length = min(self.block_size, self.media.size - start)
        return bytes([index % 251]) * max(0, length)

    async def get_block(self, index: int) -> bytes:
        self.calls.append(index)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(self.delay)
            return self.block(index)
        finally:
            self.active -= 1


def test_stream_range_slices_and_prefetches():
    async def scenario():
        room = FakeRoom(size=1000, block_size=64, delay=0.02)
        request = FakeRequest()
        chunks = [
            chunk
            async for chunk in stream_range(room, request, ByteRange(100, 299), 64)
        ]
        body = b"".join(chunks)
        assert body == b"".join(room.block(i) for i in (1, 2, 3, 4))[36 : 36 + 200]
        # Blocks 1..4 are needed; read-ahead must have overlapped the reads.
        assert sorted(room.calls) == [1, 2, 3, 4]
        assert room.max_active >= 2
        assert room.max_active <= PREFETCH_BLOCKS

    asyncio.run(scenario())


def test_stream_range_does_not_fetch_past_range():
    async def scenario():
        room = FakeRoom(size=1000, block_size=64, delay=0)
        request = FakeRequest()
        body = b"".join(
            [chunk async for chunk in stream_range(room, request, ByteRange(0, 63), 64)]
        )
        assert body == room.block(0)
        assert room.calls == [0]

    asyncio.run(scenario())


def test_stream_range_stops_when_client_disconnects():
    async def scenario():
        room = FakeRoom(size=1000, block_size=64, delay=0.01)
        request = FakeRequest(disconnect_after_calls=1)
        body = b"".join(
            [chunk async for chunk in stream_range(room, request, ByteRange(0, 999), 64)]
        )
        assert body == room.block(0)
        # Remaining prefetches are cancelled once the client goes away.
        await asyncio.sleep(0.05)
        assert room.active == 0
        assert len(room.calls) <= PREFETCH_BLOCKS

    asyncio.run(scenario())


def test_stream_stops_quietly_when_source_disappears():
    async def scenario():
        room = FakeRoom(size=1000, block_size=64, delay=0)

        async def failing(index):
            if index == 2:
                raise HostSourceError("SOURCE_OFFLINE", "host is offline")
            return room.block(index)

        room.get_block = failing
        request = FakeRequest()
        body = b"".join(
            [chunk async for chunk in stream_range(room, request, ByteRange(0, 999), 64)]
        )
        assert body == room.block(0) + room.block(1)

    asyncio.run(scenario())


def test_stream_full_prefetches_in_order():
    async def scenario():
        room = FakeRoom(size=130, block_size=64, delay=0.01)
        request = FakeRequest()
        chunks = [chunk async for chunk in stream_full(room, request, 64)]
        assert b"".join(chunks) == b"".join(room.block(i) for i in range(3))
        assert room.calls == [0, 1, 2]

    asyncio.run(scenario())
