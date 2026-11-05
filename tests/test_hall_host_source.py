import asyncio

import pytest

from hall import protocol
from hall.host_source import HostSource, SourceOffline


class FakeWs:
    def __init__(self) -> None:
        self.sent = []

    async def send_json(self, payload):
        self.sent.append(payload)


def _wait_read(ws: FakeWs, timeout=1.0):
    async def waiter():
        while not any(msg.get("type") == protocol.MSG_READ for msg in ws.sent):
            await asyncio.sleep(0.001)

    return asyncio.wait_for(waiter(), timeout)


def test_read_assembles_sub_frames():
    async def scenario():
        ws = FakeWs()
        source = HostSource("r_test", block_size=8, concurrency=2, first_byte_timeout=0.2, block_timeout=0.5)
        source.attach(ws)
        task = asyncio.create_task(source.read(0, 8, gen=0))
        await _wait_read(ws)
        read = ws.sent[0]
        assert read["offset"] == 0 and read["length"] == 8

        source.handle_binary(protocol.encode_frame(read["id"], 0, 0, 0, b"abcd"))
        source.handle_binary(protocol.encode_frame(read["id"], 0, 1, protocol.FLAG_FINAL, b"efgh"))
        assert await task == b"abcdefgh"

    asyncio.run(scenario())


def test_wrong_generation_is_dropped():
    async def scenario():
        ws = FakeWs()
        source = HostSource("r_test", block_size=8, concurrency=2, first_byte_timeout=0.2, block_timeout=0.5)
        source.attach(ws)
        task = asyncio.create_task(source.read(0, 4, gen=2))
        await _wait_read(ws)
        read = ws.sent[0]
        source.handle_binary(protocol.encode_frame(read["id"], 1, 0, protocol.FLAG_FINAL, b"xxxx"))
        source.handle_binary(protocol.encode_frame(read["id"], 2, 0, protocol.FLAG_FINAL, b"good"))
        assert await task == b"good"

    asyncio.run(scenario())


def test_out_of_order_frame_fails_read():
    async def scenario():
        ws = FakeWs()
        source = HostSource("r_test", block_size=8, concurrency=2, first_byte_timeout=0.2, block_timeout=0.5)
        source.attach(ws)
        task = asyncio.create_task(source.read(0, 4, gen=0))
        await _wait_read(ws)
        read = ws.sent[0]
        source.handle_binary(protocol.encode_frame(read["id"], 0, 1, protocol.FLAG_FINAL, b"data"))
        with pytest.raises(Exception):
            await task

    asyncio.run(scenario())


def test_detach_fails_pending_reads():
    async def scenario():
        ws = FakeWs()
        source = HostSource("r_test", block_size=8, concurrency=2, first_byte_timeout=0.2, block_timeout=0.5)
        source.attach(ws)
        task = asyncio.create_task(source.read(0, 4, gen=0))
        await _wait_read(ws)
        source.detach(ws)
        with pytest.raises(SourceOffline):
            await task

    asyncio.run(scenario())


def test_fail_all_wakes_first_byte_waiter():
    async def scenario():
        ws = FakeWs()
        source = HostSource(
            "r_test", block_size=8, concurrency=2, first_byte_timeout=5.0, block_timeout=5.0
        )
        source.attach(ws)
        task = asyncio.create_task(source.read(0, 8, gen=0))
        await _wait_read(ws)
        source.fail_all(SourceOffline("host is offline"))
        with pytest.raises(SourceOffline):
            await asyncio.wait_for(task, 0.5)
        assert source.timeouts == 0

    asyncio.run(scenario())
