import asyncio

import pytest

from hall import protocol, rooms
from hall.auth import HallAuth
from hall.host_source import SourceOffline
from hall.rooms import MediaInfo, Room


class FakeWs:
    def __init__(self) -> None:
        self.sent = []

    async def send_json(self, payload):
        self.sent.append(payload)


class FakeHall:
    def __init__(self) -> None:
        self.auth = HallAuth("test-secret")

    def enforce_global_cache(self) -> None:
        pass


def _media(media_id: str, size: int = 16) -> MediaInfo:
    return MediaInfo(
        media_id=media_id,
        kind="host_file",
        name=f"{media_id}.webm",
        size=size,
        mime="video/webm",
    )


def _room(media: MediaInfo) -> Room:
    return Room(
        FakeHall(),
        "room",
        media,
        block_size=8,
        cache_bytes=1024,
        host_read_concurrency=1,
    )


def _attach_host(room: Room, capable: bool = True) -> FakeWs:
    ws = FakeWs()
    room.host_media_capable = capable
    room.set_host(ws)
    return ws


def _wait_read(ws: FakeWs, timeout: float = 1.0):
    async def waiter():
        while not any(msg.get("type") == protocol.MSG_READ for msg in ws.sent):
            await asyncio.sleep(0.001)

    return asyncio.wait_for(waiter(), timeout)


def _answer(ws: FakeWs, room: Room, payload: bytes) -> None:
    read = next(msg for msg in ws.sent if msg.get("type") == protocol.MSG_READ)
    room.host_source.handle_binary(
        protocol.encode_frame(read["id"], read["gen"], 0, protocol.FLAG_FINAL, payload)
    )


def test_reads_wait_until_the_host_switched_media():
    async def scenario():
        room = _room(_media("m_old"))
        ws = _attach_host(room)
        room.mark_host_media("m_old")

        # A playlist switch points the room at a media the worker does not have
        # yet; reads must wait instead of being served from the previous file.
        room.replace_media(_media("m_new"))
        task = asyncio.create_task(room.get_block(0))
        await asyncio.sleep(0.1)
        assert task.done() is False
        assert not [m for m in ws.sent if m.get("type") == protocol.MSG_READ]

        room.mark_host_media("m_new")
        await _wait_read(ws)
        _answer(ws, room, b"new-data")
        assert await task == b"new-data"

    asyncio.run(scenario())


def test_announcing_another_media_does_not_unlock_reads():
    async def scenario():
        room = _room(_media("m_old"))
        ws = _attach_host(room)
        room.mark_host_media("m_old")
        room.replace_media(_media("m_new"))

        task = asyncio.create_task(room.get_block(0))
        room.mark_host_media("m_old")
        await asyncio.sleep(0.1)
        assert task.done() is False

        room.mark_host_media("m_new")
        await _wait_read(ws)
        _answer(ws, room, b"new-data")
        assert await task == b"new-data"

    asyncio.run(scenario())


def test_legacy_host_is_not_gated():
    async def scenario():
        room = _room(_media("m_old"))
        ws = _attach_host(room, capable=False)

        task = asyncio.create_task(room.get_block(0))
        await _wait_read(ws)
        _answer(ws, room, b"old-data")
        assert await task == b"old-data"

    asyncio.run(scenario())


def test_wait_times_out_when_the_host_never_switches(monkeypatch):
    async def scenario():
        room = _room(_media("m_old"))
        _attach_host(room)
        room.mark_host_media("m_old")
        room.replace_media(_media("m_new"))
        monkeypatch.setattr(rooms, "HOST_MEDIA_READY_TIMEOUT", 0.05)

        assert await room.wait_host_media() is False
        with pytest.raises(SourceOffline):
            await room.get_block(0)

    asyncio.run(scenario())


def test_reconnect_requires_a_fresh_announcement():
    async def scenario():
        room = _room(_media("m_one"))
        _attach_host(room, capable=True)
        room.mark_host_media("m_one")
        assert await room.wait_host_media(timeout=0.05) is True

        _attach_host(room, capable=True)
        assert await room.wait_host_media(timeout=0.05) is False

        room.mark_host_media("m_one")
        assert await room.wait_host_media(timeout=0.05) is True

    asyncio.run(scenario())
