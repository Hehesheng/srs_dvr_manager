import asyncio

from hall.manager import HallConfig, HallManager


class FakeWs:
    def __init__(self) -> None:
        self.sent = []
        self.closed = None

    async def send_json(self, payload):
        self.sent.append(payload)

    async def close(self, code=1000, reason=""):
        self.closed = (code, reason)


def _manager(timeout: float = 0.05) -> HallManager:
    return HallManager(HallConfig(host_offline_timeout=timeout))


def test_room_ends_after_the_host_stays_offline():
    async def scenario():
        manager = _manager()
        room = await manager.create_room("offline room", None)
        host = FakeWs()
        room.set_host(host)
        member = await manager.join_room(room.room_code, "alice")
        member_ws = FakeWs()
        member.ws = member_ws

        room.clear_host(host)
        manager.host_disconnected(room)
        assert room.ended is False

        await asyncio.sleep(0.15)
        assert room.ended is True
        # Members are told the room is over instead of waiting for a host that
        # will never come back.
        assert member_ws.closed is not None
        assert member_ws.closed[0] == 4000
        assert room.host_online is False

    asyncio.run(scenario())


def test_host_reconnecting_cancels_the_offline_end():
    async def scenario():
        manager = _manager()
        room = await manager.create_room("reconnect room", None)
        first = FakeWs()
        room.set_host(first)
        room.clear_host(first)
        manager.host_disconnected(room)

        await asyncio.sleep(0.01)
        manager.host_connected(room)
        room.set_host(FakeWs())

        await asyncio.sleep(0.15)
        assert room.ended is False

    asyncio.run(scenario())


def test_ending_the_room_directly_clears_the_offline_timer():
    async def scenario():
        manager = _manager(timeout=0.1)
        room = await manager.create_room("manual end", None)
        host = FakeWs()
        room.set_host(host)
        room.clear_host(host)
        manager.host_disconnected(room)

        await manager.end_room(room, room.host_token)
        assert room.ended is True
        assert room.room_id not in manager._host_offline_tasks

        await asyncio.sleep(0.15)
        assert room.ended is True

    asyncio.run(scenario())
