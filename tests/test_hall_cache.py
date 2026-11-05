import asyncio

from hall.cache import BlockCache


def test_single_flight_and_lru():
    async def scenario():
        cache = BlockCache(block_size=10, max_bytes=25)
        calls = {"n": 0}

        async def fetch():
            calls["n"] += 1
            await asyncio.sleep(0.01)
            return b"a" * 10

        results = await asyncio.gather(*[cache.get_or_fetch(0, fetch) for _ in range(5)])
        assert results == [b"a" * 10] * 5
        assert calls["n"] == 1
        assert cache.bytes_used == 10

        cache.put(1, b"b" * 10)
        cache.put(2, b"c" * 10)
        assert cache.bytes_used <= 25
        assert 0 not in cache
        assert 1 in cache and 2 in cache

    asyncio.run(scenario())


def test_failure_is_propagated_to_all_waiters():
    async def scenario():
        cache = BlockCache(block_size=10, max_bytes=100)

        async def fetch():
            await asyncio.sleep(0.01)
            raise RuntimeError("boom")

        outcomes = await asyncio.gather(
            *[cache.get_or_fetch(3, fetch) for _ in range(3)],
            return_exceptions=True,
        )
        assert all(isinstance(value, RuntimeError) for value in outcomes)
        assert 3 not in cache

    asyncio.run(scenario())


def test_cache_hit_does_not_call_fetch():
    async def scenario():
        cache = BlockCache(block_size=10, max_bytes=100)
        cache.put(0, b"x" * 10)
        called = False

        async def fetch():
            nonlocal called
            called = True
            return b"y" * 10

        assert await cache.get_or_fetch(0, fetch) == b"x" * 10
        assert called is False

    asyncio.run(scenario())
