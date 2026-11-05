import pytest

import hall.timeline as timeline_module
from hall.timeline import HostTimeline


class FakeClock:
    def __init__(self) -> None:
        self.mono = 0.0
        self.wall = 1_700_000_000_000.0

    def advance(self, seconds: float) -> None:
        self.mono += seconds * 1000.0
        self.wall += seconds * 1000.0


@pytest.fixture
def clock(monkeypatch):
    fake = FakeClock()
    monkeypatch.setattr(timeline_module, "_monotonic_ms", lambda: fake.mono)
    monkeypatch.setattr(timeline_module, "_wall_ms", lambda: fake.wall)
    return fake


def _state(seq=0, position=0.0, rate=1.0, playing=True, buffering=False, background=False):
    return {
        "seq": seq,
        "position": position,
        "rate": rate,
        "playing": playing,
        "buffering": buffering,
        "background": background,
    }


def test_first_state_initializes(clock):
    timeline = HostTimeline()
    assert timeline.update(_state(position=10.0)) == ["state"]
    assert timeline.snapshot()["position"] == pytest.approx(10.0)


def test_smooth_heartbeat_converges_without_broadcast(clock):
    timeline = HostTimeline()
    timeline.update(_state(position=10.0))
    clock.advance(1.0)
    # Host predicts 11.0; 11.1 is within the dead band.
    assert timeline.update(_state(position=11.1)) == []
    assert timeline.snapshot()["position"] == pytest.approx(11.0, abs=0.01)


def test_explicit_seek_is_honoured(clock):
    timeline = HostTimeline()
    timeline.update(_state(position=10.0))
    clock.advance(0.5)
    assert timeline.update(_state(seq=1, position=600.0)) == ["seek"]
    assert timeline.snapshot()["position"] == pytest.approx(600.0)


def test_force_seek_increments_sequence(clock):
    timeline = HostTimeline()
    timeline.update(_state(position=10.0))
    events = timeline.update(_state(seq=0, position=30.0), force_seek=True)
    assert events == ["seek"]
    assert timeline.seq == 1
    assert timeline.snapshot()["position"] == pytest.approx(30.0)


def test_pause_freezes_position(clock):
    timeline = HostTimeline()
    timeline.update(_state(position=10.0))
    clock.advance(2.0)
    assert timeline.update(_state(position=12.0, playing=False)) == ["state"]
    clock.advance(5.0)
    assert timeline.snapshot()["position"] == pytest.approx(12.0)


def test_buffering_freezes_position(clock):
    timeline = HostTimeline()
    timeline.update(_state(position=10.0))
    clock.advance(2.0)
    assert timeline.update(_state(position=12.0, buffering=True)) == ["state"]
    clock.advance(5.0)
    assert timeline.snapshot()["position"] == pytest.approx(12.0)


def test_rate_change_is_broadcast(clock):
    timeline = HostTimeline()
    timeline.update(_state(position=10.0))
    clock.advance(1.0)
    assert timeline.update(_state(position=12.0, rate=2.0)) == ["state"]
    clock.advance(1.0)
    assert timeline.snapshot()["position"] == pytest.approx(14.0)


def test_silent_host_freezes_and_marks_buffering(clock):
    timeline = HostTimeline()
    timeline.update(_state(position=10.0))
    clock.advance(1.0)
    snapshot = timeline.snapshot()
    assert snapshot["stale"] is False
    assert snapshot["buffering"] is False

    # Three missed heartbeats: stop projecting and tell members to wait.
    clock.advance(2.0)
    snapshot = timeline.snapshot()
    assert snapshot["stale"] is True
    assert snapshot["buffering"] is True
    assert snapshot["position"] == pytest.approx(12.5)

    # The next heartbeat clears the stale flag (the pulse broadcast conveys it).
    clock.advance(1.0)
    assert timeline.update(_state(position=14.0)) == []
    snapshot = timeline.snapshot()
    assert snapshot["stale"] is False
    assert snapshot["buffering"] is False
    assert snapshot["position"] == pytest.approx(14.0)


def test_hidden_host_keeps_projecting(clock):
    timeline = HostTimeline()
    timeline.update(_state(position=10.0))
    clock.advance(1.0)
    # The host switched tabs: still playing, but only visibility changed.
    assert timeline.update(_state(position=11.0, background=True)) == ["state"]
    assert timeline.snapshot()["background"] is True

    # Timer throttling silences heartbeats for a long time; members must not
    # see a stale/buffering host while the host tab is merely hidden.
    clock.advance(600.0)
    snapshot = timeline.snapshot()
    assert snapshot["stale"] is False
    assert snapshot["buffering"] is False
    assert snapshot["playing"] is True
    assert snapshot["position"] == pytest.approx(611.0, abs=0.01)

    # Returning to the foreground restores the short stale threshold.
    assert timeline.update(_state(position=611.1, background=False)) == ["state"]
    clock.advance(3.0)
    snapshot = timeline.snapshot()
    assert snapshot["stale"] is True
    assert snapshot["buffering"] is True


def test_repeated_large_error_triggers_resync(clock):
    timeline = HostTimeline()
    timeline.update(_state(position=10.0))
    events = []
    observed = 10.0
    for _ in range(3):
        clock.advance(1.0)
        observed += 3.0
        events.extend(timeline.update(_state(position=observed)))
    assert "resync" in events
    assert timeline.position == pytest.approx(observed)
