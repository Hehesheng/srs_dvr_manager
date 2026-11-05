"""Server-side host timeline filter (a small PLL).

The host is the only playback authority.  Heartbeats arrive every ~250ms while
playing and include a monotonically increasing ``seq`` that changes only on an
explicit seek.  We smooth ordinary heartbeat jitter so members are not told to
seek for every network hiccup, but always honour ``seq`` changes immediately.
"""

import time
from typing import Any, Dict, List


def _monotonic_ms() -> float:
    return time.monotonic() * 1000.0


def _wall_ms() -> float:
    return time.time() * 1000.0


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


class HostTimeline:
    def __init__(self) -> None:
        now = _monotonic_ms()
        self.seq = 0
        self.position = 0.0
        self.rate = 1.0
        self.playing = False
        self.buffering = False
        self.updated_mono = now
        self.updated_wall = _wall_ms()
        self._bad_count = 0
        self._seen_state = False

    def _apply(self, msg: Dict[str, Any], now_mono: float, now_wall: float) -> None:
        self.seq = int(msg.get("seq", self.seq))
        self.position = max(0.0, float(msg.get("position", self.position)))
        self.rate = _clamp(float(msg.get("rate", self.rate)), 0.25, 4.0)
        self.playing = bool(msg.get("playing", self.playing))
        self.buffering = bool(msg.get("buffering", self.buffering))
        self.updated_mono = now_mono
        self.updated_wall = now_wall

    def update(self, msg: Dict[str, Any], force_seek: bool = False) -> List[str]:
        """Apply a host heartbeat and return broadcast events.

        Events are ``seek`` (explicit discontinuity), ``state`` (play/pause/
        buffering/rate change) and ``resync`` (the filter gave up correcting).
        """
        now_mono = _monotonic_ms()
        now_wall = _wall_ms()
        msg_seq = int(msg.get("seq", self.seq))

        if not self._seen_state:
            if force_seek and msg_seq == self.seq:
                msg = dict(msg)
                msg["seq"] = self.seq + 1
            self._apply(msg, now_mono, now_wall)
            self._seen_state = True
            return ["seek"] if force_seek else ["state"]

        if force_seek or msg_seq != self.seq:
            if force_seek and msg_seq == self.seq:
                msg = dict(msg)
                msg["seq"] = self.seq + 1
            self._bad_count = 0
            self._apply(msg, now_mono, now_wall)
            return ["seek"]

        if self.playing and not self.buffering:
            predicted = self.position + (now_mono - self.updated_mono) / 1000.0 * self.rate
        else:
            predicted = self.position

        observed = max(0.0, float(msg.get("position", self.position)))
        error = observed - predicted

        playing = bool(msg.get("playing", self.playing))
        buffering = bool(msg.get("buffering", self.buffering))
        rate = _clamp(float(msg.get("rate", self.rate)), 0.25, 4.0)

        if playing != self.playing or buffering != self.buffering or abs(rate - self.rate) > 1e-6:
            self.position = observed
            self.playing = playing
            self.buffering = buffering
            self.rate = rate
            self.updated_mono = now_mono
            self.updated_wall = now_wall
            self._bad_count = 0
            return ["state"]

        if abs(error) <= 0.15:
            self.position = predicted
            self.updated_mono = now_mono
            self.updated_wall = now_wall
            return []

        self.position = predicted + 0.2 * _clamp(error, -0.5, 0.5)
        self.updated_mono = now_mono
        self.updated_wall = now_wall

        if abs(error) > 1.0:
            self._bad_count += 1
        else:
            self._bad_count = 0

        if self._bad_count >= 3:
            self.position = observed
            self.updated_mono = now_mono
            self.updated_wall = now_wall
            self._bad_count = 0
            return ["resync"]

        return []

    def mark_ended(self, duration: float = 0.0) -> None:
        self.playing = False
        self.buffering = False
        if duration > 0:
            self.position = duration
        self.updated_mono = _monotonic_ms()
        self.updated_wall = _wall_ms()

    def snapshot(self) -> Dict[str, Any]:
        """Project the timeline to now for clients."""
        now_mono = _monotonic_ms()
        now_wall = _wall_ms()
        position = self.position
        if self.playing and not self.buffering:
            position += (now_mono - self.updated_mono) / 1000.0 * self.rate
        return {
            "seq": self.seq,
            "position": round(max(0.0, position), 3),
            "rate": round(self.rate, 3),
            "playing": self.playing,
            "buffering": self.buffering,
            "updated_at": round(now_wall, 3),
            "server_ts": round(now_wall, 3),
        }

    def raw_state(self) -> Dict[str, Any]:
        return {
            "seq": self.seq,
            "position": round(self.position, 3),
            "rate": round(self.rate, 3),
            "playing": self.playing,
            "buffering": self.buffering,
            "updated_at": round(self.updated_wall, 3),
        }
