"""
Unit tests for stream lifecycle tracking and resource-leak detection.
"""

import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from py_ipfs_lite.connection_tracker import ConnectionStatsTracker


class _FakeMuxedConn:
    def __init__(self, peer_id: str) -> None:
        self.peer_id = SimpleNamespace(to_base58=lambda: peer_id)


class _FakeStream:
    def __init__(
        self, peer_id: str, protocol: str | None = None, sid: str = "1"
    ) -> None:
        self.muxed_conn = _FakeMuxedConn(peer_id)
        self.muxed_stream = SimpleNamespace(stream_id=sid)
        self.protocol_id = protocol
        self._direction = MagicMock(name="OUTBOUND")
        self._state = MagicMock(name="OPEN")
        self._closed = False

    def get_protocol(self):
        return self.protocol_id

    @property
    def is_closed(self) -> bool:
        return self._closed


def _make_tracker() -> ConnectionStatsTracker:
    return ConnectionStatsTracker()


@pytest.mark.trio
async def test_opened_and_closed_stream_balance_counts():
    tracker = _make_tracker()
    stream = _FakeStream("peer1", protocol="/ipfs/ping/1.0.0", sid="10")

    await tracker.opened_stream(None, stream)

    assert tracker.streams  # one live record
    ps = tracker.peer_stream_stats["peer1"]
    assert ps.total_opened == 1
    assert ps.current_open == 1
    assert ps.max_concurrent_open == 1
    assert ps.by_protocol == {"/ipfs/ping/1.0.0": 1}

    await tracker.closed_stream(None, stream)

    assert not tracker.streams
    ps = tracker.peer_stream_stats["peer1"]
    assert ps.total_closed == 1
    assert ps.current_open == 0
    assert tracker.avg_stream_lifetime() is not None


@pytest.mark.trio
async def test_concurrent_open_tracks_peak():
    tracker = _make_tracker()
    s1 = _FakeStream("peer1", sid="1")
    s2 = _FakeStream("peer1", sid="2")
    s3 = _FakeStream("peer1", sid="3")

    await tracker.opened_stream(None, s1)
    await tracker.opened_stream(None, s2)
    await tracker.opened_stream(None, s3)

    ps = tracker.peer_stream_stats["peer1"]
    assert ps.current_open == 3
    assert ps.max_concurrent_open == 3

    await tracker.closed_stream(None, s1)
    await tracker.closed_stream(None, s2)
    assert ps.current_open == 1
    assert ps.max_concurrent_open == 3


@pytest.mark.trio
async def test_reset_detection():
    tracker = _make_tracker()
    stream = _FakeStream("peer1", sid="5")
    stream._state = SimpleNamespace(name="RESET")

    await tracker.opened_stream(None, stream)
    await tracker.closed_stream(None, stream)

    ps = tracker.peer_stream_stats["peer1"]
    assert ps.total_resets == 1
    assert ps.total_closed == 1


def _make_record(peer_id: str, sid: str, age: float, stream) -> dict:
    return {
        "key": f"sid:{sid}",
        "peer_id": peer_id,
        "opened_at": time.monotonic() - age,
        "stream_ref": stream,
        "protocol": "/ipfs/bitswap/1.2.0",
        "direction": "outbound",
        "stream_id": sid,
        "closed_at": None,
        "duration": None,
        "was_reset": False,
        "suspected_leak": False,
    }


def test_check_for_leaks_flags_old_streams():
    tracker = _make_tracker()
    stream = _FakeStream("peer1", protocol="/ipfs/bitswap/1.2.0", sid="99")
    stream._closed = False

    rec_type = type("Rec", (), _make_record("peer1", "99", 1000.0, stream))
    tracker.streams["sid:99"] = rec_type()

    leaked = tracker.check_for_leaks(threshold_seconds=300)

    assert len(leaked) == 1
    assert leaked[0].peer_id == "peer1"
    assert tracker.peer_stream_stats["peer1"].suspected_leaks == 1


def test_check_for_leaks_reconciles_closed_without_event():
    tracker = _make_tracker()
    stream = _FakeStream("peer1", sid="7")
    stream._closed = True  # closed underneath us, notifee never fired

    rec_type = type("Rec", (), _make_record("peer1", "7", 5.0, stream))
    tracker.streams["sid:7"] = rec_type()

    leaked = tracker.check_for_leaks(threshold_seconds=300)

    assert leaked == []
    assert not tracker.streams  # reconciled away
    assert tracker.peer_stream_stats["peer1"].total_closed == 1
    assert tracker.peer_stream_stats["peer1"].current_open == 0


def test_stream_stats_snapshot_shape():
    tracker = _make_tracker()
    stream = _FakeStream("peer1", protocol="/ipfs/ping/1.0.0", sid="3")

    import trio

    trio.run(tracker.opened_stream, None, stream)

    snap = tracker.stream_stats_snapshot()
    assert snap["CurrentOpenStreams"] == 1
    assert len(snap["OpenStreams"]) == 1
    assert snap["OpenStreams"][0]["peer_id"] == "peer1"
    assert snap["PerPeer"][0]["total_opened"] == 1


def test_reset_stream_stats():
    tracker = _make_tracker()
    stream = _FakeStream("peer1", sid="1")

    import trio

    trio.run(tracker.opened_stream, None, stream)
    tracker.reset_stream_stats()

    assert not tracker.streams
    assert not tracker.peer_stream_stats
