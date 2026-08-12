"""
Unit tests for stream lifecycle tracking and resource-leak detection.
"""

import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from py_ipfs_lite.connection_tracker import ConnectionStatsTracker


class _FakeMuxedConn:
    def __init__(self, peer_id: str, is_closed: bool = False) -> None:
        self.peer_id = SimpleNamespace(to_base58=lambda: peer_id)
        self.is_closed = is_closed


class _FakeConn:
    """Minimal stand-in for the INetConn passed to ``disconnected``."""

    def __init__(self, muxed_conn: _FakeMuxedConn) -> None:
        self.muxed_conn = muxed_conn


class _FakeStream:
    def __init__(
        self,
        peer_id: str,
        protocol: str | None = None,
        sid: str = "1",
        muxed_conn: _FakeMuxedConn | None = None,
        *,
        quic_muxed_stream: bool = False,
        swarm_conn: object | None = None,
    ) -> None:
        if muxed_conn is None:
            muxed_conn = _FakeMuxedConn(peer_id)
        self.muxed_conn = muxed_conn
        if quic_muxed_stream:
            # QUICStream-like: exposes is_closed() and a state machine
            self.muxed_stream = _FakeQUICStream(sid, muxed_conn)
        else:
            self.muxed_stream = SimpleNamespace(stream_id=sid, muxed_conn=muxed_conn)
        self.protocol_id = protocol
        self._direction = MagicMock(name="OUTBOUND")
        self._state = MagicMock(name="OPEN")
        self._closed = False
        if swarm_conn is not None:
            self.swarm_conn = swarm_conn

    def get_protocol(self):
        return self.protocol_id

    @property
    def is_closed(self) -> bool:
        return self._closed


class _FakeQUICStream:
    """QUICStream-like muxed stream with is_closed() and _state."""

    def __init__(self, sid: str, muxed_conn: _FakeMuxedConn) -> None:
        self.stream_id = sid
        self.muxed_conn = muxed_conn
        self._state = MagicMock(name="OPEN")
        self._closed = False

    def is_closed(self) -> bool:
        return self._closed


class _FakeNetStreamLike:
    """
    NetStream-shaped fake that does NOT expose ``is_closed``.

    The real ``NetStream`` historically had no ``is_closed`` attribute (the
    reconcile's ``getattr`` fell back to ``None``), which is exactly the
    production shape these tests must exercise.
    """

    def __init__(
        self,
        peer_id: str,
        sid: str = "1",
        muxed_conn: _FakeMuxedConn | None = None,
        *,
        quic_muxed_stream: bool = False,
        swarm_conn: object | None = None,
    ) -> None:
        if muxed_conn is None:
            muxed_conn = _FakeMuxedConn(peer_id)
        self.muxed_conn = muxed_conn
        if quic_muxed_stream:
            self.muxed_stream = _FakeQUICStream(sid, muxed_conn)
        else:
            self.muxed_stream = SimpleNamespace(stream_id=sid, muxed_conn=muxed_conn)
        self._state = MagicMock(name="OPEN")
        if swarm_conn is not None:
            self.swarm_conn = swarm_conn

    def get_protocol(self):
        return None


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


@pytest.mark.trio
async def test_same_stream_id_on_different_connections_do_not_collide():
    """
    Regression test for the key-collision bug: two live streams that share the
    same numeric stream id (but live on different connections — even to
    *different* peers) must both be tracked.  Before the fix both mapped to
    ``sid:{id}`` and the second silently overwrote the first, making the leak
    detector blind and the open counts wrong.
    """
    tracker = _make_tracker()
    conn_a = _FakeMuxedConn("peer1")
    conn_b = _FakeMuxedConn("peer1")
    s1 = _FakeStream("peer1", sid="3", muxed_conn=conn_a)
    s2 = _FakeStream("peer1", sid="3", muxed_conn=conn_b)
    # Different peers can also reuse the same stream id
    s3 = _FakeStream("peer2", sid="3", muxed_conn=_FakeMuxedConn("peer2"))

    await tracker.opened_stream(None, s1)
    await tracker.opened_stream(None, s2)
    await tracker.opened_stream(None, s3)

    assert len(tracker.streams) == 3
    ps = tracker.peer_stream_stats["peer1"]
    assert ps.current_open == 2
    assert ps.total_opened == 2
    assert tracker.peer_stream_stats["peer2"].current_open == 1

    # Closing one of the colliding-id streams must only affect its own record
    await tracker.closed_stream(None, s1)
    assert len(tracker.streams) == 2
    assert tracker.peer_stream_stats["peer1"].current_open == 1
    assert tracker.peer_stream_stats["peer1"].total_closed == 1

    await tracker.closed_stream(None, s2)
    await tracker.closed_stream(None, s3)
    assert tracker.peer_stream_stats["peer1"].current_open == 0
    assert tracker.peer_stream_stats["peer2"].current_open == 0
    assert tracker.peer_stream_stats["peer1"].total_closed == 2
    assert tracker.peer_stream_stats["peer2"].total_closed == 1


@pytest.mark.trio
async def test_fresh_stream_with_reused_key_is_still_counted():
    """
    Simulates the allocator reusing an object id: a brand-new stream whose
    key collides with an already-finalized key must still be finalized
    normally (its live record is popped first, so the dedup set is never
    consulted for it).
    """
    tracker = _make_tracker()
    s1 = _FakeStream("peer1", sid="1")
    s2 = _FakeStream("peer1", sid="1")

    original_key = tracker._stream_key

    def _fixed_key(stream) -> str:  # type: ignore[no-untyped-def]
        return "conn:1:stream:1"  # force a key collision across streams

    tracker._stream_key = _fixed_key  # type: ignore[method-assign]

    await tracker.opened_stream(None, s1)
    await tracker.closed_stream(None, s1)  # finalizes key "conn:1:stream:1"

    # New stream reuses the same key: it must still be counted exactly once
    await tracker.opened_stream(None, s2)
    await tracker.closed_stream(None, s2)

    tracker._stream_key = original_key  # type: ignore[method-assign]
    ps = tracker.peer_stream_stats["peer1"]
    assert ps.total_opened == 2
    assert ps.total_closed == 2
    assert ps.current_open == 0
    assert not tracker.streams


@pytest.mark.trio
async def test_duplicate_close_event_is_ignored():
    """
    libp2p can dispatch closed_stream more than once for the same stream
    (multiple close paths).  The tracker must count it exactly once.
    """
    tracker = _make_tracker()
    stream = _FakeStream("peer1", sid="7")

    await tracker.opened_stream(None, stream)
    await tracker.closed_stream(None, stream)
    await tracker.closed_stream(None, stream)  # duplicate

    ps = tracker.peer_stream_stats["peer1"]
    assert ps.total_opened == 1
    assert ps.total_closed == 1
    assert ps.current_open == 0


@pytest.mark.trio
async def test_closed_stream_refreshes_protocol_bucket():
    """
    Protocol negotiation happens after opened_stream fires; when the stream
    closes, the by_protocol bucket must move from \"unknown\" to the real
    protocol instead of staying unknown forever.
    """
    tracker = _make_tracker()
    stream = _FakeStream("peer1", protocol=None, sid="8")

    await tracker.opened_stream(None, stream)
    assert tracker.peer_stream_stats["peer1"].by_protocol == {"unknown": 1}

    # Negotiation completes before the stream closes
    stream.protocol_id = "/ipfs/ping/1.0.0"
    await tracker.closed_stream(None, stream)

    ps = tracker.peer_stream_stats["peer1"]
    assert ps.by_protocol == {"/ipfs/ping/1.0.0": 1}
    assert ps.total_closed == 1


@pytest.mark.trio
async def test_per_peer_avg_lifetime_is_populated():
    tracker = _make_tracker()
    stream = _FakeStream("peer1", sid="9")
    stream._state = SimpleNamespace(name="OPEN")

    await tracker.opened_stream(None, stream)
    # Force a measurable age so the closed duration is non-zero
    rec = next(iter(tracker.streams.values()))
    rec.opened_at = time.monotonic() - 2.0
    await tracker.closed_stream(None, stream)

    snap = tracker.stream_stats_snapshot()
    peer_dump = next(p for p in snap["PerPeer"] if p["peer_id"] == "peer1")
    assert peer_dump["avg_lifetime_seconds"] is not None
    assert peer_dump["avg_lifetime_seconds"] >= 2.0


def _make_record(peer_id: str, sid: str, age: float, stream) -> dict:
    return {
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

    rec = type("Rec", (), _make_record("peer1", "99", 1000.0, stream))()
    tracker.streams[tracker._stream_key(stream)] = rec

    leaked = tracker.check_for_leaks(threshold_seconds=300)

    assert len(leaked) == 1
    assert leaked[0].peer_id == "peer1"
    assert tracker.peer_stream_stats["peer1"].suspected_leaks == 1


def test_check_for_leaks_reconciles_closed_without_event():
    tracker = _make_tracker()
    stream = _FakeStream("peer1", sid="7")
    stream._closed = True  # closed underneath us, notifee never fired

    rec = type("Rec", (), _make_record("peer1", "7", 5.0, stream))()
    tracker.streams[tracker._stream_key(stream)] = rec

    leaked = tracker.check_for_leaks(threshold_seconds=300)

    assert leaked == []
    assert not tracker.streams  # reconciled away
    assert tracker.peer_stream_stats["peer1"].total_closed == 1
    assert tracker.peer_stream_stats["peer1"].current_open == 0
    # Reconciliation should also record lifetime analytics
    assert tracker.avg_stream_lifetime() is not None


def test_check_for_leaks_reconcile_refreshes_protocol_and_reset():
    tracker = _make_tracker()
    stream = _FakeStream("peer1", protocol=None, sid="11")
    stream._closed = True
    stream._state = SimpleNamespace(name="RESET")

    import trio

    trio.run(tracker.opened_stream, None, stream)  # registers peer stats

    # Simulate the stream aging and being replaced by its live record
    key = tracker._stream_key(stream)
    rec = type("Rec", (), _make_record("peer1", "11", 5.0, stream))()
    rec.protocol = None  # protocol not yet known when the stream was tracked
    tracker.streams[key] = rec

    # Negotiation completed underneath us before the stream died
    stream.protocol_id = "/ipfs/bitswap/1.2.0"

    tracker.check_for_leaks(threshold_seconds=300)

    ps = tracker.peer_stream_stats["peer1"]
    assert ps.total_opened == 1
    assert ps.total_closed == 1
    assert ps.current_open == 0
    assert ps.total_resets == 1
    # Protocol was negotiated before close: bucket reflects the real protocol
    assert ps.by_protocol == {"/ipfs/bitswap/1.2.0": 1}


def test_lazy_protocol_refresh_rebuckets_stats():
    """Protocol negotiated after open should move the by_protocol bucket."""
    tracker = _make_tracker()
    stream = _FakeStream("peer1", protocol=None, sid="8")

    import trio

    trio.run(tracker.opened_stream, None, stream)

    ps = tracker.peer_stream_stats["peer1"]
    assert ps.by_protocol == {"unknown": 1}

    # Negotiation completes: protocol becomes known on the live object
    stream.protocol_id = "/ipfs/ping/1.0.0"

    record = tracker.streams[tracker._stream_key(stream)]
    tracker._refresh_record_metadata(record)

    assert record.protocol == "/ipfs/ping/1.0.0"
    assert ps.by_protocol == {"/ipfs/ping/1.0.0": 1}


def test_stream_stats_snapshot_shape():
    tracker = _make_tracker()
    stream = _FakeStream("peer1", protocol="/ipfs/ping/1.0.0", sid="3")

    import trio

    trio.run(tracker.opened_stream, None, stream)

    snap = tracker.stream_stats_snapshot(leak_threshold_seconds=300.0)
    assert snap["CurrentOpenStreams"] == 1
    assert len(snap["OpenStreams"]) == 1
    assert snap["OpenStreams"][0]["peer_id"] == "peer1"
    assert snap["PerPeer"][0]["total_opened"] == 1
    # The configured threshold must be reported, not hardcoded
    assert snap["LeakThresholdConfigured"] is True
    assert snap["LeakThresholdSeconds"] == 300.0


def test_stream_stats_snapshot_reports_no_threshold_when_unset():
    tracker = _make_tracker()
    snap = tracker.stream_stats_snapshot(leak_threshold_seconds=None)
    assert snap["LeakThresholdConfigured"] is False
    assert snap["LeakThresholdSeconds"] is None


def test_reset_stream_stats():
    tracker = _make_tracker()
    stream = _FakeStream("peer1", sid="1")

    import trio

    trio.run(tracker.opened_stream, None, stream)
    tracker.reset_stream_stats()

    assert not tracker.streams
    assert not tracker.peer_stream_stats


def test_check_for_leaks_reconciles_stream_with_closed_connection():
    """
    A connection that dies without dispatching per-stream close events must
    not leave phantom leak records: the sweep reconciles any record whose
    muxed connection reports itself closed.
    """
    tracker = _make_tracker()
    stream = _FakeNetStreamLike(
        "peer1", muxed_conn=_FakeMuxedConn("peer1", is_closed=True)
    )

    rec = type("Rec", (), _make_record("peer1", "13", 5.0, stream))()
    tracker.streams[tracker._stream_key(stream)] = rec

    leaked = tracker.check_for_leaks(threshold_seconds=300)

    assert leaked == []
    assert not tracker.streams  # reconciled away
    assert tracker.peer_stream_stats["peer1"].total_closed == 1
    assert tracker.peer_stream_stats["peer1"].current_open == 0


def test_check_for_leaks_reconciles_stream_with_closed_muxed_stream():
    """
    QUIC-style streams whose muxed-stream wrapper was closed at the transport
    level (but never notified upward) must be reconciled as closed.
    """
    tracker = _make_tracker()
    stream = _FakeNetStreamLike("peer1", quic_muxed_stream=True)
    stream.muxed_stream._closed = True  # QUICStream.is_closed() -> True

    rec = type("Rec", (), _make_record("peer1", "14", 5.0, stream))()
    tracker.streams[tracker._stream_key(stream)] = rec

    leaked = tracker.check_for_leaks(threshold_seconds=300)

    assert leaked == []
    assert not tracker.streams
    assert tracker.peer_stream_stats["peer1"].total_closed == 1


def test_check_for_leaks_keeps_open_stream_on_open_connection():
    """Open stream on a live connection must still be flagged as a leak."""
    tracker = _make_tracker()
    stream = _FakeStream(
        "peer1", sid="15", muxed_conn=_FakeMuxedConn("peer1", is_closed=False)
    )

    rec = type("Rec", (), _make_record("peer1", "15", 1000.0, stream))()
    tracker.streams[tracker._stream_key(stream)] = rec

    leaked = tracker.check_for_leaks(threshold_seconds=300)

    assert len(leaked) == 1
    assert tracker.streams  # record still live
    assert tracker.peer_stream_stats["peer1"].suspected_leaks == 1


@pytest.mark.trio
async def test_disconnected_prunes_streams_on_that_connection_only():
    """
    When a connection disconnects, its open stream records must be finalized
    (no closed_stream events will follow), while streams on other connections
    — even to the same peer — must survive.
    """
    tracker = _make_tracker()
    conn_a = _FakeMuxedConn("peer1")
    conn_b = _FakeMuxedConn("peer1")
    s_on_a = _FakeStream("peer1", sid="1", muxed_conn=conn_a)
    s_on_b = _FakeStream("peer1", sid="2", muxed_conn=conn_b)

    await tracker.opened_stream(None, s_on_a)
    await tracker.opened_stream(None, s_on_b)
    assert len(tracker.streams) == 2

    await tracker.disconnected(None, _FakeConn(conn_a))

    assert len(tracker.streams) == 1
    assert tracker.peer_stream_stats["peer1"].current_open == 1
    assert tracker.peer_stream_stats["peer1"].total_closed == 1
    # The surviving record belongs to connection B
    assert next(iter(tracker.streams.values())).stream_id == "2"

    await tracker.disconnected(None, _FakeConn(conn_b))
    assert not tracker.streams
    assert tracker.peer_stream_stats["peer1"].total_closed == 2
    assert tracker.peer_stream_stats["peer1"].current_open == 0


@pytest.mark.trio
async def test_disconnected_matches_by_swarm_conn_identity():
    """
    Records must also be pruned when the disconnected notifee passes the same
    SwarmConn object the stream references (identity match, not just
    muxed_conn id).
    """
    tracker = _make_tracker()
    conn_a = _FakeMuxedConn("peer1")
    fake_conn = _FakeConn(conn_a)
    s = _FakeStream("peer1", sid="3", muxed_conn=conn_a, swarm_conn=fake_conn)

    await tracker.opened_stream(None, s)
    assert len(tracker.streams) == 1

    # Same SwarmConn identity -> pruned (muxed-conn id also matches here, but
    # the identity check is what drives it)
    await tracker.disconnected(None, fake_conn)
    assert not tracker.streams
    assert tracker.peer_stream_stats["peer1"].total_closed == 1
