import logging
import time
import weakref
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from libp2p.abc import INetConn, INetStream, INetwork, INotifee
from multiaddr import Multiaddr
from pydantic import BaseModel

from py_ipfs_lite.metrics import (
    IPFS_STREAMS_CLOSED_TOTAL,
    IPFS_STREAMS_INBOUND_TOTAL,
    IPFS_STREAMS_LEAKED_TOTAL,
    IPFS_STREAMS_OPENED_TOTAL,
    IPFS_STREAMS_OUTBOUND_TOTAL,
)

logger = logging.getLogger("py_ipfs_lite.connection_tracker")


class PeerConnectionStats(BaseModel):
    peer_id: str
    total_connections: int = 0
    current_connections: int = 0
    first_connected_at: str | None = None
    last_connected_at: str | None = None
    last_disconnected_at: str | None = None
    security: str | None = None
    muxer: str | None = None
    transport: str | None = None
    identify_completed: bool = False
    identify_completed_at: str | None = None
    ping_completed: bool = False
    first_ping_at: str | None = None
    last_ping_at: str | None = None


class PeerStreamStats(BaseModel):
    """Aggregated stream lifecycle statistics for a single peer."""

    peer_id: str
    total_opened: int = 0
    total_opened_outbound: int = 0
    total_opened_inbound: int = 0
    total_closed: int = 0
    current_open: int = 0
    current_open_outbound: int = 0
    current_open_inbound: int = 0
    max_concurrent_open: int = 0
    total_resets: int = 0
    suspected_leaks: int = 0
    avg_lifetime_seconds: float | None = None
    by_protocol: dict[str, int] = {}


@dataclass
class StreamRecord:
    """Live record of a single network stream, keyed by its object id."""

    key: str
    peer_id: str
    opened_at: float
    _stream_ref: Any
    protocol: str | None = None
    direction: str = "unknown"
    stream_id: str | None = None
    closed_at: float | None = None
    duration: float | None = None
    was_reset: bool = False
    suspected_leak: bool = False
    counted_in_proto_total: bool = False

    @property
    def stream_ref(self) -> Any:
        if callable(self._stream_ref):
            return self._stream_ref()
        return self._stream_ref


def _extract_peer_id(conn: INetConn) -> str | None:
    """Extract a base58 peer id from a connection, handling QUIC layouts."""
    try:
        return conn.muxed_conn.peer_id.to_base58()
    except AttributeError:
        try:
            peer_id = str(getattr(conn, "peer_id", "unknown"))
            return None if peer_id == "unknown" else peer_id
        except Exception:
            return None


def _stream_peer_id(stream: INetStream) -> str | None:
    """Extract the base58 peer id from a network stream."""
    try:
        muxed_conn = getattr(stream, "muxed_conn", None)
        if muxed_conn is None:
            return None
        pid = getattr(muxed_conn, "peer_id", None)
        if pid is None:
            return None
        if hasattr(pid, "to_base58"):
            return pid.to_base58()
        return str(pid)
    except Exception:
        return None


def _stream_protocol(stream: INetStream) -> str | None:
    try:
        proto = stream.get_protocol()
        return str(proto) if proto is not None else None
    except Exception:
        return None


def _stream_direction(stream: Any) -> str:
    if stream is None:
        return "unknown"
    try:
        # 1. Direct _direction attribute on stream (NetStream._direction)
        direction = getattr(stream, "_direction", None)
        if direction is not None:
            name = getattr(direction, "name", None)
            if name and name.lower() in ("inbound", "outbound"):
                return name.lower()
            s = str(direction).lower()
            if "inbound" in s:
                return "inbound"
            if "outbound" in s:
                return "outbound"

        # 2. Check underlying muxed_stream (e.g. QUICStream._direction)
        muxed = getattr(stream, "muxed_stream", None)
        if muxed is not None:
            m_dir = getattr(muxed, "_direction", None)
            if m_dir is not None:
                name = getattr(m_dir, "name", None)
                if name and name.lower() in ("inbound", "outbound"):
                    return name.lower()
                s = str(m_dir).lower()
                if "inbound" in s:
                    return "inbound"
                if "outbound" in s:
                    return "outbound"
            if hasattr(muxed, "is_outbound"):
                is_out = muxed.is_outbound() if callable(muxed.is_outbound) else muxed.is_outbound
                return "outbound" if is_out else "inbound"
            if hasattr(muxed, "is_initiator"):
                is_init = muxed.is_initiator() if callable(muxed.is_initiator) else muxed.is_initiator
                return "outbound" if is_init else "inbound"
    except Exception:
        pass
    return "unknown"


def _stream_id(stream: INetStream) -> str | None:
    try:
        muxed = getattr(stream, "muxed_stream", None)
        sid = getattr(muxed, "stream_id", None)
        return str(sid) if sid is not None else None
    except Exception:
        return None


def _extract_conn_details(conn: INetConn) -> dict[str, Any]:
    """Extract multiaddr, direction, transport, security, and muxer."""
    transport = "unknown"
    security = "unknown"
    muxer = "unknown"
    direction = "unknown"
    remote_addr = None

    try:
        dir_val = getattr(conn, "direction", getattr(conn, "_direction", None))
        if dir_val is not None:
            if hasattr(dir_val, "name"):
                direction = dir_val.name.lower()
            else:
                direction = str(dir_val).lower()
    except Exception:
        pass

    try:
        actual_addrs = getattr(conn, "_actual_transport_addresses", None)
        if actual_addrs and len(actual_addrs) > 0:
            remote_addr = str(actual_addrs[0])
        if remote_addr is None:
            maddr = getattr(conn, "remote_addr", getattr(conn, "multiaddr", None))
            if maddr is None:
                raw_c = getattr(getattr(conn, "muxed_conn", None), "_raw_conn", None)
                maddr = getattr(raw_c, "multiaddr", None)
            if maddr is not None:
                remote_addr = str(maddr)
    except Exception:
        pass

    try:
        muxed_conn = getattr(conn, "muxed_conn", None)
        if muxed_conn is not None:
            muxer_name = type(muxed_conn).__name__
            if "QUIC" in muxer_name:
                transport = "quic-v1"
                security = "tls1.3"
                muxer = "quic"
            elif "Yamux" in muxer_name:
                muxer = "yamux"
                if transport == "unknown":
                    transport = "tcp"
            elif "Mplex" in muxer_name:
                muxer = "mplex"
                if transport == "unknown":
                    transport = "tcp"

            sec_conn = getattr(muxed_conn, "secured_conn", None)
            if sec_conn is not None:
                sec_name = type(sec_conn).__name__
                if "Noise" in sec_name:
                    security = "Noise"
                elif "TLS" in sec_name:
                    security = "tls"

        raw_conn = getattr(muxed_conn, "_raw_conn", getattr(conn, "_raw_conn", None))
        if raw_conn is not None and transport == "unknown":
            t_name = type(raw_conn).__name__
            if "TCP" in t_name or "Socket" in t_name:
                transport = "tcp"
            elif "WebSocket" in t_name:
                transport = "websocket"
            elif "QUIC" in t_name:
                transport = "quic-v1"
    except Exception:
        pass

    # Infer from multiaddr string if still unknown
    if remote_addr is not None:
        if "/quic-v1" in remote_addr or "/quic" in remote_addr:
            transport = "quic-v1"
            if security == "unknown":
                security = "tls1.3"
            if muxer == "unknown":
                muxer = "quic"
        elif "/ws" in remote_addr or "/wss" in remote_addr:
            transport = "websocket"
        elif "/tcp/" in remote_addr and transport == "unknown":
            transport = "tcp"

    if muxer in ("yamux", "mplex") and security == "unknown":
        security = "Noise"

    return {
        "direction": direction,
        "transport": transport,
        "security": security,
        "muxer": muxer,
        "remote_addr": remote_addr,
    }


def _extract_transport_info(
    conn: INetConn,
) -> tuple[str | None, str | None, str | None]:
    """Extract (transport, security, muxer) from connection."""
    details = _extract_conn_details(conn)
    return (
        details["transport"] if details["transport"] != "unknown" else None,
        details["security"] if details["security"] != "unknown" else None,
        details["muxer"] if details["muxer"] != "unknown" else None,
    )


class ConnectionStatsTracker(INotifee):
    def __init__(self) -> None:
        # The swarm network last seen in a notifee callback.  Its live
        # ``connections`` table is the authoritative source of truth for
        # whether a connection (and therefore its streams) is still alive.
        self._network: INetwork | None = None
        self.stats: dict[str, PeerConnectionStats] = {}
        self.streams: dict[str, StreamRecord] = {}
        self.peer_stream_stats: dict[str, PeerStreamStats] = {}
        # Per-peer lifetime analytics: peer_id -> (sum of durations, count).
        # Bounded by the number of streams, unlike a global ring buffer.
        self._peer_lifetime: dict[str, tuple[float, int]] = {}

        # Connection lifecycle counters and metrics
        self.total_connected_events: int = 0
        self.total_disconnected_events: int = 0
        self.total_outbound_opened: int = 0
        self.total_inbound_opened: int = 0
        self.total_outbound_closed: int = 0
        self.total_inbound_closed: int = 0
        self.total_streams_by_protocol: dict[str, int] = {}
        self.total_streams_by_protocol_outbound: dict[str, int] = {}
        self.total_streams_by_protocol_inbound: dict[str, int] = {}
        self._conn_meta: dict[int, dict[str, Any]] = {}
        self.recent_disconnections: deque[dict[str, Any]] = deque(maxlen=500)

        # Keys of streams that were already finalized (closed).  Guards against
        # double-counting when libp2p dispatches a second closed_stream event
        # (or a close event races with the leak-monitor reconciliation).
        # Keys are never reused (connection-scoped + stream-object identity), so
        # a simple bounded FIFO is safe.
        self._finalized_keys: dict[str, float] = {}
        self._max_finalized_keys = 20_000
        # Entries older than this are dropped from the dedup set.  Keys embed
        # Python object ids which *can* be reused by the allocator after the
        # underlying objects are garbage collected, so the dedup window must
        # be bounded in time as well as in size.
        self._finalized_ttl_seconds = 3_600.0

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    def _stream_key(self, stream: INetStream) -> str:
        """
        Return a unique, stable key for a live stream.

        Stream IDs are only unique *per muxed connection*, so a bare
        ``sid:{stream_id}`` key collides across connections to the same peer
        (and even across different peers): a new stream silently overwrites the
        live record of an unrelated open stream, corrupting open counts and
        blinding the leak detector.  Scope the key by the identity of the
        underlying muxed connection and muxed stream object, which are stable
        for the stream's whole lifetime (unlike ``stream_id``, which can start
        as ``0`` on inbound streams and only be assigned later).
        """
        try:
            muxed_stream = getattr(stream, "muxed_stream", None)
            if muxed_stream is None:
                return f"obj:{id(stream)}"
            muxed_conn = getattr(muxed_stream, "muxed_conn", None)
            if muxed_conn is None:
                muxed_conn = getattr(stream, "muxed_conn", None)
            conn_id = id(muxed_conn) if muxed_conn is not None else 0
            return f"conn:{conn_id}:stream:{id(muxed_stream)}"
        except Exception:
            return f"obj:{id(stream)}"

    def _mark_finalized(self, key: str) -> None:
        """Remember a finalized stream key so a duplicate close is ignored."""
        now = time.monotonic()
        self._finalized_keys[key] = now
        # Drop expired entries (dict preserves insertion order, which matches
        # time order), then the oldest entry if still over capacity.
        expired: list[str] = []
        for k, ts in self._finalized_keys.items():
            if now - ts > self._finalized_ttl_seconds:
                expired.append(k)
            else:
                break
        for k in expired:
            self._finalized_keys.pop(k, None)
        if len(self._finalized_keys) > self._max_finalized_keys:
            self._finalized_keys.pop(next(iter(self._finalized_keys)), None)

    def _record_lifetime(self, peer_id: str, duration: float) -> None:
        """Accumulate a closed stream's lifetime into the peer's aggregate."""
        total, count = self._peer_lifetime.get(peer_id, (0.0, 0))
        self._peer_lifetime[peer_id] = (total + duration, count + 1)

    # ------------------------------------------------------------------
    # Connection lifecycle (INotifee implementation)
    # ------------------------------------------------------------------

    async def connected(self, network: INetwork, conn: INetConn) -> None:
        self._network = network
        peer_id = _extract_peer_id(conn) or "unknown"
        now_mono = time.monotonic()
        now_ts = self._now()

        conn_meta = _extract_conn_details(conn)
        conn_meta["peer_id"] = peer_id
        conn_meta["start_mono"] = now_mono
        conn_meta["connected_at"] = now_ts
        conn_meta["streams_served"] = 0
        conn_meta["protocols"] = set()

        self._conn_meta[id(conn)] = conn_meta
        muxed_conn = getattr(conn, "muxed_conn", None)
        if muxed_conn is not None:
            self._conn_meta[id(muxed_conn)] = conn_meta

        self.total_connected_events += 1

        stats = self.stats.setdefault(peer_id, PeerConnectionStats(peer_id=peer_id))
        stats.total_connections += 1
        stats.current_connections += 1
        if stats.first_connected_at is None:
            stats.first_connected_at = now_ts
        stats.last_connected_at = now_ts

        if conn_meta["transport"] != "unknown":
            stats.transport = conn_meta["transport"]
        if conn_meta["security"] != "unknown":
            stats.security = conn_meta["security"]
        if conn_meta["muxer"] != "unknown":
            stats.muxer = conn_meta["muxer"]

        try:
            from py_ipfs_lite.metrics import IPFS_SWARM_CONNECTS_TOTAL

            IPFS_SWARM_CONNECTS_TOTAL.labels(
                transport=conn_meta.get("transport", "unknown")
            ).inc()
        except Exception:
            pass

        logger.debug(
            "notifee: peer connected: %s (conns: %d, total: %d, dir: %s, "
            "transport: %s)",
            peer_id,
            stats.current_connections,
            self.total_connected_events,
            conn_meta["direction"],
            conn_meta["transport"],
        )

    async def disconnected(self, network: INetwork, conn: INetConn) -> None:
        self._network = network
        peer_id = _extract_peer_id(conn) or "unknown"
        now_mono = time.monotonic()
        now_ts = self._now()

        conn_meta = self._conn_meta.pop(id(conn), None)
        muxed_conn = getattr(conn, "muxed_conn", None)
        if muxed_conn is not None:
            self._conn_meta.pop(id(muxed_conn), None)

        start_mono = conn_meta.get("start_mono") if conn_meta else None
        duration = (now_mono - start_mono) if start_mono is not None else None

        self.total_disconnected_events += 1

        stats = self.stats.get(peer_id)
        if stats:
            stats.current_connections = max(0, stats.current_connections - 1)
            stats.last_disconnected_at = now_ts

        # Determine reasonable root-cause classification hint
        reason_hint = "remote_closed_or_idle"
        protos = list(conn_meta.get("protocols", [])) if conn_meta else []
        streams_served = conn_meta.get("streams_served", 0) if conn_meta else 0

        if duration is not None:
            if duration < 5.0 and streams_served == 0:
                reason_hint = "handshake_failed_or_dial_cancelled"
            elif any("kad" in p for p in protos) and duration < 45.0:
                reason_hint = "dht_lookup_hop_completed"
            elif duration >= 590.0:
                reason_hint = "idle_timeout"

        event_record = {
            "peer_id": peer_id,
            "connected_at": conn_meta.get("connected_at") if conn_meta else None,
            "disconnected_at": now_ts,
            "duration_seconds": round(duration, 3) if duration is not None else None,
            "direction": conn_meta.get("direction") if conn_meta else "unknown",
            "transport": (
                conn_meta.get("transport")
                if conn_meta
                else (stats.transport if stats else None)
            ),
            "security": (
                conn_meta.get("security")
                if conn_meta
                else (stats.security if stats else None)
            ),
            "muxer": (
                conn_meta.get("muxer")
                if conn_meta
                else (stats.muxer if stats else None)
            ),
            "remote_addr": conn_meta.get("remote_addr") if conn_meta else None,
            "streams_served": streams_served,
            "protocols": sorted(protos),
            "reason_hint": reason_hint,
        }
        self.recent_disconnections.append(event_record)

        # Prune stream records on the disconnecting connection.
        conn_muxed_id = id(muxed_conn) if muxed_conn is not None else 0
        for key, record in list(self.streams.items()):
            if self._record_on_conn(record, conn, conn_muxed_id):
                self._finalize_record(key, record, now_mono)

        try:
            from py_ipfs_lite.metrics import (
                IPFS_SWARM_DISCONNECT_REASONS_TOTAL,
                IPFS_SWARM_DISCONNECTS_TOTAL,
            )

            t_val = (
                conn_meta.get("transport")
                if conn_meta
                else (stats.transport if stats else "unknown")
            ) or "unknown"
            IPFS_SWARM_DISCONNECTS_TOTAL.labels(transport=t_val).inc()
            IPFS_SWARM_DISCONNECT_REASONS_TOTAL.labels(reason_hint=reason_hint).inc()
        except Exception:
            pass

        # Enforce memory cap on stats and stream histories
        if len(self.stats) > 2000:
            self._enforce_stats_capacity()

        logger.debug(
            "notifee: peer disconnected: %s (duration: %s, hint: %s, disconns: %d)",
            peer_id,
            f"{duration:.2f}s" if duration is not None else "unknown",
            reason_hint,
            self.total_disconnected_events,
        )

    def _enforce_stats_capacity(self) -> None:
        """Evict oldest disconnected peer stats to keep memory bounded."""
        if len(self.stats) <= 2000:
            return
        to_remove = []
        for pid, s in self.stats.items():
            if s.current_connections == 0:
                to_remove.append(pid)
                if len(self.stats) - len(to_remove) <= 1500:
                    break
        for pid in to_remove:
            self.stats.pop(pid, None)
            self.peer_streams.pop(pid, None)
            self._peer_lifetime.pop(pid, None)

        if len(self._conn_meta) > 2000:
            # Prune orphan conn_meta entries if any
            now_mono = time.monotonic()
            stale_keys = [
                k
                for k, v in self._conn_meta.items()
                if now_mono - v.get("start_mono", now_mono) > 3600.0
            ]
            for k in stale_keys[:500]:
                self._conn_meta.pop(k, None)

    async def listen(self, network: INetwork, multiaddr: Multiaddr) -> None:
        self._network = network

    async def listen_close(self, network: INetwork, multiaddr: Multiaddr) -> None:
        self._network = network

    def connection_stats_snapshot(self) -> dict[str, Any]:
        """Snapshot of connection lifecycle metrics for debugging."""
        active_conns = 0
        active_by_transport: dict[str, int] = {}
        active_by_direction: dict[str, int] = {}

        if self._network is not None:
            try:
                conns_map = getattr(self._network, "connections", {})
                for c_list in conns_map.values():
                    if isinstance(c_list, list):
                        for c in c_list:
                            active_conns += 1
                            t_info = _extract_conn_details(c)
                            t = t_info["transport"]
                            d = t_info["direction"]
                            active_by_transport[t] = active_by_transport.get(t, 0) + 1
                            active_by_direction[d] = active_by_direction.get(d, 0) + 1
                    else:
                        active_conns += 1
            except Exception:
                pass

        recent = list(self.recent_disconnections)
        durations = [
            item["duration_seconds"]
            for item in recent
            if item.get("duration_seconds") is not None
        ]
        avg_dur = (sum(durations) / len(durations)) if durations else 0.0

        # Lifespan distribution buckets for disconnected sessions
        lifespan_dist = {
            "under_5s (failed/transient)": sum(1 for d in durations if d < 5.0),
            "5s_to_35s (DHT query hops)": sum(1 for d in durations if 5.0 <= d < 35.0),
            "35s_to_2m (short sessions)": (
                sum(1 for d in durations if 35.0 <= d < 120.0)
            ),
            "2m_to_5m (moderate sessions)": (
                sum(1 for d in durations if 120.0 <= d < 300.0)
            ),
            "5m_to_10m (extended sessions)": (
                sum(1 for d in durations if 300.0 <= d < 600.0)
            ),
            "10m_to_30m (long sessions)": (
                sum(1 for d in durations if 600.0 <= d < 1800.0)
            ),
            "over_30m (stable peers)": sum(1 for d in durations if d >= 1800.0),
        }

        # Lifespan distribution of currently active live connections
        now_mono = time.monotonic()
        active_durations = [
            (now_mono - meta["start_mono"])
            for meta in self._conn_meta.values()
            if "start_mono" in meta and meta.get("peer_id") != "unknown"
        ]
        # Dedup if both conn and muxed_conn are in _conn_meta
        active_durations_dedup = list({round(d, 2) for d in active_durations})

        active_lifespan_dist = {
            "under_2m": sum(1 for d in active_durations_dedup if d < 120.0),
            "2m_to_5m": sum(1 for d in active_durations_dedup if 120.0 <= d < 300.0),
            "5m_to_10m": sum(1 for d in active_durations_dedup if 300.0 <= d < 600.0),
            "10m_to_30m": (
                sum(1 for d in active_durations_dedup if 600.0 <= d < 1800.0)
            ),
            "over_30m (long-lived stable)": (
                sum(1 for d in active_durations_dedup if d >= 1800.0)
            ),
        }

        # Reason hints distribution
        reasons_dist: dict[str, int] = {}
        for r in recent:
            hint = r.get("reason_hint", "unknown")
            reasons_dist[hint] = reasons_dist.get(hint, 0) + 1

        # Protocol usage summary
        proto_counts: dict[str, int] = {}
        for r in recent:
            for p in r.get("protocols", []):
                proto_counts[p] = proto_counts.get(p, 0) + 1

        return {
            "total_connected_events": self.total_connected_events,
            "total_disconnected_events": self.total_disconnected_events,
            "current_active_connections": active_conns,
            "total_peers_tracked": len(self.stats),
            "avg_connection_lifespan_seconds": round(avg_dur, 2),
            "active_connections_breakdown": {
                "by_transport": active_by_transport,
                "by_direction": active_by_direction,
                "by_current_age": active_lifespan_dist,
            },
            "disconnections_lifespan_distribution": lifespan_dist,
            "disconnections_reason_breakdown": reasons_dist,
            "recent_disconnections_count": len(recent),
            "top_protocols_on_disconnected": dict(
                sorted(proto_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            ),
            "recent_disconnections": recent[-25:],
        }

    # ------------------------------------------------------------------
    # Stream lifecycle
    # ------------------------------------------------------------------

    async def opened_stream(self, network: INetwork, stream: INetStream) -> None:
        self._network = network
        key = self._stream_key(stream)
        peer_id = _stream_peer_id(stream) or "unknown"

        stream_ref_val: Any
        try:
            stream_ref_val = weakref.ref(stream)
        except Exception:
            stream_ref_val = stream

        record = StreamRecord(
            key=key,
            peer_id=peer_id,
            opened_at=time.monotonic(),
            _stream_ref=stream_ref_val,
            protocol=_stream_protocol(stream),
            direction=_stream_direction(stream),
            stream_id=_stream_id(stream),
        )
        self.streams[key] = record

        peer_stats = self.peer_stream_stats.setdefault(
            peer_id, PeerStreamStats(peer_id=peer_id)
        )
        peer_stats.total_opened += 1
        peer_stats.current_open += 1
        peer_stats.max_concurrent_open = max(
            peer_stats.max_concurrent_open, peer_stats.current_open
        )
        if record.direction == "outbound":
            self.total_outbound_opened += 1
            peer_stats.total_opened_outbound += 1
            peer_stats.current_open_outbound += 1
            IPFS_STREAMS_OUTBOUND_TOTAL.inc()
        elif record.direction == "inbound":
            self.total_inbound_opened += 1
            peer_stats.total_opened_inbound += 1
            peer_stats.current_open_inbound += 1
            IPFS_STREAMS_INBOUND_TOTAL.inc()

        proto = record.protocol or "unknown"
        peer_stats.by_protocol[proto] = peer_stats.by_protocol.get(proto, 0) + 1

        IPFS_STREAMS_OPENED_TOTAL.inc()

        # Link stream activity to parent connection metadata
        muxed_conn = getattr(stream, "muxed_conn", None)
        if muxed_conn is not None and id(muxed_conn) in self._conn_meta:
            conn_meta = self._conn_meta[id(muxed_conn)]
            conn_meta["streams_served"] += 1
            if record.protocol:
                conn_meta["protocols"].add(record.protocol)

        logger.debug(
            f"stream opened peer={peer_id} proto={record.protocol} "
            f"dir={record.direction} open_now={peer_stats.current_open}"
        )

    async def closed_stream(self, network: INetwork, stream: INetStream) -> None:
        self._network = network
        key = self._stream_key(stream)
        record = self.streams.pop(key, None)
        if record is None:
            if key in self._finalized_keys:
                # Duplicate close event (e.g. libp2p notifying from two close
                # paths) or a race with the leak-monitor reconciliation: the
                # stream was already counted.  Only consult the dedup set when
                # there is no live record — a fresh stream that (rarely) reused
                # an allocator object id must still be finalized normally.
                return
            # Stream opened before this tracker was registered, or key drift.
            # Finalize defensively so per-peer counts stay balanced.
            record = StreamRecord(
                key=key,
                peer_id=_stream_peer_id(stream) or "unknown",
                opened_at=0.0,
                _stream_ref=None,
            )
        else:
            # Protocol/direction are negotiated *after* the opened_stream event
            # fires; refresh now so the by_protocol bucket reflects the real
            # protocol instead of staying "unknown" forever.
            self._refresh_record_metadata(record)

        now = time.monotonic()
        record.closed_at = now
        if record.opened_at > 0:
            record.duration = now - record.opened_at
        else:
            record.duration = None

        # Detect resets from the stream state machine
        state = getattr(stream, "_state", None)
        if state is not None and getattr(state, "name", "") == "RESET":
            record.was_reset = True

        peer_stats = self.peer_stream_stats.setdefault(
            record.peer_id, PeerStreamStats(peer_id=record.peer_id)
        )
        peer_stats.total_closed += 1
        peer_stats.current_open = max(0, peer_stats.current_open - 1)
        if record.direction == "outbound":
            self.total_outbound_closed += 1
            peer_stats.current_open_outbound = max(0, peer_stats.current_open_outbound - 1)
        elif record.direction == "inbound":
            self.total_inbound_closed += 1
            peer_stats.current_open_inbound = max(0, peer_stats.current_open_inbound - 1)

        if record.was_reset:
            peer_stats.total_resets += 1

        if record.duration is not None:
            self._record_lifetime(record.peer_id, record.duration)

        self._mark_finalized(key)
        IPFS_STREAMS_CLOSED_TOTAL.inc()
        logger.debug(
            f"stream closed peer={record.peer_id} proto={record.protocol} "
            f"dir={record.direction} duration={record.duration:.2f}s reset={record.was_reset} "
            f"open_now={peer_stats.current_open}"
        )

    def _refresh_record_metadata(self, record: StreamRecord) -> None:
        """
        Lazily fill in protocol/direction after stream negotiation, and move
        the peer's by_protocol bucket if the protocol became known.
        """
        old_protocol = record.protocol
        old_direction = record.direction
        if record.protocol is None:
            record.protocol = _stream_protocol(record.stream_ref)
        if record.direction == "unknown":
            record.direction = _stream_direction(record.stream_ref)
            if old_direction != record.direction and record.direction in ("outbound", "inbound"):
                peer_stats = self.peer_stream_stats.get(record.peer_id)
                if peer_stats is not None:
                    if record.direction == "outbound":
                        self.total_outbound_opened += 1
                        peer_stats.total_opened_outbound += 1
                        peer_stats.current_open_outbound += 1
                        IPFS_STREAMS_OUTBOUND_TOTAL.inc()
                    elif record.direction == "inbound":
                        self.total_inbound_opened += 1
                        peer_stats.total_opened_inbound += 1
                        peer_stats.current_open_inbound += 1
                        IPFS_STREAMS_INBOUND_TOTAL.inc()

        if old_protocol != record.protocol and record.protocol is not None:
            peer_stats = self.peer_stream_stats.get(record.peer_id)
            if peer_stats is not None:
                old_bucket = old_protocol or "unknown"
                if peer_stats.by_protocol.get(old_bucket, 0) > 0:
                    peer_stats.by_protocol[old_bucket] -= 1
                if peer_stats.by_protocol.get(old_bucket, 0) == 0:
                    peer_stats.by_protocol.pop(old_bucket, None)
                new_bucket = record.protocol
                peer_stats.by_protocol[new_bucket] = (
                    peer_stats.by_protocol.get(new_bucket, 0) + 1
                )

        if record.protocol and record.protocol != "unknown" and not getattr(record, "counted_in_proto_total", False):
            proto = record.protocol
            self.total_streams_by_protocol[proto] = (
                self.total_streams_by_protocol.get(proto, 0) + 1
            )
            if record.direction == "outbound":
                self.total_streams_by_protocol_outbound[proto] = (
                    self.total_streams_by_protocol_outbound.get(proto, 0) + 1
                )
            elif record.direction == "inbound":
                self.total_streams_by_protocol_inbound[proto] = (
                    self.total_streams_by_protocol_inbound.get(proto, 0) + 1
                )
            setattr(record, "counted_in_proto_total", True)

    # ------------------------------------------------------------------
    # Leak detection
    # ------------------------------------------------------------------

    def _live_conn_ids(self) -> tuple[set[int], set[str]]:
        """
        Snapshot of the swarm's live connection table.

        Returns the object ids of every registered connection (and its
        underlying muxed connection) plus the base58 ids of every connected
        peer.  The swarm removes connections from this table when they close
        (``SwarmConn.close`` -> ``remove_conn``), so a stream whose owning
        connection is absent from this snapshot is dead even when the stream
        and connection objects have not yet reported themselves closed — the
        exact scenario behind phantom leak records on QUIC connections that
        terminate without dispatching per-stream close events.
        """
        live_ids: set[int] = set()
        live_peers: set[str] = set()
        network = self._network
        if network is None:
            return live_ids, live_peers
        try:
            conns_map = getattr(network, "connections", None)
            if conns_map is None:
                return live_ids, live_peers
            for pid, conns in conns_map.items():
                try:
                    b58 = pid.to_base58() if hasattr(pid, "to_base58") else str(pid)
                    live_peers.add(b58)
                except Exception:
                    pass
                items = conns if isinstance(conns, list) else [conns]
                for c in items:
                    if c is None:
                        continue
                    live_ids.add(id(c))
                    m = getattr(c, "muxed_conn", None)
                    if m is not None:
                        live_ids.add(id(m))
        except Exception:
            pass
        return live_ids, live_peers

    @staticmethod
    def _record_connection_gone(
        record: StreamRecord, live_ids: set[int], live_peers: set[str]
    ) -> bool:
        """
        True when the record's owning connection is gone from the swarm table.

        Called only when the tracker has a network reference.  If the record's
        ``SwarmConn`` or muxed connection is still registered, the stream may
        legitimately be open (fall through to the age-based leak check).  If
        neither is registered — or the peer has no connections at all — the
        stream is dead and must be finalized instead of flagged as a leak.
        """
        try:
            ref = record.stream_ref
            swarm_conn = getattr(ref, "swarm_conn", None)
            if swarm_conn is not None and id(swarm_conn) in live_ids:
                return False
            muxed_conn = getattr(ref, "muxed_conn", None)
            if muxed_conn is not None and id(muxed_conn) in live_ids:
                return False
            # Could not identify the connection by identity but the peer has
            # no registered connections: every stream on it is dead.
            if record.peer_id != "unknown" and record.peer_id not in live_peers:
                return True
            # Identifiable connection that is no longer registered.
            if swarm_conn is not None or muxed_conn is not None:
                return True
            return False
        except Exception:
            return False

    @staticmethod
    def _record_stream_dead(record: StreamRecord) -> bool:
        """
        True when a tracked stream is no longer live at the libp2p level.

        ``NetStream`` historically exposes no ``is_closed`` attribute, so the
        old reconcile (``record.stream_ref.is_closed``) silently raised
        ``AttributeError`` on every record and never pruned anything.  This
        checks every reliable signal instead:

        * the stream object itself (``is_closed`` attribute/property/method),
        * the underlying muxed stream (``QUICStream.is_closed()`` / state),
        * the stream's own terminal state machine,
        * the owning connection (a closed connection means every stream on it
          is dead — covers connections that die without dispatching per-stream
          ``closed_stream`` events).
        """
        ref = record.stream_ref
        if ref is None:
            # Underlying stream was already garbage-collected
            return True

        # 1) The stream object itself reports closed.
        try:
            is_closed = getattr(ref, "is_closed", None)
            if is_closed is not None:
                return bool(is_closed() if callable(is_closed) else is_closed)
        except Exception:
            pass

        # 2) The underlying muxed stream (QUICStream) is closed/reset.
        try:
            muxed = getattr(ref, "muxed_stream", None)
            if muxed is not None:
                is_closed = getattr(muxed, "is_closed", None)
                if is_closed is not None:
                    if callable(is_closed):
                        if is_closed():
                            return True
                    elif is_closed:
                        return True
                state = getattr(muxed, "_state", None)
                name = getattr(state, "name", None) or getattr(state, "value", None)
                if name in ("CLOSED", "RESET", "closed", "reset"):
                    return True
        except Exception:
            pass

        # 3) The NetStream state machine reached a terminal state.
        try:
            state = getattr(ref, "_state", None)
            name = getattr(state, "name", "")
            if name in ("CLOSE_BOTH", "RESET", "ERROR"):
                return True
        except Exception:
            pass

        # 4) The owning connection is closed: every stream on it is dead.
        try:
            swarm_conn = getattr(ref, "swarm_conn", None)
            if swarm_conn is not None and getattr(swarm_conn, "is_closed", False):
                return True
        except Exception:
            pass
        try:
            muxed_conn = getattr(ref, "muxed_conn", None)
            if muxed_conn is not None:
                is_closed = getattr(muxed_conn, "is_closed", None)
                if is_closed is not None:
                    if callable(is_closed):
                        if is_closed():
                            return True
                    elif is_closed:
                        return True
        except Exception:
            pass
        return False

    @staticmethod
    def _record_on_conn(
        record: StreamRecord, conn: INetConn, conn_muxed_id: int
    ) -> bool:
        """
        True when *record* lives on the connection that is disconnecting.

        Matches either by the ``SwarmConn`` object identity (the notifee
        receives the same object) or by the underlying muxed connection's
        object id (the identity the stream key is scoped on).
        """
        try:
            ref = record.stream_ref
            if getattr(ref, "swarm_conn", None) is conn:
                return True
            if conn_muxed_id:
                if id(getattr(ref, "muxed_conn", None)) == conn_muxed_id:
                    return True
        except Exception:
            pass
        return False

    def _finalize_record(self, key: str, record: StreamRecord, now: float) -> None:
        """
        Count a stream record as closed and remove it from the live set.

        Shared by the leak-sweep reconciliation and the ``disconnected``
        handler so both capture reset counts and lifetime analytics exactly
        like a normal ``closed_stream`` event (which never reached us).
        """
        state = getattr(record.stream_ref, "_state", None)
        if state is not None and getattr(state, "name", "") == "RESET":
            record.was_reset = True
        self.streams.pop(key, None)
        peer_stats = self.peer_stream_stats.setdefault(
            record.peer_id, PeerStreamStats(peer_id=record.peer_id)
        )
        peer_stats.total_closed += 1
        peer_stats.current_open = max(0, peer_stats.current_open - 1)
        if record.direction == "outbound":
            self.total_outbound_closed += 1
            peer_stats.current_open_outbound = max(0, peer_stats.current_open_outbound - 1)
        elif record.direction == "inbound":
            self.total_inbound_closed += 1
            peer_stats.current_open_inbound = max(0, peer_stats.current_open_inbound - 1)

        if record.was_reset:
            peer_stats.total_resets += 1
        if record.opened_at > 0:
            self._record_lifetime(record.peer_id, now - record.opened_at)
        self._mark_finalized(key)

    def check_for_leaks(self, threshold_seconds: float) -> list[StreamRecord]:
        """
        Flag streams that have been open longer than *threshold_seconds*.

        Reconciles first: any tracked stream whose underlying object reports
        itself closed (without a notifee event reaching us) is finalized.
        Returns the list of suspected leaked streams.
        """
        leaked: list[StreamRecord] = []
        now = time.monotonic()
        live_ids, live_peers = self._live_conn_ids()

        for key, record in list(self.streams.items()):
            # Refresh protocol/direction lazily: protocol negotiation and
            # direction tagging happen after the opened_stream notifee fires.
            self._refresh_record_metadata(record)

            # Reconcile streams closed without a notifee event.  ``key`` may
            # already be in ``_finalized_keys`` (a duplicate close raced with
            # the notifee), but this record still represents a real stream and
            # must be counted.
            if self._record_stream_dead(record):
                self._finalize_record(key, record, now)
                continue

            # The swarm's live connection table is the authoritative source
            # of truth: a stream whose owning connection is no longer
            # registered (even though the stream/conn objects have not yet
            # reported closed) is dead.  This catches QUIC connections that
            # terminate without dispatching per-stream close events, before
            # the age threshold turns them into false "leaks".
            if self._network is not None and self._record_connection_gone(
                record, live_ids, live_peers
            ):
                self._finalize_record(key, record, now)
                continue

            if record.suspected_leak:
                continue  # already flagged

            age = now - record.opened_at
            if age > threshold_seconds:
                record.suspected_leak = True
                leaked.append(record)
                peer_stats = self.peer_stream_stats.setdefault(
                    record.peer_id, PeerStreamStats(peer_id=record.peer_id)
                )
                peer_stats.suspected_leaks += 1
                IPFS_STREAMS_LEAKED_TOTAL.inc()
                logger.warning(
                    "SUSPECTED STREAM LEAK: peer=%s proto=%s dir=%s "
                    "open for %.0fs (threshold %.0fs)",
                    record.peer_id,
                    record.protocol,
                    record.direction,
                    age,
                    threshold_seconds,
                )

        if leaked:
            logger.warning(
                "Stream leak sweep found %d suspected leaked stream(s)", len(leaked)
            )
        return leaked

    def reset_stream_stats(self) -> None:
        """Clear per-peer stream stats and live records (used by tests)."""
        self.streams.clear()
        self.peer_stream_stats.clear()
        self._peer_lifetime.clear()
        self._finalized_keys.clear()

    # ------------------------------------------------------------------
    # Snapshots / reports
    # ------------------------------------------------------------------

    def avg_stream_lifetime(self) -> float | None:
        total = 0.0
        count = 0
        for sum_dur, n in self._peer_lifetime.values():
            total += sum_dur
            count += n
        if count == 0:
            return None
        return total / count

    def stream_stats_snapshot(
        self, leak_threshold_seconds: float | None = None
    ) -> dict[str, Any]:
        """JSON-ready global + per-peer stream statistics."""
        # Refresh protocol/direction so the report reflects negotiated values.
        for record in self.streams.values():
            self._refresh_record_metadata(record)

        open_streams = [
            {
                "peer_id": r.peer_id,
                "protocol": r.protocol,
                "direction": r.direction,
                "stream_id": r.stream_id,
                "open_seconds": round(time.monotonic() - r.opened_at, 2),
            }
            for r in self.streams.values()
        ]

        per_peer = []
        for s in self.peer_stream_stats.values():
            dump = s.model_dump()
            if not s.total_opened:
                continue
            # Fill in the per-peer average lifetime, which was previously
            # never computed and always serialized as null.
            agg = self._peer_lifetime.get(s.peer_id)
            if agg is not None and agg[1] > 0:
                dump["avg_lifetime_seconds"] = round(agg[0] / agg[1], 3)
            else:
                dump["avg_lifetime_seconds"] = None
            per_peer.append(dump)

        open_by_proto: dict[str, int] = {}
        for r in self.streams.values():
            proto = r.protocol or "unknown"
            open_by_proto[proto] = open_by_proto.get(proto, 0) + 1

        active_outbound = sum(
            1 for r in self.streams.values() if r.direction == "outbound"
        )
        active_inbound = sum(
            1 for r in self.streams.values() if r.direction == "inbound"
        )
        active_unknown = len(self.streams) - (active_outbound + active_inbound)

        return {
            "CurrentOpenStreams": len(self.streams),
            "ActiveOutboundStreams": active_outbound,
            "ActiveInboundStreams": active_inbound,
            "ActiveUnknownStreams": active_unknown,
            "TotalOutboundOpened": self.total_outbound_opened,
            "TotalInboundOpened": self.total_inbound_opened,
            "TotalOutboundClosed": self.total_outbound_closed,
            "TotalInboundClosed": self.total_inbound_closed,
            "open_streams_by_protocol": open_by_proto,
            "total_streams_by_protocol": dict(self.total_streams_by_protocol),
            "total_streams_by_protocol_outbound": dict(self.total_streams_by_protocol_outbound),
            "total_streams_by_protocol_inbound": dict(self.total_streams_by_protocol_inbound),
            "ByDirection": {
                "active": {
                    "outbound": active_outbound,
                    "inbound": active_inbound,
                    "unknown": active_unknown,
                },
                "total_opened": {
                    "outbound": self.total_outbound_opened,
                    "inbound": self.total_inbound_opened,
                },
                "total_closed": {
                    "outbound": self.total_outbound_closed,
                    "inbound": self.total_inbound_closed,
                },
            },
            "OpenStreams": open_streams,
            "AvgLifetimeSeconds": self.avg_stream_lifetime(),
            "PerPeer": per_peer,
            "LeakThresholdConfigured": leak_threshold_seconds is not None,
            "LeakThresholdSeconds": leak_threshold_seconds,
        }

    def mark_ping_completed(self, peer_id: str) -> None:
        if peer_id in self.stats:
            now_str = self._now()
            self.stats[peer_id].ping_completed = True
            if self.stats[peer_id].first_ping_at is None:
                self.stats[peer_id].first_ping_at = now_str
            self.stats[peer_id].last_ping_at = now_str
