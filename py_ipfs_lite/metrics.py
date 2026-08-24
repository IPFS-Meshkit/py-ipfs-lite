import os
import time
from typing import Any

from prometheus_client import Counter, Gauge, Histogram

# BlockStore Metrics
IPFS_BLOCKSTORE_SIZE_BYTES = Gauge(
    "ipfs_blockstore_size_bytes", "Total size of blocks in the blockstore in bytes"
)

IPFS_BLOCKSTORE_BLOCKS_TOTAL = Gauge(
    "ipfs_blockstore_blocks_total", "Total number of blocks in the blockstore"
)

# Bitswap Metrics
IPFS_BITSWAP_BYTES_SENT_TOTAL = Counter(
    "ipfs_bitswap_bytes_sent_total", "Total bytes sent over bitswap"
)

IPFS_BITSWAP_BYTES_RECEIVED_TOTAL = Counter(
    "ipfs_bitswap_bytes_received_total", "Total bytes received over bitswap"
)

IPFS_BITSWAP_MESSAGES_SENT_TOTAL = Counter(
    "ipfs_bitswap_messages_sent_total", "Total messages sent over bitswap"
)

IPFS_BITSWAP_MESSAGES_RECEIVED_TOTAL = Counter(
    "ipfs_bitswap_messages_received_total", "Total messages received over bitswap"
)

# DHT Routing Metrics
IPFS_DHT_QUERY_LATENCY_SECONDS = Histogram(
    "ipfs_dht_query_latency_seconds", "Latency of DHT queries in seconds"
)

# Garbage Collection Metrics
IPFS_GC_RUNS_TOTAL = Counter(
    "ipfs_gc_runs_total", "Total number of garbage collection runs"
)

IPFS_GC_RECLAIMED_BLOCKS_TOTAL = Counter(
    "ipfs_gc_reclaimed_blocks_total",
    "Total number of blocks reclaimed during garbage collection",
)

# Swarm & Connections Observability
IPFS_SWARM_PEERS = Gauge(
    "ipfs_swarm_peers", "Total number of unique connected swarm peers"
)

IPFS_SWARM_CONNECTIONS_TOTAL = Gauge(
    "ipfs_swarm_connections_total",
    "Total active raw connections across all transports",
)

IPFS_SWARM_CONNECTIONS = Gauge(
    "ipfs_swarm_connections",
    "Number of active connections by transport and direction",
    ["transport", "direction"],
)

IPFS_SWARM_PEERS_BY_AGE = Gauge(
    "ipfs_swarm_peers_by_age",
    "Number of active peers categorized by connection duration tier",
    ["age_bucket"],
)

IPFS_SWARM_CONNECTS_TOTAL = Counter(
    "ipfs_swarm_connects_total",
    "Total lifetime connection events by transport",
    ["transport"],
)

IPFS_SWARM_DISCONNECTS_TOTAL = Counter(
    "ipfs_swarm_disconnects_total",
    "Total lifetime disconnection events by transport",
    ["transport"],
)

IPFS_SWARM_DISCONNECT_REASONS_TOTAL = Counter(
    "ipfs_swarm_disconnect_reasons_total",
    "Total disconnection events by estimated reason category",
    ["reason_hint"],
)

# Auto-Connector State & Watermarks
IPFS_AUTOCONNECTOR_STATE = Gauge(
    "ipfs_autoconnector_state",
    "Auto-connector configuration and live state values",
    ["metric"],
)

# Network Streams Observability
IPFS_STREAMS_OPENED_TOTAL = Counter(
    "ipfs_streams_opened_total", "Total number of network streams opened"
)

IPFS_STREAMS_CLOSED_TOTAL = Counter(
    "ipfs_streams_closed_total", "Total number of network streams closed"
)

IPFS_STREAMS_OUTBOUND_TOTAL = Counter(
    "ipfs_streams_outbound_total",
    "Total number of network streams opened by us (outbound/initiator)",
)

IPFS_STREAMS_INBOUND_TOTAL = Counter(
    "ipfs_streams_inbound_total",
    "Total number of network streams received by us (inbound/receiver)",
)

IPFS_STREAMS_OUTBOUND_ACTIVE = Gauge(
    "ipfs_streams_outbound_active",
    "Number of currently active streams opened by us (outbound/initiator)",
)

IPFS_STREAMS_INBOUND_ACTIVE = Gauge(
    "ipfs_streams_inbound_active",
    "Number of currently active streams received by us (inbound/receiver)",
)

IPFS_STREAMS_ACTIVE_BY_DIRECTION = Gauge(
    "ipfs_streams_active_by_direction",
    "Number of currently open multiplexed streams by direction (outbound vs inbound)",
    ["direction"],
)

IPFS_STREAMS_LEAKED_TOTAL = Counter(
    "ipfs_streams_leaked_total", "Total number of suspected leaked streams detected"
)

IPFS_STREAMS_ACTIVE = Gauge(
    "ipfs_streams_active",
    "Number of currently open multiplexed streams by protocol",
    ["protocol"],
)

IPFS_STREAMS_BY_PROTOCOL_TOTAL = Gauge(
    "ipfs_streams_by_protocol_total",
    "Cumulative network streams opened by protocol and direction",
    ["protocol", "direction"],
)

IPFS_STREAMS_RESETS_TOTAL = Counter(
    "ipfs_streams_resets_total", "Total number of stream resets observed"
)

IPFS_PROCESS_PID = Gauge("ipfs_process_pid", "Process ID of the IPFS-Lite daemon")

IPFS_PROCESS_CPU_PERCENT = Gauge(
    "ipfs_process_cpu_percent", "Current process CPU utilization percentage"
)

IPFS_PROCESS_MEMORY_RSS_BYTES = Gauge(
    "ipfs_process_memory_rss_bytes",
    "Process Resident Set Size (physical memory) in bytes",
)

IPFS_PROCESS_MEMORY_VMS_BYTES = Gauge(
    "ipfs_process_memory_vms_bytes",
    "Process Virtual Memory Size in bytes",
)

IPFS_PROCESS_OPEN_FDS = Gauge(
    "ipfs_process_open_fds", "Number of open file descriptors"
)

IPFS_PROCESS_UPTIME_SECONDS = Gauge(
    "ipfs_process_uptime_seconds", "Node process uptime in seconds"
)

_PROCESS_START_TIME = time.time()
_PROCESS: Any = None
_LAST_CPU_TIMES: Any = None
_LAST_CPU_CHECK_TIME: float = 0.0

try:
    import psutil

    _PROCESS = psutil.Process(os.getpid())
    _LAST_CPU_TIMES = _PROCESS.cpu_times()
    _LAST_CPU_CHECK_TIME = time.time()
    _PROCESS.cpu_percent(interval=None)
except Exception:
    pass


IPFS_SWARM_PEERS_CONNECTED_OVER_30M = Gauge(
    "ipfs_swarm_peers_connected_over_30m",
    "Number of connected peers maintained for over 30 minutes",
)

IPFS_SWARM_PEERS_CONNECTED_OVER_10M = Gauge(
    "ipfs_swarm_peers_connected_over_10m",
    "Number of connected peers maintained for over 10 minutes",
)

IPFS_SWARM_PEERS_CONNECTED_OVER_5M = Gauge(
    "ipfs_swarm_peers_connected_over_5m",
    "Number of connected peers maintained for over 5 minutes",
)


def update_live_metrics(peer: Any) -> None:
    """Collect and update all dynamic gauges and counters from the live peer."""
    global _PROCESS, _LAST_CPU_TIMES, _LAST_CPU_CHECK_TIME

    # Process Metrics
    try:
        if _PROCESS is None:
            import psutil

            _PROCESS = psutil.Process(os.getpid())
            _LAST_CPU_TIMES = _PROCESS.cpu_times()
            _LAST_CPU_CHECK_TIME = time.time()
            _PROCESS.cpu_percent(interval=None)

        IPFS_PROCESS_PID.set(_PROCESS.pid)

        now = time.time()
        cpu_val = _PROCESS.cpu_percent(interval=None)

        # High-precision delta fallback
        if _LAST_CPU_TIMES is not None and (now - _LAST_CPU_CHECK_TIME) >= 0.5:
            curr_times = _PROCESS.cpu_times()
            user_delta = curr_times.user - _LAST_CPU_TIMES.user
            sys_delta = curr_times.system - _LAST_CPU_TIMES.system
            time_delta = now - _LAST_CPU_CHECK_TIME
            if time_delta > 0:
                calc_cpu = ((user_delta + sys_delta) / time_delta) * 100.0
                if cpu_val == 0.0 and calc_cpu > 0.0:
                    cpu_val = calc_cpu
            _LAST_CPU_TIMES = curr_times
            _LAST_CPU_CHECK_TIME = now

        IPFS_PROCESS_CPU_PERCENT.set(round(cpu_val, 2))
        mem = _PROCESS.memory_info()
        IPFS_PROCESS_MEMORY_RSS_BYTES.set(mem.rss)
        IPFS_PROCESS_MEMORY_VMS_BYTES.set(mem.vms)
        if hasattr(_PROCESS, "num_fds"):
            IPFS_PROCESS_OPEN_FDS.set(_PROCESS.num_fds())
    except Exception:
        pass

    IPFS_PROCESS_UPTIME_SECONDS.set(time.time() - _PROCESS_START_TIME)

    # Swarm / Network metrics
    if peer is None:
        return

    raw_swarm = None
    try:
        if hasattr(peer, "host") and peer.host is not None:
            if hasattr(peer.host, "_host") and peer.host._host is not None:
                raw_swarm = peer.host._host.get_network()
            elif hasattr(peer.host, "get_network"):
                raw_swarm = peer.host.get_network()
    except Exception:
        pass

    # Connection Manager / AutoConnector State
    if raw_swarm is not None:
        try:
            conns_map = getattr(raw_swarm, "connections", {})
            total_unique_peers = len(conns_map)
            IPFS_SWARM_PEERS.set(total_unique_peers)

            if hasattr(raw_swarm, "connection_config") and raw_swarm.connection_config:
                cfg = raw_swarm.connection_config
                IPFS_AUTOCONNECTOR_STATE.labels(metric="low_watermark").set(
                    getattr(cfg, "low_watermark", 0)
                )
                IPFS_AUTOCONNECTOR_STATE.labels(metric="high_watermark").set(
                    getattr(cfg, "high_watermark", 0)
                )
                IPFS_AUTOCONNECTOR_STATE.labels(metric="min_connections").set(
                    getattr(cfg, "min_connections", 0)
                )
                IPFS_AUTOCONNECTOR_STATE.labels(metric="max_connections").set(
                    getattr(cfg, "max_connections", 0)
                )

            if hasattr(raw_swarm, "auto_connector") and raw_swarm.auto_connector:
                in_flight = len(
                    getattr(raw_swarm.auto_connector, "_in_flight_dials", set())
                )
                IPFS_AUTOCONNECTOR_STATE.labels(metric="in_flight_dials").set(in_flight)
        except Exception:
            pass

    # Connection tracker metrics
    tracker = getattr(peer, "connection_tracker", None)
    if tracker is not None:
        try:
            if getattr(tracker, "_network", None) is None and raw_swarm is not None:
                tracker._network = raw_swarm
            snap = tracker.connection_stats_snapshot()
            total_active = snap.get("current_active_connections", 0)
            if total_active == 0 and raw_swarm is not None:
                try:
                    conns_map = getattr(raw_swarm, "connections", {})
                    for c_list in conns_map.values():
                        total_active += len(c_list) if isinstance(c_list, list) else 1
                except Exception:
                    pass
            IPFS_SWARM_CONNECTIONS_TOTAL.set(total_active)

            # Active connections by transport & direction & age breakdown
            breakdown = snap.get("active_connections_breakdown", {})
            by_transport = breakdown.get("by_transport", {})
            by_direction = breakdown.get("by_direction", {})
            for t, count in by_transport.items():
                IPFS_SWARM_CONNECTIONS.labels(transport=t, direction="all").set(count)
            for d, count in by_direction.items():
                IPFS_SWARM_CONNECTIONS.labels(transport="all", direction=d).set(count)

            # Active Peers by Age Bucket
            age_dist = breakdown.get("by_current_age", {})
            for bucket, count in age_dist.items():
                clean_bucket = bucket.split(" ")[0] if " " in bucket else bucket
                IPFS_SWARM_PEERS_BY_AGE.labels(age_bucket=clean_bucket).set(count)

            # Direct Prominence Gauges
            over_30m = age_dist.get(
                "over_30m (long-lived stable)", age_dist.get("over_30m", 0)
            )
            over_10m = over_30m + age_dist.get("10m_to_30m", 0)
            over_5m = over_10m + age_dist.get("5m_to_10m", 0)
            IPFS_SWARM_PEERS_CONNECTED_OVER_30M.set(over_30m)
            IPFS_SWARM_PEERS_CONNECTED_OVER_10M.set(over_10m)
            IPFS_SWARM_PEERS_CONNECTED_OVER_5M.set(over_5m)

            # Active streams and cumulative totals by protocol and direction
            if hasattr(tracker, "stream_stats_snapshot"):
                stream_snap = tracker.stream_stats_snapshot()
                open_by_proto = stream_snap.get("open_streams_by_protocol", {})
                for proto, count in open_by_proto.items():
                    IPFS_STREAMS_ACTIVE.labels(protocol=proto).set(count)

                total_by_proto = stream_snap.get("total_streams_by_protocol", {})
                total_by_proto_out = stream_snap.get(
                    "total_streams_by_protocol_outbound", {}
                )
                total_by_proto_in = stream_snap.get(
                    "total_streams_by_protocol_inbound", {}
                )
                for proto, count in total_by_proto.items():
                    IPFS_STREAMS_BY_PROTOCOL_TOTAL.labels(
                        protocol=proto, direction="all"
                    ).set(count)
                    IPFS_STREAMS_BY_PROTOCOL_TOTAL.labels(
                        protocol=proto, direction="outbound"
                    ).set(total_by_proto_out.get(proto, 0))
                    IPFS_STREAMS_BY_PROTOCOL_TOTAL.labels(
                        protocol=proto, direction="inbound"
                    ).set(total_by_proto_in.get(proto, 0))

                act_out = stream_snap.get("ActiveOutboundStreams", 0)
                act_in = stream_snap.get("ActiveInboundStreams", 0)
                IPFS_STREAMS_OUTBOUND_ACTIVE.set(act_out)
                IPFS_STREAMS_INBOUND_ACTIVE.set(act_in)
                IPFS_STREAMS_ACTIVE_BY_DIRECTION.labels(direction="outbound").set(
                    act_out
                )
                IPFS_STREAMS_ACTIVE_BY_DIRECTION.labels(direction="inbound").set(act_in)
        except Exception:
            pass


class MetricsBlockStore:
    """Wraps a libp2p BlockStore to record prometheus metrics on put/delete."""

    def __init__(self, store: Any) -> None:
        self._store = store

    async def put_block(self, cid: bytes, data: bytes) -> None:
        exists = await self.has_block(cid)
        await self._store.put_block(cid, data)
        if not exists:
            IPFS_BLOCKSTORE_BLOCKS_TOTAL.inc()
            IPFS_BLOCKSTORE_SIZE_BYTES.inc(len(data))

    async def put_many(self, blocks: Any) -> None:
        blocks_list = list(blocks)
        new_blocks = []
        for cid, data in blocks_list:
            if not await self.has_block(cid):
                new_blocks.append((cid, data))

        if hasattr(self._store, "put_many"):
            await self._store.put_many(blocks_list)
        else:
            for cid, data in blocks_list:
                await self._store.put_block(cid, data)

        for _, data in new_blocks:
            IPFS_BLOCKSTORE_BLOCKS_TOTAL.inc()
            IPFS_BLOCKSTORE_SIZE_BYTES.inc(len(data))

    async def get_block(self, cid: bytes) -> Any:
        return await self._store.get_block(cid)

    async def has_block(self, cid: bytes) -> bool:
        return await self._store.has_block(cid)

    async def delete_block(self, cid: bytes) -> None:
        if not await self.has_block(cid):
            return

        size = 0
        try:
            size = await self.get_size(cid)
        except Exception:
            pass

        await self._store.delete_block(cid)

        try:
            if IPFS_BLOCKSTORE_BLOCKS_TOTAL._value.get() > 0:
                IPFS_BLOCKSTORE_BLOCKS_TOTAL.dec()
            if size > 0 and IPFS_BLOCKSTORE_SIZE_BYTES._value.get() >= size:
                IPFS_BLOCKSTORE_SIZE_BYTES.dec(size)
        except Exception:
            pass

    async def get_size(self, cid: bytes) -> int:
        if hasattr(self._store, "get_size"):
            import inspect

            if inspect.iscoroutinefunction(self._store.get_size):
                return await self._store.get_size(cid)
            return self._store.get_size(cid)
        data = await self.get_block(cid)
        return len(data) if data else 0

    def get_all_cids(self) -> Any:
        return self._store.get_all_cids()

    def __getattr__(self, name: Any) -> Any:
        return getattr(self._store, name)
