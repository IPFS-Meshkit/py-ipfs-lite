import base64
import collections
import contextlib
import logging
import time
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Iterator
from dataclasses import dataclass
from typing import (
    Any,
    BinaryIO,
)

import trio
from libp2p import new_host
from multiaddr import Multiaddr

from py_ipfs_lite.config import AddParams, Config
from py_ipfs_lite.connection_tracker import ConnectionStatsTracker
from py_ipfs_lite.exceptions import BlockNotFoundError, PeerNotStartedError
from py_ipfs_lite.metrics import (
    IPFS_BITSWAP_BYTES_RECEIVED_TOTAL,
    IPFS_GC_RECLAIMED_BLOCKS_TOTAL,
    IPFS_GC_RUNS_TOTAL,
    MetricsBlockStore,
)

# Pubsub imports (lazy-loaded to avoid circular deps)
try:
    from libp2p.pubsub.gossipsub import GossipSub
    from libp2p.pubsub.pubsub import Pubsub
    from libp2p.tools.anyio_service import background_trio_service
    from libp2p.tools.anyio_service.trio_manager import TrioManager

    _HAS_PUBSUB = True
except ImportError:
    GossipSub = None  # type: ignore
    Pubsub = None  # type: ignore
    background_trio_service = None  # type: ignore
    TrioManager = None  # type: ignore
    _HAS_PUBSUB = False
    _HAS_PUBSUB = False


@dataclass
class GCResult:
    reclaimed_blocks: int
    retained_blocks: int


import json

import cbor2
from libp2p.bitswap import BitswapClient, MemoryBlockStore
from libp2p.bitswap.block_store import FilesystemBlockStore
from libp2p.bitswap.cid import (
    cid_to_bytes,
    compute_cid_v1,
    format_cid_for_display,
    parse_cid,
    parse_cid_codec,
)
from libp2p.bitswap.dag import MerkleDag, decode_dag_pb
from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.crypto.keys import KeyPair
from libp2p.crypto.x25519 import create_new_key_pair as create_new_x25519_key_pair
from libp2p.discovery.bootstrap.bootstrap import BootstrapDiscovery
from libp2p.kad_dht.kad_dht import DHTMode, KadDHT
from libp2p.peer.peerinfo import info_from_p2p_addr
from libp2p.security.noise.transport import Transport as NoiseTransport


def _check_nan(node: Any) -> None:
    import math

    if isinstance(node, float) and (math.isnan(node) or math.isinf(node)):
        raise ValueError(f"Out of range float values are not JSON compliant: {node}")
    elif isinstance(node, dict):
        for v in node.values():
            _check_nan(v)
    elif isinstance(node, list):
        for item in node:
            _check_nan(item)


class _IPLDNodeEncoder(json.JSONEncoder):
    """JSON encoder that serialises IPLDNode objects to their CID strings."""

    def default(self, o: Any) -> Any:
        if isinstance(o, IPLDNode):
            return str(o)
        return super().default(o)


def _convert_ipld_nodes(obj: Any) -> Any:
    """Recursively convert IPLDNode instances to strings for CBOR encoding."""
    if isinstance(obj, IPLDNode):
        return str(obj)
    if isinstance(obj, dict):
        return {k: _convert_ipld_nodes(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_ipld_nodes(item) for item in obj]
    return obj


def encode_node(node: Any, codec: str) -> bytes:
    if codec in ("dag-json", "dag-cbor", "cbor"):
        _check_nan(node)

    if codec == "dag-json":
        return json.dumps(
            node, separators=(",", ":"), allow_nan=False, cls=_IPLDNodeEncoder
        ).encode("utf-8")
    elif codec in ("dag-cbor", "cbor"):
        return cbor2.dumps(_convert_ipld_nodes(node))
    elif codec == "raw":
        if isinstance(node, bytes):
            return node
        elif isinstance(node, str):
            return node.encode("utf-8")
        raise TypeError("The 'raw' codec only supports bytes or str inputs")
    else:
        raise ValueError(f"Unsupported codec for encode_node: {codec}")


def decode_node(data: bytes, codec: str) -> Any:
    if codec == "dag-json":
        return json.loads(data.decode("utf-8"))
    elif codec in ("dag-cbor", "cbor"):
        return cbor2.loads(data)
    elif codec == "raw":
        return data
    elif codec == "dag-pb":
        links, unixfs = decode_dag_pb(data)
        return {"Links": links, "Data": unixfs}
    else:
        raise ValueError(f"Unsupported codec for decode_node: {codec}")


from py_ipfs_lite.interfaces import (
    BlockStore,
    BlockStoreAdapter,
    DagService,
    Datastore,
    Exchange,
    Host,
    HostAdapter,
    Routing,
    RoutingAdapter,
)
from py_ipfs_lite.pin import PinStore
from py_ipfs_lite.reprovider import Reprovider

logger = logging.getLogger("py_ipfs_lite.peer")


class IPLDNode:
    """Lightweight wrapper around a CID and its raw data, mimicking ipld.Node."""

    __slots__ = ("_cid_str", "_cid_bytes", "_data", "_links", "_codec")

    def __init__(
        self,
        cid_str: str,
        data: bytes | None = None,
        links: list[Any] | None = None,
        codec: str | None = None,
    ) -> None:
        self._cid_str = cid_str
        self._cid_bytes = cid_to_bytes(parse_cid(cid_str)) if cid_str else b""
        self._data = data
        self._links = links or []
        self._codec = codec

    def cid(self) -> str:
        return self._cid_str

    @property
    def cid_bytes(self) -> bytes:
        """The raw CID bytes (multicodec-prefixed)."""
        return self._cid_bytes

    def __str__(self) -> str:
        return self._cid_str

    def __repr__(self) -> str:
        return f"IPLDNode({self._cid_str})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, IPLDNode):
            return self._cid_str == other._cid_str
        if isinstance(other, str):
            return self._cid_str == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._cid_str)

    def raw_data(self) -> bytes | None:
        return self._data

    def links(self) -> list[Any]:
        return self._links

    def codec(self) -> str | None:
        return self._codec

    def loggable(self) -> dict[str, Any]:
        info: dict[str, Any] = {"cid": self._cid_str}
        if self._codec:
            info["codec"] = self._codec
        if self._links:
            info["links"] = len(self._links)
        return info


class SeekableReader:
    """Seekable reader wrapper for get_file, mimicking ufsio.ReadSeekCloser."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0

    async def read(self, n: int = -1) -> bytes:
        if n == -1:
            result = self._data[self._pos :]
            self._pos = len(self._data)
        else:
            result = self._data[self._pos : self._pos + n]
            self._pos += len(result)
        return result

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 0:
            self._pos = offset
        elif whence == 1:
            self._pos += offset
        elif whence == 2:
            self._pos = len(self._data) + offset
        else:
            raise ValueError(f"Invalid whence: {whence}")
        self._pos = max(0, min(self._pos, len(self._data)))
        return self._pos

    def tell(self) -> int:
        return self._pos

    async def close(self) -> None:
        pass

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, bytes):
            return self._data == other
        if isinstance(other, SeekableReader):
            return self._data == other._data
        return NotImplemented

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"SeekableReader({len(self._data)} bytes, pos={self._pos})"

    def __iter__(self) -> Iterator[bytes]:
        """Yield the full data as a single chunk for sync iteration."""
        yield self._data


class PeerSession:
    """Session-scoped block retrieval, creating a fresh BitswapSession per instance."""

    def __init__(self, exchange: Any) -> None:
        self._exchange = exchange
        self._session = (
            exchange.new_session() if hasattr(exchange, "new_session") else exchange
        )

    async def get_block(
        self,
        cid: Any,
        peer_id: Any = None,
        timeout: float = 90,
    ) -> bytes | None:
        return await self._session.get_block(cid, peer_id=peer_id, timeout=timeout)

    async def get_blocks_batch(
        self,
        cids: list[Any],
        peer_id: Any = None,
        timeout: float = 90,
        batch_size: int = 32,
    ) -> dict[bytes, bytes]:
        if hasattr(self._session, "get_blocks_batch"):
            return await self._session.get_blocks_batch(
                cids, peer_id=peer_id, timeout=timeout, batch_size=batch_size
            )
        raise AttributeError("Session does not support get_blocks_batch")


def _to_cid_str(value: Any) -> str:
    """Coerce a CID-like value (str, IPLDNode, CIDObject) to a plain string."""
    if isinstance(value, str):
        return value
    if isinstance(value, IPLDNode):
        return str(value)
    if hasattr(value, "cid") and not isinstance(value, str):
        return str(value.cid)
    return str(value)


def default_bootstrap_peers() -> list[str]:
    from py_ipfs_lite.cli import DEFAULT_BOOTSTRAP_PEERS

    return DEFAULT_BOOTSTRAP_PEERS.copy()


async def setup_libp2p(
    host_key: Any,
    listen_addrs: list[Any],
    datastore: Any = None,
    offline: bool = False,
    enable_metrics: bool = True,
) -> Any:
    maddrs = [Multiaddr(a) if isinstance(a, str) else a for a in listen_addrs]
    noise_key_pair = create_new_x25519_key_pair()
    sec_opt = {
        "/noise": NoiseTransport(host_key, noise_privkey=noise_key_pair.private_key),
        # "/tls/1.0.0": TLSTransport(host_key),
    }
    has_quic = any("quic" in str(a) for a in maddrs)
    raw_host = new_host(
        key_pair=host_key,
        listen_addrs=maddrs,
        sec_opt=sec_opt,  # type: ignore[arg-type]
        enable_quic=has_quic,
    )

    if enable_metrics:
        attach_libp2p_metrics(raw_host)

    if not offline:
        raw_routing = KadDHT(
            host=raw_host, mode=DHTMode.SERVER, enable_random_walk=True
        )
        return HostAdapter(raw_host), RoutingAdapter(raw_routing)
    return HostAdapter(raw_host), None


def attach_libp2p_metrics(raw_host: Any) -> Any:
    """
    Attach the event-bus driven libp2p Prometheus metrics to a host.

    Falls back silently when the running libp2p build does not provide the
    event-bus metrics API; families then simply stay absent from /metrics.
    """
    try:
        import logging as _logging

        from libp2p.metrics.metrics import Metrics

        metrics = Metrics()
        metrics.attach(raw_host.get_event_bus())
        return metrics
    except Exception as exc:
        _logging.getLogger(__name__).debug(
            "libp2p event-bus metrics not attached: %r", exc
        )
        return None


def new_in_memory_datastore() -> Any:
    return BlockStoreAdapter(MemoryBlockStore())


class RWLock:
    """A trio-compatible read-write lock to allow concurrent reads but exclusive writes."""

    def __init__(self) -> None:
        self._write_lock = trio.Semaphore(1)
        self._read_count = 0
        self._read_count_lock = trio.Lock()

    @contextlib.asynccontextmanager
    async def read_lock(self) -> AsyncGenerator[Any, None]:
        async with self._read_count_lock:
            self._read_count += 1
            if self._read_count == 1:
                await self._write_lock.acquire()
        try:
            yield
        finally:
            async with self._read_count_lock:
                self._read_count -= 1
                if self._read_count == 0:
                    self._write_lock.release()

    @contextlib.asynccontextmanager
    async def write_lock(self) -> AsyncGenerator[Any, None]:
        await self._write_lock.acquire()
        try:
            yield
        finally:
            self._write_lock.release()


from enum import Enum, auto


class PeerState(Enum):
    STOPPED = auto()
    STARTING = auto()
    RUNNING = auto()
    STOPPING = auto()


class Peer:
    def __init__(
        self,
        config: Config,
        *,
        host: Host | None = None,
        routing: Routing | None = None,
        datastore: Datastore | None = None,
        blockstore: BlockStore | None = None,
        exchange: Exchange | None = None,
        dag_service: DagService | None = None,
        host_key: KeyPair | None = None,
        listen_addrs: list[Any] | None = None,
    ) -> None:
        self.config = config
        self._host_key = host_key or create_new_key_pair()
        self._listen_addrs = listen_addrs or []

        self.host = host
        self.routing = routing
        self.datastore = datastore
        self.blockstore = blockstore
        self._exchange = exchange
        self.dag_service = dag_service

        self.libp2p_metrics = None

        pin_path = None
        if self.config.blockstore_type == "filesystem" and self.config.blockstore_path:
            import os

            pin_path = os.path.join(self.config.blockstore_path, "pins.json")
        self.pin_store = PinStore(pin_path)
        self.reprovider = Reprovider(self)

        self._gc_lock = RWLock()
        self._state = PeerState.STOPPED
        self._exit_stack = contextlib.AsyncExitStack()
        self.connection_tracker: ConnectionStatsTracker | None = None
        self._auto_connector = None
        self._connection_pruner = None
        self._inflight_pings: set[Any] = set()
        self._ping_service: Any = None
        self._last_good_peer: Any | None = None

        # Pubsub/GossipSub
        self.pubsub: Any = None
        self._gossipsub: Any = None
        self._pubsub_services: list[Any] = []
        self._pubsub_subscriptions: dict[str, Any] = {}
        # Ring buffer of recent messages per topic, filled by the receive
        # loops and drained by the /api/v0/pubsub/sub endpoint.
        self._pubsub_messages: dict[str, "collections.deque[Any]"] = {}
        # Adaptive topic scorer state
        self._topic_stats: dict[str, dict[str, int]] = {}
        self._auto_joined_topics: set[str] = set()
        self._own_ipns_topic: str = ""

    @classmethod
    async def new(
        cls, datastore: Any, blockstore: Any, host: Any, routing: Any, config: Any
    ) -> Any:
        peer = cls(
            config=config,
            datastore=datastore,
            blockstore=blockstore,
            host=host,
            routing=routing,
        )
        await peer.start()
        return peer

    async def _create_host(self) -> Any:
        from libp2p.rcmgr.connection_limits import ConnectionLimits
        from libp2p.rcmgr.manager import ResourceLimits, new_resource_manager
        from libp2p.transport.quic.config import QUICTransportConfig

        maddrs = [Multiaddr(a) if isinstance(a, str) else a for a in self._listen_addrs]
        noise_key_pair = create_new_x25519_key_pair()
        sec_opt = {
            "/noise": NoiseTransport(
                self._host_key, noise_privkey=noise_key_pair.private_key
            ),
            # "/tls/1.0.0": TLSTransport(self._host_key),
        }
        has_quic = any("quic" in str(a) for a in maddrs)
        # QUIC idle timeout must be LONG ENOUGH that quiet-but-live
        # connections are not evicted: the library default (30 s) matches
        # quic-go's default kubo uses, but here it caused a churn loop —
        # every auto-connected QUIC connection that went a few seconds
        # without traffic was closed by the idle timer ~26-30 s after
        # connect, so the swarm could never climb toward the high
        # watermark.  600 s idle + PING keep-alive every 15 s (kubo
        # parity) keeps live connections open indefinitely and only
        # evicts truly dead peers.
        # NEGOTIATE_TIMEOUT is raised to 60 s so security/multistream
        # negotiations queued behind the dial burst (16-32 parallel dials
        # per auto-connect cycle) complete instead of timing out and
        # closing freshly established connections.
        quic_cfg = (
            QUICTransportConfig(
                idle_timeout=600.0,
                NEGOTIATE_TIMEOUT=60.0,
            )
            if has_quic
            else None
        )
        import os

        from py_ipfs_lite.config import BlockStoreType

        peerstore_opt = None
        if (
            self.config.blockstore_type == BlockStoreType.FILESYSTEM
            and self.config.blockstore_path
        ):
            from libp2p.peer.persistent.datastore.sqlite_sync import (
                SQLiteDatastoreSync,
            )
            from libp2p.peer.persistent.sync.peerstore import (
                SyncPersistentPeerStore,
            )

            # Keep the persistent peerstore inside the blockstore's own
            # directory. Placing it in dirname(blockstore_path) meant two
            # daemons with sibling blockstores (e.g. /tmp/a/blocks and
            # /tmp/b/blocks) shared one peerstore.db and crashed with
            # "database is locked".
            os.makedirs(self.config.blockstore_path, exist_ok=True)
            db_path = os.path.join(self.config.blockstore_path, "peerstore.db")

            datastore = SQLiteDatastoreSync(path=db_path)
            peerstore_opt = SyncPersistentPeerStore(datastore=datastore)

        # Set rcmgr max_connections well above our high watermark to give
        # enough headroom for pending connection attempts during recovery
        # bursts (auto-connector may attempt 300+ simultaneously).
        # Disable graceful degradation: it reduces the connection limit when
        # the counter spikes during recovery — the opposite of what we need —
        # and never recovers because the degraded limit stays low permanently.
        # Disable circuit breaker: after 5 failed dials it opens and blocks
        # all new connections for 60 s, stalling recovery after a crash.
        rcmgr_max = max(self.config.conn_mgr_high_water * 4, 800)
        # Do NOT pass connection_limits: new_resource_manager would otherwise
        # install Rust-style per-direction/per-peer lifecycle caps with
        # hard-coded defaults (256 established outbound, 8 per peer).  Once
        # the node exceeds those caps every new outbound dial is denied and
        # closed ~1ms after registration — burning a core on deny churn and
        # starving stream negotiation (ping/identify time out after 10 s).
        # Connection management is owned by the swarm's conn_mgr config
        # (watermarks + max_connections), so pass empty limits (all None =
        # no lifecycle caps).
        resource_manager = new_resource_manager(
            limits=ResourceLimits(max_connections=rcmgr_max, max_streams=10000),
            connection_limits=ConnectionLimits(),
            enable_graceful_degradation=False,
            enable_circuit_breaker=False,
        )

        # Announce configured public addresses instead of the 0.0.0.0 listen
        # addrs so peers can dial this node back (without this, Identify
        # advertises 0.0.0.0 and remote peers drop the connection, leaving
        # the node stuck with only a handful of peers).
        announce_maddrs = None
        if self.config.announce_addrs:
            announce_maddrs = [
                Multiaddr(a) if isinstance(a, str) else a
                for a in self.config.announce_addrs
            ]

        from libp2p.network.config import ConnectionConfig

        swarm_conn_cfg = ConnectionConfig(
            min_connections=self.config.conn_mgr_low_water,
            low_watermark=self.config.conn_mgr_low_water,
            high_watermark=self.config.conn_mgr_high_water,
            max_connections=rcmgr_max,
        )

        raw_host = new_host(
            key_pair=self._host_key,
            listen_addrs=maddrs,
            sec_opt=sec_opt,  # type: ignore[arg-type]
            enable_quic=has_quic,
            enable_mDNS=self.config.enable_mdns,
            connection_config=swarm_conn_cfg,
            quic_transport_opt=quic_cfg,
            peerstore_opt=peerstore_opt,
            resource_manager=resource_manager,
            announce_addrs=announce_maddrs,
        )
        if getattr(self.config, "enable_libp2p_metrics", True):
            self.libp2p_metrics = attach_libp2p_metrics(raw_host)
        self.connection_tracker = ConnectionStatsTracker()
        raw_host.get_network().register_notifee(self.connection_tracker)
        return HostAdapter(raw_host)

    async def _create_routing(self) -> Any:
        if self.config.offline:
            return None

        raw_host = getattr(self.host, "_host", self.host)
        import typing

        raw_routing = KadDHT(
            host=typing.cast(typing.Any, raw_host),
            mode=DHTMode.SERVER,
            enable_random_walk=True,
        )  # type: ignore[arg-type]
        dht_adapter = RoutingAdapter(raw_routing)

        if getattr(self.config, "use_ipni", False):
            from py_ipfs_lite.routing import DelegatedHTTPRouting, TieredRouting

            ipni = DelegatedHTTPRouting(
                endpoint=getattr(self.config, "ipni_endpoint", "https://cid.contact"),
                host=raw_host,
            )
            return TieredRouting([ipni, dht_adapter])

        return dht_adapter

    def _create_blockstore(self) -> Any:
        from py_ipfs_lite.config import BlockStoreType

        if self.config.blockstore_type == BlockStoreType.FILESYSTEM:
            if not self.config.blockstore_path:
                raise ValueError(
                    "blockstore_path must be provided when blockstore_type is 'filesystem'"
                )

            from py_ipfs_lite.versioning import init_repo_version

            init_repo_version(self.config.blockstore_path)

            raw_bs = FilesystemBlockStore(self.config.blockstore_path)
        else:
            raw_bs = MemoryBlockStore()  # type: ignore[assignment]
        return BlockStoreAdapter(MetricsBlockStore(raw_bs))

    def _resolve_dht(self) -> Any | None:
        """
        Extract the raw KadDHT from self.routing, whether or not IPNI
        tiering is enabled. Returns None if no DHT is configured.
        """
        from py_ipfs_lite.routing import TieredRouting

        routing = self.routing
        if routing is None:
            return None
        if isinstance(routing, TieredRouting):
            for r in routing.routers:
                dht = getattr(r, "_routing", None)
                if dht is not None:
                    return dht
            return None
        return getattr(routing, "_routing", None)

    def _create_exchange(self) -> Any:
        raw_host = getattr(self.host, "_host", self.host)
        raw_bs = getattr(self.blockstore, "_store", self.blockstore)

        provider_query_manager = None
        if not self.config.offline:
            raw_dht = self._resolve_dht()
            if raw_dht is not None:
                from libp2p.bitswap.provider_query import ProviderQueryManager

                provider_query_manager = ProviderQueryManager(
                    raw_dht,
                    max_providers=self.config.bitswap_max_providers,
                    cache_ttl=self.config.bitswap_provider_cache_ttl,
                )
            else:
                logger.debug(
                    "No DHT available for Bitswap provider discovery "
                    "(offline mode or DHT-less routing); falling back to "
                    "broadcast-to-connected-peers only."
                )

        from typing import cast

        from libp2p.abc import IHost
        from libp2p.bitswap.block_store import BlockStore

        bitswap = BitswapClient(
            cast(IHost, raw_host),
            cast(BlockStore, raw_bs),
            provider_query_manager=provider_query_manager,
        )

        class ExchangeAdapter:
            def __init__(self, exchange: Any) -> None:
                self._exchange = exchange

            def _new_session(self) -> Any:
                return (
                    self._exchange.new_session()
                    if hasattr(self._exchange, "new_session")
                    else self._exchange
                )

            async def get_block(
                self,
                cid: Any,
                peer_id: Any = None,
                timeout: float = 90,
                return_peer: bool = False,
                session: Any = None,
            ) -> Any:
                # ``session`` lets one BitswapSession be reused across the
                # whole DAG walk of a single fetch; without it a new session
                # (and its DHT provider lookup) is created per block.
                session = session if session is not None else self._new_session()
                if return_peer:
                    with trio.fail_after(timeout):
                        data = await session.get_block(
                            cid, peer_id=peer_id, timeout=timeout
                        )
                    return (data, peer_id)
                else:
                    with trio.fail_after(timeout):
                        res = await session.get_block(
                            cid, peer_id=peer_id, timeout=timeout
                        )

                if res and not isinstance(res, tuple):
                    IPFS_BITSWAP_BYTES_RECEIVED_TOTAL.inc(len(res))
                return res

            async def get_blocks_batch(
                self,
                cids: Any,
                peer_id: Any = None,
                timeout: float = 90,
                batch_size: int = 32,
                session: Any = None,
            ) -> Any:
                session = session if session is not None else self._new_session()
                if hasattr(session, "get_blocks_batch"):
                    return await session.get_blocks_batch(
                        cids,
                        peer_id=peer_id,
                        timeout=timeout,
                        batch_size=batch_size,
                    )
                elif hasattr(self._exchange, "get_blocks_batch"):
                    return await self._exchange.get_blocks_batch(cids)
                else:
                    raise AttributeError(
                        "Neither session nor exchange has get_blocks_batch"
                    )

            async def start(self) -> None:
                await self._exchange.start()

            async def stop(self) -> None:
                await self._exchange.stop()

            def set_nursery(self, nursery: Any) -> None:
                if hasattr(self._exchange, "set_nursery"):
                    self._exchange.set_nursery(nursery)

            def __getattr__(self, name: Any) -> Any:
                return getattr(self._exchange, name)

        return ExchangeAdapter(bitswap)

    def _create_dag_service(self) -> Any:
        return MerkleDag(self._exchange)  # type: ignore[arg-type]

    @property
    def _started(self) -> bool:
        return self._state == PeerState.RUNNING

    async def start(self) -> None:
        if self._state in (PeerState.RUNNING, PeerState.STARTING):
            return

        self._state = PeerState.STARTING
        try:
            if self.host is None:
                self.host = await self._create_host()
            if (
                self.libp2p_metrics is None
                and getattr(self.config, "enable_libp2p_metrics", True)
                and getattr(self.host, "_host", None) is not None
            ):
                self.libp2p_metrics = attach_libp2p_metrics(
                    self.host._host  # type: ignore[union-attr]
                )
            if self.routing is None:
                self.routing = await self._create_routing()
            if self.blockstore is None:
                self.blockstore = self._create_blockstore()
            if self._exchange is None:
                self._exchange = self._create_exchange()
            if self.dag_service is None:
                self.dag_service = self._create_dag_service()

            # Initialize and update connection managers BEFORE starting host
            raw_swarm = self.host._host.get_network()  # type: ignore[union-attr]
            if hasattr(raw_swarm, "connection_config") and raw_swarm.connection_config:
                # min_connections: the floor the auto-connector maintains.
                # Now matching low_water=50 so we aggressively seek 50+ peers.
                raw_swarm.connection_config.min_connections = (
                    self.config.conn_mgr_low_water
                )
                raw_swarm.connection_config.low_watermark = (
                    self.config.conn_mgr_low_water
                )
                raw_swarm.connection_config.high_watermark = (
                    self.config.conn_mgr_high_water
                )
                # Hard cap: high_watermark (500) + burst buffer (50) = 550 total.
                raw_swarm.connection_config.max_connections = (
                    self.config.conn_mgr_high_water + 50
                )

                # Update the inbound limiter to match the new caps.
                # _inbound_limiter was initialized from the default config (max=300,
                # min=10 → 290 inbound slots). Update it to the real limit so the
                # atomic acquire_nowait() gate actually fires at the right threshold.
                if hasattr(raw_swarm, "_inbound_limiter"):
                    max_inbound = (
                        self.config.conn_mgr_inbound_slots
                        if self.config.conn_mgr_inbound_slots is not None
                        else max(
                            1,
                            raw_swarm.connection_config.max_connections
                            - raw_swarm.connection_config.min_connections,
                        )
                    )
                    raw_swarm._inbound_limiter = __import__("trio").CapacityLimiter(
                        max_inbound
                    )
                    import logging as _logging

                    _logging.getLogger(__name__).info(
                        "Updated _inbound_limiter to %d slots (max=%d, min=%d)",
                        max_inbound,
                        raw_swarm.connection_config.max_connections,
                        raw_swarm.connection_config.min_connections,
                    )

                if hasattr(raw_swarm, "auto_connector"):
                    # 15s interval — replenishes churn smoothly into the 300-500 operating window
                    raw_swarm.auto_connector.auto_connect_interval = 15.0

            maddrs = [
                Multiaddr(a) if isinstance(a, str) else a for a in self._listen_addrs
            ]
            await self._exit_stack.enter_async_context(self.host.run(maddrs))  # type: ignore[union-attr]

            self._nursery = await self._exit_stack.enter_async_context(
                trio.open_nursery()
            )
            if hasattr(self._exchange, "set_nursery"):
                self._exchange.set_nursery(self._nursery)

            self._nursery.start_soon(self.reprovider.start)

            await self._exchange.start()

            if self.routing and hasattr(self.routing, "start"):
                self._nursery.start_soon(self.routing.start)

            # Re-populate the DHT routing table from the persistent peerstore.
            # The SQLite-backed SyncPersistentPeerStore survives restarts (it
            # lives under blockstore_path), but the in-memory Kademlia routing
            # table starts empty — this task re-inserts known peers so
            # provider lookups work immediately instead of needing minutes
            # of random-walk discovery after every reboot.
            self._nursery.start_soon(self._warm_routing_table_from_peerstore)

            # Keep connections alive by sending periodic pings (fixes 25-30s idle disconnects)
            self._nursery.start_soon(self._keep_alive_loop)

            # NOTE: the connection_pruner_loop is intentionally NOT started.
            # Pruning works by disconnecting peers, which triggers immediate
            # reconnect attempts from both sides. Each reconnect attempt
            # floods the QUIC layer with Duplicate CRYPTO data (retransmitted
            # Initial packets) — each requiring AEAD decryption → 100% CPU.
            # Instead, use max_connections for a hard QUIC-level cap and let
            # the 90s idle timeout handle natural cleanup.
            # self._nursery.start_soon(self._connection_pruner_loop)

            # Resource-leak monitoring: periodically sweep open streams and
            # flag any that have outlived the configured threshold.
            if self.config.stream_monitor_enabled:
                self._nursery.start_soon(self._stream_leak_monitor_loop)

            # Initialize Pubsub/GossipSub if enabled
            if self.config.enable_pubsub:
                await self._init_pubsub()

            self._state = PeerState.RUNNING
        except Exception:
            await self.close()
            raise

    async def _warm_routing_table_from_peerstore(self) -> None:
        """
        Re-insert peers persisted in the peerstore into the DHT routing table.

        Runs in the background shortly after startup so a restarted node
        regains a usable routing table immediately.  Entries are added with
        ``skip_server_mode_check=True`` — no network round-trips; dead peers
        are pruned naturally by subsequent lookups and refresh cycles.
        """
        import os
        import random

        import trio
        from libp2p.peer.peerinfo import PeerInfo

        # Give the DHT service a moment to finish starting before we touch
        # its routing table.
        await trio.sleep(10)

        try:
            if not self.routing or not hasattr(self.routing, "_routing"):
                return
            raw_dht = self.routing._routing
            routing_table = getattr(raw_dht, "routing_table", None)
            if routing_table is None:
                return

            raw_host = getattr(self.host, "_host", self.host)
            peerstore = raw_host.get_peerstore()
            my_id = raw_host.get_id()

            max_peers = int(os.environ.get("IPFS_LITE_RT_WARMUP_PEERS", "256"))
            all_ids = list(peerstore.peer_ids())
            candidates = [pid for pid in all_ids if pid != my_id]
            if len(candidates) > max_peers:
                candidates = random.sample(candidates, max_peers)

            added = 0
            skipped_no_addr = 0
            for i, pid in enumerate(candidates):
                if i % 50 == 0:
                    await trio.sleep(0)
                try:
                    addrs = peerstore.addrs(pid)
                except Exception:
                    continue
                if not addrs:
                    skipped_no_addr += 1
                    continue
                try:
                    ok = await routing_table.add_peer(
                        PeerInfo(pid, list(addrs)),
                        skip_server_mode_check=True,
                    )
                    if ok:
                        added += 1
                except Exception:
                    continue

            logging.getLogger(__name__).info(
                "Routing table warm-up: inserted %d/%d persisted peers "
                "(%d without addrs, peerstore total=%d)",
                added,
                len(candidates),
                skipped_no_addr,
                len(all_ids),
            )
        except Exception as e:
            logging.getLogger(__name__).warning(
                "Routing table warm-up failed (non-fatal): %s", e
            )

    async def _keep_alive_loop(self) -> None:
        """
        Periodically ping all connected peers to keep idle connections alive.

        This keeps QUIC/TCP/WS connections alive by sending application-level
        pings via the ``/ipfs/ping/1.0.0`` protocol.  Without it, idle
        connections are closed by transport-level idle timeouts (QUIC 30 s,
        TCP remote-close, etc.) and the auto-connector spends its entire
        budget just reconnecting.

        The sweep interval (15 s) matches the QUIC transport's own keep-alive
        interval (also 15 s) and is well within the 30 s QUIC idle timeout,
        providing a belt-and-suspenders guarantee.  With ~400 connections,
        pinging every peer every 15 s with 25-way concurrency and
        100 ms spacing keeps each connection well within the 30 s QUIC
        idle window.
        """
        raw_host = getattr(self.host, "_host", self.host)
        if raw_host is None:
            return

        if not hasattr(self, "_inflight_pings"):
            self._inflight_pings = set()
        if not hasattr(self, "_last_ping_time"):
            self._last_ping_time = {}

        keepalive_semaphore = trio.Semaphore(25)

        # Use the Peer's main nursery so pings are fire-and-forget.
        # This prevents slow pings from delaying the sweep cycle.
        nursery = getattr(self, "_nursery", None)

        while True:
            await trio.sleep(15.0)  # Sweep every 15 seconds
            try:
                network = raw_host.get_network()
                connected_peers = set()
                if hasattr(network, "connections"):
                    for peer_id, conns in network.connections.items():
                        conns_list = conns if isinstance(conns, list) else [conns]
                        if any(not getattr(c, "is_closed", False) for c in conns_list):
                            connected_peers.add(peer_id)

                if not connected_peers:
                    continue

                now = time.monotonic()
                peers_to_ping = [
                    p
                    for p in connected_peers
                    if p not in self._inflight_pings
                    and now - self._last_ping_time.get(p, 0) >= 15.0
                ]

                # Mark inflight BEFORE starting tasks. Otherwise tasks queued on
                # the semaphore aren't marked yet when the next sweep runs, so
                # every sweep re-enqueues them -> unbounded task accumulation.
                self._inflight_pings.update(peers_to_ping)

                async def _ping_with_semaphore(peer_id):
                    try:
                        async with keepalive_semaphore:
                            await self._ping_peer(peer_id)
                            self._last_ping_time[peer_id] = time.monotonic()
                        await trio.sleep(0.1)
                    except Exception as e:
                        logger.debug(f"Keep-alive ping error for {peer_id}: {e}")
                    finally:
                        self._inflight_pings.discard(peer_id)

                # Fire-and-forget: use the Peer's main nursery so slow pings
                # don't delay the next sweep cycle.
                for peer_id in peers_to_ping:
                    nursery.start_soon(_ping_with_semaphore, peer_id)

                logger.info(
                    f"Keepalive sweep: {len(peers_to_ping)} pings sent, "
                    f"{len(connected_peers)} total connected"
                )

            except Exception as e:
                logger.debug(f"Keep-alive loop error: {e}")

    async def _connection_pruner_loop(self) -> None:
        """
        Actively evict connections when above high_watermark.

        The swarm's ``max_connections`` cap only blocks NEW connections.
        Existing inbound connections from DHT peers can push us to 500+
        because there's no automatic eviction of established connections.
        This loop fills that gap by closing the least-useful peers (those
        not in the DHT routing table and with no active streams) every 15s
        when above high_watermark.
        """
        raw_host = getattr(self.host, "_host", self.host)
        if raw_host is None:
            return

        high = 40
        low = 20

        while True:
            await trio.sleep(15.0)
            try:
                network = raw_host.get_network()
                if not hasattr(network, "connections"):
                    continue

                all_peer_ids = list(network.connections.keys())
                total = len(all_peer_ids)
                if total <= high:
                    continue

                to_evict = total - low
                logger.info(
                    "Connection pruner: %d connections > high_watermark %d, evicting %d",
                    total,
                    high,
                    to_evict,
                )

                # Prefer to evict peers with no active named streams (protocol=None)
                def _conn_score(peer_id: Any) -> int:
                    """Lower score = more expendable."""
                    conns = network.connections.get(peer_id, [])
                    conn_list = conns if isinstance(conns, list) else [conns]
                    # Prefer to keep connections that have established mux
                    has_mux = any(not getattr(c, "is_closed", False) for c in conn_list)
                    return 1 if has_mux else 0

                # Sort: evict closed/trivial connections first
                candidates = sorted(all_peer_ids, key=_conn_score)
                evicted = 0
                for peer_id in candidates[:to_evict]:
                    try:
                        await raw_host.disconnect(peer_id)
                        evicted += 1
                    except Exception:
                        pass

                logger.info("Connection pruner: evicted %d connections", evicted)
            except Exception as e:
                logger.debug(f"Connection pruner error: {e}")

    async def _stream_leak_monitor_loop(self) -> None:
        """
        Periodically sweep open streams and log/flag suspected leaks.

        Uses the connection tracker's ``check_for_leaks`` which both
        reconciles streams closed without a notifee event and flags streams
        open longer than ``stream_leak_threshold_seconds``.
        """
        if self.connection_tracker is None:
            return

        interval = max(1.0, self.config.stream_monitor_interval_seconds)
        threshold = max(1.0, self.config.stream_leak_threshold_seconds)
        logger.info(
            "Stream leak monitor started: sweep every %.0fs, leak threshold %.0fs",
            interval,
            threshold,
        )

        while True:
            await trio.sleep(interval)
            try:
                leaked = self.connection_tracker.check_for_leaks(threshold)
                if leaked:
                    logger.warning(
                        "Resource-leak sweep: %d stream(s) flagged for peer(s) %s",
                        len(leaked),
                        sorted({r.peer_id for r in leaked}),
                    )
                    # Actively reset streams open longer than threshold with no
                    # protocol negotiated — these are zombie QUIC streams where
                    # stream.reset() was called but aioquic held on to them.
                    raw_host = getattr(self.host, "_host", self.host)
                    if raw_host is not None:
                        network = raw_host.get_network()
                        if hasattr(network, "connections"):
                            for peer_id_obj, conns in list(network.connections.items()):
                                conns_list = (
                                    conns if isinstance(conns, list) else [conns]
                                )
                                for conn in conns_list:
                                    muxed = getattr(conn, "muxed_conn", conn)
                                    streams = getattr(muxed, "streams", {})
                                    for sid, stream in list(streams.items()):
                                        proto = getattr(stream, "_protocol", None)
                                        if proto is not None:
                                            continue  # Protocol negotiated, leave alone
                                        open_s = getattr(stream, "open_seconds", None)
                                        if open_s is None:
                                            created = getattr(
                                                stream, "_created_at", None
                                            )
                                            if created:
                                                import time as _t

                                                open_s = _t.monotonic() - created
                                        if open_s is not None and open_s > threshold:
                                            try:
                                                await stream.reset()
                                            except Exception:
                                                pass
                elif logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "Resource-leak sweep: no leaks (open streams: %d)",
                        len(self.connection_tracker.streams),
                    )

                # Periodically trigger GC and glibc malloc_trim to return free memory to OS
                try:
                    import ctypes
                    import gc

                    gc.collect()
                    libc = ctypes.CDLL("libc.so.6")
                    libc.malloc_trim(0)
                except Exception:
                    pass
            except Exception as e:
                logger.debug(f"Stream leak monitor error: {e}")

    async def _init_pubsub(self) -> None:
        """Initialize GossipSub and Pubsub services."""
        if not _HAS_PUBSUB:
            logger.warning("Pubsub requested but libp2p pubsub modules not available")
            return

        raw_host = getattr(self.host, "_host", self.host)
        if raw_host is None:
            logger.warning("Cannot init pubsub: no raw host")
            return

        from libp2p.custom_types import TProtocol

        # Use meshsub/1.1.0 (GossipSub v1.1) for good compatibility
        protocol_id = TProtocol("/meshsub/1.1.0")

        self._gossipsub = GossipSub(
            protocols=[protocol_id],
            degree=self.config.gossipsub_degree,
            degree_low=self.config.gossipsub_degree_low,
            degree_high=self.config.gossipsub_degree_high,
            heartbeat_interval=self.config.gossipsub_heartbeat_interval,
            time_to_live=self.config.gossipsub_time_to_live,
        )

        self.pubsub = Pubsub(raw_host, self._gossipsub)

        # Start pubsub services using TrioManager in our nursery
        async def _run_pubsub_service(service: Any) -> None:
            manager = TrioManager(service)
            await manager.run()

        self._nursery.start_soon(_run_pubsub_service, self.pubsub)
        self._nursery.start_soon(_run_pubsub_service, self._gossipsub)

        # Wait for services to start
        await trio.sleep(1)

        logger.info(f"Pubsub ready with GossipSub ({protocol_id})")

        # Subscribe to configured topics
        for topic in self.config.pubsub_topics:
            await self.subscribe_pubsub_topic(topic)

        # Auto-subscribe to IPNS topic if we have a peer ID
        peer_id = raw_host.get_id()
        if peer_id:
            ipns_topic = f"/ipns/{peer_id.to_base58()}"
            self._own_ipns_topic = ipns_topic
            await self.subscribe_pubsub_topic(ipns_topic)

        # Rejoin auto-discovered topics persisted from a previous run so we
        # are back in known-good meshes immediately instead of waiting for
        # the discovery loop's confirmation window.
        if self.config.pubsub_persist_topics:
            for topic in self._load_auto_pubsub_topics():
                if topic not in self._pubsub_subscriptions:
                    logger.info(f"Rejoining persisted pubsub topic: {topic}")
                    await self.subscribe_pubsub_topic(topic)
                    self._auto_joined_topics.add(topic)

        # Adaptive topic join: watch peer_topics and subscribe to shared topics
        if self.config.pubsub_auto_join_min_peers > 0:
            self._nursery.start_soon(self._pubsub_topic_discovery_loop)

    def _protected_pubsub_topics(self) -> set[str]:
        """Topics never subject to auto-leave (configured + our own IPNS)."""
        return set(self.config.pubsub_topics) | {
            getattr(self, "_own_ipns_topic", "")
        } - {""}

    def _auto_pubsub_path(self) -> str | None:
        """File path for persisting auto-joined topics, or None."""
        import os

        if (
            not self.config.pubsub_persist_topics
            or self.config.blockstore_type == "memory"
            or not self.config.blockstore_path
        ):
            return None
        base = os.path.dirname(self.config.blockstore_path) or "."
        return os.path.join(base, "auto_pubsub_topics.json")

    def _save_auto_pubsub_topics(self) -> None:
        import json
        import os
        import tempfile

        path = self._auto_pubsub_path()
        if path is None:
            return
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            data = sorted(self._auto_joined_topics)
            fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path))
            with os.fdopen(fd, "w") as f:
                json.dump(data, f)
            os.replace(tmp, path)
        except Exception as e:
            logger.debug(f"Failed saving auto pubsub topics: {e}")

    def _load_auto_pubsub_topics(self) -> list[str]:
        import json
        import os

        path = self._auto_pubsub_path()
        if path is None or not os.path.exists(path):
            return []
        try:
            with open(path) as f:
                data = json.load(f)
            return [str(t) for t in data if isinstance(t, str)]
        except Exception as e:
            logger.debug(f"Failed loading auto pubsub topics: {e}")
            return []

    async def _pubsub_topic_discovery_loop(self) -> None:
        """
        Periodically scan ``pubsub.peer_topics`` and auto-join shared topics.

        GossipSub SUB announcements reveal which topics our connected peers
        are subscribed to. Joining a topic that several peers share puts us
        in their mesh, generating regular meshsub traffic and giving remote
        connection managers a reason to keep us (retention).

        Bounded by ``pubsub_auto_join_max_topics`` extra subscriptions.
        """
        while self._state == PeerState.RUNNING:
            await trio.sleep(60.0)
            try:
                await self._pubsub_discovery_pass()
            except Exception as e:
                if self._state == PeerState.RUNNING:
                    logger.debug(f"Pubsub topic discovery error: {e}")

    async def _pubsub_discovery_pass(self) -> tuple[int, int]:
        """
        One scorer pass over ``peer_topics``; returns (joined, left).

        Join rule: topic announced by >= min_peers peers for
        ``join_confirmations`` consecutive scans (~stable mesh, not a blip).
        Candidates ranked by peak announcing-peer count so bigger meshes win.

        Leave rule: auto-joined topic whose announcers fell below the
        threshold for ``leave_misses`` consecutive scans is unsubscribed to
        free capacity. Configured topics and our own IPNS topic are protected.
        """
        if self.pubsub is None or self._state != PeerState.RUNNING:
            return 0, 0

        min_peers = max(1, self.config.pubsub_auto_join_min_peers)
        join_conf = max(1, self.config.pubsub_join_confirmations)
        leave_after = max(1, self.config.pubsub_leave_misses)
        max_extra = max(0, self.config.pubsub_auto_join_max_topics)
        base_count = len(self.config.pubsub_topics) + (
            1 if getattr(self, "_own_ipns_topic", None) else 0
        )

        counts: dict[str, int] = {
            str(topic): len(pids)
            for topic, pids in getattr(self.pubsub, "peer_topics", {}).items()
        }

        # Update per-topic streaks
        for topic in set(self._topic_stats) | {t for t, c in counts.items() if c >= 1}:
            c = counts.get(topic, 0)
            st = self._topic_stats.setdefault(
                topic, {"hits": 0, "misses": 0, "max_peers": 0}
            )
            if c >= min_peers:
                st["hits"] += 1
                st["misses"] = 0
                st["max_peers"] = max(st["max_peers"], c)
            else:
                st["misses"] += 1
                if st["misses"] >= leave_after:
                    # Streak dead — reset so a later revival starts fresh.
                    st["hits"] = 0
                    st["max_peers"] = 0

        left = 0
        # Leave pass first: free capacity before joining anything new
        for topic in list(self._pubsub_subscriptions):
            if topic in self._protected_pubsub_topics():
                continue
            st = self._topic_stats.get(topic)
            if st is not None and st["misses"] >= leave_after:
                sub = self._pubsub_subscriptions.pop(topic, None)
                try:
                    if sub is not None and hasattr(sub, "unsubscribe"):
                        await sub.unsubscribe()
                except Exception as e:
                    logger.debug(f"Error unsubscribing from {topic}: {e}")
                self._auto_joined_topics.discard(topic)
                self._topic_stats.pop(topic, None)
                left += 1
                logger.info(f"Pubsub auto-left stale topic {topic}")

        joined = 0
        candidates = sorted(
            (
                (t, s)
                for t, s in self._topic_stats.items()
                if s["hits"] >= join_conf and t not in self._pubsub_subscriptions
            ),
            key=lambda kv: kv[1]["max_peers"],
            reverse=True,
        )
        for topic, st in candidates:
            if len(self._pubsub_subscriptions) >= base_count + max_extra:
                logger.info(
                    "Pubsub auto-join hit cap (%d extra topics); skipping %s "
                    "(peak %d peers)",
                    max_extra,
                    topic,
                    st["max_peers"],
                )
                break
            logger.info(
                "Pubsub auto-joining stable topic %s (peak %d peers over %d scans)",
                topic,
                st["max_peers"],
                st["hits"],
            )
            await self.subscribe_pubsub_topic(topic)
            self._auto_joined_topics.add(topic)
            joined += 1

        if joined or left:
            self._save_auto_pubsub_topics()
            logger.info(
                "Pubsub discovery: joined %d, left %d (%d subscriptions total)",
                joined,
                left,
                len(self._pubsub_subscriptions),
            )
        return joined, left

    async def subscribe_pubsub_topic(self, topic: str) -> None:
        """Subscribe to a pubsub topic and start receive loop."""
        if self.pubsub is None:
            logger.warning(f"Cannot subscribe to {topic}: pubsub not initialized")
            return

        if topic in self._pubsub_subscriptions:
            logger.debug(f"Already subscribed to {topic}")
            return

        try:
            subscription = await self.pubsub.subscribe(topic)
            self._pubsub_subscriptions[topic] = subscription

            # Start receive loop for this topic
            self._nursery.start_soon(self._pubsub_receive_loop, topic, subscription)

            logger.info(f"Subscribed to pubsub topic: {topic}")
        except Exception as e:
            logger.error(f"Failed to subscribe to {topic}: {e}")

    async def unsubscribe_pubsub_topic(self, topic: str) -> bool:
        """Unsubscribe from a pubsub topic. Returns True if we were subscribed."""
        sub = self._pubsub_subscriptions.pop(topic, None)
        self._pubsub_messages.pop(topic, None)
        if sub is None:
            return False
        try:
            if hasattr(sub, "unsubscribe"):
                await sub.unsubscribe()
        except Exception as e:
            logger.debug(f"Error unsubscribing from {topic}: {e}")
        logger.info(f"Unsubscribed from pubsub topic: {topic}")
        return True

    def get_pubsub_messages(self, topic: str) -> list[dict[str, Any]]:
        """Return (and clear) the buffered messages received on a topic."""
        buffer = self._pubsub_messages.get(topic)
        if not buffer:
            return []
        messages = list(buffer)
        buffer.clear()
        return messages

    async def _pubsub_receive_loop(self, topic: str, subscription: Any) -> None:
        """Receive loop for the given pubsub topic."""
        logger.debug(f"Starting pubsub receive loop for {topic}")
        buffer = self._pubsub_messages.setdefault(topic, collections.deque(maxlen=100))
        while self._state == PeerState.RUNNING:
            try:
                msg = await subscription.get()
                try:
                    from_id = getattr(msg, "from_id", None)
                    if from_id is not None and hasattr(from_id, "to_base58"):
                        from_peer = from_id.to_base58()
                    elif isinstance(from_id, bytes):
                        from libp2p.peer.id import ID as _ID

                        from_peer = _ID(from_id).to_base58()
                    else:
                        from_peer = str(from_id)
                except Exception:
                    from_peer = "unknown"
                buffer.append(
                    {
                        "topic": topic,
                        "from": from_peer,
                        "data": base64.b64encode(msg.data or b"").decode(),
                        "size": len(msg.data or b""),
                        "received_at": time.time(),
                    }
                )
                logger.debug(
                    f"Pubsub message on {topic} from {msg.from_id}: "
                    f"{msg.data[:100] if msg.data else 'empty'}"
                )
            except Exception as e:
                if self._state == PeerState.RUNNING:
                    logger.debug(f"Pubsub receive error on {topic}: {e}")
                break

    async def publish_pubsub(self, topic: str, data: bytes) -> None:
        """Publish a message to a pubsub topic."""
        if self.pubsub is None:
            logger.warning(f"Cannot publish to {topic}: pubsub not initialized")
            return

        try:
            await self.pubsub.publish(topic, data)
            logger.debug(f"Published to {topic}: {len(data)} bytes")
        except Exception as e:
            logger.error(f"Failed to publish to {topic}: {e}")

    async def _ping_peer(self, peer_id: Any, timeout: float = 3.0) -> None:
        """
        Send a single keepalive ping to *peer_id*.

        A ping failure is completely ignored — the connection is left open.
        Dead connections are detected by QUIC's idle timeout, not by us.
        Closing connections on ping failure caused a churn loop because
        Identify timeouts (common on busy peers) triggered close_peer(),
        which caused reconnects, which caused more Identify timeouts.

        IMPORTANT: We must use ``fail_after`` (not ``move_on_after``).
        ``move_on_after`` raises ``trio.Cancelled`` internally, which inherits
        from ``BaseException`` and bypasses the ``except Exception`` cleanup
        block in ``PingService``. This causes the stream to be orphaned and
        leaked forever. ``fail_after`` raises ``trio.TooSlowError`` (an
        ``Exception``), which correctly triggers ``stream.reset()``.
        """
        from libp2p.host.ping import PingService

        raw_host = getattr(self.host, "_host", self.host)
        if raw_host is None:
            return
        from typing import cast

        from libp2p.abc import IHost

        peer_id_str = (
            peer_id.to_base58() if hasattr(peer_id, "to_base58") else str(peer_id)
        )

        # Reuse a single PingService so streams are cached and reused
        # across ping cycles (avoids opening a new /ipfs/ping/1.0.0
        # stream for every single ping).
        if self._ping_service is None:
            self._ping_service = PingService(cast(IHost, raw_host))
        ping_service = self._ping_service

        try:
            with trio.fail_after(timeout):
                await ping_service.ping(peer_id, ping_amt=1)
            if self.connection_tracker:
                self.connection_tracker.mark_ping_completed(peer_id_str)
        except Exception as e:
            logger.debug(f"Keep-alive ping failed for {peer_id} (ignored): {e}")

    async def __aenter__(self) -> "Peer":
        if not self._started:
            await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    async def close(self) -> None:
        if self._state in (PeerState.STOPPED, PeerState.STOPPING):
            return

        is_running = self._state == PeerState.RUNNING
        self._state = PeerState.STOPPING

        try:
            # Close exit_stack FIRST (stops pubsub services gracefully)
            try:
                await self._exit_stack.aclose()
            except Exception as e:
                logger.debug(f"Error closing exit stack: {e}")

            # THEN cancel nursery (stops keepalive, stream monitor, etc.)
            if hasattr(self, "_nursery") and self._nursery:
                self._nursery.cancel_scope.cancel()

            if is_running:
                await self.reprovider.stop()
                if self._exchange:
                    await self._exchange.stop()  # type: ignore[union-attr]

                if self.routing and hasattr(self.routing, "close"):
                    await self.routing.close()

                self._pubsub_subscriptions.clear()
        finally:
            self._state = PeerState.STOPPED
            self._state = PeerState.STOPPED

    async def bootstrap(self, peers: list[str] | list[Any]) -> None:
        """
        Connect to bootstrap peers and join the DHT network.

        Accepts either multiaddr strings or peer.AddrInfo-like objects with
        ``id`` and ``addrs`` attributes.

        Dials every address in *peers* and registers them in the peerstore.
        If a KadDHT routing instance is present, attempts a routing-table
        refresh so the node can discover additional peers.

        The refresh fires up to ``RANDOM_WALK_CONCURRENCY`` (10) concurrent
        FIND_NODE random-walks.  When the bootstrapped peer has an **empty**
        routing table — e.g. a freshly-started isolated Kubo daemon — every
        walk returns immediately with zero results, so the refresh completes
        quickly.  The guard below (``move_on_after``) ensures we never block
        the caller indefinitely even if peers are slow to respond.
        """
        self._ensure_started()

        str_addrs: list[str] = []
        for p in peers:
            if isinstance(p, str):
                str_addrs.append(p)
            elif hasattr(p, "addrs") and hasattr(p, "id"):
                for addr in p.addrs:
                    str_addrs.append(f"{addr}/p2p/{p.id}")
            else:
                str_addrs.append(str(p))

        discovery = BootstrapDiscovery(
            swarm=self.host.get_network(),  # type: ignore
            bootstrap_addrs=str_addrs,  # type: ignore[union-attr]
            event_bus=self.host.get_event_bus(),
        )
        await discovery.start()

        # Best-effort routing-table refresh: when the bootstrap peer returns an
        # empty FIND_NODE response (no known peers) the random-walk queries
        # complete immediately.  We still guard with move_on_after so that slow
        # peers cannot block the caller indefinitely.
        # 30 s >> normal empty-table response time (< 1 s) but won't stall tests.
        if self.routing and hasattr(self.routing, "refresh_routing_table"):
            _RT_REFRESH_TIMEOUT = 30.0
            try:
                with trio.move_on_after(_RT_REFRESH_TIMEOUT) as _scope:
                    await self.routing.refresh_routing_table()
                if _scope.cancelled_caught:
                    logger.debug(
                        "Routing-table refresh did not finish within %.0f s "
                        "(bootstrap peer likely has an empty routing table). "
                        "Connection is active — continuing.",
                        _RT_REFRESH_TIMEOUT,
                    )
            except Exception as e:
                logger.error("Failed to refresh routing table after bootstrap: %s", e)

    async def add_file(
        self,
        path_or_stream: str | bytes | BinaryIO,
        params: AddParams | None = None,
        timeout: float | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> IPLDNode:
        """
        Add a file to the DAGService. Returns an IPLDNode with the root CID.

        Accepts a filesystem path, raw bytes, or a readable binary stream.
        The returned IPLDNode supports ``str(node)`` for backward compatibility
        (returns the CID string).
        """
        self._ensure_started()
        chunk_size: int | None = None
        if params is not None and params.chunker and params.chunker.startswith("size-"):
            try:
                chunk_size = int(params.chunker.split("-")[1])
            except ValueError:
                pass

        wrapped_callback = None
        if progress_callback is not None:

            def _wrapped_callback(
                bytes_written: int, total_bytes: int, phase: str
            ) -> None:
                progress_callback(bytes_written, total_bytes)

            wrapped_callback = _wrapped_callback

        raw_cid: Any = None
        async with self._gc_lock.read_lock():
            if isinstance(path_or_stream, str):
                raw_cid = await self.dag_service.add_file(  # type: ignore[union-attr]
                    path_or_stream,
                    chunk_size=chunk_size,
                    progress_callback=wrapped_callback,
                    wrap_with_directory=False,
                )
            elif isinstance(path_or_stream, bytes):
                raw_cid = await self.dag_service.add_bytes(  # type: ignore[union-attr]
                    path_or_stream,
                    chunk_size=chunk_size,
                    progress_callback=wrapped_callback,
                )
            else:
                raw_cid = await self.dag_service.add_stream(  # type: ignore[union-attr]
                    path_or_stream,
                    chunk_size=chunk_size,
                    progress_callback=wrapped_callback,
                )
        cid_str = format_cid_for_display(raw_cid)
        routing = self.routing
        if routing is not None:
            # The DHT provide walk needs up to ~90s on a cold/loaded node:
            # it first finds the k-closest peers via iterative lookups (~20s),
            # then sends ADD_PROVIDER RPCs to each with per-peer timeouts.
            # Using default_timeout (30s) races against this and causes a
            # silent TooSlowError (which prints as an empty exception message).
            # Match the explicit budget used by the /api/v0/dht/provide endpoint.
            _provide_timeout = 90.0

            async def _bg_provide() -> None:
                try:
                    with trio.fail_after(_provide_timeout):
                        await routing.provide(cid_str)
                except trio.TooSlowError:
                    logger.warning(
                        f"Background DHT provide for {cid_str} timed out after "
                        f"{_provide_timeout}s — local provider record is still stored, "
                        f"but remote peers may not know about it yet."
                    )
                except Exception as e:
                    logger.warning(f"Failed to provide {cid_str} to DHT: {e}")

            if getattr(self, "_nursery", None):
                self._nursery.start_soon(_bg_provide)
            else:
                logger.warning(f"No nursery to background provide {cid_str}")

        return IPLDNode(cid_str)

    async def get_file(
        self,
        cid: Any,
        provider_addr: str | None = None,
        output_path: str | None = None,
        timeout: float | None = None,
        stream: bool = False,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> bytes | SeekableReader | AsyncIterator[bytes] | None:
        """
        Fetch a file by its CID.

        *cid* may be a CID string or an IPLDNode.

        Returns:
            - ``SeekableReader`` (default): seekable, async-readable wrapper
              around the full file bytes — mimics ``ufsio.ReadSeekCloser``.
            - ``bytes``: when *stream=False* (legacy default, buffers everything).
            - ``AsyncIterator[bytes]``: when *stream=True*, yields chunks.
            - ``None``: when *output_path* is set (written to disk).

        """
        self._ensure_started()
        cid_str = _to_cid_str(cid)
        t_val = timeout if timeout is not None else self.config.default_timeout
        cid = parse_cid(cid_str)
        has_root = await self.blockstore.has(cid_to_bytes(cid))  # type: ignore[union-attr]

        if not has_root:
            if provider_addr:
                maddr = Multiaddr(provider_addr)
                info = info_from_p2p_addr(maddr)
                await self.host.connect(info)  # type: ignore[union-attr]

        # Extract total file size from root node's UnixFS metadata for progress tracking
        total_file_size = 0
        if progress_callback is not None:
            try:
                root_data = await self.blockstore.get(cid_to_bytes(cid))  # type: ignore[union-attr]
                if root_data:
                    root_codec = parse_cid_codec(cid_to_bytes(cid))
                    if root_codec == "dag-pb":
                        _, root_unixfs = decode_dag_pb(root_data)
                        if root_unixfs and root_unixfs.filesize:
                            total_file_size = root_unixfs.filesize
            except Exception:
                pass  # If we can't get size, progress will be None/Unknown

        from libp2p.bitswap.dag import is_directory_node

        _get_file_self = self

        class _FetchAffinity:
            def __init__(self) -> None:
                # Seed with the peer that served a previous fetch (persisted
                # across calls) so child wants skip the slow broadcast.
                self.last_good_peer: Any | None = _get_file_self._last_good_peer

            def record(self, peer_id: Any | None) -> None:
                if peer_id is not None:
                    self.last_good_peer = peer_id
                    _get_file_self._last_good_peer = peer_id

        affinity = _FetchAffinity()

        # Seed affinity with provider peer ID so first block request skips DHT
        if provider_addr:
            try:
                maddr = Multiaddr(provider_addr)
                info = info_from_p2p_addr(maddr)
                affinity.record(info.peer_id)
            except Exception:
                pass

        # One Bitswap session per logical fetch. Creating a session per block
        # (as the exchange adapter did) spawned N sessions and N DHT provider
        # lookups for an N-block file.
        psession = PeerSession(self._exchange)

        # Helper to isolate trio.fail_after from the async generator
        async def fetch_block_with_timeout(current_cid: Any) -> Any:
            with trio.fail_after(t_val):
                res = await self._exchange.get_block(  # type: ignore[union-attr, call-arg]
                    current_cid,
                    peer_id=affinity.last_good_peer,
                    return_peer=True,
                    timeout=t_val,
                    session=psession._session,
                )
                if res and isinstance(res, tuple):
                    data, peer_id = res
                    affinity.record(peer_id)
                    return data
                return res

        async def fetch_stream(current_cid: Any) -> AsyncGenerator[Any, None]:
            data = await fetch_block_with_timeout(current_cid)
            if data is None:
                raise BlockNotFoundError(
                    f"Block not found for CID: {format_cid_for_display(current_cid)}"
                )

            codec = parse_cid_codec(cid_to_bytes(current_cid))
            if codec == "raw":
                yield data
                if progress_callback is not None and total_file_size > 0:
                    progress_callback(len(data), total_file_size)
                return

            if codec == "dag-pb":
                if is_directory_node(data):
                    links, _ = decode_dag_pb(data)
                    if links:
                        async for chunk in fetch_stream(links[0].cid):
                            yield chunk
                    return

                links, unixfs = decode_dag_pb(data)
                if not links:
                    if unixfs and unixfs.data:
                        yield unixfs.data
                        if progress_callback is not None and total_file_size > 0:
                            progress_callback(len(unixfs.data), total_file_size)
                    return

                batch_size = 32 if self.config.bitswap_batch_fetch else 1
                for i in range(0, len(links), batch_size):
                    batch_links = links[i : i + batch_size]

                    if (
                        self.config.bitswap_batch_fetch
                        and len(batch_links) > 1
                        and hasattr(self._exchange, "get_blocks_batch")
                    ):
                        cids = [link.cid for link in batch_links]
                        try:
                            with trio.fail_after(t_val):
                                await self._exchange.get_blocks_batch(  # type: ignore[attr-defined, union-attr, call-arg]
                                    cids,
                                    peer_id=affinity.last_good_peer,
                                    timeout=t_val,
                                    batch_size=batch_size,
                                    session=psession._session,
                                )
                        except Exception as e:
                            logger.debug(
                                f"Batch fetch failed for {len(cids)} CIDs: {e}"
                            )

                    for link in batch_links:
                        async for chunk in fetch_stream(link.cid):
                            yield chunk

        if output_path:
            bytes_written = 0
            with open(output_path, "wb") as f:
                async for chunk in fetch_stream(cid):
                    f.write(chunk)
                    bytes_written += len(chunk)
                    if progress_callback is not None and total_file_size > 0:
                        progress_callback(bytes_written, total_file_size)
            return None

        if stream:
            return fetch_stream(cid)

        # Buffer and return a seekable reader (ufsio.ReadSeekCloser equivalent)
        chunks = []
        async for chunk in fetch_stream(cid):
            chunks.append(chunk)
        return SeekableReader(b"".join(chunks))

    async def add_node(
        self,
        node: dict[Any, Any] | list[Any] | str | int | bytes,
        codec: str = "dag-json",
        timeout: float | None = None,
        params: AddParams | None = None,
    ) -> IPLDNode:
        """
        Store an IPLD node in the blockstore and return it as an IPLDNode.

        The returned IPLDNode supports ``str(node)`` returning the CID string
        for backward compatibility.
        """
        self._ensure_started()
        t_val = timeout if timeout is not None else self.config.default_timeout
        data = encode_node(node, codec)
        cid = compute_cid_v1(data, codec=codec)
        async with self._gc_lock.read_lock():
            await self.blockstore.put(cid, data)  # type: ignore[union-attr]
        cid_str = format_cid_for_display(cid)
        routing = self.routing
        if routing is not None:

            async def _bg_provide() -> None:
                try:
                    with trio.fail_after(t_val):
                        await routing.provide(cid_str)
                except Exception as e:
                    logger.warning(f"Failed to provide {cid_str} to DHT: {e}")

            if getattr(self, "_nursery", None):
                self._nursery.start_soon(_bg_provide)
            else:
                logger.warning(f"No nursery to background provide {cid_str}")
        return IPLDNode(cid_str)

    async def get_node(
        self,
        cid: Any,
        provider_addr: str | None = None,
        timeout: float | None = None,
    ) -> dict[Any, Any] | list[Any] | str | int | bytes:
        """Retrieve and decode an IPLD node. *cid* may be a string or IPLDNode."""
        self._ensure_started()
        cid_str = _to_cid_str(cid)
        t_val = timeout if timeout is not None else self.config.default_timeout
        cid = parse_cid(cid_str)

        # Check local blockstore first
        data = await self.blockstore.get(cid_to_bytes(cid))  # type: ignore[union-attr]

        if data is None:
            if provider_addr:
                maddr = Multiaddr(provider_addr)
                info = info_from_p2p_addr(maddr)
                await self.host.connect(info)  # type: ignore[union-attr]

            with trio.fail_after(t_val):
                data = await self._exchange.get_block(cid, timeout=t_val)  # type: ignore[union-attr]

        if data is None:
            raise BlockNotFoundError(f"Block not found for CID: {cid_str}")
        codec = parse_cid_codec(cid_to_bytes(cid))
        return decode_node(data, codec)

    async def remove_node(self, cid: Any) -> None:
        """Delete a block locally. *cid* may be a string or IPLDNode."""
        self._ensure_started()
        cid_str = _to_cid_str(cid)
        parsed = parse_cid(cid_str)
        await self.blockstore.delete(cid_to_bytes(parsed))  # type: ignore[union-attr]

    # ── ipld.DAGService interface ──────────────────────────────────────────

    async def add(self, node: Any, codec: str = "dag-json") -> IPLDNode:
        """Standard DAGService ``Add`` — store an IPLD node and return it."""
        return await self.add_node(node, codec=codec)

    async def get(self, cid_str: str) -> Any:
        """Standard DAGService ``Get`` — retrieve and decode an IPLD node."""
        return await self.get_node(cid_str)

    async def remove(self, cid_str: str) -> None:
        """Standard DAGService ``Remove`` — delete a block locally."""
        await self.remove_node(cid_str)

    async def get_many(self, cid_strs: list[str]) -> list[Any]:
        """Standard DAGService ``GetMany`` — retrieve multiple IPLD nodes in parallel."""
        results: list[Any] = [None] * len(cid_strs)

        async def _fetch_one(idx: int, c: str) -> None:
            results[idx] = await self.get_node(c)

        async with trio.open_nursery() as nursery:
            for idx, c in enumerate(cid_strs):
                nursery.start_soon(_fetch_one, idx, c)

        return results

    async def add_pin(self, cid: Any, recursive: bool = True) -> None:
        """Pin a CID. *cid* may be a string or IPLDNode."""
        self._ensure_started()
        cid_str = _to_cid_str(cid)
        pin_type = "recursive" if recursive else "direct"
        self.pin_store.add_pin(cid_str, pin_type)

    async def remove_pin(self, cid: Any) -> None:
        """Unpin a CID. *cid* may be a string or IPLDNode."""
        self._ensure_started()
        self.pin_store.remove_pin(_to_cid_str(cid))

    async def list_pins(self, type_filter: str = "all") -> dict[str, str]:
        """List pins by type. type_filter can be 'direct', 'recursive', 'indirect', or 'all'."""
        self._ensure_started()

        if type_filter not in ("all", "direct", "recursive", "indirect"):
            raise ValueError(
                "Invalid type_filter. Must be 'all', 'direct', 'recursive', or 'indirect'"
            )

        stored_pins = self.pin_store.get_pins()

        if type_filter in ("direct", "recursive"):
            return {k: v for k, v in stored_pins.items() if v == type_filter}

        result = stored_pins.copy()

        from libp2p.bitswap.cid import cid_to_bytes, format_cid_for_display, parse_cid

        from py_ipfs_lite.dag_utils import walk_dag

        indirect_pins = {}
        for cid_str, pin_type in stored_pins.items():
            if pin_type == "recursive":
                try:
                    c_bytes = cid_to_bytes(parse_cid(cid_str))
                    async for reachable_cid_bytes in walk_dag(
                        c_bytes,
                        self.blockstore.get,  # type: ignore[union-attr]
                        recursive=True,  # type: ignore[union-attr]
                    ):
                        if reachable_cid_bytes != c_bytes:
                            r_str = format_cid_for_display(
                                parse_cid(reachable_cid_bytes)
                            )
                            if r_str not in result:
                                indirect_pins[r_str] = "indirect"
                except Exception as e:
                    logger.warning(f"Failed to traverse pinned CID {cid_str}: {e}")

        if type_filter == "indirect":
            return indirect_pins

        result.update(indirect_pins)
        return result

    async def gc(self) -> GCResult:
        self._ensure_started()
        from libp2p.bitswap.cid import format_cid_for_display

        from py_ipfs_lite.dag_utils import walk_dag

        async with self._gc_lock.write_lock():
            IPFS_GC_RUNS_TOTAL.inc()
            all_cids = set(await self.blockstore.all_keys())  # type: ignore[union-attr]
            reachable_cids = set()

            for cid_str, pin_type in self.pin_store.get_pins().items():
                try:
                    c_bytes = cid_to_bytes(parse_cid(cid_str))
                    is_rec = pin_type == "recursive"
                    async for reachable_cid_bytes in walk_dag(
                        c_bytes,
                        self.blockstore.get,  # type: ignore[union-attr]
                        recursive=is_rec,  # type: ignore[union-attr]
                    ):
                        reachable_cids.add(
                            format_cid_for_display(parse_cid(reachable_cid_bytes))
                        )
                except Exception as e:
                    logger.warning(f"Failed to traverse pinned CID {cid_str}: {e}")

            to_delete = all_cids - reachable_cids
            deleted_count = 0
            for c_str in to_delete:
                await self.blockstore.delete(cid_to_bytes(parse_cid(c_str)))  # type: ignore[union-attr]
                deleted_count += 1

            IPFS_GC_RECLAIMED_BLOCKS_TOTAL.inc(deleted_count)
            return GCResult(
                reclaimed_blocks=deleted_count, retained_blocks=len(reachable_cids)
            )

    async def resolve_name(self, peer_id_str: str, timeout: float | None = None) -> str:
        """Resolve an IPNS name (PeerID) to its value."""
        self._ensure_started()

        from py_ipfs_lite.exceptions import RoutingError

        if self.routing is None:
            raise RoutingError("IPNS requires network routing; this peer is offline")

        t_val = timeout if timeout is not None else self.config.default_timeout
        from libp2p.peer.id import ID

        from py_ipfs_lite.ipns import resolve_name as ipns_resolve

        # We need to look up the routing
        peer_id = ID.from_base58(peer_id_str)
        try:
            with trio.fail_after(t_val):
                return await ipns_resolve(self.routing, peer_id)
        except trio.TooSlowError as e:
            raise RoutingError(f"IPNS resolution timed out after {t_val}s") from e

    async def publish_name(
        self, value: str, lifetime_hours: int = 24, timeout: float | None = None
    ) -> str:
        """Publish an IPNS record pointing to `value` using this node's private key."""
        self._ensure_started()

        from py_ipfs_lite.exceptions import RoutingError

        if self.routing is None:
            raise RoutingError("IPNS requires network routing; this peer is offline")

        t_val = timeout if timeout is not None else self.config.default_timeout
        import time

        from py_ipfs_lite.ipns import _resolve_entry
        from py_ipfs_lite.ipns import publish_name as ipns_publish

        last_sequence = 0
        seq_key = b"/ipns_seq/" + self.host.id().to_bytes()  # type: ignore[union-attr]

        if self.datastore:
            try:
                seq_bytes = await self.datastore.get(seq_key)
                if seq_bytes:
                    last_sequence = int(seq_bytes.decode("utf-8"))
            except Exception:
                pass

        remote_sequence = 0
        try:
            with trio.fail_after(min(5.0, t_val)):
                entry = await _resolve_entry(self.routing, self.host.id())  # type: ignore[union-attr]
                if entry and hasattr(entry, "sequence"):
                    remote_sequence = entry.sequence
        except Exception:
            pass

        # Use time as base to ensure we don't start at 0 if both datastore and DHT are empty
        base_seq = max(last_sequence, remote_sequence, int(time.time()))
        sequence = base_seq + 1

        if self.datastore:
            try:
                await self.datastore.put(seq_key, str(sequence).encode("utf-8"))
            except Exception:
                pass

        try:
            with trio.fail_after(t_val):
                await ipns_publish(
                    self.routing,
                    self._host_key.private_key,
                    self.host.id(),  # type: ignore[union-attr]
                    value,
                    sequence,
                    lifetime_hours,
                )
        except Exception as e:
            logger.warning(f"Failed to publish IPNS record to DHT: {e}")
            raise RoutingError(f"Failed to publish IPNS record: {e}") from e

        return self.host.id().to_base58()  # type: ignore[union-attr]

    async def export_car(self, cid: Any, output_path: str) -> None:
        """Export a DAG to a CAR file. *cid* may be a string or IPLDNode."""
        self._ensure_started()
        from py_ipfs_lite.car import export_car as _export_car

        await _export_car(self, _to_cid_str(cid), output_path)

    async def import_car(self, input_path: str, strict: bool = False) -> list[str]:
        """Import a CAR file. Returns a list of root CID strings."""
        self._ensure_started()
        from py_ipfs_lite.car import import_car as _import_car

        return await _import_car(self, input_path, strict=strict)

    def session(self) -> PeerSession:
        """
        Return a session-based NodeGetter with its own BitswapSession.

        Each call creates a fresh BitswapSession that shares block request
        state internally, enabling efficient deduplication of concurrent
        WANT messages for the same CID within the session scope.
        """
        return PeerSession(self._exchange)

    async def has_block(self, cid: Any) -> bool:
        """
        Check whether a block is available locally.

        Accepts a CID string, a ``CIDObject``, an ``IPLDNode``, or any
        object with a ``cid_bytes`` attribute.
        """
        self._ensure_started()
        if isinstance(cid, str):
            parsed = parse_cid(cid)
            return await self.blockstore.has(cid_to_bytes(parsed))  # type: ignore[union-attr]
        if isinstance(cid, IPLDNode):
            return await self.blockstore.has(cid.cid_bytes)  # type: ignore[union-attr]
        if hasattr(cid, "cid_bytes"):
            return await self.blockstore.has(cid.cid_bytes)  # type: ignore[union-attr]
        if hasattr(cid, "buffer"):
            return await self.blockstore.has(cid.buffer)  # type: ignore[union-attr]
        cid_bytes = cid_to_bytes(cid) if not isinstance(cid, bytes) else cid
        return await self.blockstore.has(cid_bytes)  # type: ignore[union-attr]

    def block_store(self) -> Any:
        return self.blockstore

    def exchange(self) -> Any:
        return self._exchange

    def block_service(self) -> Any:
        return self.dag_service

    def _ensure_started(self) -> None:
        if not self._started:
            raise PeerNotStartedError("Peer not started. Call start() first.")
