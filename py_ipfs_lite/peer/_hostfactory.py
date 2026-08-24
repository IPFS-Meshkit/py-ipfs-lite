"""Construction of the libp2p host, DHT, blockstore and exchange."""

import logging
import os
from typing import (
    Any,
)

import trio
from libp2p import new_host
from libp2p.bitswap import BitswapClient, MemoryBlockStore
from libp2p.bitswap.block_store import FilesystemBlockStore
from libp2p.bitswap.dag import MerkleDag
from libp2p.crypto.x25519 import create_new_key_pair as create_new_x25519_key_pair
from libp2p.kad_dht.kad_dht import DHTMode, KadDHT
from libp2p.security.noise.transport import Transport as NoiseTransport
from multiaddr import Multiaddr

from py_ipfs_lite.connection_tracker import ConnectionStatsTracker
from py_ipfs_lite.interfaces import (
    BlockStoreAdapter,
    HostAdapter,
    RoutingAdapter,
)
from py_ipfs_lite.metrics import (
    IPFS_BITSWAP_BYTES_RECEIVED_TOTAL,
    MetricsBlockStore,
)
from py_ipfs_lite.peer.setup import attach_libp2p_metrics

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

logger = logging.getLogger("py_ipfs_lite.peer")


class HostFactoryMixin:
    """Mixed into :class:`py_ipfs_lite.peer.core.Peer`."""

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
