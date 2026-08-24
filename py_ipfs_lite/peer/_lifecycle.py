"""Peer lifecycle: start/close/bootstrap and routing warm-up."""

import logging
import os
from typing import (
    TYPE_CHECKING,
    Any,
)

import trio
from libp2p.discovery.bootstrap.bootstrap import BootstrapDiscovery
from multiaddr import Multiaddr

from py_ipfs_lite.peer.setup import attach_libp2p_metrics
from py_ipfs_lite.peer.state import PeerState

if TYPE_CHECKING:
    from py_ipfs_lite.peer.core import Peer

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


class LifecycleMixin:
    """Mixed into :class:`py_ipfs_lite.peer.core.Peer`."""

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
