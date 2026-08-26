"""Background maintenance loops."""

import logging
import time
from typing import (
    Any,
)

import trio

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


class MaintenanceMixin:
    """Mixed into :class:`py_ipfs_lite.peer.core.Peer`."""

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

    async def _zombie_connection_cleanup_loop(self) -> None:
        """Detect and close zombie connections that have no tracker metadata.

        A zombie connection sits in ``network.connections`` but was never
        properly registered with the connection tracker (``_conn_meta`` has
        no entry for it).  This happens when:
        1. The ``connected()`` notifee was never called (connection failed
           partway through setup).
        2. The ``disconnected()`` notifee already removed the metadata but
           the connection object still lingers in the swarm dict.

        These zombies show up in the API as ``duration_seconds: 0`` with
        ``connected_at: null``.  They waste memory and confuse the
        auto-connector (it sees them as "connected" and skips re-dialing
        the peer).
        """
        raw_host = getattr(self.host, "_host", self.host)
        if raw_host is None:
            return

        ZOMBIE_THRESHOLD = 120.0  # seconds without metadata before closing

        while True:
            await trio.sleep(30.0)  # sweep every 30 seconds
            try:
                network = raw_host.get_network()
                if not hasattr(network, "connections"):
                    continue

                tracker = self.connection_tracker
                if tracker is None:
                    continue

                # Build a set of peer_ids that have tracker metadata
                tracked_peer_ids: set[str] = set()
                for meta in tracker._conn_meta.values():
                    p_id = meta.get("peer_id")
                    if p_id and p_id != "unknown":
                        tracked_peer_ids.add(p_id)

                now = time.monotonic()
                zombies_closed = 0

                for peer_id_obj, conns in list(network.connections.items()):
                    try:
                        pid_str = peer_id_obj.to_base58()
                    except Exception:
                        pid_str = str(peer_id_obj)

                    # Skip if this peer has tracker metadata
                    if pid_str in tracked_peer_ids:
                        continue

                    conns_list = conns if isinstance(conns, list) else [conns]
                    for conn in conns_list:
                        # Check connection age — only close if it's been
                        # alive long enough to be considered a zombie
                        created_at = getattr(conn, "_created_at", None)
                        if created_at is not None:
                            age = now - created_at
                            if age < ZOMBIE_THRESHOLD:
                                continue  # Too fresh, give it time

                        # This connection has no tracker metadata and is
                        # old enough to be a zombie — close it
                        logger.warning(
                            "Closing zombie connection to %s "
                            "(no tracker metadata, age=%.0fs)",
                            pid_str,
                            age if created_at is not None else -1,
                        )
                        try:
                            await raw_host.disconnect(peer_id_obj)
                            zombies_closed += 1
                        except Exception as e:
                            logger.debug(
                                "Error closing zombie connection to %s: %s",
                                pid_str,
                                e,
                            )

                if zombies_closed > 0:
                    logger.info(
                        "Zombie connection cleanup: closed %d zombie connections",
                        zombies_closed,
                    )
            except Exception as e:
                logger.debug(f"Zombie connection cleanup error: {e}")
