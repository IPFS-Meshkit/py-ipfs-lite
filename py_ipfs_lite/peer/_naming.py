"""IPNS naming and CAR import/export."""

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

from py_ipfs_lite.peer.ipld import _to_cid_str


class NamingMixin:
    """Mixed into :class:`py_ipfs_lite.peer.core.Peer`."""

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

        # Publishing walks the DHT (k-closest lookup + signed-record STORE
        # RPCs) which reliably takes longer than the 30 s default budget on
        # loaded networks — mirror the dht/provide endpoint's 90 s allowance.
        publish_timeout = max(t_val, 90.0)
        try:
            with trio.fail_after(publish_timeout):
                await ipns_publish(
                    self.routing,
                    self._host_key.private_key,
                    self.host.id(),  # type: ignore[union-attr]
                    value,
                    sequence,
                    lifetime_hours,
                )
        except trio.TooSlowError:
            logger.warning(
                "IPNS publish timed out after %.0fs — local record is stored "
                "but may not have propagated to remote peers",
                publish_timeout,
            )
            raise RoutingError(
                f"Failed to publish IPNS record: timed out after "
                f"{publish_timeout:.0f}s (local record is stored; retry or "
                f"use a longer timeout)"
            ) from None
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
