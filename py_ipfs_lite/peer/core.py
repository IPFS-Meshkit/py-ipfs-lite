"""The :class:`Peer` facade composing all capability mixins."""

import collections
import contextlib
import logging
from typing import Any

from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.crypto.keys import KeyPair

from py_ipfs_lite.config import Config
from py_ipfs_lite.connection_tracker import ConnectionStatsTracker
from py_ipfs_lite.exceptions import PeerNotStartedError
from py_ipfs_lite.interfaces import (
    BlockStore,
    DagService,
    Datastore,
    Exchange,
    Host,
    Routing,
)
from py_ipfs_lite.peer.ipld import RWLock
from py_ipfs_lite.peer.state import PeerState
from py_ipfs_lite.pin import PinStore
from py_ipfs_lite.reprovider import Reprovider

from ._content import ContentMixin
from ._hostfactory import HostFactoryMixin
from ._lifecycle import LifecycleMixin
from ._maintenance import MaintenanceMixin
from ._naming import NamingMixin
from ._pubsub import PubsubMixin

logger = logging.getLogger(__name__)


class Peer(
    ContentMixin,
    NamingMixin,
    PubsubMixin,
    MaintenanceMixin,
    LifecycleMixin,
    HostFactoryMixin,
):
    """
    High-level IPFS-lite peer.

    Capability groups live in dedicated mixin modules; this class owns the
    constructor and shared attribute surface.
    """

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

    def block_store(self) -> Any:
        return self.blockstore

    def exchange(self) -> Any:
        return self._exchange

    def block_service(self) -> Any:
        return self.dag_service

    def _ensure_started(self) -> None:
        if not self._started:
            raise PeerNotStartedError("Peer not started. Call start() first.")
