"""PubSub/GossipSub management: init, discovery, subscribe/publish."""

import base64
import collections
import json
import logging
import os
import time
from typing import (
    Any,
)

import trio

from py_ipfs_lite.peer.state import PeerState

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


class PubsubMixin:
    """Mixed into :class:`py_ipfs_lite.peer.core.Peer`."""

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
        # the discovery loop's confirmation window.  Skipped entirely when
        # auto-join is disabled (min_peers == 0).
        if (
            self.config.pubsub_persist_topics
            and self.config.pubsub_auto_join_min_peers > 0
        ):
            for topic in self._load_auto_pubsub_topics():
                if topic not in self._pubsub_subscriptions:
                    logger.info(f"Rejoining persisted pubsub topic: {topic}")
                    await self.subscribe_pubsub_topic(topic)
                    self._auto_joined_topics.add(topic)

        # Adaptive topic join: watch peer_topics and subscribe to shared topics.
        if self.config.pubsub_auto_join_min_peers > 0:
            self._nursery.start_soon(self._pubsub_topic_discovery_loop)

    def _protected_pubsub_topics(self) -> set[str]:
        """Topics never subject to auto-leave (configured + our own IPNS)."""
        return set(self.config.pubsub_topics) | {
            getattr(self, "_own_ipns_topic", "")
        } - {""}

    def _auto_pubsub_path(self) -> str | None:
        """File path for persisting auto-joined topics, or None."""
        if (
            not self.config.pubsub_persist_topics
            or self.config.blockstore_type == "memory"
            or not self.config.blockstore_path
        ):
            return None
        base = os.path.dirname(self.config.blockstore_path) or "."
        return os.path.join(base, "auto_pubsub_topics.json")

    def _save_auto_pubsub_topics(self) -> None:
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
