"""libp2p host bootstrap helpers and metrics attachment."""

from typing import Any

from libp2p import new_host
from libp2p.bitswap import MemoryBlockStore
from libp2p.crypto.x25519 import create_new_key_pair as create_new_x25519_key_pair
from libp2p.kad_dht.kad_dht import DHTMode, KadDHT
from libp2p.security.noise.transport import Transport as NoiseTransport
from multiaddr import Multiaddr

from py_ipfs_lite.interfaces import (
    BlockStoreAdapter,
    HostAdapter,
    RoutingAdapter,
)


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
