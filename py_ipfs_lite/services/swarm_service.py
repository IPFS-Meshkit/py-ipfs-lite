"""
The only module allowed to reach into peer.host._host / peer.routing._routing.
When those internals shift (as they have before), this is the one place to fix.
"""

from dataclasses import dataclass
from typing import Any

from py_ipfs_lite.peer import Peer


@dataclass
class SwarmPeers:
    count: int
    peers: list[dict[str, Any]]


async def list_connected_peers(peer: Peer) -> SwarmPeers:
    if peer.host is None:
        return SwarmPeers(count=0, peers=[])
    import time
    import typing

    raw_host = typing.cast(typing.Any, getattr(peer.host, "_host", peer.host))
    network = raw_host.get_network()
    peerstore = raw_host.get_peerstore()
    now_mono = time.monotonic()
    result = []
    seen: set[str] = set()

    # Build lookup of peer metadata from connection tracker if available
    tracker = getattr(peer, "connection_tracker", None)
    conn_meta_map: dict[str, Any] = {}
    if tracker is not None:
        for meta in getattr(tracker, "_conn_meta", {}).values():
            p_id = meta.get("peer_id")
            if p_id and p_id != "unknown" and "start_mono" in meta:
                if (
                    p_id not in conn_meta_map
                    or meta["start_mono"] < conn_meta_map[p_id]["start_mono"]
                ):
                    conn_meta_map[p_id] = meta

    for peer_id, conns in list(network.connections.items()):
        try:
            pid_str = peer_id.to_base58()
        except Exception:
            pid_str = str(peer_id)
        if pid_str in seen:
            continue
        seen.add(pid_str)
        try:
            addrs = [str(a) for a in peerstore.addrs(peer_id)]
        except Exception:
            addrs = []

        meta = conn_meta_map.get(pid_str)
        duration_secs = (
            round(now_mono - meta["start_mono"], 1)
            if meta and "start_mono" in meta
            else 0.0
        )
        connected_at = meta.get("connected_at") if meta else None
        transport = meta.get("transport", "unknown") if meta else "unknown"
        direction = meta.get("direction", "unknown") if meta else "unknown"

        if duration_secs >= 1800.0:
            age_tier = "over_30m"
        elif duration_secs >= 600.0:
            age_tier = "10m_to_30m"
        elif duration_secs >= 300.0:
            age_tier = "5m_to_10m"
        elif duration_secs >= 120.0:
            age_tier = "2m_to_5m"
        else:
            age_tier = "under_2m"

        result.append(
            {
                "peer": pid_str,
                "addrs": addrs,
                "connected_at": connected_at,
                "duration_seconds": duration_secs,
                "transport": transport,
                "direction": direction,
                "age_tier": age_tier,
            }
        )

    # Sort peers by duration descending (longest-lived stable peers first)
    result.sort(key=lambda p: p.get("duration_seconds", 0.0), reverse=True)
    return SwarmPeers(count=len(result), peers=result)


async def count_connections(peer: Peer) -> int:
    if peer.host is None:
        return 0
    import typing

    raw_host = typing.cast(typing.Any, getattr(peer.host, "_host", peer.host))
    network = raw_host.get_network()
    total = 0
    for conns in network.connections.values():
        total += len(conns) if isinstance(conns, list) else 1
    return total


async def list_peerstore_peers(peer: Peer) -> SwarmPeers:
    if peer.host is None:
        return SwarmPeers(count=0, peers=[])
    import typing

    raw_host = typing.cast(typing.Any, getattr(peer.host, "_host", peer.host))
    peerstore = raw_host.get_peerstore()
    peers = [p.to_base58() for p in peerstore.peer_ids()]
    return SwarmPeers(count=len(peers), peers=peers)


async def list_routing_table_peers(peer: Peer) -> SwarmPeers:
    if peer.host is None:
        return SwarmPeers(count=0, peers=[])
    if not peer.routing or not hasattr(peer.routing, "_routing"):
        return SwarmPeers(count=0, peers=[])
    routing_table = peer.routing._routing.routing_table
    peers = [p.to_base58() for p in routing_table.get_peer_ids()]
    return SwarmPeers(count=len(peers), peers=peers)


async def connect_peer(peer: Peer, addr: str) -> None:
    from py_ipfs_lite.exceptions import PeerNotStartedError

    if not peer.host:
        raise PeerNotStartedError("Peer is not initialized")
    from libp2p.peer.peerinfo import info_from_p2p_addr
    from multiaddr import Multiaddr

    maddr = Multiaddr(addr)
    info = info_from_p2p_addr(maddr)
    await peer.host.connect(info)


async def disconnect_peer(peer: Peer, peer_id_str: str) -> None:
    from py_ipfs_lite.exceptions import PeerNotStartedError

    if not peer.host:
        raise PeerNotStartedError("Peer is not initialized")
    from libp2p.peer.id import ID

    peer_id = ID.from_base58(peer_id_str)
    await peer.host.disconnect(peer_id)
