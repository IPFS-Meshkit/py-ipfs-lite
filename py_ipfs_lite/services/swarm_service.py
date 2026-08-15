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
    import typing

    raw_host = typing.cast(typing.Any, getattr(peer.host, "_host", peer.host))
    network = raw_host.get_network()
    peerstore = raw_host.get_peerstore()
    result = []
    seen: set[str] = set()

    # Primary source: swarm connections dict.
    # NOTE: we intentionally do NOT check `is_closed` here.
    # The swarm removes closed SwarmConns from this dict (swarm.py L2316-2318),
    # but there is a brief trio async window between `event_closed.set()` and
    # the cleanup task running. Checking `is_closed` during this window gives
    # false-0 results. Trusting the dict is accurate enough.
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
        result.append({"peer": pid_str, "addrs": addrs})

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
