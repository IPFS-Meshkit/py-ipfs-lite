"""
The only module allowed to reach into peer.host._host / peer.routing._routing.
When those internals shift (as they have before), this is the one place to fix.
"""

from dataclasses import dataclass

from py_ipfs_lite.peer import Peer


@dataclass
class SwarmPeers:
    count: int
    peers: list[str]


async def list_connected_peers(peer: Peer) -> SwarmPeers:
    if peer.host is None:
        return SwarmPeers(count=0, peers=[])
    import typing

    raw_host = typing.cast(typing.Any, getattr(peer.host, "_host", peer.host))
    network = raw_host.get_network()
    peers = [p.to_base58() for p in network.connections.keys()]
    return SwarmPeers(count=len(peers), peers=peers)


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
