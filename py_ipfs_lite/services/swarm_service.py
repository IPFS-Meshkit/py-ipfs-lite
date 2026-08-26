"""
The only module allowed to reach into peer.host._host / peer.routing._routing.
When those internals shift (as they have before), this is the one place to fix.
"""

import logging
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

logger = logging.getLogger("py_ipfs_lite.swarm_service")

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
        if meta is None:
            import sys
            print(
                f"DIAG-ZOMBIE-PRINT: peer={pid_str} conns_in_swarm={len(conns)} conn_meta_count={len(conn_meta_map)}",
                file=sys.stderr,
                flush=True,
            )
        elif meta.get("connected_at") is None:
            import sys
            print(
                f"DIAG-ZOMBIE2-PRINT: peer={pid_str} meta_keys={list(meta.keys())}",
                file=sys.stderr,
                flush=True,
            )
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

        stream_stats = tracker.peer_stream_stats.get(pid_str) if tracker else None
        streams_outbound = (
            getattr(stream_stats, "current_open_outbound", 0) if stream_stats else 0
        )
        streams_inbound = (
            getattr(stream_stats, "current_open_inbound", 0) if stream_stats else 0
        )
        streams_total = getattr(stream_stats, "current_open", 0) if stream_stats else 0

        result.append(
            {
                "peer": pid_str,
                "addrs": addrs,
                "connected_at": connected_at,
                "duration_seconds": duration_secs,
                "transport": transport,
                "direction": direction,
                "age_tier": age_tier,
                "streams_total": streams_total,
                "streams_outbound": streams_outbound,
                "streams_inbound": streams_inbound,
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

    # Parse and validate the multiaddr so invalid input returns a clean 400
    # instead of an unhandled 500.
    try:
        info = info_from_p2p_addr(Multiaddr(addr))
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Invalid peer address: {addr}"
        ) from e
    await peer.host.connect(info)


async def disconnect_peer(peer: Peer, peer_id_str: str) -> None:
    from py_ipfs_lite.exceptions import PeerNotStartedError

    if not peer.host:
        raise PeerNotStartedError("Peer is not initialized")

    from libp2p.peer.id import ID
    from libp2p.peer.peerinfo import info_from_p2p_addr
    from multiaddr import Multiaddr

    # Accept either a bare peer ID or a full multiaddr (matching Kubo).
    if peer_id_str.startswith("/"):
        try:
            info = info_from_p2p_addr(Multiaddr(peer_id_str))
            peer_id = info.peer_id
        except Exception as e:
            raise HTTPException(
                status_code=400, detail=f"Invalid peer address: {peer_id_str}"
            ) from e
    else:
        try:
            peer_id = ID.from_base58(peer_id_str)
        except Exception as e:
            raise HTTPException(
                status_code=400, detail=f"Invalid peer ID: {peer_id_str}"
            ) from e

    await peer.host.disconnect(peer_id)


def _resolve_peer_id(peer: Peer, peer_id_str: str) -> Any:
    """Resolve a bare peer-ID string or multiaddr into a libp2p ID."""
    from libp2p.peer.id import ID
    from libp2p.peer.peerinfo import info_from_p2p_addr
    from multiaddr import Multiaddr

    if peer_id_str.startswith("/"):
        try:
            return info_from_p2p_addr(Multiaddr(peer_id_str)).peer_id
        except Exception as e:
            raise HTTPException(
                status_code=400, detail=f"Invalid peer address: {peer_id_str}"
            ) from e
    try:
        return ID.from_base58(peer_id_str)
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Invalid peer ID: {peer_id_str}"
        ) from e


def _tag_store_for(peer: Peer) -> Any:
    """Return the swarm's TagStore (connection-manager protection registry)."""
    import typing

    raw_host = typing.cast(typing.Any, getattr(peer.host, "_host", peer.host))
    network = raw_host.get_network()
    tag_store = getattr(network, "tag_store", None)
    if tag_store is None:
        raise HTTPException(
            status_code=503, detail="Tag store not available on this swarm"
        )
    return tag_store


async def protect_peer(peer: Peer, peer_id_str: str, tag: str) -> dict[str, Any]:
    """Mark a peer as protected so the connection pruner never evicts it."""
    from py_ipfs_lite.exceptions import PeerNotStartedError

    if not peer.host:
        raise PeerNotStartedError("Peer is not initialized")

    peer_id = _resolve_peer_id(peer, peer_id_str)
    tag_store = _tag_store_for(peer)
    tag_store.protect(peer_id, tag)
    return {"Peers": [str(peer_id)], "Tag": tag}


async def unprotect_peer(peer: Peer, peer_id_str: str, tag: str) -> dict[str, Any]:
    """Remove protection from a previously protected peer."""
    from py_ipfs_lite.exceptions import PeerNotStartedError

    if not peer.host:
        raise PeerNotStartedError("Peer is not initialized")

    peer_id = _resolve_peer_id(peer, peer_id_str)
    tag_store = _tag_store_for(peer)
    removed = tag_store.unprotect(peer_id, tag)
    return {"Peers": [str(peer_id)], "Tag": tag, "Removed": bool(removed)}


async def list_protected_peers(peer: Peer) -> list[dict[str, Any]]:
    """List all protected peers with their protection tags and tag values."""
    if peer.host is None:
        return []
    tag_store = _tag_store_for(peer)
    # _protected maps ID -> set of protection tags (private but stable; this
    # module is the sanctioned place to reach into libp2p internals).
    protected_map = getattr(tag_store, "_protected", {})
    result = []
    for pid in tag_store.get_protected_peers():
        info = tag_store.get_tag_info(pid)
        result.append(
            {
                "peer": str(pid),
                "protections": sorted(protected_map.get(pid, set())),
                "tags": dict(info.tags) if info else {},
                "total_value": info.value if info else 0,
            }
        )
    return result


async def list_peer_tags(peer: Peer) -> dict[str, Any]:
    """List every tagged peer with its full tag map, values and protections."""
    if peer.host is None:
        return []
    tag_store = _tag_store_for(peer)
    protected_map = getattr(tag_store, "_protected", {})
    result = []
    for pid in tag_store.get_all_peers():
        info = tag_store.get_tag_info(pid)
        if info is None:
            continue
        result.append(
            {
                "peer": str(pid),
                "tags": dict(info.tags),
                "total_value": info.value,
                "protected": sorted(protected_map.get(pid, set())),
            }
        )
    # Highest-value peers first (matches conn-manager scoring semantics).
    result.sort(key=lambda p: p["total_value"], reverse=True)
    return {"count": len(result), "peers": result}


async def set_peer_tag(
    peer: Peer, peer_id_str: str, tag: str, value: int
) -> dict[str, Any]:
    """Assign an arbitrary tag with an integer weight to a peer."""
    from py_ipfs_lite.exceptions import PeerNotStartedError

    if not peer.host:
        raise PeerNotStartedError("Peer is not initialized")
    if not tag or len(tag) > 256:
        raise HTTPException(status_code=400, detail="Invalid tag name")

    peer_id = _resolve_peer_id(peer, peer_id_str)
    tag_store = _tag_store_for(peer)
    tag_store.tag_peer(peer_id, tag, value)
    return {
        "Peer": str(peer_id),
        "Tag": tag,
        "Value": value,
    }


async def remove_peer_tag(peer: Peer, peer_id_str: str, tag: str) -> dict[str, Any]:
    """Remove a previously assigned tag from a peer."""
    from py_ipfs_lite.exceptions import PeerNotStartedError

    if not peer.host:
        raise PeerNotStartedError("Peer is not initialized")

    peer_id = _resolve_peer_id(peer, peer_id_str)
    tag_store = _tag_store_for(peer)

    info = tag_store.get_tag_info(peer_id)
    if info is None or tag not in info.tags:
        raise HTTPException(
            status_code=404,
            detail=f"Peer {peer_id_str} has no tag {tag!r}",
        )
    old_value = info.tags[tag]
    tag_store.untag_peer(peer_id, tag)
    return {"Peer": str(peer_id), "Tag": tag, "RemovedValue": old_value}
