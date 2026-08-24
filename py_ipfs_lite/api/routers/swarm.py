"""Swarm & connection manager routes for the py-ipfs-lite HTTP API."""

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from py_ipfs_lite.peer import Peer

router = APIRouter()


@router.post("/api/v0/swarm/connection_stats")
@router.get("/api/v0/swarm/connection_stats")
async def swarm_connection_stats(request: Request) -> Any:
    peer: Peer = request.app.state.peer
    if not hasattr(peer, "connection_tracker") or peer.connection_tracker is None:
        raise HTTPException(
            status_code=503, detail="Connection tracker not initialized"
        )

    raw_host = getattr(peer.host, "_host", peer.host)
    identified_peers = getattr(raw_host, "_identified_peers", {})

    from libp2p.peer.id import ID

    stats = []
    for s in peer.connection_tracker.stats.values():
        dump = s.model_dump()
        try:
            peer_id_obj = ID.from_base58(s.peer_id)
            if peer_id_obj in identified_peers:
                dump["identify_completed"] = True
                dump["identify_completed_at"] = identified_peers[peer_id_obj]
        except Exception:
            pass
        stats.append(dump)

    return JSONResponse(content={"Stats": stats})


@router.post("/api/v0/swarm/stream_stats")
@router.get("/api/v0/swarm/stream_stats")
async def swarm_stream_stats(request: Request) -> Any:
    """
    Report stream lifecycle statistics for resource-leak monitoring.

    Returns per-peer open/closed stream counts, current open streams, and
    the average stream lifetime. Streams are flagged as suspected leaks by
    the background monitor when they outlive the configured threshold.
    """
    peer: Peer = request.app.state.peer
    if not hasattr(peer, "connection_tracker") or peer.connection_tracker is None:
        raise HTTPException(
            status_code=503, detail="Connection tracker not initialized"
        )
    return JSONResponse(
        content=peer.connection_tracker.stream_stats_snapshot(
            leak_threshold_seconds=peer.config.stream_leak_threshold_seconds
        )
    )


@router.get("/api/v0/swarm/connection_metrics")
async def debug_connection_stats(request: Request) -> Any:
    """
    Report live connection lifecycle metrics tracked directly via INotifee.

    Returns total connected events, total disconnected events, current active
    connections, and a rolling log of recent disconnections with exact durations.
    """
    peer: Peer = request.app.state.peer
    if not hasattr(peer, "connection_tracker") or peer.connection_tracker is None:
        raise HTTPException(
            status_code=503, detail="Connection tracker not initialized"
        )
    return JSONResponse(content=peer.connection_tracker.connection_stats_snapshot())


@router.post("/api/v0/swarm/peers")
@router.get("/api/v0/swarm/peers")
async def swarm_peers(request: Request) -> Any:
    """List peers with open connections."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    peers = await swarm_service.list_connected_peers(peer)
    return JSONResponse(content={"count": peers.count, "peers": peers.peers})


@router.post("/api/v0/swarm/connect")
async def swarm_connect(
    request: Request,
    arg: str = Query(..., description="The multiaddr of the peer to connect to"),
) -> Any:
    """Connect to a peer."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    await swarm_service.connect_peer(peer, arg)
    return JSONResponse(content={"Strings": [f"connect {arg} success"]})


@router.post("/api/v0/swarm/disconnect")
async def swarm_disconnect(
    request: Request,
    arg: str = Query(..., description="The peer ID to disconnect from"),
) -> Any:
    """Disconnect from a peer."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    await swarm_service.disconnect_peer(peer, arg)
    return JSONResponse(content={"Strings": [f"disconnect {arg} success"]})


@router.post("/api/v0/swarm/protect")
async def swarm_protect(
    request: Request,
    arg: str = Query(
        ..., description="The peer ID (or multiaddr) to protect from pruning"
    ),
    tag: str = Query(
        default="keep-alive",
        min_length=1,
        description="Protection tag (scopes ownership of the protection)",
    ),
) -> Any:
    """Protect a peer's connection from being pruned by the connection manager."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    result = await swarm_service.protect_peer(peer, arg, tag)
    return JSONResponse(content=result)


@router.post("/api/v0/swarm/unprotect")
async def swarm_unprotect(
    request: Request,
    arg: str = Query(..., description="The peer ID (or multiaddr) to unprotect"),
    tag: str = Query(
        default="keep-alive",
        min_length=1,
        description="Protection tag previously used with /swarm/protect",
    ),
) -> Any:
    """Remove a previously applied peer protection."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    result = await swarm_service.unprotect_peer(peer, arg, tag)
    return JSONResponse(content=result)


@router.get("/api/v0/swarm/protection")
async def swarm_protection(request: Request) -> Any:
    """List all protected peers with their protection tags and values."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    peers = await swarm_service.list_protected_peers(peer)
    return JSONResponse(content={"count": len(peers), "peers": peers})


@router.get("/api/v0/swarm/tags")
async def swarm_tags_list(request: Request) -> Any:
    """List every tagged peer with its tag map, total value and protections."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    result = await swarm_service.list_peer_tags(peer)
    return JSONResponse(content=result)


@router.post("/api/v0/swarm/tags")
async def swarm_tags_set(
    request: Request,
    arg: str = Query(..., description="The peer ID (or multiaddr) to tag"),
    tag: str = Query(..., min_length=1, description="Tag name"),
    value: int = Query(
        ...,
        description="Integer weight for the tag (negative values demote the peer)",
    ),
) -> Any:
    """Assign an arbitrary connection-manager tag with an integer weight."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    return JSONResponse(content=await swarm_service.set_peer_tag(peer, arg, tag, value))


@router.delete("/api/v0/swarm/tags")
@router.post("/api/v0/swarm/tags/remove")
async def swarm_tags_remove(
    request: Request,
    arg: str = Query(..., description="The peer ID (or multiaddr) to untag"),
    tag: str = Query(..., min_length=1, description="Tag name to remove"),
) -> Any:
    """Remove a previously assigned tag from a peer."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    return JSONResponse(content=await swarm_service.remove_peer_tag(peer, arg, tag))
