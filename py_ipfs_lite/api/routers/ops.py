"""Operations & debugging routes for the py-ipfs-lite HTTP API."""

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from py_ipfs_lite.peer import Peer

router = APIRouter()


@router.post("/api/v0/debug/connection-stats")
@router.get("/api/v0/debug/connection-stats")
@router.get("/debug/conns")
async def debug_conns(request: Request) -> Any:
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    total = await swarm_service.count_connections(peer)
    return JSONResponse(content={"total_connections": total})


@router.get("/metrics")
@router.get("/debug/metrics/prometheus")
async def metrics(request: Request) -> Any:
    """Expose Prometheus metrics."""
    peer: Peer = getattr(request.app.state, "peer", None)
    from py_ipfs_lite.metrics import update_live_metrics

    update_live_metrics(peer)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.post("/api/v0/debug/peerstore")
@router.get("/api/v0/debug/peerstore")
async def debug_peerstore(request: Request) -> Any:
    """Return peer store information for debugging."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    res = await swarm_service.list_peerstore_peers(peer)
    return JSONResponse(content={"count": res.count, "peers": res.peers})


@router.post("/api/v0/debug/routing_table")
@router.get("/api/v0/debug/routing_table")
async def debug_routing_table(request: Request) -> Any:
    """Return routing table information for debugging."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    res = await swarm_service.list_routing_table_peers(peer)
    return JSONResponse(content={"count": res.count, "peers": res.peers})


@router.get("/api/v0/debug/memory")
async def debug_memory(request: Request) -> Any:
    """Detailed live memory and object introspection endpoint."""
    import gc
    import sys
    from collections import Counter

    gc.collect()

    peer: Peer = request.app.state.peer
    counts: Counter[str] = Counter()
    sizes: Counter[str] = Counter()
    all_objs = gc.get_objects()
    for obj in all_objs:
        t_name = f"{type(obj).__module__}.{type(obj).__name__}"
        counts[t_name] += 1
        try:
            sizes[t_name] += sys.getsizeof(obj)
        except Exception:
            pass

    top_by_count = [
        {"type": t, "count": c, "size_mb": round(sizes[t] / (1024 * 1024), 2)}
        for t, c in counts.most_common(50)
    ]
    top_by_size = [
        {"type": t, "count": counts[t], "size_mb": round(s / (1024 * 1024), 2)}
        for t, s in sorted(sizes.items(), key=lambda x: x[1], reverse=True)[:50]
    ]

    # Subsystem inspect
    subsystems: dict[str, Any] = {}
    try:
        raw_host = getattr(peer.host, "_host", peer.host) if peer.host else None
        if raw_host is not None:
            raw_swarm = raw_host.get_network()  # type: ignore
            subsystems["swarm_connections_len"] = (
                len(raw_swarm.connections)
                if hasattr(raw_swarm, "connections")
                else None
            )
            subsystems["swarm_listeners_len"] = (
                len(raw_swarm.listeners) if hasattr(raw_swarm, "listeners") else None
            )
            if hasattr(raw_swarm, "transport_manager"):
                tm = raw_swarm.transport_manager
                subsystems["transports"] = [
                    type(t).__name__ for t in getattr(tm, "transports", {}).values()
                ]
    except Exception as e:
        subsystems["swarm_err"] = str(e)

    try:
        raw_host = getattr(peer.host, "_host", peer.host) if peer.host else None
        if raw_host is not None and hasattr(raw_host, "get_peerstore"):
            ps = raw_host.get_peerstore()
            subsystems["peerstore_peers_count"] = len(ps.peer_ids())
            if hasattr(ps, "peer_data_map"):
                subsystems["peer_data_map_len"] = len(ps.peer_data_map)
            if hasattr(ps, "peer_record_map"):
                subsystems["peer_record_map_len"] = len(ps.peer_record_map)
    except Exception as e:
        subsystems["peerstore_err"] = str(e)

    # Introspect Trio tasks via GC
    task_counts: Counter[str] = Counter()
    try:
        task_objs = [o for o in all_objs if type(o).__name__ == "Task"]
        for t in task_objs:
            if hasattr(t, "coro") and hasattr(t.coro, "cr_code"):
                co = t.coro.cr_code
                task_counts[
                    f"{co.co_filename.split('/')[-1]}:{co.co_name}:{co.co_firstlineno}"
                ] += 1
            else:
                task_counts[getattr(t, "name", "unknown")] += 1
    except Exception:
        pass

    # Introspect referrers of ServerQuicConnection
    server_conns = [o for o in all_objs if type(o).__name__ == "ServerQuicConnection"]
    server_conn_referrers = []
    try:
        if server_conns:
            for r in gc.get_referrers(server_conns[0])[:5]:
                try:
                    if isinstance(r, dict):
                        server_conn_referrers.append(f"dict(keys={list(r.keys())[:5]})")
                    else:
                        server_conn_referrers.append(
                            f"{type(r).__name__}: {str(r)[:80]}"
                        )
                except Exception:
                    pass
    except Exception:
        pass

    # Test malloc_trim
    rss_before_trim = 0.0
    rss_after_trim = 0.0
    try:
        import ctypes
        import os

        import psutil

        proc = psutil.Process(os.getpid())
        rss_before_trim = proc.memory_info().rss / (1024 * 1024)
        libc = ctypes.CDLL("libc.so.6")
        libc.malloc_trim(0)
        rss_after_trim = proc.memory_info().rss / (1024 * 1024)
    except Exception:
        pass

    return JSONResponse(
        content={
            "total_objects": len(all_objs),
            "rss_before_trim_mb": round(rss_before_trim, 2),
            "rss_after_trim_mb": round(rss_after_trim, 2),
            "server_conns_count": len(server_conns),
            "server_conn_referrers": server_conn_referrers,
            "top_tasks": [
                {"task": k, "count": int(v)} for k, v in task_counts.most_common(20)
            ],
            "top_by_count": top_by_count,
            "top_by_size": top_by_size,
            "subsystems": subsystems,
        }
    )
