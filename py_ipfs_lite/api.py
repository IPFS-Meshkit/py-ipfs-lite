import json
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import JSONResponse, Response, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from py_ipfs_lite.config import Config
from py_ipfs_lite.exceptions import (
    BlockNotFoundError,
    CarParseError,
    DagTooDeepError,
    InvalidCidError,
    IPFSLiteError,
    PayloadTooLargeError,
    PeerNotStartedError,
    PinError,
    PinNotFoundError,
    RoutingError,
)
from py_ipfs_lite.peer import Peer

logger = logging.getLogger("py_ipfs_lite.api")
# The actual instantiation of the peer depends on how the daemon is run,
# but we can set up a default initialization inside the lifespan if none exists.


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[Any, None]:
    # Check if a peer was already provided (e.g. injected during setup)
    peer = getattr(app.state, "peer", None)
    if not peer:
        # If not, initialize a default one for the daemon
        from libp2p.utils.address_validation import (
            find_free_port,
            get_available_interfaces,
        )

        config = Config()
        port = find_free_port()
        listen_addrs = get_available_interfaces(port)
        peer = Peer(config, listen_addrs=listen_addrs)
        app.state.peer = peer

    # Start the peer
    await peer.start()

    if not peer.config.offline:
        from py_ipfs_lite.cli import DEFAULT_BOOTSTRAP_PEERS

        # Start bootstrap in the background so we don't block the API server startup!
        if hasattr(peer, "_nursery") and peer._nursery:
            peer._nursery.start_soon(peer.bootstrap, DEFAULT_BOOTSTRAP_PEERS)
        else:
            await peer.bootstrap(DEFAULT_BOOTSTRAP_PEERS)

    import typing

    from py_ipfs_lite.interfaces import HostAdapter

    host = typing.cast(HostAdapter, peer.host)
    logger.info(f"Daemon P2P Peer ID: {host.id()}")
    for addr in host.addrs():
        logger.info(f"  P2P Listening on: {addr}")

    try:
        yield
    finally:
        # Clean up on shutdown
        await peer.close()


app = FastAPI(title="py-ipfs-lite HTTP API", lifespan=lifespan)


@app.exception_handler(IPFSLiteError)
async def ipfs_lite_exception_handler(request: Request, exc: IPFSLiteError) -> Any:
    status_code = 500
    if isinstance(exc, (BlockNotFoundError, PinNotFoundError, RoutingError)):
        status_code = 404
    elif isinstance(exc, PeerNotStartedError):
        status_code = 503
    elif isinstance(exc, PinError):
        status_code = 409
    elif isinstance(exc, (CarParseError, InvalidCidError, DagTooDeepError)):
        status_code = 400
    elif isinstance(exc, PayloadTooLargeError):
        status_code = 413
    return JSONResponse(status_code=status_code, content={"detail": str(exc)})


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception) -> Any:
    logger.error(f"Internal server error: {exc}")
    return JSONResponse(status_code=500, content={"detail": str(exc)})


@app.post("/api/v0/add")
async def add_file(request: Request, file: UploadFile = File(...)) -> Any:
    """Add a file to the node."""
    peer: Peer = request.app.state.peer

    if not hasattr(file, "read"):
        raise HTTPException(status_code=400, detail="Missing or invalid file")
    body = await file.read()

    async def chunks() -> AsyncGenerator[bytes, None]:
        if body:
            yield body

    from py_ipfs_lite.services import files_service

    result = await files_service.add_file_from_stream(
        peer, getattr(file, "filename", "unknown") or "unknown", chunks()
    )

    return JSONResponse(
        content={"Name": result.name, "Hash": result.cid, "Size": str(result.size)}
    )


@app.post("/api/v0/cat")
@app.get("/api/v0/cat")
async def cat_file(
    request: Request,
    arg: str = Query(..., description="The path to the IPFS object(s) to be outputted"),
) -> Any:
    """Fetch a file by its CID."""
    peer: Peer = request.app.state.peer

    from py_ipfs_lite.services import files_service

    stream = files_service.get_file_stream(peer, arg)
    return StreamingResponse(stream, media_type="application/octet-stream")


@app.post("/api/v0/dag/put")
async def dag_put(
    request: Request, store_codec: str = Query("dag-json", alias="store-codec")
) -> Any:
    """Store a generic DAG node."""
    peer: Peer = request.app.state.peer

    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" in content_type:
        form = await request.form()
        file_field = form.get("file") or form.get("data")
        if not file_field and form.keys():
            file_field = form[list(form.keys())[0]]

        if isinstance(file_field, UploadFile):
            body = await file_field.read()
        elif file_field is not None:
            body = str(file_field).encode("utf-8")
        else:
            body = b""
    else:
        body = await request.body()

    try:
        node_data = json.loads(body)
        from py_ipfs_lite.services import dag_service

        result = await dag_service.put_node(peer, node_data, codec=store_codec)
        return JSONResponse(content={"Cid": {"/": result.cid}})
    except (json.JSONDecodeError, RecursionError) as e:
        raise HTTPException(status_code=400, detail=f"{str(e)} | Body: {repr(body)}")


@app.post("/api/v0/dag/get")
@app.get("/api/v0/dag/get")
async def dag_get(
    request: Request, arg: str = Query(..., description="The object to get")
) -> Any:
    """Retrieve a generic DAG node."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import dag_encoding, dag_service

    result = await dag_service.get_node(peer, arg)

    accept = request.headers.get("accept", "")
    if result.cid_codec in ("dag-cbor", "cbor") and "application/cbor" in accept:
        import cbor2

        return Response(
            content=cbor2.dumps(result.node_data), media_type="application/cbor"
        )

    if result.cid_codec == "raw":
        return Response(content=result.node_data, media_type="application/octet-stream")

    encoded = json.dumps(result.node_data, cls=dag_encoding.DAGJSONEncoder)
    return Response(content=encoded, media_type="application/json")


@app.post("/api/v0/swarm/connection_stats")
@app.get("/api/v0/swarm/connection_stats")
async def swarm_connection_stats(request: Request) -> Any:
    peer: Peer = request.app.state.peer
    if not hasattr(peer, "connection_tracker") or peer.connection_tracker is None:
        raise HTTPException(
            status_code=503, detail="Connection tracker not initialized"
        )

    stats = list(peer.connection_tracker.stats.values())
    return JSONResponse(content={"Stats": [s.model_dump() for s in stats]})


@app.post("/api/v0/block/stat")
async def block_stat(
    request: Request,
    arg: str = Query(
        ..., description="The base58 multihash of an existing block to stat"
    ),
) -> Any:
    """Check if a block exists locally and get its size."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import block_service

    stat = await block_service.stat_block(peer, arg)
    return JSONResponse(content={"Key": stat.key, "Size": stat.size})


@app.post("/api/v0/block/get")
@app.get("/api/v0/block/get")
async def block_get(
    request: Request, arg: str = Query(..., description="The base58 encoded CID")
) -> Any:
    """Get a raw IPFS block."""
    peer: Peer = request.app.state.peer
    from fastapi.responses import Response

    from py_ipfs_lite.services import block_service

    data = await block_service.get_block(peer, arg)
    return Response(content=data, media_type="application/octet-stream")


@app.post("/api/v0/block/put")
async def block_put(request: Request, file: UploadFile = File(...)) -> Any:
    """Store a raw IPFS block."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import block_service

    if not hasattr(file, "read"):
        raise HTTPException(status_code=400, detail="Missing or invalid file")

    data = await file.read()
    cid_str = await block_service.put_block(peer, data)
    return JSONResponse(content={"Key": cid_str, "Size": len(data)})


@app.post("/api/v0/block/rm")
async def block_rm(
    request: Request,
    arg: str = Query(..., description="Bash58 multihash of block(s) to remove"),
) -> Any:
    """Remove a raw block from the local blockstore."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import block_service

    await block_service.remove_block(peer, arg)
    return JSONResponse(content={"Hash": arg, "Error": ""})


@app.post("/api/v0/pin/add")
async def pin_add(
    request: Request,
    arg: str = Query(..., description="Path to object(s) to be pinned"),
    recursive: bool = Query(True),
) -> Any:
    """Pin a CID."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import pin_service

    await pin_service.add_pin(peer, arg, recursive=recursive)
    return JSONResponse(content={"Pins": [arg]})


@app.post("/api/v0/pin/rm")
async def pin_rm(
    request: Request,
    arg: str = Query(..., description="Path to object(s) to be unpinned"),
) -> Any:
    """Unpin a CID."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import pin_service

    await pin_service.remove_pin(peer, arg)
    return JSONResponse(content={"Pins": [arg]})


@app.post("/api/v0/pin/ls")
@app.get("/api/v0/pin/ls")
async def pin_ls(
    request: Request, type_filter: str = Query("all", alias="type")
) -> Any:
    """List pins."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import pin_service

    pins = await pin_service.list_pins(peer, type_filter)

    formatted_keys = {}
    for cid_str, type_str in pins.items():
        formatted_keys[cid_str] = {"Type": type_str}

    return JSONResponse(content={"Keys": formatted_keys})


@app.post("/api/v0/repo/gc")
async def repo_gc(request: Request) -> Any:
    """Run garbage collection."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import pin_service

    await pin_service.run_gc(peer)

    # The actual IPFS API expects a stream of removed keys.
    # Our internal gc doesn't track specific removed CIDs yet, only counts.
    # Return an empty list for Key to avoid crashing clients.
    return JSONResponse(content={"Key": []})


@app.post("/api/v0/refs/local")
async def refs_local(request: Request) -> Any:
    """List all CIDs stored in the local blockstore."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import repo_service

    keys = await repo_service.list_local_refs(peer)
    results = [{"Ref": k, "Err": ""} for k in keys]
    return JSONResponse(content={"Refs": results})


@app.post("/api/v0/version")
@app.get("/api/v0/version")
async def api_version() -> Any:
    """Get the version of py-ipfs-lite."""
    from py_ipfs_lite.services import node_service

    return JSONResponse(content=node_service.get_version_info())


@app.post("/api/v0/id")
@app.get("/api/v0/id")
async def api_id(request: Request) -> Any:
    """Show IPFS node id info."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import node_service

    ident = await node_service.get_identity(peer)
    return JSONResponse(
        content={
            "ID": ident.id,
            "Addresses": ident.addresses,
        }
    )


@app.post("/api/v0/repo/stat")
@app.get("/api/v0/repo/stat")
async def repo_stat(request: Request) -> Any:
    """Get stats for the currently used repo."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import repo_service

    stat = await repo_service.get_repo_stat(peer)
    return JSONResponse(
        content={
            "NumObjects": stat.num_objects,
            "RepoSize": stat.repo_size,
            "RepoPath": stat.repo_path,
            "Version": stat.version,
        }
    )


@app.post("/api/v0/swarm/peers")
@app.get("/api/v0/swarm/peers")
async def swarm_peers(request: Request) -> Any:
    """List peers with open connections."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    peers = await swarm_service.list_connected_peers(peer)
    return JSONResponse(content={"count": peers.count, "peers": peers.peers})


@app.get("/debug/conns")
async def debug_conns(request: Request) -> Any:
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    total = await swarm_service.count_connections(peer)
    return JSONResponse(content={"total_connections": total})


@app.get("/debug/metrics/prometheus")
async def metrics(request: Request) -> Any:
    """Expose Prometheus metrics."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.metrics import IPFS_SWARM_PEERS
    from py_ipfs_lite.services import swarm_service

    conn_count = await swarm_service.count_connections(peer)

    IPFS_SWARM_PEERS.set(conn_count)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/api/v0/repo/version")
@app.get("/api/v0/repo/version")
async def repo_version(request: Request) -> Any:
    """Return the datastore/repo version."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import repo_service

    version = await repo_service.get_repo_version(peer)
    return JSONResponse(content={"Version": version})


@app.post("/api/v0/name/publish")
async def name_publish(
    request: Request,
    arg: str = Query(..., description="IPFS path of the object to be published"),
    lifetime: int = Query(24),
) -> Any:
    """Publish an IPNS record."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import naming_service

    result_name = await naming_service.publish_name(peer, arg, lifetime_hours=lifetime)
    return JSONResponse(content={"Name": result_name, "Value": arg})


@app.post("/api/v0/name/resolve")
@app.get("/api/v0/name/resolve")
async def name_resolve(
    request: Request,
    arg: str = Query(..., description="IPFS path of the name to resolve"),
) -> Any:
    """Resolve an IPNS record."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import naming_service

    path = await naming_service.resolve_name(peer, arg)
    return JSONResponse(content={"Path": path})


@app.post("/api/v0/debug/peerstore")
@app.get("/api/v0/debug/peerstore")
async def debug_peerstore(request: Request) -> Any:
    """Return peer store information for debugging."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    res = await swarm_service.list_peerstore_peers(peer)
    return JSONResponse(content={"count": res.count, "peers": res.peers})


@app.post("/api/v0/debug/routing_table")
@app.get("/api/v0/debug/routing_table")
async def debug_routing_table(request: Request) -> Any:
    """Return routing table information for debugging."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    res = await swarm_service.list_routing_table_peers(peer)
    return JSONResponse(content={"count": res.count, "peers": res.peers})


@app.post("/api/v0/swarm/connect")
async def swarm_connect(
    request: Request,
    arg: str = Query(..., description="The multiaddr of the peer to connect to"),
) -> Any:
    """Connect to a peer."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    await swarm_service.connect_peer(peer, arg)
    return JSONResponse(content={"Strings": [f"connect {arg} success"]})


@app.post("/api/v0/swarm/disconnect")
async def swarm_disconnect(
    request: Request,
    arg: str = Query(..., description="The peer ID to disconnect from"),
) -> Any:
    """Disconnect from a peer."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    await swarm_service.disconnect_peer(peer, arg)
    return JSONResponse(content={"Strings": [f"disconnect {arg} success"]})
