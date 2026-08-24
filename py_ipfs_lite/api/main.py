"""FastAPI application factory for the py-ipfs-lite HTTP API."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

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

        # Start bootstrap in the background so we don't block API startup.
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
        await peer.close()


def create_app() -> FastAPI:
    """Build the py-ipfs-lite FastAPI application with all routers attached."""
    from py_ipfs_lite.api.routers import (
        blocks,
        content,
        dag,
        dht,
        naming,
        node,
        ops,
        pins,
        pubsub,
        repo,
        swarm,
    )

    application = FastAPI(title="py-ipfs-lite HTTP API", lifespan=lifespan)
    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    for router in (
        content.router,
        blocks.router,
        dag.router,
        pins.router,
        repo.router,
        node.router,
        swarm.router,
        naming.router,
        pubsub.router,
        dht.router,
        ops.router,
    ):
        application.include_router(router)
    return application


app = create_app()


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
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})
