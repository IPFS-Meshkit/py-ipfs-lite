"""Node info routes for the py-ipfs-lite HTTP API."""

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from py_ipfs_lite.peer import Peer

router = APIRouter()


@router.post("/api/v0/version")
@router.get("/api/v0/version")
async def api_version() -> Any:
    """Get the version of py-ipfs-lite."""
    from py_ipfs_lite.services import node_service

    return JSONResponse(content=node_service.get_version_info())


@router.post("/api/v0/id")
@router.get("/api/v0/id")
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
