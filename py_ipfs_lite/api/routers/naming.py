"""IPNS naming routes for the py-ipfs-lite HTTP API."""

from typing import Any

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from py_ipfs_lite.peer import Peer

router = APIRouter()


@router.post("/api/v0/name/publish")
async def name_publish(
    request: Request,
    arg: str = Query(..., description="IPFS path of the object to be published"),
    lifetime: str = Query("24h"),
) -> Any:
    """Publish an IPNS record."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import naming_service

    # Parse lifetime: "24h", "3600s", or plain number (hours)
    lifetime_hours = 24
    if lifetime.endswith("h"):
        lifetime_hours = int(lifetime[:-1])
    elif lifetime.endswith("s"):
        lifetime_hours = max(1, int(lifetime[:-1]) // 3600)
    else:
        lifetime_hours = int(lifetime)

    result_name = await naming_service.publish_name(
        peer, arg, lifetime_hours=lifetime_hours
    )
    return JSONResponse(content={"Name": result_name, "Value": arg})


@router.post("/api/v0/name/resolve")
@router.get("/api/v0/name/resolve")
async def name_resolve(
    request: Request,
    arg: str = Query(..., description="IPFS path of the name to resolve"),
) -> Any:
    """Resolve an IPNS record."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import naming_service

    path = await naming_service.resolve_name(peer, arg)
    return JSONResponse(content={"Path": path})
