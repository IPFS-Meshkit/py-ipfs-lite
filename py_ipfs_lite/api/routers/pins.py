"""Pin management routes for the py-ipfs-lite HTTP API."""

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from py_ipfs_lite.exceptions import InvalidCidError
from py_ipfs_lite.peer import Peer

router = APIRouter()


@router.post("/api/v0/pin/add")
async def pin_add(
    request: Request,
    arg: str = Query(..., description="Path to object(s) to be pinned"),
    recursive: bool = Query(True),
) -> Any:
    """Pin a CID."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import pin_service

    try:
        await pin_service.add_pin(peer, arg, recursive=recursive)
    except (InvalidCidError, ValueError):
        raise HTTPException(status_code=400, detail=f"Invalid CID: {arg}")
    return JSONResponse(content={"Pins": [arg]})


@router.post("/api/v0/pin/rm")
async def pin_rm(
    request: Request,
    arg: str = Query(..., description="Path to object(s) to be unpinned"),
) -> Any:
    """Unpin a CID."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import pin_service

    await pin_service.remove_pin(peer, arg)
    return JSONResponse(content={"Pins": [arg]})


@router.post("/api/v0/pin/ls")
@router.get("/api/v0/pin/ls")
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
