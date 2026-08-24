"""Raw block routes for the py-ipfs-lite HTTP API."""

from typing import Any

from fastapi import APIRouter, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import JSONResponse, Response

from py_ipfs_lite.peer import Peer

router = APIRouter()


@router.post("/api/v0/block/stat")
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


@router.post("/api/v0/block/get")
@router.get("/api/v0/block/get")
async def block_get(
    request: Request, arg: str = Query(..., description="The base58 encoded CID")
) -> Any:
    """Get a raw IPFS block."""
    peer: Peer = request.app.state.peer

    from py_ipfs_lite.services import block_service

    data = await block_service.get_block(peer, arg)
    return Response(content=data, media_type="application/octet-stream")


@router.post("/api/v0/block/put")
async def block_put(request: Request, file: UploadFile = File(...)) -> Any:
    """Store a raw IPFS block."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import block_service

    if not hasattr(file, "read"):
        raise HTTPException(status_code=400, detail="Missing or invalid file")

    data = await file.read()
    cid_str = await block_service.put_block(peer, data)
    return JSONResponse(content={"Key": cid_str, "Size": len(data)})


@router.post("/api/v0/block/rm")
async def block_rm(
    request: Request,
    arg: str = Query(..., description="Bash58 multihash of block(s) to remove"),
) -> Any:
    """Remove a raw block from the local blockstore."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import block_service

    await block_service.remove_block(peer, arg)
    return JSONResponse(content={"Hash": arg, "Error": ""})
