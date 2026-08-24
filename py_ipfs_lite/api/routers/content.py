"""Content (add / cat / ls) routes for the py-ipfs-lite HTTP API."""

from collections.abc import AsyncGenerator
from typing import Any

import trio
from fastapi import APIRouter, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

from py_ipfs_lite.api.routers._shared import local_block
from py_ipfs_lite.exceptions import BlockNotFoundError, InvalidCidError
from py_ipfs_lite.peer import Peer

router = APIRouter()


@router.post("/api/v0/add")
async def add_file(request: Request, file: UploadFile = File(...)) -> Any:
    """Add a file to the node."""
    peer: Peer = request.app.state.peer

    if not hasattr(file, "read"):
        raise HTTPException(status_code=400, detail="Missing or invalid file")

    chunk_size = 1024 * 1024  # 1 MB chunks

    async def chunks() -> AsyncGenerator[bytes, None]:
        while True:
            data = await file.read(chunk_size)
            if not data:
                break
            yield data

    from py_ipfs_lite.services import files_service

    result = await files_service.add_file_from_stream(
        peer, getattr(file, "filename", "unknown") or "unknown", chunks()
    )

    return JSONResponse(
        content={"Name": result.name, "Hash": result.cid, "Size": str(result.size)}
    )


@router.post("/api/v0/cat")
@router.get("/api/v0/cat")
async def cat_file(
    request: Request,
    arg: str = Query(..., description="The path to the IPFS object(s) to be outputted"),
) -> Any:
    """Fetch a file by its CID."""
    peer: Peer = request.app.state.peer

    from py_ipfs_lite.services import files_service

    stream = files_service.get_file_stream(peer, arg)
    try:
        first_chunk = await stream.__anext__()
    except StopAsyncIteration:
        first_chunk = b""
    except BlockNotFoundError:
        raise HTTPException(status_code=404, detail=f"Block not found: {arg}")
    except (InvalidCidError, ValueError):
        raise HTTPException(status_code=400, detail=f"Invalid CID: {arg}")
    except trio.TooSlowError:
        # Network fetch for a block that exists nowhere expired.
        # Report as not-found instead of an internal server error.
        raise HTTPException(status_code=404, detail=f"Block not found: {arg}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    async def _stream_with_first() -> AsyncGenerator[bytes, None]:
        if first_chunk:
            yield first_chunk
        async for chunk in stream:
            yield chunk

    return StreamingResponse(
        _stream_with_first(), media_type="application/octet-stream"
    )


@router.post("/api/v0/ls")
@router.get("/api/v0/ls")
async def ls_unixfs(
    request: Request,
    arg: str = Query(..., description="CID of a unixfs directory"),
) -> Any:
    """List the entries of a unixfs directory (dag-pb)."""
    from libp2p.bitswap.dag_pb import decode_dag_pb

    data = await local_block(request.app.state.peer, arg)
    try:
        links, unixfs = decode_dag_pb(data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Not a dag-pb node: {e}") from e

    if unixfs is None or unixfs.type != "directory":
        raise HTTPException(status_code=400, detail="Node is not a unixfs directory")

    entries = []
    for link in links:
        cid_str = None
        try:
            from libp2p.bitswap.cid import parse_cid as _pc

            cid_str = str(_pc(link.cid))
        except Exception:
            continue
        entries.append(
            {
                "Name": link.name,
                "Hash": cid_str,
                "Size": link.size,
            }
        )
    entries.sort(key=lambda e: e["Name"])
    return JSONResponse(
        content={
            "Objects": [
                {"Hash": arg, "Links": entries},
            ]
        }
    )
