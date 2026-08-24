"""Content routing (DHT) routes for the py-ipfs-lite HTTP API."""

from typing import Any

import trio
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from py_ipfs_lite.peer import Peer

router = APIRouter()


@router.post("/api/v0/dht/provide")
async def dht_provide(
    request: Request,
    arg: str = Query(..., description="The CID to provide to the DHT"),
    recursive: bool = Query(False),
) -> Any:
    """Announce to the DHT that we are providing the given CID."""
    peer: Peer = request.app.state.peer
    if not peer._started:
        raise HTTPException(status_code=503, detail="Peer not started")
    if peer.routing is None:
        raise HTTPException(
            status_code=503, detail="Routing not available (offline mode)"
        )

    # The DHT lookup phase alone can take ~20s on a cold node, and the
    # ADD_PROVIDER announcements go out with a per-peer QUERY_TIMEOUT, so
    # give the whole provide a realistic budget instead of 30s.
    try:
        with trio.fail_after(90.0):
            await peer.routing.provide(arg)
    except trio.TooSlowError:
        raise HTTPException(status_code=504, detail="DHT provide timed out after 90s")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to provide: {e}")

    return JSONResponse(content={"OK": True})
