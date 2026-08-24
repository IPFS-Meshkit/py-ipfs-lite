"""Repository routes for the py-ipfs-lite HTTP API."""

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from py_ipfs_lite.peer import Peer

router = APIRouter()


@router.post("/api/v0/repo/gc")
async def repo_gc(request: Request) -> Any:
    """Run garbage collection."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import pin_service

    await pin_service.run_gc(peer)

    # The actual IPFS API expects a stream of removed keys.
    # Our internal gc doesn't track specific removed CIDs yet, only counts.
    # Return an empty list for Key to avoid crashing clients.
    return JSONResponse(content={"Key": []})


@router.post("/api/v0/refs/local")
async def refs_local(request: Request) -> Any:
    """List all CIDs stored in the local blockstore."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import repo_service

    keys = await repo_service.list_local_refs(peer)
    results = [{"Ref": k, "Err": ""} for k in keys]
    return JSONResponse(content={"Refs": results})


@router.post("/api/v0/repo/stat")
@router.get("/api/v0/repo/stat")
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


@router.post("/api/v0/repo/version")
@router.get("/api/v0/repo/version")
async def repo_version(request: Request) -> Any:
    """Return the datastore/repo version."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import repo_service

    version = await repo_service.get_repo_version(peer)
    return JSONResponse(content={"Version": version})
