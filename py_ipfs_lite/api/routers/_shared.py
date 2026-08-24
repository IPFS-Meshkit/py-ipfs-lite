"""Shared helpers used by multiple route modules."""

from fastapi import HTTPException

from py_ipfs_lite.peer import Peer


async def local_block(peer: Peer, cid_str: str) -> bytes:
    """Fetch a block from the local blockstore only."""
    from py_ipfs_lite.exceptions import BlockNotFoundError
    from py_ipfs_lite.peer import cid_to_bytes, parse_cid

    if not peer.blockstore:
        raise HTTPException(status_code=503, detail="Blockstore not initialized")
    try:
        cid = parse_cid(cid_str)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid CID: {e}") from e
    data = await peer.blockstore.get(cid_to_bytes(cid))
    if data is None:
        raise BlockNotFoundError(cid_str)
    return data
