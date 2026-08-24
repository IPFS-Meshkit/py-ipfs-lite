"""Shared helpers used by multiple route modules."""

from fastapi import HTTPException

from py_ipfs_lite.peer import Peer


async def local_block(peer: Peer, cid_str: str, timeout: float = 90.0) -> bytes:
    """
    Fetch raw block bytes for *cid*.

    Serves from the local blockstore when present; otherwise fetches over
    Bitswap (with DHT provider discovery) and caches the result locally.
    """
    try:
        return await peer.fetch_block(cid_str, timeout=timeout)
    except Exception as e:
        # Normalise the common failure modes to the same responses /cat uses.
        import trio as _trio

        if isinstance(e, _trio.TooSlowError):
            raise HTTPException(
                status_code=404, detail=f"Block not found: {cid_str}"
            ) from e
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=404, detail=f"Block not found: {cid_str}"
            ) from e
        raise HTTPException(status_code=500, detail=str(e)) from e
