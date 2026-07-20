"""
Add/cat operations. Handles upload buffering and download streaming so
neither FastAPI nor MCP has to.
"""

import os
import tempfile
from collections.abc import AsyncIterator
from dataclasses import dataclass

from py_ipfs_lite.exceptions import PayloadTooLargeError
from py_ipfs_lite.peer import Peer


@dataclass
class AddFileResult:
    name: str
    cid: str
    size: int


async def add_file_from_stream(
    peer: Peer,
    filename: str,
    chunks: AsyncIterator[bytes],
    max_size: int | None = None,
) -> AddFileResult:
    max_size = max_size or getattr(peer.config, "max_upload_size", 100 * 1024 * 1024)
    fd, path = tempfile.mkstemp()
    size = 0
    try:
        print("Starting to read chunks...")
        with os.fdopen(fd, "wb") as f:
            async for chunk in chunks:
                print(f"Read chunk of size {len(chunk)}")
                size += len(chunk)
                if max_size is not None and size > max_size:
                    raise PayloadTooLargeError(f"upload exceeded {max_size} bytes")
                f.write(chunk)
        print("Finished reading chunks, calling peer.add_file...")
        cid_str = await peer.add_file(path)
        print(f"peer.add_file finished, cid={cid_str}")
        return AddFileResult(name=filename, cid=cid_str, size=size)
    finally:
        print("Cleaning up temp file...")
        os.remove(path)


async def get_file_stream(
    peer: Peer, cid_or_path: str, max_size: int | None = None
) -> AsyncIterator[bytes]:
    """Used by FastAPI's StreamingResponse."""
    max_size = max_size or getattr(peer.config, "max_download_size", 100 * 1024 * 1024)
    content_iter = await peer.get_file(cid_or_path, stream=True)
    size = 0

    # We must await the first chunk to catch immediate errors
    # (e.g. InvalidCidError/BlockNotFoundError)
    # before we return the generator back to StreamingResponse.
    # But content_iter is an AsyncIterator returned by get_file.

    from collections.abc import AsyncIterator
    from typing import cast

    iterator = cast(AsyncIterator[bytes], content_iter)

    try:
        first_chunk = await iterator.__anext__()
    except StopAsyncIteration:
        first_chunk = b""

    size += len(first_chunk)
    if size > 0:
        yield first_chunk

    async for chunk in iterator:
        size += len(chunk)
        if max_size is not None and size > max_size:
            raise PayloadTooLargeError(f"download exceeded {max_size} bytes")
        yield chunk


async def get_file_bytes(
    peer: Peer, cid_or_path: str, max_size: int | None = None
) -> bytes:
    """Buffered fetch — used by adapters (like MCP) that need one blob."""
    buf = bytearray()
    async for chunk in get_file_stream(peer, cid_or_path, max_size):
        buf.extend(chunk)
    return bytes(buf)
