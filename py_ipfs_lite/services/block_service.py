from dataclasses import dataclass

from py_ipfs_lite.exceptions import BlockNotFoundError, InvalidCidError
from py_ipfs_lite.peer import Peer, parse_cid


@dataclass
class BlockStat:
    key: str
    size: int


async def stat_block(peer: Peer, cid_str: str) -> BlockStat:
    from py_ipfs_lite.exceptions import PeerNotStartedError
    if not peer.blockstore:
        raise PeerNotStartedError("Blockstore is not initialized")
    try:
        cid = parse_cid(cid_str)
    except ValueError as e:
        raise InvalidCidError(str(e)) from e

    data = await peer.blockstore.get(cid)
    if data is None:
        raise BlockNotFoundError(cid_str)
    return BlockStat(key=cid_str, size=len(data))


async def remove_block(peer: Peer, cid_str: str) -> None:
    await peer.remove_node(cid_str)


async def get_block(peer: Peer, cid_str: str) -> bytes:
    from py_ipfs_lite.exceptions import PeerNotStartedError
    if not peer.blockstore:
        raise PeerNotStartedError("Blockstore is not initialized")
    try:
        cid = parse_cid(cid_str)
    except ValueError as e:
        raise InvalidCidError(str(e)) from e

    data = await peer.blockstore.get(cid)
    if data is None:
        raise BlockNotFoundError(cid_str)
    return data


async def put_block(peer: Peer, data: bytes) -> str:
    from libp2p.bitswap.cid import compute_cid_v1, format_cid_for_display

    from py_ipfs_lite.exceptions import PeerNotStartedError
    if not peer.blockstore:
        raise PeerNotStartedError("Blockstore is not initialized")

    cid = compute_cid_v1(data, codec="raw")
    await peer.blockstore.put(cid, data)
    return format_cid_for_display(cid)
