from py_ipfs_lite.peer import GCResult, Peer


async def add_pin(peer: Peer, cid_str: str, recursive: bool = True) -> None:
    await peer.add_pin(cid_str, recursive=recursive)


async def remove_pin(peer: Peer, cid_str: str) -> None:
    await peer.remove_pin(cid_str)


async def list_pins(peer: Peer, type_filter: str = "all") -> dict[str, str]:
    return await peer.list_pins(type_filter=type_filter)


async def run_gc(peer: Peer) -> GCResult:
    return await peer.gc()
