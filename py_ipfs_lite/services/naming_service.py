from py_ipfs_lite.peer import Peer


async def publish_name(peer: Peer, arg: str, lifetime_hours: int = 24) -> str:
    return await peer.publish_name(arg, lifetime_hours=lifetime_hours)


async def resolve_name(peer: Peer, name: str) -> str:
    return await peer.resolve_name(name)
