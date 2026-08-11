from dataclasses import dataclass

from py_ipfs_lite import __version__
from py_ipfs_lite.peer import Peer


@dataclass
class NodeIdentity:
    id: str
    addresses: list[str]


async def get_identity(peer: Peer) -> NodeIdentity:
    from py_ipfs_lite.exceptions import PeerNotStartedError

    if not peer.host:
        raise PeerNotStartedError("Peer is not initialized")
    import typing

    from py_ipfs_lite.interfaces import HostAdapter

    host = typing.cast(HostAdapter, peer.host)
    return NodeIdentity(
        id=host.id().to_base58(),
        addresses=[str(a) for a in host.addrs()],
    )


def get_version_info() -> dict[str, str]:
    return {"Version": __version__, "Commit": "", "System": "py-ipfs-lite"}
