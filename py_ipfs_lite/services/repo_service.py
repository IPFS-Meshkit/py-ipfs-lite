from dataclasses import dataclass

from libp2p.bitswap.cid import cid_to_bytes, parse_cid

from py_ipfs_lite.peer import Peer
from py_ipfs_lite.versioning import get_repo_version as read_datastore_version


@dataclass
class RepoStat:
    num_objects: int
    repo_size: int
    repo_path: str
    version: str = "1"


async def get_repo_stat(peer: Peer) -> RepoStat:
    if peer.blockstore is None:
        return RepoStat(
            num_objects=0, repo_size=0, repo_path=peer.config.blockstore_path or ""
        )
    keys = await peer.blockstore.all_keys()
    repo_size = 0
    for k in keys:
        cid_bytes = cid_to_bytes(parse_cid(k))
        repo_size += await peer.blockstore.get_size(cid_bytes)
    path = peer.config.blockstore_path
    if peer.config.blockstore_type == "memory":
        path = ""
    return RepoStat(num_objects=len(keys), repo_size=repo_size, repo_path=path or "")


async def get_repo_version(peer: Peer) -> str:
    if peer.config.blockstore_type == "filesystem" and peer.config.blockstore_path:
        return read_datastore_version(peer.config.blockstore_path)
    return "memory"


async def list_local_refs(peer: Peer) -> list[str]:
    if peer.blockstore is None:
        return []
    return await peer.blockstore.all_keys()
