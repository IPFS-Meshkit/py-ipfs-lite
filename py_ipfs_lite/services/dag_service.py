"""Generic DAG node put/get. Wire encoding stays in the adapter."""

from dataclasses import dataclass
from typing import Any

from py_ipfs_lite.exceptions import DagTooDeepError, InvalidCidError
from py_ipfs_lite.peer import Peer, parse_cid


@dataclass
class DagPutResult:
    cid: str


async def put_node(peer: Peer, node_data: Any, codec: str = "dag-json") -> DagPutResult:
    try:
        node = await peer.add_node(node_data, codec=codec)
    except RecursionError as e:
        raise DagTooDeepError("DAG node exceeds maximum nesting depth") from e
    return DagPutResult(cid=str(node))


@dataclass
class DagGetResult:
    cid_codec: str  # "raw", "dag-cbor", "dag-json", ...
    node_data: Any  # decoded node — adapter decides how to put it on the wire


async def get_node(peer: Peer, cid_or_path: str) -> DagGetResult:
    try:
        cid = parse_cid(cid_or_path)
    except ValueError as e:
        raise InvalidCidError(str(e)) from e

    node_data = await peer.get_node(cid_or_path)
    return DagGetResult(cid_codec=cid.codec, node_data=node_data)
