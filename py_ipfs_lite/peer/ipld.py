"""IPLD node model, codec helpers, session and locking primitives."""

import contextlib
import json
import logging
from collections.abc import AsyncGenerator, Iterator
from dataclasses import dataclass
from typing import Any

import cbor2
import trio
from libp2p.bitswap.cid import cid_to_bytes, parse_cid
from libp2p.bitswap.dag import decode_dag_pb


@dataclass
class GCResult:
    reclaimed_blocks: int
    retained_blocks: int


def _check_nan(node: Any) -> None:
    import math

    if isinstance(node, float) and (math.isnan(node) or math.isinf(node)):
        raise ValueError(f"Out of range float values are not JSON compliant: {node}")
    elif isinstance(node, dict):
        for v in node.values():
            _check_nan(v)
    elif isinstance(node, list):
        for item in node:
            _check_nan(item)


class _IPLDNodeEncoder(json.JSONEncoder):
    """JSON encoder that serialises IPLDNode objects to their CID strings."""

    def default(self, o: Any) -> Any:
        if isinstance(o, IPLDNode):
            return str(o)
        return super().default(o)


def _convert_ipld_nodes(obj: Any) -> Any:
    """Recursively convert IPLDNode instances to strings for CBOR encoding."""
    if isinstance(obj, IPLDNode):
        return str(obj)
    if isinstance(obj, dict):
        return {k: _convert_ipld_nodes(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_ipld_nodes(item) for item in obj]
    return obj


def encode_node(node: Any, codec: str) -> bytes:
    if codec in ("dag-json", "dag-cbor", "cbor"):
        _check_nan(node)

    if codec == "dag-json":
        return json.dumps(
            node, separators=(",", ":"), allow_nan=False, cls=_IPLDNodeEncoder
        ).encode("utf-8")
    elif codec in ("dag-cbor", "cbor"):
        return cbor2.dumps(_convert_ipld_nodes(node))
    elif codec == "raw":
        if isinstance(node, bytes):
            return node
        elif isinstance(node, str):
            return node.encode("utf-8")
        raise TypeError("The 'raw' codec only supports bytes or str inputs")
    else:
        raise ValueError(f"Unsupported codec for encode_node: {codec}")


def decode_node(data: bytes, codec: str) -> Any:
    if codec == "dag-json":
        return json.loads(data.decode("utf-8"))
    elif codec in ("dag-cbor", "cbor"):
        return cbor2.loads(data)
    elif codec == "raw":
        return data
    elif codec == "dag-pb":
        links, unixfs = decode_dag_pb(data)
        return {"Links": links, "Data": unixfs}
    else:
        raise ValueError(f"Unsupported codec for decode_node: {codec}")


logger = logging.getLogger("py_ipfs_lite.peer")


class IPLDNode:
    """Lightweight wrapper around a CID and its raw data, mimicking ipld.Node."""

    __slots__ = ("_cid_str", "_cid_bytes", "_data", "_links", "_codec")

    def __init__(
        self,
        cid_str: str,
        data: bytes | None = None,
        links: list[Any] | None = None,
        codec: str | None = None,
    ) -> None:
        self._cid_str = cid_str
        self._cid_bytes = cid_to_bytes(parse_cid(cid_str)) if cid_str else b""
        self._data = data
        self._links = links or []
        self._codec = codec

    def cid(self) -> str:
        return self._cid_str

    @property
    def cid_bytes(self) -> bytes:
        """The raw CID bytes (multicodec-prefixed)."""
        return self._cid_bytes

    def __str__(self) -> str:
        return self._cid_str

    def __repr__(self) -> str:
        return f"IPLDNode({self._cid_str})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, IPLDNode):
            return self._cid_str == other._cid_str
        if isinstance(other, str):
            return self._cid_str == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._cid_str)

    def raw_data(self) -> bytes | None:
        return self._data

    def links(self) -> list[Any]:
        return self._links

    def codec(self) -> str | None:
        return self._codec

    def loggable(self) -> dict[str, Any]:
        info: dict[str, Any] = {"cid": self._cid_str}
        if self._codec:
            info["codec"] = self._codec
        if self._links:
            info["links"] = len(self._links)
        return info


class SeekableReader:
    """Seekable reader wrapper for get_file, mimicking ufsio.ReadSeekCloser."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0

    async def read(self, n: int = -1) -> bytes:
        if n == -1:
            result = self._data[self._pos :]
            self._pos = len(self._data)
        else:
            result = self._data[self._pos : self._pos + n]
            self._pos += len(result)
        return result

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 0:
            self._pos = offset
        elif whence == 1:
            self._pos += offset
        elif whence == 2:
            self._pos = len(self._data) + offset
        else:
            raise ValueError(f"Invalid whence: {whence}")
        self._pos = max(0, min(self._pos, len(self._data)))
        return self._pos

    def tell(self) -> int:
        return self._pos

    async def close(self) -> None:
        pass

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, bytes):
            return self._data == other
        if isinstance(other, SeekableReader):
            return self._data == other._data
        return NotImplemented

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"SeekableReader({len(self._data)} bytes, pos={self._pos})"

    def __iter__(self) -> Iterator[bytes]:
        """Yield the full data as a single chunk for sync iteration."""
        yield self._data


class PeerSession:
    """Session-scoped block retrieval, creating a fresh BitswapSession per instance."""

    def __init__(self, exchange: Any) -> None:
        self._exchange = exchange
        self._session = (
            exchange.new_session() if hasattr(exchange, "new_session") else exchange
        )

    async def get_block(
        self,
        cid: Any,
        peer_id: Any = None,
        timeout: float = 90,
    ) -> bytes | None:
        return await self._session.get_block(cid, peer_id=peer_id, timeout=timeout)

    async def get_blocks_batch(
        self,
        cids: list[Any],
        peer_id: Any = None,
        timeout: float = 90,
        batch_size: int = 32,
    ) -> dict[bytes, bytes]:
        if hasattr(self._session, "get_blocks_batch"):
            return await self._session.get_blocks_batch(
                cids, peer_id=peer_id, timeout=timeout, batch_size=batch_size
            )
        raise AttributeError("Session does not support get_blocks_batch")


def _to_cid_str(value: Any) -> str:
    """Coerce a CID-like value (str, IPLDNode, CIDObject) to a plain string."""
    if isinstance(value, str):
        return value
    if isinstance(value, IPLDNode):
        return str(value)
    if hasattr(value, "cid") and not isinstance(value, str):
        return str(value.cid)
    return str(value)


class RWLock:
    """A trio-compatible read-write lock to allow concurrent reads but exclusive writes."""

    def __init__(self) -> None:
        self._write_lock = trio.Semaphore(1)
        self._read_count = 0
        self._read_count_lock = trio.Lock()

    @contextlib.asynccontextmanager
    async def read_lock(self) -> AsyncGenerator[Any, None]:
        async with self._read_count_lock:
            self._read_count += 1
            if self._read_count == 1:
                await self._write_lock.acquire()
        try:
            yield
        finally:
            async with self._read_count_lock:
                self._read_count -= 1
                if self._read_count == 0:
                    self._write_lock.release()

    @contextlib.asynccontextmanager
    async def write_lock(self) -> AsyncGenerator[Any, None]:
        await self._write_lock.acquire()
        try:
            yield
        finally:
            self._write_lock.release()
