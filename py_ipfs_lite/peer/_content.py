"""Content operations: files, DAG nodes, pins, GC, sessions."""

import logging
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from typing import (
    Any,
    BinaryIO,
)

import trio
from libp2p.bitswap.cid import (
    cid_to_bytes,
    compute_cid_v1,
    format_cid_for_display,
    parse_cid,
    parse_cid_codec,
)
from libp2p.bitswap.dag import decode_dag_pb
from libp2p.peer.peerinfo import info_from_p2p_addr
from multiaddr import Multiaddr

from py_ipfs_lite.config import AddParams
from py_ipfs_lite.exceptions import BlockNotFoundError
from py_ipfs_lite.metrics import (
    IPFS_GC_RECLAIMED_BLOCKS_TOTAL,
    IPFS_GC_RUNS_TOTAL,
)
from py_ipfs_lite.peer.ipld import (
    GCResult,
    IPLDNode,
    PeerSession,
    SeekableReader,
    _to_cid_str,
    decode_node,
    encode_node,
)

try:
    from libp2p.pubsub.gossipsub import GossipSub
    from libp2p.pubsub.pubsub import Pubsub
    from libp2p.tools.anyio_service import background_trio_service
    from libp2p.tools.anyio_service.trio_manager import TrioManager

    _HAS_PUBSUB = True
except ImportError:
    GossipSub = None  # type: ignore
    Pubsub = None  # type: ignore
    background_trio_service = None  # type: ignore
    TrioManager = None  # type: ignore
    _HAS_PUBSUB = False

logger = logging.getLogger("py_ipfs_lite.peer")


class ContentMixin:
    """Mixed into :class:`py_ipfs_lite.peer.core.Peer`."""

    async def add_file(
        self,
        path_or_stream: str | bytes | BinaryIO,
        params: AddParams | None = None,
        timeout: float | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> IPLDNode:
        """
        Add a file to the DAGService. Returns an IPLDNode with the root CID.

        Accepts a filesystem path, raw bytes, or a readable binary stream.
        The returned IPLDNode supports ``str(node)`` for backward compatibility
        (returns the CID string).
        """
        self._ensure_started()
        chunk_size: int | None = None
        if params is not None and params.chunker and params.chunker.startswith("size-"):
            try:
                chunk_size = int(params.chunker.split("-")[1])
            except ValueError:
                pass

        wrapped_callback = None
        if progress_callback is not None:

            def _wrapped_callback(
                bytes_written: int, total_bytes: int, phase: str
            ) -> None:
                progress_callback(bytes_written, total_bytes)

            wrapped_callback = _wrapped_callback

        raw_cid: Any = None
        async with self._gc_lock.read_lock():
            if isinstance(path_or_stream, str):
                raw_cid = await self.dag_service.add_file(  # type: ignore[union-attr]
                    path_or_stream,
                    chunk_size=chunk_size,
                    progress_callback=wrapped_callback,
                    wrap_with_directory=False,
                )
            elif isinstance(path_or_stream, bytes):
                raw_cid = await self.dag_service.add_bytes(  # type: ignore[union-attr]
                    path_or_stream,
                    chunk_size=chunk_size,
                    progress_callback=wrapped_callback,
                )
            else:
                raw_cid = await self.dag_service.add_stream(  # type: ignore[union-attr]
                    path_or_stream,
                    chunk_size=chunk_size,
                    progress_callback=wrapped_callback,
                )
        cid_str = format_cid_for_display(raw_cid)
        routing = self.routing
        if routing is not None:
            # The DHT provide walk needs up to ~90s on a cold/loaded node:
            # it first finds the k-closest peers via iterative lookups (~20s),
            # then sends ADD_PROVIDER RPCs to each with per-peer timeouts.
            # Using default_timeout (30s) races against this and causes a
            # silent TooSlowError (which prints as an empty exception message).
            # Match the explicit budget used by the /api/v0/dht/provide endpoint.
            _provide_timeout = 90.0

            async def _bg_provide() -> None:
                try:
                    with trio.fail_after(_provide_timeout):
                        await routing.provide(cid_str)
                except trio.TooSlowError:
                    logger.warning(
                        f"Background DHT provide for {cid_str} timed out after "
                        f"{_provide_timeout}s — local provider record is still stored, "
                        f"but remote peers may not know about it yet."
                    )
                except Exception as e:
                    logger.warning(f"Failed to provide {cid_str} to DHT: {e}")

            if getattr(self, "_nursery", None):
                self._nursery.start_soon(_bg_provide)
            else:
                logger.warning(f"No nursery to background provide {cid_str}")

        return IPLDNode(cid_str)

    async def get_file(
        self,
        cid: Any,
        provider_addr: str | None = None,
        output_path: str | None = None,
        timeout: float | None = None,
        stream: bool = False,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> bytes | SeekableReader | AsyncIterator[bytes] | None:
        """
        Fetch a file by its CID.

        *cid* may be a CID string or an IPLDNode.

        Returns:
            - ``SeekableReader`` (default): seekable, async-readable wrapper
              around the full file bytes — mimics ``ufsio.ReadSeekCloser``.
            - ``bytes``: when *stream=False* (legacy default, buffers everything).
            - ``AsyncIterator[bytes]``: when *stream=True*, yields chunks.
            - ``None``: when *output_path* is set (written to disk).

        """
        self._ensure_started()
        cid_str = _to_cid_str(cid)
        t_val = timeout if timeout is not None else self.config.default_timeout
        cid = parse_cid(cid_str)
        has_root = await self.blockstore.has(cid_to_bytes(cid))  # type: ignore[union-attr]

        if not has_root:
            if provider_addr:
                maddr = Multiaddr(provider_addr)
                info = info_from_p2p_addr(maddr)
                await self.host.connect(info)  # type: ignore[union-attr]

        # Extract total file size from root node's UnixFS metadata for progress tracking
        total_file_size = 0
        if progress_callback is not None:
            try:
                root_data = await self.blockstore.get(cid_to_bytes(cid))  # type: ignore[union-attr]
                if root_data:
                    root_codec = parse_cid_codec(cid_to_bytes(cid))
                    if root_codec == "dag-pb":
                        _, root_unixfs = decode_dag_pb(root_data)
                        if root_unixfs and root_unixfs.filesize:
                            total_file_size = root_unixfs.filesize
            except Exception:
                pass  # If we can't get size, progress will be None/Unknown

        from libp2p.bitswap.dag import is_directory_node

        _get_file_self = self

        class _FetchAffinity:
            def __init__(self) -> None:
                # Seed with the peer that served a previous fetch (persisted
                # across calls) so child wants skip the slow broadcast.
                self.last_good_peer: Any | None = _get_file_self._last_good_peer

            def record(self, peer_id: Any | None) -> None:
                if peer_id is not None:
                    self.last_good_peer = peer_id
                    _get_file_self._last_good_peer = peer_id

        affinity = _FetchAffinity()

        # Seed affinity with provider peer ID so first block request skips DHT
        if provider_addr:
            try:
                maddr = Multiaddr(provider_addr)
                info = info_from_p2p_addr(maddr)
                affinity.record(info.peer_id)
            except Exception:
                pass

        # One Bitswap session per logical fetch. Creating a session per block
        # (as the exchange adapter did) spawned N sessions and N DHT provider
        # lookups for an N-block file.
        psession = PeerSession(self._exchange)

        # Helper to isolate trio.fail_after from the async generator
        async def fetch_block_with_timeout(current_cid: Any) -> Any:
            with trio.fail_after(t_val):
                res = await self._exchange.get_block(  # type: ignore[union-attr, call-arg]
                    current_cid,
                    peer_id=affinity.last_good_peer,
                    return_peer=True,
                    timeout=t_val,
                    session=psession._session,
                )
                if res and isinstance(res, tuple):
                    data, peer_id = res
                    affinity.record(peer_id)
                    return data
                return res

        async def fetch_stream(current_cid: Any) -> AsyncGenerator[Any, None]:
            data = await fetch_block_with_timeout(current_cid)
            if data is None:
                raise BlockNotFoundError(
                    f"Block not found for CID: {format_cid_for_display(current_cid)}"
                )

            codec = parse_cid_codec(cid_to_bytes(current_cid))
            if codec == "raw":
                yield data
                if progress_callback is not None and total_file_size > 0:
                    progress_callback(len(data), total_file_size)
                return

            if codec == "dag-pb":
                if is_directory_node(data):
                    links, _ = decode_dag_pb(data)
                    if links:
                        async for chunk in fetch_stream(links[0].cid):
                            yield chunk
                    return

                links, unixfs = decode_dag_pb(data)
                if not links:
                    if unixfs and unixfs.data:
                        yield unixfs.data
                        if progress_callback is not None and total_file_size > 0:
                            progress_callback(len(unixfs.data), total_file_size)
                    return

                batch_size = 32 if self.config.bitswap_batch_fetch else 1
                for i in range(0, len(links), batch_size):
                    batch_links = links[i : i + batch_size]

                    if (
                        self.config.bitswap_batch_fetch
                        and len(batch_links) > 1
                        and hasattr(self._exchange, "get_blocks_batch")
                    ):
                        cids = [link.cid for link in batch_links]
                        try:
                            with trio.fail_after(t_val):
                                await self._exchange.get_blocks_batch(  # type: ignore[attr-defined, union-attr, call-arg]
                                    cids,
                                    peer_id=affinity.last_good_peer,
                                    timeout=t_val,
                                    batch_size=batch_size,
                                    session=psession._session,
                                )
                        except Exception as e:
                            logger.debug(
                                f"Batch fetch failed for {len(cids)} CIDs: {e}"
                            )

                    for link in batch_links:
                        async for chunk in fetch_stream(link.cid):
                            yield chunk

        if output_path:
            bytes_written = 0
            with open(output_path, "wb") as f:
                async for chunk in fetch_stream(cid):
                    f.write(chunk)
                    bytes_written += len(chunk)
                    if progress_callback is not None and total_file_size > 0:
                        progress_callback(bytes_written, total_file_size)
            return None

        if stream:
            return fetch_stream(cid)

        # Buffer and return a seekable reader (ufsio.ReadSeekCloser equivalent)
        chunks = []
        async for chunk in fetch_stream(cid):
            chunks.append(chunk)
        return SeekableReader(b"".join(chunks))

    async def add_node(
        self,
        node: dict[Any, Any] | list[Any] | str | int | bytes,
        codec: str = "dag-json",
        timeout: float | None = None,
        params: AddParams | None = None,
    ) -> IPLDNode:
        """
        Store an IPLD node in the blockstore and return it as an IPLDNode.

        The returned IPLDNode supports ``str(node)`` returning the CID string
        for backward compatibility.
        """
        self._ensure_started()
        t_val = timeout if timeout is not None else self.config.default_timeout
        data = encode_node(node, codec)
        cid = compute_cid_v1(data, codec=codec)
        async with self._gc_lock.read_lock():
            await self.blockstore.put(cid, data)  # type: ignore[union-attr]
        cid_str = format_cid_for_display(cid)
        routing = self.routing
        if routing is not None:

            async def _bg_provide() -> None:
                try:
                    with trio.fail_after(t_val):
                        await routing.provide(cid_str)
                except Exception as e:
                    logger.warning(f"Failed to provide {cid_str} to DHT: {e}")

            if getattr(self, "_nursery", None):
                self._nursery.start_soon(_bg_provide)
            else:
                logger.warning(f"No nursery to background provide {cid_str}")
        return IPLDNode(cid_str)

    async def get_node(
        self,
        cid: Any,
        provider_addr: str | None = None,
        timeout: float | None = None,
    ) -> dict[Any, Any] | list[Any] | str | int | bytes:
        """Retrieve and decode an IPLD node. *cid* may be a string or IPLDNode."""
        self._ensure_started()
        cid_str = _to_cid_str(cid)
        t_val = timeout if timeout is not None else self.config.default_timeout
        cid = parse_cid(cid_str)

        # Check local blockstore first
        data = await self.blockstore.get(cid_to_bytes(cid))  # type: ignore[union-attr]

        if data is None:
            if provider_addr:
                maddr = Multiaddr(provider_addr)
                info = info_from_p2p_addr(maddr)
                await self.host.connect(info)  # type: ignore[union-attr]

            with trio.fail_after(t_val):
                data = await self._exchange.get_block(cid, timeout=t_val)  # type: ignore[union-attr]

        if data is None:
            raise BlockNotFoundError(f"Block not found for CID: {cid_str}")
        codec = parse_cid_codec(cid_to_bytes(cid))
        return decode_node(data, codec)

    async def remove_node(self, cid: Any) -> None:
        """Delete a block locally. *cid* may be a string or IPLDNode."""
        self._ensure_started()
        cid_str = _to_cid_str(cid)
        parsed = parse_cid(cid_str)
        await self.blockstore.delete(cid_to_bytes(parsed))  # type: ignore[union-attr]

    # ── ipld.DAGService interface ──────────────────────────────────────────

    async def add(self, node: Any, codec: str = "dag-json") -> IPLDNode:
        """Standard DAGService ``Add`` — store an IPLD node and return it."""
        return await self.add_node(node, codec=codec)

    async def get(self, cid_str: str) -> Any:
        """Standard DAGService ``Get`` — retrieve and decode an IPLD node."""
        return await self.get_node(cid_str)

    async def remove(self, cid_str: str) -> None:
        """Standard DAGService ``Remove`` — delete a block locally."""
        await self.remove_node(cid_str)

    async def get_many(self, cid_strs: list[str]) -> list[Any]:
        """Standard DAGService ``GetMany`` — retrieve multiple IPLD nodes in parallel."""
        results: list[Any] = [None] * len(cid_strs)

        async def _fetch_one(idx: int, c: str) -> None:
            results[idx] = await self.get_node(c)

        async with trio.open_nursery() as nursery:
            for idx, c in enumerate(cid_strs):
                nursery.start_soon(_fetch_one, idx, c)

        return results

    async def add_pin(self, cid: Any, recursive: bool = True) -> None:
        """Pin a CID. *cid* may be a string or IPLDNode."""
        self._ensure_started()
        cid_str = _to_cid_str(cid)
        pin_type = "recursive" if recursive else "direct"
        self.pin_store.add_pin(cid_str, pin_type)

    async def remove_pin(self, cid: Any) -> None:
        """Unpin a CID. *cid* may be a string or IPLDNode."""
        self._ensure_started()
        self.pin_store.remove_pin(_to_cid_str(cid))

    async def list_pins(self, type_filter: str = "all") -> dict[str, str]:
        """List pins by type. type_filter can be 'direct', 'recursive', 'indirect', or 'all'."""
        self._ensure_started()

        if type_filter not in ("all", "direct", "recursive", "indirect"):
            raise ValueError(
                "Invalid type_filter. Must be 'all', 'direct', 'recursive', or 'indirect'"
            )

        stored_pins = self.pin_store.get_pins()

        if type_filter in ("direct", "recursive"):
            return {k: v for k, v in stored_pins.items() if v == type_filter}

        result = stored_pins.copy()

        from libp2p.bitswap.cid import cid_to_bytes, format_cid_for_display, parse_cid

        from py_ipfs_lite.dag_utils import walk_dag

        indirect_pins = {}
        for cid_str, pin_type in stored_pins.items():
            if pin_type == "recursive":
                try:
                    c_bytes = cid_to_bytes(parse_cid(cid_str))
                    async for reachable_cid_bytes in walk_dag(
                        c_bytes,
                        self.blockstore.get,  # type: ignore[union-attr]
                        recursive=True,  # type: ignore[union-attr]
                    ):
                        if reachable_cid_bytes != c_bytes:
                            r_str = format_cid_for_display(
                                parse_cid(reachable_cid_bytes)
                            )
                            if r_str not in result:
                                indirect_pins[r_str] = "indirect"
                except Exception as e:
                    logger.warning(f"Failed to traverse pinned CID {cid_str}: {e}")

        if type_filter == "indirect":
            return indirect_pins

        result.update(indirect_pins)
        return result

    async def gc(self) -> GCResult:
        self._ensure_started()
        from libp2p.bitswap.cid import format_cid_for_display

        from py_ipfs_lite.dag_utils import walk_dag

        async with self._gc_lock.write_lock():
            IPFS_GC_RUNS_TOTAL.inc()
            all_cids = set(await self.blockstore.all_keys())  # type: ignore[union-attr]
            reachable_cids = set()

            for cid_str, pin_type in self.pin_store.get_pins().items():
                try:
                    c_bytes = cid_to_bytes(parse_cid(cid_str))
                    is_rec = pin_type == "recursive"
                    async for reachable_cid_bytes in walk_dag(
                        c_bytes,
                        self.blockstore.get,  # type: ignore[union-attr]
                        recursive=is_rec,  # type: ignore[union-attr]
                    ):
                        reachable_cids.add(
                            format_cid_for_display(parse_cid(reachable_cid_bytes))
                        )
                except Exception as e:
                    logger.warning(f"Failed to traverse pinned CID {cid_str}: {e}")

            to_delete = all_cids - reachable_cids
            deleted_count = 0
            for c_str in to_delete:
                await self.blockstore.delete(cid_to_bytes(parse_cid(c_str)))  # type: ignore[union-attr]
                deleted_count += 1

            IPFS_GC_RECLAIMED_BLOCKS_TOTAL.inc(deleted_count)
            return GCResult(
                reclaimed_blocks=deleted_count, retained_blocks=len(reachable_cids)
            )

    def session(self) -> PeerSession:
        """
        Return a session-based NodeGetter with its own BitswapSession.

        Each call creates a fresh BitswapSession that shares block request
        state internally, enabling efficient deduplication of concurrent
        WANT messages for the same CID within the session scope.
        """
        return PeerSession(self._exchange)

    async def fetch_block(
        self, cid: Any, timeout: float | None = None, cache: bool = True
    ) -> bytes:
        """
        Fetch raw block bytes for *cid*: local blockstore first, then Bitswap.

        Unlike :meth:`get_node` this returns the undecoded block and works
        for any codec.  When ``cache`` is true a network-fetched block is
        written back into the local blockstore.
        """
        self._ensure_started()
        t_val = timeout if timeout is not None else self.config.default_timeout

        cid_bytes = (
            cid_to_bytes(parse_cid(cid)) if isinstance(cid, str) else cid_to_bytes(cid)
        )

        data = await self.blockstore.get(cid_bytes)  # type: ignore[union-attr]
        if data is not None:
            return data

        with trio.fail_after(t_val):
            data = await self._exchange.get_block(  # type: ignore[union-attr]
                parse_cid(cid) if isinstance(cid, str) else cid,
                timeout=t_val,
            )
        if data is None:
            raise BlockNotFoundError(f"Block not found for CID: {cid}")

        if cache:
            try:
                await self.blockstore.put(cid_bytes, data)  # type: ignore[union-attr]
            except Exception as e:  # pragma: no cover - cache best-effort
                logger.debug("Failed caching fetched block %s: %s", cid, e)
        return data

    async def has_block(self, cid: Any) -> bool:
        """
        Check whether a block is available locally.

        Accepts a CID string, a ``CIDObject``, an ``IPLDNode``, or any
        object with a ``cid_bytes`` attribute.
        """
        self._ensure_started()
        if isinstance(cid, str):
            parsed = parse_cid(cid)
            return await self.blockstore.has(cid_to_bytes(parsed))  # type: ignore[union-attr]
        if isinstance(cid, IPLDNode):
            return await self.blockstore.has(cid.cid_bytes)  # type: ignore[union-attr]
        if hasattr(cid, "cid_bytes"):
            return await self.blockstore.has(cid.cid_bytes)  # type: ignore[union-attr]
        if hasattr(cid, "buffer"):
            return await self.blockstore.has(cid.buffer)  # type: ignore[union-attr]
        cid_bytes = cid_to_bytes(cid) if not isinstance(cid, bytes) else cid
        return await self.blockstore.has(cid_bytes)  # type: ignore[union-attr]
