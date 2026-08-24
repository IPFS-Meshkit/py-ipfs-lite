"""
py-ipfs-lite peer implementation.

The :class:`Peer` facade is composed from focused capability modules:

- :mod:`.core`          Peer class + state
- :mod:`._hostfactory`  host/DHT/blockstore/exchange construction
- :mod:`._lifecycle`    start/close/bootstrap/routing warm-up
- :mod:`._maintenance`  keepalive, pruner and stream-leak loops
- :mod:`._pubsub`       gossipsub management & message buffering
- :mod:`._content`      files / DAG / pins / GC
- :mod:`._naming`       IPNS + CAR
- :mod:`.ipld`          IPLD node model, codecs, sessions, locks
- :mod:`.setup`         libp2p bootstrap helpers
"""

from libp2p.bitswap.cid import (
    cid_to_bytes,
    compute_cid_v1,
    format_cid_for_display,
    parse_cid,
    parse_cid_codec,
)

from py_ipfs_lite.peer.core import Peer, PeerState
from py_ipfs_lite.peer.ipld import (
    GCResult,
    IPLDNode,
    PeerSession,
    RWLock,
    SeekableReader,
    decode_node,
    encode_node,
)
from py_ipfs_lite.peer.setup import (
    attach_libp2p_metrics,
    default_bootstrap_peers,
    new_in_memory_datastore,
    setup_libp2p,
)

__all__ = [
    "cid_to_bytes",
    "compute_cid_v1",
    "format_cid_for_display",
    "parse_cid",
    "parse_cid_codec",
    "GCResult",
    "IPLDNode",
    "Peer",
    "PeerSession",
    "PeerState",
    "RWLock",
    "SeekableReader",
    "attach_libp2p_metrics",
    "decode_node",
    "default_bootstrap_peers",
    "encode_node",
    "new_in_memory_datastore",
    "setup_libp2p",
]
