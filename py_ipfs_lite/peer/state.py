"""Peer lifecycle states."""

from enum import Enum


class PeerState(Enum):
    """Lifecycle states of a :class:`py_ipfs_lite.peer.core.Peer`."""

    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
