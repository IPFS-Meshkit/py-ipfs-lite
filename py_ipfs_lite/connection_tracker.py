from datetime import datetime, timezone
from typing import Dict, Optional
from pydantic import BaseModel
from libp2p.abc import INotifee, INetwork, INetStream, INetConn


class PeerConnectionStats(BaseModel):
    peer_id: str
    total_connections: int = 0
    current_connections: int = 0
    first_connected_at: Optional[str] = None
    last_connected_at: Optional[str] = None
    last_disconnected_at: Optional[str] = None


class ConnectionStatsTracker(INotifee):
    def __init__(self) -> None:
        self.stats: Dict[str, PeerConnectionStats] = {}

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    async def opened_stream(self, network: INetwork, stream: INetStream) -> None:
        pass

    async def closed_stream(self, network: INetwork, stream: INetStream) -> None:
        pass

    async def connected(self, network: INetwork, conn: INetConn) -> None:
        peer_id = conn.muxed_conn.peer_id.to_base58()
        now_str = self._now()

        if peer_id not in self.stats:
            self.stats[peer_id] = PeerConnectionStats(
                peer_id=peer_id,
                first_connected_at=now_str,
            )

        stats = self.stats[peer_id]
        stats.total_connections += 1
        stats.current_connections += 1
        stats.last_connected_at = now_str

    async def disconnected(self, network: INetwork, conn: INetConn) -> None:
        peer_id = conn.muxed_conn.peer_id.to_base58()
        now_str = self._now()

        if peer_id in self.stats:
            stats = self.stats[peer_id]
            stats.current_connections = max(0, stats.current_connections - 1)
            stats.last_disconnected_at = now_str

    async def listen(self, network: INetwork, multiaddr: "Multiaddr") -> None:
        pass

    async def listen_close(self, network: INetwork, multiaddr: "Multiaddr") -> None:
        pass
