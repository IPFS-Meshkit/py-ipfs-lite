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
    security: Optional[str] = None
    muxer: Optional[str] = None
    transport: Optional[str] = None
    identify_completed: bool = False
    identify_completed_at: Optional[str] = None
    ping_completed: bool = False
    first_ping_at: Optional[str] = None


class ConnectionStatsTracker(INotifee):
    def __init__(self) -> None:
        self.stats: Dict[str, PeerConnectionStats] = {}

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    def mark_ping_completed(self, peer_id: str) -> None:
        if peer_id in self.stats:
            self.stats[peer_id].ping_completed = True
            if self.stats[peer_id].first_ping_at is None:
                self.stats[peer_id].first_ping_at = self._now()

    async def opened_stream(self, network: INetwork, stream: INetStream) -> None:
        pass

    async def closed_stream(self, network: INetwork, stream: INetStream) -> None:
        pass

    async def connected(self, network: INetwork, conn: INetConn) -> None:
        peer_id = conn.muxed_conn.peer_id.to_base58()
        now_str = self._now()

        security_type = "unknown"
        muxer_type = "unknown"
        transport_type = "unknown"

        try:
            muxer_conn = getattr(conn, "muxed_conn", None)
            if muxer_conn is not None:
                muxer_type = type(muxer_conn).__name__
                if muxer_type == "QUICConnection":
                    security_type = "quic-tls"
                    transport_type = "quic"
                    muxer_type = "quic-muxer"
                else:
                    sec_conn = getattr(muxer_conn, "secured_conn", None)
                    if sec_conn is not None:
                        security_type = type(sec_conn).__name__
                        if security_type == "SecureSession":
                            if hasattr(sec_conn, "conn"):
                                if type(sec_conn.conn).__name__ == "TLSReadWriter":
                                    security_type = "tls"
                                elif type(sec_conn.conn).__name__ == "NoiseTransportReadWriter":
                                    security_type = "Noise"
                        
                        # Unwrap transport
                        curr = sec_conn
                        for _ in range(10):
                            if hasattr(curr, "conn") and curr.conn is not None and type(curr.conn).__name__ != type(curr).__name__:
                                curr = curr.conn
                            elif hasattr(curr, "raw_conn") and curr.raw_conn is not None and type(curr.raw_conn).__name__ != type(curr).__name__:
                                curr = curr.raw_conn
                            elif hasattr(curr, "transport_conn") and curr.transport_conn is not None and type(curr.transport_conn).__name__ != type(curr).__name__:
                                curr = curr.transport_conn
                            elif hasattr(curr, "read_writer") and curr.read_writer is not None and type(curr.read_writer).__name__ != type(curr).__name__:
                                curr = curr.read_writer
                            elif hasattr(curr, "read_write_closer") and curr.read_write_closer is not None and type(curr.read_write_closer).__name__ != type(curr).__name__:
                                curr = curr.read_write_closer
                            else:
                                break
                        transport_type = type(curr).__name__
                        if transport_type in ("TCPConnection", "RawConnection", "TLSReadWriter", "NoiseTransportReadWriter"):
                            transport_type = "tcp"
        except Exception:
            pass

        if peer_id not in self.stats:
            self.stats[peer_id] = PeerConnectionStats(
                peer_id=peer_id,
                first_connected_at=now_str,
                security=security_type,
                muxer=muxer_type,
                transport=transport_type,
            )

        stats = self.stats[peer_id]
        stats.total_connections += 1
        stats.current_connections += 1
        stats.last_connected_at = now_str
        # Update protocols in case they changed
        stats.security = security_type
        stats.muxer = muxer_type
        stats.transport = transport_type

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
