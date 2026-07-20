import logging
import sys

import trio
from multiaddr import Multiaddr

# Configure base logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# Suppress noisy logs from subsystems
logging.getLogger("libp2p.network").setLevel(logging.WARNING)
logging.getLogger("libp2p.kad_dht.routing_table").setLevel(logging.WARNING)
logging.getLogger("libp2p.kad_dht.peer_routing").setLevel(logging.DEBUG)
logging.getLogger("libp2p.kad_dht.kad_dht").setLevel(logging.DEBUG)
logging.getLogger("py_ipfs_lite.peer").setLevel(logging.WARNING)
logging.getLogger("libp2p.discovery.random_walk").setLevel(logging.INFO)

logger = logging.getLogger("random_walk_debug")
logger.setLevel(logging.INFO)

from libp2p.discovery.random_walk.random_walk import RandomWalk

from py_ipfs_lite.config import Config
from py_ipfs_lite.peer import Peer, default_bootstrap_peers


async def monitor_stats(peer: Peer):
    """Periodically prints the exact size of the swarm, peerstore, and routing table."""
    logger.info("Starting stats monitor...")
    while True:
        try:
            host = peer.host._host if hasattr(peer.host, "_host") else peer.host
            connected_peers = len(host.get_network().connections)
            peerstore_peers = len(host.get_peerstore().peer_ids())

            rt_peers = 0
            bucket_sizes = []
            if peer.routing and hasattr(peer.routing, "_routing"):
                routing_table = peer.routing._routing.routing_table
                rt_peers = routing_table.size()
                bucket_sizes = [b.size() for b in routing_table.buckets]

            logger.info(
                f"\n=== NODE STATS ===\n"
                f" Connected Peers : {connected_peers}\n"
                f" PeerStore Size  : {peerstore_peers}\n"
                f" Routing Table   : {rt_peers} (Buckets: {bucket_sizes})\n"
                f"=================="
            )
        except Exception as e:
            logger.error(f"Error getting stats: {e}")

        await trio.sleep(15)


# Monkey-patch to intercept and log detailed random walk activity
_original_perform_random_walk = RandomWalk.perform_random_walk


async def patched_perform_random_walk(self):
    random_peer_id = self.generate_random_peer_id()
    logger.info(f"\n🚀 [RANDOM WALK START] Target Peer ID (hex): {random_peer_id}")

    try:
        discovered_peer_ids = []
        with trio.move_on_after(60.0):
            target_key = bytes.fromhex(random_peer_id)
            discovered_peer_ids = await self.query_function(target_key) or []

        logger.info(
            f"✅ [RANDOM WALK COMPLETE] Target: {random_peer_id[:8]}...\n"
            f"   -> Discovered {len(discovered_peer_ids)} unique peers from the network."
        )

        validated_peers = []
        for peer_id in discovered_peer_ids:
            try:
                addrs = self.host.get_peerstore().addrs(peer_id)
                if addrs:
                    from libp2p.peer.peerinfo import PeerInfo

                    validated_peers.append(PeerInfo(peer_id, addrs))
            except Exception:
                continue

        logger.info(
            f"   -> {len(validated_peers)} of these peers had known addresses and were successfully validated.\n"
        )
        return validated_peers
    except Exception as e:
        logger.error(f"❌ [RANDOM WALK FAILED] Error: {e}\n")
        return []


RandomWalk.perform_random_walk = patched_perform_random_walk


async def main():
    logger.info("Initializing IPFS Lite node...")

    config = Config(blockstore_type="memory")
    peer = Peer(config)

    await peer.start()

    host = peer.host._host if hasattr(peer.host, "_host") else peer.host
    logger.info(f"Node started with Peer ID: {host.get_id().to_base58()}")

    logger.info("Connecting to bootstrap nodes to seed the DHT...")
    bootstrap_peers = default_bootstrap_peers()

    async def connect_to_bootstrapper(addr_str):
        from libp2p.peer.peerinfo import info_from_p2p_addr

        try:
            maddr = Multiaddr(addr_str)
            info = info_from_p2p_addr(maddr)
            await host.connect(info)
            logger.info(f"Connected to bootstrap node: {addr_str}")
        except Exception as e:
            pass

    async with trio.open_nursery() as nursery:
        for addr_str in bootstrap_peers:
            nursery.start_soon(connect_to_bootstrapper, addr_str)

    logger.info("Setup complete. The Random Walk runs every 1 minute.")

    async with trio.open_nursery() as nursery:
        nursery.start_soon(monitor_stats, peer)

        try:
            await trio.sleep_forever()
        except trio.Cancelled:
            pass
        finally:
            await peer.stop()


if __name__ == "__main__":
    try:
        trio.run(main)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
