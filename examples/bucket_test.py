import trio

from py_ipfs_lite.config import Config
from py_ipfs_lite.peer import Peer


async def main():
    config = Config(blockstore_type="memory")
    peer = Peer(config)
    await peer.start()

    # We will just print the buckets after a short delay
    # But wait, we need to populate it.

    # Just print the logic
    pass


if __name__ == "__main__":
    trio.run(main)
