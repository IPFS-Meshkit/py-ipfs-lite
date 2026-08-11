#!/usr/bin/env python3
"""Start py-ipfs-lite API server using hypercorn (trio-compatible)."""

import trio
from hypercorn.config import Config as HyperConfig
from hypercorn.trio import serve

from py_ipfs_lite.api import app


async def main():
    config = HyperConfig()
    config.bind = ["0.0.0.0:5001"]
    await serve(app, config)


if __name__ == "__main__":
    trio.run(main)
