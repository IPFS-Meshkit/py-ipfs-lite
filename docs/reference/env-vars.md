# Environment Variable Reference

All `IPFS_LITE_*` environment variables understood by the daemon and library.
Every variable is optional; defaults are shown.

## Networking & connection management

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `IPFS_LITE_CONN_MGR_LOW_WATER`  | `50` | Minimum connections; auto-connector dials when below this. |
| `IPFS_LITE_CONN_MGR_HIGH_WATER` | `100` | High watermark; above it connection pruning kicks in. Hard cap = high + 50. |
| `IPFS_LITE_CONN_MGR_INBOUND_SLOTS` | *(max − min)* | Explicit inbound admission slot count for the post-handshake limiter. Set this to leave inbound headroom on public nodes. |
| `IPFS_LITE_BOOTSTRAP_PEERS` | libp2p bootstrappers | Comma-separated multiaddrs used as bootstrap/DHT entry points. |
| `IPFS_LITE_ANNOUNCE_ADDRS` | — | Comma-separated multiaddrs advertised via Identify instead of the (unreachable) listen addresses. |

## Content & storage

| Variable | Default | Description |
| -------- | ------- | ----------- |
| *(blockstore type/path)* | filesystem / `.py_ipfs_lite/blocks` | Configured via CLI flags (`--blockstore-type`, `--blockstore-path`), not env vars. |

## PubSub / GossipSub

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `IPFS_LITE_ENABLE_PUBSUB` | `false` | Enables gossipsub and the pubsub HTTP endpoints. |
| `IPFS_LITE_PUBSUB_TOPICS` | — | Comma-separated topics joined at startup (protected from auto-leave). |
| `IPFS_LITE_PUBSUB_AUTO_JOIN_MIN_PEERS` | `2` | Adaptive topic discovery threshold. **`0` disables adaptive auto-join AND persisted-topic rejoin** (recommended on public nodes that should not inherit high-traffic meshes). |
| `IPFS_LITE_GOSSIPSUB_DEGREE` | `6` | Target mesh degree. |

## Observability

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `IPFS_LITE_ENABLE_LIBP2P_METRICS` | `true` | Attach py-libp2p Prometheus metric hooks. |

> **Note (2026-08):** `IPFS_LITE_CONN_MGR_INBOUND_SLOTS` and
> `IPFS_LITE_PUBSUB_AUTO_JOIN_MIN_PEERS` were historically documented but never
> parsed by the code; both are wired up as of commit `f6c11c07` / `b41617ea`.
