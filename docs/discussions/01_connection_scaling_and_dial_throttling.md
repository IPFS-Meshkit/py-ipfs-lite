# [RFC] Swarm Connection Scaling & Dial Throttling: Maintaining 500+ Stable Peers Under Python Async/Trio

* **Category:** Architecture & Networking / RFC
* **Status:** Draft / Open for Feedback
* **Target Audience:** Core Contributors, Node Operators, and P2P Protocol Developers

---

## 1. Executive Summary

In a distributed peer-to-peer network like IPFS, maintaining a robust, well-connected swarm of peers (300 to 500+ active connections) is critical for:
1. **DHT Routing Performance:** Rapid convergence during iterative lookups (`FIND_NODE`, `GET_VALUE`, `GET_PROVIDERS`).
2. **Bitswap Block Discovery:** Maximizing the likelihood of locating blocks directly from connected peers without initiating heavy network-wide queries.
3. **PubSub Mesh Reliability:** Ensuring high mesh degree ($D=6$, $D_{high}=12$) across active topics without partition risks.

However, unlike Go's multi-threaded M:N runtime scheduler (`goroutines`), Python executes async tasks on a single-threaded cooperative event loop (`trio` / `anyio`). Initiating hundreds of concurrent connection attempts, handshakes, and cryptographic key exchanges simultaneously can overwhelm the event loop, trigger socket exhaustion (`EMFILE`), and cause high memory fragmentation.

This RFC outlines our architectural strategy for **dial throttling, adaptive rate limiting, smart address filtering, and connection watermark pruning** to scale `py-ipfs-lite` to **500+ stable, long-lived peer connections** with minimal resource footprints.

---

## 2. Challenges in Async Python P2P Networking

```
                     ┌──────────────────────────────────────────────┐
                     │          Unthrottled Dial Burst              │
                     │  (Bootstrap / Random Walk: 400+ Targets)     │
                     └──────────────────────┬───────────────────────┘
                                            │
                                            ▼
                     ┌──────────────────────────────────────────────┐
                     │         Single-Threaded Event Loop           │
                     │  - 400 TLS/Noise handshakes simultaneously   │
                     │  - High CPU spikes / Coroutine queue lag     │
                     │  - Socket descriptor exhaustion (EMFILE)     │
                     │  - Unreachable IPv6/relay dials timing out   │
                     └──────────────────────┬───────────────────────┘
                                            │
                                            ▼
                     ┌──────────────────────────────────────────────┐
                     │   Result: Connection Churn & Memory Spikes   │
                     └──────────────────────────────────────────────┘
```

When scaling peer counts, four major bottlenecks emerge:

1. **Event Loop Starvation During Crypto Handshakes:**
   Establishing a libp2p connection requires a multi-step protocol handshake:
   `Transport (TCP/QUIC) -> Security (Noise / TLS 1.3 with ECDSA/Ed25519) -> Muxer (Yamux / Mplex) -> Identify`.
   Performing 100+ simultaneous cryptographic handshakes saturates the CPU, delaying task scheduling across other subsystems (Bitswap, HTTP API).

2. **File Descriptor & Memory Pressure:**
   Each half-open TCP socket consumes OS file descriptors and buffer memory. Unchecked dial floods can breach `ulimit -n` and cause memory fragmentation in glibc's arena allocator.

3. **Dead / Unroutable Multiaddr Dials:**
   Public DHT routing tables contain many transient or unroutable multiaddresses (e.g., IPv6 addresses on hosts without public IPv6, circuit-relay addresses without active relays). Dialing these unthrottled results in long 30-second timeouts that consume concurrency slots.

4. **Connection Churn at High Watermarks:**
   Without a graceful pruning policy, nodes oscillate between dialing new peers and aggressively disconnecting existing ones.

---

## 3. Proposed Architecture & Solutions

### A. Sliding-Window Dial Throttling & Batching

Instead of spawning unbounded async tasks to dial all discovered candidates at once, `AutoConnector` uses a **sliding window rate limiter** backed by `trio.CapacityLimiter`:

```
Discovered Peers Queue ──▶ [ Batching (ALPHA=10) ] ──▶ [ CapacityLimiter (Max In-Flight = 20) ]
                                                                       │
                                                         ┌─────────────┴─────────────┐
                                                         ▼                           ▼
                                                  Dial Worker 1                Dial Worker 2 ...
```

* **In-Flight Dial Cap:** Maximum of 20 concurrent dials in progress at any instant.
* **Batch Staggering:** Dials are initiated in small waves with micro-delays (e.g., 50ms–100ms) between batches to allow the event loop to interleave I/O and process active streams.
* **Per-Peer Timeout Budget:** Each dial attempt is bounded by an aggressive individual timeout (`trio.fail_after(10.0)` for direct connections), preventing dead candidates from holding dial tokens.

---

### B. Proactive Address Filtering

Before a dial task is scheduled, candidate multiaddrs pass through an address validation pipeline:

| Address Pattern | Validation Policy | Rationale |
| :--- | :--- | :--- |
| `/ip6/...` (Public IPv6) | **Filtered Out** if host has no public IPv6 | Prevents guaranteed timeout errors on IPv4-only cloud instances (AWS EC2, Docker bridge). |
| `/p2p-circuit/...` | **Filtered Out** unless circuit relay v2 transport is explicitly enabled | Avoids dialing third-party relays for unconfigured nodes. |
| Private / Local IPs (`10.0.0.0/8`, `192.168.0.0/16`) | **Filtered Out** in public WAN mode | Protects against local subnet scans and invalid public routes. |
| Duplicate Peer IDs | **Deduplicated** across candidate queues | Ensures we only maintain one active dial per remote peer. |

---

### C. Two-Tier Watermark & Connection Pruner

To maintain a healthy equilibrium between `300` and `500` peers without constant connection churn:

```
0 Peers           300 Peers (Low Watermark)      500 Peers (High Watermark)      600+ Peers
  │                          │                               │                      │
  └──── Fast Auto-Dial ──────┴───── Normal Maintenance ──────┴───── Active Pruning ─┘
```

1. **Below Low Watermark (`N < 300`):**
   * AutoConnector runs in **high-priority mode**, triggering bootstrap lookups and K-bucket random walks to discover and connect to new peers.
2. **Normal Operating Band (`300 <= N <= 500`):**
   * Inbound and outbound connections are accepted without pruning. Background maintenance dials occur only periodically.
3. **Above High Watermark (`N > 500`):**
   * `ConnectionPruner` initiates graceful eviction. Peers are sorted using a **weighted score**:
     - **Protected / High Priority:** Bootstrap nodes, active Bitswap transfer partners, DHT server nodes.
     - **Grace Period Protection:** Peers connected for less than 60 seconds are protected from early eviction to allow Identify and protocol negotiation to complete.
     - **Eviction Candidates:** Idle connections with 0 active streams, high round-trip latency, or unsupported protocol sets.

---

### D. Memory Optimization & OS Tuning

To prevent memory bloat over extended runtimes:
* **Glibc Arena Tuning:** Configured with `MALLOC_ARENA_MAX=2` and `MALLOC_MMAP_THRESHOLD_=131072` inside container environments to prevent arena heap fragmentation.
* **Periodic Garbage Collection:** Automatic invocation of `malloc_trim(0)` during periodic maintenance sweeps.
* **File Descriptors:** System `ulimit -n` raised to `65536:65536` in production Docker configurations.

---

## 4. Configuration & Environment Variables

The following parameters are exposed to configure connection scaling in `py-ipfs-lite`:

| Environment Variable | Default | Recommended (500+ Peers) | Description |
| :--- | :--- | :--- | :--- |
| `IPFS_LITE_CONN_MGR_LOW_WATER` | `100` | `300` | Minimum active peer threshold before auto-connector initiates dials. |
| `IPFS_LITE_CONN_MGR_HIGH_WATER` | `200` | `500` | Maximum active peer cap before connection pruner triggers evictions. |
| `IPFS_LITE_MAX_INBOUND_STREAMS` | `250` | `550` | Inbound stream capacity limiter token count. |
| `IPFS_LITE_DIAL_TIMEOUT` | `15` | `10` | Timeout in seconds for individual peer dial attempts. |
| `IPFS_LITE_ENABLE_QUIC` | `true` | `true` | Enables QUIC transport (reduces TLS handshake round trips to 1-RTT/0-RTT). |

---

## 5. Observability & Telemetry

To verify swarm stability in production, `py-ipfs-lite` exposes both Prometheus metric endpoints and JSON diagnostics endpoints:

### A. Prometheus Metrics (`GET /metrics` or `GET /debug/metrics/prometheus`)

* **Swarm & Connection Totals:**
  - `ipfs_swarm_peers`: Current count of unique connected peers.
  - `ipfs_swarm_connections_total`: Total raw connections active across all transports.
  - `ipfs_swarm_connections{transport, direction}`: Directional breakdown (`tcp`, `quic-v1`, `ws` $\times$ `inbound`, `outbound`, `all`).
* **Connection Longevity & Age Tiers:**
  - `ipfs_swarm_peers_by_age{age_bucket}`: Distribution across duration tiers (`under_2m`, `2m_to_5m`, `5m_to_10m`, `10m_to_30m`, `over_30m`).
  - `ipfs_swarm_peers_connected_over_5m`, `ipfs_swarm_peers_connected_over_10m`, `ipfs_swarm_peers_connected_over_30m`: Longevity gauges.
* **Lifecycle & Disconnect Analysis:**
  - `ipfs_swarm_connects_total{transport}` / `ipfs_swarm_disconnects_total{transport}`: Lifetime event counters.
  - `ipfs_swarm_disconnect_reasons_total{reason_hint}`: Classified disconnect causes (`remote_closed_or_idle`, `handshake_failed_or_dial_cancelled`, `idle_timeout`).
* **Auto-Connector & Dial Pipeline:**
  - `ipfs_autoconnector_state{metric}`: Live gauges for `low_watermark`, `high_watermark`, `min_connections`, `max_connections`, and `in_flight_dials`.
* **Multiplexed Streams & Leak Detection:**
  - `ipfs_streams_active{protocol}`: Live open streams by protocol (`/ipfs/kad/1.0.0`, `/ipfs/bitswap/1.2.0`, `/ipfs/id/1.0.0`, `/ipfs/id/push/1.0.0`, `/ipfs/ping/1.0.0`).
  - `ipfs_streams_by_protocol_total{direction, protocol}`: Cumulative opened streams.
  - `ipfs_streams_leaked_total`: Watchdog counter for streams exceeding max lifetime thresholds.
* **Process & Host Health:**
  - `ipfs_process_memory_rss_bytes`, `ipfs_process_cpu_percent`, `ipfs_process_open_fds`, `ipfs_process_uptime_seconds`.

### B. HTTP Swarm & Diagnostics Endpoints

* `GET /api/v0/swarm/peers`: Lists all connected peers with latency, multiaddrs, age tiers, and stream counts (`{"count": N, "peers": [...]}`).
* `GET /api/v0/swarm/connection_stats`: Detailed connection tracker records, identify status, and connection duration for every peer.
* `GET /api/v0/swarm/stream_stats`: Real-time stream leak monitor reporting active, closed, and suspected leaked streams.
* `GET /api/v0/swarm/connection_metrics` (or `GET /api/v0/debug/connection-stats`): Summary metrics including total connected/disconnected events and recent disconnect logs.
* `GET /debug/conns`: Fast total raw connection counter (`{"total_connections": N}`).
* `GET /api/v0/debug/memory`: Process memory footprint, RSS before/after `malloc_trim(0)`, and active Trio coroutine introspections.
* `GET /api/v0/debug/peerstore` & `GET /api/v0/debug/routing_table`: Real-time size and peer ID inventory for PeerStore and Kademlia Routing Table.

---

## 6. Feedback & Community Questions

We would love feedback from developers and node operators on the following points:

1. **Peer Eviction Heuristics:** What additional scoring criteria (e.g., latency, past Bitswap transfer volume, DHT server status) should we incorporate into the connection pruner?
2. **Transport Preference:** When a peer advertises both TCP and QUIC-v1, should QUIC always be preferred to reduce handshake latency and socket overhead?
3. **Dial Concurrency:** What dial concurrency limits have worked best in your deployment environments (cloud VMs vs. resource-constrained edge devices)?

---
*Please share your thoughts, benchmark results, and suggestions below!*
