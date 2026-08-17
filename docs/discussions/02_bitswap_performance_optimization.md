# [RFC] Bitswap 1.2.0 Optimization: Long-Lived Stream Multiplexing & Request Queue Deduplication

* **Category:** Performance & Protocols / RFC
* **Status:** Draft / Open for Feedback
* **Target Audience:** Core Contributors, IPFS / Bitswap Implementers, and P2P Protocol Developers

---

## 1. Executive Summary

Bitswap is the primary block-exchange engine powering IPFS. While the Kademlia DHT locates *which* peers host specific content (Content Routing), Bitswap is responsible for *negotiating, requesting, and transferring* the underlying raw DAG blocks.

In high-throughput scenarios—such as fetching multi-gigabyte files, traversing deep UnixFS DAGs, or serving concurrent CAR file downloads—the naive approach of opening an ephemeral libp2p stream for each block request introduces severe bottlenecks:
1. **Multistream-Select Handshake Overhead:** 1–2 RTTs of protocol negotiation per stream before transmitting data.
2. **Stream Allocation & Reset Churn:** Continuous stream open/close cycles strain the Yamux/QUIC stream multiplexer.
3. **Duplicate In-Flight Requests:** Redundant `WANT_BLOCK` messages sent across multiple concurrent tasks for the same CID.

This RFC proposes a comprehensive architectural upgrade to Bitswap 1.2.0 in `py-ipfs-lite` and `py-libp2p`, focusing on **persistent stream reuse, request queue deduplication, `WANT_HAVE` vs. `WANT_BLOCK` tiering, and session-based query routing**.

---

## 2. Problem Statement & Baseline Analysis

```
[ Naive Bitswap Flow: Ephemeral Stream per Block ]

Client Task 1 (CID A) ──▶ open_stream() ──▶ multistream-select ──▶ send(WANT_BLOCK) ──▶ receive(BLOCK) ──▶ close_stream()
Client Task 2 (CID B) ──▶ open_stream() ──▶ multistream-select ──▶ send(WANT_BLOCK) ──▶ receive(BLOCK) ──▶ close_stream()
Client Task 3 (CID C) ──▶ open_stream() ──▶ multistream-select ──▶ send(WANT_BLOCK) ──▶ receive(BLOCK) ──▶ close_stream()

Result: 3 handshakes, 3 stream lifecycle allocations, high latency.
```

When fetching a 100 MB file split into 400 UnixFS blocks (256 KB each):
* **Ephemeral Streams:** Requires 400 separate stream opens and protocol negotiations. Over a 50ms RTT link, protocol negotiation alone accounts for **20+ seconds of wasted latency**.
* **Stream Multiplexer Load:** Rapidly opening and closing hundreds of streams triggers high garbage collection and memory allocations in the stream table.
* **Bandwidth Waste Without `WANT_HAVE`:** Broadcasting `WANT_BLOCK` to 20 connected peers causes multiple peers to transmit identical 256 KB payloads simultaneously, congesting the network link.

---

## 3. Proposed Architecture & Key Enhancements

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                             Bitswap Client Subsystem                             │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   Incoming Want Requests (Tasks A, B, C)                                         │
│               │                                                                  │
│               ▼                                                                  │
│   ┌────────────────────────┐                                                     │
│   │ Request Deduplication  │  (Coalesces identical CIDs onto single Future/Event)│
│   │      & In-Flight Map   │                                                     │
│   └───────────┬────────────┘                                                     │
│               │                                                                  │
│               ▼                                                                  │
│   ┌────────────────────────┐                                                     │
│   │ Message Queue Batcher  │  (Groups CIDs into batched Protobuf messages)       │
│   └───────────┬────────────┘                                                     │
│               │                                                                  │
│               ▼                                                                  │
│   ┌────────────────────────┐                                                     │
│   │ Persistent Peer Stream │  (Single long-lived bi-directional stream per peer) │
│   │   Pool & Mutex Lock    │                                                     │
│   └───────────┬────────────┘                                                     │
│               │                                                                  │
│               ▼                                                                  │
│       Remote Swarm Peer (Protocol: /ipfs/bitswap/1.2.0)                          │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### A. Long-Lived Stream Multiplexing & Connection Pooling

Instead of creating ephemeral streams, the `BitswapClient` maintains a **single persistent bidirectional stream** per connected peer:

1. **Lazy Initialization:** When a block or wantlist needs to be sent to Peer $X$, the client retrieves the cached stream from `_peer_streams[peer_id]` or opens a new `/ipfs/bitswap/1.2.0` stream if none exists.
2. **Dedicated Background Receiver Loop:** Each active stream runs an async worker that continuously reads length-prefixed Bitswap Protobuf messages (`read_varint_prefixed_bytes_limited`) until EOF or stream reset.
3. **Stream Mutex Gating:** Outbound writes (`send_message`) are serialized using a per-peer async lock (`trio.Lock`) to prevent concurrent interleaved frames.
4. **Auto-Teardown on Disconnect:** Connected via `INotifee` to Swarm disconnect events: when a peer disconnects, its cached stream and pending write queues are cleanly recycled.

---

### B. Message Queue & Batching (`WANT_HAVE` vs `WANT_BLOCK`)

Bitswap 1.2.0 introduces fine-grained want types to minimize redundant data transfer:

```protobuf
enum WantType {
    Block = 0; // Remote peer must return the full block data
    Have  = 1; // Remote peer must return whether it HAS or DONT_HAVE the block
}
```

#### Optimization Strategy:
1. **Tiered Querying:**
   - Broadcast lightweight `WANT_HAVE` messages to up to $\alpha=20$ candidate peers.
   - Once a peer responds with a `HAVE` presence acknowledgment, send a targeted `WANT_BLOCK` only to the fastest responding peer.
2. **Request Batching:**
   - Incoming want requests within a short time window (e.g., 5ms) are batched into a single `BitswapMessage` containing up to 32 entries.
3. **Cancel Want Prioritization:**
   - As soon as a block is received and validated from Peer $A$, immediate `CANCEL` messages are dispatched with highest priority to Peers $B, C, D$ to abort in-flight uploads.

---

### C. Request Deduplication & In-Flight Tracking

When multiple concurrent tasks (or child coroutines in a DAG traversal) request the same CID:
* The first requester registers an async `trio.Event` in the `_in_flight_blocks` dictionary.
* Subsequent requests for the same CID attach to the existing event rather than issuing duplicate network RPCs.
* When the block arrives and passes cryptographic multihash verification, all awaiting tasks are resumed simultaneously.

---

### D. Session-Based Querying & Peer Latency Scoring

For structured operations (e.g., recursive UnixFS directory or CAR archive extraction), queries are organized into a `BitswapSession`:
* **Live Latency Scoring:** Tracks exponential moving average (EMA) response times and block delivery success rates for each peer.
* **Dynamic Peer Set Expansion:** If the active session peers fail to return blocks within a 2-second timeout, the session automatically queries the DHT to discover and recruit new provider peers.

---

## 4. Expected Performance Gains & Benchmarks

| Metric | Ephemeral Streams (Baseline) | Persistent Streams + Batching (Proposed) | Improvement |
| :--- | :--- | :--- | :--- |
| **Stream Open Count (100 MB File)** | ~400 streams | **1 stream per peer** | **99.7% reduction** |
| **Protocol Negotiation Overhead** | ~20,000 ms (total) | **~50 ms** (initial handshake only) | **~400x faster** |
| **Duplicate Block Bandwidth** | Up to $N \times \text{FileSize}$ | **$\approx 1.05 \times \text{FileSize}$** (via `WANT_HAVE`) | **~75% bandwidth saved** |
| **CPU Utilization During Fetch** | Spikes from stream allocations | Smooth, steady stream I/O | **~40% lower CPU** |

---

## 5. Live Observability & Telemetry

The Bitswap engine exports the following Prometheus metrics at `GET /metrics` and `GET /debug/metrics/prometheus`:

* `ipfs_bitswap_bytes_sent_total`: Cumulative payload bytes sent to peers.
* `ipfs_bitswap_bytes_received_total`: Cumulative payload bytes downloaded from peers.
* `ipfs_bitswap_messages_sent_total`: Number of Bitswap Protobuf messages transmitted.
* `ipfs_bitswap_messages_received_total`: Number of Bitswap Protobuf messages received.
* `ipfs_streams_active{protocol="/ipfs/bitswap/1.2.0"}`: Current count of active persistent Bitswap streams.
* `ipfs_streams_by_protocol_total{direction="...", protocol="/ipfs/bitswap/1.2.0"}`: Total lifetime Bitswap streams opened.

---

## 6. Questions for Community Feedback

1. **Backpressure & Flow Control:** When downloading blocks faster than disk I/O can write to the Blockstore, what window size or credit-based flow control should we apply to inbound Bitswap streams?
2. **Session Scope:** Should `BitswapSession` instances be managed explicitly by callers (e.g., `dag.get(cid, session=s)`) or automatically inferred by the DAG service?
3. **Targeted WANT_BLOCK Timeout:** What is the optimal timeout before falling back from the preferred `HAVE` peer to a secondary peer (e.g., 500ms vs 1500ms)?

---
*We invite developers, researchers, and community members to share feedback, suggestions, and test cases below!*
