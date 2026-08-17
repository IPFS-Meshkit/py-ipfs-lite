# [Architecture] py-ipfs-lite Metrics & Telemetry: Real-Time Node Visibility, Diagnostics, and Lifecycle Tracking

* **Category:** Observability & Developer Experience / Architecture
* **Status:** Draft / Open for Feedback
* **Target Audience:** Node Operators, Frontend/Dashboard Developers, Core Contributors, and SREs

---

## 1. Executive Summary

Operating a peer-to-peer node in production requires continuous, granular visibility into what the node is doing across its entire lifecycle:
* *How many peers are connected, and what transports (TCP vs. QUIC-v1) are they using?*
* *Are connections stable over time, or is the node experiencing rapid connection churn?*
* *Are any async coroutines or multiplexed streams leaking in memory?*
* *What is our Bitswap throughput and DHT query latency distribution?*

`py-ipfs-lite` features an end-to-end **observability and telemetry architecture** combining:
1. **Prometheus Exposition:** Standard `/metrics` endpoint exposing 25+ real-time gauges, counters, and latency histograms.
2. **HTTP Swarm & Diagnostic APIs:** Kubo-compatible `/api/v0/` endpoints for real-time peer, stream, and routing inspection.
3. **Deep Runtime Introspection:** `/api/v0/debug/memory` object tracking, Trio task profiling, and glibc arena trim analysis.
4. **Automated Stream Leak Watchdog:** Proactive background monitor detecting and tracking orphaned streams.

This document details the metrics architecture, explains what each metric represents, and outlines our roadmap for pre-built Grafana dashboards and web frontend integrations.

---

## 2. Telemetry Architecture Overview

```
                      ┌─────────────────────────────────────────────────────────┐
                      │                   py-ipfs-lite Daemon                   │
                      └────────────────────────────┬────────────────────────────┘
                                                   │
                ┌──────────────────────────────────┴──────────────────────────────────┐
                │                                                                     │
                ▼                                                                     ▼
   ┌──────────────────────────┐                                          ┌──────────────────────────┐
   │ Prometheus Metrics Store │                                          │ HTTP Swarm & Diagnostics │
   │ (py_ipfs_lite.metrics)   │                                          │ (py_ipfs_lite.api)       │
   └────────────┬─────────────┘                                          └────────────┬─────────────┘
                │                                                                     │
                ▼                                                                     ▼
   GET /metrics                                                           GET /api/v0/swarm/peers
   GET /debug/metrics/prometheus                                          GET /api/v0/swarm/connection_stats
        │                                                                 GET /api/v0/swarm/stream_stats
        ▼                                                                 GET /api/v0/debug/memory
   [ Prometheus / Grafana / Datadog ]                                     GET /debug/conns
                                                                               │
                                                                               ▼
                                                                  [ WebUI / CLI Tools / Scripts ]
```

---

## 3. Complete Prometheus Metrics Catalog

All Prometheus metrics are exposed with the `ipfs_` prefix at `GET /metrics` and `GET /debug/metrics/prometheus`:

### A. Swarm & Connection Lifecycle

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `ipfs_swarm_peers` | Gauge | — | Current count of unique active swarm peers. |
| `ipfs_swarm_connections_total` | Gauge | — | Total raw active connections across all transports. |
| `ipfs_swarm_connections` | Gauge | `transport`, `direction` | Active connections categorized by transport (`tcp`, `quic-v1`, `ws`) and direction (`inbound`, `outbound`, `all`). |
| `ipfs_swarm_peers_by_age` | Gauge | `age_bucket` | Number of active peers in duration tiers: `under_2m`, `2m_to_5m`, `5m_to_10m`, `10m_to_30m`, `over_30m`. |
| `ipfs_swarm_peers_connected_over_5m` | Gauge | — | Gauge tracking peers connected continuously for $>5\text{ minutes}$. |
| `ipfs_swarm_peers_connected_over_10m` | Gauge | — | Gauge tracking peers connected continuously for $>10\text{ minutes}$. |
| `ipfs_swarm_peers_connected_over_30m` | Gauge | — | Gauge tracking long-lived anchor peers connected for $>30\text{ minutes}$. |
| `ipfs_swarm_connects_total` | Counter | `transport` | Lifetime counter of successful connections established. |
| `ipfs_swarm_disconnects_total` | Counter | `transport` | Lifetime counter of disconnection events. |
| `ipfs_swarm_disconnect_reasons_total` | Counter | `reason_hint` | Disconnection root causes: `remote_closed_or_idle`, `handshake_failed_or_dial_cancelled`, `idle_timeout`. |

### B. Auto-Connector Pipeline & Watermarks

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `ipfs_autoconnector_state` | Gauge | `metric` | Live configuration and state values: `low_watermark`, `high_watermark`, `min_connections`, `max_connections`, `in_flight_dials`. |

### C. Multiplexed Streams & Leak Detection

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `ipfs_streams_active` | Gauge | `protocol` | Live open streams per protocol (`/ipfs/kad/1.0.0`, `/ipfs/bitswap/1.2.0`, `/ipfs/id/1.0.0`, `/ipfs/id/push/1.0.0`, `/ipfs/ping/1.0.0`). |
| `ipfs_streams_active_by_direction` | Gauge | `direction` | Live open streams broken down by `inbound` vs `outbound`. |
| `ipfs_streams_by_protocol_total` | Gauge | `direction`, `protocol` | Cumulative streams opened across protocol and direction. |
| `ipfs_streams_opened_total` | Counter | — | Total network streams opened since node boot. |
| `ipfs_streams_closed_total` | Counter | — | Total network streams closed cleanly. |
| `ipfs_streams_leaked_total` | Counter | — | Total suspected leaked streams flagged by the background watchdog (lifetime $>300\text{s}$). |
| `ipfs_streams_resets_total` | Counter | — | Total stream reset events observed. |

### D. Bitswap & Content Routing

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `ipfs_bitswap_bytes_sent_total` | Counter | — | Cumulative bytes sent to remote peers over Bitswap. |
| `ipfs_bitswap_bytes_received_total` | Counter | — | Cumulative bytes downloaded from remote peers over Bitswap. |
| `ipfs_bitswap_messages_sent_total` | Counter | — | Total Bitswap Protobuf messages transmitted. |
| `ipfs_bitswap_messages_received_total` | Counter | — | Total Bitswap Protobuf messages received. |
| `ipfs_dht_query_latency_seconds` | Histogram | — | Latency distribution of DHT `find_providers` and peer routing queries. |

### E. Blockstore & Storage

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `ipfs_blockstore_blocks_total` | Gauge | — | Total number of blocks stored locally in the Blockstore. |
| `ipfs_blockstore_size_bytes` | Gauge | — | Total storage size consumed by raw blocks in bytes. |
| `ipfs_gc_runs_total` | Counter | — | Total number of garbage collection cycles executed. |
| `ipfs_gc_reclaimed_blocks_total` | Counter | — | Cumulative unpinned blocks reclaimed by GC. |

### F. Host Process & Resource Footprint

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `ipfs_process_cpu_percent` | Gauge | — | High-precision CPU utilization percentage of the node process. |
| `ipfs_process_memory_rss_bytes` | Gauge | — | Resident Set Size (physical RAM) in bytes. |
| `ipfs_process_memory_vms_bytes` | Gauge | — | Virtual Memory Size in bytes. |
| `ipfs_process_open_fds` | Gauge | — | Active file descriptor count (monitors OS socket limits). |
| `ipfs_process_uptime_seconds` | Gauge | — | Node process uptime in seconds. |

---

## 4. Live Diagnostic & Inspection Endpoints

For interactive CLI debugging, automated health checks, or web dashboard polling, `py-ipfs-lite` provides rich JSON endpoints:

### 1. Swarm Peers (`GET /api/v0/swarm/peers`)
Returns the complete list of connected peers with active connection duration, multiaddrs, age tiers, and directional stream counts:
```json
{
  "count": 189,
  "peers": [
    {
      "peer": "12D3KooWT3Ye1Fo2bXoA4FrTc9B8TTQjw21RdTHLcT8NvZUjifXc",
      "addrs": ["/ip4/152.53.65.116/tcp/7147"],
      "connected_at": "2026-08-16T18:40:12.118+00:00",
      "duration_seconds": 312.4,
      "transport": "tcp",
      "direction": "outbound",
      "age_tier": "5m_to_10m",
      "streams_total": 1,
      "streams_outbound": 1,
      "streams_inbound": 0
    }
  ]
}
```

### 2. Stream Leak Watchdog (`GET /api/v0/swarm/stream_stats`)
Snapshot of the background stream leak monitor:
```json
{
  "CurrentOpenStreams": 25,
  "ActiveOutboundStreams": 1,
  "ActiveInboundStreams": 24,
  "TotalOutboundOpened": 4250,
  "TotalInboundOpened": 13341,
  "SuspectedLeakedStreams": 0,
  "AverageStreamLifetimeSeconds": 4.12
}
```

### 3. Memory & Runtime Profiling (`GET /api/v0/debug/memory`)
Deep memory and coroutine introspection:
```json
{
  "total_objects": 381281,
  "rss_before_trim_mb": 215.47,
  "rss_after_trim_mb": 205.32,
  "server_conns_count": 189,
  "top_tasks": [
    {"task": "AutoConnector._reconnect_loop", "count": 1},
    {"task": "RTRefreshManager._refresh_loop", "count": 1},
    {"task": "swarm_stream_handler", "count": 24}
  ]
}
```

### 4. Routing Table & PeerStore Inventory
* `GET /api/v0/debug/routing_table`: Returns total count and peer IDs currently resident in K-buckets.
* `GET /api/v0/debug/peerstore`: Returns total count and peer IDs stored in the PeerStore.

---

## 5. Dashboarding & Frontend Roadmap

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                                py-ipfs-lite Web Dashboard                                │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  [ Swarm Status ]              [ Memory & Host ]              [ Bitswap & DHT ]          │
│  ● 189 Connected Peers         ● RSS: 205 MB (Trimmed)        ● Downloaded: 45.2 MB      │
│  ● 138 TCP / 97 QUIC-v1        ● CPU: 8.7%                    ● Uploaded: 12.1 MB        │
│  ● In-Flight Dials: 15 / 20    ● Open FDs: 260                ● DHT Avg Latency: 240ms   │
│                                                                                          │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│  [ Peer Duration Tiers ]                                                                 │
│  ■ Under 2m (97)   ■ 2m-5m (11)   ■ 5m-10m (27)   ■ 10m-30m (97)   ■ Over 30m (0)        │
│                                                                                          │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│  [ Active Multiplexed Streams ]                                                          │
│  • /ipfs/kad/1.0.0 (12)    • /ipfs/bitswap/1.2.0 (7)    • /ipfs/id/1.0.0 (3)             │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### Planned Visualizations & Tooling:
1. **Official Grafana Dashboard Template:**
   - Bundling a production-ready `grafana-dashboard.json` in `docs/dashboards/` tracking swarm watermarks, peer duration distribution, and Bitswap throughput over 1h/24h/7d windows.
2. **Interactive WebUI Status Page:**
   - A lightweight, self-contained single-page dashboard served at `/webui` or `/status` rendering real-time peer topology, age charts, and diagnostic controls.
3. **Event Streaming / WebSocket Hook:**
   - Exposing a `/api/v0/events` WebSocket stream streaming live DHT random walks, peer connect/disconnect events, and block transfer updates.

---

## 6. Questions for the Community

1. **Dashboard Preferences:** Do you prefer standalone Grafana dashboards scraped via Prometheus, or an embedded lightweight WebUI directly built into the daemon?
2. **Additional Metrics:** What additional subsystem metrics (e.g., IPNS publish latency, CAR export speed, UnixFS DAG traversal rate) would you like to see added?
3. **Alerting Rules:** What standard Prometheus alerting rules (e.g., `PeerCountDropAlert`, `StreamLeakDetectedAlert`, `MemoryThresholdAlert`) would be most useful as defaults?

---
*Please share your feedback, monitoring setups, and ideas in the discussion below!*
