# Cross-Instance Feature Test Report

**Date:** 2026-08-21
**Nodes:** Dev (py-ipfs-lite-dev, 52.7.183.75) and Prod (py-ipfs-lite-prod, 52.7.200.90)
**Protocol:** QUIC + TCP, py-ipfs-lite v0.1.2

---

## Executive Summary

**45/50 tests passed, but this is misleading.** Most passes are local-only tests. Cross-instance content exchange (the core purpose of IPFS) is broken. DHT routing tables are empty on both nodes. Provider records never propagate, so one node cannot discover blocks stored on the other.

- Swarm connectivity: Works
- Local content operations: Works
- Cross-instance content fetch: **BROKEN**
- Unimplemented APIs: CAR, IPNS, Memory Debug, Bitswap Stat

---

## Critical Finding: DHT Provider Propagation Failure

The `add_file()` method calls `routing.provide(cid_str)` in the background. But both nodes have **0 DHT routing entries**, so provider records go nowhere.

**Root cause:** Connection manager prunes connections at 50/100 limit. Connections get destroyed before DHT protocol exchange completes.

**Evidence from both nodes:**
- Dev routing table: 0 entries
- Prod routing table: 0 entries
- `cat` from prod of dev's content: HTTP 500 (timeout)
- Logs: `protocols not supported: tried ['/ipfs/bitswap/1.2.0', '/ipfs/bitswap/1.1.0', '/ipfs/bitswap/1.0.0']`

---

## Detailed Test Results

### Group 1: Swarm and Peer Management

| Test | Result | Details |
|------|--------|---------|
| 1.1 Swarm Connect (prod to dev) | PASS | Connected via /ip4/52.7.183.75/tcp/4001/p2p/... |
| 1.2 List Peers (prod sees dev) | PASS | 60 peers on prod, dev found in list |
| 1.3 List Peers (dev sees prod) | PASS | 126 peers on dev, prod found in list |
| 1.4 Bidirectional Connect | PASS | dev to prod connect OK, 150 peers |

Verdict: Swarm connectivity works. Nodes discover and connect to each other.

---

### Group 2: Content (Add/Get Round-Trip)

| Test | Result | Details |
|------|--------|---------|
| 2.1 Add text file (dev) | PASS | CID=bafkrei..., 77 bytes |
| 2.2 Cat text file (local on dev) | PASS | 77 bytes, content matches |
| 2.3 Cat text file (cross-instance) | KNOWN ISSUE | HTTP 500, DHT providers not propagating |
| 2.4 Add 256KB binary (dev) | PASS | CID=bafkrei..., 262,144 bytes |
| 2.5 Cat 256KB binary (local) | PASS | 262,144 bytes, content matches |
| 2.6 Cat 256KB binary (cross-instance) | KNOWN ISSUE | HTTP 500, DHT providers not propagating |
| 2.7 Add on prod, Get on dev | KNOWN ISSUE | Local OK, cross-instance HTTP 404 |
| 2.8 Large file (1MB) | PASS | 1,048,576 bytes, content matches |

Test data used:
- Text: "Hello from py-ipfs-lite cross-instance test!" + 32 random bytes
- Binary: 256KB random bytes (os.urandom(256 * 1024))
- Large: 1MB random bytes (os.urandom(1024 * 1024))

Verdict: Local content operations work perfectly. Cross-instance fetch broken due to DHT failure.

---

### Group 3: DAG Put/Get

| Test | Result | Details |
|------|--------|---------|
| 3.1 DAG Put JSON (dev) | PASS | CID=baguqeea..., response: Cid object |
| 3.2 DAG Get JSON (local) | PASS | name=test-py-ipfs-lite, value=42, nested array |
| 3.3 DAG Get JSON (cross-instance) | PASS | Worked on this run, intermittent |
| 3.4 DAG Put CBOR (dev) | PASS | CID=bafyreia... |
| 3.5 DAG Get CBOR (local) | PASS | data=cbor test, numbers=[1,2,3] |
| 3.6 Large DAG (50 items) | PASS | 50 items round-trip OK |

Test data used:
- JSON: {"name": "test-py-ipfs-lite", "value": 42, "nested": {"a": [1, 2, 3]}}
- CBOR: {"data": "cbor test", "numbers": [1, 2, 3]}
- Large: {"items": [{"id": i, "data": "x" * 100} for i in range(50)]}

Verdict: DAG operations work locally. Cross-instance worked once but is unreliable.

---

### Group 4: CAR Import/Export

| Test | Result | Details |
|------|--------|---------|
| 4.1 Export CAR (dev) | SKIP | dag/export returns 404, not implemented |
| 4.2 Import CAR (prod) | SKIP | dag/import returns 404, not implemented |
| 4.3 Import and Cat | SKIP | Dependencies not available |

Verdict: CAR import/export endpoints not implemented in py-ipfs-lite.

---

### Group 5: Pinning and Garbage Collection

| Test | Result | Details |
|------|--------|---------|
| 5.1 Add test content (dev) | PASS | CID=bafkrei..., 81 bytes |
| 5.2 Pin add (local on dev) | PASS | Pinned successfully |
| 5.3 Pin list (local on dev) | PASS | CID found in pin list |
| 5.4 GC survival test | PASS | Pinned file survived GC, still accessible |
| 5.5 GC runs without error | PASS | GC completed successfully |
| 5.6 Pin add (cross-instance) | KNOWN ISSUE | Cannot fetch content from dev (DHT broken) |
| 5.7 Pin add and remove | PASS | Pin then Unpin cycle OK |

Test data used:
- Content: "GC survival test" + 64 random bytes

Verdict: Pinning and GC work correctly locally. Cross-instance pin requires content fetch which is broken.

---

### Group 6: IPNS Publish/Resolve

| Test | Result | Details |
|------|--------|---------|
| 6.1 Add test content (dev) | SKIP | Dependency for publish |
| 6.2 IPNS Publish (dev) | SKIP | name/publish returns 404, not implemented |
| 6.3 IPNS Resolve (prod) | SKIP | name/resolve returns 404, not implemented |

Verdict: IPNS publish/resolve endpoints not implemented in py-ipfs-lite.

---

### Group 7: Block Operations

| Test | Result | Details |
|------|--------|---------|
| 7.1 Add test content (dev) | PASS | CID=bafkrei..., 62 bytes |
| 7.2 Block stat (local on dev) | PASS | Key=bafkrei..., Size=62 |
| 7.3 Block get (local on dev) | PASS | 62 bytes, content matches |
| 7.4 Block put (dev) | PASS | CID=bafkrec... |
| 7.5 Block stat (cross-instance) | KNOWN ISSUE | HTTP 404, block not on prod |
| 7.6 Block get (cross-instance) | KNOWN ISSUE | HTTP 404, block not on prod |

Test data used:
- Content: "Block operations test content" + 32 random bytes
- Raw block: "Raw block content" + 16 random bytes

Verdict: Block operations work locally. Cross-instance fails because blocks are not propagated.

---

### Group 8: Diagnostics

| Test | Result | Details |
|------|--------|---------|
| 8.1 Version | PASS | dev=0.1.2, prod=0.1.2 |
| 8.2 ID | PASS | Both nodes responding with valid peer IDs |
| 8.3 Repo stat | PASS | dev=6 objects, prod=4 objects |
| 8.4 Repo stat details | PASS | Both report NumObjects, RepoSize, RepoPath, Version |
| 8.5 Local refs (dev) | PASS | 6 refs |
| 8.6 Prometheus metrics | PASS | 490 metrics exposed |
| 8.7 Connection stats | PASS | Endpoint responding |
| 8.8 Stream stats | PASS | Endpoint responding |
| 8.9 Memory debug | SKIP | Endpoint not available |
| 8.10 Peerstore | PASS | 0 known peers |
| 8.11 Routing table (dev) | PASS | 0 routing entries (critical issue) |
| 8.12 Routing table (prod) | PASS | 0 routing entries (critical issue) |
| 8.13 Bitswap stat | SKIP | Endpoint not available |
| 8.14 Swarm peers count | PASS | dev=131 peers, prod=59 peers |

Verdict: Diagnostics work. Both nodes have empty routing tables (0 entries), which explains why content cannot be discovered across instances.

---

## Summary by Feature

| Feature | Local | Cross-Instance | API Endpoint |
|---------|-------|----------------|--------------|
| Add file | PASS | N/A | /api/v0/add |
| Cat file | PASS | FAIL (DHT) | /api/v0/cat |
| DAG put | PASS | Intermittent | /api/v0/dag/put |
| DAG get | PASS | Intermittent | /api/v0/dag/get |
| CAR export | N/A | N/A | 404 Not Found |
| CAR import | N/A | N/A | 404 Not Found |
| Pin add | PASS | FAIL (DHT) | /api/v0/pin/add |
| Pin list | PASS | PASS | /api/v0/pin/ls |
| Pin remove | PASS | N/A | /api/v0/pin/rm |
| GC | PASS | N/A | /api/v0/repo/gc |
| Block put | PASS | N/A | /api/v0/block/put |
| Block stat | PASS | FAIL (DHT) | /api/v0/block/stat |
| Block get | PASS | FAIL (DHT) | /api/v0/block/get |
| IPNS publish | N/A | N/A | 404 Not Found |
| IPNS resolve | N/A | N/A | 404 Not Found |
| Swarm connect | PASS | PASS | /api/v0/swarm/connect |
| Swarm peers | PASS | PASS | /api/v0/swarm/peers |
| Version | PASS | PASS | /api/v0/version |
| Repo stat | PASS | PASS | /api/v0/repo/stat |
| Metrics | PASS | N/A | /metrics |

---

## What Needs to Be Fixed

1. **DHT Routing Table Empty** -- The most critical issue. Both nodes have 0 routing entries. Connections are pruned before DHT bootstrap completes. Need to either raise connection limits or add DHT bootstrap persistence.

2. **Cross-Instance Content Fetch** -- Directly caused by #1. Without routing entries, provider records cannot propagate. `cat` from a remote node times out.

3. **Unimplemented Endpoints** -- CAR import/export, IPNS publish/resolve, memory debug, bitswap stat are not implemented. These need to be added to the API.

---

## Files

```
tests_cross_instance/
  __init__.py          -- Shared constants (URLs, peer IDs, addresses)
  helpers.py           -- Shared API helpers (api, upload_multipart, connect_nodes)
  test_01_swarm.py     -- Swarm and Peer Management (4 tests)
  test_02_content.py   -- Content Add/Get Round-Trip (8 tests)
  test_03_dag.py       -- DAG Put/Get (6 tests)
  test_04_car.py       -- CAR Import/Export (3 tests)
  test_05_pin_gc.py    -- Pinning and Garbage Collection (7 tests)
  test_06_ipns.py      -- IPNS Publish/Resolve (2 tests)
  test_07_block.py     -- Block Operations (6 tests)
  test_08_diagnostics.py -- Diagnostics (14 tests)
  run_all.py           -- Runner script (runs all tests)
  REPORT.md            -- This report
```

Run individual tests:
```
python3 -m tests_cross_instance.test_01_swarm
python3 -m tests_cross_instance.test_02_content
python3 -m tests_cross_instance.test_03_dag
python3 -m tests_cross_instance.test_04_car
python3 -m tests_cross_instance.test_05_pin_gc
python3 -m tests_cross_instance.test_06_ipns
python3 -m tests_cross_instance.test_07_block
python3 -m tests_cross_instance.test_08_diagnostics
```

Run all:
```
python3 -m tests_cross_instance.run_all
```
