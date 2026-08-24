# py-ipfs-lite — Modularisation, Interop & Hardening Report

**Date:** 2026-08-24
**Scope:** Full refactor of monolithic modules → packages · example validation · docs overhaul · deep Kubo interop matrix · metrics/log analysis
**Final stable markers:**
- py-ipfs-lite **`0279c5ec`** (tag `stable-2026-08-24-v2`) — deployed on both DEV & PROD
- py-libp2p **`ccd7f6d1`** (tag `stable-2026-08-24`) — metrics branch, consumed by Docker builds
- Previous stable baseline preserved as tag `stable-2026-08-24` (pre-refactor)

---

## 1. Refactoring

### 1.1 Before → After

| File | Before | After |
|---|---|---|
| `py_ipfs_lite/peer.py` | 2,278 lines, one class | `peer/` package — 10 focused modules, largest 483 lines |
| `py_ipfs_lite/api.py` | 1,143 lines, single file | `api/` package — app factory + 12 router modules, largest 271 lines |

### 1.2 New structure

```
py_ipfs_lite/peer/                 py_ipfs_lite/api/
├── core.py        Peer facade     ├── main.py            app factory/lifespan/handlers
├── state.py       PeerState       └── routers/
├── ipld.py        IPLDNode etc.   │   ├── content.py     add/cat/ls
├── setup.py       libp2p setup    │   ├── blocks.py      block/*
├── _hostfactory.py host build     │   ├── dag.py         dag/* + CAR + refs
├── _lifecycle.py  start/close     │   ├── pins.py        pin/*
├── _maintenance.py keepalive/etc.  │   ├── repo.py        repo/*
├── _pubsub.py     gossipsub mgmt  │   ├── node.py        id/version
├── _content.py    files/dag/pins  │   ├── swarm.py       swarm/*+tags
├── _naming.py     IPNS + CAR      │   ├── naming.py      name/*
└── __init__.py    public surface  │   ├── pubsub.py      pubsub/*
                                   │   ├── dht.py         dht/provide
                                   │   ├── ops.py         metrics/debug
                                   │   └── _shared.py     fetch helpers
```

**Compatibility guarantee (verified programmatically):**
- All previous imports still work: `from py_ipfs_lite.peer import Peer, GCResult, cid_to_bytes…`
- HTTP route parity: 45 paths / 68 method-pairs — **zero missing, zero added** (OpenAPI diff vs. pre-refactor commit in a clean git worktree)
- Lint (`ruff check`) and format clean across the whole package

### 1.3 Commits
| Commit | Content |
|---|---|
| `b2fc55b7` | api.py → domain router package |
| `8542e796` | peer.py → mixin-based package |
| `fd4245a3` | docs updates |
| `b24495ad` | interop gap fixes (below) |
| `0279c5ec` | metrics cleanup |

---

## 2. Examples validation (run inside the live DEV container)

| Example | Result |
|---|---|
| 01_embeddable_peers | ✅ works (runs forever by design) |
| 03_ipld_node | ✅ completes; hangs only at process exit (KI-1) |
| 04_pin_and_gc | ✅ pin+GC correct ("reclaimed 2, retained 1"); teardown crash (KI-1, pre-existing) |
| 05a/b localstore | ✅ write/read roundtrip |
| 10_ipld_linked_dag | ✅ "traversed 4-hop DAG" |
| 11_car_export_import | ✅ export/import verified |
| 12_streaming_large_file | ✅ **50 MB streamed, integrity verified** |
| 13/14 agent/RAG demos | ✅ work completes; same teardown crash (KI-1, pre-existing) |
| 16_metrics_dashboard, 21_resource_footprint | ✅ monitors run (GC reclaimed 5000 blocks in 21) |
| 02_dht_discovery, 08 | ✅ long-running discovery loops (by design) |
| 06,07,09,15,17–20 | shell scripts & kubo-dependent — covered via interop phase below |

**KI-1 (known issue, pre-existing):** several examples crash with
`TrioInternalError` at *interpreter shutdown* after all work completes.
Reproduced identically on the pre-refactor stable deployment → not caused by
the refactor. Root cause: suspended async generators at `trio.run()` end
(`assert len(runner.tasks) == 2` in trio's finalizer). Impact: non-zero exit
code only. Recommended follow-up: audit async-generator ownership across
`Peer.close()`.

---

## 3. Documentation

- **`docs/architecture.md`** — rewritten component map for both new packages,
  added §7 "Peer & API package internals" tables
- **NEW `docs/reference/env-vars.md`** — full `IPFS_LITE_*` environment table
  with defaults and semantics; documents the two historically-dead variables
- **`docs/CONNECTION_MANAGER_REPORT.md`** — marked historical (pre-refactor
  line numbers); notes its INBOUND_SLOTS finding is now fixed
- README quickstart verified against current code

---

## 4. Kubo interop matrix (kubo 0.43.0)

Two kubo instances used: a NAT'd local daemon and a cloud daemon on the PROD
host (real DHT participation). Final score: **11 / 11 PASS**

| # | Test | Result |
|---|---|---|
| T1 | bitswap: kubo cats CID added+provided by py-ipfs-lite | ✅ |
| T2 | bitswap: py-ipfs-lite cats kubo-added CID over DHT | ✅ |
| T3/T3b | CAR exported by py-ipfs-lite imported & read by kubo (`dag import`/`dag get`) | ✅ |
| T4 | CAR exported by kubo imported by py-ipfs-lite | ✅ |
| T5 | py-ipfs-lite reads kubo's `dag-cbor` node | ✅ |
| T6 | `ls` lists kubo-created unixfs directory entries | ✅ (after fix) |
| T6b | cat of individual kubo unixfs chunks | ✅ |
| T7 | pin lifecycle on interop CIDs | ✅ |
| T9 | cloud-kubo `cat` of DEV content over network | ✅ |
| T8a | IPNS publish (post-fix): success + spec-compliant `/ipfs/<cid>` values | ✅ |
| swarm | QUIC peering kubo ↔ py-ipfs-lite | ✅ |

### Defects found by the matrix — fixed in this cycle
1. **Remote `ls`/`refs` impossible** — endpoints were local-blockstore-only;
   kubo-created content always 404'd. Fix: new public `Peer.fetch_block()`
   (local-first, Bitswap fallback, cache-on-fetch); `_shared.local_block`
   normalises failures to cat-compatible 404s. (`b24495ad`)
2. **IPNS publish broken two ways** — bare CIDs produced spec-invalid record
   values (now auto-prefixed to `/ipfs/<cid>`), and the DHT announcement ran
   on a 30 s budget (now 90 s with explicit timeout messaging). (`b24495ad`)
3. Harness-only false failure (T1 multipart) corrected in the test script.

### Known limitation (documented, unfixed)
- **Cross-implementation IPNS resolution over the public DHT fails in both
  directions** (kubo cannot resolve records published by py-ipfs-lite and
  vice-versa), while publishing itself succeeds and records are locally
  resolvable. Suspect signed-record encoding/validation differences between
  py-libp2p's KadDHT and go-amino-DHT. Needs packet-level DHT tracing —
  recommended follow-up project.
- kubo `dht findprovs` does not surface py-ipfs-lite provider records even
  after successful `provide`; likely related to the same record-layer issue.
  Practical impact is low because bitswap retrieval works via direct peering
  and IPNI remains available.

---

## 5. Metrics & log analysis (post-refactor, clean state)

### Fixed
- **`ipfs_bitswap_bytes_sent_total` was never instrumented** (always 0 despite
  actively serving blocks to kubo). Sent bytes are already correctly captured
  by libp2p's own `bitswap_block_sent_bytes` histogram on `/metrics`; the
  misleading zeroed duplicate was removed (`0279c5ec`).

### Healthy signals
- Connections: DEV 350 inbound (cap) + ~190 outbound; PROD 250 + ~185 — both
  nodes saturated by legitimate public DHT interest
- PubSub warning flood eliminated (~43,000/min → 0) since auto-join disable
- Bidirectional messaging verified repeatedly post-refactor
- Peer identity stable across every rebuild (seed-file persistence works)

### Remaining log noise (normal public-node churn — no action needed)
| Log | Rate | Assessment |
|---|---|---|
| `[INBOUND_LIMITER_DENY]` | ~500/min DEV | Admission control at capacity; visible-by-design |
| `Failed to open stream…` | ~240/min | Outbound dials to dead public peers |
| `Closed inbound stream N` | ~120/min | Remotes closing streams abruptly |
| security/multiselect handshake failures | ~50/min | Hostile/misconfigured dialers |

### Improvement backlog (recommended, not blocking)
1. Demote expected churn errors (`Failed to open stream`, closed-stream) to
   DEBUG to cut ERROR-level noise ~80%
2. Filter loopback self-dials (`[::1]:4001` attempts observed)
3. KI-1 asyncgen teardown audit
4. Cross-implementation IPNS DHT trace (above)

---

## 6. Deployment state at report time

| Node | Commit | Tag | Peer ID |
|---|---|---|---|
| py-ipfs-lite-dev | `0279c5ec` | stable-2026-08-24-v2 | `12D3KooWNVKp8J8GDsoT…` |
| py-ipfs-lite-prod | `0279c5ec` | stable-2026-08-24-v2 | `12D3KooWE2YPw2VvK4eW…` |

Post-deploy verification on refactored code: id ✓ add/cat ✓ dag ✓ pin ✓
swarm/tags/protection ✓ pubsub bidirectional (`ship-it` delivered) ✓ metrics ✓
examples ✓ kubo interop 11/11 ✓

All changes pushed:
- `sumanjeet0012/py-ipfs-lite` + `IPFS-Meshkit/py-ipfs-lite` @ main
- `sumanjeet0012/py-libp2p` @ metrics (unchanged this cycle; ccd7f6d1 tagged)
