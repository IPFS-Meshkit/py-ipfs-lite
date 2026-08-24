# py-ipfs-lite — Manual End-to-End Feature Test Report

**Date:** 2026-08-24
**Nodes:** py-ipfs-lite-dev (`52.7.183.75`, peer `…NVKp8J8GDsoT…`) and py-ipfs-lite-prod (`52.7.200.90`, peer `…E2YPw2VvK4eW…`)
**Deployed commits:** py-ipfs-lite `670ea680` (main) · py-libp2p `ccd7f6d1` (metrics)
**Method:** scripted API exercise against `127.0.0.1:5001` on each node + manual curl verification of every harness failure + bidirectional cross-machine transfer tests.

---

## 1. Results summary

**28 automated checks + manual follow-ups.** After eliminating test-harness artifacts:
**22 features verified working · 3 real defects · several Kubo-API gaps.**

| # | Feature | Status | Notes |
|---|---------|--------|-------|
| 1 | `id` | ✅ PASS | returns peer ID + advertised addrs |
| 2 | `version` | ✅ PASS | 0.1.2 |
| 3 | `add` (multipart file) | ✅ PASS | CIDv1 raw-leaves; verified via curl |
| 4 | `cat` (local blockstore hit) | ✅ PASS | byte-for-byte md5 match |
| 5 | `cat` cross-node DEV→PROD | ✅ PASS | 17.9 s cold DHT discovery + bitswap transfer |
| 6 | `cat` cross-node PROD→DEV | ⚠️ FLAKY | 1st attempt timed out at 30 s → 404; retry 1.5 s (see RCA-1) |
| 7 | `dag/put` + `dag/get` roundtrip | ✅ PASS | JSON object round-trips exactly |
| 8 | `block/stat` | ✅ PASS | |
| 9 | `block/get` | ✅ PASS | |
| 10 | `block/put` | ⚠️ PARTIAL | works but form-field deviates from Kubo (see RCA-3) |
| 11 | `block/rm` | ✅ PASS | |
| 12 | `pin/add`, `pin/ls`, `pin/rm` | ✅ PASS | full lifecycle |
| 13 | `repo/stat`, `repo/version`, `repo/gc`, `refs/local` | ✅ PASS | |
| 14 | `dht/provide` (explicit) | ✅ PASS | `{"OK":true}` on both nodes (~60 s cold, faster warm) |
| 15 | internal `_bg_provide` after `add` | ✅ PASS | "Successfully advertised to 15 peers" (30→90 s fix holds in prod) |
| 16 | `debug/routing_table` | ✅ PASS | 3441–3790 peers after restart (warm-up working) |
| 17 | `debug/peerstore` | ✅ PASS | 5688 persisted peers |
| 18 | `swarm/peers` | ✅ PASS | 484–560 conns |
| 19 | `swarm/connect` / `swarm/disconnect` | ✅ PASS | |
| 20 | `swarm/tags` set/list/remove | ✅ PASS | generic conn-manager tagging |
| 21 | `swarm/protect` / `unprotect` / `protection` | ✅ PASS | pruner-level protection |
| 22 | `pubsub/ls` | ✅ PASS | shows auto-joined topics incl. `/ipns/<id>` |
| 23 | `name/publish` (IPNS) | ❌ FAIL | see RCA-2 (two bugs) |
| 24 | `name/resolve` (self) | ✅ PASS | resolves from local record instantly |
| 25 | `/metrics`, `/debug/metrics/prometheus` | ✅ PASS | Prometheus exposition |
| 26 | `/debug/conns`, `/debug/memory`, connection/stream stats | ✅ PASS | |
| 27 | Persistence across restart | ✅ PASS | peer ID stable (seed file), peerstore.db survives, routing table warms (256 inserted → 3400+) |
| 28 | Mutual peer protection under churn | ✅ PASS | mesh-peer tag held through DHT flood |

### Not exposed over HTTP (feature gaps vs Kubo, not defects)
- `pubsub/pub`, `pubsub/sub`, `pubsub/show` — only `ls` exists; gossipsub runs internally (auto-topic join works)
- CAR export/import — implemented in library (`car.py`, `peer.export_car`) but no HTTP route
- `refs <cid>` (per-object), `ls` (unixfs directory listing), `get` (tar download), `files/*` MFS API

---

## 1b. GAP-FIX ROUND (2026-08-24, commits ffb791bd → f6c11c07)

New endpoints implemented, deployed to both nodes, and tested:

| Endpoint | Status | Evidence |
|---|---|---|
| `POST /api/v0/pubsub/pub?arg=<topic>` | ✅ PASS | publishes raw body; returns `{Topic, Size}` |
| `GET/POST /api/v0/pubsub/sub?arg=<topic>&count&timeout` | ✅ PASS (same-node + DEV→PROD) | receive loops buffer into per-topic ring buffer; drain-once semantics; base64 payload verified |
| `DELETE /api/v0/pubsub/sub?arg=<topic>` | ✅ PASS | unsubscribes cleanly |
| `GET/POST /api/v0/dag/export?arg=<cid>` | ✅ PASS | 700 KB multi-chunk DAG exported as valid CAR |
| `POST /api/v0/dag/import` | ✅ PASS | re-imported CAR returns correct root CID |
| `GET/POST /api/v0/refs?arg=<cid>[&recursive]` | ✅ PASS | 3 chunk links listed with names/sizes |
| `GET/POST /api/v0/ls?arg=<dir-cid>` | ✅ PASS | unixfs directory entries with Name/Hash/Size |

**Additional fixes shipped in this round:**
- `60d0079` — ls crashed on `unixfs.Type != 1` (attr is `.type`, value `"directory"`)
- `c9ed4ab`/`a50d9c6` — pubsub `from` field now decodes protobuf bytes to a peer-ID string
- `f6c11c0` — **`IPFS_LITE_CONN_MGR_INBOUND_SLOTS` was a dead env var** (never read by code). Now wired through `Config.conn_mgr_inbound_slots`; DEV limiter confirmed at 350 slots after deploy
- PROD restart script was missing `IPFS_LITE_ENABLE_PUBSUB=1` — added

### NEW DEFECT discovered while testing cross-node pubsub (RCA-4)

**Symptom:** gossipsub delivery is asymmetric — DEV→PROD delivers; PROD→DEV silently drops.

**Root cause chain:**
1. The adaptive topic scorer auto-joined both nodes to high-traffic public blockchain topics (`harmony/0.0.1/node/shard/1`, `/beacon`, etc.)
2. PROD relays that flood toward DEV
3. DEV's gossipsub **rate limiter penalizes PROD** — 219 rate-limit hits against PROD's peer ID in 20 min (13,473 total events)
4. Legitimate messages from PROD on unrelated small topics get dropped as collateral

**Recommended fix options:**
- Raise/exempt per-peer rate limits for protected (`mesh-peer` tagged) peers
- Exclude known-high-volume public topics from auto-join (blocklist or min-peers threshold raise)
- Or disable `pubsub_auto_join_min_peers` on production nodes

Also observed: `swarm/connect` can return success from a stale one-sided registry entry after the remote node restarted, without re-dialing (workaround: explicit `swarm/disconnect` first). Filed as RCA-5 for follow-up.

---

## 2. Defects with RCA

### RCA-1 — Cross-node `cat` intermittently times out (404 "Block not found")

**Observed:** First PROD-added CID fetched from DEV failed after exactly 30 s; identical retry succeeded in **1.5 s**.

**Root cause (two compounding factors):**
1. `/api/v0/cat` accepts **no timeout parameter**; `get_file_stream` uses `config.default_timeout = 30 s`. A cold Kademlia provider walk on a node with a 3400+-peer routing table routinely exceeds 30 s before any WANT-BLOCK is even sent. The 30 s is consumed by `find_providers`, so the failure surfaces misleadingly as "Block not found".
2. During the failure window, DEV's inbound admission limiter was saturated (**250/250 live inbound**, 3498 `[INBOUND_LIMITER_DENY]` warnings in 10 min from public DHT flood). A fresh `swarm/connect` from the requester can be rejected *after* transport handshake succeeds, leaving a one-sided connection that bitswap broadcast cannot use.

**Why retry succeeds:** provider record has propagated + an existing/outbound connection to the provider exists by then.

**Fix options (in order of preference):**
- Expose `timeout` query param on `/api/v0/cat` (Kubo parity) and pass through to `get_file`
- Raise the effective fetch budget (e.g., 90 s like `dht/provide`) or make it adaptive
- Longer term: reserve admission slots for tagged/protected peers so known-good peers are never rejected by the inbound limiter

### RCA-2 — `name/publish` fails with empty error message

**Observed:** `{"detail":"Failed to publish IPNS record: "}` — empty exception string.

**Root cause (two distinct bugs):**
1. **Same class as the previously fixed `_bg_provide` bug:** `publish_name()` wraps the DHT announcement (`ipns_publish` → `put_value`) in `trio.fail_after(default_timeout)` = **30 s** (peer.py:2145). On a loaded DHT the signed-record STORE RPCs exceed 30 s → `trio.TooSlowError` whose `str()` is empty → uninformative log + 500. The local record IS written (which is why `name/resolve` self still works) but remote announcement silently fails.
2. **Non-compliant record value:** the API passes the raw CID (`bafkrei…`) as the IPNS value instead of `/ipfs/<cid>`. libp2p logs `IPNS value doesn't start with expected prefix (/ipfs/, /ipns/, /dnslink/)` (records/ipns). Even when publish succeeds, spec-compliant resolvers would reject the record.

**Fix:** give the publish step its own 90 s budget + explicit `TooSlowError` handling (mirror the `_bg_provide` fix), and normalize value to `/ipfs/<arg>` in `naming_service.publish_name`.

### RCA-3 — `block/put` form-field name deviates from Kubo

**Observed:** Kubo-style `curl -F data=@file` → 422 `{"detail":[{"loc":["body","file"],"msg":"Field required"}]}`; `-F file=@file` works.

**Root cause:** FastAPI handler declares `UploadFile` with alias `file`, while Kubo's `block/put` expects multipart field **`data`** (and response key `Key`, which py-ipfs-lite matches correctly).

**Fix:** accept both field names (`data` primary for Kubo parity, `file` fallback).

---

## 3. Test-harness false alarms (for the record)
- Initial script reported `add`, `block/put`, `/metrics`, `/debug/conns`, tags/protection listing as FAIL — all traced to harness issues (hand-rolled multipart, POST to GET-only routes, missing `/api/v0` prefix). Re-verified manually with curl: all pass.

## 4. Environment observations during testing
- DEV inbound limiter sits pinned at cap (250/250) under continuous public DHT flood; denials are legitimate admission control and now visibly logged (`[INBOUND_LIMITER_DENY]`).
- Routing-table warm-up fired correctly on both nodes after restart ("inserted 256/256 persisted peers").
