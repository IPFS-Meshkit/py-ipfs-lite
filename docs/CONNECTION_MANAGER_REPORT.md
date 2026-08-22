# Connection Manager — Deep-Dive Audit Report

**Date:** 2026-08-19 · **Node:** `py-ipfs-lite-dev` container `ipfs-node` (rebooted 09:31 UTC)
**Config (env):** `IPFS_LITE_CONN_MGR_LOW_WATER=400`, `HIGH_WATER=600`
**Code:** py-libp2p (sibling repo `/Users/sumanjeet/code/py-libp2p`) + `py_ipfs_lite`
**Reference:** `go-libp2p/p2p/net/connmgr/connmgr.go` (incl. braidpool fork's AutoConnect)

---

## 1. How the connection manager works (mental model)

### 1.1 Components

```
                 ┌────────────────────────────────────────────────────┐
   DHT / bitswap │  libp2p Network (Swarm)                            │
   streams       │                                                    │
        │        │  connections: dict[PeerID → list[SwarmConn]]       │
        ▼        │                                                    │
  SwarmConn ─────┴── add_conn() ──► ConnectionPruner.maybe_prune()    │
  (muxed QUIC/                                                         │
   TCP conn)     │  AutoConnector (background loop)                    │
                 │    │                                                │
                 │    │ every 10s (critical) / 30s (normal)            │
                 │    ▼                                                │
                 │  count < target = (400+600)/2 = 500 ?               │
                 │    └─► pick candidates from peerstore               │
                 │        └─► dial up to 16/32 per cycle (50ms stagger)│
                 │                                                      │
                 │  notify_disconnected() ──► record_disconnect(60s    │
                 │                              backoff) + schedule    │
                 │                              maybe_connect (λ5s)    │
                 └────────────────────────────────────────────────────┘
```

**Swarm** owns the connection table (`swarm.py:435` `get_connections()`). A `SwarmConn`
wraps the muxed connection with `_created_at`, `event_closed`, `streams`, direction.

**ConnectionPruner** (`libp2p/network/connection_pruner.py`) is the *high-water side*:
- Trigger: every `add_conn()` (`maybe_prune_connections`, `swarm.py:2333`) — **but only acts when `conns > high_watermark`** (line 239).
- Sorts connections: temp-flag → tag score (lowest first) → stream count → direction → age; skips grace-period (< 20 s), active streams, positive tags, protected peers, allow-list.
- Closes at most **20 per cycle** (line 266-267) toward `low_watermark`.

**AutoConnector** (`libp2p/network/auto_connector.py`) is the *low-water side* (this is the braidpool-fork-style AutoConnect behavior):
- Periodically computes `target = midpoint(low, high)` (line 299-302 — **500** here).
- If `conns + in_flight < target`, takes all non-connected peers from the peerstore that have at least one "direct" (routable, non-relay, transport-supported) address (`_get_candidate_peers`, filter private-only when node is public), shuffles, dials in batches of **16 (normal) or 32 (needed > 32)** with a `CapacityLimiter`, 50 ms stagger, `dial_timeout=10 s` per peer.
- Per-peer exponential cooldown: 5 s → 15 s → 40 s → … capped **300 s** (capped 60 s when critically below `min_connections=400`).
- On disconnect: `record_disconnect()` → peer is **not re-dialable for 60 s** (`swarm.py:2989`).
- `min_connections` (400) is the *critical floor*: below it the poll interval drops to 10 s (`auto_connector.py:185,233-236`).

**TagStore** (`libp2p/network/tag_store.py`): weighted tags, `Protect`/`is_protected`, temp entries reaped after grace period (`connection_pruner.py:364`).

**ConnectionStatsTracker** (`py_ipfs_lite/connection_tracker.py`): INotifee that records every connect/disconnect, duration, transport, reason hint; powers `/debug/connection-stats`.

**Keep-alive** (`py_ipfs_lite/peer.py:852` `_keep_alive_loop`): every 180 s, pings at most **5** idle peers via `PingService`, 500 ms apart, `Semaphore(20)`.

**Watermark ladder (current node):**

| Level | Value | Effect |
|---|---|---|
| `min_connections` | 400 | critical floor → 10 s poll below it |
| `low_watermark` | 400 | prune target; disconnect→auto-connect gate |
| target (midpoint) | 500 | what the auto-connector actually aims for |
| `high_watermark` | 600 | prune trigger (batch 20/cycle toward 400) |
| rcmgr `max_connections` | 2400 (`max(600×4, 800)`, peer.py:509) | hard dial/accept cap |
| QUIC idle timeout | 600 s (peer.py:473) | local close of idle conns |

### 1.2 What the manager does NOT do
- It does **not** dial above target; it only prunes above high water.
- There is **no age-based eviction** anywhere (no `AgeWindow` logic).
- It does **not** keep connections alive — keep-alive is a separate 5-peers-per-3-min loop in the app layer.

---

## 2. Q2: Why can't the node reach even the low water (400)?

### 2.1 Live evidence (taken 15:00 UTC, ~5.5 h uptime)

| Metric | Value |
|---|---|
| Peers in persistent peerstore | **5,408** (4,275 with ≥1 public-IP addr; QUIC/TCP/WS addrs 30k/20k/3.7k) |
| Candidates for auto-connect | ~4,000+ (non-connected public peers) |
| Connect events since boot | **105,946** |
| Disconnect events since boot | **105,580** |
| Current active | **376** (outbound-dominant; inbound ~15–20%) |
| Avg connection lifespan | **50.0 s** |
| Live age breakdown (376) | `<2m: 171 · 2–5m: 14 · 5–10m: 16 · 10–30m: 26 · >30m: 8` |
| Disconnect lifespan (last 500) | `<5s: 2 · 5–35s: 435 · 35s–2m: 41 · 2–5m: 8 · 5–10m: 6 · 10–30m: 6 · >30m: 1` |
| Disconnect reasons (last 500) | `remote_closed_or_idle: 493 · idle_timeout: 7` |

**The node is not failing to connect — it connects 105,946 times and loses 105,580 of them.** Churn, not dial failure, is the ceiling.

### 2.2 The churn math

Sustained connection count is an equilibrium:

```
dN/dt = connect_rate − N / avg_lifespan
```

- To *hold* 400 connections with lifespan ~50 s you need a sustained connect rate of **8/s** — **1.9–6.4 s average dial completion** (QUIC handshake + Noise + multistream ≈ 1–3 s in the best case).
- Actual connect rate: 105,946 / ≈19,800 s ≈ **5.3/s** — below the 8/s needed → equilibrium settles at **N ≈ 5.3 × 50 ≈ 265…376 observed**.
- The auto-connector can *replenish* at most **32 dials per 10 s cycle ≈ 3.2/s** `auto_connector.py:357-359` — a hard replenishment ceiling below the death rate at 4-500.

### 2.3 Root causes (in order of impact)

**A. No transport-level keepalive → connections die of idle.**
Verified: the QUIC transport (`libp2p/transport/quic/`) has **zero keepalive** — `aioquic` exposes only `idle_timeout` (default 600 s), no PING-frame timer; the config fields `stream_keep_alive*` (`quic/config.py:178-181`) are **never consumed**. The comment in `peer.py:888` ("Transport-level keep-alives (QUIC PING frames, Yamux PING frames) maintain underlying connections") describes a mechanism that **does not exist**. Every connection that is not actively used idles out; the remote closes it (kubo/enodal nodes trim idle conns) or our 600 s QUIC idle timer eventually fires. The 5–35 s DHT-hop bucket (435/500) is exactly this: connect → serve 1–2 DHT queries → idle → dead.

**B. The app keepalive starves: only 5 peers per 3 minutes.**
`peer.py:889-892`: `peers_to_ping = [...][:5]` once per 180 s sweep. With ~376 connections that's **1.6 % coverage per sweep** — a connection receives a heartbeat at most once every ~3.7 h. It cannot keep 400 connections alive.

**C. One-shot connections are never reused.**
Connections are opened, serve 1–2 DHT queries (`streams_served: 1-2`, `protocols: []` from tracker records), then nothing ever opens a stream over them again. Nothing keeps the working set warm except brief query bursts.

**D. Disconnect → 60 s re-dial backoff.**
`swarm.py:2989` `record_disconnect()` blocks re-dialing the peer just lost for 60 s; at a death rate of 5.3/s that's ~300 peers in backoff — constantly shrinking the dialable pool between cycles, on top of per-peer failure cooldowns (up to 300 s).

**E. (Not the cause — ruled out) the pruner.**
`ConnectionPruner` only fires above high water (600); the node sits at ~350–380, so **it never trims** (`connection_pruner.py:239`). There is also no age-based ("30 min") eviction anywhere in the code. `IPFS_LITE_CONN_MGR_INBOUND_SLOTS=350` is set in the container env but **never read by any code**.

---

## 3. Q3: Why are connections older than 30 min nearly zero?

Same retention story, from the tracker:

- **Live:** only **8 of 376** connections are older than 30 min (`over_30m (long-lived stable): 8`); 171 are under 2 minutes.
- **History:** only **1 of the last 500 disconnections** survived past 30 min (`over_30m (stable peers): 1`); 435 survived under 35 s.
- The node does **not** prune them (`pruner never fires at 376 < 600`), and there is **no 30-min age eviction** in py-libp2p (nor in modern go-libp2p's connmgr — its `AgeWindow` feature was removed upstream).

The mechanism is: a connection is only as old as its last activity. 99 % of connections perform one DHT hop and go idle; the remote then closes them (10–60 s) or the 600 s QUIC idle timer eventually does. The handful that survive (8/376) are the working set that keeps receiving traffic — DHT/bitswap reuse, inbound streams, or the occasional keepalive ping.

---

## 4. py-libp2p connmgr vs go-libp2p connmgr — complete comparison

| Feature | go-libp2p `connmgr.go` | py-libp2p (current) | Gap? |
|---|---|---|---|
| Watermarks (low/high) | `lowWater`/`highWater` (kubo: 300/600 or 400/600) | 400/600 via env (defaults 50/100) | parity |
| Prune trigger | background `trim()` every `CheckInterval` (30–120 s) | on every `add_conn()` only | py prunes on-every-connect; go on-timer — py ok since connect is the only growth path, but inbound floods also trigger py prunes (go's does not) |
| Prune batch / convergence | trims all the way to `lowWater` each cycle | **capped at 20 per cycle** (`connection_pruner.py:266-267`) | gap — slow convergence back to 400 if flooded to 600+ |
| Grace period | 10 s (30 s newer versions) | 20 s (`GRACE_PERIOD`, config.py:29) | parity |
| Age-based eviction (`AgeWindow`) | removed upstream (braidpool fork kept it) | none | parity (modern go) |
| Connection age source | `established` timestamp from notifee `Connected` | `_created_at` set at SwarmConn construction | minor — counts handshake time as age |
| Sort key | tag score (lowest first), then connection age | temp → tag score → streams → direction → age | near-parity; py additionally protects in-use connections (go relies on tags) |
| `Protect`/`Unprotect` | yes (`ProtectWithTag`) | yes (`tag_store.is_protected` + pruner skip) | parity |
| Weighted tags w/ expiry | `upsertTag` (weight, expiry) | tags + temp entries; no expiry | minor gap — no tag expiry |
| Selective trimming by tags (`SelectPeers`) | yes (scan for tag values) | metric collection for scoring; explicit protect | parity (py uses score order) |
| AutoConnect (dialing when below low-water) | **not in upstream go connmgr** — only in the braidpool fork (background task, dials via peerstore/DHT) | full AutoConnector w/ candidate filtering, cooldowns, batches | py has more machinery than upstream go; mirrors braidpool fork |
| Dial concurrency | braidpool fork: dial limiter (~10) | `CapacityLimiter(16/32)` + 50 ms stagger | parity-class; py bigger batches |
| Keepalive duty | **none in go** — but go connections survive because go-swarm never idle-closes, muxers keep streams, and kubo traffic reuses them | **absent too**, and py connections idle-close fast (no reuse, no pings) | **the real gap is here, at the host/swarm level, not the connmgr** |
| Per-peer connection limit | 1 by default (swarm dedup) | `max_connections_per_peer=3` + dedup of duplicate muxed conns (`_shared_muxed_conn`) | py allows up to 3, then dedups |
| Idle-close of connections | go-swarm doesn't close idle conns (only transport errors / explicit close) | QUIC 600 s idle timeout + remote closes; app keepalive only 5 peers/3 min | **primary divergence** |
| Reconnection on death | none (go doesn't re-dial dropped conns; relies on ambient dials) | record_disconnect 60 s backoff + periodic redial (max 32/cycle) | parity of *absence*, but py pays a 60 s penalty that delays recovery |
| Statistics/telemetry | bare (GetInfo/Dump) | rich: `/debug/connection-stats`, metrics, reasons | py exceeds go |

### 4.1 What is genuinely missing in py-libp2p's connmgr vs go-libp2p
1. **No convergent trimming under inbound floods** — 20/cycle cap means sustained >600 connections if inbound ever floods (go trims to low water in one pass).
2. **No tag expiry** (`upsertTag` weight lifetime) — tags accumulate until the temp reaper clears them.
3. **No `CMInfo`-style exposure of `last_trim`/trim history** — `swarm.get_conn_mgr_info()` exists but `/debug/connmgr` returns 404 (not wired into the API).
4. **Keepalive is not owned by the connmgr at all** — go-libp2p doesn't need it (connections don't idle-close); py-libp2p does but the 5-per-3-min app loop is far too weak.

---

## 5. Recommended fixes (prioritized)

1. **Real keepalive (highest leverage).** Rotate pings over *all* idle connections, not 5 per 3 min: e.g. sweep every 60 s, ping up to 100 idle conns with `Semaphore(25)`, 100 ms spacing — full coverage of ~400 conns in ≈4 cycles. Alternatively implement a transport-level timer (aioquic has no PING API; opening a lightweight stream ~30 s before the remote's expected idle close is the pragmatic route). Target: lifespan 50 s → 5+ min.
2. **Remove/relax the 60 s disconnect backoff** for peers we only just successfully dialed — reuse the existing success-clears-cooldown logic: on disconnect of a peer dialed < 5 min ago, allow immediate re-dial (or backoff 10 s). This alone lifts replenishment toward the death rate.
3. **Scale dial batch with need:** `batch = min(needed, 64)` + keep 50 ms stagger — pushes replenishment ceiling from 3.2/s to ~6/s when critically low.
4. **Trim convergence:** raise the pruner cap from 20 to a floor-of-`(conns − low_water)/4` so floods above 600 converge in 4 cycles.
5. **Wire `/debug/connmgr`** to `CMInfo` (low/high/connected/grace/last_trim) for operational verification of pruning activity.
6. **Reuse the working set:** tag peers used within the last ~10 min (`DHT`-warm tag) so the pruner never kills them and the keepalive prioritizes them first.

**Bottom line:** the connection manager logic is sound and matches go-libp2p; the node's failure to stay above 400 is a **retention** problem (idle-death without keepalive + replenishment ceiling vs. 5.3/s churn), not a pruning problem — and there is no 30-min pruning cliff; connections simply never survive that long because they are abandoned after one DHT hop.