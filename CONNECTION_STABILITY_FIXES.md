# py-ipfs-lite / py-libp2p — Connection Stability Fixes

This document summarises every issue discovered and fixed during the
connection-stability audit session.

---

## 1. Keep-alive loop crashing and evicting peers

**Commit:** `25507dcb` — `fix(peer): fix keep-alive loop crashing and evicting peers`
**File:** `py_ipfs_lite/peer.py`

### Issue

`Peer._keep_alive_loop` created a single `PingService` instance at startup
and reused it across all keep-alive cycles.  On the **second** cycle
`PingService.ping_iter` found a cached outbound stream and called
`stream.is_closed()` on it — but the Yamux `NetStream` type has no
`is_closed` attribute, raising:

```
'NetStream' object has no attribute 'is_closed'
```

`_ping_peer` caught that exception and then called
`network.peerstore.clear_addrs(peer_id)`, **permanently wiping the peer's
known addresses** and making reconnection impossible.  The connection
dropped after ~30 s on every run.

### Fix

1. Create a **fresh `PingService`** inside `_ping_peer` on every call so
   `_outbound_streams` is always empty and `is_closed()` is never reached.
2. Remove `peerstore.clear_addrs()` from the failure path — on a ping
   failure only close the dead connection; retain addresses so the
   auto-connector can reconnect.

---

## 2. Keep-alive thundering herd after crash recovery

**Commit:** `923c15de` — `fix(peer): cap keep-alive ping concurrency to 20`
**File:** `py_ipfs_lite/peer.py`

### Issue

After a crash the auto-connector brought in 300–400 new peers at once.
The very next keep-alive cycle (15 s later) spawned one `_ping_peer` task
per connected peer with no concurrency limit — up to 400 simultaneous
QUIC streams were opened at the same time, producing a cascade of:

```
Error writing to stream 5: cannot call write() after reset()
```

### Fix

Gate all keep-alive pings behind a `trio.Semaphore(20)` so at most 20
QUIC ping streams are open at any point during a single cycle.

---

## 3. Resource manager graceful-degradation blocking recovery

**Commit:** `4f7a85f5` — `fix(peer): configure rcmgr to prevent graceful-degradation blocking recovery`
**File:** `py_ipfs_lite/peer.py`

### Issue

When the auto-connector fired 300+ simultaneous dials, each attempt
called `rcmgr.acquire_connection()` and incremented `_current_connections`.
Failed handshakes that did not release their scope caused the counter to
spike above the default `max_connections = 1000`.

`GracefulDegradation` then:

1. Reduced `max_connections` by 20% per trigger (levels 1–5).
2. After 5 reductions the limit was permanently halved to 500.
3. `_can_recover()` rarely succeeded because the degraded limit was already
   close to current usage.
4. End result: `degradation_level >= 5` → `handle_resource_exhaustion`
   returned `False` → **all new connections were blocked**, even when
   `num_connections = 1`.

Observed log:
```
AUTO_CONNECTOR_STATE: num_connections=1, low_watermark=300
the connection (1) is less the low limit (300) so connection manager is
initiating 299 number of new connections
Maximum degradation level reached for connections
  (suppressing repeated warnings for 60s)
```

### Fix

Pass a custom `ResourceManager` to `new_host` with:

- `max_connections = max(conn_mgr_high_water × 4, 4000)` — well above any
  realistic burst.
- `enable_graceful_degradation = False` — stops the self-inflicted limit
  reduction entirely.

---

## 4. Resource manager circuit breaker blocking recovery

**Commit:** `9407d3fe` — `fix(peer): disable rcmgr circuit breaker`
**File:** `py_ipfs_lite/peer.py`

### Issue

`CircuitBreaker(failure_threshold=5, timeout=60.0)` opens after just 5
failed `acquire_connection` calls and then **blocks all new connections for
60 seconds**.  After a crash the auto-connector floods 300+ dials; 5
failures (timeout, unreachable peer, etc.) trip the breaker and stall the
entire recovery for a minute on every cycle.

Additionally, `_on_failure()` is only triggered via `call()`, which is
never invoked from `acquire_connection` — so the circuit breaker was
half-broken by design but still capable of opening via other paths.

### Fix

Pass `enable_circuit_breaker = False` to `new_resource_manager`.

---

## 5. rcmgr `_current_connections` counter leaks (root-cause fix)

**Commit (py-libp2p):** `3a79b343` — `fix(swarm): eliminate rcmgr _current_connections counter leaks`
**Commit (py-ipfs-lite):** `73fefe34` — `fix(deps): pin libp2p to commit 3a79b343`
**File:** `libp2p/network/swarm.py`

### Issue

Fixes 3 and 4 above raised the ceiling and removed the self-inflicted
damage, but the **underlying counter leak** remained.
`_current_connections` can increment without a matching decrement whenever
a connection is acquired but its `ConnectionScope.close()` is never called.

Three concrete paths were identified:

#### Leak 1 — `add_conn`: `trio.Cancelled` at any await after `swarm_conn` is created

After the resource scope is set on `swarm_conn` (line 1873), the function
awaits five operations:

```
await muxed_conn.event_started.wait()
await getattr(muxed_conn, "_connected_event").wait()
await swarm_conn.event_started.wait()
await self.connection_pruner.maybe_prune_connections()
await self.notify_connected(swarm_conn)
```

`trio.Cancelled` can fire at any of these points.  At that moment
`swarm_conn` holds a live resource scope but is **not yet in
`self.connections`**, so the swarm's normal teardown never calls
`swarm_conn.close()`.  The `_current_connections` slot leaks permanently.

#### Leak 2 — `add_conn`: `SwarmException("Connection closed while starting")`

```python
if muxed_conn.is_closed:
    raise SwarmException("Connection closed while starting")
```

This raised without closing `swarm_conn` first, leaving its scope
unreleased.

#### Leak 3 — `upgrade_outbound_raw_conn`: silent `setattr` failure

```python
try:
    setattr(muxed_conn, "_resource_scope", conn_scope)
except Exception:
    pass   # ← conn_scope acquired but orphaned
```

If `setattr` raised, the bare `except Exception: pass` swallowed it.
`conn_scope` was acquired (`_current_connections += 1`) but never stored
anywhere, so `close()` was never called.

### Fix

**Leak 1 & 2** — wrap the entire post-creation block in `add_conn` with
`try/except BaseException` that calls `swarm_conn.close()` on any failure:

```python
try:
    self.manager.run_task(muxed_conn.start)
    await muxed_conn.event_started.wait()
    # ... all the awaits ...
    return swarm_conn
except BaseException:
    try:
        await swarm_conn.close()   # always releases the rcmgr slot
    except Exception:
        pass
    raise
```

**Leak 3** — on `setattr` failure, explicitly close `conn_scope` before
re-raising:

```python
try:
    setattr(muxed_conn, "_resource_scope", conn_scope)
except Exception:
    try:
        conn_scope.close()         # release the slot we just acquired
    except Exception:
        pass
    raise SwarmException("Failed to attach resource scope to muxed connection")
```

---

## Summary table

| # | Symptom | Root cause | Fix location |
|---|---------|-----------|--------------|
| 1 | Connection drops after ~30 s; peer evicted with addresses wiped | `PingService` stream cache → `AttributeError: is_closed` → aggressive eviction | `peer.py` — fresh `PingService` per call, remove `clear_addrs` |
| 2 | `write() after reset()` errors after crash recovery | 400 concurrent QUIC ping streams with no limit | `peer.py` — `Semaphore(20)` on keep-alive pings |
| 3 | All connections blocked with `num_connections=1` | `GracefulDegradation` ratcheted `max_connections` down to 500 and stuck there | `peer.py` — `enable_graceful_degradation=False`, `max_connections=4000` |
| 4 | Recovery stalls for 60 s after 5 failed dials | `CircuitBreaker` opens on 5 failures, blocks all `acquire_connection` calls | `peer.py` — `enable_circuit_breaker=False` |
| 5 | `_current_connections` drifts above live connection count permanently | Three paths in `swarm.py` where scope is acquired but `close()` never called | `swarm.py` — `try/except BaseException` guard in `add_conn`; explicit scope release on `setattr` failure |
