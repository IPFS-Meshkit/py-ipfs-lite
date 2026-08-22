#!/usr/bin/env python3
"""
Benchmark: measure CPU cost of QUIC connections (matching production dual-stack).

Phases:
  0) Bootstrap to seed nodes (TCP) for peer discovery
  1) DIAL phase  – connect to N peers concurrently over QUIC, measure CPU time
  2) IDLE phase  – hold connections open, sample CPU every 1s to measure steady-state

Key difference from TCP: QUIC runs aioquic event loop per connection (2-3 trio tasks each).
We measure CPU at different connection counts to find the per-connection cost.
"""
import logging
import os
import sys
import time
import resource
import statistics

import trio

from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.peer.id import ID
from libp2p.peer.peerinfo import info_from_p2p_addr
from multiaddr import Multiaddr

from py_ipfs_lite.config import Config
from py_ipfs_lite.peer import Peer

logging.basicConfig(level=logging.WARNING, stream=sys.stderr)
logging.getLogger("libp2p.network.transport_manager").setLevel(logging.ERROR)

# Bootstrap nodes (TCP only) — used for initial peer discovery
SEED_PEERS = [
    "/ip4/51.81.93.51/tcp/4001/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
    "/ip4/147.135.44.132/tcp/4001/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
    "/ip4/15.235.144.210/tcp/4001/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
    "/ip4/54.38.47.166/tcp/4001/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
]

# Sweep: test at these connection counts
TARGET_COUNTS = [20, 50, 100, 200]
IDLE_SECONDS = 30
SAMPLE_INTERVAL = 1.0  # CPU sample every 1s


def cpu_seconds() -> float:
    u = resource.getrusage(resource.RUSAGE_SELF)
    return u.ru_utime + u.ru_stime


async def sample_cpu(nursery, samples, stop_event):
    """Sample CPU usage every SAMPLE_INTERVAL until stop_event is set."""
    prev_cpu = cpu_seconds()
    prev_wall = time.monotonic()
    while not stop_event.is_set():
        await trio.sleep(SAMPLE_INTERVAL)
        cur_cpu = cpu_seconds()
        cur_wall = time.monotonic()
        dt_cpu = cur_cpu - prev_cpu
        dt_wall = cur_wall - prev_wall
        pct = dt_cpu / dt_wall * 100 if dt_wall > 0 else 0
        samples.append((cur_wall, pct, dt_cpu, dt_wall))
        prev_cpu = cur_cpu
        prev_wall = cur_wall


def count_alive(raw_host) -> int:
    alive = 0
    for net_conn in raw_host.get_network().connections.values():
        for c in net_conn:
            if not c.is_closed:
                alive += 1
    return alive


async def discover_peers(raw_host, seed_infos, needed: int) -> list:
    """Discover peers via peerstore + DHT."""
    all_peer_infos = list(seed_infos)
    seen_ids = {info.peer_id for info in seed_infos}

    try:
        peerstore = raw_host.get_peerstore()
        for pid in peerstore.get_all_keys():
            if pid not in seen_ids:
                addrs = peerstore.get_addrs(pid)
                if addrs:
                    from libp2p.peer.peerinfo import PeerInfo
                    all_peer_infos.append(PeerInfo(pid, addrs))
                    seen_ids.add(pid)
    except Exception:
        pass

    # DHT walk to find more
    for info in seed_infos[:3]:
        if len(all_peer_infos) >= needed:
            break
        try:
            closest = await raw_host.get_routing_table().find_nearest(info.peer_id)
            for pi in closest:
                if pi.peer_id not in seen_ids:
                    seen_ids.add(pi.peer_id)
                    all_peer_infos.append(pi)
                    if len(all_peer_infos) >= needed:
                        break
        except Exception:
            pass

    return all_peer_infos[:needed]


async def run_benchmark(target_count: int):
    """Run dial + idle benchmark for a given connection count."""
    print()
    print("=" * 70)
    print(f"  BENCHMARK: QUIC connections — target {target_count}")
    print("=" * 70)
    print()

    tmp_dir = f"/tmp/bench_quic_{os.getpid()}_{target_count}"
    os.makedirs(tmp_dir, exist_ok=True)
    config = Config(blockstore_path=f"{tmp_dir}/blocks")

    host_key = create_new_key_pair()
    peer = Peer(
        config=config,
        host_key=host_key,
        listen_addrs=["/ip4/0.0.0.0/tcp/0", "/ip4/0.0.0.0/udp/0/quic-v1"],
    )

    print("  Starting peer (dual-stack TCP + QUIC)...")
    await peer.start()
    raw_host = getattr(peer.host, "_host", peer.host)
    my_id = ID.from_pubkey(host_key.public_key)
    print(f"  Peer ready: {my_id}")
    print()

    # ── Phase 0: Bootstrap ──────────────────────────────────────────────
    print("  Phase 0: Bootstrapping to seed nodes (TCP)...")
    seed_infos = []
    for addr_str in SEED_PEERS:
        try:
            seed_infos.append(info_from_p2p_addr(Multiaddr(addr_str)))
        except Exception:
            pass

    connected = 0
    async with trio.open_nursery() as nursery:
        async def try_connect(info):
            nonlocal connected
            try:
                with trio.fail_after(10):
                    await raw_host.connect(info)
                connected += 1
            except Exception:
                pass
        for info in seed_infos[:6]:
            nursery.start_soon(try_connect, info)

    print(f"  Connected to {connected}/{len(seed_infos)} seed nodes")

    # Discover more peers
    all_peers = await discover_peers(raw_host, seed_infos, needed=target_count + 50)
    print(f"  Discovered {len(all_peers)} peers total")
    print()

    # ── Phase 1: DIAL N peers ──────────────────────────────────────────
    # Disconnect seed nodes first so we only measure QUIC dial cost
    print("  Disconnecting seed nodes to measure fresh QUIC dial...")
    for net_conn in list(raw_host.get_network().connections.values()):
        for c in list(net_conn):
            if not c.is_closed:
                try:
                    await c.close()
                except Exception:
                    pass

    targets = all_peers[:target_count]
    print(f"  Phase 1: Dialing {len(targets)} peers in parallel (QUIC)...")
    print()

    cpu_before = cpu_seconds()
    wall_before = time.monotonic()
    results = {"ok": 0, "fail": 0}

    async def dial_one(info, idx):
        try:
            with trio.fail_after(15):
                await raw_host.connect(info)
            results["ok"] += 1
        except Exception:
            results["fail"] += 1

    async with trio.open_nursery() as nursery:
        for idx, info in enumerate(targets):
            nursery.start_soon(dial_one, info, idx)

    wall_after = time.monotonic()
    cpu_after = cpu_seconds()

    wall_dial = wall_after - wall_before
    cpu_dial = cpu_after - cpu_before
    alive = count_alive(raw_host)

    print(f"  DIAL RESULTS:")
    print(f"    Success:     {results['ok']}/{len(targets)}")
    print(f"    Wall time:   {wall_dial:.2f}s")
    print(f"    CPU time:    {cpu_dial:.2f}s")
    print(f"    CPU/Wall:    {cpu_dial/wall_dial:.1f}x")
    if results["ok"] > 0:
        print(f"    CPU/dial:    {cpu_dial/results['ok']*1000:.0f}ms per dial")
    print()

    # ── Phase 2: IDLE with CPU sampling ────────────────────────────────
    print(f"  Phase 2: Idle for {IDLE_SECONDS}s with 1s CPU sampling...")
    samples = []
    stop_event = trio.Event()

    async with trio.open_nursery() as nursery:
        nursery.start_soon(sample_cpu, nursery, samples, stop_event)

        # Let it run for IDLE_SECONDS then stop
        await trio.sleep(IDLE_SECONDS)
        stop_event.set()

    alive = count_alive(raw_host)

    # Analyze samples
    if samples:
        pcts = [s[1] for s in samples]
        avg_cpu = statistics.mean(pcts)
        max_cpu = max(pcts)
        min_cpu = min(pcts)
        stdev_cpu = statistics.stdev(pcts) if len(pcts) > 1 else 0
        total_cpu_time = sum(s[2] for s in samples)
        total_wall_time = sum(s[3] for s in samples)

        print(f"  IDLE RESULTS:")
        print(f"    Alive conns:      {alive}")
        print(f"    Samples:          {len(samples)}")
        print(f"    Avg CPU:          {avg_cpu:.1f}%")
        print(f"    Min CPU:          {min_cpu:.1f}%")
        print(f"    Max CPU:          {max_cpu:.1f}%")
        print(f"    Stdev CPU:        {stdev_cpu:.1f}%")
        print(f"    Total CPU time:   {total_cpu_time:.2f}s")
        print(f"    Total wall time:  {total_wall_time:.1f}s")
        if alive > 0:
            print(f"    Per-conn CPU:     {avg_cpu/alive:.3f}% per conn")
            print(f"    Per-conn overhead:{total_cpu_time/alive*1000:.1f}ms per conn per {IDLE_SECONDS}s")
        print()

        # Print timeline
        print("    Timeline (1s samples):")
        for i, (t, pct, dt_cpu, dt_wall) in enumerate(samples):
            bar = "#" * int(pct / 2)
            print(f"      t+{i:3d}s: {pct:5.1f}% CPU  {bar}")
        print()
    else:
        print("  No CPU samples collected!")
        print()

    # ── Summary ────────────────────────────────────────────────────────
    print("=" * 70)
    print(f"  SUMMARY for {target_count} connections")
    print("=" * 70)
    print(f"  Dial CPU:      {cpu_dial:.2f}s ({results['ok']} dials, {wall_dial:.1f}s wall)")
    if results["ok"] > 0:
        print(f"  CPU per dial:  {cpu_dial/results['ok']*1000:.0f}ms")
    if samples:
        print(f"  Idle CPU avg:  {avg_cpu:.1f}% ({alive} connections)")
        if alive > 0:
            print(f"  Per-conn CPU:  {avg_cpu/alive:.3f}%")
            print(f"  Projected 200: ~{avg_cpu/alive*200:.1f}% CPU")
            print(f"  Projected 500: ~{avg_cpu/alive*500:.1f}% CPU")
    print("=" * 70)
    print()

    await peer.close()
    return alive, cpu_dial, wall_dial, avg_cpu if samples else 0


async def main():
    print()
    print("=" * 70)
    print("  QUIC BENCHMARK: Measure per-connection CPU cost")
    print("  Testing at N = " + ", ".join(str(n) for n in TARGET_COUNTS))
    print("=" * 70)
    print()

    results = {}
    for target in TARGET_COUNTS:
        alive, cpu_dial, wall_dial, avg_idle_cpu = await run_benchmark(target)
        results[target] = {
            "alive": alive,
            "dial_cpu": cpu_dial,
            "dial_wall": wall_dial,
            "idle_cpu_pct": avg_idle_cpu,
        }

    # ── Final comparison table ──────────────────────────────────────────
    print()
    print("=" * 70)
    print("  FINAL RESULTS: CPU vs Connection Count")
    print("=" * 70)
    print(f"  {'Target':>8}  {'Alive':>6}  {'Dial CPU':>10}  {'Idle CPU%':>10}  {'Per-Conn%':>10}")
    print("-" * 70)
    for target in TARGET_COUNTS:
        r = results[target]
        per_conn = r["idle_cpu_pct"] / r["alive"] if r["alive"] > 0 else 0
        print(f"  {target:>8}  {r['alive']:>6}  {r['dial_cpu']:>9.2f}s  {r['idle_cpu_pct']:>9.1f}%  {per_conn:>9.3f}%")
    print("=" * 70)

    # Check if per-conn CPU is constant (event-loop dominated) or grows
    if len(results) >= 2:
        first_key = TARGET_COUNTS[0]
        last_key = TARGET_COUNTS[-1]
        r1 = results[first_key]
        r2 = results[last_key]
        if r1["alive"] > 0 and r2["alive"] > 0:
            pc1 = r1["idle_cpu_pct"] / r1["alive"]
            pc2 = r2["idle_cpu_pct"] / r2["alive"]
            ratio = pc2 / pc1 if pc1 > 0 else float("inf")
            print()
            print(f"  Per-conn CPU ratio ({last_key} vs {first_key}): {ratio:.2f}x")
            if ratio < 1.5:
                print("  -> Per-conn CPU is CONSTANT: bottleneck is event-loop overhead, not per-conn work")
            else:
                print(f"  -> Per-conn CPU SCALES {ratio:.1f}x: bottleneck is per-connection work")
    print()


if __name__ == "__main__":
    # Raise FD limit
    try:
        import resource as _rl
        _rl.setrlimit(_rl.RLIMIT_NOFILE, (65536, 65536))
    except Exception:
        pass
    trio.run(main)
