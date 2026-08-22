#!/usr/bin/env python3
"""Test 08: Diagnostics (metrics, stats, repo, memory, version)"""
import json

from tests_cross_instance import DEV_URL, PROD_URL
from tests_cross_instance.helpers import api

passed = 0
failed = 0
skipped = 0
results = []


def test(name, fn):
    global passed, failed, skipped
    try:
        result = fn()
        if result == "skip":
            skipped += 1
            results.append(("SKIP", name, ""))
            print(f"  SKIP  {name}")
        elif isinstance(result, str) and result.startswith("KNOWN ISSUE"):
            failed += 1
            results.append(("KNOWN ISSUE", name, result))
            print(f"  ISSUE {name} — {result}")
        else:
            passed += 1
            results.append(("PASS", name, result or ""))
            print(f"  PASS  {name}" + (f" — {result}" if result else ""))
    except AssertionError as e:
        failed += 1
        results.append(("FAIL", name, str(e)))
        print(f"  FAIL  {name}: {e}")
    except Exception as e:
        failed += 1
        results.append(("FAIL", name, f"{type(e).__name__}: {e}"))
        print(f"  FAIL  {name}: {type(e).__name__}: {e}")


def main():
    global passed, failed, skipped

    print()
    print("=" * 70)
    print("  TEST 08: DIAGNOSTICS")
    print("=" * 70)
    print()

    def test_version():
        s1, d1 = api(DEV_URL, "/api/v0/version")
        s2, d2 = api(PROD_URL, "/api/v0/version")
        assert s1 == 200 and s2 == 200, f"version failed: dev={s1} prod={s2}"
        v1 = d1.get("Version", "?")
        v2 = d2.get("Version", "?")
        return f"dev={v1} prod={v2}"

    test("8.1 Version", test_version)

    def test_id():
        s1, d1 = api(DEV_URL, "/api/v0/id")
        s2, d2 = api(PROD_URL, "/api/v0/id")
        assert s1 == 200 and s2 == 200, f"id failed: dev={s1} prod={s2}"
        id1 = d1.get("ID", "?")
        id2 = d2.get("ID", "?")
        return f"dev={id1[:16]}... prod={id2[:16]}..."

    test("8.2 ID", test_id)

    def test_repo_stat():
        s1, d1 = api(DEV_URL, "/api/v0/repo/stat")
        s2, d2 = api(PROD_URL, "/api/v0/repo/stat")
        assert s1 == 200 and s2 == 200, f"repo/stat failed: dev={s1} prod={s2}"
        n1 = d1.get("NumObjects", d1.get("numObjects", "?"))
        n2 = d2.get("NumObjects", d2.get("numObjects", "?"))
        return f"dev={n1} objects, prod={n2} objects"

    test("8.3 Repo stat", test_repo_stat)

    def test_repo_stat_size():
        s1, d1 = api(DEV_URL, "/api/v0/repo/stat")
        s2, d2 = api(PROD_URL, "/api/v0/repo/stat")
        assert s1 == 200 and s2 == 200, f"repo/stat failed"
        # Get repo size info
        keys1 = list(d1.keys()) if isinstance(d1, dict) else []
        keys2 = list(d2.keys()) if isinstance(d2, dict) else []
        return f"dev keys={keys1[:5]}, prod keys={keys2[:5]}"

    test("8.4 Repo stat details", test_repo_stat_size)

    def test_local_refs():
        s, d = api(DEV_URL, "/api/v0/refs/local", timeout=30)
        assert s == 200, f"refs/local failed: {s}"
        if isinstance(d, bytes):
            lines = d.decode(errors="replace").strip().split("\n")
            return f"{len(lines)} refs"
        elif isinstance(d, dict):
            refs = d.get("refs", d.get("Refs", []))
            return f"{len(refs)} refs"
        return f"status={s}"

    test("8.5 Local refs (dev)", test_local_refs)

    def test_prometheus_metrics():
        s, d = api(DEV_URL, "/metrics", method="GET", headers={"Accept": "text/plain"})
        assert s == 200, f"metrics failed: {s}"
        assert isinstance(d, bytes), f"expected bytes"
        lines = d.decode(errors="replace").split("\n")
        metric_lines = [l for l in lines if l and not l.startswith("#")]
        return f"{len(metric_lines)} metrics"

    test("8.6 Prometheus metrics (dev)", test_prometheus_metrics)

    def test_connection_stats():
        s, d = api(DEV_URL, "/api/v0/swarm/connection_stats", timeout=10)
        if s != 200:
            s, d = api(DEV_URL, "/api/v0/debug/connection-stats", timeout=10)
        if s != 200:
            return "skip — endpoint not available"
        return f"status={s}"

    test("8.7 Connection stats (dev)", test_connection_stats)

    def test_stream_stats():
        s, d = api(DEV_URL, "/api/v0/swarm/stream_stats", timeout=10)
        if s != 200:
            return "skip — endpoint not available"
        return f"status={s}"

    test("8.8 Stream stats (dev)", test_stream_stats)

    def test_memory_debug():
        s, d = api(DEV_URL, "/api/v0/debug/memory", timeout=30)
        if s != 200:
            return "skip — endpoint not available"
        if isinstance(d, dict):
            return f"keys={list(d.keys())[:5]}"
        elif isinstance(d, bytes):
            return f"{len(d)} bytes"
        return f"status={s}"

    test("8.9 Memory debug (dev)", test_memory_debug)

    def test_peerstore():
        s, d = api(DEV_URL, "/api/v0/debug/peerstore", timeout=10)
        if s != 200:
            return "skip — endpoint not available"
        if isinstance(d, dict):
            keys = d.get("Keys", d.get("keys", []))
            return f"{len(keys)} known peers"
        return f"status={s}"

    test("8.10 Peerstore (dev)", test_peerstore)

    def test_routing_table_dev():
        s, d = api(DEV_URL, "/api/v0/debug/routing_table", timeout=10)
        if s != 200:
            return "skip — endpoint not available"
        if isinstance(d, dict):
            keys = d.get("Keys", d.get("keys", []))
            return f"{len(keys)} routing entries"
        return f"status={s}"

    test("8.11 Routing table (dev)", test_routing_table_dev)

    def test_routing_table_prod():
        s, d = api(PROD_URL, "/api/v0/debug/routing_table", timeout=10)
        if s != 200:
            return "skip — endpoint not available"
        if isinstance(d, dict):
            keys = d.get("Keys", d.get("keys", []))
            return f"{len(keys)} routing entries"
        return f"status={s}"

    test("8.12 Routing table (prod)", test_routing_table_prod)

    def test_bitswap_stat():
        s, d = api(DEV_URL, "/api/v0/bitswap/stat", timeout=10)
        if s in (404, 405):
            return "skip — bitswap/stat not available"
        if s == 200 and isinstance(d, dict):
            provide_buf_len = d.get("ProvideBufLen", d.get("provideBufLen", "?"))
            peers = d.get("Peers", d.get("peers", []))
            return f"ProvideBufLen={provide_buf_len}, peers={len(peers)}"
        return f"status={s}"

    test("8.13 Bitswap stat (dev)", test_bitswap_stat)

    def test_swarm_peers_count():
        s1, d1 = api(DEV_URL, "/api/v0/swarm/peers")
        s2, d2 = api(PROD_URL, "/api/v0/swarm/peers")
        assert s1 == 200 and s2 == 200, f"swarm/peers failed"
        n1 = len(d1.get("peers", d1.get("Peers", [])))
        n2 = len(d2.get("peers", d2.get("Peers", [])))
        return f"dev={n1} peers, prod={n2} peers"

    test("8.14 Swarm peers count", test_swarm_peers_count)

    print()
    print("=" * 70)
    issues = sum(1 for s, _, _ in results if s == "KNOWN ISSUE")
    print(f"  RESULTS: {passed} passed, {failed} failed, {issues} known issues, {skipped} skipped")
    print("=" * 70)
    if failed > 0:
        print("  FAILURES:")
        for status, name, msg in results:
            if status in ("FAIL", "KNOWN ISSUE"):
                print(f"    {name}: {msg}")
    print()
    return failed


if __name__ == "__main__":
    exit(main())
