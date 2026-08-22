#!/usr/bin/env python3
"""Test 01: Swarm & Peer Management"""
import time

from tests_cross_instance import DEV_URL, PROD_URL, DEV_ID, PROD_ID, DEV_ADDR, PROD_ADDR
from tests_cross_instance.helpers import api, get_peers, connect_nodes, get_id

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
    print("  TEST 01: SWARM & PEER MANAGEMENT")
    print("=" * 70)
    print()

    def test_swarm_connect():
        status, data = connect_nodes(PROD_URL, DEV_ADDR)
        assert status == 200 or (isinstance(data, dict) and "Addresses" in data), \
            f"connect failed: {status} {data}"
        return "connected"

    test("1.1 Swarm Connect (prod→dev)", test_swarm_connect)
    time.sleep(3)

    def test_list_peers_prod():
        peers = get_peers(PROD_URL)
        peer_ids = [p.get("peer", p.get("Peer", "")) for p in peers]
        assert DEV_ID in peer_ids, f"dev {DEV_ID} not in prod peers ({len(peers)} peers)"
        return f"{len(peers)} peers, dev found"

    test("1.2 List Peers (prod sees dev)", test_list_peers_prod)

    def test_list_peers_dev():
        peers = get_peers(DEV_URL)
        peer_ids = [p.get("peer", p.get("Peer", "")) for p in peers]
        assert PROD_ID in peer_ids, f"prod {PROD_ID} not in dev peers ({len(peers)} peers)"
        return f"{len(peers)} peers, prod found"

    test("1.3 List Peers (dev sees prod)", test_list_peers_dev)

    def test_swarm_connect_bidirectional():
        status, data = connect_nodes(DEV_URL, PROD_ADDR)
        assert status == 200 or (isinstance(data, dict) and "Addresses" in data), \
            f"connect failed: {status} {data}"
        peers = get_peers(DEV_URL)
        peer_ids = [p.get("peer", p.get("Peer", "")) for p in peers]
        assert PROD_ID in peer_ids, f"prod {PROD_ID} not in dev peers after explicit connect"
        return f"bidirectional connect OK, dev has {len(peers)} peers"

    test("1.4 Bidirectional Connect (dev→prod)", test_swarm_connect_bidirectional)

    print()
    print("=" * 70)
    issues = sum(1 for s, _, _ in results if s == "KNOWN ISSUE")
    print(f"  RESULTS: {passed} passed, {failed} failed, {issues} known issues, {skipped} skipped")
    print("=" * 70)
    if failed > 0:
        print("  FAILURES:")
        for status, name, msg in results:
            if status == "FAIL":
                print(f"    {name}: {msg}")
    print()
    return failed


if __name__ == "__main__":
    exit(main())
