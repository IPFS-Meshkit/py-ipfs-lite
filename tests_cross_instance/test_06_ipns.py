#!/usr/bin/env python3
"""Test 06: IPNS Publish/Resolve"""
import os
import time

from tests_cross_instance import DEV_URL, PROD_URL, DEV_ID, DEV_ADDR, PROD_ADDR
from tests_cross_instance.helpers import api, upload_multipart, connect_nodes, check_endpoint

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
    print("  TEST 06: IPNS PUBLISH/RESOLVE")
    print("=" * 70)
    print()

    has_publish = check_endpoint(DEV_URL, "/api/v0/name/publish")
    has_resolve = check_endpoint(PROD_URL, "/api/v0/name/resolve")
    print(f"  name/publish: {'available' if has_publish else 'NOT IMPLEMENTED'}")
    print(f"  name/resolve: {'available' if has_resolve else 'NOT IMPLEMENTED'}")
    print()

    if not has_publish and not has_resolve:
        print("  All tests skipped — IPNS endpoints not implemented.")
        print()
        print("=" * 70)
        print(f"  RESULTS: 0 passed, 0 failed, 2 skipped")
        print("=" * 70)
        return 0

    # Ensure connection
    connect_nodes(PROD_URL, DEV_ADDR)
    connect_nodes(DEV_URL, PROD_ADDR)
    time.sleep(2)

    # Add test content
    test_content = b"IPNS test content " + os.urandom(32)
    test_cid = None

    def test_add_content():
        nonlocal test_cid
        status, data = upload_multipart(DEV_URL, "/api/v0/add", "ipns_test.txt", test_content)
        assert status == 200, f"add failed: {status} {data}"
        test_cid = data.get("Hash", data.get("hash", ""))
        assert test_cid, f"no hash: {data}"
        return f"CID={test_cid[:20]}..."

    test("6.1 Add test content (dev)", test_add_content)

    def test_ipns_publish():
        if not has_publish:
            return "skip"
        assert test_cid, "no CID"
        status, data = api(DEV_URL, f"/api/v0/name/publish?arg=/ipfs/{test_cid}&lifetime=1h")
        assert status == 200, f"name/publish failed: {status} {data}"
        name = data.get("Name", data.get("name", ""))
        return f"published: {name}"

    test("6.2 IPNS Publish (dev)", test_ipns_publish)

    def test_ipns_resolve():
        if not has_resolve:
            return "skip"
        assert DEV_ID, "no dev ID"
        time.sleep(5)  # DHT propagation time
        status, data = api(PROD_URL, f"/api/v0/name/resolve?arg={DEV_ID}", timeout=120)
        assert status == 200, f"name/resolve failed: {status} {data}"
        resolved = data.get("Path", data.get("path", ""))
        assert test_cid in resolved, f"resolved path {resolved} does not contain CID {test_cid}"
        return f"resolved to {resolved}"

    test("6.3 IPNS Resolve (prod resolves dev's name)", test_ipns_resolve)

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
