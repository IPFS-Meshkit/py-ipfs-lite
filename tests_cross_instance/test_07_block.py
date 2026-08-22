#!/usr/bin/env python3
"""Test 07: Block Operations"""
import os
import time

from tests_cross_instance import DEV_URL, PROD_URL, DEV_ADDR, PROD_ADDR
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
    print("  TEST 07: BLOCK OPERATIONS")
    print("=" * 70)
    print()

    has_stat = check_endpoint(DEV_URL, "/api/v0/block/stat")
    has_get = check_endpoint(DEV_URL, "/api/v0/block/get")
    has_put = check_endpoint(DEV_URL, "/api/v0/block/put")
    print(f"  block/stat: {'available' if has_stat else 'NOT IMPLEMENTED'}")
    print(f"  block/get:  {'available' if has_get else 'NOT IMPLEMENTED'}")
    print(f"  block/put:  {'available' if has_put else 'NOT IMPLEMENTED'}")
    print()

    if not has_stat and not has_get and not has_put:
        print("  All tests skipped — Block endpoints not implemented.")
        print()
        print("=" * 70)
        print(f"  RESULTS: 0 passed, 0 failed, 4 skipped")
        print("=" * 70)
        return 0

    # Ensure connection
    connect_nodes(PROD_URL, DEV_ADDR)
    connect_nodes(DEV_URL, PROD_ADDR)
    time.sleep(2)

    # Add test content on dev
    test_content = b"Block operations test content " + os.urandom(32)
    test_cid = None

    def test_add_content():
        nonlocal test_cid
        status, data = upload_multipart(DEV_URL, "/api/v0/add", "block_test.txt", test_content)
        assert status == 200, f"add failed: {status} {data}"
        test_cid = data.get("Hash", data.get("hash", ""))
        assert test_cid, f"no hash: {data}"
        return f"CID={test_cid[:20]}..."

    test("7.1 Add test content (dev)", test_add_content)

    def test_block_stat_local():
        if not has_stat:
            return "skip"
        assert test_cid, "no CID"
        status, data = api(DEV_URL, f"/api/v0/block/stat?arg={test_cid}")
        assert status == 200, f"block/stat failed: {status} {data}"
        key = data.get("Key", data.get("key", ""))
        size = data.get("Size", data.get("size", 0))
        return f"Key={key[:20]}..., Size={size}"

    test("7.2 Block stat (local on dev)", test_block_stat_local)

    def test_block_get_local():
        if not has_get:
            return "skip"
        assert test_cid, "no CID"
        status, data = api(DEV_URL, f"/api/v0/block/get?arg={test_cid}", timeout=30)
        assert status == 200, f"block/get failed: {status}"
        assert isinstance(data, bytes), f"expected bytes, got {type(data)}"
        assert data == test_content, f"block data mismatch"
        return f"{len(data)} bytes, matches"

    test("7.3 Block get (local on dev)", test_block_get_local)

    def test_block_put():
        if not has_put:
            return "skip"
        raw_block = b"Raw block content " + os.urandom(16)
        status, data = upload_multipart(DEV_URL, "/api/v0/block/put", "raw.bin", raw_block)
        assert status == 200, f"block/put failed: {status} {data}"
        cid = data.get("Key", data.get("Hash", ""))
        assert cid, f"no CID: {data}"
        return f"CID={cid[:20]}..."

    test("7.4 Block put (dev)", test_block_put)

    def test_block_stat_cross():
        if not has_stat:
            return "skip"
        assert test_cid, "no CID"
        # Block won't exist locally on prod without DHT propagation
        connect_nodes(PROD_URL, DEV_ADDR)
        time.sleep(3)
        status, data = api(PROD_URL, f"/api/v0/block/stat?arg={test_cid}")
        if status in (404, 405):
            return f"KNOWN ISSUE: block not on prod (status={status}). DHT providers not propagating."
        assert status == 200, f"block/stat failed: {status} {data}"
        return f"cross-instance block stat OK"

    test("7.5 Block stat (cross-instance: prod)", test_block_stat_cross)

    def test_block_get_cross():
        if not has_get:
            return "skip"
        assert test_cid, "no CID"
        connect_nodes(PROD_URL, DEV_ADDR)
        time.sleep(3)
        status, data = api(PROD_URL, f"/api/v0/block/get?arg={test_cid}", timeout=30)
        if status in (404, 405):
            return f"KNOWN ISSUE: block not on prod (status={status}). DHT providers not propagating."
        assert status == 200, f"block/get failed: {status}"
        assert isinstance(data, bytes) and data == test_content, f"data mismatch"
        return f"cross-instance block get OK, {len(data)} bytes"

    test("7.6 Block get (cross-instance: prod)", test_block_get_cross)

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
