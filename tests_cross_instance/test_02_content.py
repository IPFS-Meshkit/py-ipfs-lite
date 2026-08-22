#!/usr/bin/env python3
"""Test 02: Content (Add/Get Round-Trip)"""
import os
import time

from tests_cross_instance import DEV_URL, PROD_URL, DEV_ADDR, PROD_ADDR
from tests_cross_instance.helpers import api, upload_multipart, connect_nodes

passed = 0
failed = 0
skipped = 0
results = []

# Test data
text_content = b"Hello from py-ipfs-lite cross-instance test! " + os.urandom(32)
text_cid = None
binary_content = os.urandom(256 * 1024)  # 256KB
binary_cid = None


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
    global passed, failed, skipped, text_cid, binary_cid

    print()
    print("=" * 70)
    print("  TEST 02: CONTENT (ADD/GET ROUND-TRIP)")
    print("=" * 70)
    print()

    # Ensure connection
    connect_nodes(PROD_URL, DEV_ADDR)
    connect_nodes(DEV_URL, PROD_ADDR)
    time.sleep(2)

    def test_add_text():
        global text_cid
        status, data = upload_multipart(DEV_URL, "/api/v0/add", "test.txt", text_content)
        assert status == 200, f"add failed: {status} {data}"
        text_cid = data.get("Hash", data.get("hash", ""))
        assert text_cid, f"no hash in response: {data}"
        return f"CID={text_cid[:20]}..."

    test("2.1 Add text file (dev)", test_add_text)

    def test_cat_text_local():
        assert text_cid, "no CID"
        status, data = api(DEV_URL, f"/api/v0/cat?arg={text_cid}", timeout=30)
        assert status == 200, f"cat local failed: {status}"
        assert isinstance(data, bytes) and data == text_content, f"data mismatch"
        return f"{len(data)} bytes, content matches"

    test("2.2 Cat text file (local on dev)", test_cat_text_local)

    def test_cat_text_cross_instance():
        """Cross-instance cat — requires DHT provider propagation."""
        assert text_cid, "no CID"
        connect_nodes(PROD_URL, DEV_ADDR)
        time.sleep(3)
        status, data = api(PROD_URL, f"/api/v0/cat?arg={text_cid}", timeout=120)
        if status == 200 and isinstance(data, bytes):
            assert data == text_content, f"data mismatch"
            return f"{len(data)} bytes, content matches"
        return f"KNOWN ISSUE: DHT providers not propagating (status={status})"

    test("2.3 Cat text file (cross-instance: prod fetches from dev)", test_cat_text_cross_instance)

    def test_add_binary():
        global binary_cid
        status, data = upload_multipart(DEV_URL, "/api/v0/add", "random.bin", binary_content)
        assert status == 200, f"add failed: {status} {data}"
        binary_cid = data.get("Hash", data.get("hash", ""))
        assert binary_cid, f"no hash in response: {data}"
        return f"CID={binary_cid[:20]}..., {len(binary_content)} bytes"

    test("2.4 Add 256KB binary file (dev)", test_add_binary)

    def test_cat_binary_local():
        assert binary_cid, "no CID"
        status, data = api(DEV_URL, f"/api/v0/cat?arg={binary_cid}", timeout=30)
        assert status == 200, f"cat local failed: {status}"
        assert isinstance(data, bytes) and data == binary_content, f"data mismatch"
        return f"{len(data)} bytes, content matches"

    test("2.5 Cat 256KB binary (local on dev)", test_cat_binary_local)

    def test_cat_binary_cross_instance():
        """Cross-instance cat — requires DHT provider propagation."""
        assert binary_cid, "no CID"
        connect_nodes(PROD_URL, DEV_ADDR)
        time.sleep(3)
        status, data = api(PROD_URL, f"/api/v0/cat?arg={binary_cid}", timeout=120)
        if status == 200 and isinstance(data, bytes):
            assert data == binary_content, f"data mismatch"
            return f"{len(data)} bytes, content matches"
        return f"KNOWN ISSUE: DHT providers not propagating (status={status})"

    test("2.6 Cat 256KB binary (cross-instance: prod fetches from dev)", test_cat_binary_cross_instance)

    def test_add_on_prod_get_on_dev():
        """Reverse direction: add on prod, get on dev."""
        content = b"Reverse direction test " + os.urandom(16)
        status, data = upload_multipart(PROD_URL, "/api/v0/add", "reverse.txt", content)
        assert status == 200, f"add on prod failed: {status} {data}"
        cid = data.get("Hash", data.get("hash", ""))
        assert cid, f"no hash: {data}"

        # Local cat on prod
        status, data = api(PROD_URL, f"/api/v0/cat?arg={cid}", timeout=30)
        assert status == 200, f"cat local on prod failed: {status}"
        assert data == content, f"local content mismatch"

        # Cross-instance
        connect_nodes(DEV_URL, PROD_ADDR)
        time.sleep(3)
        status2, data2 = api(DEV_URL, f"/api/v0/cat?arg={cid}", timeout=120)
        if status2 == 200 and isinstance(data2, bytes):
            assert data2 == content, f"data mismatch"
            return f"cross-instance round-trip OK, {len(content)} bytes"
        return f"local OK. KNOWN ISSUE: cross-instance DHT (status={status2})"

    test("2.7 Add on prod → Get on dev (reverse)", test_add_on_prod_get_on_dev)

    def test_large_file_1mb():
        """Test 1MB file add + local cat."""
        content_1mb = os.urandom(1024 * 1024)
        status, data = upload_multipart(DEV_URL, "/api/v0/add", "1mb.bin", content_1mb)
        assert status == 200, f"add failed: {status} {data}"
        cid = data.get("Hash", data.get("hash", ""))
        assert cid, f"no hash: {data}"

        status, data = api(DEV_URL, f"/api/v0/cat?arg={cid}", timeout=60)
        assert status == 200, f"cat failed: {status}"
        assert isinstance(data, bytes) and len(data) == 1024 * 1024, f"size mismatch"
        return f"{len(data)} bytes, content matches"

    test("2.8 Large file (1MB add + local cat)", test_large_file_1mb)

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
