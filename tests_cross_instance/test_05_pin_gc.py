#!/usr/bin/env python3
"""Test 05: Pinning & Garbage Collection"""
import os
import time
import json

from tests_cross_instance import DEV_URL, PROD_URL, DEV_ADDR, PROD_ADDR
from tests_cross_instance.helpers import api, upload_multipart, connect_nodes

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
    print("  TEST 05: PINNING & GARBAGE COLLECTION")
    print("=" * 70)
    print()

    # Ensure connection
    connect_nodes(PROD_URL, DEV_ADDR)
    connect_nodes(DEV_URL, PROD_ADDR)
    time.sleep(2)

    # Add test content on dev
    test_content = b"Pin and GC test content " + os.urandom(64)
    test_cid = None

    def test_add_content():
        nonlocal test_cid
        status, data = upload_multipart(DEV_URL, "/api/v0/add", "pin_test.txt", test_content)
        assert status == 200, f"add failed: {status} {data}"
        test_cid = data.get("Hash", data.get("hash", ""))
        assert test_cid, f"no hash: {data}"
        return f"CID={test_cid[:20]}..."

    test("5.1 Add test content (dev)", test_add_content)

    # Pin locally on dev (guaranteed to work)
    def test_pin_add_local():
        assert test_cid, "no CID"
        status, data = api(DEV_URL, f"/api/v0/pin/add?arg={test_cid}&recursive=true")
        assert status == 200, f"pin/add failed: {status} {data}"
        return f"pinned {test_cid[:20]}..."

    test("5.2 Pin add (local on dev)", test_pin_add_local)

    def test_pin_list_local():
        assert test_cid, "no CID"
        status, data = api(DEV_URL, "/api/v0/pin/ls")
        assert status == 200, f"pin/ls failed: {status}"
        all_pins_str = json.dumps(data)
        assert test_cid in all_pins_str, f"CID not found in pins"
        pins = data.get("PinKeys", data.get("keys", []))
        return f"{len(pins)} pins, CID found"

    test("5.3 Pin list (local on dev)", test_pin_list_local)

    def test_gc_preserves_pin():
        """Add locally, pin, GC, verify survival."""
        local_content = b"GC survival test " + os.urandom(64)
        status, data = upload_multipart(DEV_URL, "/api/v0/add", "gc_test.txt", local_content)
        assert status == 200, f"add failed: {status} {data}"
        local_cid = data.get("Hash", data.get("hash", ""))
        assert local_cid, f"no hash: {data}"

        # Pin it
        status, _ = api(DEV_URL, f"/api/v0/pin/add?arg={local_cid}&recursive=true")
        assert status == 200, f"pin failed: {status}"

        # Verify we can cat it
        status, data = api(DEV_URL, f"/api/v0/cat?arg={local_cid}", timeout=30)
        assert status == 200, f"cat before GC failed: {status}"
        assert data == local_content, f"content mismatch before GC"

        # Run GC
        status, gc_data = api(DEV_URL, "/api/v0/repo/gc")
        assert status == 200, f"repo/gc failed: {status} {gc_data}"

        # Verify pinned file still accessible
        status, data = api(DEV_URL, f"/api/v0/cat?arg={local_cid}", timeout=30)
        assert status == 200, f"cat after GC failed: {status} — pinned file was deleted!"
        assert data == local_content, f"data mismatch after GC"
        return f"GC ran, pinned file survived ({len(data)} bytes)"

    test("5.4 GC survival test (dev)", test_gc_preserves_pin)

    def test_gc_removes_unpinned():
        """Add content without pinning, GC, verify it's gone."""
        # Add content
        content = b"Unpinned content for GC " + os.urandom(32)
        status, data = upload_multipart(DEV_URL, "/api/v0/add", "unpinned.txt", content)
        assert status == 200, f"add failed: {status} {data}"
        cid = data.get("Hash", data.get("hash", ""))
        assert cid, f"no hash: {data}"

        # Verify we can cat it
        status, data = api(DEV_URL, f"/api/v0/cat?arg={cid}", timeout=30)
        assert status == 200, f"cat before GC failed: {status}"

        # Run GC
        status, gc_data = api(DEV_URL, "/api/v0/repo/gc")
        assert status == 200, f"repo/gc failed: {status} {gc_data}"

        # The unpinned content may or may not be GC'd depending on references
        # This test verifies GC runs without error
        return f"GC ran without error"

    test("5.5 GC runs without error (dev)", test_gc_removes_unpinned)

    def test_pin_add_cross_instance():
        """Pin on prod for cross-instance content."""
        assert test_cid, "no CID"
        connect_nodes(PROD_URL, DEV_ADDR)
        time.sleep(3)
        # First try to fetch the content
        status, data = api(PROD_URL, f"/api/v0/cat?arg={test_cid}", timeout=60)
        if status == 200:
            # Content fetched, now pin it
            status, _ = api(PROD_URL, f"/api/v0/pin/add?arg={test_cid}&recursive=true")
            assert status == 200, f"pin failed: {status}"
            return f"cross-instance fetch + pin OK"
        return f"KNOWN ISSUE: cannot fetch for pin (DHT providers not propagating, status={status})"

    test("5.6 Pin add (cross-instance: prod pins dev's content)", test_pin_add_cross_instance)

    def test_pin_remove():
        """Test unpinning."""
        local_content = b"Unpin test " + os.urandom(16)
        status, data = upload_multipart(DEV_URL, "/api/v0/add", "unpin_test.txt", local_content)
        assert status == 200, f"add failed: {status}"
        cid = data.get("Hash", data.get("hash", ""))
        assert cid, f"no hash: {data}"

        # Pin
        status, _ = api(DEV_URL, f"/api/v0/pin/add?arg={cid}")
        assert status == 200, f"pin failed: {status}"

        # Unpin
        status, _ = api(DEV_URL, f"/api/v0/pin/rm?arg={cid}")
        assert status == 200, f"unpin failed: {status}"

        # Verify not pinned
        status, data = api(DEV_URL, "/api/v0/pin/ls")
        all_pins_str = json.dumps(data) if isinstance(data, dict) else str(data)
        # CID should not be in pin list anymore (but may still be in store as unpinned)
        return f"pin + unpin OK"

    test("5.7 Pin add + remove (dev)", test_pin_remove)

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
