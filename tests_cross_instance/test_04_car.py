#!/usr/bin/env python3
"""Test 04: CAR Import/Export"""
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
    print("  TEST 04: CAR IMPORT/EXPORT")
    print("=" * 70)
    print()

    has_export = check_endpoint(DEV_URL, "/api/v0/dag/export")
    has_import = check_endpoint(PROD_URL, "/api/v0/dag/import")
    print(f"  dag/export: {'available' if has_export else 'NOT IMPLEMENTED'}")
    print(f"  dag/import: {'available' if has_import else 'NOT IMPLEMENTED'}")
    print()

    if not has_export and not has_import:
        print("  All tests skipped — CAR endpoints not implemented.")
        print()
        print("=" * 70)
        print(f"  RESULTS: 0 passed, 0 failed, 3 skipped")
        print("=" * 70)
        return 0

    # Ensure connection
    connect_nodes(PROD_URL, DEV_ADDR)
    connect_nodes(DEV_URL, PROD_ADDR)
    time.sleep(2)

    # Add test content on dev
    test_content = b"CAR export test content " + os.urandom(32)
    test_cid = None

    def test_add_content():
        nonlocal test_cid
        status, data = upload_multipart(DEV_URL, "/api/v0/add", "car_test.txt", test_content)
        assert status == 200, f"add failed: {status} {data}"
        test_cid = data.get("Hash", data.get("hash", ""))
        assert test_cid, f"no hash: {data}"
        return f"CID={test_cid[:20]}..."

    test("4.1 Add test content (dev)", test_add_content)

    def test_car_export():
        if not has_export:
            return "skip"
        assert test_cid, "no CID"
        status, data = api(DEV_URL, f"/api/v0/dag/export?arg={test_cid}", timeout=60)
        assert status == 200, f"dag/export failed: {status}"
        assert isinstance(data, bytes), f"expected bytes, got {type(data)}"
        assert len(data) > 100, f"CAR too small: {len(data)} bytes"
        return f"CAR exported: {len(data)} bytes"

    test("4.2 Export CAR (dev)", test_car_export)

    def test_car_import():
        if not has_export or not has_import:
            return "skip"
        assert test_cid, "no CID"

        # Export on dev
        status, car_data = api(DEV_URL, f"/api/v0/dag/export?arg={test_cid}", timeout=60)
        assert status == 200, f"export failed: {status}"

        # Import on prod
        status, data = upload_multipart(PROD_URL, "/api/v0/dag/import", "test.car", car_data)
        assert status == 200, f"dag/import failed: {status} {data}"

        # Cat on prod
        status, data = api(PROD_URL, f"/api/v0/cat?arg={test_cid}", timeout=30)
        assert status == 200, f"cat after import failed: {status}"
        assert isinstance(data, bytes) and data == test_content, f"data mismatch after import"
        return f"CAR import + cat OK, {len(data)} bytes"

    test("4.3 Import CAR (prod) → Cat", test_car_import)

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
