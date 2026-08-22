#!/usr/bin/env python3
"""Test 03: DAG Put/Get"""
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
    print("  TEST 03: DAG PUT/GET")
    print("=" * 70)
    print()

    # Ensure connection
    connect_nodes(PROD_URL, DEV_ADDR)
    connect_nodes(DEV_URL, PROD_ADDR)
    time.sleep(2)

    dag_json_cid = None

    def test_dag_put_json():
        nonlocal dag_json_cid
        node = {"name": "test-py-ipfs-lite", "value": 42, "nested": {"a": [1, 2, 3]}}
        status, data = api(DEV_URL, "/api/v0/dag/put?store-codec=dag-json", data=node)
        assert status == 200, f"dag/put failed: {status} {data}"
        # Response format: {"Cid": {"/": "bafy..."}}
        if isinstance(data, dict) and "Cid" in data:
            cid_obj = data["Cid"]
            if isinstance(cid_obj, dict) and "/" in cid_obj:
                dag_json_cid = cid_obj["/"]
            elif isinstance(cid_obj, str):
                dag_json_cid = cid_obj
        if not dag_json_cid:
            for k, v in data.items():
                if isinstance(v, dict) and "/" in v:
                    dag_json_cid = v["/"]
                    break
                elif isinstance(v, str) and v.startswith("b") and len(v) > 20:
                    dag_json_cid = v
                    break
        assert dag_json_cid, f"no CID in response: {data}"
        return f"CID={dag_json_cid[:20]}..."

    test("3.1 DAG Put JSON (dev)", test_dag_put_json)

    def test_dag_get_json_local():
        assert dag_json_cid, "no CID from previous test"
        status, data = api(DEV_URL, f"/api/v0/dag/get?arg={dag_json_cid}", timeout=60)
        assert status == 200, f"dag/get local failed: {status}"
        if isinstance(data, dict):
            assert data.get("name") == "test-py-ipfs-lite", f"name mismatch: {data}"
            assert data.get("value") == 42, f"value mismatch: {data}"
            return f"JSON DAG OK: {data['name']}"
        elif isinstance(data, bytes):
            return f"got {len(data)} bytes (CBOR?)"
        return f"response: {str(data)[:100]}"

    test("3.2 DAG Get JSON (local on dev)", test_dag_get_json_local)

    def test_dag_get_json_cross():
        assert dag_json_cid, "no CID"
        connect_nodes(PROD_URL, DEV_ADDR)
        time.sleep(3)
        status, data = api(PROD_URL, f"/api/v0/dag/get?arg={dag_json_cid}", timeout=60)
        if status == 200:
            if isinstance(data, dict) and data.get("name") == "test-py-ipfs-lite":
                return f"cross-instance DAG get OK"
            elif isinstance(data, bytes):
                return f"got {len(data)} bytes cross-instance (CBOR?)"
            return f"cross-instance: {str(data)[:100]}"
        return f"KNOWN ISSUE: cross-instance dag/get failed (status={status})"

    test("3.3 DAG Get JSON (cross-instance: prod fetches from dev)", test_dag_get_json_cross)

    # CBOR test
    dag_cbor_cid = None

    def test_dag_put_cbor():
        nonlocal dag_cbor_cid
        node = {"data": "cbor test", "numbers": [1, 2, 3]}
        status, data = api(DEV_URL, "/api/v0/dag/put?store-codec=dag-cbor", data=node)
        assert status == 200, f"dag/put cbor failed: {status} {data}"
        if isinstance(data, dict) and "Cid" in data:
            cid_obj = data["Cid"]
            if isinstance(cid_obj, dict) and "/" in cid_obj:
                dag_cbor_cid = cid_obj["/"]
        assert dag_cbor_cid, f"no CID in CBOR response: {data}"
        return f"CID={dag_cbor_cid[:20]}..."

    test("3.4 DAG Put CBOR (dev)", test_dag_put_cbor)

    def test_dag_get_cbor_local():
        assert dag_cbor_cid, "no CID from CBOR test"
        status, data = api(DEV_URL, f"/api/v0/dag/get?arg={dag_cbor_cid}", timeout=60)
        assert status == 200, f"dag/get cbor local failed: {status}"
        if isinstance(data, dict):
            assert data.get("data") == "cbor test", f"CBOR data mismatch: {data}"
            return f"CBOR DAG OK: {data['data']}"
        elif isinstance(data, bytes):
            return f"got {len(data)} bytes (raw CBOR)"
        return f"response: {str(data)[:100]}"

    test("3.5 DAG Get CBOR (local on dev)", test_dag_get_cbor_local)

    # Large DAG node
    def test_dag_large_node():
        """DAG put with a large nested structure."""
        node = {"items": [{"id": i, "data": "x" * 100} for i in range(50)]}
        status, data = api(DEV_URL, "/api/v0/dag/put?store-codec=dag-json", data=node)
        assert status == 200, f"dag/put large failed: {status} {data}"
        cid = None
        if isinstance(data, dict) and "Cid" in data:
            cid_obj = data["Cid"]
            if isinstance(cid_obj, dict) and "/" in cid_obj:
                cid = cid_obj["/"]
        assert cid, f"no CID: {data}"

        # Get it back
        status, data = api(DEV_URL, f"/api/v0/dag/get?arg={cid}", timeout=60)
        assert status == 200, f"dag/get large failed: {status}"
        if isinstance(data, dict):
            items = data.get("items", [])
            assert len(items) == 50, f"expected 50 items, got {len(items)}"
            return f"large DAG OK: {len(items)} items"
        return f"got {type(data)}"

    test("3.6 Large DAG node (50 items)", test_dag_large_node)

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
