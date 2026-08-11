#!/usr/bin/env python3
"""Manual API test suite for py-ipfs-lite."""

import json
import os
import subprocess
import sys
import time

BASE = "http://localhost:5001"
RESULTS = []


def run_server():
    """Start the server in a subprocess."""
    proc = subprocess.Popen(
        [sys.executable, "serve.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )
    return proc


def wait_for_server(timeout=20):
    """Wait until server is ready."""
    import urllib.error
    import urllib.request

    for _ in range(timeout):
        try:
            urllib.request.urlopen(f"{BASE}/api/v0/version", timeout=2)
            return True
        except (urllib.error.URLError, ConnectionRefusedError, OSError):
            time.sleep(1)
    return False


def test_endpoint(method, path, description, data=None, files=None, check_fn=None):
    """Test a single endpoint and record the result."""
    import urllib.error
    import urllib.request

    url = f"{BASE}{path}"
    result = {
        "description": description,
        "method": method,
        "path": path,
        "status": "FAIL",
        "detail": "",
    }

    try:
        if method == "GET":
            req = urllib.request.Request(url, method="GET")
        elif method == "POST":
            if files:
                boundary = "----TestBoundary123"
                body = b""
                for field_name, (filename, filedata, content_type) in files.items():
                    body += f"--{boundary}\r\n".encode()
                    body += f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'.encode()
                    body += f"Content-Type: {content_type}\r\n\r\n".encode()
                    body += filedata
                    body += b"\r\n"
                body += f"--{boundary}--\r\n".encode()
                req = urllib.request.Request(url, data=body, method="POST")
                req.add_header(
                    "Content-Type", f"multipart/form-data; boundary={boundary}"
                )
            else:
                req = urllib.request.Request(url, data=b"", method="POST")
        elif method == "DELETE":
            req = urllib.request.Request(url, data=b"", method="DELETE")
        else:
            result["detail"] = f"Unknown method: {method}"
            RESULTS.append(result)
            return result

        resp = urllib.request.urlopen(req, timeout=10)
        body_bytes = resp.read()
        status = resp.getcode()
        content_type = resp.headers.get("Content-Type", "")

        result["status"] = "PASS"
        result["http_status"] = status
        result["content_type"] = content_type

        if "application/json" in content_type:
            result["response"] = json.loads(body_bytes.decode())
        elif "application/octet-stream" in content_type:
            result["response_bytes"] = len(body_bytes)
            result["response_preview"] = body_bytes[:200].decode(
                "utf-8", errors="replace"
            )
        else:
            result["response_text"] = body_bytes.decode("utf-8", errors="replace")[:500]

        if check_fn:
            check_fn(result)

    except urllib.error.HTTPError as e:
        result["status"] = "FAIL"
        result["http_status"] = e.code
        result["detail"] = e.read().decode("utf-8", errors="replace")[:200]
    except Exception as e:
        result["status"] = "ERROR"
        result["detail"] = str(e)[:200]

    RESULTS.append(result)
    return result


def main():
    proc = run_server()
    try:
        if not wait_for_server():
            print("FAIL: Server did not start within timeout")
            proc.kill()
            sys.exit(1)

        print("=" * 70)
        print("py-ipfs-lite Manual API Test Suite")
        print("=" * 70)

        # ---- BASIC ENDPOINTS ----
        print("\n--- Basic Endpoints ---")

        test_endpoint("GET", "/api/v0/version", "GET version")
        test_endpoint("POST", "/api/v0/version", "POST version")
        test_endpoint("GET", "/api/v0/id", "GET node identity")
        test_endpoint("POST", "/api/v0/id", "POST node identity")

        # ---- REPO ----
        print("\n--- Repository ---")

        test_endpoint("GET", "/api/v0/repo/stat", "GET repo stats")
        test_endpoint("POST", "/api/v0/repo/stat", "POST repo stats")
        test_endpoint("GET", "/api/v0/repo/version", "GET repo version")
        test_endpoint("POST", "/api/v0/repo/version", "POST repo version")

        # ---- REFS ----
        print("\n--- References ---")

        test_endpoint("POST", "/api/v0/refs/local", "POST refs local")

        # ---- PIN ----
        print("\n--- Pinning ---")

        test_endpoint("GET", "/api/v0/pin/ls", "GET pin list (default)")
        test_endpoint(
            "GET", "/api/v0/pin/ls?type=recursive", "GET pin list (recursive)"
        )
        test_endpoint("GET", "/api/v0/pin/ls?type=direct", "GET pin list (direct)")
        test_endpoint("POST", "/api/v0/pin/ls", "POST pin list")

        # ---- SWARM ----
        print("\n--- Swarm ---")

        test_endpoint("GET", "/api/v0/swarm/peers", "GET swarm peers")
        test_endpoint("POST", "/api/v0/swarm/peers", "POST swarm peers")
        test_endpoint("GET", "/api/v0/swarm/connection_stats", "GET connection stats")
        test_endpoint("POST", "/api/v0/swarm/connection_stats", "POST connection stats")

        # ---- ADD FILE ----
        print("\n--- File Operations ---")

        # Create test file
        test_content = b"Hello, IPFS! This is a test file for manual testing."
        r = test_endpoint(
            "POST",
            "/api/v0/add",
            "POST add file (text)",
            files={"file": ("test.txt", test_content, "text/plain")},
        )
        added_cid = None
        if r.get("status") == "PASS" and r.get("response"):
            added_cid = r["response"].get("Hash")
            print(f"  -> Added CID: {added_cid}")

        # Add another file
        test_content2 = json.dumps(
            {"key": "value", "number": 42, "nested": {"a": True}}
        ).encode()
        r2 = test_endpoint(
            "POST",
            "/api/v0/add",
            "POST add file (JSON)",
            files={"file": ("test.json", test_content2, "application/json")},
        )
        json_cid = None
        if r2.get("status") == "PASS" and r2.get("response"):
            json_cid = r2["response"].get("Hash")
            print(f"  -> JSON CID: {json_cid}")

        # ---- CAT ----
        print("\n--- Cat Operations ---")

        if added_cid:
            test_endpoint(
                "GET",
                f"/api/v0/cat?arg={added_cid}",
                f"GET cat file ({added_cid[:16]}...)",
            )
            test_endpoint(
                "POST",
                f"/api/v0/cat?arg={added_cid}",
                f"POST cat file ({added_cid[:16]}...)",
            )
        else:
            RESULTS.append(
                {
                    "description": "GET cat (skipped - no CID)",
                    "status": "SKIP",
                    "detail": "Add failed",
                }
            )

        # Cat with invalid CID
        test_endpoint(
            "GET", "/api/v0/cat?arg=QmInvalidCid123", "GET cat invalid CID (expect 404)"
        )

        # ---- BLOCK OPERATIONS ----
        print("\n--- Block Operations ---")

        if added_cid:
            r_stat = test_endpoint(
                "POST",
                f"/api/v0/block/stat?arg={added_cid}",
                f"POST block stat ({added_cid[:16]}...)",
            )
            r_get = test_endpoint(
                "GET",
                f"/api/v0/block/get?arg={added_cid}",
                f"GET block get ({added_cid[:16]}...)",
            )

            # Block put
            block_data = b"Raw block test data"
            r_put = test_endpoint(
                "POST",
                "/api/v0/block/put",
                "POST block put",
                files={"file": ("block.bin", block_data, "application/octet-stream")},
            )
            put_cid = None
            if r_put.get("status") == "PASS" and r_put.get("response"):
                put_cid = r_put["response"].get("Key")
                print(f"  -> Put CID: {put_cid}")

            # Block rm
            if put_cid:
                test_endpoint(
                    "POST",
                    f"/api/v0/block/rm?arg={put_cid}",
                    f"POST block rm ({put_cid[:16]}...)",
                )
            else:
                RESULTS.append(
                    {"description": "POST block rm (skipped)", "status": "SKIP"}
                )
        else:
            RESULTS.append(
                {"description": "Block ops (skipped - no CID)", "status": "SKIP"}
            )

        # Block stat with invalid CID
        test_endpoint(
            "POST",
            "/api/v0/block/stat?arg=QmInvalid",
            "POST block stat invalid (expect error)",
        )

        # ---- DAG OPERATIONS ----
        print("\n--- DAG Operations ---")

        dag_data = {"hello": "world", "count": 42, "nested": {"a": [1, 2, 3]}}
        r_dag_put = test_endpoint(
            "POST",
            "/api/v0/dag/put?store-codec=dag-json",
            "POST dag put (dag-json)",
            data=json.dumps(dag_data).encode(),
        )
        dag_cid = None
        if r_dag_put.get("status") == "PASS" and r_dag_put.get("response"):
            dag_cid = r_dag_put["response"].get("Cid", {}).get("/")
            print(f"  -> DAG CID: {dag_cid}")

        # DAG put with dag-cbor
        test_endpoint(
            "POST",
            "/api/v0/dag/put?store-codec=dag-cbor",
            "POST dag put (dag-cbor)",
            data=json.dumps(dag_data).encode(),
        )

        if dag_cid:
            r_dag_get = test_endpoint(
                "GET",
                f"/api/v0/dag/get?arg={dag_cid}",
                f"GET dag get ({dag_cid[:16]}...)",
            )
        else:
            RESULTS.append({"description": "GET dag get (skipped)", "status": "SKIP"})

        # ---- NAME (IPNS) ----
        print("\n--- IPNS ---")

        if added_cid:
            test_endpoint(
                "POST",
                f"/api/v0/name/publish?arg=/ipfs/{added_cid}&lifetime=24h",
                f"POST name publish (/ipfs/{added_cid[:16]}...)",
            )

            r_resolve = test_endpoint(
                "GET",
                f"/api/v0/name/resolve?arg={added_cid}",
                f"GET name resolve ({added_cid[:16]}...)",
            )
        else:
            RESULTS.append({"description": "IPNS (skipped - no CID)", "status": "SKIP"})

        # ---- DEBUG ----
        print("\n--- Debug ---")

        test_endpoint("GET", "/api/v0/debug/peerstore", "GET debug peerstore")
        test_endpoint("POST", "/api/v0/debug/peerstore", "POST debug peerstore")
        test_endpoint("GET", "/api/v0/debug/routing_table", "GET debug routing table")
        test_endpoint("POST", "/api/v0/debug/routing_table", "POST debug routing table")

        # ---- REPO GC ----
        print("\n--- Garbage Collection ---")

        test_endpoint("POST", "/api/v0/repo/gc", "POST repo gc")

        # ---- SWARM CONNECT/DISCONNECT (dry - will fail but tests error handling) ----
        print("\n--- Swarm Connect/Disconnect ---")

        test_endpoint(
            "POST",
            "/api/v0/swarm/connect?arg=/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWTestInvalid",
            "POST swarm connect (invalid peer - expect error)",
        )
        test_endpoint(
            "POST",
            "/api/v0/swarm/disconnect?arg=12D3KooWTestInvalid",
            "POST swarm disconnect (invalid peer - expect error)",
        )

        # ---- PIN ADD / RM ----
        print("\n--- Pin Add/Rm ---")

        if added_cid:
            r_pin_add = test_endpoint(
                "POST",
                f"/api/v0/pin/add?arg={added_cid}&recursive=true",
                f"POST pin add ({added_cid[:16]}...)",
            )
            # Pin again (should succeed or return 409)
            test_endpoint(
                "POST",
                f"/api/v0/pin/add?arg={added_cid}&recursive=true",
                "POST pin add (duplicate - expect success or 409)",
            )
            # Verify pin in list
            test_endpoint("GET", "/api/v0/pin/ls", "GET pin list (after pin add)")

            # Unpin
            r_pin_rm = test_endpoint(
                "POST",
                f"/api/v0/pin/rm?arg={added_cid}",
                f"POST pin rm ({added_cid[:16]}...)",
            )
            # Verify pin removed
            test_endpoint("GET", "/api/v0/pin/ls", "GET pin list (after pin rm)")
        else:
            RESULTS.append({"description": "Pin add/rm (skipped)", "status": "SKIP"})

        # Pin invalid CID
        test_endpoint(
            "POST",
            "/api/v0/pin/add?arg=QmInvalid",
            "POST pin add invalid CID (expect error)",
        )

        # ---- ERROR HANDLING ----
        print("\n--- Error Handling ---")

        test_endpoint("GET", "/api/v0/cat?arg=", "GET cat empty arg (expect error)")
        test_endpoint(
            "GET", "/api/v0/dag/get?arg=", "GET dag get empty arg (expect error)"
        )
        test_endpoint(
            "POST",
            "/api/v0/block/stat?arg=",
            "POST block stat empty arg (expect error)",
        )

        # ---- RESULTS ----
        print("\n" + "=" * 70)
        print("TEST RESULTS SUMMARY")
        print("=" * 70)

        passed = sum(1 for r in RESULTS if r["status"] == "PASS")
        failed = sum(1 for r in RESULTS if r["status"] == "FAIL")
        errors = sum(1 for r in RESULTS if r["status"] == "ERROR")
        skipped = sum(1 for r in RESULTS if r["status"] == "SKIP")
        total = len(RESULTS)

        for r in RESULTS:
            icon = {"PASS": "✅", "FAIL": "❌", "ERROR": "⚠️", "SKIP": "⏭️"}.get(
                r["status"], "?"
            )
            print(f"  {icon} [{r['method']}] {r['description']}")
            if r["status"] == "FAIL" or r["status"] == "ERROR":
                print(f"     Detail: {r.get('detail', 'N/A')[:150]}")
            elif r["status"] == "PASS" and "response" in r:
                resp_str = json.dumps(r["response"], indent=None)
                if len(resp_str) > 120:
                    resp_str = resp_str[:120] + "..."
                print(f"     Response: {resp_str}")

        print(
            f"\n  Total: {total} | ✅ Passed: {passed} | ❌ Failed: {failed} | ⚠️ Errors: {errors} | ⏭️ Skipped: {skipped}"
        )

    finally:
        proc.kill()
        proc.wait(timeout=5)
        print("\nServer stopped.")


if __name__ == "__main__":
    main()
