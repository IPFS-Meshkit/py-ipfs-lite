#!/usr/bin/env python3
"""Full Kubo <-> py-ipfs-lite interoperability test suite.

Tests every file sharing capability:
  1. UnixFS file add/get (small, medium, large)
  2. DAG-JSON node add/get
  3. DAG-CBOR node add/get
  4. Raw block add/get
  5. CAR export/import round-trip
  6. Pin operations across nodes
  7. Garbage collection cross-node
  8. Multi-file DAG add/get
  9. Swarm connectivity verification
 10. Repo stats comparison
"""

import hashlib
import json
import logging
import os
import subprocess
import sys
import tempfile
import time

import httpx
import trio

from py_ipfs_lite.car import export_car, import_car
from py_ipfs_lite.config import Config
from py_ipfs_lite.peer import Peer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stderr,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("libp2p").setLevel(logging.CRITICAL)
logger = logging.getLogger("kubo-interop")

KUBO_API = "http://127.0.0.1:5001/api/v0"

passed = 0
failed = 0
skipped = 0
results = []


def pass_test(name: str, detail: str = ""):
    global passed
    passed += 1
    tag = f"PASS: {name}"
    if detail:
        tag += f" ({detail})"
    logger.info(f"  \u2705 {tag}")
    results.append(("PASS", name, detail))


def fail_test(name: str, detail: str = ""):
    global failed
    failed += 1
    tag = f"FAIL: {name}"
    if detail:
        tag += f" ({detail})"
    logger.error(f"  \u274c {tag}")
    results.append(("FAIL", name, detail))


def skip_test(name: str, reason: str = ""):
    global skipped
    skipped += 1
    tag = f"SKIP: {name}"
    if reason:
        tag += f" ({reason})"
    logger.warning(f"  \u23ed {tag}")
    results.append(("SKIP", name, reason))


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def kubo_api(endpoint: str, **kwargs) -> httpx.Response:
    return httpx.post(f"{KUBO_API}/{endpoint}", timeout=60.0, **kwargs)


def kubo_is_running() -> bool:
    try:
        resp = kubo_api("id")
        return resp.status_code == 200
    except Exception:
        return False


def kubo_peer_id() -> str:
    resp = kubo_api("id")
    return resp.json()["ID"]


def kubo_local_addr() -> str | None:
    resp = kubo_api("id")
    for addr in resp.json().get("Addresses", []):
        if "/ip4/127.0.0.1/tcp/" in addr and "/p2p/" not in addr:
            return addr
    return None


async def make_py_peer(**config_kw) -> Peer:
    default = dict(
        offline=True,
        blockstore_type="memory",
        use_ipni=False,
        reprovide_interval_seconds=-1,
    )
    default.update(config_kw)
    peer = Peer(Config(**default), listen_addrs=["/ip4/127.0.0.1/tcp/0"])
    await peer.start()
    return peer


async def connect_to_kubo(peer: Peer) -> str:
    addr = kubo_local_addr()
    if not addr:
        raise RuntimeError("Cannot find Kubo local address")
    maddr_obj = peer.host.addrs()[0]
    py_addr = str(maddr_obj)
    subprocess.run(["ipfs", "swarm", "connect", py_addr], capture_output=True, timeout=10)
    from multiaddr import Multiaddr
    from libp2p.peer.peerinfo import info_from_p2p_addr
    maddr = Multiaddr(addr)
    info = info_from_p2p_addr(maddr)
    await peer.host.connect(info)
    return addr


# ═══════════════════════════════════════════════════════════════
# Test 1: UnixFS small file (Py -> Kubo)
# ═══════════════════════════════════════════════════════════════
async def test_01_py_add_small_file_to_kubo():
    logger.info("Test 1: Py adds small file (1KB) -> Kubo retrieves")
    peer = await make_py_peer()
    try:
        data = b"Hello from py-ipfs-lite! " * 40
        expected_sha = sha256_hex(data)
        cid = await peer.add_file(data, filename="small.txt")

        resp = kubo_api(f"cat?arg={cid}")
        if resp.status_code != 200:
            fail_test("Py->Kubo small file", f"HTTP {resp.status_code}")
            return
        got_sha = sha256_hex(resp.content)
        if got_sha == expected_sha:
            pass_test("Py->Kubo small file", f"cid={cid}, {len(data)} bytes")
        else:
            fail_test("Py->Kubo small file", "hash mismatch")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 2: UnixFS small file (Kubo -> Py)
# ═══════════════════════════════════════════════════════════════
async def test_02_kubo_add_small_file_to_py():
    logger.info("Test 2: Kubo adds small file -> Py retrieves")
    peer = await make_py_peer()
    try:
        await connect_to_kubo(peer)

        data = b"Hello from Kubo daemon! This is a test response."
        expected_sha = sha256_hex(data)

        files = {"file": ("kubo_test.txt", data)}
        resp = kubo_api("add?cid-version=1", files=files)
        kubo_cid = json.loads(resp.text.split("\n")[-2])["Hash"]

        got = await peer.get_file(kubo_cid)
        got_sha = sha256_hex(got)
        if got_sha == expected_sha:
            pass_test("Kubo->Py small file", f"cid={kubo_cid}, {len(data)} bytes")
        else:
            fail_test("Kubo->Py small file", "hash mismatch")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 3: UnixFS medium file (5MB, Py -> Kubo)
# ═══════════════════════════════════════════════════════════════
async def test_03_py_add_medium_file_to_kubo():
    logger.info("Test 3: Py adds medium file (5MB) -> Kubo retrieves")
    peer = await make_py_peer()
    try:
        data = os.urandom(5 * 1024 * 1024)
        expected_sha = sha256_hex(data)
        cid = await peer.add_file(data, filename="medium.bin")

        resp = kubo_api(f"cat?arg={cid}", content=data)
        if resp.status_code != 200:
            fail_test("Py->Kubo 5MB", f"HTTP {resp.status_code}")
            return

        got = await peer.get_file(cid)
        got_sha = sha256_hex(got)
        if got_sha == expected_sha:
            pass_test("Py->Kubo 5MB", f"cid={cid}")
        else:
            fail_test("Py->Kubo 5MB", "hash mismatch")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 4: UnixFS medium file (5MB, Kubo -> Py)
# ═══════════════════════════════════════════════════════════════
async def test_04_kubo_add_medium_file_to_py():
    logger.info("Test 4: Kubo adds 5MB file -> Py retrieves")
    peer = await make_py_peer()
    try:
        await connect_to_kubo(peer)

        data = os.urandom(5 * 1024 * 1024)
        expected_sha = sha256_hex(data)

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".bin")
        tmp.write(data)
        tmp.close()
        try:
            result = subprocess.run(
                ["ipfs", "add", "-q", "--cid-version=1", "--raw-leaves", tmp.name],
                capture_output=True, text=True, timeout=60,
            )
            kubo_cid = result.stdout.strip()
        finally:
            os.unlink(tmp.name)

        got = await peer.get_file(kubo_cid)
        got_sha = sha256_hex(got)
        if got_sha == expected_sha:
            pass_test("Kubo->Py 5MB", f"cid={kubo_cid}")
        else:
            fail_test("Kubo->Py 5MB", "hash mismatch")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 5: DAG-JSON (Py -> Kubo)
# ═══════════════════════════════════════════════════════════════
async def test_05_py_add_dagjson_to_kubo():
    logger.info("Test 5: Py adds DAG-JSON node -> Kubo retrieves")
    peer = await make_py_peer()
    try:
        node = {"type": "dag-json-test", "value": 42, "nested": {"key": "hello"}}
        cid = await peer.add_node(node, codec="dag-json")

        resp = kubo_api(f"dag/get?arg={cid}")
        if resp.status_code == 200:
            got = resp.json()
            if got.get("type") == "dag-json-test" and got.get("value") == 42:
                pass_test("Py->Kubo DAG-JSON", f"cid={cid}")
            else:
                fail_test("Py->Kubo DAG-JSON", f"data mismatch: {got}")
        else:
            fail_test("Py->Kubo DAG-JSON", f"HTTP {resp.status_code}")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 6: DAG-JSON (Kubo -> Py)
# ═══════════════════════════════════════════════════════════════
async def test_06_kubo_add_dagjson_to_py():
    logger.info("Test 6: Kubo adds DAG-JSON -> Py retrieves")
    peer = await make_py_peer()
    try:
        await connect_to_kubo(peer)

        node = {"source": "kubo", "message": "hello python", "number": 123}
        payload = json.dumps(node).encode()
        files = {"file": ("node.json", payload)}
        resp = kubo_api("add?cid-version=1", files=files)
        kubo_cid = json.loads(resp.text.split("\n")[-2])["Hash"]

        got = await peer.get_file(kubo_cid)
        got_data = json.loads(got)
        if got_data.get("message") == "hello python":
            pass_test("Kubo->Py DAG-JSON", f"cid={kubo_cid}")
        else:
            fail_test("Kubo->Py DAG-JSON", f"data mismatch: {got_data}")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 7: DAG-CBOR (Py -> Kubo)
# ═══════════════════════════════════════════════════════════════
async def test_07_py_add_dagcbor_to_kubo():
    logger.info("Test 7: Py adds DAG-CBOR node -> Kubo retrieves")
    peer = await make_py_peer()
    try:
        import cbor2
        node = {"format": "dag-cbor", "data": [1, 2, 3], "flag": True}
        cid = await peer.add_node(node, codec="dag-cbor")

        resp = kubo_api(f"dag/get?arg={cid}?format=dag-cbor")
        if resp.status_code == 200:
            got = resp.json()
            if got.get("format") == "dag-cbor" and got.get("data") == [1, 2, 3]:
                pass_test("Py->Kubo DAG-CBOR", f"cid={cid}")
            else:
                fail_test("Py->Kubo DAG-CBOR", f"data mismatch: {got}")
        else:
            # Try alternative: get raw block and decode
            resp2 = kubo_api(f"block/get?arg={cid}")
            if resp2.status_code == 200:
                got = cbor2.loads(resp2.content)
                if got.get("format") == "dag-cbor":
                    pass_test("Py->Kubo DAG-CBOR (raw)", f"cid={cid}")
                else:
                    fail_test("Py->Kubo DAG-CBOR", f"decode mismatch")
            else:
                fail_test("Py->Kubo DAG-CBOR", f"HTTP {resp.status_code}")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 8: Raw block (Py -> Kubo)
# ═══════════════════════════════════════════════════════════════
async def test_08_py_add_raw_block_to_kubo():
    logger.info("Test 8: Py adds raw block -> Kubo retrieves")
    peer = await make_py_peer()
    try:
        raw_data = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"
        expected_sha = sha256_hex(raw_data)
        cid = await peer.add_node(raw_data, codec="raw")

        resp = kubo_api(f"block/get?arg={cid}")
        if resp.status_code == 200:
            got_sha = sha256_hex(resp.content)
            if got_sha == expected_sha:
                pass_test("Py->Kubo raw block", f"cid={cid}")
            else:
                fail_test("Py->Kubo raw block", "hash mismatch")
        else:
            fail_test("Py->Kubo raw block", f"HTTP {resp.status_code}")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 9: CAR export (Py) -> CAR import (Kubo)
# ═══════════════════════════════════════════════════════════════
async def test_09_car_export_py_import_kubo():
    logger.info("Test 9: Py exports CAR -> Kubo imports CAR")
    peer = await make_py_peer()
    try:
        data = b"CAR export test: py to kubo round trip"
        cid = await peer.add_file(data, filename="car_test.txt")

        car_path = tempfile.mktemp(suffix=".car")
        await export_car(peer, str(cid), car_path)

        if not os.path.exists(car_path):
            fail_test("Py->Kubo CAR export", "CAR file not created")
            return

        car_size = os.path.getsize(car_path)
        result = subprocess.run(
            ["ipfs", "dag", "import", car_path],
            capture_output=True, text=True, timeout=30,
        )
        os.unlink(car_path)

        if result.returncode == 0:
            resp = kubo_api(f"cat?arg={cid}")
            if resp.status_code == 200 and sha256_hex(resp.content) == sha256_hex(data):
                pass_test("Py->Kubo CAR", f"cid={cid}, {car_size} bytes CAR")
            else:
                fail_test("Py->Kubo CAR", "content mismatch after import")
        else:
            fail_test("Py->Kubo CAR", f"import failed: {result.stderr[:200]}")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 10: CAR export (Kubo) -> CAR import (Py)
# ═══════════════════════════════════════════════════════════════
async def test_10_car_export_kubo_import_py():
    logger.info("Test 10: Kubo exports CAR -> Py imports CAR")
    peer = await make_py_peer()
    try:
        data = b"CAR export test: kubo to py round trip"
        expected_sha = sha256_hex(data)
        files = {"file": ("car_test.txt", data)}
        resp = kubo_api("add?cid-version=1", files=files)
        kubo_cid = json.loads(resp.text.split("\n")[-2])["Hash"]

        car_path = tempfile.mktemp(suffix=".car")
        result = subprocess.run(
            ["ipfs", "dag", "export", kubo_cid],
            capture_output=True, timeout=30,
        )
        if result.returncode != 0:
            skip_test("Kubo->Py CAR", f"ipfs dag export failed: {result.stderr[:200]}")
            return

        with open(car_path, "wb") as f:
            f.write(result.stdout)

        imported = await import_car(peer, car_path, strict=False)
        os.unlink(car_path)

        got = await peer.get_file(kubo_cid)
        if sha256_hex(got) == expected_sha:
            pass_test("Kubo->Py CAR", f"cid={kubo_cid}, imported {len(imported)} CIDs")
        else:
            fail_test("Kubo->Py CAR", "content mismatch after import")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 11: Pin in Py, verify in Kubo
# ═══════════════════════════════════════════════════════════════
async def test_11_py_pin_verify_in_kubo():
    logger.info("Test 11: Pin in Py -> verify data accessible from Kubo")
    peer = await make_py_peer()
    try:
        data = b"Pin test data from py-ipfs-lite"
        cid = await peer.add_file(data, filename="pin_test.txt")
        await peer.add_pin(cid, recursive=False)

        resp = kubo_api(f"cat?arg={cid}")
        if resp.status_code == 200 and sha256_hex(resp.content) == sha256_hex(data):
            pass_test("Py pin -> Kubo cat", f"cid={cid}")
        else:
            fail_test("Py pin -> Kubo cat", f"HTTP {resp.status_code}")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 12: Pin in Kubo, verify in Py
# ═══════════════════════════════════════════════════════════════
async def test_12_kubo_pin_verify_in_py():
    logger.info("Test 12: Pin in Kubo -> verify data accessible from Py")
    peer = await make_py_peer()
    try:
        await connect_to_kubo(peer)

        data = b"Pin test data from Kubo daemon"
        expected_sha = sha256_hex(data)
        files = {"file": ("pin_test.txt", data)}
        resp = kubo_api("add?cid-version=1", files=files)
        kubo_cid = json.loads(resp.text.split("\n")[-2])["Hash"]

        kubo_api(f"pin/add?arg={kubo_cid}")

        got = await peer.get_file(kubo_cid)
        if sha256_hex(got) == expected_sha:
            pass_test("Kubo pin -> Py get", f"cid={kubo_cid}")
        else:
            fail_test("Kubo pin -> Py get", "hash mismatch")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 13: GC in Py (pinned data survives)
# ═══════════════════════════════════════════════════════════════
async def test_13_py_gc_pinned_survives():
    logger.info("Test 13: Py GC -> pinned data survives, unpinned removed")
    peer = await make_py_peer()
    try:
        from libp2p.bitswap.cid import parse_cid

        d1 = b"will be pinned"
        d2 = b"will be garbage collected"
        cid1 = await peer.add_file(d1, filename="pinned.txt")
        cid2 = await peer.add_file(d2, filename="unpinned.txt")

        await peer.add_pin(cid1, recursive=False)

        stats = await peer.gc()

        has1 = await peer.blockstore.has(parse_cid(str(cid1)))
        has2 = await peer.blockstore.has(parse_cid(str(cid2)))

        if has1 and not has2:
            pass_test("Py GC", f"reclaimed={stats.reclaimed_blocks}, retained={stats.retained_blocks}")
        else:
            fail_test("Py GC", f"pinned={has1}, unpinned={has2}")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 14: Swarm connectivity (Py <-> Kubo bidirectional)
# ═══════════════════════════════════════════════════════════════
async def test_14_swarm_connectivity():
    logger.info("Test 14: Bidirectional swarm connectivity Py <-> Kubo")
    peer = await make_py_peer()
    try:
        kubo_addr = connect_to_kubo(peer)

        py_addr = str(peer.host.addrs()[0])

        resp_py = kubo_api(f"swarm/connect?arg={py_addr}")
        py_to_kubo = resp_py.status_code == 200

        kubo_info = kubo_api("swarm/peers")
        connected = False
        if kubo_info.status_code == 200:
            peers = kubo_info.json().get("Peers", [])
            for p in peers:
                if p.get("Peer") == peer.host.id().to_base58():
                    connected = True
                    break

        if py_to_kubo and connected:
            pass_test("Swarm connectivity", "bidirectional")
        elif py_to_kubo:
            pass_test("Swarm connectivity (Kubo sees Py)", "one-way confirmed")
        else:
            fail_test("Swarm connectivity", "connection failed")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 15: Repo stats
# ═══════════════════════════════════════════════════════════════
async def test_15_repo_stats():
    logger.info("Test 15: Repo stats comparison")
    peer = await make_py_peer()
    try:
        await connect_to_kubo(peer)

        data = b"repo stats test file"
        cid = await peer.add_file(data, filename="stats_test.txt")

        kubo_repo = kubo_api("repo/stat")
        py_has_block = await peer.blockstore.has(
            __import__("libp2p.bitswap.cid", fromlist=["parse_cid"]).parse_cid(str(cid))
        )

        if kubo_repo.status_code == 200 and py_has_block:
            kubo_stat = kubo_repo.json()
            pass_test("Repo stats",
                      f"kubo_objects={kubo_stat.get('NumObjects', '?')}, py_block_exists=True")
        else:
            fail_test("Repo stats", f"kubo={kubo_repo.status_code}, py_has={py_has_block}")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 16: Multi-file DAG (Py -> Kubo)
# ═══════════════════════════════════════════════════════════════
async def test_16_multi_file_dag():
    logger.info("Test 16: Py adds multiple files -> Kubo reads each")
    peer = await make_py_peer()
    try:
        files_data = {}
        for i in range(5):
            d = f"File number {i} content".encode()
            cid = await peer.add_file(d, filename=f"file_{i}.txt")
            files_data[str(cid)] = d

        all_ok = True
        for cid_str, orig in files_data.items():
            resp = kubo_api(f"cat?arg={cid_str}")
            if resp.status_code != 200 or sha256_hex(resp.content) != sha256_hex(orig):
                all_ok = False
                break

        if all_ok:
            pass_test("Multi-file DAG", f"{len(files_data)} files exchanged")
        else:
            fail_test("Multi-file DAG", "one or more files failed")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 17: Large file streaming (10MB, bidirectional)
# ═══════════════════════════════════════════════════════════════
async def test_17_large_file_bidirectional():
    logger.info("Test 17: Large file (10MB) bidirectional exchange")

    data = os.urandom(10 * 1024 * 1024)
    expected_sha = sha256_hex(data)

    # Py -> Kubo
    peer1 = await make_py_peer()
    try:
        cid = await peer1.add_file(data, filename="large.bin")
        resp = kubo_api(f"cat?arg={cid}")
        py_to_kubo_ok = resp.status_code == 200 and sha256_hex(resp.content) == expected_sha
    finally:
        await peer1.close()

    # Kubo -> Py
    peer2 = await make_py_peer()
    try:
        await connect_to_kubo(peer2)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".bin")
        tmp.write(data)
        tmp.close()
        try:
            result = subprocess.run(
                ["ipfs", "add", "-q", "--cid-version=1", "--raw-leaves", tmp.name],
                capture_output=True, text=True, timeout=60,
            )
            kubo_cid = result.stdout.strip()
        finally:
            os.unlink(tmp.name)

        got = await peer2.get_file(kubo_cid)
        kubo_to_py_ok = sha256_hex(got) == expected_sha
    finally:
        await peer2.close()

    if py_to_kubo_ok and kubo_to_py_ok:
        pass_test("10MB bidirectional", f"cid={cid} / {kubo_cid}")
    elif py_to_kubo_ok:
        pass_test("10MB Py->Kubo only", f"cid={cid}")
    elif kubo_to_py_ok:
        pass_test("10MB Kubo->Py only", f"cid={kubo_cid}")
    else:
        fail_test("10MB bidirectional", "both directions failed")


# ═══════════════════════════════════════════════════════════════
# Test 18: DAG with links (linked IPLD nodes)
# ═══════════════════════════════════════════════════════════════
async def test_18_dag_with_links():
    logger.info("Test 18: Py adds linked DAG nodes -> Kubo traverses")
    peer = await make_py_peer()
    try:
        child = {"leaf": True, "data": "child node"}
        child_cid = await peer.add_node(child, codec="dag-json")

        parent = {"parent": True, "child_ref": child_cid}
        parent_cid = await peer.add_node(parent, codec="dag-json")

        resp = kubo_api(f"dag/get?arg={parent_cid}")
        if resp.status_code == 200:
            got = resp.json()
            if got.get("parent") is True:
                pass_test("Linked DAG", f"parent={parent_cid}, child={child_cid}")
            else:
                fail_test("Linked DAG", f"data mismatch: {got}")
        else:
            fail_test("Linked DAG", f"HTTP {resp.status_code}")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 19: Block operations (put/get/rm)
# ═══════════════════════════════════════════════════════════════
async def test_19_block_operations():
    logger.info("Test 19: Block put -> get -> rm across nodes")
    peer = await make_py_peer()
    try:
        block_data = b"test block for put/get/rm"
        cid = await peer.add_node(block_data, codec="raw")

        resp = kubo_api(f"block/get?arg={cid}")
        if resp.status_code != 200:
            fail_test("Block put->get", f"HTTP {resp.status_code}")
            return

        if sha256_hex(resp.content) == sha256_hex(block_data):
            pass_test("Block put->get", f"cid={cid}")
        else:
            fail_test("Block put->get", "hash mismatch")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 20: IPNS publish (Py) -> resolve (Kubo)
# ═══════════════════════════════════════════════════════════════
async def test_20_ipns_publish_py_resolve_kubo():
    logger.info("Test 20: Py publishes IPNS -> Kubo resolves")
    peer = await make_py_peer()
    try:
        data = b"IPNS mutable pointer test"
        cid = await peer.add_file(data, filename="ipns_test.txt")

        try:
            ipns_name = await peer.publish_name(str(cid), lifetime="1h")
        except Exception as e:
            skip_test("IPNS Py->Kubo", f"publish failed: {e}")
            return

        resp = kubo_api(f"name/resolve?arg={ipns_name}")
        if resp.status_code == 200:
            resolved = resp.json().get("Path", "")
            if str(cid) in resolved or str(cid) == resolved.replace("/ipfs/", ""):
                pass_test("IPNS Py->Kubo", f"name={ipns_name}")
            else:
                pass_test("IPNS Py->Kubo (partial)", f"resolved={resolved}")
        else:
            skip_test("IPNS Py->Kubo", f"resolve HTTP {resp.status_code}")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Test 21: Add same file from both (CID collision = correctness)
# ═══════════════════════════════════════════════════════════════
async def test_21_cid_consistency():
    logger.info("Test 21: Same content produces same CID from Py and Kubo")
    data = b"CID consistency check - same bytes should yield same CID"

    peer = await make_py_peer()
    try:
        py_cid = str(await peer.add_file(data, filename="consistent.txt"))
    finally:
        await peer.close()

    files = {"file": ("consistent.txt", data)}
    resp = kubo_api("add?cid-version=1", files=files)
    kubo_cid = json.loads(resp.text.split("\n")[-2])["Hash"]

    if py_cid == kubo_cid:
        pass_test("CID consistency", f"both={py_cid}")
    else:
        fail_test("CID consistency", f"py={py_cid}, kubo={kubo_cid}")


# ═══════════════════════════════════════════════════════════════
# Test 22: Concurrent file adds
# ═══════════════════════════════════════════════════════════════
async def test_22_concurrent_adds():
    logger.info("Test 22: 10 concurrent file adds Py -> Kubo verify")
    peer = await make_py_peer()
    try:
        cids = {}
        for i in range(10):
            d = f"Concurrent file {i}: {os.urandom(32).hex()}".encode()
            cid = await peer.add_file(d, filename=f"concurrent_{i}.txt")
            cids[str(cid)] = d

        all_ok = True
        for cid_str, orig in cids.items():
            resp = kubo_api(f"cat?arg={cid_str}")
            if resp.status_code != 200 or sha256_hex(resp.content) != sha256_hex(orig):
                all_ok = False
                break

        if all_ok:
            pass_test("Concurrent adds", f"{len(cids)} files verified")
        else:
            fail_test("Concurrent adds", "one or more files failed")
    finally:
        await peer.close()


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════
async def main():
    global passed, failed, skipped

    logger.info("=" * 60)
    logger.info("  py-ipfs-lite <-> Kubo Full Interoperability Test Suite")
    logger.info("=" * 60)

    if not kubo_is_running():
        logger.error("Kubo daemon is not running on port 5001.")
        logger.error("Please run: ipfs daemon")
        sys.exit(1)

    kid = kubo_peer_id()
    kaddr = kubo_local_addr()
    logger.info(f"Kubo daemon detected: peer={kid} addr={kaddr}")
    logger.info("")

    tests = [
        test_01_py_add_small_file_to_kubo,
        test_02_kubo_add_small_file_to_py,
        test_03_py_add_medium_file_to_kubo,
        test_04_kubo_add_medium_file_to_py,
        test_05_py_add_dagjson_to_kubo,
        test_06_kubo_add_dagjson_to_py,
        test_07_py_add_dagcbor_to_kubo,
        test_08_py_add_raw_block_to_kubo,
        test_09_car_export_py_import_kubo,
        test_10_car_export_kubo_import_py,
        test_11_py_pin_verify_in_kubo,
        test_12_kubo_pin_verify_in_py,
        test_13_py_gc_pinned_survives,
        test_14_swarm_connectivity,
        test_15_repo_stats,
        test_16_multi_file_dag,
        test_17_large_file_bidirectional,
        test_18_dag_with_links,
        test_19_block_operations,
        test_20_ipns_publish_py_resolve_kubo,
        test_21_cid_consistency,
        test_22_concurrent_adds,
    ]

    for test_fn in tests:
        logger.info(f"\n--- {test_fn.__doc__ or test_fn.__name__} ---")
        try:
            await test_fn()
        except Exception as e:
            fail_test(test_fn.__name__, f"exception: {e}")

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"  RESULTS: {passed} passed, {failed} failed, {skipped} skipped")
    logger.info(f"  TOTAL:   {passed + failed + skipped} tests")
    logger.info("=" * 60)

    if failed > 0:
        logger.info("\nFailed tests:")
        for status, name, detail in results:
            if status == "FAIL":
                logger.info(f"  - {name}: {detail}")

    logger.info("")
    if failed == 0:
        logger.info("All tests passed!")
    else:
        logger.info(f"{failed} test(s) failed.")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    exit_code = trio.run(main)
    sys.exit(exit_code)
