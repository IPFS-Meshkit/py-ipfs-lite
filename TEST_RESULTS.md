# Manual API Test Results

## Date: 2026-07-29

### PASS (27 endpoints)
| # | Method | Endpoint | Status |
|---|--------|----------|--------|
| 1 | GET | /api/v0/version | PASS |
| 2 | POST | /api/v0/version | PASS |
| 3 | GET | /api/v0/id | PASS |
| 4 | POST | /api/v0/id | PASS |
| 5 | GET | /api/v0/repo/stat | PASS |
| 6 | POST | /api/v0/repo/stat | PASS |
| 7 | GET | /api/v0/repo/version | PASS |
| 8 | POST | /api/v0/repo/version | PASS |
| 9 | POST | /api/v0/refs/local | PASS |
| 10 | GET | /api/v0/pin/ls (default) | PASS |
| 11 | GET | /api/v0/pin/ls (recursive) | PASS |
| 12 | GET | /api/v0/pin/ls (direct) | PASS |
| 13 | POST | /api/v0/pin/ls | PASS |
| 14 | GET | /api/v0/swarm/peers | PASS |
| 15 | POST | /api/v0/swarm/peers | PASS |
| 16 | GET | /api/v0/swarm/connection_stats | PASS |
| 17 | POST | /api/v0/swarm/connection_stats | PASS |
| 18 | POST | /api/v0/add (text file) | PASS |
| 19 | POST | /api/v0/add (JSON file) | PASS |
| 20 | GET | /api/v0/cat | PASS |
| 21 | POST | /api/v0/cat | PASS |
| 22 | POST | /api/v0/block/stat | PASS |
| 23 | GET | /api/v0/block/get | PASS |
| 24 | GET | /api/v0/debug/peerstore | PASS |
| 25 | POST | /api/v0/debug/peerstore | PASS |
| 26 | GET | /api/v0/debug/routing_table | PASS |
| 27 | POST | /api/v0/debug/routing_table | PASS |

### FAIL (3 bugs found)
| # | Method | Endpoint | Status | Bug |
|---|--------|----------|--------|-----|
| 1 | POST | /api/v0/block/put | PASS but **returns garbage CID** | `Key` field shows `b'\x01U\x12...'` (Python bytes repr) instead of proper CID string |
| 2 | POST | /api/v0/block/rm | **FAIL** | Cannot use the invalid CID from block/put |
| 3 | POST | /api/v0/dag/put | **FAIL** | Empty body received when sending JSON via POST (test script bug - confirmed API works via frontend) |

### WARNINGS
| # | Method | Endpoint | Status | Issue |
|---|--------|----------|--------|-------|
| 1 | GET | /api/v0/cat?arg=Invalid | ERROR | Returns `IncompleteRead(0 bytes read)` instead of proper 404 JSON error |
| 2 | POST | /api/v0/block/stat?arg=QmInvalid | FAIL | Returns `Invalid CID string: QmInvalid` (correct error, but status 500 instead of 400) |
| 3 | POST | /api/v0/pin/add?arg=QmInvalid | - | Need to verify error handling |

### ROOT CAUSE: block/put CID Bug
The `block_service.put_block()` returns `str(cid)` where `cid` comes from `compute_cid_v1(data, codec="raw")`. The CID object's `__str__` method returns the raw multihash bytes representation instead of a base-encoded CID string.

Fix: Use `format_cid_for_display(cid)` instead of `str(cid)` in `block_service.py:put_block()`.
