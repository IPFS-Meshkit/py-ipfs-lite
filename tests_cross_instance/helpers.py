"""Shared helpers for cross-instance tests."""
import json
import time
import urllib.request
import urllib.parse
import urllib.error

from . import DEV_URL, PROD_URL, DEV_ADDR, PROD_ADDR


def api(base_url, path, data=None, method="POST", headers=None, timeout=60):
    """Make an API call. Returns (status_code, parsed_json_or_bytes)."""
    url = f"{base_url}{path}"
    req_data = None
    if data is not None:
        if isinstance(data, (dict, list)):
            req_data = json.dumps(data).encode()
            headers = headers or {}
            headers["Content-Type"] = "application/json"
        elif isinstance(data, bytes):
            req_data = data
        elif isinstance(data, str):
            req_data = data.encode()
    req = urllib.request.Request(url, data=req_data, method=method, headers=headers or {})
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        content_type = resp.headers.get("Content-Type", "")
        body = resp.read()
        if "json" in content_type:
            return resp.status, json.loads(body)
        else:
            return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, body
    except Exception as e:
        return -1, str(e)


def check_endpoint(base_url, path):
    """Check if an endpoint is implemented (not 404/405)."""
    status, _ = api(base_url, path + "?arg=test", timeout=10)
    return status not in (404, 405, -1)


def upload_multipart(base_url, path, filename, file_bytes, extra_fields=None):
    """Upload a file via multipart/form-data."""
    url = f"{base_url}{path}"
    boundary = f"----WebKitFormBoundary{int(time.time() * 1000)}"
    parts = []

    if extra_fields:
        for k, v in extra_fields.items():
            parts.append(
                f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}'.encode()
            )

    parts.append(
        f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{filename}"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n".encode()
        + file_bytes
        + f"\r\n--{boundary}--\r\n".encode()
    )

    body = b"\r\n".join(parts)
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    try:
        resp = urllib.request.urlopen(req, timeout=120)
        return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, body
    except Exception as e:
        return -1, str(e)


def connect_nodes(from_url, to_addr):
    """Connect two nodes via swarm/connect."""
    status, data = api(from_url, f"/api/v0/swarm/connect?arg={urllib.parse.quote(to_addr, safe='')}")
    time.sleep(2)
    return status, data


def get_peers(base_url):
    """Get list of connected peers."""
    status, data = api(base_url, "/api/v0/swarm/peers")
    if status == 200:
        return data.get("peers", data.get("Peers", []))
    return []


def get_id(base_url):
    """Get peer ID from a node."""
    status, data = api(base_url, "/api/v0/id")
    assert status == 200, f"GET /api/v0/id failed: {status}"
    return data["ID"]
