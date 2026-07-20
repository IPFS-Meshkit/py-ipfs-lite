"""
DAG-JSON encoding shared by the FastAPI dag/get route and any MCP tool
that returns DAG nodes. Moved out of api.py verbatim — same behavior.
"""

import base64
import json
from typing import Any


class DAGJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, bytes):
            return {"/": {"bytes": base64.b64encode(obj).decode("ascii")}}

        obj_type = type(obj).__name__

        if obj_type == "CBORTag" and getattr(obj, "tag", None) == 42:
            from py_ipfs_lite.peer import format_cid_for_display, parse_cid

            cid_bytes = obj.value[1:]
            link_cid = parse_cid(cid_bytes)
            return {"/": format_cid_for_display(link_cid)}

        if obj_type == "PBLink":
            from py_ipfs_lite.peer import format_cid_for_display, parse_cid

            res = {}
            if getattr(obj, "Hash", None):
                res["Hash"] = {"/": format_cid_for_display(parse_cid(obj.Hash))}
            if getattr(obj, "Name", None):
                res["Name"] = obj.Name
            if getattr(obj, "Tsize", None) is not None:
                res["Tsize"] = obj.Tsize
            return res

        return super().default(obj)


def node_data_to_json_safe(node_data: Any) -> Any:
    """
    For adapters (like MCP) that need a plain dict/list rather than a
    json.JSONEncoder — round-trips through the encoder above.
    """
    return json.loads(json.dumps(node_data, cls=DAGJSONEncoder))
