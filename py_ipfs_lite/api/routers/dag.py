"""IPLD DAG & CAR routes for the py-ipfs-lite HTTP API."""

import json
from typing import Any

import trio
from fastapi import APIRouter, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import JSONResponse, Response

from py_ipfs_lite.api.routers._shared import local_block
from py_ipfs_lite.exceptions import BlockNotFoundError, InvalidCidError
from py_ipfs_lite.peer import Peer

router = APIRouter()


@router.post("/api/v0/dag/put")
async def dag_put(
    request: Request, store_codec: str = Query("dag-json", alias="store-codec")
) -> Any:
    """Store a generic DAG node."""
    peer: Peer = request.app.state.peer

    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" in content_type:
        form = await request.form()
        file_field = form.get("file") or form.get("data")
        if not file_field and form.keys():
            file_field = form[list(form.keys())[0]]

        if isinstance(file_field, UploadFile):
            body = await file_field.read()
        elif file_field is not None:
            body = str(file_field).encode("utf-8")
        else:
            body = b""
    else:
        body = await request.body()

    try:
        node_data = json.loads(body)
        from py_ipfs_lite.services import dag_service

        result = await dag_service.put_node(peer, node_data, codec=store_codec)
        return JSONResponse(content={"Cid": {"/": result.cid}})
    except (json.JSONDecodeError, RecursionError) as e:
        raise HTTPException(status_code=400, detail=f"{str(e)} | Body: {repr(body)}")


@router.post("/api/v0/dag/get")
@router.get("/api/v0/dag/get")
async def dag_get(
    request: Request, arg: str = Query(..., description="The object to get")
) -> Any:
    """Retrieve a generic DAG node."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import dag_encoding, dag_service

    try:
        result = await dag_service.get_node(peer, arg)
    except BlockNotFoundError:
        raise HTTPException(status_code=404, detail=f"Block not found: {arg}")
    except (InvalidCidError, ValueError):
        raise HTTPException(status_code=400, detail=f"Invalid CID: {arg}")
    except trio.TooSlowError:
        # Network fetch for a block that exists nowhere expired.
        raise HTTPException(status_code=404, detail=f"Block not found: {arg}")

    accept = request.headers.get("accept", "")
    if result.cid_codec in ("dag-cbor", "cbor") and "application/cbor" in accept:
        import cbor2

        return Response(
            content=cbor2.dumps(result.node_data), media_type="application/cbor"
        )

    if result.cid_codec == "raw":
        return Response(content=result.node_data, media_type="application/octet-stream")

    encoded = json.dumps(result.node_data, cls=dag_encoding.DAGJSONEncoder)
    return Response(content=encoded, media_type="application/json")


@router.post("/api/v0/dag/export")
@router.get("/api/v0/dag/export")
async def dag_export(
    request: Request,
    arg: str = Query(..., description="Root CID of the DAG to export as CAR"),
) -> Response:
    """Export the DAG rooted at *arg* as a CARv1 file."""
    import os
    import tempfile

    from fastapi.responses import FileResponse
    from starlette.background import BackgroundTask

    peer: Peer = request.app.state.peer
    fd, path = tempfile.mkstemp(suffix=".car", prefix="py-ipfs-lite-export-")
    os.close(fd)
    try:
        await peer.export_car(arg, path)
    except Exception:
        os.unlink(path)
        raise
    return FileResponse(
        path,
        media_type="application/vnd.ipld.car",
        filename=f"{arg}.car",
        background=BackgroundTask(os.unlink, path),
    )


@router.post("/api/v0/dag/import")
async def dag_import(request: Request, file: UploadFile = File(...)) -> Any:
    """Import a CAR file into the blockstore. Returns the root CIDs found."""
    import os
    import tempfile

    peer: Peer = request.app.state.peer
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Empty CAR payload")

    fd, path = tempfile.mkstemp(suffix=".car", prefix="py-ipfs-lite-import-")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        roots = await peer.import_car(path)
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
    return JSONResponse(content={"Roots": [{"Hash": r} for r in roots]})


@router.post("/api/v0/refs")
@router.get("/api/v0/refs")
async def refs_local_object(
    request: Request,
    arg: str = Query(..., description="CID whose direct children should be listed"),
    recursive: bool = Query(False, description="Walk the whole DAG"),
) -> Any:
    """List references (child links) of a local DAG node."""
    from libp2p.bitswap.cid import parse_cid as _parse_cid_raw

    from py_ipfs_lite.dag_utils import extract_cids

    # Validate the root exists locally before walking.
    await local_block(request.app.state.peer, arg)
    seen: set[str] = set()
    out: list[dict[str, Any]] = []

    def _cid_str(raw: bytes) -> str | None:
        try:
            return str(_parse_cid_raw(raw))
        except Exception:
            return None

    async def visit(cid_str: str) -> None:
        if cid_str in seen:
            return
        seen.add(cid_str)
        data = await local_block(request.app.state.peer, cid_str)

        # dag-pb: decode protobuf links with names/sizes
        try:
            from libp2p.bitswap.dag_pb import decode_dag_pb

            links, _unixfs = decode_dag_pb(data)
            children: list[bytes] = []
            for link in links:
                child = _cid_str(link.cid)
                if child is None:
                    continue
                children.append(link.cid)
                out.append({"Ref": child, "Name": link.name, "Size": link.size})
            if recursive:
                for raw_child in children:
                    child = _cid_str(raw_child)
                    if child:
                        await visit(child)
            return
        except Exception:
            pass

        # dag-cbor / dag-json: structural CID extraction
        for raw in extract_cids(data):
            child = _cid_str(raw)
            if child is not None and child not in {r["Ref"] for r in out}:
                out.append({"Ref": child})
        if recursive:
            pass  # structural walk handled by extract_cids above (non-recursive)

    await visit(arg)
    return JSONResponse(content={"Refs": out})
