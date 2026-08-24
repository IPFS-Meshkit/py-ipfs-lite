"""PubSub messaging routes for the py-ipfs-lite HTTP API."""

from typing import Any

import trio
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from py_ipfs_lite.peer import Peer

router = APIRouter()


@router.post("/api/v0/pubsub/ls")
@router.get("/api/v0/pubsub/ls")
async def pubsub_ls(request: Request) -> Any:
    """
    List our pubsub subscriptions and topics peers have announced.

    ``peer_topics`` is live topic discovery: it shows every topic that at
    least one currently-connected peer has subscribed to (via gossipsub
    SUB announcements), with the number of such peers. Useful for deciding
    which topics are worth joining.
    """
    peer: Peer = request.app.state.peer
    ps = getattr(peer, "pubsub", None)
    if ps is None:
        return JSONResponse(content={"error": "pubsub not enabled"}, status_code=400)

    my_topics = sorted(str(t) for t in getattr(ps, "topic_ids", ()) or ())
    peer_topics: dict[str, int] = {}
    for topic, pids in getattr(ps, "peer_topics", {}).items():
        peer_topics[str(topic)] = len(pids)

    mesh: dict[str, int] = {}
    gs = getattr(peer, "_gossipsub", None)
    if gs is not None:
        for topic, members in getattr(gs, "mesh", {}).items():
            mesh[str(topic)] = len(members)

    return JSONResponse(
        content={
            "my_topics": my_topics,
            "auto_joined": sorted(getattr(peer, "_auto_joined_topics", ()) or ()),
            "peer_topics": dict(
                sorted(peer_topics.items(), key=lambda kv: kv[1], reverse=True)
            ),
            "mesh": mesh,
        }
    )


@router.post("/api/v0/pubsub/pub")
async def pubsub_pub(
    request: Request,
    arg: str = Query(..., description="The topic to publish to"),
) -> Any:
    """Publish a message (raw request body) to a pubsub topic."""
    peer: Peer = request.app.state.peer
    if getattr(peer, "pubsub", None) is None:
        raise HTTPException(status_code=400, detail="pubsub not enabled")
    body = await request.body()
    if not body:
        raise HTTPException(status_code=400, detail="Empty message body")
    await peer.publish_pubsub(arg, body)
    return JSONResponse(content={"Topic": arg, "Size": len(body)})


@router.post("/api/v0/pubsub/sub")
@router.get("/api/v0/pubsub/sub")
async def pubsub_sub(
    request: Request,
    arg: str = Query(..., description="The topic to read messages from"),
    count: int = Query(0, ge=0, description="Stop after this many messages (0 = all)"),
    timeout: float = Query(
        5.0, gt=0, le=120, description="Seconds to wait for messages"
    ),
) -> Any:
    """
    Read messages received on a pubsub topic.

    Subscribes first if necessary. Returns messages buffered since the last
    call (each message is delivered exactly once). Waits up to ``timeout``
    seconds for at least one message when the buffer is empty.
    """
    peer: Peer = request.app.state.peer
    if getattr(peer, "pubsub", None) is None:
        raise HTTPException(status_code=400, detail="pubsub not enabled")

    if arg not in peer._pubsub_subscriptions:
        await peer.subscribe_pubsub_topic(arg)

    deadline = trio.current_time() + timeout
    while True:
        messages = peer.get_pubsub_messages(arg)
        if messages and (count <= 0 or len(messages) >= count):
            break
        if trio.current_time() >= deadline:
            break
        await trio.sleep(0.1)

    if count > 0:
        messages = messages[:count]
    return JSONResponse(content={"Topic": arg, "Messages": messages})


@router.delete("/api/v0/pubsub/sub")
async def pubsub_unsub(
    request: Request,
    arg: str = Query(..., description="The topic to unsubscribe from"),
) -> Any:
    """Unsubscribe from a pubsub topic."""
    peer: Peer = request.app.state.peer
    removed = await peer.unsubscribe_pubsub_topic(arg)
    return JSONResponse(content={"Topic": arg, "Unsubscribed": bool(removed)})
