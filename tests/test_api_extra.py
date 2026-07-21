import pytest
from httpx import ASGITransport, AsyncClient

from py_ipfs_lite.api import app
from py_ipfs_lite.config import Config
from py_ipfs_lite.peer import Peer


@pytest.fixture
def memory_config():
    return Config(blockstore_type="memory", reprovide_interval_seconds=-1)


@pytest.fixture
async def client(memory_config):
    peer = Peer(memory_config, listen_addrs=["/ip4/127.0.0.1/tcp/0"])
    await peer.start()
    try:
        app.state.peer = peer
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac
    finally:
        await peer.close()


@pytest.mark.trio
async def test_api_add_and_cat(client):
    # Test /api/v0/add
    data = b"hello ipfs api!"
    res = await client.post("/api/v0/add", files={"file": ("hello.txt", data, "application/octet-stream")})
    assert res.status_code == 200, res.text
    cid = res.json()["Hash"]
    assert cid is not None

    # Test /api/v0/cat
    res = await client.post(f"/api/v0/cat?arg={cid}")
    assert res.status_code == 200, res.text
    assert res.content == data


@pytest.mark.trio
async def test_api_dag_put_and_get(client):
    # Test /api/v0/dag/put
    json_data = b'{"hello": "world"}'
    res = await client.post("/api/v0/dag/put?store-codec=dag-json", content=json_data, headers={"content-type": "application/json"})
    assert res.status_code == 200, res.text
    cid = res.json()["Cid"]["/"]
    assert cid is not None

    # Test /api/v0/dag/get
    res = await client.post(f"/api/v0/dag/get?arg={cid}")
    assert res.status_code == 200, res.text
    assert res.json() == {"hello": "world"}


@pytest.mark.trio
async def test_api_pin_add_and_rm(client):
    # Add a block to pin
    data = b"pin me!"
    res = await client.post("/api/v0/add", files={"file": ("pin.txt", data, "application/octet-stream")})
    assert res.status_code == 200, res.text
    cid = res.json()["Hash"]

    # Test /api/v0/pin/add
    res = await client.post(f"/api/v0/pin/add?arg={cid}")
    assert res.status_code == 200, res.text
    assert cid in res.json()["Pins"]

    # Test /api/v0/pin/ls
    res = await client.post("/api/v0/pin/ls")
    assert res.status_code == 200, res.text
    assert cid in res.json()["Keys"]

    # Test /api/v0/pin/rm
    res = await client.post(f"/api/v0/pin/rm?arg={cid}")
    assert res.status_code == 200, res.text
    assert cid in res.json()["Pins"]
