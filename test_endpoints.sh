#!/bin/bash
set -e

API_URL="http://127.0.0.1:5001/api/v0"

echo "=== Testing api_version ==="
curl -s -X POST "${API_URL}/version" | grep '"Version"'
echo

echo "=== Testing api_id ==="
curl -s -X POST "${API_URL}/id" | grep '"ID"'
echo

echo "=== Testing swarm_peers ==="
curl -s -X POST "${API_URL}/swarm/peers" | grep '"count"'
echo

echo "=== Testing debug/conns ==="
curl -s -X GET "http://127.0.0.1:5001/debug/conns" | grep '"total_connections"'
echo

echo "=== Testing repo_stat ==="
curl -s -X POST "${API_URL}/repo/stat" | grep '"NumObjects"'
echo

echo "=== Testing repo_version ==="
curl -s -X POST "${API_URL}/repo/version" | grep '"Version"'
echo

echo "=== Testing add_file ==="
echo "Hello IPFS" > test.txt
RES=$(curl -s -X POST -F "file=@test.txt" "${API_URL}/add")
echo $RES
CID=$(echo $RES | grep -o '"Hash":"[^"]*"' | cut -d'"' -f4)
echo "Got CID: $CID"
echo

echo "=== Testing cat_file ==="
curl -s -X POST "${API_URL}/cat?arg=$CID"
echo

echo "=== Testing dag_put ==="
DAG_RES=$(curl -s -X POST "${API_URL}/dag/put?format=dag-cbor&input-enc=json" \
  -H "Content-Type: application/json" \
  -d '{"name":"Sumanjeet","project":"py-ipfs-lite","language":"Python","version":1}')
echo $DAG_RES
DAG_CID=$(echo $DAG_RES | grep -o '"/":"[^"]*"' | cut -d'"' -f4)
echo "Got DAG CID: $DAG_CID"
echo

echo "=== Testing dag_get ==="
curl -s -X POST "${API_URL}/dag/get?arg=$DAG_CID"
echo
echo

echo "=== Testing block_stat ==="
curl -s -X POST "${API_URL}/block/stat?arg=$DAG_CID"
echo

echo "=== Testing block_rm ==="
curl -s -X POST "${API_URL}/block/rm?arg=$DAG_CID"
echo

echo "=== Testing pin_add ==="
curl -s -X POST "${API_URL}/pin/add?arg=$CID"
echo

echo "=== Testing pin_ls ==="
curl -s -X POST "${API_URL}/pin/ls" | grep "$CID"
echo

echo "=== Testing pin_rm ==="
curl -s -X POST "${API_URL}/pin/rm?arg=$CID"
echo

echo "=== Testing repo_gc ==="
curl -s -X POST "${API_URL}/repo/gc" | grep "Key"
echo

echo "All tests completed!"
rm test.txt
