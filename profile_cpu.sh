#!/bin/bash
# Quick CPU profiling via py-spy stack sampling
# Run on the production host, NOT inside the container

declare -A counts
samples=30

for i in $(seq 1 $samples); do
    # Get Thread 1 stack trace
    stack=$(docker exec --privileged ipfs-node py-spy dump --pid 1 2>/dev/null | grep -A 30 "Thread 1 (active" | head -30)
    
    # Extract first meaningful function from the stack
    top_func=$(echo "$stack" | grep "^    " | head -1 | sed 's/ (.*//;s/    //')
    
    # Categorize
    case "$top_func" in
        *yamux*) cat="yamux" ;;
        *noise*|*encrypt*) cat="noise" ;;
        *multiselect*) cat="multiselect" ;;
        *bitswap*) cat="bitswap" ;;
        *aioquic*|*quic*) cat="aioquic" ;;
        *epoll*|*trio*|*run*|*unrolled*) cat="trio/event_loop" ;;
        *cid*|*multibase*|*baseconv*) cat="cid/encoding" ;;
        *peer*|*py_ipfs*) cat="py_ipfs_lite" ;;
        *msgio*|*io*|*read*|*write*) cat="io/read_write" ;;
        *leak_monitor*) cat="MEM_DIAG" ;;
        *service*|*iterate*) cat="anyio_service" ;;
        *stream*|*muxer*) cat="muxer" ;;
        *) cat="other: $top_func" ;;
    esac
    
    counts["$cat"]=$(( ${counts["$cat"]:-0} + 1 ))
    sleep 1
done

echo "=== CPU Profile: $samples samples, 1s apart ==="
echo ""
for cat in "${!counts[@]}"; do
    pct=$(echo "scale=1; ${counts[$cat]} * 100 / $samples" | bc)
    bar=$(printf '#%.0s' $(seq 1 $(echo "${counts[$cat]} * 50 / $samples" | bc)))
    echo "  ${pct}%  ${cat}  ${bar}"
done | sort -rn
