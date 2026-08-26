FROM python:3.12-slim

# Set environment variables
ENV MALLOC_ARENA_MAX=2 \
    MALLOC_MMAP_THRESHOLD_=131072 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
# Set timezone to IST
ENV TZ=Asia/Kolkata

# Install system dependencies required for cryptography (e.g. fastecdsa used by libp2p)
# Also install jemalloc to replace glibc malloc — jemalloc has much better
# fragmentation handling and actively returns memory to the OS.
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    libgmp-dev \
    tzdata \
    libjemalloc2 \
    && rm -rf /var/lib/apt/lists/*

# Use jemalloc as the memory allocator to prevent RSS growth from
# C-level allocation fragmentation (OpenSSL encrypt/decrypt cycles,
# aioquic packet buffers, etc.)
# - background_thread:true: use background threads for arena decay
# - decay_time:1: decay unused pages within 1 second (default is 10s)
# - narenas:4: limit arenas to reduce idle memory overhead
ENV LD_PRELOAD=libjemalloc.so.2 \
    JEMALLOC_CONF=background_thread:true,decay_time:1,narenas:4

# Set working directory
WORKDIR /app

# Install uv
RUN pip install uv

# Bust Docker cache when py-libp2p metrics branch is updated
ADD https://api.github.com/repos/sumanjeet0012/py-libp2p/git/refs/heads/metrics /tmp/libp2p_version.json

# Copy project files
COPY . .

# Install the application without using uv's internal cache for git dependencies
RUN uv pip install --system --no-cache .

# Declare data volume for persistent storage
VOLUME ["/app/.py_ipfs_lite"]

# Expose the API and Swarm ports
EXPOSE 5001
EXPOSE 4001

# Set the entrypoint to the CLI
ENTRYPOINT ["py-ipfs-lite"]
CMD ["--help"]
