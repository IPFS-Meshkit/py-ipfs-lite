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
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    libgmp-dev \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

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
