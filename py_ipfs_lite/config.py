import os
from dataclasses import dataclass
from enum import Enum


class BlockStoreType(str, Enum):
    FILESYSTEM = "filesystem"
    MEMORY = "memory"


@dataclass(slots=True)
class Config:
    offline: bool = False
    reprovide_interval_seconds: int = 43200
    reprovider_strategy: str = "all"
    conn_mgr_high_water: int = 100
    conn_mgr_low_water: int = 50
    blockstore_type: BlockStoreType | str = "filesystem"
    blockstore_path: str | None = ".py_ipfs_lite/blocks"
    use_ipni: bool = False
    ipni_endpoint: str = "https://cid.contact"
    default_timeout: float = 30.0
    max_upload_size: int = 104857600  # 100MB
    max_download_size: int = 104857600  # 100MB
    # Bitswap DHT-backed provider discovery
    bitswap_max_providers: int = 10
    bitswap_provider_cache_ttl: float = 300.0
    bitswap_batch_fetch: bool = False
    # mDNS peer discovery
    enable_mdns: bool = False
    # Addresses advertised to peers via Identify, replacing the 0.0.0.0
    # listen addresses (which no peer can dial back). Comma-separated
    # multiaddrs, e.g. "/ip4/203.0.113.5/tcp/4001,/ip4/203.0.113.5/udp/4001/quic-v1".
    announce_addrs: tuple[str, ...] = ()
    # Resource-leak monitoring: streams open longer than this many seconds
    # are flagged as suspected leaks by the periodic sweep.
    stream_leak_threshold_seconds: float = 300.0
    # How often the stream leak monitor sweeps (seconds).
    stream_monitor_interval_seconds: float = 60.0
    # Set to False to disable the background leak monitor loop entirely.
    stream_monitor_enabled: bool = True
    # Event-bus driven libp2p Prometheus metrics (transport/muxer/security/
    # kad/bitswap/gossipsub families) exposed on /metrics. Disable with
    # IPFS_LITE_ENABLE_LIBP2P_METRICS=0.
    enable_libp2p_metrics: bool = True

    # Pubsub/GossipSub configuration
    enable_pubsub: bool = False
    pubsub_topics: tuple[str, ...] = ()
    gossipsub_degree: int = 6
    gossipsub_degree_low: int = 4
    gossipsub_degree_high: int = 12
    gossipsub_heartbeat_interval: float = 1.0
    gossipsub_time_to_live: int = 60

    def __post_init__(self) -> None:
        if self.reprovide_interval_seconds == 0:
            raise ValueError(
                "reprovide_interval_seconds cannot be 0. Use < 0 to disable."
            )
        if self.reprovider_strategy not in ("all", "pinned", "roots"):
            raise ValueError(
                f"Unknown reprovider_strategy: '{self.reprovider_strategy}'"
            )
        # Allow connection watermarks to be configured via env vars
        env_low = os.getenv("IPFS_LITE_CONN_MGR_LOW_WATER")
        if env_low is not None:
            self.conn_mgr_low_water = int(env_low)
        env_high = os.getenv("IPFS_LITE_CONN_MGR_HIGH_WATER")
        if env_high is not None:
            self.conn_mgr_high_water = int(env_high)

        env_metrics = os.getenv("IPFS_LITE_ENABLE_LIBP2P_METRICS")
        if env_metrics is not None:
            self.enable_libp2p_metrics = env_metrics.strip().lower() not in (
                "0",
                "false",
                "no",
                "off",
            )

        env_pubsub = os.getenv("IPFS_LITE_ENABLE_PUBSUB")
        if env_pubsub is not None:
            self.enable_pubsub = env_pubsub.strip().lower() not in (
                "0",
                "false",
                "no",
                "off",
            )

        env_topics = os.getenv("IPFS_LITE_PUBSUB_TOPICS")
        if env_topics:
            self.pubsub_topics = tuple(
                t.strip() for t in env_topics.split(",") if t.strip()
            )

        env_degree = os.getenv("IPFS_LITE_GOSSIPSUB_DEGREE")
        if env_degree is not None:
            self.gossipsub_degree = int(env_degree)

        env_degree_low = os.getenv("IPFS_LITE_GOSSIPSUB_DEGREE_LOW")
        if env_degree_low is not None:
            self.gossipsub_degree_low = int(env_degree_low)

        env_degree_high = os.getenv("IPFS_LITE_GOSSIPSUB_DEGREE_HIGH")
        if env_degree_high is not None:
            self.gossipsub_degree_high = int(env_degree_high)

        env_heartbeat = os.getenv("IPFS_LITE_GOSSIPSUB_HEARTBEAT_INTERVAL")
        if env_heartbeat is not None:
            self.gossipsub_heartbeat_interval = float(env_heartbeat)

        env_ttl = os.getenv("IPFS_LITE_GOSSIPSUB_TTL")
        if env_ttl is not None:
            self.gossipsub_time_to_live = int(env_ttl)

        if self.conn_mgr_low_water < 0 or self.conn_mgr_high_water < 0:
            raise ValueError("Connection watermarks cannot be negative.")
        if self.conn_mgr_low_water > self.conn_mgr_high_water:
            raise ValueError(
                "conn_mgr_low_water cannot be greater than conn_mgr_high_water."
            )

        # Allow announce addresses to be configured via env var so the
        # Docker image / restart script does not need code changes.
        if not self.announce_addrs:
            env_announce = os.getenv("IPFS_LITE_ANNOUNCE_ADDRS")
            if env_announce:
                self.announce_addrs = tuple(
                    a.strip() for a in env_announce.split(",") if a.strip()
                )

        try:
            self.blockstore_type = BlockStoreType(self.blockstore_type)
        except ValueError:
            valid = [e.value for e in BlockStoreType]
            raise ValueError(
                f"Unsupported blockstore_type: '{self.blockstore_type}'. "
                f"Must be one of {valid}"
            )


@dataclass(slots=True)
class AddParams:
    chunker: str = "size-262144"
    layout: str = "balanced"
    raw_leaves: bool = True
    hidden: bool = False
    hash_fun: str = "sha2-256"
    max_links: int = 174

    def __post_init__(self) -> None:
        if not self.chunker.startswith("size-"):
            raise ValueError(
                f"Invalid chunker '{self.chunker}'. Must start with 'size-'."
            )
        chunk_size_str = self.chunker[5:]
        if not chunk_size_str.isdigit() or int(chunk_size_str) <= 0:
            raise ValueError(
                f"Invalid chunker '{self.chunker}'. Size must be a positive integer."
            )
        if self.layout not in ("balanced", "trickle"):
            raise ValueError(
                f"Invalid layout '{self.layout}'. Must be 'balanced' or 'trickle'."
            )
        if self.hash_fun not in (
            "sha2-256",
            "sha2-512",
            "sha3-256",
            "sha3-512",
            "blake2b-256",
        ):
            raise ValueError(
                f"Invalid hash_fun '{self.hash_fun}'. "
                "Must be a supported multihash function."
            )
        if self.max_links < 1:
            raise ValueError(f"Invalid max_links '{self.max_links}'. Must be >= 1.")


@dataclass(slots=True)
class CLIConfig:
    port: int = int(os.getenv("IPFS_LITE_PORT", "4001"))
    api_port: int = int(os.getenv("IPFS_LITE_API_PORT", "5001"))
    seed: str | None = None
    debug: bool = False
