from py_ipfs_lite.config import AddParams, Config


def test_config_defaults() -> None:
    cfg = Config()
    assert cfg.offline is False
    assert cfg.reprovide_interval_seconds == 43200
    assert cfg.use_ipni is False
    assert cfg.announce_addrs == ()


def test_config_announce_addrs_from_env(monkeypatch) -> None:
    """announce_addrs is parsed from the IPFS_LITE_ANNOUNCE_ADDRS env var."""
    monkeypatch.setenv(
        "IPFS_LITE_ANNOUNCE_ADDRS",
        "/ip4/52.7.183.75/tcp/4001,/ip4/52.7.183.75/udp/4001/quic-v1",
    )
    cfg = Config()
    assert cfg.announce_addrs == (
        "/ip4/52.7.183.75/tcp/4001",
        "/ip4/52.7.183.75/udp/4001/quic-v1",
    )


def test_config_announce_addrs_explicit_wins(monkeypatch) -> None:
    """Explicitly passed announce_addrs override the env var."""
    monkeypatch.setenv("IPFS_LITE_ANNOUNCE_ADDRS", "/ip4/8.8.8.8/tcp/4001")
    cfg = Config(announce_addrs=("/ip4/52.7.183.75/tcp/4001",))
    assert cfg.announce_addrs == ("/ip4/52.7.183.75/tcp/4001",)


def test_add_params_defaults() -> None:
    params = AddParams()
    assert params.chunker == "size-262144"


def test_config_blockstore_type_validation() -> None:
    import pytest

    with pytest.raises(ValueError, match="Unsupported blockstore_type"):
        Config(blockstore_type="FILESYSTEM")

    with pytest.raises(ValueError, match="Unsupported blockstore_type"):
        Config(blockstore_type="invalid")

    cfg = Config(blockstore_type="filesystem")
    from py_ipfs_lite.config import BlockStoreType

    assert cfg.blockstore_type == BlockStoreType.FILESYSTEM


def test_config_validation() -> None:
    import pytest

    with pytest.raises(ValueError, match="cannot be 0"):
        Config(reprovide_interval_seconds=0)

    with pytest.raises(ValueError, match="Unknown reprovider_strategy"):
        Config(reprovider_strategy="foo")

    with pytest.raises(ValueError, match="cannot be negative"):
        Config(conn_mgr_low_water=-1)

    with pytest.raises(ValueError, match="cannot be greater than"):
        Config(conn_mgr_low_water=1000, conn_mgr_high_water=500)


def test_add_params_validation() -> None:
    import pytest

    with pytest.raises(ValueError, match="Must start with 'size-'"):
        AddParams(chunker="SIZE-1024")

    with pytest.raises(ValueError, match="must be a positive integer"):
        AddParams(chunker="size-0")

    with pytest.raises(ValueError, match="must be a positive integer"):
        AddParams(chunker="size-abc")
