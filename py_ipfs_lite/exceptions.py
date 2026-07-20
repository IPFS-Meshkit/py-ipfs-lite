class IPFSLiteError(Exception):
    """Base exception for all py-ipfs-lite errors."""

    pass


class BlockNotFoundError(IPFSLiteError):
    """Raised when a block/CID cannot be found in the local store or network."""

    pass


class PinNotFoundError(IPFSLiteError):
    """Raised when attempting to operate on a pin that does not exist."""

    pass


class PinError(IPFSLiteError):
    """Raised when an invalid pin operation is attempted (e.g., downgrading a pin)."""

    pass


class PeerNotStartedError(IPFSLiteError):
    """Raised when attempting to use a Peer that has not been started."""

    pass


class RoutingError(IPFSLiteError):
    """Raised when a routing operation (e.g., DHT publish/resolve) fails."""

    pass


class CarParseError(IPFSLiteError):
    """Raised when a CAR file is malformed, truncated, or cannot be parsed."""

    pass


class InvalidCidError(IPFSLiteError):
    """Raised when a CID/path string cannot be parsed."""

    pass


class DagTooDeepError(IPFSLiteError):
    """Raised when a DAG node's structure exceeds a safe recursion depth."""

    pass


class PayloadTooLargeError(IPFSLiteError):
    """Raised when an upload or download exceeds the configured size limit."""

    pass
