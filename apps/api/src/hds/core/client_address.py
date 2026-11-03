"""Which network address a request came from, and the key it is counted under.

Layer: core. Imports only the standard library.

``X-Forwarded-For`` is written by the client, and each proxy appends the address it saw, so only the
entries added by trusted proxies can be believed, counted from the right. With no trusted proxy the
header is ignored. A wrong count cannot be detected at start-up: too low and every user shares the
proxy's address; too high and a client can pick its own.
"""

import ipaddress
from typing import Any

UNKNOWN = "unknown"
IPV6_PREFIX = 64


def client_address(request: Any, trusted_proxy_count: int) -> str:
    """Return the address to count a request against.

    Args:
        request: The incoming request (``headers`` and ``client``).
        trusted_proxy_count: Reverse proxies in front of the API; 0 ignores the header.

    Returns:
        The Nth entry from the right of ``X-Forwarded-For`` for N trusted proxies, or the connecting
        address when the header is ignored, too short or malformed there.
    """
    peer = request.client.host if request.client else UNKNOWN
    if trusted_proxy_count <= 0:
        return str(peer)
    entries = [entry.strip() for entry in request.headers.get("x-forwarded-for", "").split(",")]
    if len(entries) < trusted_proxy_count:
        return str(peer)
    chosen = str(entries[-trusted_proxy_count])
    try:
        ipaddress.ip_address(chosen)
    except ValueError:
        return str(peer)
    return chosen


def network_key(address: str) -> str:
    """Reduce an address to what is counted as one network.

    Args:
        address: An IPv4 or IPv6 address, or anything else.

    Returns:
        An IPv4 address whole; an IPv6 address as its /64, because a client controls the whole /64
        and could otherwise rotate addresses to avoid the limit; any other value unchanged.
    """
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError:
        return address
    if isinstance(parsed, ipaddress.IPv6Address):
        if parsed.ipv4_mapped is not None:
            return str(parsed.ipv4_mapped)
        return str(ipaddress.ip_network((parsed, IPV6_PREFIX), strict=False))
    return str(parsed)
