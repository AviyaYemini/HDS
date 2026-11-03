"""Which address a failed sign-in is counted against (T353, FR-223).

The forwarded header is written by the client unless a trusted proxy overwrites its end, so it is
read only as configured, and only from the right. An IPv6 client controls a whole /64 and could
otherwise rotate addresses to avoid the network limit.

Covers: FR-223, FR-221.
"""

from types import SimpleNamespace
from typing import Any

import pytest

from hds.core.client_address import client_address, network_key

PEER = "10.0.0.9"


def request(forwarded: str | None = None, peer: str | None = PEER) -> Any:
    headers = {} if forwarded is None else {"x-forwarded-for": forwarded}
    client = None if peer is None else SimpleNamespace(host=peer)
    return SimpleNamespace(headers=headers, client=client)


def test_at_zero_the_header_is_ignored_so_a_client_cannot_pick_its_address() -> None:
    assert client_address(request("1.2.3.4"), 0) == PEER


@pytest.mark.parametrize(
    ("forwarded", "count", "expected"),
    [
        ("203.0.113.7", 1, "203.0.113.7"),
        ("9.9.9.9, 203.0.113.7", 1, "203.0.113.7"),  # the client wrote the left entry
        ("9.9.9.9, 203.0.113.7, 10.0.0.2", 2, "203.0.113.7"),
        ("203.0.113.7", 2, PEER),  # shorter than the count: nothing can be trusted
        ("not-an-address", 1, PEER),  # malformed: ignored, not trusted
        ("203.0.113.7, not-an-address", 1, PEER),
        ("", 1, PEER),
    ],
)
def test_the_entry_is_chosen_from_the_right(forwarded: str, count: int, expected: str) -> None:
    assert client_address(request(forwarded), count) == expected


def test_no_header_falls_back_to_the_connecting_address() -> None:
    assert client_address(request(), 1) == PEER


def test_no_connecting_address_still_gives_a_key() -> None:
    assert client_address(request(peer=None), 0) == "unknown"


def test_an_ipv4_address_is_kept_whole() -> None:
    assert network_key("203.0.113.7") != network_key("203.0.113.8")


def test_ipv6_addresses_in_one_slash_64_share_a_key() -> None:
    assert network_key("2001:db8:1:2::1") == network_key("2001:db8:1:2:ffff::9")
    assert network_key("2001:db8:1:2::1") != network_key("2001:db8:1:3::1")


def test_an_unparseable_value_still_gives_a_key() -> None:
    assert network_key("unknown") == "unknown"
