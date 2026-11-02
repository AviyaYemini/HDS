"""The read-only report of sites whose time zone is not a real IANA name (T297, FR-218).

Sites created before the check may hold a bad zone. The report lists each by site id and
organisation id (no names or addresses), prints nothing when the data is clean (exit 0) and exits
1 when it finds any. It changes nothing: a bad zone cannot be guessed, so staff correct it.

Covers: FR-218.
"""

import uuid

import pytest

from hds.scripts import check_site_timezones as script

ORG = uuid.uuid4()


def site(zone: str) -> tuple[uuid.UUID, uuid.UUID, str]:
    return uuid.uuid4(), ORG, zone


def test_real_zones_are_valid_and_the_rest_are_not() -> None:
    assert script.is_valid_zone("Asia/Jerusalem") and script.is_valid_zone("UTC")
    assert not any(script.is_valid_zone(z) for z in ("Mars/Base", "", "../etc/passwd"))


def test_only_sites_with_an_invalid_zone_are_found() -> None:
    good, bad = site("Asia/Kolkata"), site("Mars/Base")

    assert script.find_invalid([good, bad]) == [bad]


async def test_clean_data_prints_nothing_and_exits_zero(capsys: pytest.CaptureFixture[str]) -> None:
    async def fetch() -> list[tuple[uuid.UUID, uuid.UUID, str]]:
        return [site("UTC"), site("Asia/Jerusalem")]

    assert await script.run(fetch) == 0
    assert capsys.readouterr().out == ""


async def test_invalid_zones_are_listed_by_id_and_the_exit_status_is_one(
    capsys: pytest.CaptureFixture[str],
) -> None:
    bad = site("Mars/Base")

    async def fetch() -> list[tuple[uuid.UUID, uuid.UUID, str]]:
        return [site("UTC"), bad]

    assert await script.run(fetch) == 1
    out = capsys.readouterr().out
    assert str(bad[0]) in out and str(ORG) in out
