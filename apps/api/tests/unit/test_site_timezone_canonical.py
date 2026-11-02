"""Only exact names on the tzdata list are valid, whatever the platform (T373, F10).

Loading a name was not enough: ``posixrules`` and ``Factory`` load but are not zones, and on a
case-insensitive file system ``asia/jerusalem`` loads too, so one name was valid on a laptop and
refused on a server. The list now comes from the ``tzdata`` package, which is the same everywhere.

Covers: FR-217, FR-231.
"""

import uuid

import pytest

from hds.core.exceptions import DomainValidationError
from hds.scripts import check_site_timezones as script
from hds.services import site
from hds.services.site import validate_timezone

REFUSED = ["posixrules", "Factory", "asia/jerusalem", "ASIA/JERUSALEM", "utc", "localtime"]


@pytest.mark.parametrize("name", REFUSED)
def test_a_name_that_is_not_canonical_is_refused(name: str) -> None:
    with pytest.raises(DomainValidationError) as refused:
        validate_timezone(name)
    assert refused.value.code == "sites.timezone_invalid"


def test_the_result_does_not_depend_on_the_file_system(monkeypatch: pytest.MonkeyPatch) -> None:
    def no_loading_from_disk(name: str) -> object:
        return object()  # a file system that would load anything, whatever the case

    monkeypatch.setattr(site, "ZoneInfo", no_loading_from_disk)

    with pytest.raises(DomainValidationError):
        validate_timezone("asia/jerusalem")
    validate_timezone("Asia/Jerusalem")


def test_a_listed_name_must_still_load(monkeypatch: pytest.MonkeyPatch) -> None:
    def nothing_loads(name: str) -> object:
        raise KeyError(name)

    monkeypatch.setattr(site, "ZoneInfo", nothing_loads)

    with pytest.raises(DomainValidationError):
        validate_timezone("Asia/Jerusalem")


@pytest.mark.parametrize("name", REFUSED)
async def test_the_report_lists_a_stored_value_that_fails_the_rule(
    name: str, capsys: pytest.CaptureFixture[str]
) -> None:
    site_id, org_id = uuid.uuid4(), uuid.uuid4()

    async def fetch() -> list[tuple[uuid.UUID, uuid.UUID, str]]:
        return [(site_id, org_id, name)]

    assert await script.run(fetch) == 1
    assert str(site_id) in capsys.readouterr().out
