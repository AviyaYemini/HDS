"""The read-only report of addresses that differ only by letter case (T285, FR-216).

Staff run it before migration 016 to find the accounts they must resolve. It reads and never
changes anything, prints nothing when the data is clean (exit 0), and otherwise lists the accounts
of each clash by id, with no addresses, and exits 1.

Covers: FR-216.
"""

import uuid

import pytest

from hds.scripts import find_email_case_duplicates as script

ORG_A, ORG_B = uuid.uuid4(), uuid.uuid4()


def row(email: str, org: uuid.UUID = ORG_A) -> tuple[uuid.UUID, uuid.UUID, str]:
    return uuid.uuid4(), org, email


def test_addresses_that_differ_only_by_case_form_one_group() -> None:
    bob_1, bob_2, other = row("Bob@corp.com"), row("bob@CORP.com", ORG_B), row("carol@corp.com")

    groups = script.find_clashes([bob_1, bob_2, other])

    assert [sorted(str(a[0]) for a in g) for g in groups] == [
        sorted([str(bob_1[0]), str(bob_2[0])])
    ]


def test_distinct_addresses_and_a_single_spelling_are_not_clashes() -> None:
    assert script.find_clashes([row("a@corp.com"), row("b@corp.com")]) == []


async def test_a_clean_database_prints_nothing_and_exits_zero(
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def fetch() -> list[tuple[uuid.UUID, uuid.UUID, str]]:
        return [row("a@corp.com"), row("b@corp.com")]

    assert await script.run(fetch) == 0
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == ""


async def test_clashes_are_listed_by_id_without_addresses_and_the_exit_status_is_one(
    capsys: pytest.CaptureFixture[str],
) -> None:
    first, second = row("Bob@corp.com"), row("bob@corp.com", ORG_B)

    async def fetch() -> list[tuple[uuid.UUID, uuid.UUID, str]]:
        return [first, second, row("carol@corp.com")]

    assert await script.run(fetch) == 1
    out = capsys.readouterr().out
    assert str(first[0]) in out and str(second[0]) in out
    assert str(ORG_A) in out and str(ORG_B) in out
    assert "corp.com" not in out.lower()
