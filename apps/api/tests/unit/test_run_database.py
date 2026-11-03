"""Each test run has its own database, and a killed run's is recognised (T385, F26).

Two runs at once used to share one database, so one emptied the tables the other was using, and a
test failed for no reason of its own. It happened during this very feature.

Covers: FR-245.
"""

import os

from sqlalchemy.engine import make_url

from tests.run_database import (
    database_url,
    process_exists,
    run_database_name,
    scratch_name,
    stale_databases,
)


def test_the_name_holds_the_process_id_and_ends_in_test() -> None:
    assert run_database_name(4242) == "hds_run_4242_test"


def test_an_explicit_url_wins() -> None:
    explicit = "postgresql+asyncpg://hds:x@db:5432/hds_ci_test"

    assert database_url(explicit, 4242) == explicit


def test_without_one_the_run_gets_its_own_database() -> None:
    url = make_url(database_url(None, 4242))

    assert url.database == "hds_run_4242_test"
    assert url.password == "hds_dev"


def test_an_empty_value_counts_as_not_set() -> None:
    assert make_url(database_url("", 4242)).database == "hds_run_4242_test"


def test_a_scratch_database_belongs_to_its_run() -> None:
    assert scratch_name("hds_migration_014_test", 4242) == "hds_run_4242_migration_014_test"


def test_a_database_whose_process_is_gone_is_stale() -> None:
    alive = {1}
    names = ["hds_run_1_test", "hds_run_2_test", "hds_run_2_migration_014_test"]

    stale = stale_databases(names, lambda pid: pid in alive)

    assert stale == ["hds_run_2_test", "hds_run_2_migration_014_test"]


def test_a_database_somebody_named_is_never_stale() -> None:
    names = [
        "hds",
        "hds_test",
        "hds_p3_test",
        "hds_scratch",
        "postgres",
        "hds_12_test",
        "hds_20240101_test",
        "hds_run_12_test_old",
        "xhds_run_12_test",
    ]

    assert stale_databases(names, lambda pid: False) == []


def test_this_process_exists_and_a_finished_one_does_not() -> None:
    assert process_exists(os.getpid())
    assert not process_exists(2**22 + 12345)  # above the highest process id on this system


def test_a_number_too_large_to_be_a_process_is_not_one() -> None:
    assert not process_exists(20261103120000)
