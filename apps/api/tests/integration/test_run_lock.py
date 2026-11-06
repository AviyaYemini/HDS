"""A second test run on the same database stops at once (the harness's test-database guard).

The session holds the advisory lock on its database from the start; another run's attempt is
refused with a message naming the database, and once released, the lock can be taken again.

Covers: the harness's test-database guard (.claude/hds/harness-log.md, item 4).
"""

import pytest
from sqlalchemy.engine import make_url

from tests.run_lock import RunLock


def test_a_second_run_on_the_same_database_is_refused_naming_it(migrated_test_db: str) -> None:
    second = RunLock(migrated_test_db)
    with pytest.raises(RuntimeError, match="another test run is using the database"):
        second.acquire()


def test_a_released_lock_on_another_database_can_be_taken_again(migrated_test_db: str) -> None:
    # The session's own server and credentials (CI's differ from a developer's); only the name is
    # another, so this lock is not the session's.
    url = (
        make_url(migrated_test_db)
        .set(database="hds_run_1_lockcheck_test")
        .render_as_string(hide_password=False)
    )
    first = RunLock(url)
    first.acquire()
    first.release()
    again = RunLock(url)
    again.acquire()
    again.release()
