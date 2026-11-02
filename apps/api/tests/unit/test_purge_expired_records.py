"""The bulk cleanup works out its cut-offs from the clock and the retention (T365, F08).

The script is split as ``check_site_timezones.py`` is: this pure decision, then ``run``, then a thin
database entry. ``tests/integration/test_purge_expired_records.py`` runs it on PostgreSQL.

Covers: FR-209, FR-221.
"""

from datetime import UTC, datetime, timedelta

from hds.scripts import purge_expired_records as script

NOW = datetime(2026, 11, 3, 12, tzinfo=UTC)


def test_the_throttle_cut_off_is_the_retention_before_now() -> None:
    assert script.throttle_cut_off(NOW, 24) == NOW - timedelta(hours=24)
