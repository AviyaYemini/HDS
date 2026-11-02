"""Sign-in takes the same time for unknown and known addresses (T279, SC-207). Opt-in.

This is the statistical companion of ``tests/unit/test_login_equal_work.py``, which counts the
work and gates every run. Timing is noisy on shared machines, so this test is marked ``timing`` and
is skipped unless asked for:

    uv run pytest -m timing tests/integration/test_login_timing.py

It sends 500 wrong-password sign-ins for an unknown address and 500 for a known one, interleaved
so drift affects both equally, then compares the two samples with a Mann-Whitney U test (normal
approximation) and compares their medians. Run it by hand before a release.

What it asserts: the medians differ by less than 15%. The p-value is printed, not asserted: with
500 samples each, even a real difference of a few milliseconds is "significant", and one remains
by design. A known account's wrong password also writes its failure count and commits it (about
3 ms on a development machine against about 55 ms for the Argon2 check, a 5% difference), which
an unknown address cannot do. Without the dummy check the same measurement shows a gap of about
an order of magnitude, which is what this test exists to catch.

Covers: FR-214, SC-207.
"""

import math
import statistics
import time

import pytest
from httpx import AsyncClient

from hds.models.user import User

pytestmark = pytest.mark.timing

SAMPLES = 500


def mann_whitney_p(a: list[float], b: list[float]) -> float:
    """Two-sided p-value of the Mann-Whitney U test, using the normal approximation."""
    combined = sorted([(v, 0) for v in a] + [(v, 1) for v in b])
    ranks = [0.0] * len(combined)
    i = 0
    while i < len(combined):
        j = i
        while j + 1 < len(combined) and combined[j + 1][0] == combined[i][0]:
            j += 1
        for k in range(i, j + 1):
            ranks[k] = (i + j) / 2 + 1
        i = j + 1
    rank_sum_a = sum(r for r, (_, group) in zip(ranks, combined, strict=True) if group == 0)
    n1, n2 = len(a), len(b)
    u = rank_sum_a - n1 * (n1 + 1) / 2
    mean = n1 * n2 / 2
    sd = math.sqrt(n1 * n2 * (n1 + n2 + 1) / 12)
    z = (u - mean) / sd
    return math.erfc(abs(z) / math.sqrt(2))


async def test_unknown_and_known_addresses_cannot_be_told_apart_by_timing(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    unknown: list[float] = []
    known: list[float] = []
    for _ in range(SAMPLES):
        for email, bucket in (("nobody@hds.test", unknown), ("employee@hds.test", known)):
            started = time.perf_counter()
            response = await async_client.post(
                "/auth/login", json={"email": email, "password": "not-the-password"}
            )
            bucket.append(time.perf_counter() - started)
            assert response.status_code == 401
        if len(known) % 4 == 0:  # keep the real account from locking; it is not what is measured
            await async_client.post(
                "/auth/login", json={"email": "employee@hds.test", "password": "Dev1234!"}
            )

    median_unknown, median_known = statistics.median(unknown), statistics.median(known)
    ratio = max(median_unknown, median_known) / min(median_unknown, median_known)
    p_value = mann_whitney_p(unknown, known)
    print(  # shown with -s: the evidence for a release
        f"\nmedian unknown {median_unknown * 1000:.1f} ms, known {median_known * 1000:.1f} ms, "
        f"ratio {ratio:.3f}, p={p_value:.3g}"
    )
    assert ratio < 1.15, (
        f"medians differ by {ratio:.2f}x ({median_unknown:.4f}s vs {median_known:.4f}s)"
    )
