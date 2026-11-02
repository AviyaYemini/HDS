"""The Schemathesis CLI fuzzes with write access, so it must refuse non-scratch databases (T145).

Covers: no spec requirement; protects real data from the contract run (Constitution IV).
"""

import os
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "contract_cli.sh"


def _run(database_url: str | None) -> subprocess.CompletedProcess[str]:
    env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
    if database_url is not None:
        env["DATABASE_URL"] = database_url
    # Port 9 (discard) refuses connections, so a script that gets past the guard fails fast.
    return subprocess.run(
        [str(SCRIPT), "http://127.0.0.1:9", "1"],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+asyncpg://hds:pw@localhost:5432/hds",
        "postgresql+asyncpg://hds:pw@localhost:5432/hds_dev",
        "postgresql+asyncpg://hds:pw@localhost:5432/hds_contract",
        "postgresql+asyncpg://hds:pw@localhost:5432/production",
        "postgresql+asyncpg://hds:pw@localhost:5432/hds_test_backup",
        "postgresql+asyncpg://hds:pw@localhost:5432/",
        None,
    ],
)
def test_refuses_databases_that_are_not_test_or_scratch(url: str | None) -> None:
    result = _run(url)
    assert result.returncode == 3, result.stderr
    assert "REFUSING" in result.stderr
    assert "_scratch" in result.stderr  # tells the user how to point at a scratch database
    assert "DATABASE_URL" in result.stderr


@pytest.mark.parametrize("name", ["hds_scratch", "hds_test", "x_scratch"])
def test_allows_test_and_scratch_databases(name: str) -> None:
    result = _run(f"postgresql+asyncpg://hds:pw@localhost:5432/{name}")
    assert result.returncode != 3, result.stderr
    assert "REFUSING" not in result.stderr
