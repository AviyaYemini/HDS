#!/usr/bin/env bash
# T385: two test runs at the same time must both pass and leave no database behind.
#
#   scripts/two_runs_at_once.sh
#
# Not collected by pytest. Needs the local PostgreSQL. TEST_DATABASE_URL must be unset, or both runs
# would share that database, which is what this checks does not happen by default.
set -uo pipefail
cd "$(dirname "$0")/.."
unset TEST_DATABASE_URL

logs="$(mktemp -d)"
list_run_databases() {
  uv run python - <<'PY'
import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from tests.run_database import DEFAULT_SERVER, RUN_DATABASE


async def main() -> None:
    engine = create_async_engine(DEFAULT_SERVER)
    async with engine.connect() as connection:
        rows = await connection.execute(text("SELECT datname FROM pg_database ORDER BY 1"))
        names = [row[0] for row in rows]
    await engine.dispose()
    print("\n".join(n for n in names if RUN_DATABASE.fullmatch(n)))


asyncio.run(main())
PY
}

before="$(list_run_databases)"
uv run pytest tests/integration/test_me.py tests/integration/test_migration_018.py -q \
  -p no:cacheprovider > "$logs/first.log" 2>&1 &
first=$!
uv run pytest tests/integration/test_me.py tests/integration/test_migration_018.py -q \
  -p no:cacheprovider > "$logs/second.log" 2>&1 &
second=$!
wait "$first"; first_status=$?
wait "$second"; second_status=$?
# Only what these runs added counts: another run may start or end meanwhile.
left="$(comm -13 <(echo "$before") <(list_run_databases))"

echo "first run: $first_status, second run: $second_status (logs in $logs)"
if [ "$first_status" -ne 0 ] || [ "$second_status" -ne 0 ]; then exit 1; fi
if [ -n "$left" ]; then
  echo "left behind: $left"
  exit 1
fi
echo "both passed, nothing left behind"
